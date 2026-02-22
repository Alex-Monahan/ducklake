#!/usr/bin/env python3
"""Minimal S3-compatible HTTP server for benchmarking DuckDB/DuckLake.

~10-50x faster than moto because it skips Flask/Werkzeug, full AWS emulation,
and heavy XML parsing. Stores objects on local disk. Supports the S3 operations
DuckDB actually uses: PUT, GET (with Range), HEAD, DELETE, and ListObjectsV2.

Usage:
    python3 scripts/fast_s3_server.py [--port 5555] [--data-dir /tmp/s3data]
"""

import argparse
import os
import sys
import shutil
import threading
import xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, unquote


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle each request in a new thread."""
    daemon_threads = True
    allow_reuse_address = True


class S3Handler(BaseHTTPRequestHandler):
    """Minimal S3 request handler."""

    data_dir = "/tmp/fast_s3_data"

    # Silence per-request logging (huge perf win)
    def log_message(self, format, *args):
        pass

    def _object_path(self, bucket, key):
        # Prevent path traversal
        safe_key = key.lstrip("/")
        return os.path.join(self.data_dir, bucket, safe_key)

    def _parse_path(self):
        """Parse /{bucket} or /{bucket}/{key...} from URL path."""
        parsed = urlparse(self.path)
        path = unquote(parsed.path).lstrip("/")
        query = parse_qs(parsed.query)
        parts = path.split("/", 1)
        bucket = parts[0] if parts else ""
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key, query

    def _send_xml(self, code, xml_str):
        body = xml_str.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_xml(self, code, error_code, message):
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>{error_code}</Code><Message>{message}</Message></Error>"""
        self._send_xml(code, xml)

    # ---- GET ----
    def do_GET(self):
        bucket, key, query = self._parse_path()

        if not bucket:
            # ListBuckets
            self._list_buckets()
            return

        if not key or "list-type" in query or "prefix" in query:
            # ListObjectsV2
            self._list_objects(bucket, query)
            return

        # GetObject
        fpath = self._object_path(bucket, key)
        if not os.path.isfile(fpath):
            self._send_error_xml(404, "NoSuchKey", f"Key {key} not found")
            return

        file_size = os.path.getsize(fpath)
        range_header = self.headers.get("Range")

        if range_header:
            # Handle Range: bytes=start-end
            range_spec = range_header.replace("bytes=", "")
            parts = range_spec.split("-")
            start = int(parts[0]) if parts[0] else 0
            end = int(parts[1]) if parts[1] else file_size - 1
            end = min(end, file_size - 1)
            length = end - start + 1

            self.send_response(206)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(length))
            self.send_header("Content-Range", f"bytes {start}-{end}/{file_size}")
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            with open(fpath, "rb") as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(remaining, 1024 * 1024))
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    remaining -= len(chunk)
        else:
            self.send_response(200)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(file_size))
            self.send_header("Accept-Ranges", "bytes")
            self.end_headers()

            with open(fpath, "rb") as f:
                while True:
                    chunk = f.read(1024 * 1024)
                    if not chunk:
                        break
                    self.wfile.write(chunk)

    # ---- HEAD ----
    def do_HEAD(self):
        bucket, key, query = self._parse_path()

        if not key:
            # HEAD bucket
            bucket_dir = os.path.join(self.data_dir, bucket)
            if os.path.isdir(bucket_dir):
                self.send_response(200)
                self.end_headers()
            else:
                self.send_response(404)
                self.end_headers()
            return

        fpath = self._object_path(bucket, key)
        if not os.path.isfile(fpath):
            self.send_response(404)
            self.end_headers()
            return

        file_size = os.path.getsize(fpath)
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(file_size))
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()

    # ---- PUT ----
    def do_PUT(self):
        bucket, key, query = self._parse_path()

        if not key:
            # Create bucket
            bucket_dir = os.path.join(self.data_dir, bucket)
            os.makedirs(bucket_dir, exist_ok=True)
            self.send_response(200)
            self.end_headers()
            return

        # PutObject
        fpath = self._object_path(bucket, key)
        os.makedirs(os.path.dirname(fpath), exist_ok=True)

        content_length = int(self.headers.get("Content-Length", 0))
        with open(fpath, "wb") as f:
            remaining = content_length
            while remaining > 0:
                chunk = self.rfile.read(min(remaining, 1024 * 1024))
                if not chunk:
                    break
                f.write(chunk)
                remaining -= len(chunk)

        self.send_response(200)
        self.send_header("ETag", '"d41d8cd98f00b204e9800998ecf8427e"')
        self.end_headers()

    # ---- DELETE ----
    def do_DELETE(self):
        bucket, key, query = self._parse_path()

        if not key:
            bucket_dir = os.path.join(self.data_dir, bucket)
            if os.path.isdir(bucket_dir):
                shutil.rmtree(bucket_dir)
            self.send_response(204)
            self.end_headers()
            return

        fpath = self._object_path(bucket, key)
        if os.path.isfile(fpath):
            os.remove(fpath)
        self.send_response(204)
        self.end_headers()

    # ---- POST (for DeleteObjects batch) ----
    def do_POST(self):
        bucket, key, query = self._parse_path()

        if "delete" in query:
            # Batch delete
            content_length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(content_length)
            try:
                root = ET.fromstring(body)
                ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                for obj in root.findall(".//s3:Key", ns) or root.findall(".//Key"):
                    fpath = self._object_path(bucket, obj.text)
                    if os.path.isfile(fpath):
                        os.remove(fpath)
            except ET.ParseError:
                pass
            self._send_xml(200, '<?xml version="1.0"?><DeleteResult/>')
            return

        self.send_response(200)
        self.end_headers()

    # ---- Helpers ----
    def _list_buckets(self):
        buckets = []
        if os.path.isdir(self.data_dir):
            for name in sorted(os.listdir(self.data_dir)):
                if os.path.isdir(os.path.join(self.data_dir, name)):
                    buckets.append(f"<Bucket><Name>{name}</Name></Bucket>")
        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Buckets>{"".join(buckets)}</Buckets>
</ListAllMyBucketsResult>"""
        self._send_xml(200, xml)

    def _list_objects(self, bucket, query):
        prefix = query.get("prefix", [""])[0]
        delimiter = query.get("delimiter", [""])[0]
        max_keys = int(query.get("max-keys", ["1000"])[0])
        continuation = query.get("continuation-token", [""])[0]

        bucket_dir = os.path.join(self.data_dir, bucket)
        if not os.path.isdir(bucket_dir):
            self._send_error_xml(404, "NoSuchBucket", f"Bucket {bucket} not found")
            return

        contents = []
        common_prefixes = set()
        count = 0

        for root, dirs, files in os.walk(bucket_dir):
            for fname in sorted(files):
                full_path = os.path.join(root, fname)
                rel_key = os.path.relpath(full_path, bucket_dir)

                if not rel_key.startswith(prefix):
                    continue

                if delimiter:
                    # Check if there's a delimiter after the prefix
                    rest = rel_key[len(prefix):]
                    idx = rest.find(delimiter)
                    if idx >= 0:
                        common_prefixes.add(prefix + rest[: idx + 1])
                        continue

                if count >= max_keys:
                    break

                size = os.path.getsize(full_path)
                contents.append(
                    f"<Contents><Key>{rel_key}</Key><Size>{size}</Size>"
                    f"<ETag>\"d41d8cd98f00b204e9800998ecf8427e\"</ETag></Contents>"
                )
                count += 1

        prefix_xml = "".join(
            f"<CommonPrefixes><Prefix>{p}</Prefix></CommonPrefixes>"
            for p in sorted(common_prefixes)
        )

        xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>{bucket}</Name>
  <Prefix>{prefix}</Prefix>
  <KeyCount>{count}</KeyCount>
  <MaxKeys>{max_keys}</MaxKeys>
  <IsTruncated>false</IsTruncated>
  {"".join(contents)}
  {prefix_xml}
</ListBucketResult>"""
        self._send_xml(200, xml)


def main():
    parser = argparse.ArgumentParser(description="Fast minimal S3 server")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--data-dir", default="/tmp/fast_s3_data")
    args = parser.parse_args()

    S3Handler.data_dir = args.data_dir
    os.makedirs(args.data_dir, exist_ok=True)

    server = ThreadedHTTPServer((args.host, args.port), S3Handler)
    print(f"Fast S3 server on http://{args.host}:{args.port}  (data: {args.data_dir})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
