#!/usr/bin/env python3
"""Start an S3-compatible server for testing DuckDB/DuckLake.

By default uses a minimal fast S3 server (scripts/fast_s3_server.py) that is
~100x faster than moto for benchmarking. Use --moto to fall back to the full
moto server if you need complete AWS S3 API compatibility.

When --moto is used with gunicorn available, runs moto via gunicorn's gthread
worker (1 process, N threads) for better concurrent request handling than
werkzeug's default threading.

Note: moto cannot use multiple worker processes because it stores all S3 state
in-memory. Separate processes would each have independent state, breaking
consistency. The gthread worker gives a proper thread pool within one process.
"""

import sys
import os
import subprocess
import time
import signal
import shutil
import urllib.request

DEFAULT_PORT = 5555
DEFAULT_HOST = "127.0.0.1"
DEFAULT_THREADS = 8
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)


def _wait_for_server(host, port, timeout=30):
    """Wait for the server to accept connections."""
    for _ in range(timeout * 2):
        try:
            urllib.request.urlopen(f"http://{host}:{port}/")
            return True
        except Exception:
            time.sleep(0.5)
    return False


def _create_bucket_curl(host, port, bucket="test-bucket"):
    """Create a bucket via HTTP PUT (works with both fast server and moto)."""
    try:
        req = urllib.request.Request(
            f"http://{host}:{port}/{bucket}", method="PUT"
        )
        urllib.request.urlopen(req)
        print(f"Created bucket: {bucket}")
    except Exception as e:
        print(f"Warning: Could not create test bucket: {e}", file=sys.stderr)


def _start_moto_gunicorn(host, port, threads):
    """Start moto via gunicorn gthread (1 process, N threads)."""
    print(f"Starting moto S3 via gunicorn gthread ({threads} threads) on {host}:{port}...")
    proc = subprocess.Popen(
        [
            "gunicorn",
            "--bind", f"{host}:{port}",
            "--worker-class", "gthread",
            "--workers", "1",
            "--threads", str(threads),
            "--timeout", "600",
            "scripts.moto_wsgi:app",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=PROJECT_DIR,
    )
    return proc, f"moto/gunicorn-gthread ({threads} threads)"


def _start_moto_werkzeug(host, port):
    """Start moto via its built-in werkzeug server (threaded)."""
    print(f"Starting moto S3 via werkzeug (threaded) on {host}:{port}...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port), "-H", host],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc, "moto/werkzeug (threaded)"


def _start_fast_server(host, port, data_dir):
    """Start the minimal fast S3 server."""
    fast_server = os.path.join(SCRIPT_DIR, "fast_s3_server.py")
    print(f"Starting fast S3 server on {host}:{port}...")
    proc = subprocess.Popen(
        [sys.executable, fast_server, "--port", str(port), "--host", host,
         "--data-dir", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return proc, "fast"


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Start S3-compatible server")
    parser.add_argument("--port", type=int,
                        default=int(os.environ.get("MOTO_PORT", DEFAULT_PORT)))
    parser.add_argument("--host",
                        default=os.environ.get("MOTO_HOST", DEFAULT_HOST))
    parser.add_argument("--moto", action="store_true",
                        help="Use moto instead of fast S3 server")
    parser.add_argument("--threads", type=int,
                        default=int(os.environ.get("MOTO_THREADS", DEFAULT_THREADS)),
                        help="Thread count for moto gunicorn gthread (default: 8)")
    parser.add_argument("--data-dir", default="/tmp/fast_s3_data",
                        help="Data directory for fast S3 server")
    args = parser.parse_args()

    host, port = args.host, args.port

    if args.moto:
        # Prefer gunicorn gthread for better concurrency; fall back to werkzeug
        if shutil.which("gunicorn"):
            proc, server_type = _start_moto_gunicorn(host, port, args.threads)
        else:
            print("  (gunicorn not found, using werkzeug; "
                  "install gunicorn for better concurrency: pip install gunicorn)")
            proc, server_type = _start_moto_werkzeug(host, port)
    else:
        proc, server_type = _start_fast_server(host, port, args.data_dir)

    if not _wait_for_server(host, port):
        print("ERROR: S3 server failed to start", file=sys.stderr)
        try:
            stderr = proc.stderr.read().decode() if proc.stderr else ""
            if stderr:
                print(f"  stderr: {stderr[:1000]}", file=sys.stderr)
        except Exception:
            pass
        proc.kill()
        sys.exit(1)

    print(f"S3 server running on http://{host}:{port} [{server_type}]")
    print(f"  S3_ENDPOINT={host}:{port}")
    print(f"  USE_SSL=false, URL_STYLE=path")

    _create_bucket_curl(host, port)

    # Write PID file
    pid_file = os.path.join(SCRIPT_DIR, ".moto_s3.pid")
    with open(pid_file, "w") as f:
        f.write(str(proc.pid))

    def cleanup(sig, frame):
        proc.terminate()
        try:
            os.remove(pid_file)
        except OSError:
            pass
        sys.exit(0)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)

    try:
        proc.wait()
    except KeyboardInterrupt:
        proc.terminate()
    finally:
        try:
            os.remove(pid_file)
        except OSError:
            pass


if __name__ == "__main__":
    main()
