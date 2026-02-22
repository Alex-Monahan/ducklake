#!/usr/bin/env python3
"""Start a moto S3-compatible server for testing DuckDB file writes to S3."""

import sys
import os
import subprocess
import time
import signal

DEFAULT_PORT = 5555
DEFAULT_HOST = "127.0.0.1"

def main():
    port = int(os.environ.get("MOTO_PORT", DEFAULT_PORT))
    host = os.environ.get("MOTO_HOST", DEFAULT_HOST)

    print(f"Starting moto S3 server on {host}:{port}...")

    # Start moto_server
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port), "-H", host],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to start
    for _ in range(30):
        try:
            import urllib.request
            urllib.request.urlopen(f"http://{host}:{port}/moto-api/")
            print(f"moto S3 server running on http://{host}:{port}")
            print(f"  AWS_ACCESS_KEY_ID=testing")
            print(f"  AWS_SECRET_ACCESS_KEY=testing")
            print(f"  AWS_DEFAULT_REGION=us-east-1")
            print(f"  DUCKDB_S3_ENDPOINT={host}:{port}")
            print(f"  DUCKDB_S3_USE_SSL=false")
            break
        except Exception:
            time.sleep(0.5)
    else:
        print("ERROR: moto server failed to start", file=sys.stderr)
        proc.kill()
        sys.exit(1)

    # Create a test bucket
    try:
        import boto3
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{host}:{port}",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )
        s3.create_bucket(Bucket="test-bucket")
        print("Created bucket: test-bucket")
    except Exception as e:
        print(f"Warning: Could not create test bucket: {e}", file=sys.stderr)

    # Write PID file
    pid_file = os.path.join(os.path.dirname(__file__), ".moto_s3.pid")
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

    # Wait for process
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
