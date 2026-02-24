#!/usr/bin/env python3
"""
Benchmark TPC-H queries: DuckLake on moto S3 vs plain Parquet on moto S3.

Uses a single DuckDB session per system to avoid process startup overhead.
Captures per-query timing via epoch markers in the output.
"""

import os
import sys
import time
import re
import subprocess
import tempfile
import shutil

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
MOTO_HOST = "127.0.0.1"
MOTO_PORT = 5556
SF = 1
DUCKDB_BIN = os.path.join(os.path.dirname(__file__), "..", "build", "release", "duckdb")
QUERY_DIR = os.path.join(
    os.path.dirname(__file__), "..", "duckdb", "extension", "tpch", "dbgen", "queries"
)
TPCH_TABLES = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
NUM_QUERIES = 22

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def start_moto():
    """Start a moto S3 server and return the subprocess."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(MOTO_PORT), "-H", MOTO_HOST],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    import urllib.request
    for _ in range(60):
        try:
            urllib.request.urlopen(f"http://{MOTO_HOST}:{MOTO_PORT}/moto-api/")
            break
        except Exception:
            time.sleep(0.5)
    else:
        proc.kill()
        raise RuntimeError("moto server failed to start")
    import boto3
    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://{MOTO_HOST}:{MOTO_PORT}",
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket="bench-bucket")
    return proc


def s3_config_sql():
    return f"""SET s3_access_key_id='testing';
SET s3_secret_access_key='testing';
SET s3_region='us-east-1';
SET s3_endpoint='{MOTO_HOST}:{MOTO_PORT}';
SET s3_use_ssl=false;
SET s3_url_style='path';
"""


def read_query(qnum):
    path = os.path.join(QUERY_DIR, f"q{qnum:02d}.sql")
    with open(path) as f:
        return f.read()


def build_timed_queries_sql(prefix_sql, label):
    """Build a SQL script that runs all 22 TPC-H queries with timing markers."""
    parts = [prefix_sql, ""]

    for qnum in range(1, NUM_QUERIES + 1):
        query_sql = read_query(qnum)
        # Use SELECT to print markers with epoch_ms timestamps
        parts.append(f"SELECT 'TIMER_START_{label}_Q{qnum:02d}_' || epoch_ms(now()::TIMESTAMP) AS marker;")
        parts.append(query_sql)
        parts.append(f"SELECT 'TIMER_END_{label}_Q{qnum:02d}_' || epoch_ms(now()::TIMESTAMP) AS marker;")
        parts.append("")

    return "\n".join(parts)


def parse_timings(stdout, label):
    """Parse TIMER_START/TIMER_END markers from DuckDB output."""
    timings = {}
    starts = {}
    for line in stdout.split("\n"):
        m = re.search(rf'TIMER_START_{label}_Q(\d+)_(\d+)', line)
        if m:
            qnum = int(m.group(1))
            starts[qnum] = int(m.group(2))
            continue
        m = re.search(rf'TIMER_END_{label}_Q(\d+)_(\d+)', line)
        if m:
            qnum = int(m.group(1))
            end_ms = int(m.group(2))
            if qnum in starts:
                timings[qnum] = (end_ms - starts[qnum]) / 1000.0  # seconds
    return timings


def run_ducklake_benchmark(tmpdir):
    """Set up DuckLake on S3 and run all TPC-H queries."""
    ducklake_db = os.path.join(tmpdir, "ducklake_catalog.db")

    # Step 1: Setup - generate data and copy to DuckLake
    print("  Setting up DuckLake on moto S3 (SF={})...".format(SF))
    setup_sql = f"""{s3_config_sql()}
CALL dbgen(sf={SF});
ATTACH 'ducklake:{ducklake_db}' AS ducklake (DATA_PATH 's3://bench-bucket/ducklake_data/');
COPY FROM DATABASE memory TO ducklake;
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-unsigned"],
        input=setup_sql, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"ERROR setting up DuckLake:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print("  DuckLake setup complete.")

    # Step 2: Run queries in a single session
    print("  Running TPC-H queries against DuckLake...")
    query_prefix = f"""{s3_config_sql()}
ATTACH 'ducklake:{ducklake_db}' AS ducklake;
USE ducklake;
"""
    full_sql = build_timed_queries_sql(query_prefix, "DL")
    result = subprocess.run(
        [DUCKDB_BIN, "-unsigned"],
        input=full_sql, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"WARNING: DuckLake queries had errors:\n{result.stderr[:500]}", file=sys.stderr)
    timings = parse_timings(result.stdout, "DL")
    return timings, result.stderr


def run_parquet_benchmark(tmpdir):
    """Export TPC-H as Parquet on S3 and run all queries."""

    # Step 1: Setup - generate data and export to Parquet on S3
    print("  Setting up Parquet files on moto S3 (SF={})...".format(SF))
    export_stmts = "\n".join(
        f"COPY {t} TO 's3://bench-bucket/parquet_data/{t}.parquet' (FORMAT PARQUET);"
        for t in TPCH_TABLES
    )
    setup_sql = f"""{s3_config_sql()}
CALL dbgen(sf={SF});
{export_stmts}
"""
    result = subprocess.run(
        [DUCKDB_BIN, "-unsigned"],
        input=setup_sql, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"ERROR setting up Parquet:\n{result.stderr}", file=sys.stderr)
        sys.exit(1)
    print("  Parquet export complete.")

    # Step 2: Run queries in a single session using views over S3 parquet
    print("  Running TPC-H queries against Parquet on S3...")
    view_stmts = "\n".join(
        f"CREATE VIEW {t} AS SELECT * FROM read_parquet('s3://bench-bucket/parquet_data/{t}.parquet');"
        for t in TPCH_TABLES
    )
    query_prefix = f"""{s3_config_sql()}
{view_stmts}
"""
    full_sql = build_timed_queries_sql(query_prefix, "PQ")
    result = subprocess.run(
        [DUCKDB_BIN, "-unsigned"],
        input=full_sql, capture_output=True, text=True, timeout=600,
    )
    if result.returncode != 0:
        print(f"WARNING: Parquet queries had errors:\n{result.stderr[:500]}", file=sys.stderr)
    timings = parse_timings(result.stdout, "PQ")
    return timings, result.stderr


def main():
    global DUCKDB_BIN, QUERY_DIR
    DUCKDB_BIN = os.path.abspath(DUCKDB_BIN)
    QUERY_DIR = os.path.abspath(QUERY_DIR)

    print("=" * 70)
    print("TPC-H Benchmark: DuckLake on moto S3 vs Parquet on moto S3")
    print(f"Scale Factor: {SF}")
    print("=" * 70)

    if not os.path.isfile(DUCKDB_BIN):
        print(f"ERROR: DuckDB binary not found at {DUCKDB_BIN}", file=sys.stderr)
        sys.exit(1)

    # Start moto
    print("\nStarting moto S3 server...")
    moto_proc = start_moto()
    print(f"moto S3 running on {MOTO_HOST}:{MOTO_PORT}")

    tmpdir = tempfile.mkdtemp(prefix="tpch_bench_")

    try:
        print("\n--- Data Setup & Benchmark ---")

        # Run DuckLake benchmark
        dl_timings, dl_errors = run_ducklake_benchmark(tmpdir)

        # Run Parquet benchmark
        pq_timings, pq_errors = run_parquet_benchmark(tmpdir)

        # Print results table
        print("\n" + "=" * 70)
        print(f"RESULTS: TPC-H SF{SF} on moto S3")
        print("=" * 70)
        header = f"{'Query':<8} {'DuckLake (s)':>14} {'Parquet (s)':>14} {'Ratio (DL/PQ)':>15}"
        print(header)
        print("-" * len(header))

        dl_total = 0.0
        pq_total = 0.0

        for qnum in range(1, NUM_QUERIES + 1):
            label = f"Q{qnum:02d}"
            dl_t = dl_timings.get(qnum)
            pq_t = pq_timings.get(qnum)

            dl_str = f"{dl_t:.3f}" if dl_t is not None else "FAILED"
            pq_str = f"{pq_t:.3f}" if pq_t is not None else "FAILED"

            if dl_t is not None and pq_t is not None:
                ratio = f"{dl_t / pq_t:.2f}x"
            else:
                ratio = "N/A"

            if dl_t is not None:
                dl_total += dl_t
            if pq_t is not None:
                pq_total += pq_t

            print(f"{label:<8} {dl_str:>14} {pq_str:>14} {ratio:>15}")

        print("-" * len(header))
        dl_total_str = f"{dl_total:.3f}"
        pq_total_str = f"{pq_total:.3f}"
        if dl_total > 0 and pq_total > 0:
            total_ratio = f"{dl_total / pq_total:.2f}x"
        else:
            total_ratio = "N/A"
        print(f"{'TOTAL':<8} {dl_total_str:>14} {pq_total_str:>14} {total_ratio:>15}")
        print("=" * 70)

        if dl_total > 0 and pq_total > 0:
            pct = ((dl_total - pq_total) / pq_total) * 100
            if pct > 0:
                print(f"\nDuckLake is {pct:.1f}% slower than Parquet overall")
            else:
                print(f"\nDuckLake is {abs(pct):.1f}% faster than Parquet overall")

        # Show any errors
        missing_dl = [q for q in range(1, NUM_QUERIES + 1) if q not in dl_timings]
        missing_pq = [q for q in range(1, NUM_QUERIES + 1) if q not in pq_timings]
        if missing_dl:
            print(f"\nDuckLake failed queries: {missing_dl}")
            if dl_errors:
                print(f"  Errors: {dl_errors[:500]}")
        if missing_pq:
            print(f"\nParquet failed queries: {missing_pq}")
            if pq_errors:
                print(f"  Errors: {pq_errors[:500]}")

    finally:
        moto_proc.terminate()
        moto_proc.wait()
        shutil.rmtree(tmpdir, ignore_errors=True)
        print("\nCleanup complete.")


if __name__ == "__main__":
    main()
