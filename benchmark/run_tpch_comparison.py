#!/usr/bin/env python3
"""
TPC-H Benchmark: DuckDB files vs Parquet (via DuckLake)

Runs all 22 TPC-H queries in both configurations and compares timings.
Requires:
  - DuckDB binary with ducklake, httpfs, tpch extensions
  - Local S3-compatible server (moto) running on port 5555
  - DuckDB built with 4MB block size and httpfs S3 streaming writes

Usage:
  python3 benchmark/run_tpch_comparison.py [--sf 1] [--duckdb-path ./build/release/duckdb]
"""

import subprocess
import sys
import os
import time
import json
import argparse

# TPC-H query files are in the DuckDB source tree
TPCH_QUERY_DIR = "duckdb/extension/tpch/dbgen/queries"

S3_ENDPOINT = "127.0.0.1:5555"
S3_KEY_ID = "testing"
S3_SECRET = "testing"
S3_REGION = "us-east-1"
S3_BUCKET = "test-bucket"

ROW_GROUP_SIZE = 122880 * 8  # 983040


def run_duckdb_sql(duckdb_path, sql, timeout=600):
    """Run SQL in DuckDB and return (stdout, stderr, duration)."""
    start = time.time()
    proc = subprocess.run(
        [duckdb_path, "-c", sql],
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    duration = time.time() - start
    return proc.stdout, proc.stderr, duration, proc.returncode


def get_tpch_queries():
    """Load TPC-H queries from DuckDB source."""
    queries = {}
    for i in range(1, 23):
        qfile = os.path.join(TPCH_QUERY_DIR, f"q{i:02d}.sql")
        if os.path.exists(qfile):
            with open(qfile) as f:
                queries[i] = f.read().strip()
    return queries


def setup_s3_secret():
    return f"""
CREATE SECRET (
    TYPE S3, PROVIDER config,
    KEY_ID '{S3_KEY_ID}', SECRET '{S3_SECRET}',
    REGION '{S3_REGION}', ENDPOINT '{S3_ENDPOINT}',
    USE_SSL false, URL_STYLE 'path'
);
"""


def generate_tpch_data(duckdb_path, sf):
    """Generate TPC-H data and write in both formats."""
    print(f"Generating TPC-H SF={sf} data...")

    # Write Parquet via DuckLake
    sql = f"""
{setup_s3_secret()}
SET threads=4;
CALL dbgen(sf={sf});
ATTACH 'ducklake:__benchmark_ducklake_pq.db' AS dlpq (DATA_PATH '__benchmark_ducklake_pq_data');
CREATE TABLE dlpq.customer AS SELECT * FROM customer;
CREATE TABLE dlpq.lineitem AS SELECT * FROM lineitem;
CREATE TABLE dlpq.nation AS SELECT * FROM nation;
CREATE TABLE dlpq.orders AS SELECT * FROM orders;
CREATE TABLE dlpq.part AS SELECT * FROM part;
CREATE TABLE dlpq.partsupp AS SELECT * FROM partsupp;
CREATE TABLE dlpq.region AS SELECT * FROM region;
CREATE TABLE dlpq.supplier AS SELECT * FROM supplier;
DETACH dlpq;
SELECT 'DuckLake parquet write complete';
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    if rc != 0:
        print(f"ERROR generating parquet: {stderr}")
        return False
    print(f"  Parquet (DuckLake) write: {duration:.2f}s")

    # Write DuckDB file to S3
    sql = f"""
{setup_s3_secret()}
SET threads=4;
SET default_block_size='4194304';
CALL dbgen(sf={sf});
ATTACH 's3://{S3_BUCKET}/tpch_sf{sf}.duckdb' AS s3db (RECOVERY_MODE NO_WAL_WRITES, READ_WRITE);
CREATE TABLE s3db.customer AS SELECT * FROM customer;
CREATE TABLE s3db.lineitem AS SELECT * FROM lineitem;
CREATE TABLE s3db.nation AS SELECT * FROM nation;
CREATE TABLE s3db.orders AS SELECT * FROM orders;
CREATE TABLE s3db.part AS SELECT * FROM part;
CREATE TABLE s3db.partsupp AS SELECT * FROM partsupp;
CREATE TABLE s3db.region AS SELECT * FROM region;
CREATE TABLE s3db.supplier AS SELECT * FROM supplier;
DETACH s3db;
SELECT 'DuckDB S3 write complete';
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    if rc != 0:
        print(f"ERROR writing DuckDB to S3: {stderr}")
        return False
    print(f"  DuckDB (S3) write: {duration:.2f}s")

    # Write DuckDB file locally
    sql = f"""
SET threads=4;
CALL dbgen(sf={sf});
ATTACH '__benchmark_tpch_local.duckdb' AS localdb (BLOCK_SIZE 4194304);
CREATE TABLE localdb.customer AS SELECT * FROM customer;
CREATE TABLE localdb.lineitem AS SELECT * FROM lineitem;
CREATE TABLE localdb.nation AS SELECT * FROM nation;
CREATE TABLE localdb.orders AS SELECT * FROM orders;
CREATE TABLE localdb.part AS SELECT * FROM part;
CREATE TABLE localdb.partsupp AS SELECT * FROM partsupp;
CREATE TABLE localdb.region AS SELECT * FROM region;
CREATE TABLE localdb.supplier AS SELECT * FROM supplier;
DETACH localdb;
SELECT 'DuckDB local write complete';
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    if rc != 0:
        print(f"ERROR writing DuckDB locally: {stderr}")
        return False
    print(f"  DuckDB (local) write: {duration:.2f}s")

    return True


def run_benchmark_ducklake_parquet(duckdb_path, query_sql, query_num):
    """Run a single TPC-H query on DuckLake (parquet)."""
    sql = f"""
ATTACH 'ducklake:__benchmark_ducklake_pq.db' AS dlpq;
USE dlpq;
{query_sql}
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    return duration, rc, stderr


def run_benchmark_duckdb_s3(duckdb_path, query_sql, query_num, sf):
    """Run a single TPC-H query on DuckDB file (S3)."""
    # Use the pre-written full TPC-H S3 database
    sf_str = str(int(sf)) if sf == int(sf) else str(sf)
    s3_file = f"tpch_sf{sf_str}_full2.duckdb" if sf >= 1 else f"tpch_sf{sf_str}.duckdb"
    sql = f"""
{setup_s3_secret()}
ATTACH 's3://{S3_BUCKET}/{s3_file}' AS s3db (READ_ONLY);
USE s3db;
{query_sql}
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    return duration, rc, stderr


def run_benchmark_duckdb_local(duckdb_path, query_sql, query_num):
    """Run a single TPC-H query on DuckDB file (local)."""
    sql = f"""
ATTACH '__benchmark_tpch_local.duckdb' AS localdb (READ_ONLY);
USE localdb;
{query_sql}
"""
    stdout, stderr, duration, rc = run_duckdb_sql(duckdb_path, sql)
    return duration, rc, stderr


def main():
    parser = argparse.ArgumentParser(description="TPC-H Benchmark: DuckDB vs Parquet")
    parser.add_argument("--sf", type=float, default=1, help="TPC-H scale factor")
    parser.add_argument("--duckdb-path", default="./build/release/duckdb", help="Path to DuckDB binary")
    parser.add_argument("--queries", default="1,3,5,6,10,14", help="Comma-separated query numbers to run")
    parser.add_argument("--iterations", type=int, default=3, help="Number of iterations per query")
    parser.add_argument("--skip-generate", action="store_true", help="Skip data generation")
    args = parser.parse_args()

    duckdb_path = args.duckdb_path
    sf = args.sf

    if not os.path.exists(duckdb_path):
        print(f"ERROR: DuckDB binary not found at {duckdb_path}")
        sys.exit(1)

    queries = get_tpch_queries()
    if not queries:
        print(f"ERROR: No TPC-H queries found in {TPCH_QUERY_DIR}")
        sys.exit(1)

    query_nums = [int(q) for q in args.queries.split(",")]

    # Generate data
    if not args.skip_generate:
        if not generate_tpch_data(duckdb_path, sf):
            print("Data generation failed!")
            sys.exit(1)

    # Run benchmarks
    results = {}
    print(f"\n{'='*70}")
    print(f"TPC-H Benchmark SF={sf} - {args.iterations} iterations per query")
    print(f"{'='*70}")
    print(f"{'Query':<8} {'Parquet/DL':>12} {'DuckDB/S3':>12} {'DuckDB/Local':>14} {'S3 vs PQ':>10}")
    print(f"{'-'*8} {'-'*12} {'-'*12} {'-'*14} {'-'*10}")

    for qnum in query_nums:
        if qnum not in queries:
            print(f"Q{qnum:<6} SKIP (query not found)")
            continue

        query_sql = queries[qnum]
        pq_times = []
        s3_times = []
        local_times = []

        for iteration in range(args.iterations):
            # Parquet/DuckLake
            duration, rc, err = run_benchmark_ducklake_parquet(duckdb_path, query_sql, qnum)
            if rc == 0:
                pq_times.append(duration)

            # DuckDB/S3
            duration, rc, err = run_benchmark_duckdb_s3(duckdb_path, query_sql, qnum, sf)
            if rc == 0:
                s3_times.append(duration)
            elif iteration == 0:
                print(f"  [S3 Q{qnum} error: {err[:300]}]", file=sys.stderr)

            # DuckDB/Local
            duration, rc, err = run_benchmark_duckdb_local(duckdb_path, query_sql, qnum)
            if rc == 0:
                local_times.append(duration)

        pq_avg = sum(pq_times) / len(pq_times) if pq_times else float('inf')
        s3_avg = sum(s3_times) / len(s3_times) if s3_times else float('inf')
        local_avg = sum(local_times) / len(local_times) if local_times else float('inf')

        ratio = s3_avg / pq_avg if pq_avg > 0 and pq_avg != float('inf') else float('inf')
        ratio_str = f"{ratio:.2f}x" if ratio != float('inf') else "N/A"

        pq_str = f"{pq_avg:.3f}s" if pq_times else "FAIL"
        s3_str = f"{s3_avg:.3f}s" if s3_times else "FAIL"
        local_str = f"{local_avg:.3f}s" if local_times else "FAIL"

        print(f"Q{qnum:<6} {pq_str:>12} {s3_str:>12} {local_str:>14} {ratio_str:>10}")

        results[qnum] = {
            "parquet_ducklake": pq_avg if pq_times else None,
            "duckdb_s3": s3_avg if s3_times else None,
            "duckdb_local": local_avg if local_times else None,
        }

    # Save results
    results_file = f"benchmark/tpch_results_sf{sf}.json"
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {results_file}")


if __name__ == "__main__":
    main()
