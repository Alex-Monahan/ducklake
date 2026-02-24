#!/usr/bin/env python3
"""
TPC-H Benchmark: DuckLake (Parquet on S3) vs DuckLake (DuckDB on S3)

Compares DuckLake using Parquet data files vs DuckLake using DuckDB data files,
both stored on a local S3-compatible server (moto). Measures insert times and
query execution times for all 22 TPC-H queries.

Usage:
  python3 benchmark/tpch_ducklake_parquet_vs_duckdb.py [--sf 1] [--iterations 3]
"""

import subprocess
import sys
import os
import time
import json
import argparse

DUCKDB_PATH = "./build/release/duckdb"
TPCH_QUERY_DIR = "duckdb/extension/tpch/dbgen/queries"

S3_ENDPOINT = "127.0.0.1:5555"
S3_KEY_ID = "testing"
S3_SECRET = "testing"
S3_REGION = "us-east-1"
S3_BUCKET = "test-bucket"

TPCH_TABLES = ["region", "nation", "supplier", "customer", "part", "partsupp", "orders", "lineitem"]

S3_SECRET_SQL = f"""
CREATE SECRET (
    TYPE S3, PROVIDER config,
    KEY_ID '{S3_KEY_ID}', SECRET '{S3_SECRET}',
    REGION '{S3_REGION}', ENDPOINT '{S3_ENDPOINT}',
    USE_SSL false, URL_STYLE 'path'
);
"""


def run_sql(sql, timeout=600):
    """Run SQL in DuckDB and return (stdout, stderr, returncode)."""
    proc = subprocess.run(
        [DUCKDB_PATH, "-c", sql],
        capture_output=True, text=True, timeout=timeout,
    )
    return proc.stdout, proc.stderr, proc.returncode


def get_tpch_queries():
    """Load all 22 TPC-H queries."""
    queries = {}
    for i in range(1, 23):
        qfile = os.path.join(TPCH_QUERY_DIR, f"q{i:02d}.sql")
        if os.path.exists(qfile):
            with open(qfile) as f:
                queries[i] = f.read().strip()
    return queries


def ingest_ducklake_parquet(sf):
    """Create DuckLake catalog with Parquet files on S3. Returns duration."""
    print(f"  Ingesting into DuckLake (Parquet on S3)...")
    sql = f"""{S3_SECRET_SQL}
SET threads=4;
SET preserve_insertion_order=false;
CALL dbgen(sf={sf});

ATTACH 'ducklake:__bench_pq_metadata.db' AS dl_pq
    (DATA_PATH 's3://{S3_BUCKET}/bench_parquet/data/');

CALL dl_pq.set_option('parquet_row_group_size_bytes', '8MB');
"""
    for t in TPCH_TABLES:
        sql += f"CREATE TABLE dl_pq.{t} AS SELECT * FROM {t};\n"

    sql += "DETACH dl_pq;\nSELECT 'done';\n"

    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start
    if rc != 0:
        print(f"    ERROR: {stderr[:500]}")
        return None
    print(f"    Insert time: {duration:.2f}s")
    return duration


def ingest_ducklake_duckdb(sf):
    """Create DuckLake catalog with DuckDB files on S3. Returns duration."""
    print(f"  Ingesting into DuckLake (DuckDB on S3)...")
    sql = f"""{S3_SECRET_SQL}
SET threads=4;
CALL dbgen(sf={sf});

ATTACH 'ducklake:__bench_db_metadata.db' AS dl_db
    (DATA_PATH 's3://{S3_BUCKET}/bench_duckdb/data/');

CALL dl_db.set_option('data_file_format', 'duckdb');
"""
    for t in TPCH_TABLES:
        sql += f"CREATE TABLE dl_db.{t} AS SELECT * FROM {t};\n"

    sql += "DETACH dl_db;\nSELECT 'done';\n"

    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start
    if rc != 0:
        print(f"    ERROR: {stderr[:500]}")
        return None
    print(f"    Insert time: {duration:.2f}s")
    return duration


def run_query_parquet(query_sql):
    """Run a TPC-H query on DuckLake (Parquet on S3)."""
    sql = f"""{S3_SECRET_SQL}
ATTACH 'ducklake:__bench_pq_metadata.db' AS dl_pq (READ_ONLY);
USE dl_pq;
{query_sql}
"""
    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start
    return duration, rc, stderr


def run_query_duckdb(query_sql):
    """Run a TPC-H query on DuckLake (DuckDB on S3)."""
    sql = f"""{S3_SECRET_SQL}
ATTACH 'ducklake:__bench_db_metadata.db' AS dl_db (READ_ONLY);
USE dl_db;
{query_sql}
"""
    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start
    return duration, rc, stderr


def main():
    parser = argparse.ArgumentParser(description="TPC-H: DuckLake Parquet vs DuckDB on S3")
    parser.add_argument("--sf", type=float, default=1, help="TPC-H scale factor (default: 1)")
    parser.add_argument("--iterations", type=int, default=3, help="Iterations per query (default: 3)")
    parser.add_argument("--queries", default=None, help="Comma-separated query numbers (default: all 22)")
    args = parser.parse_args()

    sf = args.sf
    iterations = args.iterations

    queries = get_tpch_queries()
    if not queries:
        print(f"ERROR: No TPC-H queries found in {TPCH_QUERY_DIR}")
        sys.exit(1)

    query_nums = [int(q) for q in args.queries.split(",")] if args.queries else list(range(1, 23))

    # === Phase 1: Ingest ===
    print(f"\n{'='*70}")
    print(f"TPC-H SF={sf} - DuckLake Parquet vs DuckDB on S3")
    print(f"{'='*70}")
    print(f"\nPhase 1: Data Ingestion")

    pq_ingest = ingest_ducklake_parquet(sf)
    db_ingest = ingest_ducklake_duckdb(sf)

    print(f"\n  Ingest Summary:")
    print(f"    DuckLake (Parquet): {pq_ingest:.2f}s" if pq_ingest else "    DuckLake (Parquet): FAILED")
    print(f"    DuckLake (DuckDB):  {db_ingest:.2f}s" if db_ingest else "    DuckLake (DuckDB):  FAILED")
    if pq_ingest and db_ingest:
        ratio = db_ingest / pq_ingest
        print(f"    Ratio (DuckDB/Parquet): {ratio:.2f}x")

    # === Phase 2: Queries ===
    print(f"\nPhase 2: Query Benchmark ({iterations} iterations each)")
    print(f"{'Query':<8} {'Parquet':>12} {'DuckDB':>12} {'Ratio':>10} {'Winner':>10}")
    print(f"{'-'*8} {'-'*12} {'-'*12} {'-'*10} {'-'*10}")

    results = {
        "config": {"sf": sf, "iterations": iterations, "s3_endpoint": S3_ENDPOINT},
        "ingest": {
            "parquet_s3": pq_ingest,
            "duckdb_s3": db_ingest,
        },
        "queries": {},
    }

    pq_total = 0.0
    db_total = 0.0
    pq_wins = 0
    db_wins = 0

    for qnum in query_nums:
        if qnum not in queries:
            print(f"Q{qnum:<6} SKIP (not found)")
            continue

        query_sql = queries[qnum]
        pq_times = []
        db_times = []

        for i in range(iterations):
            # Parquet
            dur, rc, err = run_query_parquet(query_sql)
            if rc == 0:
                pq_times.append(dur)
            elif i == 0:
                print(f"  [Parquet Q{qnum} error: {err[:200]}]", file=sys.stderr)

            # DuckDB
            dur, rc, err = run_query_duckdb(query_sql)
            if rc == 0:
                db_times.append(dur)
            elif i == 0:
                print(f"  [DuckDB Q{qnum} error: {err[:200]}]", file=sys.stderr)

        pq_avg = sum(pq_times) / len(pq_times) if pq_times else None
        db_avg = sum(db_times) / len(db_times) if db_times else None

        pq_str = f"{pq_avg:.3f}s" if pq_avg is not None else "FAIL"
        db_str = f"{db_avg:.3f}s" if db_avg is not None else "FAIL"

        if pq_avg is not None and db_avg is not None:
            ratio = db_avg / pq_avg
            ratio_str = f"{ratio:.2f}x"
            winner = "Parquet" if pq_avg <= db_avg else "DuckDB"
            if pq_avg <= db_avg:
                pq_wins += 1
            else:
                db_wins += 1
            pq_total += pq_avg
            db_total += db_avg
        else:
            ratio_str = "N/A"
            winner = "N/A"

        print(f"Q{qnum:<6} {pq_str:>12} {db_str:>12} {ratio_str:>10} {winner:>10}")

        results["queries"][qnum] = {
            "parquet_avg": pq_avg,
            "duckdb_avg": db_avg,
            "parquet_times": pq_times,
            "duckdb_times": db_times,
        }

    # === Summary ===
    print(f"\n{'='*70}")
    print(f"SUMMARY")
    print(f"{'='*70}")
    print(f"  Ingest - Parquet: {pq_ingest:.2f}s, DuckDB: {db_ingest:.2f}s" if pq_ingest and db_ingest else "  Ingest: incomplete")
    if pq_total > 0 and db_total > 0:
        print(f"  Query totals - Parquet: {pq_total:.3f}s, DuckDB: {db_total:.3f}s")
        print(f"  Overall query ratio (DuckDB/Parquet): {db_total/pq_total:.2f}x")
        print(f"  Wins - Parquet: {pq_wins}, DuckDB: {db_wins}")

    results["summary"] = {
        "pq_total_query_time": pq_total,
        "db_total_query_time": db_total,
        "pq_wins": pq_wins,
        "db_wins": db_wins,
    }

    outfile = f"benchmark/tpch_ducklake_pq_vs_db_sf{sf}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {outfile}")


if __name__ == "__main__":
    main()
