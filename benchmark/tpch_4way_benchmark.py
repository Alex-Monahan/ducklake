#!/usr/bin/env python3
"""
TPC-H 4-Way Benchmark: DuckLake Parquet vs DuckDB x preserve_insertion_order true/false

Runs 4 configurations:
  1. DuckLake Parquet, preserve_insertion_order=true
  2. DuckLake Parquet, preserve_insertion_order=false
  3. DuckLake DuckDB,  preserve_insertion_order=true
  4. DuckLake DuckDB,  preserve_insertion_order=false

Each configuration gets its own S3 prefix and metadata DB.
Measures insert times and query execution times for all 22 TPC-H queries.

Usage:
  python3 benchmark/tpch_4way_benchmark.py [--sf 1] [--iterations 3] [--queries 1,3,5,6]
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

# 4 configurations: (format, preserve_insertion_order)
CONFIGS = [
    {"name": "parquet_ordered",   "format": "parquet", "preserve_order": True,  "label": "Parquet (ordered)"},
    {"name": "parquet_unordered", "format": "parquet", "preserve_order": False, "label": "Parquet (unordered)"},
    {"name": "duckdb_ordered",    "format": "duckdb",  "preserve_order": True,  "label": "DuckDB (ordered)"},
    {"name": "duckdb_unordered",  "format": "duckdb",  "preserve_order": False, "label": "DuckDB (unordered)"},
]


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


def ingest_config(sf, config):
    """Ingest TPC-H data for a given configuration. Returns duration or None."""
    name = config["name"]
    fmt = config["format"]
    ordered = config["preserve_order"]
    label = config["label"]

    metadata_db = f"__bench_4way_{name}_metadata.db"
    s3_prefix = f"bench_4way_{name}/data/"

    print(f"  Ingesting: {label} ...")

    order_setting = "true" if ordered else "false"

    sql = f"""{S3_SECRET_SQL}
SET threads=4;
SET preserve_insertion_order={order_setting};
CALL dbgen(sf={sf});

ATTACH 'ducklake:{metadata_db}' AS dl
    (DATA_PATH 's3://{S3_BUCKET}/{s3_prefix}');
"""

    if fmt == "duckdb":
        sql += "CALL dl.set_option('data_file_format', 'duckdb');\n"
    else:
        # parquet_row_group_size (in rows) works with both ordered and unordered
        sql += "CALL dl.set_option('parquet_row_group_size', '122880');\n"

    for t in TPCH_TABLES:
        sql += f"CREATE TABLE dl.{t} AS SELECT * FROM {t};\n"

    sql += "DETACH dl;\nSELECT 'done';\n"

    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start

    if rc != 0:
        print(f"    ERROR: {stderr[:500]}")
        return None
    print(f"    Insert time: {duration:.2f}s")
    return duration


def run_query_config(config, query_sql):
    """Run a TPC-H query for a given configuration."""
    name = config["name"]
    metadata_db = f"__bench_4way_{name}_metadata.db"

    sql = f"""{S3_SECRET_SQL}
ATTACH 'ducklake:{metadata_db}' AS dl (READ_ONLY);
USE dl;
{query_sql}
"""
    start = time.time()
    stdout, stderr, rc = run_sql(sql)
    duration = time.time() - start
    return duration, rc, stderr


def main():
    parser = argparse.ArgumentParser(description="TPC-H 4-Way Benchmark")
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

    # === Phase 1: Ingest all 4 configurations ===
    print(f"\n{'='*80}")
    print(f"TPC-H SF={sf} - 4-Way Benchmark (Parquet/DuckDB x Ordered/Unordered)")
    print(f"{'='*80}")
    print(f"\nPhase 1: Data Ingestion")

    ingest_times = {}
    for config in CONFIGS:
        duration = ingest_config(sf, config)
        ingest_times[config["name"]] = duration

    print(f"\n  Ingest Summary:")
    for config in CONFIGS:
        t = ingest_times[config["name"]]
        tstr = f"{t:.2f}s" if t else "FAILED"
        print(f"    {config['label']:<25} {tstr}")

    # === Phase 2: Query Benchmark ===
    print(f"\nPhase 2: Query Benchmark ({iterations} iterations each)")

    # Header
    labels = [c["label"] for c in CONFIGS]
    header = f"{'Query':<8}"
    for lbl in labels:
        header += f" {lbl:>22}"
    print(header)
    print(f"{'-'*8}" + f" {'-'*22}" * len(CONFIGS))

    results = {
        "config": {
            "sf": sf,
            "iterations": iterations,
            "s3_endpoint": S3_ENDPOINT,
            "configurations": [c["label"] for c in CONFIGS],
        },
        "ingest": {c["name"]: ingest_times[c["name"]] for c in CONFIGS},
        "queries": {},
    }

    totals = {c["name"]: 0.0 for c in CONFIGS}
    wins = {c["name"]: 0 for c in CONFIGS}

    for qnum in query_nums:
        if qnum not in queries:
            print(f"Q{qnum:<6} SKIP (not found)")
            continue

        query_sql = queries[qnum]
        config_times = {}

        for config in CONFIGS:
            times = []
            for i in range(iterations):
                dur, rc, err = run_query_config(config, query_sql)
                if rc == 0:
                    times.append(dur)
                elif i == 0:
                    print(f"  [{config['label']} Q{qnum} error: {err[:200]}]", file=sys.stderr)

            avg = sum(times) / len(times) if times else None
            config_times[config["name"]] = {"avg": avg, "times": times}

        # Print row
        row = f"Q{qnum:<6}"
        avgs = {}
        for config in CONFIGS:
            avg = config_times[config["name"]]["avg"]
            avgs[config["name"]] = avg
            if avg is not None:
                row += f" {avg:>21.3f}s"
                totals[config["name"]] += avg
            else:
                row += f" {'FAIL':>22}"
        print(row)

        # Determine winner for this query
        valid = {k: v for k, v in avgs.items() if v is not None}
        if valid:
            winner = min(valid, key=valid.get)
            wins[winner] += 1

        results["queries"][qnum] = {
            c["name"]: {
                "avg": config_times[c["name"]]["avg"],
                "times": config_times[c["name"]]["times"],
            }
            for c in CONFIGS
        }

    # === Summary ===
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")

    print(f"\n  Ingest times:")
    for config in CONFIGS:
        t = ingest_times[config["name"]]
        tstr = f"{t:.2f}s" if t else "FAILED"
        print(f"    {config['label']:<25} {tstr}")

    print(f"\n  Total query times:")
    for config in CONFIGS:
        t = totals[config["name"]]
        print(f"    {config['label']:<25} {t:.3f}s")

    print(f"\n  Query wins (fastest on individual queries):")
    for config in CONFIGS:
        print(f"    {config['label']:<25} {wins[config['name']]}")

    # Comparison: ordered vs unordered
    print(f"\n  Effect of preserve_insertion_order:")
    for fmt in ["parquet", "duckdb"]:
        ordered_name = f"{fmt}_ordered"
        unordered_name = f"{fmt}_unordered"
        if totals[ordered_name] > 0 and totals[unordered_name] > 0:
            ratio = totals[ordered_name] / totals[unordered_name]
            print(f"    {fmt.capitalize()}: ordered/unordered = {ratio:.2f}x (query total)")
        ordered_ingest = ingest_times.get(ordered_name)
        unordered_ingest = ingest_times.get(unordered_name)
        if ordered_ingest and unordered_ingest:
            ratio = ordered_ingest / unordered_ingest
            print(f"    {fmt.capitalize()}: ordered/unordered = {ratio:.2f}x (ingest)")

    # Comparison: parquet vs duckdb
    print(f"\n  Format comparison (Parquet vs DuckDB):")
    for order_label, order_suffix in [("ordered", "ordered"), ("unordered", "unordered")]:
        pq_name = f"parquet_{order_suffix}"
        db_name = f"duckdb_{order_suffix}"
        if totals[pq_name] > 0 and totals[db_name] > 0:
            ratio = totals[db_name] / totals[pq_name]
            print(f"    {order_label.capitalize()}: DuckDB/Parquet = {ratio:.2f}x (query total)")

    results["summary"] = {
        "totals": totals,
        "wins": wins,
        "ingest_times": {c["name"]: ingest_times[c["name"]] for c in CONFIGS},
    }

    outfile = f"benchmark/tpch_4way_sf{sf}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {outfile}")


if __name__ == "__main__":
    main()
