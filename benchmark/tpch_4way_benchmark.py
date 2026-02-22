#!/usr/bin/env python3
"""
TPC-H 4-Way Benchmark: DuckLake Parquet vs DuckDB x preserve_insertion_order true/false

Runs 4 configurations:
  1. DuckLake Parquet, preserve_insertion_order=true
  2. DuckLake Parquet, preserve_insertion_order=false
  3. DuckLake DuckDB,  preserve_insertion_order=true
  4. DuckLake DuckDB,  preserve_insertion_order=false

Uses a single DuckDB process per config+iteration to avoid process startup overhead.
Measures actual query execution time via DuckDB's .timer, not wall-clock subprocess time.

Usage:
  python3 benchmark/tpch_4way_benchmark.py [--sf 1] [--iterations 3] [--queries 1,3,5,6]
  python3 benchmark/tpch_4way_benchmark.py --storage s3  # use moto S3 instead of local
"""

import subprocess
import sys
import os
import re
import time
import json
import argparse
import shutil

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

TIMER_RE = re.compile(r"Run Time \(s\): real ([\d.]+)")


def run_sql(sql, timeout=600):
    """Run SQL in DuckDB and return (stdout, stderr, returncode)."""
    proc = subprocess.run(
        [DUCKDB_PATH, "-c", sql],
        capture_output=True, text=True, timeout=timeout,
    )
    return proc.stdout, proc.stderr, proc.returncode


def run_sql_stdin(sql, timeout=600):
    """Run SQL via DuckDB stdin (for multi-statement scripts). Returns (stdout, stderr, rc)."""
    proc = subprocess.run(
        [DUCKDB_PATH],
        input=sql, capture_output=True, text=True, timeout=timeout,
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


def get_data_path(config, storage):
    """Get the data path for a config (local dir or S3 prefix)."""
    name = config["name"]
    if storage == "local":
        return f"__bench_4way_{name}_data/"
    else:
        return f"s3://{S3_BUCKET}/bench_4way_{name}/data/"


def ingest_config(sf, config, storage):
    """Ingest TPC-H data for a given configuration. Returns duration or None."""
    name = config["name"]
    fmt = config["format"]
    ordered = config["preserve_order"]
    label = config["label"]

    metadata_db = f"__bench_4way_{name}_metadata.db"
    data_path = get_data_path(config, storage)

    # Clean up previous run
    if os.path.exists(metadata_db):
        os.remove(metadata_db)
    local_data_dir = f"__bench_4way_{name}_data"
    if os.path.exists(local_data_dir):
        shutil.rmtree(local_data_dir)

    print(f"  Ingesting: {label} ...", flush=True)

    order_setting = "true" if ordered else "false"
    secret_sql = S3_SECRET_SQL if storage == "s3" else ""

    sql = f"""{secret_sql}
SET threads=4;
SET preserve_insertion_order={order_setting};
CALL dbgen(sf={sf});

ATTACH 'ducklake:{metadata_db}' AS dl
    (DATA_PATH '{data_path}');
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
    print(f"    Insert time: {duration:.2f}s", flush=True)
    return duration


def run_queries_single_process(config, query_nums, queries, storage, timeout=600):
    """Run multiple TPC-H queries in a single DuckDB process. Returns {qnum: real_time}.

    Uses .timer on with full output (no /dev/null) so timer lines are captured.
    Pattern after setup: marker(0.001s), query(real_time), marker, query, ...
    Query timers are at odd positions (1, 3, 5...) after the setup marker.
    """
    name = config["name"]
    metadata_db = f"__bench_4way_{name}_metadata.db"
    secret_sql = S3_SECRET_SQL if storage == "s3" else ""

    # Build SQL: setup without timer, then enable timer for queries
    # We disable timer during setup to avoid noise from ATTACH/USE
    sql = f"""{secret_sql}
ATTACH 'ducklake:{metadata_db}' AS dl (READ_ONLY);
USE dl;
.timer on
SELECT '---SETUP_DONE---';
"""

    for qnum in query_nums:
        if qnum not in queries:
            continue
        # Pattern: marker SELECT, then actual query
        # Timer lines will be: marker(~0.001s), query(real_time)
        sql += f"SELECT '---QSTART_{qnum}---';\n"
        sql += f"{queries[qnum]};\n"

    stdout, stderr, rc = run_sql_stdin(sql, timeout=timeout)
    if rc != 0:
        return None, stderr

    # Parse all "Run Time (s): real X.XXX" lines from stdout
    all_times = TIMER_RE.findall(stdout)
    all_times = [float(t) for t in all_times]

    # After SETUP_DONE, timer lines alternate: marker_time, query_time, marker_time, query_time...
    # First timer line is for SETUP_DONE marker itself
    # Then: QSTART marker (skip), query (capture), QSTART marker (skip), query (capture), ...

    # Find SETUP_DONE in stdout to locate the starting timer index
    lines = stdout.split("\n")
    setup_timer_count = 0
    for line in lines:
        if "---SETUP_DONE---" in line:
            break
        if "Run Time" in line:
            setup_timer_count += 1

    # Timer line at setup_timer_count is for SETUP_DONE marker
    # After that: pairs of (marker_timer, query_timer)
    after_setup = all_times[setup_timer_count + 1:]  # skip SETUP_DONE's timer

    result = {}
    qi = 0
    for qnum in query_nums:
        if qnum not in queries:
            continue
        idx = qi * 2 + 1  # odd positions are query timers (0=marker, 1=query, 2=marker, 3=query...)
        if idx < len(after_setup):
            result[qnum] = after_setup[idx]
        else:
            result[qnum] = None
        qi += 1

    return result, None


def main():
    parser = argparse.ArgumentParser(description="TPC-H 4-Way Benchmark")
    parser.add_argument("--sf", type=float, default=1, help="TPC-H scale factor (default: 1)")
    parser.add_argument("--iterations", type=int, default=3, help="Iterations per query (default: 3)")
    parser.add_argument("--queries", default=None, help="Comma-separated query numbers (default: all 22)")
    parser.add_argument("--storage", choices=["local", "s3"], default="local",
                        help="Storage backend: 'local' (default) or 's3' (moto)")
    args = parser.parse_args()

    sf = args.sf
    iterations = args.iterations
    storage = args.storage

    queries = get_tpch_queries()
    if not queries:
        print(f"ERROR: No TPC-H queries found in {TPCH_QUERY_DIR}")
        sys.exit(1)

    query_nums = [int(q) for q in args.queries.split(",")] if args.queries else list(range(1, 23))

    # === Phase 1: Ingest all 4 configurations ===
    print(f"\n{'='*80}")
    print(f"TPC-H SF={sf} - 4-Way Benchmark (Parquet/DuckDB x Ordered/Unordered)")
    print(f"Storage: {storage} | Iterations: {iterations}")
    print(f"{'='*80}")
    print(f"\nPhase 1: Data Ingestion", flush=True)

    ingest_times = {}
    for config in CONFIGS:
        duration = ingest_config(sf, config, storage)
        ingest_times[config["name"]] = duration

    print(f"\n  Ingest Summary:")
    for config in CONFIGS:
        t = ingest_times[config["name"]]
        tstr = f"{t:.2f}s" if t else "FAILED"
        print(f"    {config['label']:<25} {tstr}")

    # === Phase 2: Query Benchmark ===
    print(f"\nPhase 2: Query Benchmark ({iterations} iterations each)")
    print(f"  (Using single-process timing via .timer - measures actual query execution)")
    sys.stdout.flush()

    # Run all iterations for all configs
    # all_run_times[config_name][iteration] = {qnum: time}
    all_run_times = {c["name"]: [] for c in CONFIGS}

    for iteration in range(iterations):
        print(f"\n  --- Iteration {iteration + 1}/{iterations} ---", flush=True)
        for config in CONFIGS:
            qtimes, err = run_queries_single_process(config, query_nums, queries, storage)
            if qtimes is None:
                print(f"    {config['label']}: ERROR - {err[:200] if err else 'unknown'}", flush=True)
                all_run_times[config["name"]].append({})
            else:
                all_run_times[config["name"]].append(qtimes)
                total = sum(t for t in qtimes.values() if t is not None)
                print(f"    {config['label']:<25} total: {total:.3f}s", flush=True)

    # === Compute averages ===
    print(f"\n{'='*80}")
    print(f"RESULTS (average of {iterations} iterations)")
    print(f"{'='*80}")

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
            "storage": storage,
            "timing_method": "duckdb_timer_single_process",
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

        row = f"Q{qnum:<6}"
        avgs = {}

        for config in CONFIGS:
            times = []
            for run in all_run_times[config["name"]]:
                t = run.get(qnum)
                if t is not None:
                    times.append(t)

            avg = sum(times) / len(times) if times else None
            avgs[config["name"]] = avg

            if avg is not None:
                row += f" {avg:>21.3f}s"
                totals[config["name"]] += avg
            else:
                row += f" {'FAIL':>22}"

            results.setdefault("queries", {})[qnum] = results.get("queries", {}).get(qnum, {})
            results["queries"][qnum][config["name"]] = {
                "avg": avg,
                "times": times,
            }

        print(row, flush=True)

        # Determine winner
        valid = {k: v for k, v in avgs.items() if v is not None}
        if valid:
            winner = min(valid, key=valid.get)
            wins[winner] += 1

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

    outfile = f"benchmark/tpch_4way_{storage}_sf{sf}.json"
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {outfile}")


if __name__ == "__main__":
    main()
