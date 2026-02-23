#!/usr/bin/env python3
"""
TPC-H Block Size / Row Group Size Benchmark:
  DuckLake Parquet vs DuckLake DuckDB across different block sizes and row group sizes.

Runs a matrix of configurations:
  - Formats: "parquet" and "duckdb"
  - Block sizes (DuckDB only): 256KB, 512KB, 1MB, 2MB, 4MB, 8MB, 16MB
  - Row group sizes (both): 122880, 245760, 491520, 983040, 1966080
  - Parquet ignores block size (tested once per row group size)
  - Total: 5 Parquet + 35 DuckDB = 40 configurations

Uses gofakes3 server at 127.0.0.1:9123 for S3 storage.

Usage:
  python3 benchmark/tpch_block_rowgroup_benchmark.py
  python3 benchmark/tpch_block_rowgroup_benchmark.py --sf 1 --iterations 3
  python3 benchmark/tpch_block_rowgroup_benchmark.py --skip-parquet --block-sizes 1048576,4194304
  python3 benchmark/tpch_block_rowgroup_benchmark.py --storage local
"""

import subprocess
import sys
import os
import re
import time
import json
import argparse
import shutil
import signal
import urllib.request

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DUCKDB_PATH = "./build/release/duckdb"
TPCH_QUERY_DIR = "duckdb/extension/tpch/dbgen/queries"

S3_HOST = "127.0.0.1"
S3_PORT = 9123
S3_ENDPOINT = f"{S3_HOST}:{S3_PORT}"
S3_KEY_ID = "testing"
S3_SECRET = "testing"
S3_REGION = "us-east-1"
S3_BUCKET = "test-bucket"

TPCH_TABLES = [
    "region", "nation", "supplier", "customer",
    "part", "partsupp", "orders", "lineitem",
]

DEFAULT_BLOCK_SIZES = [262144, 524288, 1048576, 2097152, 4194304, 8388608, 16777216]
DEFAULT_ROWGROUP_SIZES = [122880, 245760, 491520, 983040, 1966080]

TIMER_RE = re.compile(r"Run Time \(s\): real ([\d.]+)")

# ---------------------------------------------------------------------------
# S3 server management (gofakes3 Go server)
# ---------------------------------------------------------------------------
REPO_ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
GOFAKES3_BINARY = os.path.join(REPO_ROOT, "build", "gofakes3")
GOFAKES3_SRC = os.path.join(REPO_ROOT, "scripts", "gofakes3")


def _wait_for_server(host, port, timeout=30):
    """Wait for the S3 server to accept connections."""
    for _ in range(timeout * 2):
        try:
            urllib.request.urlopen(f"http://{host}:{port}/")
            return True
        except Exception:
            time.sleep(0.5)
    return False


def _create_bucket(host, port, bucket=S3_BUCKET):
    """Create a bucket via HTTP PUT."""
    try:
        req = urllib.request.Request(
            f"http://{host}:{port}/{bucket}", method="PUT"
        )
        urllib.request.urlopen(req)
    except Exception as e:
        print(f"  Warning: could not create bucket '{bucket}': {e}", file=sys.stderr)


def _build_gofakes3():
    """Build the gofakes3 binary if it doesn't exist."""
    if os.path.exists(GOFAKES3_BINARY):
        return
    if not os.path.isdir(GOFAKES3_SRC):
        print(f"ERROR: gofakes3 source not found at {GOFAKES3_SRC}", file=sys.stderr)
        sys.exit(1)
    print("  Building gofakes3 from source...", flush=True)
    os.makedirs(os.path.dirname(GOFAKES3_BINARY), exist_ok=True)
    proc = subprocess.run(
        ["go", "build", "-o", GOFAKES3_BINARY, "."],
        cwd=GOFAKES3_SRC,
        capture_output=True, text=True,
    )
    if proc.returncode != 0:
        print(f"ERROR: Failed to build gofakes3: {proc.stderr}", file=sys.stderr)
        sys.exit(1)
    print(f"  Built gofakes3 at {GOFAKES3_BINARY}", flush=True)


def start_s3_server(data_dir="/tmp/bench_brg_s3_data"):
    """Start the gofakes3 S3 server and return the subprocess."""
    _build_gofakes3()

    # Clean data dir for a fresh start
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    print(f"Starting gofakes3 server on {S3_HOST}:{S3_PORT} (data: {data_dir}) ...")
    proc = subprocess.Popen(
        [GOFAKES3_BINARY,
         "--port", str(S3_PORT),
         "--host", S3_HOST,
         "--data-dir", data_dir],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if not _wait_for_server(S3_HOST, S3_PORT):
        stderr_out = ""
        try:
            stderr_out = proc.stderr.read().decode()[:1000]
        except Exception:
            pass
        print(f"ERROR: gofakes3 server failed to start. stderr: {stderr_out}", file=sys.stderr)
        proc.kill()
        sys.exit(1)

    _create_bucket(S3_HOST, S3_PORT)
    print(f"  gofakes3 server ready at http://{S3_ENDPOINT}")
    return proc


def stop_s3_server(proc):
    """Terminate the gofakes3 server subprocess."""
    if proc is None:
        return
    try:
        proc.terminate()
        proc.wait(timeout=5)
    except Exception:
        proc.kill()


# ---------------------------------------------------------------------------
# SQL helpers
# ---------------------------------------------------------------------------

def run_sql(duckdb_path, sql, timeout=600):
    """Run SQL via DuckDB -c flag. Returns (stdout, stderr, returncode)."""
    proc = subprocess.run(
        [duckdb_path, "-c", sql],
        capture_output=True, text=True, timeout=timeout,
    )
    return proc.stdout, proc.stderr, proc.returncode


def run_sql_stdin(duckdb_path, sql, timeout=600):
    """Run SQL via DuckDB stdin (multi-statement). Returns (stdout, stderr, returncode)."""
    proc = subprocess.run(
        [duckdb_path],
        input=sql, capture_output=True, text=True, timeout=timeout,
    )
    return proc.stdout, proc.stderr, proc.returncode


# ---------------------------------------------------------------------------
# TPC-H query loading
# ---------------------------------------------------------------------------

def get_tpch_queries(query_dir):
    """Load all 22 TPC-H queries from disk."""
    queries = {}
    for i in range(1, 23):
        qfile = os.path.join(query_dir, f"q{i:02d}.sql")
        if os.path.exists(qfile):
            with open(qfile) as f:
                queries[i] = f.read().strip()
    return queries


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def build_configs(formats, block_sizes, rowgroup_sizes):
    """Build the full list of configuration dicts.

    For Parquet: block_size is None, one entry per row group size.
    For DuckDB:  one entry per (block_size, row_group_size) pair.
    """
    configs = []
    if "parquet" in formats:
        for rg in rowgroup_sizes:
            configs.append({
                "name": f"parquet_rg{rg}",
                "format": "parquet",
                "block_size": None,
                "rg_size": rg,
                "label": f"Parquet rg={rg}",
            })
    if "duckdb" in formats:
        for bs in block_sizes:
            for rg in rowgroup_sizes:
                configs.append({
                    "name": f"duckdb_bs{bs}_rg{rg}",
                    "format": "duckdb",
                    "block_size": bs,
                    "rg_size": rg,
                    "label": f"DuckDB bs={bs} rg={rg}",
                })
    return configs


def s3_secret_sql():
    return f"""CREATE SECRET (
    TYPE S3, PROVIDER config,
    KEY_ID '{S3_KEY_ID}', SECRET '{S3_SECRET}',
    REGION '{S3_REGION}', ENDPOINT '{S3_ENDPOINT}',
    USE_SSL false, URL_STYLE 'path'
);
"""


def get_data_path(config, storage):
    """Return the data path (local dir or S3 prefix) for a config."""
    name = config["name"]
    if storage == "local":
        return f"__bench_{name}_data/"
    else:
        return f"s3://{S3_BUCKET}/bench_{name}/data/"


# ---------------------------------------------------------------------------
# Ingest phase
# ---------------------------------------------------------------------------

def ingest_config(duckdb_path, sf, config, storage):
    """Ingest TPC-H data for a single configuration. Returns wall-clock seconds or None."""
    name = config["name"]
    fmt = config["format"]
    block_size = config["block_size"]
    rg_size = config["rg_size"]
    label = config["label"]

    metadata_db = f"__bench_{name}_metadata.db"
    data_path = get_data_path(config, storage)

    # Clean up previous run artifacts
    for f_path in [metadata_db, metadata_db + ".wal"]:
        if os.path.exists(f_path):
            os.remove(f_path)
    local_data_dir = f"__bench_{name}_data"
    if os.path.exists(local_data_dir):
        shutil.rmtree(local_data_dir)

    print(f"  [{name}] Ingesting: {label} ...", flush=True)

    # Build SQL
    parts = []

    if storage == "s3":
        parts.append(s3_secret_sql())

    parts.append("SET threads=4;")
    parts.append("SET preserve_insertion_order=false;")

    # Block size pragma only relevant for DuckDB format
    if fmt == "duckdb" and block_size is not None:
        parts.append(f"PRAGMA duckdb_block_size='{block_size}';")

    parts.append(f"CALL dbgen(sf={sf});")
    parts.append(
        f"ATTACH 'ducklake:{metadata_db}' AS dl (DATA_PATH '{data_path}');"
    )
    parts.append(f"CALL dl.set_option('data_file_format', '{fmt}');")
    parts.append(f"CALL dl.set_option('parquet_row_group_size', '{rg_size}');")

    for t in TPCH_TABLES:
        parts.append(f"CREATE TABLE dl.{t} AS SELECT * FROM {t};")

    parts.append("DETACH dl;")
    parts.append("SELECT 'ingest_done';")

    sql = "\n".join(parts)

    start = time.time()
    stdout, stderr, rc = run_sql(duckdb_path, sql, timeout=1200)
    duration = time.time() - start

    if rc != 0:
        print(f"    ERROR during ingest: {stderr[:600]}", flush=True)
        return None

    print(f"    Insert time: {duration:.2f}s", flush=True)
    return duration


# ---------------------------------------------------------------------------
# Query phase (single-process, .timer based)
# ---------------------------------------------------------------------------

def run_queries_single_process(duckdb_path, config, query_nums, queries, storage, timeout=600):
    """Run TPC-H queries inside one DuckDB process using .timer.

    Returns ({qnum: real_seconds}, error_string_or_None).
    """
    name = config["name"]
    metadata_db = f"__bench_{name}_metadata.db"

    parts = []
    if storage == "s3":
        parts.append(s3_secret_sql())

    parts.append("SET threads=4;")
    parts.append(f"ATTACH 'ducklake:{metadata_db}' AS dl (READ_ONLY);")
    parts.append("USE dl;")
    parts.append(".timer on")
    parts.append("SELECT '---SETUP_DONE---';")

    for qnum in query_nums:
        if qnum not in queries:
            continue
        parts.append(f"SELECT '---QSTART_{qnum}---';")
        parts.append(f"{queries[qnum]};")

    sql = "\n".join(parts)
    stdout, stderr, rc = run_sql_stdin(duckdb_path, sql, timeout=timeout)

    if rc != 0:
        return None, stderr

    # Parse all "Run Time (s): real X.XXX" lines
    all_times = [float(t) for t in TIMER_RE.findall(stdout)]

    # Find how many timer lines precede SETUP_DONE
    lines = stdout.split("\n")
    setup_timer_count = 0
    for line in lines:
        if "---SETUP_DONE---" in line:
            break
        if "Run Time" in line:
            setup_timer_count += 1

    # After SETUP_DONE timer: pairs of (marker_timer, query_timer)
    after_setup = all_times[setup_timer_count + 1:]

    result = {}
    qi = 0
    for qnum in query_nums:
        if qnum not in queries:
            continue
        idx = qi * 2 + 1  # odd positions are query timers
        if idx < len(after_setup):
            result[qnum] = after_setup[idx]
        else:
            result[qnum] = None
        qi += 1

    return result, None


# ---------------------------------------------------------------------------
# Cleanup helpers
# ---------------------------------------------------------------------------

def cleanup_config_artifacts(config, storage):
    """Remove metadata DB and local data directory for a config."""
    name = config["name"]
    for path in [f"__bench_{name}_metadata.db", f"__bench_{name}_metadata.db.wal"]:
        if os.path.exists(path):
            try:
                os.remove(path)
            except OSError:
                pass
    local_dir = f"__bench_{name}_data"
    if os.path.exists(local_dir):
        shutil.rmtree(local_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Summary / reporting
# ---------------------------------------------------------------------------

def human_size(nbytes):
    """Format byte count as human-readable string."""
    if nbytes is None:
        return "N/A"
    for unit in ["B", "KB", "MB", "GB"]:
        if abs(nbytes) < 1024.0:
            return f"{nbytes:.0f}{unit}"
        nbytes /= 1024.0
    return f"{nbytes:.0f}TB"


def print_summary(configs, ingest_times, avg_query_times, query_nums):
    """Print a formatted summary table to stdout."""
    sep = "=" * 100
    print(f"\n{sep}")
    print("RESULTS SUMMARY")
    print(sep)

    # --- Insert times ---
    print(f"\n{'--- Insert Times ---':^100}")
    print(f"  {'Config':<40} {'Insert (s)':>12}")
    print(f"  {'-'*40} {'-'*12}")
    for cfg in configs:
        t = ingest_times.get(cfg["name"])
        tstr = f"{t:.2f}" if t is not None else "FAILED"
        print(f"  {cfg['label']:<40} {tstr:>12}")

    # --- Total query times ---
    print(f"\n{'--- Total Query Times ---':^100}")
    print(f"  {'Config':<40} {'Total (s)':>12}")
    print(f"  {'-'*40} {'-'*12}")
    for cfg in configs:
        qtimes = avg_query_times.get(cfg["name"], {})
        total = sum(v for v in qtimes.values() if v is not None)
        print(f"  {cfg['label']:<40} {total:>12.3f}")

    # --- Per-query comparison (abbreviated) ---
    print(f"\n{'--- Per-Query Averages (seconds) ---':^100}")
    # Print header
    hdr = f"  {'Query':<8}"
    for cfg in configs:
        short = cfg["name"][:20]
        hdr += f" {short:>20}"
    print(hdr)
    print(f"  {'-'*8}" + f" {'-'*20}" * len(configs))

    for qnum in query_nums:
        row = f"  Q{qnum:<6}"
        for cfg in configs:
            t = avg_query_times.get(cfg["name"], {}).get(qnum)
            if t is not None:
                row += f" {t:>19.3f}s"
            else:
                row += f" {'FAIL':>20}"
        print(row)

    print(sep)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="TPC-H Block Size / Row Group Size Benchmark"
    )
    parser.add_argument(
        "--sf", type=float, default=1,
        help="TPC-H scale factor (default: 1)",
    )
    parser.add_argument(
        "--iterations", type=int, default=3,
        help="Query iterations per configuration (default: 3)",
    )
    parser.add_argument(
        "--queries", default=None,
        help="Comma-separated query numbers to run (default: all 22)",
    )
    parser.add_argument(
        "--skip-parquet", action="store_true",
        help="Skip Parquet format configurations",
    )
    parser.add_argument(
        "--skip-duckdb", action="store_true",
        help="Skip DuckDB format configurations",
    )
    parser.add_argument(
        "--block-sizes", default=None,
        help=(
            "Comma-separated block sizes in bytes "
            "(default: 262144,524288,1048576,2097152,4194304,8388608,16777216)"
        ),
    )
    parser.add_argument(
        "--rowgroup-sizes", default=None,
        help=(
            "Comma-separated row group sizes in rows "
            "(default: 122880,245760,491520,983040,1966080)"
        ),
    )
    parser.add_argument(
        "--storage", choices=["s3", "local"], default="s3",
        help="Storage backend: 's3' (default) or 'local'",
    )
    parser.add_argument(
        "--duckdb-path", default=DUCKDB_PATH,
        help=f"Path to DuckDB binary (default: {DUCKDB_PATH})",
    )
    args = parser.parse_args()

    duckdb_path = args.duckdb_path
    sf = args.sf
    iterations = args.iterations
    storage = args.storage

    if not os.path.exists(duckdb_path):
        print(f"ERROR: DuckDB binary not found at {duckdb_path}")
        sys.exit(1)

    query_dir = TPCH_QUERY_DIR
    queries = get_tpch_queries(query_dir)
    if not queries:
        print(f"ERROR: No TPC-H queries found in {query_dir}")
        sys.exit(1)

    query_nums = (
        [int(q) for q in args.queries.split(",")]
        if args.queries
        else list(range(1, 23))
    )

    # Determine formats to test
    formats = []
    if not args.skip_parquet:
        formats.append("parquet")
    if not args.skip_duckdb:
        formats.append("duckdb")
    if not formats:
        print("ERROR: both --skip-parquet and --skip-duckdb specified; nothing to do.")
        sys.exit(1)

    # Parse block/row-group sizes
    block_sizes = (
        [int(x) for x in args.block_sizes.split(",")]
        if args.block_sizes
        else DEFAULT_BLOCK_SIZES
    )
    rowgroup_sizes = (
        [int(x) for x in args.rowgroup_sizes.split(",")]
        if args.rowgroup_sizes
        else DEFAULT_ROWGROUP_SIZES
    )

    configs = build_configs(formats, block_sizes, rowgroup_sizes)

    print(f"\n{'='*100}")
    print(f"TPC-H Block Size / Row Group Size Benchmark")
    print(f"{'='*100}")
    print(f"  Scale factor : {sf}")
    print(f"  Iterations   : {iterations}")
    print(f"  Storage      : {storage}")
    print(f"  DuckDB binary: {duckdb_path}")
    print(f"  Queries      : {','.join(str(q) for q in query_nums)}")
    print(f"  Formats      : {', '.join(formats)}")
    print(f"  Block sizes  : {', '.join(human_size(b) for b in block_sizes)}")
    print(f"  Row groups   : {', '.join(str(r) for r in rowgroup_sizes)}")
    print(f"  Total configs: {len(configs)}")
    print(f"{'='*100}\n")

    # --- Start S3 server if needed ---
    s3_proc = None
    if storage == "s3":
        s3_proc = start_s3_server()

    # Register cleanup on signals
    def signal_handler(sig, frame):
        print("\nInterrupted. Cleaning up...")
        stop_s3_server(s3_proc)
        sys.exit(1)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # --- Run all configurations ---
    ingest_times = {}              # config_name -> seconds or None
    all_run_times = {}             # config_name -> [iter_dict, ...]  iter_dict = {qnum: time}
    avg_query_times = {}           # config_name -> {qnum: avg_time}

    try:
        for ci, config in enumerate(configs):
            name = config["name"]
            label = config["label"]
            print(f"\n[{ci+1}/{len(configs)}] Configuration: {label}")
            print(f"  format={config['format']}  block_size={config['block_size']}  rg_size={config['rg_size']}")

            # -- Ingest --
            duration = ingest_config(duckdb_path, sf, config, storage)
            ingest_times[name] = duration

            if duration is None:
                print(f"  SKIPPING queries for {name} (ingest failed)", flush=True)
                all_run_times[name] = []
                avg_query_times[name] = {}
                cleanup_config_artifacts(config, storage)
                continue

            # -- Query iterations --
            iter_results = []
            for it in range(iterations):
                qtimes, err = run_queries_single_process(
                    duckdb_path, config, query_nums, queries, storage
                )
                if qtimes is None:
                    print(f"    Iteration {it+1}: ERROR - {err[:300] if err else 'unknown'}", flush=True)
                    iter_results.append({})
                else:
                    total = sum(v for v in qtimes.values() if v is not None)
                    print(f"    Iteration {it+1}: total={total:.3f}s", flush=True)
                    iter_results.append(qtimes)

            all_run_times[name] = iter_results

            # Compute per-query averages
            avgs = {}
            for qnum in query_nums:
                times = [
                    run.get(qnum) for run in iter_results if run.get(qnum) is not None
                ]
                avgs[qnum] = sum(times) / len(times) if times else None
            avg_query_times[name] = avgs

            total_avg = sum(v for v in avgs.values() if v is not None)
            print(f"  Average total query time: {total_avg:.3f}s", flush=True)

            # Clean up data to free disk space before next config
            cleanup_config_artifacts(config, storage)

    finally:
        stop_s3_server(s3_proc)

    # --- Print summary ---
    print_summary(configs, ingest_times, avg_query_times, query_nums)

    # --- Build results dict ---
    results = {
        "config": {
            "sf": sf,
            "iterations": iterations,
            "storage": storage,
            "duckdb_path": duckdb_path,
            "timing_method": "duckdb_timer_single_process",
            "block_sizes": block_sizes,
            "rowgroup_sizes": rowgroup_sizes,
            "formats": formats,
            "query_nums": query_nums,
        },
        "configurations": [],
        "queries": {},
    }

    for cfg in configs:
        name = cfg["name"]
        qtimes = avg_query_times.get(name, {})
        total_q = sum(v for v in qtimes.values() if v is not None)
        results["configurations"].append({
            "name": name,
            "format": cfg["format"],
            "block_size": cfg["block_size"],
            "rg_size": cfg["rg_size"],
            "label": cfg["label"],
            "insert_time": ingest_times.get(name),
            "total_query_time": total_q,
            "per_query_avg": {str(q): qtimes.get(q) for q in query_nums},
        })

    for qnum in query_nums:
        q_entry = {}
        for cfg in configs:
            name = cfg["name"]
            iters = all_run_times.get(name, [])
            times_list = [
                run.get(qnum) for run in iters if run.get(qnum) is not None
            ]
            avg = sum(times_list) / len(times_list) if times_list else None
            q_entry[name] = {
                "avg": avg,
                "times": times_list,
            }
        results["queries"][str(qnum)] = q_entry

    results["summary"] = {
        "ingest_times": {cfg["name"]: ingest_times.get(cfg["name"]) for cfg in configs},
        "total_query_times": {
            cfg["name"]: sum(
                v for v in avg_query_times.get(cfg["name"], {}).values() if v is not None
            )
            for cfg in configs
        },
    }

    # --- Save JSON ---
    outfile = "benchmark/tpch_block_rowgroup_results.json"
    os.makedirs(os.path.dirname(outfile), exist_ok=True)
    with open(outfile, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to {outfile}")


if __name__ == "__main__":
    main()
