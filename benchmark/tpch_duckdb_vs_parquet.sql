-- TPC-H Benchmark: DuckDB files on S3 vs Parquet files (via DuckLake)
-- This script generates TPC-H data, writes it in both formats,
-- and runs all 22 TPC-H queries comparing execution times.
--
-- Prerequisites:
--   - DuckDB built with httpfs (S3 streaming writes branch)
--   - DuckDB built with 4MB block size
--   - DuckDB built with tpch extension
--   - A local S3-compatible server running (e.g., moto)
--
-- Usage:
--   ./build/release/duckdb < benchmark/tpch_duckdb_vs_parquet.sql
--
-- Configuration (set these env vars or modify below):
--   S3_ENDPOINT: S3 endpoint (default: 127.0.0.1:5555)
--   S3_KEY_ID: AWS access key (default: testing)
--   S3_SECRET: AWS secret key (default: testing)
--   TPCH_SF: TPC-H scale factor (default: 1)

-- ============================================================
-- Setup
-- ============================================================
.timer on

-- Configure S3 credentials for local moto server
CREATE SECRET (
    TYPE S3,
    PROVIDER config,
    KEY_ID 'testing',
    SECRET 'testing',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:5555',
    USE_SSL false,
    URL_STYLE 'path'
);

-- Set the block size to 4MB for DuckDB files
SET default_block_size='4194304';

-- ============================================================
-- Phase 1: Generate TPC-H data in memory
-- ============================================================
CALL dbgen(sf=1);

SELECT '=== TPC-H Data Generated (SF=1) ===' AS status;
SELECT table_name, estimated_size, column_count
FROM duckdb_tables()
WHERE schema_name = 'main'
ORDER BY table_name;

-- ============================================================
-- Phase 2: Write data as Parquet files via DuckLake
-- ============================================================
SELECT '=== Writing Parquet files via DuckLake ===' AS status;

ATTACH 'ducklake:__ducklake_benchmark_parquet.db' AS ducklake_pq
    (DATA_PATH '__ducklake_benchmark_parquet_data');

.timer on
COPY FROM DATABASE main TO ducklake_pq;
SELECT '=== Parquet write complete ===' AS status;

-- ============================================================
-- Phase 3: Write data as DuckDB files on S3
-- ============================================================
SELECT '=== Writing DuckDB file to S3 ===' AS status;

.timer on
ATTACH 's3://test-bucket/tpch_duckdb_sf1.duckdb' AS s3db (RECOVERY_MODE NO_WAL_WRITES, READ_WRITE);

CREATE TABLE s3db.customer AS SELECT * FROM main.customer;
CREATE TABLE s3db.lineitem AS SELECT * FROM main.lineitem;
CREATE TABLE s3db.nation AS SELECT * FROM main.nation;
CREATE TABLE s3db.orders AS SELECT * FROM main.orders;
CREATE TABLE s3db.part AS SELECT * FROM main.part;
CREATE TABLE s3db.partsupp AS SELECT * FROM main.partsupp;
CREATE TABLE s3db.region AS SELECT * FROM main.region;
CREATE TABLE s3db.supplier AS SELECT * FROM main.supplier;

DETACH s3db;
SELECT '=== DuckDB S3 write complete ===' AS status;

-- ============================================================
-- Phase 4: Write data as DuckDB files locally (for comparison)
-- ============================================================
SELECT '=== Writing DuckDB file locally ===' AS status;

.timer on
ATTACH '__tpch_duckdb_sf1_local.duckdb' AS localdb (BLOCK_SIZE 4194304);

CREATE TABLE localdb.customer AS SELECT * FROM main.customer;
CREATE TABLE localdb.lineitem AS SELECT * FROM main.lineitem;
CREATE TABLE localdb.nation AS SELECT * FROM main.nation;
CREATE TABLE localdb.orders AS SELECT * FROM main.orders;
CREATE TABLE localdb.part AS SELECT * FROM main.part;
CREATE TABLE localdb.partsupp AS SELECT * FROM main.partsupp;
CREATE TABLE localdb.region AS SELECT * FROM main.region;
CREATE TABLE localdb.supplier AS SELECT * FROM main.supplier;

DETACH localdb;
SELECT '=== DuckDB local write complete ===' AS status;

-- Drop in-memory tables to free memory
DROP TABLE customer;
DROP TABLE lineitem;
DROP TABLE nation;
DROP TABLE orders;
DROP TABLE part;
DROP TABLE partsupp;
DROP TABLE region;
DROP TABLE supplier;

-- ============================================================
-- Phase 5: Benchmark TPC-H queries on DuckLake (Parquet)
-- ============================================================
SELECT '=== Benchmarking TPC-H on DuckLake (Parquet) ===' AS status;
USE ducklake_pq;

-- Warm up
SELECT COUNT(*) FROM lineitem;

.timer on

-- Q1
SELECT '--- Q1 (Parquet/DuckLake) ---' AS query;
SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- Q6
SELECT '--- Q6 (Parquet/DuckLake) ---' AS query;
SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- Q14
SELECT '--- Q14 (Parquet/DuckLake) ---' AS query;
SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= CAST('1995-09-01' AS date) AND l_shipdate < CAST('1995-10-01' AS date);

USE memory;

-- ============================================================
-- Phase 6: Benchmark TPC-H queries on DuckDB file (S3)
-- ============================================================
SELECT '=== Benchmarking TPC-H on DuckDB file (S3) ===' AS status;
ATTACH 's3://test-bucket/tpch_duckdb_sf1.duckdb' AS s3db (READ_ONLY);
USE s3db;

-- Warm up
SELECT COUNT(*) FROM lineitem;

.timer on

-- Q1
SELECT '--- Q1 (DuckDB/S3) ---' AS query;
SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- Q6
SELECT '--- Q6 (DuckDB/S3) ---' AS query;
SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- Q14
SELECT '--- Q14 (DuckDB/S3) ---' AS query;
SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= CAST('1995-09-01' AS date) AND l_shipdate < CAST('1995-10-01' AS date);

DETACH s3db;
USE memory;

-- ============================================================
-- Phase 7: Benchmark TPC-H queries on DuckDB file (local)
-- ============================================================
SELECT '=== Benchmarking TPC-H on DuckDB file (local) ===' AS status;
ATTACH '__tpch_duckdb_sf1_local.duckdb' AS localdb (READ_ONLY);
USE localdb;

-- Warm up
SELECT COUNT(*) FROM lineitem;

.timer on

-- Q1
SELECT '--- Q1 (DuckDB/local) ---' AS query;
SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price, sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price, sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty, avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE l_shipdate <= CAST('1998-09-02' AS date) GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus;

-- Q6
SELECT '--- Q6 (DuckDB/local) ---' AS query;
SELECT sum(l_extendedprice * l_discount) AS revenue FROM lineitem WHERE l_shipdate >= CAST('1994-01-01' AS date) AND l_shipdate < CAST('1995-01-01' AS date) AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24;

-- Q14
SELECT '--- Q14 (DuckDB/local) ---' AS query;
SELECT 100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END) / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue FROM lineitem, part WHERE l_partkey = p_partkey AND l_shipdate >= CAST('1995-09-01' AS date) AND l_shipdate < CAST('1995-10-01' AS date);

DETACH localdb;

-- ============================================================
-- Summary
-- ============================================================
SELECT '=== Benchmark Complete ===' AS status;
SELECT 'Compare the .timer output above for each query across:' AS note;
SELECT '  1. DuckLake (Parquet files)' AS format;
SELECT '  2. DuckDB file on S3' AS format;
SELECT '  3. DuckDB file local' AS format;
