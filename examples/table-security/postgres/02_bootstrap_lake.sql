-- Run inside DuckDB as the admin principal:
--   duckdb -c ".read 02_bootstrap_lake.sql"
-- Requires: INSTALL ducklake; INSTALL postgres;
-- The test runner substitutes ${LAKE_DATA_PATH}; replace it manually if you
-- run this file by hand (e.g. with an s3:// prefix).
ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake_catalog user=lake_admin password=admin_pw' AS lake
    (DATA_PATH '${LAKE_DATA_PATH}');

-- Data inlining stores small inserts INSIDE the catalog database in
-- ducklake_inlined_data_* tables (the default limit is 10 rows per insert for
-- transactional catalogs). Those tables sit outside the row-level-security
-- policies below, so a table-level-security deployment should either disable
-- inlining (done here, persisted in ducklake_metadata) or sync per-table
-- GRANTs on the inlined tables (see 03_row_level_security.sql).
CALL lake.set_option('data_inlining_row_limit', '0');

CREATE TABLE lake.public_orders (order_id INTEGER, customer VARCHAR, amount DECIMAL(10, 2));
INSERT INTO lake.public_orders VALUES (1, 'acme', 100.50), (2, 'globex', 250.00), (3, 'initech', 75.25);

CREATE TABLE lake.hr_salaries (employee VARCHAR, salary DECIMAL(12, 2));
INSERT INTO lake.hr_salaries VALUES ('alice', 185000), ('bob', 142000);
