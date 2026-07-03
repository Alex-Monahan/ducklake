-- DuckLake + Quack: table-level security at the protocol gateway.
--
-- This DuckDB process is BOTH the DuckLake host and the Quack server.
-- Clients never open the catalog or the data files themselves: every query
-- arrives over the Quack protocol and passes through the authorization
-- callback below before it executes. Object-store credentials and the
-- catalog therefore only need to live on this server.
--
-- Identity model: Quack's authorization callback receives (connection_id,
-- query) but no user, and the built-in way to map a connection to a user
-- (recording it from the authentication callback) requires a scalar UDF from
-- a custom extension, because SQL macros cannot write. This example instead
-- gives each ROLE its own listen endpoint + token, and maps connection ->
-- endpoint -> role via quack_active_connections() (its server_id is the
-- listen URI). Pure SQL, no custom extension needed.
--
--   quack:localhost:9494  admin    unrestricted
--   quack:localhost:9495  analyst  read-only, table grants apply
--
-- Run with:  duckdb quack_server.db -init server.sql   (or pipe this file in
-- and keep stdin open; see test_quack_security.sh). Replace the tokens and
-- LAKE_* paths for real deployments, and put the server behind TLS
-- (disable_ssl is only acceptable on localhost).

LOAD quack;
LOAD ducklake;

-- 1. The lake. A plain DuckDB-file catalog is fine here because only this
--    server process ever opens it. DATA_PATH would be s3://... in production.
ATTACH 'ducklake:${LAKE_CATALOG}' AS lake (DATA_PATH '${LAKE_DATA_PATH}');
CREATE TABLE IF NOT EXISTS lake.public_orders (order_id INTEGER, customer VARCHAR, amount DECIMAL(10, 2));
CREATE TABLE IF NOT EXISTS lake.hr_salaries (employee VARCHAR, salary DECIMAL(12, 2));
INSERT INTO lake.public_orders VALUES (1, 'acme', 100.50), (2, 'globex', 250.00), (3, 'initech', 75.25);
INSERT INTO lake.hr_salaries VALUES ('alice', 185000), ('bob', 142000);

-- 2. Role -> endpoint mapping and table grants.
CREATE OR REPLACE TABLE lake_roles (listen_uri VARCHAR PRIMARY KEY, role_name VARCHAR, unrestricted BOOLEAN);
INSERT INTO lake_roles VALUES
    ('quack:localhost:9494', 'admin', true),
    ('quack:localhost:9495', 'analyst', false);

CREATE OR REPLACE TABLE lake_table_grants (role_name VARCHAR, table_name VARCHAR);
INSERT INTO lake_table_grants VALUES ('analyst', 'public_orders');

-- 3. The authorization callback. Quack calls this for every statement a
--    client sends, before it runs: return true to allow, false to reject.
--    Notes on the checks, in order:
--    * unknown connection            -> deny (coalesce(false))
--    * unrestricted role             -> allow
--    * statement must be a read      -> the regex tolerates leading
--      whitespace (Quack's internal handshake statements arrive with a
--      leading newline, and DuckDB's trim() only strips spaces)
--    * multi-statement batches       -> deny (';' would smuggle a write
--      behind an allowed first statement)
--    * escape-hatch functions        -> deny query()/query_table() (dynamic
--      table names dodge the name check) and raw file readers (they could
--      open the lake's parquet files directly)
--    * ungranted lake tables         -> deny any statement that mentions
--      them, matched case-insensitively as whole words
CREATE OR REPLACE MACRO lake_authorize(conn_id, sql_text) AS (
    coalesce((
        SELECT CASE
            WHEN r.unrestricted THEN true
            WHEN NOT regexp_matches(lower(sql_text), '^\s*(select|from|with|describe|show|explain)\b') THEN false
            WHEN position(';' IN sql_text) > 0 THEN false
            WHEN regexp_matches(lower(sql_text), '\b(query|query_table|read_\w+|glob|getenv|sniff_csv|parquet_\w+|scan_\w+)\s*\(') THEN false
            WHEN EXISTS (
                SELECT 1
                FROM duckdb_tables() t
                WHERE t.database_name = 'lake'
                  AND regexp_matches(lower(sql_text), '\b' || lower(t.table_name) || '\b')
                  AND NOT EXISTS (
                      SELECT 1 FROM lake_table_grants g
                      WHERE g.role_name = r.role_name
                        AND lower(g.table_name) = lower(t.table_name))
            ) THEN false
            ELSE true
        END
        FROM quack_active_connections() c
        JOIN lake_roles r ON r.listen_uri = c.server_id
        WHERE c.connection_id = conn_id
    ), false)
);
SET GLOBAL quack_authorization_function = 'lake_authorize';

-- 4. One endpoint per role. Each token only opens its own endpoint, and the
--    endpoint determines the role, so token = role credential.
CALL quack_serve('quack:localhost:9494', token := 'admin_token_123', disable_ssl := true);
CALL quack_serve('quack:localhost:9495', token := 'analyst_token_456', disable_ssl := true);

SELECT 'quack lakehouse gateway ready' AS status;
