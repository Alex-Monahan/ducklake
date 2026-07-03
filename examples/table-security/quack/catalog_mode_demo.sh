#!/usr/bin/env bash
# Demo: using a Quack server as the DuckLake CATALOG database
# (ATTACH 'ducklake:quack:...'). This shows the plumbing works; note that in
# this topology clients talk to the catalog directly and read/write data
# files themselves, so per-table security CANNOT be enforced here today —
# see ../README.md. Use the gateway pattern (server.sql) when you need
# table-level security with Quack.
set -euo pipefail
cd "$(dirname "$0")"

DUCKDB=${DUCKDB:-duckdb}
WORK=$(mktemp -d)
mkdir -p "$WORK/lake_data"
mkfifo "$WORK/srv.fifo"
sleep 3600 > "$WORK/srv.fifo" &
HOLDER=$!
"$DUCKDB" "$WORK/catalog_server.db" < "$WORK/srv.fifo" > "$WORK/srv.log" 2>&1 &
SERVER=$!
trap 'kill $SERVER $HOLDER 2>/dev/null' EXIT

cat > "$WORK/srv.fifo" <<'SQL'
LOAD quack;
CALL quack_serve('quack:localhost:9497', token := 'catalog_token_789', disable_ssl := true);
SQL
sleep 2

"$DUCKDB" <<SQL
LOAD quack;
LOAD ducklake;
-- META_* options are forwarded by DuckLake to the catalog-database attach,
-- which is how the Quack token reaches the quack client code.
ATTACH 'ducklake:quack:localhost:9497' AS lake
    (DATA_PATH '$WORK/lake_data', META_TOKEN 'catalog_token_789', META_DISABLE_SSL true);
CREATE TABLE lake.t1 AS SELECT 42 AS answer;
SELECT * FROM lake.t1;
SELECT snapshot_id, changes FROM lake.snapshots();
SQL
echo "quack-as-catalog demo complete"
