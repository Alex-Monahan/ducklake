#!/usr/bin/env bash
# End-to-end test for DuckLake table-level security over the Quack protocol.
#
# Starts a fresh Quack lakehouse gateway (see server.sql), then asserts from
# real Quack clients:
#   1. analyst can read the granted table (public_orders)
#   2. analyst cannot read the ungranted table (hr_salaries)
#   3. analyst cannot reach it via the query() escape hatch
#   4. analyst cannot read the lake's parquet files directly
#   5. analyst cannot write
#   6. analyst cannot swap out the authorization function
#   7. admin endpoint can read both tables and write
#
# Prerequisites: `duckdb` (>= 1.5.3) on PATH with the ducklake and quack
# extensions installed (INSTALL ducklake; INSTALL quack;).
set -uo pipefail
cd "$(dirname "$0")"

DUCKDB=${DUCKDB:-duckdb}
WORK=$(mktemp -d)
mkdir -p "$WORK/lake_data"

PASS=0; FAIL=0
check() { # check <description> <ok|err> <exit code>
    local desc=$1 want=$2 got=$3
    if { [ "$want" = ok ] && [ "$got" -eq 0 ]; } || { [ "$want" = err ] && [ "$got" -ne 0 ]; }; then
        echo "PASS: $desc"; PASS=$((PASS+1))
    else
        echo "FAIL: $desc (exit=$got, wanted $want)"; FAIL=$((FAIL+1))
    fi
}

echo "== start server =="
sed -e "s|\${LAKE_CATALOG}|$WORK/lake_catalog.ducklake|" \
    -e "s|\${LAKE_DATA_PATH}|$WORK/lake_data|" server.sql > "$WORK/server.sql"
# Keep stdin open after the init script so the CLI (and the server) stays up.
mkfifo "$WORK/srv.fifo"
sleep 3600 > "$WORK/srv.fifo" &
HOLDER=$!
"$DUCKDB" "$WORK/quack_server.db" < "$WORK/srv.fifo" > "$WORK/srv.log" 2>&1 &
SERVER=$!
cat "$WORK/server.sql" > "$WORK/srv.fifo"
trap 'kill $SERVER $HOLDER 2>/dev/null' EXIT
for _ in $(seq 1 20); do grep -q 'gateway ready' "$WORK/srv.log" && break; sleep 0.5; done
grep -q 'gateway ready' "$WORK/srv.log" || { echo "server failed to start"; cat "$WORK/srv.log"; exit 1; }

as_analyst() {
    "$DUCKDB" -c "LOAD quack; CREATE SECRET (TYPE quack, TOKEN 'analyst_token_456');
                  ATTACH 'quack:localhost:9495' AS remote (DISABLE_SSL true); $1"
}
as_admin() {
    "$DUCKDB" -c "LOAD quack; CREATE SECRET (TYPE quack, TOKEN 'admin_token_123');
                  ATTACH 'quack:localhost:9494' AS remote (DISABLE_SSL true); $1"
}

echo "== tests =="
as_analyst "FROM remote.query('SELECT * FROM lake.public_orders ORDER BY order_id');" >/dev/null 2>&1
check "analyst can read public_orders" ok $?

as_analyst "FROM remote.query('SELECT * FROM lake.hr_salaries');" >/dev/null 2>&1
check "analyst cannot read hr_salaries" err $?

as_analyst "FROM remote.query('SELECT * FROM query(''SELECT * FROM lake.hr_sal'' || ''aries'')');" >/dev/null 2>&1
check "analyst cannot use query() to dodge the table check" err $?

as_analyst "FROM remote.query('SELECT * FROM read_parquet(''$WORK/lake_data/**/*.parquet'')');" >/dev/null 2>&1
check "analyst cannot read lake parquet files directly" err $?

as_analyst "FROM remote.query('INSERT INTO lake.public_orders VALUES (99, ''mallory'', 1.00)');" >/dev/null 2>&1
check "analyst cannot write" err $?

as_analyst "FROM remote.query('SET GLOBAL quack_authorization_function = ''quack_nop_authorization''');" >/dev/null 2>&1
check "analyst cannot replace the authorization function" err $?

as_admin "FROM remote.query('SELECT * FROM lake.hr_salaries');
          FROM remote.query('INSERT INTO lake.public_orders VALUES (4, ''hooli'', 10.00)');" >/dev/null 2>&1
check "admin can read hr_salaries and write" ok $?

ROWS=$("$DUCKDB" -noheader -list -c "LOAD quack; CREATE SECRET (TYPE quack, TOKEN 'analyst_token_456');
        ATTACH 'quack:localhost:9495' AS remote (DISABLE_SSL true);
        FROM remote.query('SELECT count(*) FROM lake.public_orders');" 2>/dev/null | tail -1)
[ "$ROWS" = "4" ]; check "admin write landed, analyst write did not (public_orders rows=$ROWS)" ok $?

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
