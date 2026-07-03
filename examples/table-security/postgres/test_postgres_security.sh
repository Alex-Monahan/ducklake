#!/usr/bin/env bash
# End-to-end test for DuckLake table-level security on a PostgreSQL catalog.
#
# Resets the demo cluster state, runs 01/02/03, then asserts:
#   1. analyst listing shows public_orders and NOT hr_salaries
#   2. analyst can read public_orders
#   3. analyst cannot read hr_salaries (table invisible)
#   4. analyst cannot write (catalog commit denied)
#   5. lake_admin sees both tables and can write
#
# Prerequisites: local PostgreSQL running with a superuser reachable via
# `sudo -u postgres psql`, and a `duckdb` binary (>= 1.4) on PATH with the
# ducklake and postgres extensions installable.
set -uo pipefail
cd "$(dirname "$0")"

DUCKDB=${DUCKDB:-duckdb}
PSQL="sudo -u postgres psql -v ON_ERROR_STOP=1"
LAKE_DATA_PATH=${LAKE_DATA_PATH:-$(mktemp -d)/lake_data}
mkdir -p "$LAKE_DATA_PATH"

PASS=0; FAIL=0
check() { # check <description> <expected:0|1 for success> <actual exit code>
    local desc=$1 want=$2 got=$3
    if { [ "$want" = ok ] && [ "$got" -eq 0 ]; } || { [ "$want" = err ] && [ "$got" -ne 0 ]; }; then
        echo "PASS: $desc"; PASS=$((PASS+1))
    else
        echo "FAIL: $desc (exit=$got, wanted $want)"; FAIL=$((FAIL+1))
    fi
}

echo "== reset =="
sudo -u postgres psql <<'SQL' >/dev/null
DROP DATABASE IF EXISTS ducklake_catalog;
DROP ROLE IF EXISTS analyst;
DROP ROLE IF EXISTS lake_admin;
SQL

echo "== setup =="
$PSQL -q -f 01_roles.sql || exit 1
sed "s|\${LAKE_DATA_PATH}|$LAKE_DATA_PATH|" 02_bootstrap_lake.sql | $DUCKDB || exit 1
$PSQL -q -d ducklake_catalog -f 03_row_level_security.sql || exit 1

ANALYST="ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake_catalog user=analyst password=analyst_pw' AS lake"
ADMIN="ATTACH 'ducklake:postgres:host=localhost port=5432 dbname=ducklake_catalog user=lake_admin password=admin_pw' AS lake"

echo "== tests =="
LISTING=$($DUCKDB -noheader -list -c "$ANALYST (READ_ONLY); SELECT table_name FROM duckdb_tables() WHERE database_name='lake';")
[ "$LISTING" = "public_orders" ]; check "analyst listing shows only public_orders (got: ${LISTING:-<empty>})" ok $?

$DUCKDB -c "$ANALYST (READ_ONLY); SELECT * FROM lake.public_orders;" >/dev/null 2>&1
check "analyst can read public_orders" ok $?

$DUCKDB -c "$ANALYST (READ_ONLY); SELECT * FROM lake.hr_salaries;" >/dev/null 2>&1
check "analyst cannot read hr_salaries" err $?

$DUCKDB -c "$ANALYST; INSERT INTO lake.public_orders VALUES (99, 'mallory', 1.00);" >/dev/null 2>&1
check "analyst cannot write to the lake" err $?

$DUCKDB -c "$ADMIN; SELECT * FROM lake.hr_salaries; SELECT * FROM lake.public_orders; INSERT INTO lake.public_orders VALUES (4, 'hooli', 10.00);" >/dev/null 2>&1
check "lake_admin sees both tables and can write" ok $?

ROWCOUNT=$($DUCKDB -noheader -list -c "$ADMIN (READ_ONLY); SELECT count(*) FROM lake.public_orders;")
[ "$ROWCOUNT" = "4" ]; check "admin write landed, analyst write did not (public_orders rows=$ROWCOUNT)" ok $?

echo
echo "$PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
