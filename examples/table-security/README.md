# Table-level security for a DuckLake lakehouse

Two tested, self-contained implementations of per-table access control for
DuckLake, one for each common catalog/transport choice:

| Directory   | Topology                                             | Enforcement mechanism                              |
|-------------|------------------------------------------------------|----------------------------------------------------|
| `postgres/` | PostgreSQL catalog, clients attach the lake directly | PostgreSQL row-level security on the catalog tables |
| `quack/`    | DuckDB serving the lake over the Quack protocol      | Quack `quack_authorization_function` callback       |

DuckLake itself has no `GRANT` statement: a DuckLake client can see exactly
the tables whose *metadata rows* it can read, and can commit exactly when it
can *write* to the catalog. Security is therefore implemented in the layer
that stores the catalog (Postgres) or the layer that transports the queries
(Quack) — which is precisely what these examples do.

## PostgreSQL catalog: row-level security (`postgres/`)

Every DuckLake table is described by rows keyed by `table_id` in the catalog
tables (`ducklake_table`, `ducklake_column`, `ducklake_data_file`,
`ducklake_table_stats`, ...). Postgres RLS policies filter those rows per
role, so an unauthorized table simply *does not exist* from the client's
point of view: it is absent from `SHOW TABLES`, unresolvable in queries, and
its schema, statistics, and data-file paths are all invisible. A role with
only `SELECT` on the catalog is additionally read-only: DuckLake commits
insert into `ducklake_snapshot` and fail with `permission denied`, rolling
back the whole transaction.

Run it (needs local PostgreSQL + `duckdb` with the `ducklake` and `postgres`
extensions):

```bash
./postgres/test_postgres_security.sh
```

The scripts, in order:

1. `01_roles.sql` — creates `lake_admin` (catalog owner) and `analyst`.
2. `02_bootstrap_lake.sql` — attaches
   `ducklake:postgres:...user=lake_admin...`, creates `public_orders` and
   `hr_salaries`, and persists `data_inlining_row_limit = 0` (see caveats).
3. `03_row_level_security.sql` — creates the `lake_table_grants(role_name,
   table_id)` mapping, enables RLS with a `table_visibility` policy on every
   `ducklake_*` table that has a `table_id` column (driven off
   `information_schema`, so it survives DuckLake schema-version changes),
   grants the analyst plain `SELECT`, and grants visibility of
   `public_orders` only.

Verified behavior (all asserted by the test script):

- analyst listing shows only `public_orders`
- analyst reads `public_orders`; `SELECT * FROM lake.hr_salaries` fails with
  `Table with name hr_salaries does not exist`
- analyst `INSERT` fails at commit (`permission denied for table
  ducklake_snapshot`) and rolls back
- `lake_admin` sees both tables and writes normally (RLS exempts owners)

### Caveats you must handle in production

- **Data inlining.** Transactional catalogs inline small inserts into
  `ducklake_inlined_data_<table_id>_<v>` tables *inside* the catalog. Those
  have no `table_id` column, so the RLS policy does not cover them. Either
  disable inlining (`CALL lake.set_option('data_inlining_row_limit', '0')`
  plus `CALL ducklake_flush_inlined_data('lake')` for existing rows — what
  the bootstrap script does) or sync per-table `GRANT`s on them (step 5 of
  `03_row_level_security.sql`).
- **Storage access is a separate boundary.** RLS hides file *paths*, but a
  client that can read the whole `DATA_PATH` bucket/directory can still open
  Parquet files it discovers by other means. Pair catalog RLS with
  object-store permissions (e.g. per-prefix IAM) — or use the Quack gateway
  below, which keeps storage credentials server-side.
- **Minor metadata leaks.** `ducklake_snapshot_changes.changes_made` contains
  strings like `created_table:"main"."hr_salaries"`, and `ducklake_schema` is
  fully visible. Hidden tables' *names* can leak; their schema and data do
  not. Add policies on those tables too if names are sensitive.
- Writer roles that are not the catalog owner need `INSERT/UPDATE/DELETE` on
  the catalog tables plus policies `FOR ALL`, and `CREATE` on new inlined
  tables; the simplest split is "owner writes, everyone else reads".

## Quack: the lakehouse gateway (`quack/`)

With Quack (DuckDB's HTTP client-server protocol, beta since DuckDB v1.5.3)
the recommended security topology is a **gateway**: one DuckDB process
attaches the DuckLake and serves it with `quack_serve()`. Clients never open
the catalog or the Parquet files — every statement arrives over HTTP and is
vetted by the server's authorization callback before execution, and
object-store credentials live only on the server.

```
                    ┌──────────────── DuckDB server process ───────────────┐
 admin client ──────► quack:9494 ─┐                                        │
                    │             ├─ lake_authorize(conn_id, sql_text)     │
 analyst client ────► quack:9495 ─┘        │ allowed?                      │
                    │                      ▼                               │
                    │            ATTACH 'ducklake:...' AS lake ──► parquet │
                    └──────────────────────────────────────────────────────┘
```

Run it (needs `duckdb` >= 1.5.3 with `ducklake` and `quack` installed):

```bash
./quack/test_quack_security.sh
```

`server.sql` contains the entire server: the lake, a `lake_roles` table, a
`lake_table_grants` table, and the `lake_authorize` macro registered via
`SET GLOBAL quack_authorization_function`. Quack invokes that macro with
`(connection_id, query_text)` for **every** client statement; returning
`false` rejects it with `Authorization failed`.

**Why one endpoint per role?** The authorization callback receives a
connection id but no user identity, and the documented way to record
"connection X authenticated as user Y" from the authentication callback
needs a writing scalar UDF from a custom C++ extension (SQL macros cannot
write). This example avoids that dependency: each role gets its own
`quack_serve` endpoint + token, and the macro resolves
connection → endpoint → role with `quack_active_connections()`, whose
`server_id` is the endpoint's listen URI. For true per-user identity on a
single endpoint, ship a tiny extension exposing a `record_session()` UDF and
use the `quack_sessions` pattern from the Quack security docs.

The analyst policy in `lake_authorize` (verified by the test script):

- unknown connections are denied (`coalesce(..., false)`)
- only read-shaped statements (`SELECT/FROM/WITH/DESCRIBE/SHOW/EXPLAIN`);
  the regex allows leading whitespace because Quack's internal handshake
  statements arrive with a leading newline and DuckDB's `trim()` only strips
  spaces
- no `;` (blocks smuggling a second statement), no `query()` /
  `query_table()` (dynamic table names would dodge the name check), no
  `read_parquet()`-style raw file readers (would bypass the lake and read
  Parquet directly), no `SET` (protects `quack_authorization_function`
  itself)
- any statement mentioning a lake table not in `lake_table_grants` for the
  connection's role is denied

Verified behavior: analyst reads `public_orders` but is denied on
`hr_salaries`, on `query('...hr_sal'' || ''aries...')`, on `read_parquet()`
of the lake's data files, on `INSERT`, and on replacing the authorization
function; the admin endpoint reads and writes everything.

### Caveats

- The policy is a SQL firewall built on regexes: good defense-in-depth for a
  demo and for trusted-ish internal clients, but a determined attacker and a
  rich SQL dialect are a bad combination. For hard multi-tenant isolation,
  prefer allow-listing exact query shapes, per-role *views* + denying raw
  table names, or per-tenant server processes.
- Quack is beta (breaking changes expected until DuckDB v2.0); re-verify the
  callback contract on upgrade.
- Use real TLS in production (`disable_ssl` only ever on localhost) and
  rotate tokens; each endpoint's token is effectively the role credential.

## Quack as the DuckLake *catalog* (no table-level security)

Quack can also replace Postgres as the catalog database itself
(`./quack/catalog_mode_demo.sh`):

```sql
ATTACH 'ducklake:quack:localhost:9497' AS lake
    (DATA_PATH '...', META_TOKEN 'catalog_token_789', META_DISABLE_SSL true);
```

(`META_*` attach options are forwarded to the catalog attach — that is how
the token reaches the Quack client inside DuckLake.)

In this topology clients run DuckLake's metadata SQL against shared
`ducklake_*` rows and read/write Parquet themselves, so a boolean
query-text callback cannot hide individual tables and there is no RLS
equivalent inside DuckDB. Treat every catalog token as full-lake access, or
put the gateway pattern above in front of it. (Postgres remains the catalog
of choice when you need row-filtered, per-role catalog visibility.)

## Tested with

- DuckDB v1.5.4 (`ducklake`, `postgres`, `quack` extensions from the core
  repositories), PostgreSQL 16.13, Linux x86_64, July 2026.
- Both test scripts print `N passed, 0 failed` on this setup.
