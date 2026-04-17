# Known Postgres Test Failures

## test/sql/alter/alter_timestamptz_promotion.test

**Status:** Known failure when running with Postgres catalog
**Line:** 29

**Description:**
The test inserts `TIMESTAMP` values and promotes the column to `TIMESTAMPTZ` via `ALTER TABLE`. It then expects the output to show `+00` (UTC offset). However, when Postgres is used as the catalog backend, the timestamptz values are rendered using the Postgres server's local timezone (e.g., `-07` for US Mountain Time) rather than UTC.

**Expected output:**
```
2025-01-15 12:30:45+00
2025-01-15 12:30:45+00
```

**Actual output (example, depends on server timezone):**
```
2025-01-15 12:30:45-07
2025-01-15 12:30:45-07
```

**Root cause:**
Postgres applies its `timezone` setting when rendering `TIMESTAMPTZ` values, so the offset in the output depends on the server's configured timezone. The test assumes UTC rendering which only holds when using DuckDB as the catalog backend.

**Possible fix:**
Either set the Postgres session timezone to UTC in the test's `on_init`, or add this test to the `skip_tests` list in `test/configs/postgres.json`.
