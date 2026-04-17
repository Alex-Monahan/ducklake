# Plan: Replace FlushChanges with Postgres Stored Procedures

## Context

Currently, `FlushChanges()` in `ducklake_transaction.cpp` builds a batch SQL string and executes it against Postgres via `postgres_execute`, then commits with a separate `connection->Commit()` call. This requires at minimum 2 round trips per commit (execute + commit), and 3+ on retry (conflict check + execute + commit). The goal is to reduce this to 1 round trip by moving the execute, commit, retry, and conflict detection logic into PL/pgSQL stored procedures.

The stored procedure path is only used when Postgres is the metadata catalog AND no inline data operations exist in the transaction (inline operations write to dynamically-named tables that depend on runtime schema, making them incompatible with procedure-side retry). Inline table creation is allowed, but not insertion into the inlined table or deletion table.

## Architecture

### Two Procedures, Multiple Helper Functions

1. **`ducklake_commit_with_retry`** (PROCEDURE) — Wrapper with retry loop, uses `COMMIT`/`ROLLBACK`
2. **`ducklake_try_commit`** (FUNCTION) — Inner commit: get snapshot, check conflicts, replace placeholders, execute SQL
3. **`ducklake_check_conflicts`** (FUNCTION) — Full conflict detection (all ~20 conflict types from C++)
4. **`ducklake_parse_changes`** (FUNCTION) — Parse comma-separated `changes_made` string into a table of `(change_type, change_value)` pairs
5. **`ducklake_replace_placeholders`** (FUNCTION) — Replace `{SNAPSHOT_ID}`, `{SCHEMA_VERSION}`, `{CATALOG_ID_BASE}`, `{FILE_ID_BASE}`, `{NEXT_CATALOG_ID}`, `{NEXT_FILE_ID}` in SQL

### ID Offset Approach

The batch SQL uses offset-based expressions for catalog/file IDs instead of hardcoded absolute values. This allows the procedure to re-resolve IDs on retry without the C++ side rebuilding the SQL.

- `{CATALOG_ID_BASE}` resolves to `latest_snapshot.next_catalog_id`
- `{FILE_ID_BASE}` resolves to `latest_snapshot.next_file_id`
- Individual IDs formatted as `({CATALOG_ID_BASE}+N)` or `({FILE_ID_BASE}+N)`
- `{NEXT_CATALOG_ID}` = `{CATALOG_ID_BASE} + delta_catalog`
- `{NEXT_FILE_ID}` = `{FILE_ID_BASE} + delta_file`

### Stored Procedure Call Flow (1 round trip)

```
C++ builds batch_sql with offset placeholders
C++ calls: postgres_execute(catalog, 'CALL schema.ducklake_commit_with_retry(...)')
  │
  └── PROCEDURE ducklake_commit_with_retry:
      LOOP (max_retry times):
      ├── ducklake_try_commit(batch_sql, ...)
      │   ├── SELECT MAX(snapshot_id), ... FROM ducklake_snapshot
      │   ├── If snapshot > txn_start: ducklake_check_conflicts(...)
      │   ├── Compute: snapshot_id+1, schema_version, catalog_base, file_base
      │   ├── ducklake_replace_placeholders(batch_sql, ...)
      │   └── EXECUTE replaced_sql
      ├── COMMIT (success → return)
      └── On error: ROLLBACK, pg_sleep(backoff), retry
      RAISE EXCEPTION when max retries exceeded
```

## C++ Changes

### 1. `src/include/storage/ducklake_metadata_manager.hpp` — Add virtual ID formatting methods

```cpp
// New virtual methods (default: return to_string(id))
virtual string FormatCatalogId(idx_t id);
virtual string FormatFileId(idx_t id);
```

### 2. `src/storage/ducklake_metadata_manager.cpp` — Use FormatCatalogId/FormatFileId in ~10 SQL generation functions

Functions to modify (change `%d` with absolute IDs to `%s` with `FormatCatalogId`/`FormatFileId`):

- `WriteNewSchemas` — schema_id
- `WriteNewTables` — table_id, schema_id FK
- `WriteNewViews` — view_id, schema_id FK
- `WriteNewPartitionKeys` — partition_id
- `WriteNewSortKeys` — sort_id
- `WriteNewMacros` — macro_id
- `WriteNewDataFiles` — data_file_id, table_id FK
- `WriteNewDeleteFiles` — delete_file_id, data_file_id FK, table_id FK
- `WriteNewColumnMappings` — mapping_id
- `WriteCompactions` (WriteMergeAdjacent, WriteDeleteRewrites) — file IDs
- `InsertSnapshot` — `{NEXT_CATALOG_ID}` and `{NEXT_FILE_ID}` (already placeholders, just verify)
- `DropTables`, `DropDataFiles`, etc. — these use existing IDs (not newly allocated), no change needed
- `UpdateGlobalTableStats` — uses table_id which is existing, no change needed
- `WriteNewColumns` — column_id is NOT from next_catalog_id, no change needed
- `WriteNewTags`, `WriteNewColumnTags` — use existing object_id, no change needed

Cross-references: When a table references its schema_id (also newly allocated), both use `FormatCatalogId`, so they resolve consistently.

### 3. `src/include/metadata_manager/postgres_metadata_manager.hpp` — Add stored procedure support

```cpp
class PostgresMetadataManager : public DuckLakeMetadataManager {
    // Existing...

    // New: ID formatting overrides
    string FormatCatalogId(idx_t id) override;
    string FormatFileId(idx_t id) override;

    // New: stored procedure support
    void SetStoredProcedureMode(idx_t catalog_base, idx_t file_base);
    void EnsureStoredProceduresExist(const string &schema);
    unique_ptr<QueryResult> ExecuteStoredProcCommit(
        DuckLakeSnapshot snapshot, string &batch_sql,
        bool schema_changed, idx_t txn_start_snapshot,
        const string &our_changes, idx_t max_retry,
        idx_t retry_wait_ms, double retry_backoff);

private:
    bool use_stored_procedure_ = false;
    idx_t catalog_id_base_ = 0;
    idx_t file_id_base_ = 0;
    bool procedures_created_ = false;
};
```

### 4. `src/metadata_manager/postgres_metadata_manager.cpp` — Implement new methods

- `FormatCatalogId`: When `use_stored_procedure_`, return `({CATALOG_ID_BASE}+offset)` (offset = `id - catalog_id_base_`); otherwise `to_string(id)`
- `FormatFileId`: Same pattern with `{FILE_ID_BASE}`
- `EnsureStoredProceduresExist`: Execute `CREATE OR REPLACE` for all 5 functions/procedures
- `ExecuteStoredProcCommit`: Replace non-snapshot placeholders (`{METADATA_CATALOG}`, `{AUTHOR}`, etc.), then call `ducklake_commit_with_retry`
- Also partially replace placeholders in C++ that don't change between retries: `{METADATA_CATALOG}`, `{METADATA_PATH}`, `{DATA_PATH}`, `{AUTHOR}`, `{COMMIT_MESSAGE}`, `{COMMIT_EXTRA_INFO}`

### 5. `src/storage/ducklake_transaction.cpp` — Route to stored procedure in FlushChanges

In `FlushChanges()`, before the retry loop:

```cpp
// Check if we can use the stored procedure path:
// 1. Must be Postgres metadata catalog
// 2. Must have no inline operations (inline inserts, inline deletes, inline file deletes)
bool can_use_stored_proc = (ducklake_catalog.MetadataType() == "postgres");
if (can_use_stored_proc) {
    for (auto &entry : table_data_changes) {
        if (entry.second.new_inlined_data ||
            !entry.second.new_inlined_data_deletes.empty() ||
            entry.second.new_inlined_file_deletes) {
            can_use_stored_proc = false;
            break;
        }
    }
}

if (can_use_stored_proc) {
    FlushChangesStoredProc(); // New method
    return;
}
// ... existing retry loop ...
```

New method `FlushChangesStoredProc()`:
1. Get snapshot, transaction changes
2. Set stored procedure mode on metadata manager (passes catalog_base, file_base)
3. Build batch SQL (same CommitChanges + WriteSnapshotChanges calls)
4. Ensure stored procedures exist
5. Call `ExecuteStoredProcCommit(...)`
6. On success: update `catalog_version` and `SetCommittedSnapshotId`
7. Skip `connection->Commit()` (procedure already committed)
8. On failure: `CleanupFiles()` and throw

## PL/pgSQL Implementation

### `ducklake_parse_changes(p_changes TEXT)` → TABLE(change_type TEXT, change_value TEXT)

Parse comma-separated `changes_made` string. Handle quoted values (schema/table names in double quotes). Return set of (type, value) pairs.

### `ducklake_check_conflicts(p_schema TEXT, p_txn_start_snapshot BIGINT, p_our_changes TEXT)`

Full conflict detection replicating all C++ `CheckForConflicts` logic:

1. Query `changes_made` from `ducklake_snapshot_changes WHERE snapshot_id > txn_start`
2. Parse our changes and other changes via `ducklake_parse_changes`
3. Check all conflict types:
   - Dropped tables/views/macros/schemas vs same drops
   - Created schemas vs same created schemas
   - Created tables/views in dropped schemas
   - Created tables/views with same name
   - Inserts into dropped/altered tables
   - Deletes from dropped/altered/compacted tables
   - Inlined operations vs drops/alters/other inlined ops
   - Compaction vs drops/deletes/other compactions
   - Alters vs drops/other alters
4. RAISE EXCEPTION on any conflict with descriptive message

### `ducklake_replace_placeholders(p_sql TEXT, p_snapshot_id BIGINT, p_schema_version BIGINT, p_catalog_base BIGINT, p_file_base BIGINT, p_next_catalog_id BIGINT, p_next_file_id BIGINT)` → TEXT

Simple string replacement:
```sql
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p_sql,
  '{SNAPSHOT_ID}', p_snapshot_id::TEXT),
  '{SCHEMA_VERSION}', p_schema_version::TEXT),
  '{CATALOG_ID_BASE}', p_catalog_base::TEXT),
  '{FILE_ID_BASE}', p_file_base::TEXT),
  '{NEXT_CATALOG_ID}', p_next_catalog_id::TEXT),
  '{NEXT_FILE_ID}', p_next_file_id::TEXT)
```

### `ducklake_try_commit(p_schema TEXT, p_batch_sql TEXT, p_schema_changed BOOLEAN, p_txn_start_snapshot BIGINT, p_our_changes TEXT, p_delta_catalog BIGINT, p_delta_file BIGINT)` → TABLE(new_snapshot_id BIGINT, new_schema_version BIGINT)

1. Get latest snapshot: `SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) ...)`
2. If `latest_snapshot_id > p_txn_start_snapshot`: call `ducklake_check_conflicts`
3. Compute: `new_snapshot_id = latest + 1`, `new_schema_version = latest_sv + (1 if schema_changed)`, `catalog_base = latest_nci`, `file_base = latest_nfi`, `next_catalog_id = catalog_base + delta_catalog`, `next_file_id = file_base + delta_file`
4. Replace placeholders in batch SQL
5. `EXECUTE` the replaced SQL
6. Return (new_snapshot_id, new_schema_version)

### `ducklake_commit_with_retry(p_schema TEXT, p_batch_sql TEXT, p_schema_changed BOOLEAN, p_txn_start_snapshot BIGINT, p_our_changes TEXT, p_delta_catalog BIGINT, p_delta_file BIGINT, p_max_retry INT, p_retry_wait_ms INT, p_retry_backoff FLOAT, INOUT p_result_snapshot_id BIGINT DEFAULT NULL, INOUT p_result_schema_version BIGINT DEFAULT NULL)`

```sql
LOOP v_attempt from 1 to p_max_retry:
    BEGIN
        SELECT * INTO v_result FROM ducklake_try_commit(...);
        p_result_snapshot_id := v_result.new_snapshot_id;
        p_result_schema_version := v_result.new_schema_version;
        COMMIT;
        RETURN;
    EXCEPTION WHEN OTHERS THEN
        ROLLBACK;
        -- Check if error is retryable (PK violation, unique, conflict, concurrent)
        IF NOT is_retryable(SQLERRM) THEN
            RAISE EXCEPTION 'Failed to commit DuckLake transaction and was not retryable after % attempts: %', v_attempt, SQLERRM;
        END IF;
        IF v_attempt >= p_max_retry THEN
            RAISE EXCEPTION 'Failed to commit DuckLake transaction after % attempts: %', v_attempt, SQLERRM;
        END IF;
        -- Exponential backoff with jitter
        PERFORM pg_sleep((p_retry_wait_ms / 1000.0) * power(p_retry_backoff, v_attempt - 1) * (0.5 + random() * 0.5));
    END;
END LOOP;
```

## Inline Operation Detection

Check before choosing stored procedure path:
```cpp
bool has_inline_ops = false;
for (auto &entry : table_data_changes) {
    auto &changes = entry.second;
    if (changes.new_inlined_data ||
        !changes.new_inlined_data_deletes.empty() ||
        changes.new_inlined_file_deletes) {
        has_inline_ops = true;
        break;
    }
}
```


When using stored procedure path, `WriteNewInlinedTables` output is still included. Make sure that new InlinedTables are created. Only inline inserts and deletes are prohibited.

## Files to Create/Modify

| File | Change |
|------|--------|
| `src/include/storage/ducklake_metadata_manager.hpp` | Add `FormatCatalogId`/`FormatFileId` virtual methods |
| `src/storage/ducklake_metadata_manager.cpp` | Default implementations + update ~10 SQL generation functions |
| `src/include/metadata_manager/postgres_metadata_manager.hpp` | Add stored proc methods + state |
| `src/metadata_manager/postgres_metadata_manager.cpp` | Implement stored proc methods, ID formatting, procedure SQL |
| `src/storage/ducklake_transaction.cpp` | Add `FlushChangesStoredProc()`, routing logic in `FlushChanges()` |
| `src/include/storage/ducklake_transaction.hpp` | Declare `FlushChangesStoredProc()` |

## Verification

1. **Build**: `ENABLE_POSTGRES_SCANNER=1 GEN=ninja make release`
2. **Run Postgres tests**: `build/release/test/unittest --test-config test/configs/postgres.json`
3. Key test scenarios:
   - Simple INSERT (stored proc path)
   - CREATE TABLE + INSERT (stored proc path with schema changes)
   - INSERT with inlined data (should fall back to C++ path)
   - Concurrent commits (stored proc retry logic)
   - ALTER TABLE, DROP TABLE (stored proc path)
