## Failing Test Cases - Detailed Analysis

### PRE-EXISTING (not caused by this PR)

**1. `test/sql/encryption/encryption.test` (line 17)**
- **Error**: `Invalid Configuration Error: DuckDB requires a secure random engine to be loaded`
- **Root cause**: Missing `httpfs` extension autoloading for crypto support on this machine
- **Pre-existing?**: YES - fails identically with stored proc disabled

**2. `test/sql/encryption/partitioning_encryption.test` (line 28)**
- **Error**: Same httpfs/crypto error
- **Pre-existing?**: YES - same root cause as above

**3. `test/sql/alter/alter_timestamptz_promotion.test` (line 29)**
- **Error**: Expected `2025-01-15 12:30:45+00` got `2025-01-15 12:30:45-07`
- **Root cause**: Postgres-specific timezone handling - Postgres stores/returns TIMESTAMPTZ in the server's timezone rather than UTC. This is a Postgres backend incompatibility unrelated to the commit path.
- **Pre-existing?**: YES - fails identically with stored proc disabled

---

### REGRESSIONS (introduced by stored procedure commit path)

#### Category A: Concurrent transaction failures (all share the same root cause)

**4. `test/sql/transaction/transaction_conflicts_view.test` (line 36)**
**5. `test/sql/transaction/transaction_conflict_cleanup.test` (line 45)**
**6. `test/sql/transaction/transaction_conflicts_delete.test` (line 43)**
**7. `test/sql/transaction/concurrent_table_creation.test` (line 36)**
**8. `test/sql/transaction/transaction_conflicts.test` (line 36)**
**9. `test/sql/stats/global_stats_transactions.test` (line 45)**
**10. `test/sql/snapshot_info/ducklake_current_commit.test` (line 85)**

- **Error**: `TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction via stored procedure: ... DuckLake: exceeded max retry count (10) for commit`
- **Root cause**: When two DuckDB connections commit concurrently to Postgres, each opens its own Postgres transaction. The stored procedure's retry loop runs inside a single Postgres transaction, but it can't see the other connection's committed snapshot (Postgres transaction isolation). The retry loop gets `unique_violation` on `snapshot_id`, rolls back the savepoint, re-reads the snapshot, but under `READ COMMITTED` isolation the snapshot read still sees the same data within the same statement context, causing infinite retries.
- **Pre-existing?**: NO - all pass with stored proc disabled (using the old multi-round-trip C++ retry path which opens fresh Postgres transactions on each retry)

#### Category B: Single-connection data correctness failures

**11. `test/sql/partitioning/partition_nop.test` (line 40)**
- **Error**: Expected 1 partition entry in `ducklake_partition_info`, got 2
- **What it tests**: `SET PARTITIONED BY (part_key)` twice on the same table with the same partition should be a no-op the second time
- **Root cause**: The nop detection in `GetNewPartitions()` compares old partitions (from committed catalog) with new partitions. Some aspect of the partition data differs between what's stored in Postgres vs what's in the in-memory representation after going through the stored proc commit path, causing the equality check to fail and treating identical partitions as different.
- **Pre-existing?**: NO - passes with stored proc disabled

**12. `test/sql/partitioning/drop_partition_column.test` (line 45)**
- **Error**: `Cannot drop column "part_key" - the table is partitioned by this column` even after `RESET PARTITIONED BY`
- **What it tests**: After resetting the partition, the column should be droppable
- **Root cause**: Related to #11 - the `RESET PARTITIONED BY` creates a new partition entry (end_snapshot set) but the nop/partition tracking is broken, so the catalog still thinks the table is partitioned.
- **Pre-existing?**: NO - passes with stored proc disabled

**13. `test/sql/stats/variant_shredded_stats.test` (line 28)**
- **Error**: Expected `shredding_state: SHREDDED`, got `shredding_state: INCONSISTENT`
- **What it tests**: VARIANT type column should show SHREDDED stats after creating a fully-shredded primitive table
- **Root cause**: The variant file stats (in `ducklake_file_variant_stats`) are likely being written with incorrect file IDs or not being found during stats reading. Since variant stats reference `data_file_id`, the offset-based file IDs may not be resolving correctly in this path.
- **Pre-existing?**: NO - passes with stored proc disabled

**14. `test/sql/stats/count_star_optimization_inlined.test` (line 119)**
- **Error**: Expected 80, got 83 after `DELETE FROM ducklake.inlined_test WHERE i % 10 = 1`
- **What it tests**: COUNT(*) optimization across multiple schema versions with inlined data after ALTER TABLE + DELETE
- **Root cause**: 3 extra rows being counted. The test has 3 inlined tables (from ALTER TABLE schema changes) each with ~30 rows. The DELETE should remove ~9 rows (9 values where i%10=1 in 0-89), but the count says only 7 were removed. This suggests some inlined table records are being double-counted or the delete isn't hitting all versions correctly.
- **Pre-existing?**: NO - passes with stored proc disabled

**15. `test/sql/delete/test_delete_partial_max_snapshot.test` (line 35)**
- **Error**: Expected 2 rows in `ducklake_delete_file` (with `partial_max` values 4 and NULL), got 1 row (NULL only)
- **What it tests**: Multiple deletes on different data files should create separate delete file entries, with the first one having a `partial_max` value
- **Root cause**: The `partial_max` field in `WriteNewDeleteFiles` uses `to_string(file.max_snapshot.GetIndex())` (line 2963) instead of going through the stored proc's snapshot resolution. If the `max_snapshot` value uses an offset-based reference, it could be stored incorrectly. Additionally, the first delete file entry may be missing entirely if the delete file consolidation logic is affected.
- **Pre-existing?**: NO - passes with stored proc disabled

---

### Summary

| Category | Count | Root Cause |
|----------|-------|------------|
| Pre-existing (httpfs/timezone) | 3 | Unrelated to this PR |
| Concurrent transaction retry | 7 | Stored proc retry can't see other connections' commits |
| Partition nop/reset | 2 | Partition equality check breaks after stored proc commit |
| Variant stats | 1 | File ID resolution issue in variant stats path |
| Inlined data count | 1 | Count mismatch after DELETE across schema versions |
| Delete file partial_max | 1 | Delete file entry or partial_max not stored correctly |

**All non-encryption/timezone failures are regressions introduced by the stored procedure commit path** - they all pass when the stored proc is disabled and the old multi-round-trip C++ commit path is used.
