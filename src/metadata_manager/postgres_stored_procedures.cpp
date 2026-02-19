#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

static string GetStoredProcedureSQL(const string &schema) {
	// PL/pgSQL stored procedure for atomic commit with retry
	// {SCHEMA} will be replaced with the actual schema identifier
	return StringUtil::Format(R"(

CREATE OR REPLACE FUNCTION %s.ducklake_commit(
    p_batch_sql TEXT,
    p_snapshot_insert_sql TEXT,
    p_changes_sql TEXT,
    p_our_changes TEXT,
    p_transaction_snapshot_id BIGINT,
    p_schema_changes_made BOOLEAN,
    p_file_id_count BIGINT,
    p_catalog_id_count BIGINT,
    p_max_retry_count INT DEFAULT 10,
    p_retry_wait_ms FLOAT DEFAULT 100,
    p_retry_backoff FLOAT DEFAULT 1.5
) RETURNS TABLE (snapshot_id BIGINT, schema_version BIGINT,
                 next_catalog_id BIGINT, next_file_id BIGINT) AS $fn$
DECLARE
    v_snap RECORD;
    v_new_sid BIGINT;
    v_new_sv BIGINT;
    v_sql TEXT;
    v_retry INT := 0;
    v_other_changes TEXT;
BEGIN
    LOOP
        -- Each iteration uses an implicit savepoint via BEGIN...EXCEPTION
        BEGIN
            -- 1. Get latest snapshot
            SELECT s.snapshot_id, s.schema_version, s.next_catalog_id, s.next_file_id
            INTO v_snap FROM %s.ducklake_snapshot s
            ORDER BY s.snapshot_id DESC LIMIT 1;

            -- 2. Compute new snapshot values
            v_new_sid := v_snap.snapshot_id + 1;
            v_new_sv := v_snap.schema_version;
            IF p_schema_changes_made THEN
                v_new_sv := v_new_sv + 1;
            END IF;

            -- 3. Check for semantic conflicts if snapshot changed since our tx started
            IF v_snap.snapshot_id > p_transaction_snapshot_id THEN
                SELECT STRING_AGG(sc.changes_made, ',') INTO v_other_changes
                FROM %s.ducklake_snapshot_changes sc
                WHERE sc.snapshot_id > p_transaction_snapshot_id;

                IF v_other_changes IS NOT NULL THEN
                    PERFORM %s.ducklake_check_conflicts(
                        p_our_changes, v_other_changes, p_transaction_snapshot_id);
                END IF;
            END IF;

            -- 4. Resolve placeholders in batch SQL using current snapshot values
            v_sql := REPLACE(p_batch_sql, '{FILE_ID_BASE}', v_snap.next_file_id::TEXT);
            v_sql := REPLACE(v_sql, '{CATALOG_ID_BASE}', v_snap.next_catalog_id::TEXT);
            v_sql := REPLACE(v_sql, '{SNAPSHOT_ID}', v_new_sid::TEXT);
            v_sql := REPLACE(v_sql, '{SCHEMA_VERSION}', v_new_sv::TEXT);

            -- 5. Execute the resolved batch SQL
            IF v_sql IS NOT NULL AND v_sql != '' THEN
                EXECUTE v_sql;
            END IF;

            -- 6. Insert new snapshot with updated counters
            INSERT INTO %s.ducklake_snapshot VALUES (
                v_new_sid, NOW(), v_new_sv,
                v_snap.next_catalog_id + p_catalog_id_count,
                v_snap.next_file_id + p_file_id_count
            );

            -- 7. Insert snapshot changes (resolve placeholders first)
            IF p_changes_sql IS NOT NULL AND p_changes_sql != '' THEN
                v_sql := REPLACE(p_changes_sql, '{SNAPSHOT_ID}', v_new_sid::TEXT);
                v_sql := REPLACE(v_sql, '{SCHEMA_VERSION}', v_new_sv::TEXT);
                EXECUTE v_sql;
            END IF;

            -- 8. Success - return committed snapshot info
            RETURN QUERY SELECT v_new_sid, v_new_sv,
                v_snap.next_catalog_id + p_catalog_id_count,
                v_snap.next_file_id + p_file_id_count;
            RETURN;

        EXCEPTION WHEN unique_violation THEN
            -- Concurrent commit took our snapshot_id or file_id
            -- Savepoint is automatically rolled back
            v_retry := v_retry + 1;
            IF v_retry > p_max_retry_count THEN
                RAISE EXCEPTION 'DuckLake: exceeded max retry count (%%) for commit', p_max_retry_count;
            END IF;
            -- Exponential backoff with jitter (pg_sleep arg is in seconds)
            PERFORM pg_sleep(
                p_retry_wait_ms * power(p_retry_backoff, v_retry - 1)
                * (0.5 + 0.5 * random()) / 1000.0
            );
        END;
    END LOOP;
END;
$fn$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION %s.ducklake_check_conflicts(
    p_our_changes TEXT,
    p_other_changes TEXT,
    p_transaction_snapshot_id BIGINT
) RETURNS VOID AS $fn$
DECLARE
    v_our_entry TEXT;
    v_other_entry TEXT;
    v_our_type TEXT;
    v_our_value TEXT;
    v_other_type TEXT;
    v_other_value TEXT;
    -- Sets for our changes
    v_our_dropped_tables BIGINT[];
    v_our_dropped_views BIGINT[];
    v_our_dropped_schemas BIGINT[];
    v_our_dropped_scalar_macros BIGINT[];
    v_our_dropped_table_macros BIGINT[];
    v_our_created_schemas TEXT[];
    v_our_created_tables TEXT[];
    v_our_created_scalar_macros TEXT[];
    v_our_inserted_tables BIGINT[];
    v_our_deleted_tables BIGINT[];
    v_our_altered_tables BIGINT[];
    v_our_altered_views BIGINT[];
    v_our_inlined_insert BIGINT[];
    v_our_inlined_delete BIGINT[];
    v_our_inline_flush BIGINT[];
    v_our_merge_adjacent BIGINT[];
    v_our_rewrite_delete BIGINT[];
    -- Sets for other changes
    v_other_dropped_tables BIGINT[];
    v_other_dropped_views BIGINT[];
    v_other_dropped_schemas BIGINT[];
    v_other_dropped_scalar_macros BIGINT[];
    v_other_dropped_table_macros BIGINT[];
    v_other_created_schemas TEXT[];
    v_other_created_tables TEXT[];
    v_other_created_scalar_macros TEXT[];
    v_other_inserted_tables BIGINT[];
    v_other_deleted_tables BIGINT[];
    v_other_altered_tables BIGINT[];
    v_other_altered_views BIGINT[];
    v_other_inlined_insert BIGINT[];
    v_other_inlined_delete BIGINT[];
    v_other_inline_flush BIGINT[];
    v_other_merge_adjacent BIGINT[];
    v_other_rewrite_delete BIGINT[];
    v_val BIGINT;
    v_txt TEXT;
BEGIN
    -- Initialize arrays
    v_our_dropped_tables := ARRAY[]::BIGINT[];
    v_our_dropped_views := ARRAY[]::BIGINT[];
    v_our_dropped_schemas := ARRAY[]::BIGINT[];
    v_our_dropped_scalar_macros := ARRAY[]::BIGINT[];
    v_our_dropped_table_macros := ARRAY[]::BIGINT[];
    v_our_created_schemas := ARRAY[]::TEXT[];
    v_our_created_tables := ARRAY[]::TEXT[];
    v_our_created_scalar_macros := ARRAY[]::TEXT[];
    v_our_inserted_tables := ARRAY[]::BIGINT[];
    v_our_deleted_tables := ARRAY[]::BIGINT[];
    v_our_altered_tables := ARRAY[]::BIGINT[];
    v_our_altered_views := ARRAY[]::BIGINT[];
    v_our_inlined_insert := ARRAY[]::BIGINT[];
    v_our_inlined_delete := ARRAY[]::BIGINT[];
    v_our_inline_flush := ARRAY[]::BIGINT[];
    v_our_merge_adjacent := ARRAY[]::BIGINT[];
    v_our_rewrite_delete := ARRAY[]::BIGINT[];
    v_other_dropped_tables := ARRAY[]::BIGINT[];
    v_other_dropped_views := ARRAY[]::BIGINT[];
    v_other_dropped_schemas := ARRAY[]::BIGINT[];
    v_other_dropped_scalar_macros := ARRAY[]::BIGINT[];
    v_other_dropped_table_macros := ARRAY[]::BIGINT[];
    v_other_created_schemas := ARRAY[]::TEXT[];
    v_other_created_tables := ARRAY[]::TEXT[];
    v_other_created_scalar_macros := ARRAY[]::TEXT[];
    v_other_inserted_tables := ARRAY[]::BIGINT[];
    v_other_deleted_tables := ARRAY[]::BIGINT[];
    v_other_altered_tables := ARRAY[]::BIGINT[];
    v_other_altered_views := ARRAY[]::BIGINT[];
    v_other_inlined_insert := ARRAY[]::BIGINT[];
    v_other_inlined_delete := ARRAY[]::BIGINT[];
    v_other_inline_flush := ARRAY[]::BIGINT[];
    v_other_merge_adjacent := ARRAY[]::BIGINT[];
    v_other_rewrite_delete := ARRAY[]::BIGINT[];

    -- Parse our changes
    IF p_our_changes IS NOT NULL AND p_our_changes != '' THEN
        FOREACH v_our_entry IN ARRAY string_to_array(p_our_changes, ',') LOOP
            v_our_type := split_part(v_our_entry, ':', 1);
            v_our_value := split_part(v_our_entry, ':', 2);
            CASE v_our_type
                WHEN 'dropped_table' THEN v_our_dropped_tables := v_our_dropped_tables || v_our_value::BIGINT;
                WHEN 'dropped_view' THEN v_our_dropped_views := v_our_dropped_views || v_our_value::BIGINT;
                WHEN 'dropped_schema' THEN v_our_dropped_schemas := v_our_dropped_schemas || v_our_value::BIGINT;
                WHEN 'dropped_scalar_macro' THEN v_our_dropped_scalar_macros := v_our_dropped_scalar_macros || v_our_value::BIGINT;
                WHEN 'dropped_table_macro' THEN v_our_dropped_table_macros := v_our_dropped_table_macros || v_our_value::BIGINT;
                WHEN 'created_schema' THEN v_our_created_schemas := v_our_created_schemas || v_our_value;
                WHEN 'created_table' THEN v_our_created_tables := v_our_created_tables || v_our_value;
                WHEN 'created_view' THEN v_our_created_tables := v_our_created_tables || v_our_value;
                WHEN 'created_scalar_macro' THEN v_our_created_scalar_macros := v_our_created_scalar_macros || v_our_value;
                WHEN 'inserted_into_table' THEN v_our_inserted_tables := v_our_inserted_tables || v_our_value::BIGINT;
                WHEN 'deleted_from_table' THEN v_our_deleted_tables := v_our_deleted_tables || v_our_value::BIGINT;
                WHEN 'altered_table' THEN v_our_altered_tables := v_our_altered_tables || v_our_value::BIGINT;
                WHEN 'altered_view' THEN v_our_altered_views := v_our_altered_views || v_our_value::BIGINT;
                WHEN 'inlined_insert' THEN v_our_inlined_insert := v_our_inlined_insert || v_our_value::BIGINT;
                WHEN 'inlined_delete' THEN v_our_inlined_delete := v_our_inlined_delete || v_our_value::BIGINT;
                WHEN 'inline_flush' THEN v_our_inline_flush := v_our_inline_flush || v_our_value::BIGINT;
                WHEN 'merge_adjacent' THEN v_our_merge_adjacent := v_our_merge_adjacent || v_our_value::BIGINT;
                WHEN 'rewrite_delete' THEN v_our_rewrite_delete := v_our_rewrite_delete || v_our_value::BIGINT;
                ELSE NULL; -- ignore unknown change types
            END CASE;
        END LOOP;
    END IF;

    -- Parse other changes
    IF p_other_changes IS NOT NULL AND p_other_changes != '' THEN
        FOREACH v_other_entry IN ARRAY string_to_array(p_other_changes, ',') LOOP
            v_other_type := split_part(v_other_entry, ':', 1);
            v_other_value := split_part(v_other_entry, ':', 2);
            CASE v_other_type
                WHEN 'dropped_table' THEN v_other_dropped_tables := v_other_dropped_tables || v_other_value::BIGINT;
                WHEN 'dropped_view' THEN v_other_dropped_views := v_other_dropped_views || v_other_value::BIGINT;
                WHEN 'dropped_schema' THEN v_other_dropped_schemas := v_other_dropped_schemas || v_other_value::BIGINT;
                WHEN 'dropped_scalar_macro' THEN v_other_dropped_scalar_macros := v_other_dropped_scalar_macros || v_other_value::BIGINT;
                WHEN 'dropped_table_macro' THEN v_other_dropped_table_macros := v_other_dropped_table_macros || v_other_value::BIGINT;
                WHEN 'created_schema' THEN v_other_created_schemas := v_other_created_schemas || v_other_value;
                WHEN 'created_table' THEN v_other_created_tables := v_other_created_tables || v_other_value;
                WHEN 'created_view' THEN v_other_created_tables := v_other_created_tables || v_other_value;
                WHEN 'created_scalar_macro' THEN v_other_created_scalar_macros := v_other_created_scalar_macros || v_other_value;
                WHEN 'inserted_into_table' THEN v_other_inserted_tables := v_other_inserted_tables || v_other_value::BIGINT;
                WHEN 'deleted_from_table' THEN v_other_deleted_tables := v_other_deleted_tables || v_other_value::BIGINT;
                WHEN 'altered_table' THEN v_other_altered_tables := v_other_altered_tables || v_other_value::BIGINT;
                WHEN 'altered_view' THEN v_other_altered_views := v_other_altered_views || v_other_value::BIGINT;
                WHEN 'inlined_insert' THEN v_other_inlined_insert := v_other_inlined_insert || v_other_value::BIGINT;
                WHEN 'inlined_delete' THEN v_other_inlined_delete := v_other_inlined_delete || v_other_value::BIGINT;
                WHEN 'inline_flush' THEN v_other_inline_flush := v_other_inline_flush || v_other_value::BIGINT;
                WHEN 'merge_adjacent' THEN v_other_merge_adjacent := v_other_merge_adjacent || v_other_value::BIGINT;
                WHEN 'rewrite_delete' THEN v_other_rewrite_delete := v_other_rewrite_delete || v_other_value::BIGINT;
                ELSE NULL; -- ignore unknown change types
            END CASE;
        END LOOP;
    END IF;

    -- Check conflicts: dropping same table
    FOREACH v_val IN ARRAY v_our_dropped_tables LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to drop table with index "%%" - but another transaction has dropped it already', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: dropping same view
    FOREACH v_val IN ARRAY v_our_dropped_views LOOP
        IF v_val = ANY(v_other_dropped_views) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to drop view with index "%%" - but another transaction has dropped it already', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: dropping same macro
    FOREACH v_val IN ARRAY v_our_dropped_scalar_macros LOOP
        IF v_val = ANY(v_other_dropped_scalar_macros) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to drop macro with index "%%" - but another transaction has dropped it already', v_val;
        END IF;
    END LOOP;
    FOREACH v_val IN ARRAY v_our_dropped_table_macros LOOP
        IF v_val = ANY(v_other_dropped_table_macros) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to drop macro with index "%%" - but another transaction has dropped it already', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: dropping same schema
    FOREACH v_val IN ARRAY v_our_dropped_schemas LOOP
        IF v_val = ANY(v_other_dropped_schemas) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to drop schema with index "%%" - but another transaction has dropped it already', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: creating same schema
    FOREACH v_txt IN ARRAY v_our_created_schemas LOOP
        IF v_txt = ANY(v_other_created_schemas) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to create schema "%%" - but another transaction has created a schema with this name already', v_txt;
        END IF;
    END LOOP;

    -- Check conflicts: creating same table/view
    FOREACH v_txt IN ARRAY v_our_created_tables LOOP
        IF v_txt = ANY(v_other_created_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to create entry "%%" - but it has been created by another transaction already', v_txt;
        END IF;
    END LOOP;

    -- Check conflicts: creating same scalar macro
    FOREACH v_txt IN ARRAY v_our_created_scalar_macros LOOP
        IF v_txt = ANY(v_other_created_scalar_macros) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to create macro "%%" - but it has been created by another transaction already', v_txt;
        END IF;
    END LOOP;

    -- Check conflicts: insert into dropped/altered table
    FOREACH v_val IN ARRAY v_our_inserted_tables LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_altered_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%%" - but another transaction has altered it', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: inlined insert into dropped/altered table
    FOREACH v_val IN ARRAY v_our_inlined_insert LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_altered_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%%" - but another transaction has altered it', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: delete from dropped/altered/compacted table
    FOREACH v_val IN ARRAY v_our_deleted_tables LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_altered_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has altered it', v_val;
        END IF;
        IF v_val = ANY(v_other_merge_adjacent) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
        IF v_val = ANY(v_other_rewrite_delete) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: inlined delete from dropped/altered/deleted table
    FOREACH v_val IN ARRAY v_our_inlined_delete LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_altered_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has altered it', v_val;
        END IF;
        IF v_val = ANY(v_other_inlined_delete) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has deleted from it', v_val;
        END IF;
        IF v_val = ANY(v_other_inline_flush) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%%" - but another transaction has flushed the inlined data', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: inline flush vs dropped/deleted/flushed
    FOREACH v_val IN ARRAY v_our_inline_flush LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data for table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_inlined_delete) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data for table with index "%%" - but another transaction has deleted from it', v_val;
        END IF;
        IF v_val = ANY(v_other_inline_flush) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data for table with index "%%" - but another transaction has flushed it', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: compaction vs dropped/deleted/compacted
    FOREACH v_val IN ARRAY v_our_merge_adjacent LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_deleted_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has deleted from it', v_val;
        END IF;
        IF v_val = ANY(v_other_merge_adjacent) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
        IF v_val = ANY(v_other_rewrite_delete) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
    END LOOP;

    FOREACH v_val IN ARRAY v_our_rewrite_delete LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_deleted_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has deleted from it', v_val;
        END IF;
        IF v_val = ANY(v_other_merge_adjacent) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
        IF v_val = ANY(v_other_rewrite_delete) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%%" - but another transaction has compacted it', v_val;
        END IF;
    END LOOP;

    -- Check conflicts: alter vs dropped/altered
    FOREACH v_val IN ARRAY v_our_altered_tables LOOP
        IF v_val = ANY(v_other_dropped_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to alter table with index "%%" - but another transaction has dropped it', v_val;
        END IF;
        IF v_val = ANY(v_other_altered_tables) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to alter table with index "%%" - but another transaction has altered it', v_val;
        END IF;
    END LOOP;

    FOREACH v_val IN ARRAY v_our_altered_views LOOP
        IF v_val = ANY(v_other_altered_views) THEN
            RAISE EXCEPTION 'Transaction conflict - attempting to alter view with index "%%" - but another transaction has altered it', v_val;
        END IF;
    END LOOP;

END;
$fn$ LANGUAGE plpgsql;

)", schema, schema, schema, schema, schema, schema);
}

void PostgresMetadataManager::InstallStoredProcedures() {
	auto schema_escaped = GetSchemaIdentifierEscaped();
	auto proc_sql = GetStoredProcedureSQL(schema_escaped);

	// Execute via postgres_execute
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &connection = transaction.GetConnection();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());

	auto result = connection.Query(StringUtil::Format("CALL postgres_execute(%s, %s)", catalog_literal,
	                                                  SQLString(proc_sql)));
	if (result->HasError()) {
		// Stored procedures are optional - if they fail to install, we fall back to the multi-round-trip path
		// This can happen if the user doesn't have CREATE FUNCTION permissions
		return;
	}
}

} // namespace duckdb
