#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
		return false;
	default:
		return true;
	}
}

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	return connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
}
unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_query");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

string PostgresMetadataManager::FormatCatalogId(idx_t id) {
	if (use_stored_procedure && id >= catalog_id_base) {
		return StringUtil::Format("{CATALOG_ID:%d}", id - catalog_id_base);
	}
	return to_string(id);
}

string PostgresMetadataManager::FormatFileId(idx_t id) {
	if (use_stored_procedure && id >= file_id_base) {
		return StringUtil::Format("{FILE_ID:%d}", id - file_id_base);
	}
	return to_string(id);
}

string PostgresMetadataManager::GetInlinedTableName(const DuckLakeTableInfo &table, const DuckLakeSnapshot &snapshot) {
	if (use_stored_procedure) {
		return StringUtil::Format("ducklake_inlined_data_%s_{SCHEMA_VERSION}", FormatCatalogId(table.id.index));
	}
	return StringUtil::Format("ducklake_inlined_data_%s_%d", FormatCatalogId(table.id.index), snapshot.schema_version);
}

string PostgresMetadataManager::FormatSchemaVersion(const DuckLakeSnapshot &snapshot) {
	if (use_stored_procedure) {
		return "{SCHEMA_VERSION}";
	}
	return to_string(snapshot.schema_version);
}

void PostgresMetadataManager::SetStoredProcedureMode(idx_t catalog_base, idx_t file_base) {
	use_stored_procedure = true;
	catalog_id_base = catalog_base;
	file_id_base = file_base;
}

bool PostgresMetadataManager::TryCreateStoredProcedures() {
	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());

	// Pre-check: is PL/pgSQL available? This avoids aborting the Postgres transaction.
	auto check = connection.Query(StringUtil::Format(
	    "SELECT * FROM postgres_query(%s, 'SELECT 1 FROM pg_language WHERE lanname = ''plpgsql''')",
	    catalog_literal));
	if (check->HasError()) {
		return false;
	}
	auto chunk = check->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}

	// PL/pgSQL available — try to create (with safety catch)
	try {
		CreateStoredProcedures();
		return true;
	} catch (std::exception &) {
		return false;
	}
}

void PostgresMetadataManager::ResolveStoredProcedures() {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto sp_setting = ducklake_catalog.StoredProceduresOption();

	if (sp_setting == StoredProceduresSetting::DISABLED) {
		ducklake_catalog.SetStoredProceduresAvailable(false);
	} else if (sp_setting == StoredProceduresSetting::ENABLED) {
		// Explicitly enabled — throw on failure
		CreateStoredProcedures();
		ducklake_catalog.SetStoredProceduresAvailable(true);
	} else {
		// Auto — probe and try
		bool ok = TryCreateStoredProcedures();
		ducklake_catalog.SetStoredProceduresAvailable(ok);
	}

	// Persist the resolved value in ducklake_metadata
	DuckLakeSnapshot empty_snapshot;
	string insert = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value) VALUES ('use_stored_procedures', '%s')",
	    ducklake_catalog.UseStoredProcedures() ? "true" : "false");
	Execute(empty_snapshot, insert);
}

void PostgresMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	DuckLakeMetadataManager::InitializeDuckLake(has_explicit_schema, encryption);
	ResolveStoredProcedures();
}

void PostgresMetadataManager::MigrateV03(bool allow_failures) {
	DuckLakeMetadataManager::MigrateV03(allow_failures);
	ResolveStoredProcedures();
}

void PostgresMetadataManager::CreateStoredProcedures() {
	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());

	// Use {PROC_SCHEMA} as placeholder, then replace with actual schema
	string proc_sql = R"(
-- Helper: check if an error is retryable
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_is_retryable(p_message TEXT)
RETURNS BOOLEAN
LANGUAGE plpgsql AS $fn$
BEGIN
    RETURN (
        p_message ILIKE '%primary key%' OR
        p_message ILIKE '%unique%' OR
        p_message ILIKE '%conflict%' OR
        p_message ILIKE '%concurrent%'
    );
END;
$fn$;

-- Helper: parse comma-separated changes string into rows
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_parse_changes(p_changes TEXT)
RETURNS TABLE(change_type TEXT, change_value TEXT)
LANGUAGE plpgsql AS $fn$
DECLARE
    v_pos INT := 1;
    v_len INT;
    v_colon INT;
    v_end INT;
    v_type TEXT;
    v_value TEXT;
    v_char CHAR;
BEGIN
    IF p_changes IS NULL OR p_changes = '' THEN
        RETURN;
    END IF;
    v_len := length(p_changes);
    WHILE v_pos <= v_len LOOP
        -- find the colon separating type:value
        v_colon := position(':' IN substring(p_changes FROM v_pos));
        IF v_colon = 0 THEN
            EXIT;
        END IF;
        v_type := substring(p_changes FROM v_pos FOR v_colon - 1);
        v_pos := v_pos + v_colon;  -- move past the colon
        -- parse the value (may be quoted with double quotes)
        v_char := substring(p_changes FROM v_pos FOR 1);
        IF v_char = '"' THEN
            -- quoted value - find matching end quote, handling escaped quotes
            v_pos := v_pos + 1;  -- skip opening quote
            v_value := '';
            WHILE v_pos <= v_len LOOP
                v_char := substring(p_changes FROM v_pos FOR 1);
                IF v_char = '"' THEN
                    -- check for escaped quote (double quote)
                    IF v_pos + 1 <= v_len AND substring(p_changes FROM v_pos + 1 FOR 1) = '"' THEN
                        v_value := v_value || '"';
                        v_pos := v_pos + 2;
                    ELSE
                        v_pos := v_pos + 1;  -- skip closing quote
                        EXIT;
                    END IF;
                ELSE
                    v_value := v_value || v_char;
                    v_pos := v_pos + 1;
                END IF;
            END LOOP;
            -- Handle dotted quoted values like "schema"."table"
            WHILE v_pos <= v_len AND substring(p_changes FROM v_pos FOR 1) = '.' LOOP
                v_value := v_value || '.';
                v_pos := v_pos + 1;
                IF v_pos <= v_len AND substring(p_changes FROM v_pos FOR 1) = '"' THEN
                    v_pos := v_pos + 1;  -- skip opening quote
                    WHILE v_pos <= v_len LOOP
                        v_char := substring(p_changes FROM v_pos FOR 1);
                        IF v_char = '"' THEN
                            IF v_pos + 1 <= v_len AND substring(p_changes FROM v_pos + 1 FOR 1) = '"' THEN
                                v_value := v_value || '"';
                                v_pos := v_pos + 2;
                            ELSE
                                v_pos := v_pos + 1;
                                EXIT;
                            END IF;
                        ELSE
                            v_value := v_value || v_char;
                            v_pos := v_pos + 1;
                        END IF;
                    END LOOP;
                END IF;
            END LOOP;
        ELSE
            -- unquoted value - read until comma or end
            v_end := position(',' IN substring(p_changes FROM v_pos));
            IF v_end = 0 THEN
                v_value := substring(p_changes FROM v_pos);
                v_pos := v_len + 1;
            ELSE
                v_value := substring(p_changes FROM v_pos FOR v_end - 1);
                v_pos := v_pos + v_end - 1;
            END IF;
        END IF;
        change_type := v_type;
        change_value := v_value;
        RETURN NEXT;
        -- skip comma
        IF v_pos <= v_len AND substring(p_changes FROM v_pos FOR 1) = ',' THEN
            v_pos := v_pos + 1;
        END IF;
    END LOOP;
END;
$fn$;

-- Conflict detection function
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_check_conflicts(
    p_schema TEXT,
    p_txn_start_snapshot BIGINT,
    p_our_changes TEXT
)
RETURNS VOID
LANGUAGE plpgsql AS $fn$
DECLARE
    v_other_changes TEXT;
    v_our RECORD;
    v_other RECORD;
    v_our_type TEXT;
    v_our_value TEXT;
    v_other_type TEXT;
    v_other_value TEXT;
    v_resolved_name TEXT;
    v_resolved_prefix TEXT;
BEGIN
    -- Gather all changes from snapshots after our start
    EXECUTE format(
        'SELECT COALESCE(STRING_AGG(changes_made, '',''), '''') FROM %I.ducklake_snapshot_changes WHERE snapshot_id > $1',
        p_schema
    ) INTO v_other_changes USING p_txn_start_snapshot;

    IF v_other_changes = '' THEN
        RETURN;
    END IF;

    -- Check each of our changes against other changes
    FOR v_our IN SELECT * FROM {PROC_SCHEMA}.ducklake_parse_changes(p_our_changes) LOOP
        v_our_type := v_our.change_type;
        v_our_value := v_our.change_value;

        FOR v_other IN SELECT * FROM {PROC_SCHEMA}.ducklake_parse_changes(v_other_changes) LOOP
            v_other_type := v_other.change_type;
            v_other_value := v_other.change_value;

            -- Dropping same table/view/macro/schema
            IF v_our_type = 'dropped_table' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to drop table with index "%" - but another transaction has dropped it already', v_our_value;
            END IF;
            IF v_our_type = 'dropped_view' AND v_other_type = 'dropped_view' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to drop view with index "%" - but another transaction has dropped it already', v_our_value;
            END IF;
            IF v_our_type = 'dropped_scalar_macro' AND v_other_type = 'dropped_scalar_macro' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to drop macro with index "%" - but another transaction has dropped it already', v_our_value;
            END IF;
            IF v_our_type = 'dropped_table_macro' AND v_other_type = 'dropped_table_macro' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to drop macro with index "%" - but another transaction has dropped it already', v_our_value;
            END IF;
            IF v_our_type = 'dropped_schema' AND v_other_type = 'dropped_schema' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to drop schema with index "%" - but another transaction has dropped it already', v_our_value;
            END IF;

            -- Creating same schema
            IF v_our_type = 'created_schema' AND v_other_type = 'created_schema' AND lower(v_our_value) = lower(v_other_value) THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to create schema with name "%" - but another transaction has created a schema with this name already', v_our_value;
            END IF;

            -- Dropped schema vs created entries in that schema (resolve schema ID to name)
            IF v_our_type = 'dropped_schema' AND v_other_type IN ('created_table', 'created_view') THEN
                EXECUTE format('SELECT name FROM %I.ducklake_schema WHERE schema_id = $1', p_schema)
                    INTO v_resolved_name USING v_our_value::BIGINT;
                IF v_resolved_name IS NOT NULL THEN
                    v_resolved_prefix := '"' || replace(v_resolved_name, '"', '""') || '".';
                    IF position(v_resolved_prefix IN v_other_value) = 1 THEN
                        RAISE EXCEPTION 'Transaction conflict - attempting to drop schema with index "%" - but another transaction has created an entry in this schema', v_our_value;
                    END IF;
                END IF;
            END IF;

            -- Created tables/views in dropped schemas (resolve schema ID to name)
            IF v_our_type IN ('created_table', 'created_view') AND v_other_type = 'dropped_schema' THEN
                EXECUTE format('SELECT name FROM %I.ducklake_schema WHERE schema_id = $1', p_schema)
                    INTO v_resolved_name USING v_other_value::BIGINT;
                IF v_resolved_name IS NOT NULL THEN
                    v_resolved_prefix := '"' || replace(v_resolved_name, '"', '""') || '".';
                    IF position(v_resolved_prefix IN v_our_value) = 1 THEN
                        RAISE EXCEPTION 'Transaction conflict - attempting to create entry with name "%" - but another transaction has dropped this schema', v_our_value;
                    END IF;
                END IF;
            END IF;

            -- Creating same table/view (case-insensitive name match, including cross-type table vs view)
            IF (v_our_type IN ('created_table', 'created_view')) AND (v_other_type IN ('created_table', 'created_view'))
               AND lower(v_our_value) = lower(v_other_value) THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to create entry with name "%" - but another transaction has created an entry with this name already', v_our_value;
            END IF;

            -- Creating same macro
            IF v_our_type = 'created_scalar_macro' AND v_other_type = 'created_scalar_macro' AND lower(v_our_value) = lower(v_other_value) THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to create macro with name "%" - but another transaction has created a macro with this name already', v_our_value;
            END IF;
            IF v_our_type = 'created_table_macro' AND v_other_type = 'created_table_macro' AND lower(v_our_value) = lower(v_other_value) THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to create table macro with name "%" - but another transaction has created a table macro with this name already', v_our_value;
            END IF;

            -- Insert into dropped/altered table
            IF v_our_type = 'inserted_into_table' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'inserted_into_table' AND v_other_type = 'altered_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has altered it', v_our_value;
            END IF;

            -- EXTRA SAFETY: inserted_into_table overlap (conservative check for stats correctness)
            IF v_our_type = 'inserted_into_table' AND v_other_type = 'inserted_into_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has inserted into it', v_our_value;
            END IF;

            -- Inlined insert into dropped/altered table
            IF v_our_type = 'inlined_insert' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'inlined_insert' AND v_other_type = 'altered_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has altered it', v_our_value;
            END IF;

            -- EXTRA SAFETY: inlined_insert overlap
            IF v_our_type = 'inlined_insert' AND v_other_type = 'inlined_insert' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to insert into table with index "%" - but another transaction has inserted into it', v_our_value;
            END IF;

            -- Delete from dropped/altered/compacted table
            IF v_our_type = 'deleted_from_table' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'deleted_from_table' AND v_other_type = 'altered_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has altered it', v_our_value;
            END IF;
            IF v_our_type = 'deleted_from_table' AND v_other_type = 'merge_adjacent' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;
            IF v_our_type = 'deleted_from_table' AND v_other_type = 'rewrite_delete' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;
            -- Conservative table-level check: both transactions deleted from the same table
            IF v_our_type = 'deleted_from_table' AND v_other_type = 'deleted_from_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has deleted from it', v_our_value;
            END IF;

            -- Inlined deletes
            IF v_our_type = 'inlined_delete' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'inlined_delete' AND v_other_type = 'altered_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has altered it', v_our_value;
            END IF;
            IF v_our_type = 'inlined_delete' AND v_other_type = 'inlined_delete' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has deleted from it', v_our_value;
            END IF;
            IF v_our_type = 'inlined_delete' AND v_other_type = 'inline_flush' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to delete from table with index "%" - but another transaction has flushed the inlined data', v_our_value;
            END IF;

            -- Inline flush
            IF v_our_type = 'inline_flush' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'inline_flush' AND v_other_type = 'inlined_delete' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data with index "%" - but another transaction has deleted from it', v_our_value;
            END IF;
            IF v_our_type = 'inline_flush' AND v_other_type = 'inline_flush' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to flush inline data with index "%" - but another transaction has flushed it', v_our_value;
            END IF;

            -- Compaction conflicts
            IF v_our_type = 'merge_adjacent' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'merge_adjacent' AND v_other_type = 'deleted_from_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has deleted from it', v_our_value;
            END IF;
            IF v_our_type = 'merge_adjacent' AND v_other_type = 'merge_adjacent' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;
            IF v_our_type = 'merge_adjacent' AND v_other_type = 'rewrite_delete' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;

            IF v_our_type = 'rewrite_delete' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'rewrite_delete' AND v_other_type = 'deleted_from_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has deleted from it', v_our_value;
            END IF;
            IF v_our_type = 'rewrite_delete' AND v_other_type = 'merge_adjacent' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;
            IF v_our_type = 'rewrite_delete' AND v_other_type = 'rewrite_delete' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to compact table with index "%" - but another transaction has compacted it', v_our_value;
            END IF;

            -- Alter conflicts
            IF v_our_type = 'altered_table' AND v_other_type = 'dropped_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to alter table with index "%" - but another transaction has dropped it', v_our_value;
            END IF;
            IF v_our_type = 'altered_table' AND v_other_type = 'altered_table' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to alter table with index "%" - but another transaction has altered it', v_our_value;
            END IF;
            IF v_our_type = 'altered_view' AND v_other_type = 'altered_view' AND v_our_value = v_other_value THEN
                RAISE EXCEPTION 'Transaction conflict - attempting to alter view with index "%" - but another transaction has altered it', v_our_value;
            END IF;
        END LOOP;
    END LOOP;
END;
$fn$;

-- Replace placeholders in SQL
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_replace_placeholders(
    p_sql TEXT,
    p_snapshot_id BIGINT,
    p_schema_version BIGINT,
    p_catalog_base BIGINT,
    p_file_base BIGINT,
    p_next_catalog_id BIGINT,
    p_next_file_id BIGINT
)
RETURNS TEXT
LANGUAGE plpgsql AS $fn$
DECLARE
    result TEXT := p_sql;
    v_match TEXT[];
    v_offset BIGINT;
BEGIN
    -- Replace simple placeholders
    result := replace(result, '{SNAPSHOT_ID}', p_snapshot_id::TEXT);
    result := replace(result, '{SCHEMA_VERSION}', p_schema_version::TEXT);
    result := replace(result, '{NEXT_CATALOG_ID}', p_next_catalog_id::TEXT);
    result := replace(result, '{NEXT_FILE_ID}', p_next_file_id::TEXT);

    -- Replace offset-based catalog ID placeholders
    WHILE result ~ '\{CATALOG_ID:\d+\}' LOOP
        v_match := regexp_matches(result, '\{CATALOG_ID:(\d+)\}');
        v_offset := v_match[1]::BIGINT;
        result := replace(result, '{CATALOG_ID:' || v_offset || '}',
                         (p_catalog_base + v_offset)::TEXT);
    END LOOP;

    -- Replace offset-based file ID placeholders
    WHILE result ~ '\{FILE_ID:\d+\}' LOOP
        v_match := regexp_matches(result, '\{FILE_ID:(\d+)\}');
        v_offset := v_match[1]::BIGINT;
        result := replace(result, '{FILE_ID:' || v_offset || '}',
                         (p_file_base + v_offset)::TEXT);
    END LOOP;

    RETURN result;
END;
$fn$;

-- Inner commit function: get snapshot, check conflicts, replace placeholders, execute
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_try_commit(
    p_schema TEXT,
    p_batch_sql TEXT,
    p_schema_delta BIGINT,
    p_txn_start_snapshot BIGINT,
    p_our_changes TEXT,
    p_delta_catalog BIGINT,
    p_delta_file BIGINT
)
RETURNS TABLE(new_snapshot_id BIGINT, new_schema_version BIGINT)
LANGUAGE plpgsql AS $fn$
DECLARE
    v_latest_sid BIGINT;
    v_latest_sv BIGINT;
    v_latest_nci BIGINT;
    v_latest_nfi BIGINT;
    v_new_sid BIGINT;
    v_new_sv BIGINT;
    v_catalog_base BIGINT;
    v_file_base BIGINT;
    v_next_catalog_id BIGINT;
    v_next_file_id BIGINT;
    v_sql TEXT;
BEGIN
    -- Get latest snapshot
    EXECUTE format(
        'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM %I.ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM %I.ducklake_snapshot)',
        p_schema, p_schema
    ) INTO v_latest_sid, v_latest_sv, v_latest_nci, v_latest_nfi;

    -- Check for conflicts if snapshot has advanced
    IF v_latest_sid > p_txn_start_snapshot THEN
        PERFORM {PROC_SCHEMA}.ducklake_check_conflicts(p_schema, p_txn_start_snapshot, p_our_changes);
    END IF;

    -- Compute new values
    v_new_sid := v_latest_sid + 1;
    v_new_sv := v_latest_sv + p_schema_delta;
    v_catalog_base := v_latest_nci;
    v_file_base := v_latest_nfi;
    v_next_catalog_id := v_latest_nci + p_delta_catalog;
    v_next_file_id := v_latest_nfi + p_delta_file;

    -- Replace placeholders and execute
    SELECT {PROC_SCHEMA}.ducklake_replace_placeholders(p_batch_sql, v_new_sid, v_new_sv, v_catalog_base, v_file_base, v_next_catalog_id, v_next_file_id)
        INTO v_sql;

    EXECUTE v_sql;

    RETURN QUERY SELECT v_new_sid, v_new_sv;
END;
$fn$;

-- Outer commit function with retry loop
CREATE OR REPLACE FUNCTION {PROC_SCHEMA}.ducklake_commit_with_retry(
    p_schema TEXT,
    p_batch_sql TEXT,
    p_schema_delta BIGINT,
    p_txn_start_snapshot BIGINT,
    p_our_changes TEXT,
    p_delta_catalog BIGINT,
    p_delta_file BIGINT,
    p_max_retry INT,
    p_retry_wait_ms INT,
    p_retry_backoff FLOAT
)
RETURNS TABLE(result_snapshot_id BIGINT, result_schema_version BIGINT)
LANGUAGE plpgsql AS $fn$
DECLARE
    v_result RECORD;
    v_attempt INT;
BEGIN
    FOR v_attempt IN 1..p_max_retry LOOP
        BEGIN
            SELECT * INTO v_result
            FROM {PROC_SCHEMA}.ducklake_try_commit(p_schema, p_batch_sql, p_schema_delta,
                p_txn_start_snapshot, p_our_changes, p_delta_catalog, p_delta_file);
            result_snapshot_id := v_result.new_snapshot_id;
            result_schema_version := v_result.new_schema_version;
            RETURN NEXT;
            RETURN;
        EXCEPTION WHEN OTHERS THEN
            IF NOT {PROC_SCHEMA}.ducklake_is_retryable(SQLERRM) THEN
                RAISE EXCEPTION 'Failed to commit DuckLake transaction (not retryable) after % attempts: %',
                    v_attempt, SQLERRM;
            END IF;
            IF v_attempt >= p_max_retry THEN
                RAISE EXCEPTION 'Exceeded maximum retry count of % for DuckLake commit: %',
                    p_max_retry, SQLERRM;
            END IF;
            PERFORM pg_sleep(
                (p_retry_wait_ms / 1000.0) *
                power(p_retry_backoff, v_attempt - 1) *
                (0.5 + random() * 0.5)
            );
        END;
    END LOOP;
END;
$fn$;
)";

	proc_sql = StringUtil::Replace(proc_sql, "{PROC_SCHEMA}", schema_identifier);
	auto result = connection.Query(StringUtil::Format("CALL postgres_execute(%s, %s)", catalog_literal, SQLString(proc_sql)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to create DuckLake stored procedures: ");
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteStoredProcCommit(
    DuckLakeSnapshot snapshot, string &batch_sql,
    idx_t schema_delta, idx_t txn_start_snapshot,
    const string &our_changes, idx_t delta_catalog, idx_t delta_file) {

	auto &commit_info = transaction.GetCommitInfo();
	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	// Replace non-retryable placeholders in C++ (these don't change on retry)
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_CATALOG}", schema_identifier);
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_CATALOG_NAME_IDENTIFIER}",
	                                DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName()));
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	batch_sql = StringUtil::Replace(batch_sql, "{METADATA_PATH}", metadata_path);
	batch_sql = StringUtil::Replace(batch_sql, "{DATA_PATH}", data_path);
	batch_sql = StringUtil::Replace(batch_sql, "{AUTHOR}", commit_info.author.ToSQLString());
	batch_sql = StringUtil::Replace(batch_sql, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	batch_sql = StringUtil::Replace(batch_sql, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	// Escape single quotes in batch_sql for embedding in the SQL statement
	auto escaped_batch_sql = StringUtil::Replace(batch_sql, "'", "''");
	auto escaped_our_changes = StringUtil::Replace(our_changes, "'", "''");
	// p_schema must be the raw (unquoted) schema name since stored functions use format('%I', p_schema)
	auto raw_schema_name = ducklake_catalog.MetadataSchemaName();
	auto escaped_raw_schema = StringUtil::Replace(raw_schema_name, "'", "''");

	// Call ducklake_try_commit directly (single attempt, retry handled by C++)
	string call_sql = StringUtil::Format(
	    "SELECT * FROM %s.ducklake_try_commit("
	    "'%s', "     // p_schema (raw, unquoted - %I in PL/pgSQL will quote it)
	    "'%s', "     // p_batch_sql
	    "%llu, "     // p_schema_delta
	    "%llu, "     // p_txn_start_snapshot
	    "'%s', "     // p_our_changes
	    "%llu, "     // p_delta_catalog
	    "%llu"       // p_delta_file
	    ")",
	    schema_identifier_escaped,
	    escaped_raw_schema,
	    escaped_batch_sql,
	    schema_delta,
	    txn_start_snapshot,
	    escaped_our_changes,
	    delta_catalog,
	    delta_file);

	return connection.Query(StringUtil::Format("SELECT * FROM postgres_query(%s, %s)", catalog_literal, SQLString(call_sql)));
}

} // namespace duckdb
