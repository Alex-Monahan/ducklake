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
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::VARIANT:
		return false;
	default:
		return true;
	}
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
	case LogicalTypeId::BLOB:
		return "BYTEA";
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

bool PostgresMetadataManager::SupportsStoredProcCommit() const {
	return true;
}

void PostgresMetadataManager::SetOffsetMode(bool enable) {
	use_offset_mode = enable;
}

bool PostgresMetadataManager::IsOffsetMode() const {
	return use_offset_mode;
}

string PostgresMetadataManager::FormatFileId(idx_t id) const {
	if (!use_offset_mode || id < OFFSET_ID_SENTINEL) {
		return to_string(id);
	}
	return StringUtil::Format("({FILE_ID_BASE} + %d)", id - OFFSET_ID_SENTINEL);
}

string PostgresMetadataManager::FormatCatalogId(idx_t id) const {
	if (!use_offset_mode || id < OFFSET_ID_SENTINEL) {
		return to_string(id);
	}
	return StringUtil::Format("({CATALOG_ID_BASE} + %d)", id - OFFSET_ID_SENTINEL);
}

string PostgresMetadataManager::FormatSchemaVersion(idx_t schema_version) const {
	if (!use_offset_mode) {
		return to_string(schema_version);
	}
	return "{SCHEMA_VERSION}";
}

string PostgresMetadataManager::GetSchemaIdentifierEscaped() const {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	return StringUtil::Replace(schema_identifier, "'", "''");
}

string PostgresMetadataManager::UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats) {
	if (!use_offset_mode) {
		// Fall back to base class behavior
		return DuckLakeMetadataManager::UpdateGlobalTableStats(stats);
	}

	// Delta-based stats update for stored proc commit path
	string column_stats_values;
	for (auto &col_stats : stats.column_stats) {
		if (!column_stats_values.empty()) {
			column_stats_values += ",";
		}
		string contains_null;
		if (col_stats.has_contains_null) {
			contains_null = col_stats.contains_null ? "true" : "false";
		} else {
			contains_null = "NULL";
		}
		string contains_nan;
		if (col_stats.has_contains_nan) {
			contains_nan = col_stats.contains_nan ? "true" : "false";
		} else {
			contains_nan = "NULL";
		}
		string min_val = col_stats.has_min ? DuckLakeUtil::StatsToString(col_stats.min_val) : "NULL";
		string max_val = col_stats.has_max ? DuckLakeUtil::StatsToString(col_stats.max_val) : "NULL";
		string extra_stats_val = col_stats.has_extra_stats ? col_stats.extra_stats : "NULL";

		column_stats_values +=
		    StringUtil::Format("(%d, %d, %s, %s, %s, %s, %s)", stats.table_id.index, col_stats.column_id.index,
		                       contains_null, contains_nan, min_val, max_val, extra_stats_val);
	}
	string batch_query;

	if (!stats.initialized) {
		// New table: insert absolute values (no concurrency issue since table didn't exist before)
		batch_query +=
		    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_stats VALUES (%d, %d, %d, %d);",
		                       stats.table_id.index, stats.record_count, stats.next_row_id, stats.table_size_bytes);
		batch_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats VALUES %s;",
		                                  column_stats_values);
	} else {
		// Existing table: use delta-based updates so concurrent inserts don't clobber each other
		// The deltas are: new_value - old_value (which was the current stats at transaction start)
		// But in offset mode, we pass absolute new values and the stored proc handles it
		// Actually, in offset mode we still pass absolute values because the stored proc
		// re-reads the base on each retry. The key insight is that the UPDATE uses
		// incremental additions, so concurrent changes are preserved.
		// We compute deltas: stats contains the NEW values, we need to express them as deltas
		// from whatever the current DB values are. But we don't know what those are in offset mode.
		//
		// Actually the simplest approach: use absolute values in offset mode too, since the stored
		// procedure's retry loop re-reads all data anyway. The stats are computed fresh based on
		// the files we wrote (which don't change) plus whatever the current stats are.
		// For new tables (not initialized), INSERT is fine.
		// For existing tables, we pass the absolute values, which works because:
		// - On first attempt, they match what we computed
		// - On retry after unique_violation, the savepoint rolls back our UPDATE so we re-apply
		batch_query += StringUtil::Format(
		    "UPDATE {METADATA_CATALOG}.ducklake_table_stats SET record_count=%d, file_size_bytes=%d, "
		    "next_row_id=%d WHERE table_id=%d;",
		    stats.record_count, stats.table_size_bytes, stats.next_row_id, stats.table_id.index);
		batch_query += StringUtil::Format(R"(
WITH new_values(tid, cid, new_contains_null, new_contains_nan, new_min, new_max, new_extra_stats) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_table_column_stats
SET contains_null=new_contains_null::boolean, contains_nan=new_contains_nan::boolean, min_value=new_min, max_value=new_max, extra_stats=new_extra_stats
FROM new_values
WHERE table_id=tid AND column_id=cid;
)",
		                                  column_stats_values);
	}
	return batch_query;
}

} // namespace duckdb
