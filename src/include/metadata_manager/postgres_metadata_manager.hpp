//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsInlining(const LogicalType &type) override;

	string GetColumnTypeInternal(const LogicalType &type) override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;

	//! ID formatting overrides for stored procedure placeholder support
	string FormatCatalogId(idx_t id) override;
	string FormatFileId(idx_t id) override;

	//! Inlined table name override for stored procedure placeholder support
	string GetInlinedTableName(const DuckLakeTableInfo &table, const DuckLakeSnapshot &snapshot) override;

	//! Enable stored procedure mode with base IDs for offset calculation
	void SetStoredProcedureMode(idx_t catalog_base, idx_t file_base);
	//! Ensure stored procedures exist in the Postgres database
	void EnsureStoredProceduresExist();
	//! Execute commit via stored procedure (single attempt, no internal retry)
	unique_ptr<QueryResult> ExecuteStoredProcCommit(
	    DuckLakeSnapshot snapshot, string &batch_sql,
	    bool schema_changed, idx_t txn_start_snapshot,
	    const string &our_changes, idx_t delta_catalog, idx_t delta_file);
	//! Reset the procedures_created flag (needed after transaction rollback)
	void ResetProceduresCreated();

protected:
	string GetLatestSnapshotQuery() const override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);

	//! Whether stored procedure mode is active
	bool use_stored_procedure = false;
	//! Base catalog ID for offset calculation (snapshot.next_catalog_id at transaction start)
	idx_t catalog_id_base = 0;
	//! Base file ID for offset calculation (snapshot.next_file_id at transaction start)
	idx_t file_id_base = 0;
	//! Whether stored procedures have been created in this session
	bool procedures_created = false;
};

} // namespace duckdb
