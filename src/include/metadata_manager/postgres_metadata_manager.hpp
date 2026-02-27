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

	//! Initialize a new DuckLake - conditionally creates stored procedures
	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;
	//! Migrate to v0.4 - conditionally creates stored procedures
	void MigrateV03(bool allow_failures = false) override;

	//! Try to create stored procedures. Returns true on success, false if PL/pgSQL unavailable.
	bool TryCreateStoredProcedures();

	//! Enable stored procedure mode with base IDs for offset calculation
	void SetStoredProcedureMode(idx_t catalog_base, idx_t file_base);
	//! Execute commit via stored procedure (single attempt, no internal retry)
	unique_ptr<QueryResult> ExecuteStoredProcCommit(
	    DuckLakeSnapshot snapshot, string &batch_sql,
	    bool schema_changed, idx_t txn_start_snapshot,
	    const string &our_changes, idx_t delta_catalog, idx_t delta_file);

protected:
	string GetLatestSnapshotQuery() const override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);

	//! Resolve stored procedure availability based on the ATTACH setting, and persist the result
	void ResolveStoredProcedures();
	//! Ensure stored procedures exist in the Postgres database
	void CreateStoredProcedures();

	//! Whether stored procedure mode is active
	bool use_stored_procedure = false;
	//! Base catalog ID for offset calculation (snapshot.next_catalog_id at transaction start)
	idx_t catalog_id_base = 0;
	//! Base file ID for offset calculation (snapshot.next_file_id at transaction start)
	idx_t file_id_base = 0;
};

} // namespace duckdb
