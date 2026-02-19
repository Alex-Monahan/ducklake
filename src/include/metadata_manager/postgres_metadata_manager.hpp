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

	string GetColumnTypeInternal(const LogicalType &type) override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;

	//! Stored procedure commit support
	bool SupportsStoredProcCommit() const override;
	void SetOffsetMode(bool enable) override;
	bool IsOffsetMode() const override;
	string FormatFileId(idx_t id) const override;
	string FormatCatalogId(idx_t id) const override;
	string FormatSchemaVersion(idx_t schema_version) const override;

	//! Delta-based stats update for stored proc commit path
	string UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats) override;

	//! Install stored procedures into the metadata schema
	void InstallStoredProcedures() override;

	//! Get the Postgres schema identifier (escaped for use inside SQL strings)
	string GetSchemaIdentifierEscaped() const;

protected:
	string GetLatestSnapshotQuery() const override;

private:
	unique_ptr<QueryResult> ExecuteQuery(DuckLakeSnapshot snapshot, string &query, string command);
	bool use_offset_mode = false;
};

} // namespace duckdb
