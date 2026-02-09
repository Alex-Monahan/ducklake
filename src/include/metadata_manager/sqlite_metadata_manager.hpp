//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/sqlite_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class SQLiteMetadataManager : public DuckLakeMetadataManager {
public:
	explicit SQLiteMetadataManager(DuckLakeTransaction &transaction);

	bool TypeIsNativelySupported(const LogicalType &type) override;
};

} // namespace duckdb
