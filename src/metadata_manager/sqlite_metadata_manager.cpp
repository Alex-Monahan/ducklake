#include "metadata_manager/sqlite_metadata_manager.hpp"

namespace duckdb {

SQLiteMetadataManager::SQLiteMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

bool SQLiteMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// SQLite converts IEEE 754 NaN to NULL when storing double values,
	// so FLOAT/DOUBLE must be stored as VARCHAR to preserve NaN through the round-trip
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::TIMESTAMP_TZ:
		return false;
	default:
		return true;
	}
}

} // namespace duckdb
