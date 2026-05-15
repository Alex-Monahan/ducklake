#include "storage/ducklake_transaction_manager.hpp"

#include "duckdb/main/settings.hpp"

namespace duckdb {

DuckLakeTransactionManager::DuckLakeTransactionManager(AttachedDatabase &db_p, DuckLakeCatalog &ducklake_catalog)
    : TransactionManager(db_p), ducklake_catalog(ducklake_catalog) {
}

Transaction &DuckLakeTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_shared_ptr<DuckLakeTransaction>(ducklake_catalog, *this, context);
	transaction->Start();
	if (Settings::Get<ImmediateTransactionModeSetting>(context) && get_snapshot) {
		get_snapshot = false;
		// no snapshot loaded yet for this transaction - load it
		transaction->GetSnapshot();
		get_snapshot = true;
	}
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	// If this context is the ClientContext owned by an existing transaction's metadata connection,
	// link the new transaction to that user-side transaction. The metadata connection's transaction
	// will then consult the user-side transaction's transaction-local catalog state during lookups
	// (see DuckLakeSchemaEntry::LookupEntry / GetSimilarEntry). IsMetadataConnectionContext is
	// lock-free (atomic ClientContext pointer) so it is safe to call while we hold transaction_lock,
	// even when this StartTransaction was reached recursively from inside another transaction's
	// GetConnection -> BeginTransaction (which holds that transaction's connection_lock).
	for (auto &entry : transactions) {
		auto &existing = entry.second->Cast<DuckLakeTransaction>();
		if (existing.IsMetadataConnectionContext(context)) {
			transaction->linked_transaction = &existing;
			break;
		}
	}
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData DuckLakeTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	try {
		ducklake_transaction.Commit();
	} catch (std::exception &ex) {
		return ErrorData(ex);
	}
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void DuckLakeTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &ducklake_transaction = transaction.Cast<DuckLakeTransaction>();
	ducklake_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

} // namespace duckdb
