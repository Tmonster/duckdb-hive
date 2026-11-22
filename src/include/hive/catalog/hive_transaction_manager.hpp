#pragma once

#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "hive/catalog/hive_transaction.hpp"

namespace duckdb {
class HiveCatalog;

class HiveTransactionManager : public TransactionManager {
public:
	HiveTransactionManager(AttachedDatabase &db_p, HiveCatalog &hive_catalog);

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;

	void Checkpoint(ClientContext &context, bool force = false) override;

	//! Keep a catalog entry that was replaced or removed alive until every transaction that may still use it has ended:
	//! a running statement refers to the entries it bound
	void RetireEntry(unique_ptr<CatalogEntry> entry);

private:
	//! Free the retired entries no running transaction started before. Called with transaction_lock held.
	void FreeRetiredEntries();

private:
	struct RetiredEntry {
		//! The transactions started before the entry was retired are those with a lower sequence number
		idx_t retired_at;
		unique_ptr<CatalogEntry> entry;
	};

	HiveCatalog &hive_catalog;
	mutex transaction_lock;
	reference_map_t<Transaction, unique_ptr<HiveTransaction>> transactions;
	//! The sequence number of every running transaction, in the order they started
	reference_map_t<Transaction, idx_t> transaction_sequence;
	idx_t next_sequence = 0;
	vector<RetiredEntry> retired_entries;
};

} // namespace duckdb
