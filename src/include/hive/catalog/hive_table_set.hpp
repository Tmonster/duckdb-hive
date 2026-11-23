#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"

#include "hive/core/hive_info.hpp"
#include "hive/catalog/hive_entry_set.hpp"
#include "hive/catalog/hive_table.hpp"
#include "hive/catalog/hive_view.hpp"

namespace duckdb {
class HiveCatalog;
class HiveSchemaEntry;
class HiveTransactionCache;

//! The set of tables of a single Hive database, lazily loaded from the metastore
class HiveTableSet {
public:
	explicit HiveTableSet(HiveSchemaEntry &schema);

public:
	//! The entry with this name whatever its type (callers check entry->type against what they need), replaced when
	//! The metastore holds another version of it and gone when the metastore no longer has it
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const EntryLookupInfo &lookup);
	//! The entries of the given type (TABLE_ENTRY or VIEW_ENTRY), listed from the metastore once per metadata cache
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! The entries already in the set, without listing the metastore
	void ScanLoaded(const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<CatalogEntry> entry);
	void RemoveEntry(const string &name);

	//! Build the catalog entry for a Hive table definition: a HiveView for a VIRTUAL_VIEW, else a HiveTable
	unique_ptr<CatalogEntry> CreateEntry(const HiveTableInfo &table);
	//! The definition a table or view entry was built from
	static const HiveTableInfo &GetTableInfo(const CatalogEntry &entry);

private:
	//! Lists the database from the metastore unless the transaction's cache has this set listed, and merges the listing
	//! into entries. Called with entry_lock held.
	void LoadEntries(ClientContext &context, HiveTransactionCache &cache);
	//! One first-page GetTables warming the metadata cache, instead of a GetTable per table. Only warms the
	//! cache: entries stay per referenced table and Scan still enumerates, so neither outlives the page's truth.
	void BulkLoadTableInfo(ClientContext &context, HiveTransactionCache &cache);
	static bool BatchTableLookups(ClientContext &context);
	static void SetTableTypeTag(HiveTable &entry);
	//! Move an entry out of the set; the transaction manager keeps it alive while a running statement may use it.
	//! Called with entry_lock held.
	void RetireEntry(const string &name);

private:
	HiveSchemaEntry &schema;
	HiveCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> entries;
	const hive_entry_set_id_t id;
	//! Direct-lookup round trips since the last bulk fetch: bulk pays off from the second one on
	idx_t direct_lookups = 0;
	//! The HiveTransactionCache::Id() the last successful bulk fetch warmed (0 = none yet)
	idx_t bulk_loaded_cache_id = 0;
	//! GetTables was refused for lack of permission: no more bulk fetches
	bool batch_forbidden = false;
};

} // namespace duckdb
