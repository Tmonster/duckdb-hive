#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"

#include "hive/core/hive_info.hpp"
#include "hive/catalog/hive_entry_set.hpp"
#include "hive/catalog/hive_schema_entry.hpp"

namespace duckdb {
class HiveCatalog;
class HiveTransactionCache;

//! The set of databases of a Hive catalog, lazily loaded from the metastore
class HiveSchemaSet {
public:
	explicit HiveSchemaSet(HiveCatalog &catalog);

public:
	//! Look up a schema by name. Loads the schema from the metastore if it is not cached yet, returns nullptr if it
	//! does not exist
	optional_ptr<CatalogEntry> GetEntry(ClientContext &context, const string &name);
	//! The schema this set already holds, nullptr if it holds none of that name; never asks the metastore. A schema
	//! found through a table lookup is held before its definition is loaded (HiveSchemaEntry::GetSchemaInfo)
	optional_ptr<CatalogEntry> GetCachedEntry(const string &name);
	//! The schema with this oid; an oid is only assigned to a schema of this set, so this never asks the metastore
	//! either
	optional_ptr<CatalogEntry> GetEntryByOid(ClientContext &context, idx_t oid);
	//! Insert 'entry' unless a schema of that name was loaded meanwhile; returns nullptr if one was
	optional_ptr<CatalogEntry> AddEntryIfAbsent(unique_ptr<HiveSchemaEntry> entry);
	//! The schemas, listed from the metastore once per metadata cache
	void Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback);
	//! Insert a (new) schema entry into the set, replacing any existing entry with the same name
	optional_ptr<CatalogEntry> CreateEntry(unique_ptr<HiveSchemaEntry> entry);
	void RemoveEntry(const string &name);

	//! Build a schema catalog entry from a Hive database definition, or from its name alone ('loaded' false)
	unique_ptr<HiveSchemaEntry> CreateSchemaEntry(const HiveDatabaseInfo &database, bool loaded = true);

private:
	//! Lists the databases from the metastore unless the transaction's cache has this set listed, and merges the
	//! listing into entries. Called with entry_lock held.
	void LoadEntries(ClientContext &context, HiveTransactionCache &cache);
	//! Move an entry out of the set; the transaction manager keeps it, and the tables it caches, alive while a
	//! running statement may use them. Called with entry_lock held.
	void RetireEntry(const string &name);

private:
	HiveCatalog &catalog;
	mutex entry_lock;
	case_insensitive_map_t<unique_ptr<HiveSchemaEntry>> entries;
	const hive_entry_set_id_t id;
};

} // namespace duckdb
