#include "hive/catalog/hive_schema_set.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/catalog/hive_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

HiveSchemaSet::HiveSchemaSet(HiveCatalog &catalog) : catalog(catalog), id(HiveTransactionCache::NextId()) {
}

unique_ptr<HiveSchemaEntry> HiveSchemaSet::CreateSchemaEntry(const HiveDatabaseInfo &database, bool loaded) {
	CreateSchemaInfo info;
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), Identifier(database.name), info.GetQualifiedName().Name()));
	info.internal = false;
	return make_uniq<HiveSchemaEntry>(catalog, info, database, loaded);
}

void HiveSchemaSet::LoadEntries(ClientContext &context, HiveTransactionCache &cache) {
	if (cache.HasListed(id)) {
		return;
	}
	auto databases = catalog.Metastore().GetDatabases(context);
	case_insensitive_set_t listed;
	for (auto &database : databases) {
		listed.insert(database.name);
		auto entry = entries.find(database.name);
		if (entry != entries.end()) {
			// already loaded through a direct lookup, possibly from a table lookup that only knew the name
			entry->second->SetSchemaInfo(database);
			continue;
		}
		entries.emplace(database.name, CreateSchemaEntry(database));
	}
	// what the metastore no longer returns was dropped by another client
	for (auto &name : UnlistedNames(entries, listed)) {
		RetireEntry(name);
	}
	cache.MarkListed(id);
}

optional_ptr<CatalogEntry> HiveSchemaSet::GetEntry(ClientContext &context, const string &name) {
	if (name.empty()) {
		return nullptr;
	}
	// as in HiveTableSet::GetEntry, the reader's Hive transaction keeps a retired entry alive while it is used
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry != entries.end()) {
		return entry->second.get();
	}
	// not cached, ask the metastore for this database directly
	HiveDatabaseInfo database;
	if (!catalog.Metastore().GetDatabase(context, name, database)) {
		return nullptr;
	}
	auto schema_entry = CreateSchemaEntry(database);
	auto result = entries.emplace(database.name, std::move(schema_entry));
	return result.first->second.get();
}

optional_ptr<CatalogEntry> HiveSchemaSet::GetCachedEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<CatalogEntry> HiveSchemaSet::GetEntryByOid(ClientContext &context, idx_t oid) {
	Transaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	for (auto &entry : entries) {
		if (entry.second->oid == oid) {
			return entry.second.get();
		}
	}
	return nullptr;
}

optional_ptr<CatalogEntry> HiveSchemaSet::AddEntryIfAbsent(unique_ptr<HiveSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	if (entries.find(name) != entries.end()) {
		return nullptr;
	}
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void HiveSchemaSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	auto &transaction = HiveTransaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context, *transaction.cache);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> HiveSchemaSet::CreateEntry(unique_ptr<HiveSchemaEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	RetireEntry(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void HiveSchemaSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
}

void HiveSchemaSet::RetireEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return;
	}
	catalog.GetAttached().GetTransactionManager().Cast<HiveTransactionManager>().RetireEntry(std::move(entry->second));
	entries.erase(entry);
}

} // namespace duckdb
