#include "hive/catalog/hive_table_set.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

#include "hive/core/hive_types.hpp"
#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_schema_entry.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/catalog/hive_transaction_manager.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

HiveTableSet::HiveTableSet(HiveSchemaEntry &schema)
    : schema(schema), catalog(schema.catalog.Cast<HiveCatalog>()), id(HiveTransactionCache::NextId()) {
}

unique_ptr<CatalogEntry> HiveTableSet::CreateEntry(const HiveTableInfo &table) {
	if (table.IsView()) {
		return HiveView::FromTableInfo(catalog, schema, table);
	}
	CreateTableInfo info(schema, Identifier(table.name));
	if (!table.description.empty()) {
		// So duckdb_tables().comment shows what the metastore stores as Table.Description
		info.comment = Value(table.description);
	}
	auto add_column = [&](const HiveColumn &column) {
		ColumnDefinition definition(Identifier(column.name), HiveTypes::ToLogicalType(column.type));
		if (!column.comment.empty()) {
			definition.SetComment(Value(column.comment));
		}
		info.columns.AddColumn(std::move(definition));
	};
	for (auto &column : table.columns) {
		add_column(column);
	}
	// Hive tables store their partition columns separately, they are regular (trailing) columns for a scan
	for (auto &column : table.partition_keys) {
		add_column(column);
	}
	auto entry = make_uniq<HiveTable>(catalog, schema, info, table);
	SetTableTypeTag(*entry);
	return entry;
}

void HiveTableSet::SetTableTypeTag(HiveTable &entry) {
	// exposed through duckdb_tables().tags['table_type'] (ICEBERG / DELTA / HUDI / SYMLINK / HIVE / UNKNOWN)
	entry.tags["table_type"] = entry.table_info.GetFormat().ToString();
}

bool HiveTableSet::BatchTableLookups(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("hive_batch_table_lookup", setting) && !setting.IsNull()) {
		return BooleanValue::Get(setting);
	}
	return true;
}

void HiveTableSet::LoadEntries(ClientContext &context, HiveTransactionCache &cache) {
	if (cache.HasListed(id)) {
		return;
	}
	vector<HiveTableInfo> tables;
	try {
		tables = catalog.Metastore().GetTables(context, schema.GetDatabaseName(), false, nullptr, true);
	} catch (const MetastoreException &ex) {
		if (!ex.access_denied) {
			throw;
		}
		// A refused listing says nothing about which tables exist: entries stay and the set is not marked, so the next
		// transaction asks again
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Not listing the tables of %s database '%s': %s", catalog.Metastore().ServiceName(),
		                   schema.GetDatabaseName(), error.RawMessage());
		return;
	}
	case_insensitive_set_t listed;
	for (auto &table : tables) {
		listed.insert(table.name);
		// The definition this transaction's lookups see: one the cache already holds wins, otherwise the listed one is
		// cached so the per reference GetEntry does not re-fetch what this one request returned
		shared_ptr<const HiveTableInfo> listed_info = make_shared_ptr<const HiveTableInfo>(table);
		auto info = cache.GetTable(schema.GetDatabaseName(), table.name, [&listed_info]() { return listed_info; });
		auto existing = entries.find(info->name);
		if (existing != entries.end() && GetTableInfo(*existing->second).IsSameVersion(*info)) {
			// already loaded, and still what the metastore holds
			continue;
		}
		unique_ptr<CatalogEntry> entry;
		try {
			entry = CreateEntry(*info);
		} catch (std::exception &ex) {
			// A table whose metastore definition we can not turn into a DuckDB table (e.g. an unsupported column type)
			// must not break listing the other tables: leave it out and log why. Looking the table up by name
			// still reports the error to the user.
			ErrorData error(ex);
			DUCKDB_LOG_ERROR(
			    context, "%s table '%s.%s' could not be converted from the GetTables response and is not listed: %s",
			    catalog.Metastore().ServiceName(), schema.GetDatabaseName(), info->name, error.RawMessage());
			RetireEntry(info->name);
			continue;
		}
		RetireEntry(info->name);
		entries.emplace(info->name, std::move(entry));
	}
	// what the metastore no longer returns was dropped by another client
	for (auto &name : UnlistedNames(entries, listed)) {
		RetireEntry(name);
	}
	cache.MarkListed(id);
}

void HiveTableSet::BulkLoadTableInfo(ClientContext &context, HiveTransactionCache &cache) {
	// Both outcomes reset the round-trip count: on success the page is warm and later misses need no second
	// page; on failure the next bulk attempt waits for another direct round trip first, so a throttled or
	// failing GetTables costs at most one extra request per direct lookup while the per-table path keeps working.
	direct_lookups = 0;
	bool has_more = false;
	vector<HiveTableInfo> tables;
	try {
		tables = catalog.Metastore().GetTables(context, schema.GetDatabaseName(), true, &has_more);
	} catch (const MetastoreException &ex) {
		// A failed bulk fetch must not fail a lookup that never needed GetTables: log it and let the caller
		// fall through to the direct GetTable
		if (ex.access_denied) {
			batch_forbidden = true;
		}
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Not batching the table lookups of %s database '%s', GetTables failed: %s",
		                   catalog.Metastore().ServiceName(), schema.GetDatabaseName(), error.RawMessage());
		return;
	}
	// One page per cache instance: misses past the page (a database larger than one page) stay direct instead
	// of re-reading the same first page
	bulk_loaded_cache_id = cache.Id();
	for (auto &table : tables) {
		// Warm the metadata cache only - already-cached values win; the entry/Scan story is on the declaration
		shared_ptr<const HiveTableInfo> info = make_shared_ptr<const HiveTableInfo>(table);
		cache.GetTable(schema.GetDatabaseName(), table.name, [&info]() { return info; });
	}
	if (has_more) {
		DUCKDB_LOG_INFO(context,
		                "%s database '%s' holds more tables than one GetTables page: read %llu, the "
		                "remaining lookups stay per table",
		                catalog.Metastore().ServiceName(), schema.GetDatabaseName(),
		                NumericCast<uint64_t>(tables.size()));
	}
}

optional_ptr<CatalogEntry> HiveTableSet::GetEntry(ClientContext &context, const EntryLookupInfo &lookup) {
	auto &name = lookup.GetEntryName();
	// the reader's Hive transaction keeps the entry alive while it is used (HiveTransactionManager::RetireEntry); it
	// is started before entry_lock is taken, the order RetireEntry takes the locks in
	Transaction::Get(context, catalog);
	auto cache = HiveTransactionCache::Of(context, catalog);
	bool cached;
	string hive_name;
	{
		lock_guard<mutex> guard(entry_lock);
		auto existing = entries.find(name);
		cached = existing != entries.end();
		// the lookup name may differ in case from the one the metastore knows
		hive_name = cached ? existing->second->name.GetIdentifierName() : name;
		// The attach's first round-trip miss stays direct: a lone lookup must not enumerate a large database.
		// From the second round-trip-causing miss on (round trips count across cache instances), one GetTables
		// page warms the cache, at most once per instance - under the default scope that is once per transaction.
		if (!cached && cache && !batch_forbidden && cache->Id() != bulk_loaded_cache_id && direct_lookups > 0 &&
		    BatchTableLookups(context)) {
			BulkLoadTableInfo(context, *cache);
		}
	}
	bool asked_metastore = false;
	auto load = [&]() -> shared_ptr<const HiveTableInfo> {
		asked_metastore = true;
		HiveTableInfo table;
		if (!catalog.Metastore().GetTable(context, schema.GetDatabaseName(), hive_name, table)) {
			return nullptr;
		}
		return make_shared_ptr<const HiveTableInfo>(std::move(table));
	};
	// the definition the metastore holds, fetched once per transaction (HiveTransactionCache) and outside entry_lock,
	// so that a metastore round trip does not hold up the lookups of other tables
	auto table = cache ? cache->GetTable(schema.GetDatabaseName(), hive_name, load) : load();
	lock_guard<mutex> guard(entry_lock);
	if (!cached && asked_metastore) {
		// counts the round trips of misses, not lookups: a cache hit (including a bulk-warmed one) costs nothing and
		// must not count towards switching to bulk
		direct_lookups++;
	}
	if (!table) {
		RetireEntry(hive_name);
		return nullptr;
	}
	// another lookup may have replaced the entry while the metastore was asked: compare with what is there now
	auto existing = entries.find(table->name);
	if (existing != entries.end() && GetTableInfo(*existing->second).IsSameVersion(*table)) {
		return existing->second.get();
	}
	// new, or another client changed it since the entry was built: a statement that bound the old entry keeps it
	RetireEntry(table->name);
	auto result = entries.emplace(table->name, CreateEntry(*table));
	return result.first->second.get();
}

const HiveTableInfo &HiveTableSet::GetTableInfo(const CatalogEntry &entry) {
	if (entry.type == CatalogType::VIEW_ENTRY) {
		return entry.Cast<HiveView>().table_info;
	}
	return entry.Cast<HiveTable>().table_info;
}

void HiveTableSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	// tables and views share the set and a scan of either type yields both, as in DuckDB's own schema: the catalog
	// functions filter by entry type themselves, and duckdb_columns() collects the columns of views through a
	// TABLE_ENTRY scan
	auto &transaction = HiveTransaction::Get(context, catalog);
	lock_guard<mutex> guard(entry_lock);
	LoadEntries(context, *transaction.cache);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

void HiveTableSet::ScanLoaded(const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> guard(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> HiveTableSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> guard(entry_lock);
	auto name = entry->name.GetIdentifierName();
	RetireEntry(name);
	auto result = entries.emplace(name, std::move(entry));
	return result.first->second.get();
}

void HiveTableSet::RemoveEntry(const string &name) {
	lock_guard<mutex> guard(entry_lock);
	RetireEntry(name);
}

void HiveTableSet::RetireEntry(const string &name) {
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return;
	}
	catalog.GetAttached().GetTransactionManager().Cast<HiveTransactionManager>().RetireEntry(std::move(entry->second));
	entries.erase(entry);
}

} // namespace duckdb
