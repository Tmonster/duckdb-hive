#include "hive/planning/hive_partition_cache.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/core/hive_log_types.hpp"
#include "hive/planning/hive_multi_file_reader.hpp"
#include "hive/planning/hive_query_cache.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

#include <chrono>

namespace duckdb {

static idx_t GetIdxSetting(ClientContext &context, const char *name) {
	Value setting;
	if (!context.TryGetCurrentSetting(name, setting) || setting.IsNull()) {
		// SET ... = NULL: the database's value, which is the registered default unless SET GLOBAL
		DBConfig::GetConfig(context).TryGetCurrentSetting(name, setting);
	}
	if (setting.IsNull()) {
		throw InvalidInputException("%s can not be NULL", name);
	}
	return setting.GetValue<idx_t>();
}

namespace {

//! When the metastore last throttled a prefetch, by HiveMetastore::Identity. Process wide: every connection and
//! database shares the metastore's request rate
class MetastoreThrottledCatalogs {
public:
	static MetastoreThrottledCatalogs &Get() {
		static MetastoreThrottledCatalogs instance;
		return instance;
	}
	void Throttled(const string &catalog) {
		lock_guard<mutex> guard(lock);
		since[catalog] = std::chrono::steady_clock::now();
	}
	//! Whether the metastore throttled a prefetch against 'catalog' less than 'cooldown_seconds' ago
	bool CoolingDown(const string &catalog, idx_t cooldown_seconds) {
		lock_guard<mutex> guard(lock);
		auto entry = since.find(catalog);
		if (entry == since.end()) {
			return false;
		}
		// kept past this caller's cooldown: another connection may wait longer
		return std::chrono::steady_clock::now() - entry->second <
		       std::chrono::seconds(static_cast<int64_t>(cooldown_seconds));
	}

private:
	mutex lock;
	unordered_map<string, std::chrono::steady_clock::time_point> since;
};

} // namespace

static HiveCatalog &GetHiveCatalog(ClientContext &context, const HiveScanInfo &scan) {
	return Catalog::GetCatalog(context, Identifier(scan.catalog_name)).Cast<HiveCatalog>();
}

shared_ptr<const vector<HivePartitionInfo>> HivePartitionCache::Resolve(ClientContext &context, HiveCatalog &catalog,
                                                                        const HiveScanInfo &scan, const Loader &load) {
	auto entry =
	    query_cache.GetHiveTable(HiveQueryCache::TableKey(scan.catalog_name, scan.database_name, scan.table_name));
	// per table, so the metastore calls for different tables do not wait on each other
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->partitions) {
		auto transaction_cache = HiveTransactionCache::Of(context, catalog);
		entry->partitions =
		    transaction_cache ? transaction_cache->GetPartitions(scan.database_name, scan.table_name, load) : load();
	}
	return entry->partitions;
}

shared_ptr<const vector<HivePartitionInfo>> HivePartitionCache::Get(ClientContext &context, const HiveScanInfo &scan) {
	{
		annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
		if (!registered_scans.empty()) {
			vector<shared_ptr<const HiveScanInfo>> scans;
			for (auto &registered : registered_scans) {
				auto alive = registered.lock();
				if (alive) {
					scans.push_back(std::move(alive));
				}
			}
			registered_scans.clear();
			Prefetch(context, scans);
		}
	}
	auto &catalog = GetHiveCatalog(context, scan);
	return Resolve(context, catalog, scan, [&]() {
		return make_shared_ptr<const vector<HivePartitionInfo>>(
		    catalog.Metastore().GetPartitions(context, scan.database_name, scan.table_name));
	});
}

void HivePartitionCache::RegisterScan(const shared_ptr<const HiveScanInfo> &scan) {
	annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
	registered_scans.push_back(scan);
}

void HivePartitionCache::Clear() {
	annotated_lock_guard<annotated_mutex> guard(prefetch_lock);
	registered_scans.clear();
}

bool HivePartitionCache::PrefetchBatch(ClientContext &context, HiveCatalog &catalog, const string &identity,
                                       const vector<reference<const HiveScanInfo>> &batch) {
	vector<HivePartitionLoad> loads;
	vector<string> names;
	for (auto &scan : batch) {
		HivePartitionLoad load;
		load.database_name = scan.get().database_name;
		load.table_name = scan.get().table_name;
		loads.push_back(std::move(load));
		names.push_back(scan.get().Describe());
	}
	HiveScanEvent event(StringUtil::Join(names, ", "), 0, "prefetch");
	ErrorData failure;
	try {
		catalog.Metastore().GetPartitionsOfTables(context, loads);
	} catch (std::exception &ex) {
		failure = ErrorData(ex);
		if (failure.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
	}
	idx_t loaded = 0;
	idx_t partitions = 0;
	bool throttled = false;
	for (idx_t i = 0; i < loads.size(); i++) {
		auto &load = loads[i];
		throttled = throttled || load.throttled;
		if (!failure.HasError() && load.error.HasError()) {
			failure = load.error;
		}
		if (!load.partitions) {
			continue;
		}
		loaded++;
		partitions += load.partitions->size();
		Resolve(context, catalog, batch[i], [&]() { return load.partitions; });
	}
	event.partitions = partitions;
	event.Finish();
	event.detail = StringUtil::Format("loaded %llu of %llu tables", loaded, loads.size());
	if (loaded < loads.size()) {
		event.detail += "; the others load when their scans need them";
		if (failure.HasError()) {
			event.detail += ": " + failure.RawMessage();
		}
	}
	if (throttled) {
		auto cooldown = GetIdxSetting(context, "hive_partition_load_cooldown");
		event.detail += StringUtil::Format("; %s throttled the requests: no prefetch against this catalog for %llu s",
		                                   catalog.Metastore().ServiceName(), cooldown);
		MetastoreThrottledCatalogs::Get().Throttled(identity);
	}
	DUCKDB_LOG(context, HiveScanLogType, event);
	return loaded == loads.size();
}

void HivePartitionCache::Prefetch(ClientContext &context, const vector<shared_ptr<const HiveScanInfo>> &scans) {
	auto parallelism = GetIdxSetting(context, "hive_partition_load_parallelism");
	if (parallelism <= 1) {
		return;
	}
	// per catalog, one scan of each table no cache holds
	map<string, vector<reference<const HiveScanInfo>>> by_catalog;
	unordered_set<string> seen;
	for (auto &scan : scans) {
		auto &info = *scan;
		if (!seen.insert(HiveQueryCache::TableKey(info.catalog_name, info.database_name, info.table_name)).second) {
			continue;
		}
		auto &catalog = GetHiveCatalog(context, info);
		if (Resolve(context, catalog, info, []() { return nullptr; })) {
			continue;
		}
		by_catalog[info.catalog_name].push_back(info);
	}
	auto cooldown = GetIdxSetting(context, "hive_partition_load_cooldown");
	for (auto &entry : by_catalog) {
		auto &tables = entry.second;
		if (tables.size() < 2) {
			// nothing to gain: the table loads when its scan first needs it
			continue;
		}
		auto &catalog = GetHiveCatalog(context, tables[0].get());
		string identity;
		try {
			identity = catalog.Metastore().Identity(context);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT) {
				throw;
			}
			HiveScanEvent event(entry.first, 0, "prefetch");
			event.Finish();
			event.detail = StringUtil::Format("skipped %llu tables: the %s catalog id is unknown: %s", tables.size(),
			                                  catalog.Metastore().ServiceName(), error.RawMessage());
			DUCKDB_LOG(context, HiveScanLogType, event);
			continue;
		}
		if (MetastoreThrottledCatalogs::Get().CoolingDown(identity, cooldown)) {
			HiveScanEvent event(entry.first, 0, "prefetch");
			event.Finish();
			event.detail = StringUtil::Format("skipped %llu tables: %s throttled a prefetch against this catalog "
			                                  "less than %llu s ago",
			                                  tables.size(), catalog.Metastore().ServiceName(), cooldown);
			DUCKDB_LOG(context, HiveScanLogType, event);
			continue;
		}
		for (idx_t start = 0; start < tables.size(); start += parallelism) {
			auto end = MinValue<idx_t>(start + parallelism, tables.size());
			vector<reference<const HiveScanInfo>> batch(tables.begin() + static_cast<int64_t>(start),
			                                            tables.begin() + static_cast<int64_t>(end));
			if (!PrefetchBatch(context, catalog, identity, batch)) {
				break;
			}
		}
	}
}

} // namespace duckdb
