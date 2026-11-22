#include "hive/planning/hive_query_cache.hpp"

#include "hive/core/hive_location.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/planning/hive_multi_file_reader.hpp"

namespace duckdb {

static constexpr const char *HIVE_QUERY_CACHE = "hive_query_cache";
static constexpr const char *HIVE_LISTING_CACHE = "hive_listing_cache";

namespace {

//! The directory listings of Hive tables, kept across statements under hive_cache_listings
class HiveListingCache : public ClientContextState {
public:
	shared_ptr<MultiFileList> GetListing(ClientContext &context, const string &directory) {
		return listings.Get(context, directory);
	}

	void Clear() {
		listings.Clear();
	}

	void QueryEnd(ClientContext &context) override {
		Value enabled;
		if (!context.TryGetCurrentSetting("hive_cache_listings", enabled) || enabled.IsNull() ||
		    !BooleanValue::Get(enabled)) {
			Clear();
		}
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!HiveTransactionCache::ConnectionScoped(context)) {
			Clear();
		}
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

private:
	HiveDirectoryListings listings;
};

} // namespace

shared_ptr<MultiFileList> HiveDirectoryListings::Get(ClientContext &context, const string &directory) {
	shared_ptr<HiveDirectoryListingEntry> entry;
	{
		annotated_lock_guard<annotated_mutex> guard(lock);
		auto &slot = listings[directory];
		if (!slot) {
			slot = make_shared_ptr<HiveDirectoryListingEntry>();
		}
		entry = slot;
	}
	// lock per directory: GlobFileList fetches the first page right away, and listings of other directories must not
	// wait behind it
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (!entry->listing) {
		auto &fs = FileSystem::GetFileSystem(context);
		entry->listing = shared_ptr<MultiFileList>(ListLocation(fs, directory));
	}
	return entry->listing;
}

void HiveDirectoryListings::Clear() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	listings.clear();
}

void ClearHiveListingCache(ClientContext &context) {
	auto cache = context.GetContext().GetRegisteredState().Get<HiveListingCache>(HIVE_LISTING_CACHE);
	if (cache) {
		cache->Clear();
	}
}

shared_ptr<HiveQueryCache> HiveQueryCache::Get(ClientContext &context) {
	return context.GetContext().GetRegisteredState().GetOrCreate<HiveQueryCache>(HIVE_QUERY_CACHE);
}

string HiveQueryCache::TableKey(const string &catalog, const string &database, const string &table) {
	return catalog + "\x1f" + database + "\x1f" + table;
}

void HiveQueryCache::QueryEnd(ClientContext &context) {
	partitions.Clear();
	directory_listings.Clear();
	annotated_lock_guard<annotated_mutex> guard(lock);
	hive_tables.clear();
	samples.clear();
	footers.clear();
	scan_count = 0;
}

idx_t HiveQueryCache::NextScanId() {
	annotated_lock_guard<annotated_mutex> guard(lock);
	return ++scan_count;
}

shared_ptr<HiveTableEntry> HiveQueryCache::GetHiveTable(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = hive_tables[key];
	if (!entry) {
		entry = make_shared_ptr<HiveTableEntry>();
	}
	return entry;
}

shared_ptr<HiveTableEntry> HiveQueryCache::FindHiveTable(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto entry = hive_tables.find(key);
	return entry == hive_tables.end() ? nullptr : entry->second;
}

shared_ptr<HiveTableSampleEntry> HiveQueryCache::GetSample(const string &table) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = samples[table];
	if (!entry) {
		entry = make_shared_ptr<HiveTableSampleEntry>();
	}
	return entry;
}

shared_ptr<HiveFooterEntry> HiveQueryCache::GetFooters(const string &key) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto &entry = footers[key];
	if (!entry) {
		entry = make_shared_ptr<HiveFooterEntry>();
	}
	return entry;
}

shared_ptr<MultiFileList> HiveQueryCache::GetDirectoryListing(ClientContext &context, const string &directory,
                                                              bool cache_in_connection) {
	auto key = directory;
	StringUtil::RTrim(key, "/");
	if (cache_in_connection) {
		// the connection's listings (hive_cache_listings): kept for later statements, cleared with the metadata cache
		return context.GetContext()
		    .GetRegisteredState()
		    .GetOrCreate<HiveListingCache>(HIVE_LISTING_CACHE)
		    ->GetListing(context, key);
	}
	return directory_listings.Get(context, key);
}

shared_ptr<const vector<HivePartitionInfo>> FindTablePartitions(ClientContext &context, const HiveScanInfo &info) {
	auto entry = HiveQueryCache::Get(context)->FindHiveTable(info.CacheKey());
	if (!entry) {
		return nullptr;
	}
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	return entry->partitions;
}

} // namespace duckdb
