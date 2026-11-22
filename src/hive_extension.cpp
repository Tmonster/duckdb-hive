#define DUCKDB_EXTENSION_MAIN

#include "hive_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction_manager.hpp"
#include "hive/core/hive_log_types.hpp"
#include "hive/core/readers/hive_csv_options.hpp"
#include "hive/execution/hive_written_files.hpp"
#include "hive/functions/hive_functions.hpp"
#include "hive/functions/hive_repair_table.hpp"
#include "hive/grammar/hive_grammar.hpp"
#include "hive/metastore/hive_attach.hpp"
#include "hive/planning/stats/hive_stats.hpp"

namespace duckdb {

static unique_ptr<TransactionManager> CreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                               AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<HiveTransactionManager>(db, catalog.Cast<HiveCatalog>());
}

class HiveStorageExtension : public StorageExtension {
public:
	HiveStorageExtension() {
		attach = HiveAttach::Attach;
		create_transaction_manager = CreateTransactionManager;
	}
};

static void SetHiveFooterStatisticsMaxFiles(ClientContext &context, SetScope scope, Value &parameter) {
	if (!parameter.IsNull() && UBigIntValue::Get(parameter) > HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT) {
		throw InvalidInputException("hive_footer_statistics_max_files is at most %d, not %d",
		                            HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT, UBigIntValue::Get(parameter));
	}
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);

	config.AddExtensionOption(
	    HiveWrittenFiles::SETTING,
	    "Add the data files each INSERT or CREATE TABLE AS into a Hive table wrote to the temporary "
	    "table hive_written_files",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));

	config.AddExtensionOption("hive_partition_load_parallelism",
	                          "The number of partitioned Hive tables a statement scans whose partitions are loaded "
	                          "concurrently, when the first of its scans needs partitions. 1 loads each table's "
	                          "partitions on its own, when its scan first needs them. Default 8.",
	                          LogicalType::UBIGINT, Value::UBIGINT(8));
	config.AddExtensionOption(
	    "hive_partition_load_cooldown",
	    "After the metastore throttles a concurrent partition load (hive_partition_load_parallelism), the "
	    "number of seconds every connection loads that metastore's tables one at a time. "
	    "Default 300.",
	    LogicalType::UBIGINT, Value::UBIGINT(300));

	config.AddExtensionOption("hive_create_bucketed_tables",
	                          "Allow CREATE TABLE ... WITH (BucketColumns = [...], NumberOfBuckets = n, SortColumns = "
	                          "[...]) to create a bucketed (clustered) Hive table. DuckDB reads such a table but does "
	                          "not write one: INSERT into it and CREATE TABLE ... AS with these options are refused. "
	                          "Default false.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
	// global only: a view entry is shared by all sessions and reads it without a ClientContext
	ExtensionOption external_catalog_types(
	    "Types of non-Hive catalogs (e.g. ['duckdb', 'iceberg']) whose tables Hive views may store, by type and alias, "
	    "and read; never catalogs in memory, temporary, system or hidden. Global only. Default empty.",
	    LogicalType::LIST(LogicalType::VARCHAR), nullptr, Value::LIST(LogicalType::VARCHAR, vector<Value>()),
	    SetScope::GLOBAL, false, false);
	external_catalog_types.supports_session = false;
	config.AddExtensionOption("experimental_hive_view_external_catalog_types", std::move(external_catalog_types));
	config.AddExtensionOption(
	    "hive_metadata_cache_scope",
	    "How long resolved metastore metadata (tables, partitions) is reused: 'transaction' (default) "
	    "re-resolves per transaction, so writes from other sessions become visible between "
	    "statements; 'connection' serves both binds of a prepared statement and later statements "
	    "from one cache until a write through this connection invalidates it. A change takes "
	    "effect on the next transaction.",
	    LogicalType::VARCHAR, Value("transaction"));

	config.AddExtensionOption("hive_batch_table_lookup",
	                          "After the first table of a database is looked up one at a time, fetch one GetTables "
	                          "page of the database into the metadata cache instead of one lookup per table; larger "
	                          "databases keep per-table lookups for the remainder. Default true.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption(
	    "hive_cache_listings",
	    "Keep the listing of a Hive table's directory for later statements of the connection "
	    "(within one statement every scan and the cardinality sample share a listing already). "
	    "Kept like the metadata cache (hive_metadata_cache_scope) and cleared by the same writes; "
	    "files added any other way (another writer, COPY TO the table location) are not seen "
	    "until then. Files removed any other way (compaction, an overwrite, dropping and recreating "
	    "the table at the same location) make scans under the cached listing fail, or read the "
	    "superseded files, until then. Default false.",
	    LogicalType::BOOLEAN, Value::BOOLEAN(false));

	config.AddExtensionOption("hive_enable_partition_key_statistics",
	                          "Give the optimizer the bounds and distinct count of a Hive table's partition keys, from "
	                          "the partitions a scan reads. Default false, as for read_parquet with hive partitioning.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));

	config.AddExtensionOption("hive_partition_listing_threshold",
	                          "When set, a scan reading at least this many partitions below the table location lists "
	                          "the location once (recursively) instead of one listing per partition. Unset (default), "
	                          "the location is listed when that takes fewer rounds of requests than listing the "
	                          "partitions read, or as many rounds and a tenth of the requests.",
	                          LogicalType::UBIGINT, Value(LogicalType::UBIGINT));

	config.AddExtensionOption("hive_footer_statistics_max_files",
	                          "An unpartitioned parquet scan that reads at most this many files gives the optimizer "
	                          "its row count and each column's min, max and NULLs, from the footers of all of them, "
	                          "which it reads at bind; a scan of more files gives a sampled estimate and no column "
	                          "statistics. 0 turns it off. Default 16, at most 25.",
	                          LogicalType::UBIGINT, Value::UBIGINT(HIVE_FOOTER_STATISTICS_DEFAULT_MAX_FILES),
	                          SetHiveFooterStatisticsMaxFiles);

	config.AddExtensionOption("hive_csv_parallel",
	                          "Read each file of an OpenCSVSerde table with several threads when a scan has at most "
	                          "twice `threads` files. False reads one thread per file, which a file that holds a line "
	                          "break inside a quoted field needs: the parallel scanner refuses such a field together "
	                          "with the NULL padding of short rows. Default true.",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true));

	config.AddExtensionOption("hive_csv_max_line_size",
	                          "The longest line, in bytes, a scan of a csv table reads; a longer line fails the query. "
	                          "Hive reads a line of any length. Up to 8000000 the reader's buffers and how a file is "
	                          "split between threads are unchanged; a larger limit makes each thread read at least "
	                          "this many bytes. Default 8000000.",
	                          LogicalType::UBIGINT, Value::UBIGINT(HIVE_CSV_DEFAULT_MAX_LINE_SIZE));

	RegisterHiveLogTypes(instance);

	// Hive tables are read with read_parquet. A linked parquet is loaded as linked: autoloading would install it first
	if (!instance.ExtensionIsLoaded("parquet")) {
		DuckDB db(instance);
		if (ExtensionHelper::LoadExtension(db, "parquet") != ExtensionLoadResult::LOADED_EXTENSION) {
			ExtensionHelper::AutoLoadExtension(instance, "parquet");
		}
	}
	if (!instance.ExtensionIsLoaded("parquet")) {
		throw MissingExtensionException("The hive extension requires the parquet extension to be loaded!");
	}
	// ATTACH 'thrift://<host>:<port>' (TYPE HIVE) or ATTACH '<file>.duckdb' (TYPE HIVE)
	StorageExtension::Register(config, "hive", make_shared_ptr<HiveStorageExtension>());

	loader.RegisterFunction(GetHiveGetTableResponseFunction());
	loader.RegisterFunction(GetHiveGetDatabaseResponseFunction());
	loader.RegisterFunction(GetHiveGetPartitionResponseFunction());
	loader.RegisterFunction(GetHivePartitionsFunction());
	loader.RegisterFunction(GetHiveAddPartitionFunction());
	loader.RegisterFunction(GetHiveDropPartitionFunction());
	loader.RegisterFunction(GetHiveRenamePartitionFunction());
	loader.RegisterFunction(GetHiveSetPartitionLocationFunction());
	loader.RegisterFunction(GetHiveSetTableLocationFunction());
	loader.RegisterFunction(GetHiveReplaceColumnsFunction());
	loader.RegisterFunction(GetHiveAlterTableFunction());
	loader.RegisterFunction(GetHiveRepairTableFunction());
	// ALTER TABLE ... ADD / DROP PARTITION etc., switched on with SET active_grammar_extensions = ['hive_ddl']
	RegisterHiveGrammarExtension(instance);
	loader.RegisterFunction(GetHiveScanFunction(instance));
}

void HiveExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HiveExtension::Name() {
	return "hive";
}

std::string HiveExtension::Version() const {
#ifdef EXT_VERSION_HIVE
	return EXT_VERSION_HIVE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hive, loader) {
	duckdb::LoadInternal(loader);
}
}
