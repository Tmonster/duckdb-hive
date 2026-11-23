#include "hive/execution/hive_copy.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_schema_entry.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/core/hive_location.hpp"
#include "hive/planning/hive_multi_file_reader.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"

namespace duckdb {

//! 'overwrite' deletes files that must not belong to a table: refuse when a table of the same Hive database, or a
//! partition of one, reads them. A partition can lie outside its table's location, so the partitions of partitioned
//! tables are listed as well. Tables in other databases, catalogs or accounts are not checked.
static void ThrowIfLocationInUse(ClientContext &context, HiveSchemaEntry &schema, const HiveTableInfo &table_info,
                                 const string &location) {
	auto &catalog = schema.catalog.Cast<HiveCatalog>();
	auto &database_name = schema.GetDatabaseName();
	auto target = ComparableLocation(location);
	auto overlaps = [&](const string &other) {
		return !other.empty() && LocationsOverlap(target, ComparableLocation(other));
	};
	auto tables = catalog.Metastore().GetTables(context, database_name, false, nullptr, true);
	for (auto &table : tables) {
		if (overlaps(table.location)) {
			throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' WITH (overwrite = true) is not "
			                            "supported: its location '%s' overlaps the location '%s' of table '%s.%s', "
			                            "whose files it would delete. Give the table another location",
			                            table_info.schema_name, table_info.name, location, table.location,
			                            table_info.schema_name, table.name);
		}
	}
	for (auto &table : tables) {
		if (table.partition_keys.empty()) {
			continue;
		}
		for (auto &partition : catalog.Metastore().GetPartitions(context, database_name, table.name)) {
			if (!overlaps(partition.location)) {
				continue;
			}
			throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' WITH (overwrite = true) is not "
			                            "supported: its location '%s' overlaps the location '%s' of a partition of "
			                            "table '%s.%s', whose files it would delete. Give the table another location",
			                            table_info.schema_name, table_info.name, location, partition.location,
			                            table_info.schema_name, table.name);
		}
	}
}

//! A CREATE TABLE AS writes only to a location without data files: the files of a dropped table there would read as
//! rows of the new one. Files a scan skips (_SUCCESS, directory markers, empty files) do not count; with 'overwrite'
//! the data files are deleted instead.
static void ClearLocation(ClientContext &context, HiveSchemaEntry &schema, const HiveTableInfo &table_info,
                          const string &location, bool overwrite) {
	if (LocationHasGlob(location)) {
		throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
		                            "contains a glob character (*, ? or [)",
		                            table_info.schema_name, table_info.name, location);
	}
	auto &fs = FileSystem::GetFileSystem(context);
	auto listing = ListLocation(fs, location);
	vector<string> data_files;
	for (auto &file : listing->Files()) {
		if (!IsDataFile(file, location + "/")) {
			continue;
		}
		if (!overwrite) {
			throw InvalidInputException("CREATE TABLE AS for Hive table '%s.%s' is not supported: its location '%s' "
			                            "already holds files (e.g. '%s'), such as those of a dropped table, which "
			                            "would read as rows of the new table. Delete them, give the table another "
			                            "location, or create it WITH (overwrite = true) to delete them",
			                            table_info.schema_name, table_info.name, location, file.path);
		}
		data_files.push_back(file.path);
	}
	if (data_files.empty()) {
		return;
	}
	ThrowIfLocationInUse(context, schema, table_info, location);
	try {
		fs.RemoveFiles(data_files);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		throw IOException("CREATE TABLE AS for Hive table '%s.%s' failed deleting the %d data files at its location "
		                  "'%s', so the table was not created; some of the files may already be deleted: %s",
		                  table_info.schema_name, table_info.name, data_files.size(), location, error.RawMessage());
	}
}

HiveCopy::HiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
                   unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, HiveCatalog &catalog_p,
                   optional_ptr<HiveSchemaEntry> schema_p, unique_ptr<BoundCreateTableInfo> info_p,
                   HiveTableInfo table_info_p, const string &write_id_p)
    : PhysicalCopyToFile(physical_plan, std::move(types), std::move(function), std::move(bind_data),
                         estimated_cardinality),
      catalog(catalog_p), schema(schema_p), info(std::move(info_p)), table_info(std::move(table_info_p)),
      write_id(write_id_p) {
	D_ASSERT((schema != nullptr) == (info != nullptr));
}

unique_ptr<GlobalSinkState> HiveCopy::GetGlobalSinkState(ClientContext &context) const {
	annotated_lock_guard<annotated_mutex> guard(start_lock);
	auto &transaction = HiveTransaction::Get(context, catalog);
	// keyed on the transaction, not this operator: each execution of the plan registers its write
	if (!transaction.FindHiveWrite(write_id)) {
		auto location = table_info.location;
		StringUtil::RTrim(location, "/");
		HiveWrite write;
		write.write_id = write_id;
		write.database_name = table_info.schema_name;
		write.table_name = table_info.name;
		write.location = location;
		if (info) {
			// CREATE TABLE AS
			auto overwrite = HiveSchemaEntry::ParseCreateTableOptions(context, info->Base()).overwrite;
			ClearLocation(context, *schema.get_mutable(), table_info, location, overwrite);
			// the client to drop the table with is taken first: once the table exists, nothing may stop the undo
			write.metastore = catalog.Metastore().GetHandle(context);
			auto &create_schema = *schema.get_mutable();
			auto created = create_schema.CreateTableFromInfo(catalog.GetCatalogTransaction(context), *info, table_info);
			if (!created) {
				throw CatalogException("Table with name \"%s\" appeared between planning and execution; retry CREATE "
				                       "TABLE IF NOT EXISTS",
				                       table_info.name);
			}
			write.created_table = true;
			try {
				write.created_table_time = write.metastore->GetTableCreateTime(table_info.schema_name, table_info.name);
			} catch (std::exception &ex) {
				// not known: the undo then drops the table if it holds no other writer's files
				DUCKDB_LOG_WARNING(context, "Could not fetch the Hive table '%s.%s' just created: %s",
				                   table_info.schema_name, table_info.name, ErrorData(ex).RawMessage());
			}
		}
		transaction.AddHiveWrite(std::move(write));
	}
	// object stores have no directories; a local table location is created with its parents
	if (!FileSystem::IsRemoteFile(table_info.location)) {
		FileSystem::GetFileSystem(context).CreateDirectoriesRecursive(table_info.location);
	}
	return PhysicalCopyToFile::GetGlobalSinkState(context);
}

string HiveCopy::GetName() const {
	return info ? "HIVE_CREATE_TABLE_AS_COPY" : PhysicalCopyToFile::GetName();
}

} // namespace duckdb
