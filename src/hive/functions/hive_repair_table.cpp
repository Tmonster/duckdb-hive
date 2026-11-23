#include "hive/functions/hive_repair_table.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_file_credentials.hpp"
#include "hive/catalog/hive_table.hpp"
#include "hive/core/hive_types.hpp"
#include "hive/core/hive_location.hpp"
#include "hive/functions/hive_functions.hpp"
#include "hive/planning/hive_multi_file_reader.hpp"

namespace duckdb {

namespace {

struct HiveRepairTableBindData : public TableFunctionData {
	HivePartitionTarget target;
	vector<LogicalType> key_types;
	string root;
	//! missing := true: also report the registered partitions that are gone, with a status column
	bool report_missing = false;

	//! The partitions are created from the table definition fetched at bind, so a prepared statement binds every time
	bool SupportStatementCache() const override {
		return false;
	}
};

struct HiveRepairTableState : public GlobalTableFunctionState {
	vector<HivePartitionInput> added;
	//! registered partitions below the table location without any file below their location
	vector<HivePartitionInput> missing;
	idx_t offset = 0;
	bool applied = false;
};

//! The repair lists the table's files by path, without the credentials its scans read with
// TODO: support repairing a table with credentials, by listing its files with them
void ThrowIfReadWithFileCredentials(ClientContext &context, const Value &table_name,
                                    const HivePartitionTarget &target) {
	if (!HiveFileCredentialsCallback::Find(context)) {
		return;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY,
	                       ResolveHiveTableName(context, "hive_repair_table", table_name.GetValue<string>()));
	auto entry = Catalog::GetEntry(context, lookup, OnEntryNotFound::RETURN_NULL);
	if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
		return;
	}
	if (!HiveFileCredentialsCallback::Get(context, entry->Cast<HiveTable>()).empty()) {
		throw NotImplementedException("hive_repair_table of %s table '%s', which is read with file credentials, is "
		                              "not supported",
		                              target.catalog->Metastore().ServiceName(), target.TableName());
	}
}

unique_ptr<FunctionData> HiveRepairTableBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HiveRepairTableBindData>();
	result->target = ResolveHiveTable(context, "hive_repair_table", input.inputs[0]);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_repair_table");
	auto &table = result->target.table;
	// the partitions are read from the directory layout Hive writes; another engine's InputFormat lays files out its
	// way
	auto file_format = table.GetFileFormat();
	if (!table.GetFormat().IsWritable(table, file_format)) {
		throw NotImplementedException("hive_repair_table of %s table '%s' is not supported: its InputFormat '%s' is "
		                              "not one Hive uses for %s tables, so DuckDB does not know how the files below "
		                              "the table location are laid out",
		                              result->target.catalog->Metastore().ServiceName(), result->target.TableName(),
		                              table.input_format, HiveFileFormatToString(file_format));
	}
	ThrowIfReadWithFileCredentials(context, input.inputs[0], result->target);
	auto missing = input.named_parameters.find("missing");
	if (missing != input.named_parameters.end() && !missing->second.IsNull()) {
		result->report_missing = BooleanValue::Get(missing->second);
	}
	result->root = result->target.table.location;
	StringUtil::RTrim(result->root, "/");
	if (result->root.empty()) {
		throw InvalidInputException("Table '%s' has no location in %s", result->target.TableName(),
		                            result->target.catalog->Metastore().ServiceName());
	}
	auto keys_count = result->target.table.partition_keys.size();
	for (auto &key : result->target.table.partition_keys) {
		auto type = HiveTypes::ToLogicalType(key.type);
		result->key_types.push_back(type);
		names.emplace_back(key.name);
		return_types.push_back(type);
	}
	names.emplace_back("location");
	return_types.emplace_back(LogicalType::VARCHAR);
	if (result->report_missing) {
		names.emplace_back("status");
		return_types.emplace_back(LogicalType::VARCHAR);
	}
	auto deduplicated = names;
	QueryResult::DeduplicateColumns(deduplicated);
	for (auto i = keys_count; i < names.size(); i++) {
		names[i] = deduplicated[i];
	}
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> HiveRepairTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HiveRepairTableState>();
}

//! 'dt=2024-01-01/country=US/part' -> values [2024-01-01, US], partition_directory 'dt=2024-01-01/country=US'
bool ReadDiscoveredPartition(ClientContext &context, const HiveRepairTableBindData &bind_data,
                             const string &relative_directory, vector<string> &values, string &partition_directory) {
	auto &keys = bind_data.target.table.partition_keys;
	auto components = StringUtil::Split(relative_directory, '/');
	if (components.size() < keys.size()) {
		return false;
	}
	values.clear();
	partition_directory.clear();
	for (idx_t k = 0; k < keys.size(); k++) {
		auto &component = components[k];
		auto equals = component.find('=');
		if (equals == string::npos) {
			return false;
		}
		try {
			if (!StringUtil::CIEquals(HivePartitioning::Unescape(component.substr(0, equals)), keys[k].name)) {
				return false;
			}
			auto value = HivePartitioning::Unescape(component.substr(equals + 1));
			if (value.empty()) {
				return false;
			}
			auto typed = HiveTypes::PartitionValue(context, keys[k].name, value, bind_data.key_types[k]);
			// stored as hive_add_partition stores it: year=07 -> '7'
			values.push_back(typed.IsNull() ? value : typed.ToString());
		} catch (std::exception &) {
			// not UTF-8 once unescaped, or not a value of the key's type
			return false;
		}
		if (k > 0) {
			partition_directory += "/";
		}
		partition_directory += component;
	}
	return true;
}

//! Calls 'callback' for every file below 'directory' as it is listed, keeping none of them: S3 lists the whole tree,
//! a local file system one directory at a time
void ListFilesBelow(FileSystem &fs, const string &directory,
                    const std::function<void(const OpenFileInfo &)> &callback) {
	vector<string> directories {directory};
	while (!directories.empty()) {
		auto current = std::move(directories.back());
		directories.pop_back();
		fs.ListFiles(current, [&](OpenFileInfo &info) {
			info.path = fs.JoinPath(current, info.path);
			if (FileSystem::IsDirectory(info)) {
				directories.push_back(std::move(info.path));
			} else {
				callback(info);
			}
		});
	}
}

void HiveRepairTableApply(ClientContext &context, TableFunctionInput &data, HiveRepairTableState &state) {
	auto &bind_data = data.bind_data->Cast<HiveRepairTableBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	catalog.CheckWritable("BatchCreatePartition");

	auto root_prefix = bind_data.root + "/";
	// locations are compared in ComparableLocation form, so s3a:// and s3n:// partitions match an s3:// table
	auto comparable_root_prefix = ComparableLocation(bind_data.root) + "/";
	set<vector<string>> existing_values;
	unordered_set<string> existing_locations;
	// the registered partitions the listing below can tell are gone: those whose location is below the table location,
	// as values -> (registered location, comparable location)
	map<vector<string>, pair<string, string>> listed_partitions;
	for (auto &partition : catalog.Metastore().GetPartitions(context, table.schema_name, table.name)) {
		existing_values.insert(partition.values);
		auto location = partition.location;
		StringUtil::RTrim(location, "/");
		auto comparable = ComparableLocation(location);
		if (bind_data.report_missing && StringUtil::StartsWith(comparable, comparable_root_prefix)) {
			listed_partitions.emplace(partition.values, make_pair(location, comparable));
		}
		existing_locations.insert(std::move(comparable));
	}

	// with missing := true, every directory below the table location that holds a file, at any depth: a partition
	// location exists when an object is stored below it, as on S3
	unordered_set<string> directories_with_files;
	unordered_set<string> seen_directories;
	// values -> location: the first directory found for the values wins
	map<vector<string>, string> discovered;
	// never from a listing kept by hive_cache_listings (it misses files added after it), and not kept: a table can
	// hold more files than fit in memory
	ListFilesBelow(FileSystem::GetFileSystem(context), bind_data.root, [&](const OpenFileInfo &file) {
		if (bind_data.report_missing) {
			for (auto end = file.path.find_last_of('/'); end != string::npos && end > bind_data.root.size();
			     end = file.path.find_last_of('/', end - 1)) {
				if (!directories_with_files.insert(ComparableLocation(file.path.substr(0, end))).second) {
					break;
				}
			}
		}
		if (!IsDataFile(file, root_prefix)) {
			return;
		}
		auto separator = file.path.find_last_of('/');
		if (separator == string::npos || separator < root_prefix.size()) {
			return;
		}
		auto directory = file.path.substr(0, separator);
		if (!seen_directories.insert(directory).second) {
			return;
		}
		auto relative = directory.substr(root_prefix.size());
		vector<string> values;
		string partition_directory;
		if (!ReadDiscoveredPartition(context, bind_data, relative, values, partition_directory)) {
			return;
		}
		auto location = root_prefix + partition_directory;
		if (existing_values.count(values) || existing_locations.count(ComparableLocation(location))) {
			return;
		}
		discovered.emplace(std::move(values), std::move(location));
	});

	for (auto &entry : discovered) {
		HivePartitionInput partition;
		partition.values = entry.first;
		partition.location = entry.second;
		state.added.push_back(std::move(partition));
	}
	for (auto &entry : listed_partitions) {
		if (!directories_with_files.count(entry.second.second)) {
			HivePartitionInput partition;
			partition.values = entry.first;
			partition.location = entry.second.first;
			state.missing.push_back(std::move(partition));
		}
	}
	if (state.added.empty()) {
		return;
	}
	catalog.Metastore().BatchCreatePartitions(context, table, state.added);
}

void HiveRepairTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<HiveRepairTableBindData>();
	auto &state = data.global_state->Cast<HiveRepairTableState>();
	if (!state.applied) {
		state.applied = true;
		HiveRepairTableApply(context, data, state);
	}
	auto &keys = bind_data.target.table.partition_keys;
	idx_t count = 0;
	auto total = state.added.size() + state.missing.size();
	while (state.offset < total && count < STANDARD_VECTOR_SIZE) {
		auto is_added = state.offset < state.added.size();
		auto &partition = is_added ? state.added[state.offset] : state.missing[state.offset - state.added.size()];
		state.offset++;
		for (idx_t k = 0; k < keys.size(); k++) {
			output.data[k].Append(
			    PartitionListingValue(context, keys[k].name, partition.values[k], bind_data.key_types[k]));
		}
		output.data[keys.size()].Append(Value(partition.location));
		if (bind_data.report_missing) {
			output.data[keys.size() + 1].Append(Value(is_added ? "added" : "missing"));
		}
		count++;
	}
}

} // namespace

TableFunction GetHiveRepairTableFunction() {
	TableFunction function("hive_repair_table", {LogicalType::VARCHAR}, HiveRepairTableScan, HiveRepairTableBind,
	                       HiveRepairTableInit);
	function.GetSignature().WithTypedKwargs("options",
	                                        [](TypedKwargs &options) { options.Add("missing", LogicalType::BOOLEAN); });
	return function;
}

} // namespace duckdb
