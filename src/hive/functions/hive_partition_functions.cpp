#include "hive/functions/hive_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/core/hive_types.hpp"
#include "hive/core/hive_location.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/catalog/hive_schema_entry.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

namespace {

void ThrowIsView(const string &function_name, const string &table_name, bool require_partitions,
                 const string &service_name) {
	if (require_partitions) {
		throw BinderException("%s: %s view '%s' has no partitions", function_name, service_name, table_name);
	}
	throw BinderException("%s: '%s' is a %s view, not a table", function_name, table_name, service_name);
}

} // namespace

HivePartitionTarget ResolveHiveTable(ClientContext &context, const string &function_name, const Value &table_name,
                                     bool require_partitions) {
	auto name = ResolveHiveTableName(context, function_name, table_name.GetValue<string>());
	HivePartitionTarget result;
	result.catalog = &Catalog::GetCatalog(context, name.Catalog()).Cast<HiveCatalog>();
	if (!result.catalog->Metastore().GetTable(context, name.Schema().GetIdentifierName(),
	                                          name.Name().GetIdentifierName(), result.table)) {
		throw CatalogException("Table '%s.%s' does not exist in %s catalog '%s'", name.Schema().GetIdentifierName(),
		                       name.Name().GetIdentifierName(), result.catalog->Metastore().ServiceName(),
		                       name.Catalog().GetIdentifierName());
	}
	if (result.table.IsView()) {
		ThrowIsView(function_name, result.TableName(), require_partitions, result.catalog->Metastore().ServiceName());
	}
	auto format = result.table.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("%s only works on Hive tables, '%s' is a %s table", function_name,
		                              result.TableName(), format.Describe(result.table));
	}
	if (require_partitions && result.table.partition_keys.empty()) {
		throw InvalidInputException("Table '%s' is not partitioned", result.TableName());
	}
	return result;
}

Value PartitionListingValue(ClientContext &context, const string &key, const string &str_value,
                            const LogicalType &type) {
	try {
		return HiveTypes::PartitionValue(context, key, str_value, type);
	} catch (std::exception &) {
		return Value(type);
	}
}

namespace {

//! Turn a partition spec {key: value, ...} into the partition values (as strings) in partition key order. The spec
//! must name every partition key and nothing else.
vector<string> ParsePartitionSpec(const string &function_name, const HivePartitionTarget &target, const Value &spec) {
	auto &keys = target.table.partition_keys;
	vector<string> key_names;
	for (auto &key : keys) {
		key_names.push_back(key.name);
	}
	auto describe_keys = StringUtil::Join(key_names, ", ");
	if (spec.type().id() != LogicalTypeId::STRUCT || spec.IsNull()) {
		throw BinderException("%s expects the partition as a struct of the partition keys of table '%s': {%s}",
		                      function_name, target.TableName(), describe_keys);
	}
	auto &spec_types = StructType::GetChildTypes(spec.type());
	auto &spec_values = StructValue::GetChildren(spec);
	vector<string> result(keys.size());
	vector<bool> seen(keys.size(), false);
	for (idx_t i = 0; i < spec_types.size(); i++) {
		auto &name = spec_types[i].first.GetIdentifierName();
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keys.size(); k++) {
			if (StringUtil::CIEquals(keys[k].name, name)) {
				key_index = k;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw BinderException("'%s' is not a partition key of table '%s', the partition keys are: %s", name,
			                      target.TableName(), describe_keys);
		}
		if (seen[key_index]) {
			throw BinderException("Partition key '%s' is given twice", keys[key_index].name);
		}
		seen[key_index] = true;
		auto &value = spec_values[i];
		if (value.IsNull()) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		// store the value the way the partition key type would render it
		auto key_type = HiveTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = value.DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

//! Turn (key, value) pairs into the partition values in partition key order; NULL values (empty optional) become
//! the default partition
vector<string> ParsePartitionPairs(const string &function_name, const HivePartitionTarget &target,
                                   const vector<pair<string, optional<string>>> &pairs) {
	auto &keys = target.table.partition_keys;
	vector<string> key_names;
	for (auto &key : keys) {
		key_names.push_back(key.name);
	}
	auto describe_keys = StringUtil::Join(key_names, ", ");
	vector<string> result(keys.size());
	vector<bool> seen(keys.size(), false);
	for (auto &pair : pairs) {
		idx_t key_index = DConstants::INVALID_INDEX;
		for (idx_t k = 0; k < keys.size(); k++) {
			if (StringUtil::CIEquals(keys[k].name, pair.first)) {
				key_index = k;
				break;
			}
		}
		if (key_index == DConstants::INVALID_INDEX) {
			throw BinderException("'%s' is not a partition key of table '%s', the partition keys are: %s", pair.first,
			                      target.TableName(), describe_keys);
		}
		if (seen[key_index]) {
			throw BinderException("Partition key '%s' is given twice", keys[key_index].name);
		}
		seen[key_index] = true;
		if (!pair.second) {
			result[key_index] = HivePartitioning::DEFAULT_PARTITION_NAME;
			continue;
		}
		auto key_type = HiveTypes::ToLogicalType(keys[key_index].type);
		result[key_index] = Value(*pair.second).DefaultCastAs(key_type).ToString();
	}
	for (idx_t k = 0; k < keys.size(); k++) {
		if (!seen[k]) {
			throw BinderException("%s: the partition must name every partition key of table '%s' (%s), '%s' is missing",
			                      function_name, target.TableName(), describe_keys, keys[k].name);
		}
	}
	return result;
}

//! The location a partition gets when none is given: <table location>/<key>=<value>/...
string DefaultPartitionLocation(const HivePartitionTarget &target, const vector<string> &values) {
	auto location = target.table.location;
	StringUtil::RTrim(location, "/");
	if (location.empty()) {
		throw InvalidInputException("Table '%s' has no location in %s, provide the partition location explicitly",
		                            target.TableName(), target.catalog->Metastore().ServiceName());
	}
	vector<string> keys;
	for (auto &key : target.table.partition_keys) {
		keys.push_back(key.name);
	}
	return Path::FromString(location).Join(HivePartitionDirectory(keys, values)).ToString();
}

//! Bind data for the functions that change one partition and report one row when executed
struct HivePartitionChangeBindData : public TableFunctionData {
	HivePartitionTarget target;

	//! The table is rewritten from the definition bind fetched: a prepared statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
	vector<string> values;
	vector<string> new_values;
	string location;
	bool if_not_exists = false;
	bool if_exists = false;
};

struct HivePartitionChangeState : public GlobalTableFunctionState {
	bool done = false;
};

unique_ptr<GlobalTableFunctionState> HivePartitionChangeInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HivePartitionChangeState>();
}

//! The partition a change rewrites, throws a CatalogException if it does not exist
HivePartitionDetails FetchPartition(ClientContext &context, const HivePartitionTarget &target,
                                    const vector<string> &values) {
	HivePartitionDetails result;
	if (!target.catalog->Metastore().GetPartition(context, target.table.schema_name, target.table.name, values,
	                                              result)) {
		throw CatalogException("Partition [%s] does not exist in %s table '%s'", StringUtil::Join(values, ", "),
		                       target.catalog->Metastore().ServiceName(), target.TableName());
	}
	return result;
}

//! Throws a CatalogException if a partition with the given values exists
void ThrowIfPartitionExists(ClientContext &context, const HivePartitionTarget &target, const vector<string> &values) {
	HivePartitionInfo existing;
	if (target.catalog->Metastore().GetPartition(context, target.table.schema_name, target.table.name, values,
	                                             existing)) {
		throw CatalogException("Partition [%s] already exists in %s table '%s'", StringUtil::Join(values, ", "),
		                       target.catalog->Metastore().ServiceName(), target.TableName());
	}
}

//===--------------------------------------------------------------------===//
// hive_partitions
//===--------------------------------------------------------------------===//
struct HivePartitionsBindData : public TableFunctionData {
	HivePartitionTarget target;
	vector<LogicalType> key_types;
	vector<HivePartitionInfo> partitions;
};

struct HivePartitionsState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

unique_ptr<FunctionData> HivePartitionsBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionsBindData>();
	result->target = ResolveHiveTable(context, "hive_partitions", input.inputs[0]);
	auto &table = result->target.table;
	for (auto &key : table.partition_keys) {
		auto type = HiveTypes::ToLogicalType(key.type);
		result->key_types.push_back(type);
		names.emplace_back(key.name);
		return_types.push_back(type);
	}
	names.emplace_back("location");
	// the strings as the metastore stores them: 'abc' on an INT key is NULL above but 'abc' here
	names.emplace_back("partition_values");
	// numbered like duplicate result columns (location_1) when a partition key has the name; the keys keep their names
	auto deduplicated = names;
	QueryResult::DeduplicateColumns(deduplicated);
	for (idx_t i = table.partition_keys.size(); i < names.size(); i++) {
		names[i] = deduplicated[i];
	}
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
	auto load_partitions = [&]() {
		return make_shared_ptr<const vector<HivePartitionInfo>>(
		    result->target.catalog->Metastore().GetPartitions(context, table.schema_name, table.name));
	};
	auto &cache = *HiveTransaction::Get(context, *result->target.catalog).cache;
	result->partitions = *cache.GetPartitions(table.schema_name, table.name, load_partitions);
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> HivePartitionsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HivePartitionsState>();
}

void HivePartitionsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<HivePartitionsBindData>();
	auto &state = data.global_state->Cast<HivePartitionsState>();
	auto &keys = bind_data.target.table.partition_keys;
	idx_t count = 0;
	while (state.offset < bind_data.partitions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &partition = bind_data.partitions[state.offset++];
		for (idx_t k = 0; k < keys.size(); k++) {
			Value value =
			    k < partition.values.size()
			        ? PartitionListingValue(context, keys[k].name, partition.values[k], bind_data.key_types[k])
			        : Value(bind_data.key_types[k]);
			output.data[k].Append(value);
		}
		output.data[keys.size()].Append(Value(partition.location));
		auto &raw_column = output.data[keys.size() + 1];
		vector<Value> raw_values;
		raw_values.reserve(partition.values.size());
		for (auto &str_value : partition.values) {
			raw_values.emplace_back(str_value);
		}
		raw_column.Append(Value::LIST(ListType::GetChildType(raw_column.GetType()), std::move(raw_values)));
		count++;
	}
}

//===--------------------------------------------------------------------===//
// hive_add_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> HiveAddPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionChangeBindData>();
	result->target = ResolveHiveTable(context, "hive_add_partition", input.inputs[0]);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_add_partition");
	result->values = ParsePartitionSpec("hive_add_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "location") {
			result->location = option.second.GetValue<string>();
			StringUtil::RTrim(result->location, "/");
			if (result->location.empty()) {
				throw BinderException("hive_add_partition: 'location' must not be empty");
			}
		} else if (name == "if_not_exists") {
			result->if_not_exists = option.second.GetValue<bool>();
		}
	}
	if (result->location.empty()) {
		result->location = DefaultPartitionLocation(result->target, result->values);
	}
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void HiveAddPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HivePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	HivePartitionInput partition;
	partition.values = bind_data.values;
	partition.location = bind_data.location;
	bind_data.target.catalog->Metastore().CreatePartition(context, table, partition, bind_data.if_not_exists);
	output.data[0].Append(Value(bind_data.location));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// hive_drop_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> HiveDropPartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionChangeBindData>();
	result->target = ResolveHiveTable(context, "hive_drop_partition", input.inputs[0]);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_drop_partition");
	result->values = ParsePartitionSpec("hive_drop_partition", result->target, input.inputs[1]);
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "if_exists") {
			result->if_exists = option.second.GetValue<bool>();
		}
	}
	names = {"dropped"};
	return_types = {LogicalType::BOOLEAN};
	return std::move(result);
}

void HiveDropPartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HivePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto dropped =
	    bind_data.target.catalog->Metastore().DeletePartition(context, table.schema_name, table.name, bind_data.values);
	if (!dropped && !bind_data.if_exists) {
		throw CatalogException("Partition [%s] does not exist in %s table '%s'",
		                       StringUtil::Join(bind_data.values, ", "),
		                       bind_data.target.catalog->Metastore().ServiceName(), bind_data.target.TableName());
	}
	output.data[0].Append(Value::BOOLEAN(dropped));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// hive_rename_partition
//===--------------------------------------------------------------------===//
unique_ptr<FunctionData> HiveRenamePartitionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionChangeBindData>();
	result->target = ResolveHiveTable(context, "hive_rename_partition", input.inputs[0]);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_rename_partition");
	result->values = ParsePartitionSpec("hive_rename_partition", result->target, input.inputs[1]);
	result->new_values = ParsePartitionSpec("hive_rename_partition", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void HiveRenamePartitionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HivePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto &catalog = *bind_data.target.catalog;
	auto partition = FetchPartition(context, bind_data.target, bind_data.values);
	ThrowIfPartitionExists(context, bind_data.target, bind_data.new_values);
	auto location =
	    catalog.Metastore().RenamePartition(context, table.schema_name, table.name, partition, bind_data.new_values);
	output.data[0].Append(Value(location));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// hive_set_partition_location / hive_set_table_location
//===--------------------------------------------------------------------===//
string ParseLocation(const string &function_name, const HivePartitionTarget &target, const Value &location) {
	if (location.IsNull()) {
		throw BinderException("%s: the location must not be NULL", function_name);
	}
	auto result = location.GetValue<string>();
	StringUtil::RTrim(result, "/");
	if (result.empty()) {
		throw BinderException("%s: the location must not be empty", function_name);
	}
	target.catalog->Metastore().CheckLocation(function_name, result);
	return result;
}

unique_ptr<FunctionData> HiveSetPartitionLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionChangeBindData>();
	result->target = ResolveHiveTable(context, "hive_set_partition_location", input.inputs[0]);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_set_partition_location");
	result->values = ParsePartitionSpec("hive_set_partition_location", result->target, input.inputs[1]);
	result->location = ParseLocation("hive_set_partition_location", result->target, input.inputs[2]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void HiveSetPartitionLocationScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HivePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	auto partition = FetchPartition(context, bind_data.target, bind_data.values);
	bind_data.target.catalog->Metastore().SetPartitionLocation(context, table.schema_name, table.name, partition,
	                                                           bind_data.location);
	output.data[0].Append(Value(bind_data.location));
	output.CheckCardinality(1);
}

unique_ptr<FunctionData> HiveSetTableLocationBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HivePartitionChangeBindData>();
	result->target = ResolveHiveTable(context, "hive_set_table_location", input.inputs[0], false);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_set_table_location");
	result->location = ParseLocation("hive_set_table_location", result->target, input.inputs[1]);
	names = {"location"};
	return_types = {LogicalType::VARCHAR};
	return std::move(result);
}

void HiveSetTableLocationScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HivePartitionChangeBindData>();
	auto &table = bind_data.target.table;
	bind_data.target.catalog->Metastore().SetTableLocation(context, table, bind_data.location);
	output.data[0].Append(Value(bind_data.location));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// hive_replace_columns: replace the data columns of a Hive table
//===--------------------------------------------------------------------===//
struct HiveReplaceColumnsBindData : public TableFunctionData {
	HivePartitionTarget target;
	vector<HiveColumn> columns;

	//! The checks in bind are against the table as it is then: a prepared statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
};

//! A struct value whose fields are all strings (or NULL), e.g. {id: 'BIGINT'}
static bool IsStringStruct(const Value &value) {
	if (value.IsNull() || value.type().id() != LogicalTypeId::STRUCT) {
		return false;
	}
	for (auto &child : StructType::GetChildTypes(value.type())) {
		if (child.second.id() != LogicalTypeId::VARCHAR && child.second.id() != LogicalTypeId::SQLNULL) {
			return false;
		}
	}
	return true;
}

unique_ptr<FunctionData> HiveReplaceColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HiveReplaceColumnsBindData>();
	result->target = ResolveHiveTable(context, "hive_replace_columns", input.inputs[0], false);
	HiveCatalog::ThrowIfInExplicitTransaction(context, "hive_replace_columns");
	auto &table = result->target.table;
	auto &columns = input.inputs[1];
	if (!IsStringStruct(columns) || StructType::GetChildCount(columns.type()) == 0) {
		throw BinderException("hive_replace_columns: the columns must be a struct of column name to type, e.g. "
		                      "{id: 'BIGINT', name: 'VARCHAR'}");
	}
	optional_ptr<const Value> comments;
	// as in Hive, the comments of the columns that stay are dropped unless keep_comments is set
	bool keep_comments = false;
	for (auto &option : input.named_parameters) {
		auto name = StringUtil::Lower(option.first.GetIdentifierName());
		if (name == "comments") {
			comments = option.second;
		} else if (name == "keep_comments") {
			keep_comments = !option.second.IsNull() && option.second.GetValue<bool>();
		}
	}
	if (comments && !IsStringStruct(*comments)) {
		throw BinderException("hive_replace_columns: 'comments' must be a struct of column name to comment");
	}
	case_insensitive_map_t<idx_t> current;
	for (idx_t i = 0; i < table.columns.size(); i++) {
		current[table.columns[i].name] = i;
	}
	auto &column_names = StructType::GetChildTypes(columns.type());
	auto &column_types = StructValue::GetChildren(columns);
	// csv files are read by position, the other formats by name; other SerDes are refused
	bool by_position = table.GetFileFormat() == HiveFileFormat::CSV;
	if (by_position && column_names.size() != table.columns.size()) {
		throw BinderException("hive_replace_columns: table '%s' is stored as csv, which is read by position, so it "
		                      "must keep its %d columns",
		                      result->target.TableName(), table.columns.size());
	}
	case_insensitive_map_t<idx_t> given;
	for (idx_t i = 0; i < column_names.size(); i++) {
		HiveColumn column;
		column.name = column_names[i].first.GetIdentifierName();
		if (column_types[i].IsNull()) {
			throw BinderException("hive_replace_columns: no type given for column '%s'", column.name);
		}
		column.type =
		    HiveTypes::FromLogicalType(TransformStringToLogicalType(column_types[i].GetValue<string>(), context));
		// the type as it reads back from the metastore; a type that can not be read back fails here, before the
		// metastore is changed
		auto type = HiveTypes::ToLogicalType(column.type);
		// a column that stays must keep a type its existing files can be read with, as for ALTER COLUMN TYPE
		optional_ptr<const HiveColumn> previous;
		if (by_position) {
			previous = &table.columns[i];
		} else {
			auto existing = current.find(column.name);
			if (existing != current.end()) {
				previous = &table.columns[existing->second];
			}
		}
		if (previous) {
			// a column matched by name keeps its stored name: the json reader matches field names case-sensitively
			if (!by_position) {
				column.name = previous->name;
			}
			if (keep_comments) {
				column.comment = previous->comment;
			}
			auto from = HiveTypes::ToLogicalType(previous->type);
			if (!HiveSchemaEntry::IsAllowedHiveTypeChange(from, type)) {
				throw BinderException("hive_replace_columns: can not change column '%s' of table '%s' from %s to %s: "
				                      "existing files keep their types, only widening changes are supported",
				                      previous->name, result->target.TableName(), from.ToString(), type.ToString());
			}
		}
		for (auto &key : table.partition_keys) {
			if (StringUtil::CIEquals(key.name, column.name)) {
				throw BinderException(
				    "hive_replace_columns: '%s' is a partition key of table '%s', the columns are the "
				    "data columns only",
				    column.name, result->target.TableName());
			}
		}
		given[column.name] = result->columns.size();
		result->columns.push_back(std::move(column));
	}
	if (comments) {
		auto &comment_names = StructType::GetChildTypes(comments->type());
		auto &comment_values = StructValue::GetChildren(*comments);
		for (idx_t i = 0; i < comment_names.size(); i++) {
			auto &name = comment_names[i].first.GetIdentifierName();
			auto column = given.find(name);
			if (column == given.end()) {
				throw BinderException("hive_replace_columns: 'comments' names '%s', which is not one of the columns",
				                      name);
			}
			// NULL removes the comment
			result->columns[column->second].comment =
			    comment_values[i].IsNull() ? string() : comment_values[i].GetValue<string>();
		}
	}
	auto require_kept = [&](const string &column) {
		if (given.find(column) == given.end()) {
			throw BinderException("hive_replace_columns: table '%s' is %s, the columns must keep '%s'",
			                      result->target.TableName(), table.DescribeBucketing(), column);
		}
	};
	for (auto &column : table.bucket_columns) {
		require_kept(column);
	}
	for (auto &column : table.sort_columns) {
		require_kept(column.name);
	}
	names = {"columns"};
	return_types = {LogicalType::LIST(LogicalType::STRUCT(
	    {{"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}}))};
	return std::move(result);
}

void HiveReplaceColumnsScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HiveReplaceColumnsBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	// keeps the refreshed entry alive while its columns are read below
	Transaction::Get(context, catalog);
	catalog.Metastore().UpdateTableColumns(context, table, bind_data.columns);
	auto schema = catalog.GetSchemas().GetEntry(context, table.schema_name);
	if (!schema) {
		throw CatalogException("Table \"%s\" was altered but its %s database could not be found afterwards",
		                       bind_data.target.TableName(), catalog.Metastore().ServiceName());
	}
	auto &updated = schema->Cast<HiveSchemaEntry>().RefreshTable(context, table.name);
	vector<Value> columns;
	for (auto &column : updated.table_info.columns) {
		columns.push_back(
		    Value::STRUCT({{"name", Value(column.name)},
		                   {"type", Value(column.type)},
		                   {"comment", column.comment.empty() ? Value(LogicalType::VARCHAR) : Value(column.comment)}}));
	}
	output.data[0].Append(Value::LIST(ListType::GetChildType(output.data[0].GetType()), std::move(columns)));
	output.CheckCardinality(1);
}

//===--------------------------------------------------------------------===//
// hive_alter_table: the SQL partition DDL, several actions in one statement
//===--------------------------------------------------------------------===//
enum class HiveAlterAction : uint8_t { ADD, DROP, RENAME, SET_PARTITION_LOCATION, SET_TABLE_LOCATION };

struct HiveAlterStep {
	HiveAlterAction action;
	bool if_not_exists = false;
	bool if_exists = false;
	vector<string> values;
	vector<string> new_values;
	string location;
};

struct HiveAlterTableBindData : public TableFunctionData {
	HivePartitionTarget target;
	vector<HiveAlterStep> steps;

	//! The table is rewritten from the definition bind fetched: a prepared statement binds again on every execution
	bool SupportStatementCache() const override {
		return false;
	}
};

//! hive_alter_table reports one row per action and an ALTER TABLE may carry more actions than a DataChunk
//! holds, so the rows are collected once when the actions are applied and then paged out. Its own state
//! rather than HivePartitionChangeState, which the single-row functions share and which needs no offset.
struct HiveAlterTableState : public GlobalTableFunctionState {
	struct EmittedRow {
		Value action;
		Value partition;
		Value location;
	};
	vector<EmittedRow> rows;
	idx_t offset = 0;
	//! The metastore calls happen on the first scan only; the later ones just hand out the remaining rows
	bool applied = false;
};

unique_ptr<GlobalTableFunctionState> HiveAlterTableInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HiveAlterTableState>();
}

//! A struct field by name (case-insensitive), NULL if the struct has no such field
Value GetStructField(const Value &value, const string &field_name) {
	auto &types = StructType::GetChildTypes(value.type());
	auto &children = StructValue::GetChildren(value);
	for (idx_t i = 0; i < types.size(); i++) {
		if (StringUtil::CIEquals(types[i].first.GetIdentifierName(), field_name)) {
			return children[i];
		}
	}
	return Value();
}

//! A partition given as a list of {key, value} structs (value NULL for the default partition)
vector<string> ParsePartitionPairsValue(const string &function_name, const HivePartitionTarget &target,
                                        const Value &pairs) {
	if (pairs.IsNull() || pairs.type().id() != LogicalTypeId::LIST) {
		throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
	}
	vector<pair<string, optional<string>>> result;
	for (auto &entry : ListValue::GetChildren(pairs)) {
		if (entry.IsNull() || entry.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("%s: a partition must be given as a list of {key, value} structs", function_name);
		}
		auto key = GetStructField(entry, "key");
		auto value = GetStructField(entry, "value");
		if (key.IsNull()) {
			throw BinderException("%s: a partition entry needs a key", function_name);
		}
		optional<string> value_string;
		if (!value.IsNull()) {
			value_string = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
		}
		result.emplace_back(key.GetValue<string>(), std::move(value_string));
	}
	return ParsePartitionPairs(function_name, target, result);
}

unique_ptr<FunctionData> HiveAlterTableBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HiveAlterTableBindData>();
	auto &actions = input.inputs[1];
	if (actions.IsNull() || actions.type().id() != LogicalTypeId::LIST) {
		throw BinderException("hive_alter_table expects a list of actions");
	}
	// table-level actions do not need a partitioned table
	bool needs_partitions = false;
	for (auto &action : ListValue::GetChildren(actions)) {
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		if (name != "set_table_location") {
			needs_partitions = true;
		}
	}
	result->target = ResolveHiveTable(context, "hive_alter_table", input.inputs[0], needs_partitions);
	HiveCatalog::ThrowIfInExplicitTransaction(context);
	for (auto &action : ListValue::GetChildren(actions)) {
		if (action.IsNull() || action.type().id() != LogicalTypeId::STRUCT) {
			throw BinderException("hive_alter_table: every action must be a struct");
		}
		HiveAlterStep step;
		auto name = StringUtil::Lower(GetStructField(action, "action").ToString());
		auto if_not_exists = GetStructField(action, "if_not_exists");
		auto if_exists = GetStructField(action, "if_exists");
		auto partition = GetStructField(action, "partition");
		auto new_partition = GetStructField(action, "new_partition");
		auto location = GetStructField(action, "location");
		step.if_not_exists = !if_not_exists.IsNull() && if_not_exists.GetValue<bool>();
		step.if_exists = !if_exists.IsNull() && if_exists.GetValue<bool>();
		if (name == "add") {
			step.action = HiveAlterAction::ADD;
			step.values = ParsePartitionPairsValue("ALTER TABLE ADD PARTITION", result->target, partition);
			step.location = location.IsNull() ? DefaultPartitionLocation(result->target, step.values)
			                                  : ParseLocation("ALTER TABLE ADD PARTITION", result->target, location);
		} else if (name == "drop") {
			step.action = HiveAlterAction::DROP;
			step.values = ParsePartitionPairsValue("ALTER TABLE DROP PARTITION", result->target, partition);
		} else if (name == "rename") {
			step.action = HiveAlterAction::RENAME;
			step.values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, partition);
			step.new_values = ParsePartitionPairsValue("ALTER TABLE RENAME PARTITION", result->target, new_partition);
		} else if (name == "set_partition_location") {
			step.action = HiveAlterAction::SET_PARTITION_LOCATION;
			step.values = ParsePartitionPairsValue("ALTER TABLE PARTITION SET LOCATION", result->target, partition);
			step.location = ParseLocation("ALTER TABLE PARTITION SET LOCATION", result->target, location);
		} else if (name == "set_table_location") {
			step.action = HiveAlterAction::SET_TABLE_LOCATION;
			step.location = ParseLocation("ALTER TABLE SET LOCATION", result->target, location);
		} else {
			throw BinderException("hive_alter_table: unknown action '%s'", name);
		}
		result->steps.push_back(std::move(step));
	}
	names = {"action", "partition", "location"};
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR};
	return std::move(result);
}

string DescribeAction(HiveAlterAction action) {
	switch (action) {
	case HiveAlterAction::ADD:
		return "add partition";
	case HiveAlterAction::DROP:
		return "drop partition";
	case HiveAlterAction::RENAME:
		return "rename partition";
	case HiveAlterAction::SET_PARTITION_LOCATION:
		return "set partition location";
	case HiveAlterAction::SET_TABLE_LOCATION:
		return "set table location";
	}
	return "unknown";
}

//! Apply every action, once, and record the row each one reports. Kept separate from emitting them because
//! there can be more actions than a DataChunk holds, so the rows are handed out over several scan calls
//! while the metastore calls must happen exactly once.
void HiveAlterTableApply(ClientContext &context, TableFunctionInput &data, HiveAlterTableState &state) {
	auto &bind_data = data.bind_data->Cast<HiveAlterTableBindData>();
	auto &catalog = *bind_data.target.catalog;
	auto &table = bind_data.target.table;
	auto table_name = bind_data.target.TableName();

	// The metastore has no transactions: check everything that can fail against the catalog first, so that a statement
	// which fails half way registers or removes nothing. Which partitions exist is fetched together, each partition
	// once; the partitions a rename or a new location rewrites are fetched whole and kept from this check.
	set<vector<string>> lookups;
	for (auto &step : bind_data.steps) {
		switch (step.action) {
		case HiveAlterAction::ADD:
			if (!step.if_not_exists) {
				lookups.insert(step.values);
			}
			break;
		case HiveAlterAction::DROP:
			if (!step.if_exists) {
				lookups.insert(step.values);
			}
			break;
		case HiveAlterAction::RENAME:
			lookups.insert(step.new_values);
			break;
		case HiveAlterAction::SET_PARTITION_LOCATION:
		case HiveAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}
	auto existing = catalog.Metastore().ExistingPartitions(context, table.schema_name, table.name,
	                                                       vector<vector<string>>(lookups.begin(), lookups.end()));
	auto exists = [&](const vector<string> &values) {
		return existing.find(values) != existing.end();
	};
	vector<unique_ptr<HivePartitionDetails>> checked(bind_data.steps.size());
	for (idx_t i = 0; i < bind_data.steps.size(); i++) {
		auto &step = bind_data.steps[i];
		switch (step.action) {
		case HiveAlterAction::ADD:
			if (!step.if_not_exists && exists(step.values)) {
				throw CatalogException("Partition [%s] already exists in %s table '%s'",
				                       StringUtil::Join(step.values, ", "), catalog.Metastore().ServiceName(),
				                       table_name);
			}
			break;
		case HiveAlterAction::DROP:
			if (!step.if_exists && !exists(step.values)) {
				throw CatalogException("Partition [%s] does not exist in %s table '%s'",
				                       StringUtil::Join(step.values, ", "), catalog.Metastore().ServiceName(),
				                       table_name);
			}
			break;
		case HiveAlterAction::SET_PARTITION_LOCATION:
			if (!step.if_exists) {
				checked[i] = make_uniq<HivePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			}
			break;
		case HiveAlterAction::RENAME:
			checked[i] = make_uniq<HivePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			if (exists(step.new_values)) {
				throw CatalogException("Partition [%s] already exists in %s table '%s'",
				                       StringUtil::Join(step.new_values, ", "), catalog.Metastore().ServiceName(),
				                       table_name);
			}
			break;
		case HiveAlterAction::SET_TABLE_LOCATION:
			break;
		}
	}

	// consecutive adds go out as one BatchCreatePartition call, consecutive drops as one BatchDeletePartition call
	auto emit = [&](const HiveAlterStep &step) {
		HiveAlterTableState::EmittedRow row;
		row.action = Value(DescribeAction(step.action));
		row.partition = step.values.empty() ? Value(LogicalType::VARCHAR) : Value(StringUtil::Join(step.values, ", "));
		row.location = step.location.empty() ? Value(LogicalType::VARCHAR) : Value(step.location);
		state.rows.push_back(std::move(row));
	};
	bool table_updated = false;
	HiveTableInfo refetched_table;
	vector<HivePartitionInput> pending_adds;
	auto flush_adds = [&]() {
		if (pending_adds.empty()) {
			return;
		}
		catalog.Metastore().BatchCreatePartitions(context, table, pending_adds);
		pending_adds.clear();
	};
	vector<vector<string>> pending_drops;
	set<vector<string>> pending_drop_set;
	auto flush_drops = [&]() {
		if (pending_drops.empty()) {
			return;
		}
		catalog.Metastore().BatchDeletePartitions(context, table.schema_name, table.name, pending_drops);
		pending_drops.clear();
		pending_drop_set.clear();
	};
	for (idx_t i = 0; i < bind_data.steps.size(); i++) {
		auto &step = bind_data.steps[i];
		if (step.action != HiveAlterAction::ADD) {
			flush_adds();
		}
		if (step.action != HiveAlterAction::DROP) {
			flush_drops();
		}
		switch (step.action) {
		case HiveAlterAction::ADD: {
			HivePartitionInput partition;
			partition.values = step.values;
			partition.location = step.location;
			pending_adds.push_back(std::move(partition));
			break;
		}
		case HiveAlterAction::DROP:
			// a partition named twice is deleted once
			if (pending_drop_set.insert(step.values).second) {
				pending_drops.push_back(step.values);
			}
			break;
		case HiveAlterAction::RENAME:
			catalog.Metastore().RenamePartition(context, table.schema_name, table.name, *checked[i], step.new_values);
			break;
		case HiveAlterAction::SET_PARTITION_LOCATION: {
			// with if_exists no check ran, so the partition is fetched here
			if (!checked[i]) {
				checked[i] = make_uniq<HivePartitionDetails>(FetchPartition(context, bind_data.target, step.values));
			}
			catalog.Metastore().SetPartitionLocation(context, table.schema_name, table.name, *checked[i],
			                                         step.location);
			break;
		}
		case HiveAlterAction::SET_TABLE_LOCATION:
			// UpdateTable is refused with the VersionId of the definition an earlier step already replaced
			if (table_updated &&
			    !catalog.Metastore().GetTable(context, table.schema_name, table.name, refetched_table)) {
				throw CatalogException("%s table '%s' was dropped while it was altered",
				                       catalog.Metastore().ServiceName(), table_name);
			}
			catalog.Metastore().SetTableLocation(context, table_updated ? refetched_table : table, step.location);
			table_updated = true;
			break;
		}
		emit(step);
	}
	flush_adds();
	flush_drops();
}

void HiveAlterTableScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HiveAlterTableState>();
	if (!state.applied) {
		state.applied = true;
		HiveAlterTableApply(context, data, state);
	}
	// one row per action, so there can be more rows than a chunk holds: page them out
	idx_t count = 0;
	while (state.offset < state.rows.size() && count < STANDARD_VECTOR_SIZE) {
		auto &row = state.rows[state.offset++];
		output.data[0].Append(row.action);
		output.data[1].Append(row.partition);
		output.data[2].Append(row.location);
		count++;
	}
}

//===--------------------------------------------------------------------===//
// hive_get_partition_response
//===--------------------------------------------------------------------===//
struct HiveGetPartitionResponseBindData : public TableFunctionData {
	HivePartitionTarget target;
	HivePartitionDetails partition;
	string raw_json;
};

unique_ptr<FunctionData> HiveGetPartitionResponseBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto result = make_uniq<HiveGetPartitionResponseBindData>();
	result->target = ResolveHiveTable(context, "hive_get_partition_response", input.inputs[0]);
	auto values = ParsePartitionSpec("hive_get_partition_response", result->target, input.inputs[1]);
	auto &table = result->target.table;
	if (!result->target.catalog->Metastore().GetPartition(context, table.schema_name, table.name, values,
	                                                      result->partition, &result->raw_json)) {
		throw CatalogException("Partition [%s] does not exist in %s table '%s'", StringUtil::Join(values, ", "),
		                       result->target.catalog->Metastore().ServiceName(), result->target.TableName());
	}
	auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	names = {"database_name", "table_name", "partition_values", "location", "serde_library",
	         "columns",       "parameters", "serde_parameters", "response"};
	return_types = {LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                HiveColumnsType(),
	                map_type,
	                map_type,
	                LogicalType::VARIANT()};
	return std::move(result);
}

void HiveGetPartitionResponseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HivePartitionChangeState>();
	if (state.done) {
		return;
	}
	state.done = true;
	auto &bind_data = data.bind_data->Cast<HiveGetPartitionResponseBindData>();
	auto &table = bind_data.target.table;
	auto &partition = bind_data.partition;
	vector<Value> values;
	for (auto &value : partition.values) {
		values.emplace_back(value);
	}

	output.data[0].Append(Value(table.schema_name));
	output.data[1].Append(Value(table.name));
	output.data[2].Append(Value::LIST(LogicalType::VARCHAR, std::move(values)));
	output.data[3].Append(Value(partition.location));
	output.data[4].Append(Value(partition.serde_library));
	output.data[5].Append(HiveColumnsToValue(partition.partition_columns));
	output.data[6].Append(HiveParametersToValue(partition.parameters));
	output.data[7].Append(HiveParametersToValue(partition.serde_parameters));

	// The complete metastore partition as JSON, cast to VARIANT
	Vector json(LogicalType::JSON(), 1);
	json.SetValue(0, Value(bind_data.raw_json));
	VectorOperations::Cast(context, json, output.data[8], 1);

	output.CheckCardinality(1);
}

} // namespace

TableFunction GetHiveGetPartitionResponseFunction() {
	TableFunction function("hive_get_partition_response", {LogicalType::VARCHAR, LogicalType::ANY},
	                       HiveGetPartitionResponseScan, HiveGetPartitionResponseBind, HivePartitionChangeInit);
	return function;
}

TableFunction GetHivePartitionsFunction() {
	TableFunction function("hive_partitions", {LogicalType::VARCHAR}, HivePartitionsScan, HivePartitionsBind,
	                       HivePartitionsInit);
	return function;
}

TableFunction GetHiveAddPartitionFunction() {
	TableFunction function("hive_add_partition", {LogicalType::VARCHAR, LogicalType::ANY}, HiveAddPartitionScan,
	                       HiveAddPartitionBind, HivePartitionChangeInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("location", LogicalType::VARCHAR);
		options.Add("if_not_exists", LogicalType::BOOLEAN);
	});
	return function;
}

TableFunction GetHiveDropPartitionFunction() {
	TableFunction function("hive_drop_partition", {LogicalType::VARCHAR, LogicalType::ANY}, HiveDropPartitionScan,
	                       HiveDropPartitionBind, HivePartitionChangeInit);
	function.GetSignature().WithTypedKwargs(
	    "options", [](TypedKwargs &options) { options.Add("if_exists", LogicalType::BOOLEAN); });
	return function;
}

TableFunction GetHiveRenamePartitionFunction() {
	TableFunction function("hive_rename_partition", {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
	                       HiveRenamePartitionScan, HiveRenamePartitionBind, HivePartitionChangeInit);
	return function;
}

TableFunction GetHiveSetPartitionLocationFunction() {
	TableFunction function("hive_set_partition_location",
	                       {LogicalType::VARCHAR, LogicalType::ANY, LogicalType::VARCHAR}, HiveSetPartitionLocationScan,
	                       HiveSetPartitionLocationBind, HivePartitionChangeInit);
	return function;
}

TableFunction GetHiveSetTableLocationFunction() {
	TableFunction function("hive_set_table_location", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                       HiveSetTableLocationScan, HiveSetTableLocationBind, HivePartitionChangeInit);
	return function;
}

TableFunction GetHiveReplaceColumnsFunction() {
	TableFunction function("hive_replace_columns", {LogicalType::VARCHAR, LogicalType::ANY}, HiveReplaceColumnsScan,
	                       HiveReplaceColumnsBind, HivePartitionChangeInit);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("comments", LogicalType::ANY);
		options.Add("keep_comments", LogicalType::BOOLEAN);
	});
	return function;
}

TableFunction GetHiveAlterTableFunction() {
	TableFunction function("hive_alter_table", {LogicalType::VARCHAR, LogicalType::ANY}, HiveAlterTableScan,
	                       HiveAlterTableBind, HiveAlterTableInit);
	return function;
}

} // namespace duckdb
