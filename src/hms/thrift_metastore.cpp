#include "hms/thrift_metastore.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"

#include "hive/catalog/hive_catalog.hpp"
#include "hive/metastore/hive_metastore_json.hpp"
#include "hive/metastore/metastore_write.hpp"

namespace duckdb {

namespace {

//! Field ids of hive_metastore.thrift
namespace field_schema {
constexpr int16_t NAME = 1;
constexpr int16_t TYPE = 2;
constexpr int16_t COMMENT = 3;
} // namespace field_schema

namespace serde_info {
constexpr int16_t NAME = 1;
constexpr int16_t SERIALIZATION_LIB = 2;
constexpr int16_t PARAMETERS = 3;
} // namespace serde_info

namespace order {
constexpr int16_t COL = 1;
constexpr int16_t ORDER = 2;
} // namespace order

namespace storage_descriptor {
constexpr int16_t COLS = 1;
constexpr int16_t LOCATION = 2;
constexpr int16_t INPUT_FORMAT = 3;
constexpr int16_t OUTPUT_FORMAT = 4;
constexpr int16_t COMPRESSED = 5;
constexpr int16_t NUM_BUCKETS = 6;
constexpr int16_t SERDE_INFO = 7;
constexpr int16_t BUCKET_COLS = 8;
constexpr int16_t SORT_COLS = 9;
constexpr int16_t PARAMETERS = 10;
} // namespace storage_descriptor

namespace table {
constexpr int16_t TABLE_NAME = 1;
constexpr int16_t DB_NAME = 2;
constexpr int16_t OWNER = 3;
constexpr int16_t CREATE_TIME = 4;
constexpr int16_t LAST_ACCESS_TIME = 5;
constexpr int16_t RETENTION = 6;
constexpr int16_t SD = 7;
constexpr int16_t PARTITION_KEYS = 8;
constexpr int16_t PARAMETERS = 9;
constexpr int16_t VIEW_ORIGINAL_TEXT = 10;
constexpr int16_t VIEW_EXPANDED_TEXT = 11;
constexpr int16_t TABLE_TYPE = 12;
} // namespace table

namespace database {
constexpr int16_t NAME = 1;
constexpr int16_t DESCRIPTION = 2;
constexpr int16_t LOCATION_URI = 3;
constexpr int16_t PARAMETERS = 4;
} // namespace database

namespace partition {
constexpr int16_t VALUES = 1;
constexpr int16_t DB_NAME = 2;
constexpr int16_t TABLE_NAME = 3;
constexpr int16_t CREATE_TIME = 4;
constexpr int16_t LAST_ACCESS_TIME = 5;
constexpr int16_t SD = 6;
constexpr int16_t PARAMETERS = 7;
} // namespace partition

namespace add_partitions_request {
constexpr int16_t DB_NAME = 1;
constexpr int16_t TBL_NAME = 2;
constexpr int16_t PARTS = 3;
constexpr int16_t IF_NOT_EXISTS = 4;
constexpr int16_t NEED_RESULT = 5;
} // namespace add_partitions_request

//! The Table / Partition a projection was made from
struct ThriftFetchedTable : public HiveFetchedTable {
	explicit ThriftFetchedTable(ThriftValue raw_p) : raw(std::move(raw_p)) {
	}
	ThriftValue raw;
};

struct ThriftFetchedPartition : public HiveFetchedPartition {
	explicit ThriftFetchedPartition(ThriftValue raw_p) : raw(std::move(raw_p)) {
	}
	ThriftValue raw;
};

ThriftValue ThriftArguments() {
	return ThriftValue::Struct();
}

vector<HiveColumn> ThriftColumns(const ThriftValue &list) {
	vector<HiveColumn> result;
	for (auto &element : list.elements) {
		HiveColumn column;
		column.name = element.GetString(field_schema::NAME);
		column.type = element.GetString(field_schema::TYPE);
		column.comment = element.GetString(field_schema::COMMENT);
		result.push_back(std::move(column));
	}
	return result;
}

ThriftValue ToThriftColumns(const vector<HiveColumn> &columns) {
	auto result = ThriftValue::List(ThriftType::STRUCT);
	for (auto &column : columns) {
		auto field = ThriftValue::Struct();
		field.SetField(field_schema::NAME, ThriftValue::String(column.name));
		field.SetField(field_schema::TYPE, ThriftValue::String(column.type));
		if (!column.comment.empty()) {
			field.SetField(field_schema::COMMENT, ThriftValue::String(column.comment));
		}
		result.elements.push_back(std::move(field));
	}
	return result;
}

//! Hive keeps names in lower case
string ThriftName(const string &name) {
	return StringUtil::Lower(name);
}

HiveDatabaseInfo DatabaseFromThrift(const ThriftValue &value) {
	HiveDatabaseInfo result;
	result.name = value.GetString(database::NAME);
	result.description = value.GetString(database::DESCRIPTION);
	result.location_uri = value.GetString(database::LOCATION_URI);
	result.parameters = value.GetStringMap(database::PARAMETERS);
	return result;
}

HiveTableInfo TableFromThrift(const ThriftValue &value, const string &schema_name) {
	HiveTableInfo result;
	result.name = value.GetString(table::TABLE_NAME);
	result.schema_name = schema_name;
	result.hive_database_name = value.GetString(table::DB_NAME);
	result.hive_table_type = value.GetString(table::TABLE_TYPE);
	result.table_type = HiveTableTypeFromString(result.hive_table_type);
	result.view_original_text = value.GetString(table::VIEW_ORIGINAL_TEXT);
	result.view_expanded_text = value.GetString(table::VIEW_EXPANDED_TEXT);
	result.parameters = value.GetStringMap(table::PARAMETERS);
	auto comment = result.parameters.find("comment");
	if (comment != result.parameters.end()) {
		result.description = comment->second;
	}
	auto &sd = value.Field(table::SD);
	result.location = sd.GetString(storage_descriptor::LOCATION);
	result.input_format = sd.GetString(storage_descriptor::INPUT_FORMAT);
	result.output_format = sd.GetString(storage_descriptor::OUTPUT_FORMAT);
	auto &serde = sd.Field(storage_descriptor::SERDE_INFO);
	result.serde_library = serde.GetString(serde_info::SERIALIZATION_LIB);
	result.serde_parameters = serde.GetStringMap(serde_info::PARAMETERS);
	result.columns = ThriftColumns(sd.Field(storage_descriptor::COLS));
	result.partition_keys = ThriftColumns(value.Field(table::PARTITION_KEYS));
	result.bucket_columns = sd.GetStringList(storage_descriptor::BUCKET_COLS);
	result.number_of_buckets = static_cast<int32_t>(sd.GetInteger(storage_descriptor::NUM_BUCKETS, -1));
	for (auto &element : sd.Field(storage_descriptor::SORT_COLS).elements) {
		HiveColumn column;
		column.name = element.GetString(order::COL);
		// Hive's Order is 1 for ascending, 0 for descending
		column.sort_order =
		    element.GetInteger(order::ORDER, 1) == 0 ? HiveSortOrder::DESCENDING : HiveSortOrder::ASCENDING;
		result.sort_columns.push_back(std::move(column));
	}
	// the metastore has no version: the time of the last DDL tells a changed table apart
	auto ddl_time = result.parameters.find("transient_lastDdlTime");
	if (ddl_time != result.parameters.end()) {
		result.version_id = ddl_time->second;
	}
	result.create_time_ms = value.GetInteger(table::CREATE_TIME) * 1000;
	result.fetched = make_shared_ptr<const ThriftFetchedTable>(value);
	return result;
}

HivePartitionDetails PartitionFromThrift(const ThriftValue &value) {
	HivePartitionDetails result;
	result.values = value.GetStringList(partition::VALUES);
	auto &sd = value.Field(partition::SD);
	result.location = sd.GetString(storage_descriptor::LOCATION);
	result.input_format = sd.GetString(storage_descriptor::INPUT_FORMAT);
	auto &serde = sd.Field(storage_descriptor::SERDE_INFO);
	result.serde_library = serde.GetString(serde_info::SERIALIZATION_LIB);
	result.serde_parameters = serde.GetStringMap(serde_info::PARAMETERS);
	result.partition_columns = ThriftColumns(sd.Field(storage_descriptor::COLS));
	result.parameters = value.GetStringMap(partition::PARAMETERS);
	result.fetched = make_shared_ptr<const ThriftFetchedPartition>(value);
	return result;
}

//! The storage descriptor of a table DuckDB creates (HiveTableInfo::ForCreate)
ThriftValue StorageDescriptorValue(const HiveTableInfo &definition) {
	auto serde = ThriftValue::Struct();
	serde.SetField(serde_info::NAME, ThriftValue::String(definition.name));
	serde.SetField(serde_info::SERIALIZATION_LIB, ThriftValue::String(definition.serde_library));
	serde.SetField(serde_info::PARAMETERS, ThriftValue::StringMap(definition.serde_parameters));
	auto sd = ThriftValue::Struct();
	sd.SetField(storage_descriptor::COLS, ToThriftColumns(definition.columns));
	sd.SetField(storage_descriptor::LOCATION, ThriftValue::String(definition.location));
	sd.SetField(storage_descriptor::INPUT_FORMAT, ThriftValue::String(definition.input_format));
	sd.SetField(storage_descriptor::OUTPUT_FORMAT, ThriftValue::String(definition.output_format));
	sd.SetField(storage_descriptor::COMPRESSED, ThriftValue::Bool(false));
	sd.SetField(storage_descriptor::NUM_BUCKETS, ThriftValue::I32(definition.number_of_buckets));
	sd.SetField(storage_descriptor::SERDE_INFO, std::move(serde));
	sd.SetField(storage_descriptor::BUCKET_COLS, ThriftValue::StringList(definition.bucket_columns));
	auto sort_columns = ThriftValue::List(ThriftType::STRUCT);
	for (auto &column : definition.sort_columns) {
		auto entry = ThriftValue::Struct();
		entry.SetField(order::COL, ThriftValue::String(column.name));
		entry.SetField(order::ORDER, ThriftValue::I32(column.sort_order == HiveSortOrder::DESCENDING ? 0 : 1));
		sort_columns.elements.push_back(std::move(entry));
	}
	sd.SetField(storage_descriptor::SORT_COLS, std::move(sort_columns));
	sd.SetField(storage_descriptor::PARAMETERS, ThriftValue::StringMap({}));
	return sd;
}

ThriftValue TableValue(const HiveTableInfo &definition) {
	auto result = ThriftValue::Struct();
	result.SetField(table::TABLE_NAME, ThriftValue::String(ThriftName(definition.name)));
	result.SetField(table::DB_NAME, ThriftValue::String(ThriftName(definition.schema_name)));
	result.SetField(table::OWNER, ThriftValue::String("duckdb"));
	result.SetField(table::CREATE_TIME, ThriftValue::I32(0));
	result.SetField(table::LAST_ACCESS_TIME, ThriftValue::I32(0));
	result.SetField(table::RETENTION, ThriftValue::I32(0));
	result.SetField(table::SD, StorageDescriptorValue(definition));
	result.SetField(table::PARTITION_KEYS, ToThriftColumns(definition.partition_keys));
	result.SetField(table::PARAMETERS, ThriftValue::StringMap(definition.parameters));
	result.SetField(table::VIEW_ORIGINAL_TEXT, ThriftValue::String(definition.view_original_text));
	result.SetField(table::VIEW_EXPANDED_TEXT, ThriftValue::String(definition.view_expanded_text));
	result.SetField(table::TABLE_TYPE, ThriftValue::String(definition.hive_table_type));
	return result;
}

ThriftValue ViewValue(const HiveViewInfo &view, unordered_map<string, string> parameters) {
	HiveTableInfo definition;
	definition.name = view.name;
	definition.schema_name = view.database_name;
	definition.hive_table_type = "VIRTUAL_VIEW";
	definition.view_original_text = view.sql;
	definition.view_expanded_text = view.expanded_sql;
	definition.columns = view.columns;
	view.SetParameters(parameters);
	definition.parameters = std::move(parameters);
	return TableValue(definition);
}

const ThriftValue &FetchedTable(const HiveTableInfo &table) {
	auto fetched = dynamic_cast<const ThriftFetchedTable *>(table.fetched.get());
	if (!fetched) {
		throw InternalException("Table '%s.%s' was not fetched from the Hive metastore", table.schema_name, table.name);
	}
	return fetched->raw;
}

const ThriftValue &FetchedPartition(const HivePartitionDetails &partition) {
	auto fetched = dynamic_cast<const ThriftFetchedPartition *>(partition.fetched.get());
	if (!fetched) {
		throw InternalException("Partition [%s] was not fetched from the Hive metastore",
		                        StringUtil::Join(partition.values, ", "));
	}
	return fetched->raw;
}

ThriftValue DatabaseArguments(const string &database_name) {
	auto arguments = ThriftArguments();
	arguments.SetField(1, ThriftValue::String(ThriftName(database_name)));
	return arguments;
}

ThriftValue TableArguments(const string &database_name, const string &table_name) {
	auto arguments = DatabaseArguments(database_name);
	arguments.SetField(2, ThriftValue::String(ThriftName(table_name)));
	return arguments;
}

ThriftValue PartitionArguments(const string &database_name, const string &table_name, const vector<string> &values) {
	auto arguments = TableArguments(database_name, table_name);
	arguments.SetField(3, ThriftValue::StringList(values));
	return arguments;
}

class ThriftMetastoreHandle : public HiveMetastoreHandle {
public:
	explicit ThriftMetastoreHandle(ThriftMetastore &metastore) : metastore(metastore) {
	}

	optional_idx GetTableCreateTime(const string &database_name, const string &table_name) override {
		return metastore.GetTableCreateTime(database_name, table_name);
	}
	void DeleteTable(ClientContext &context, const string &database_name, const string &table_name) override {
		metastore.DropTable(database_name, table_name);
	}

private:
	ThriftMetastore &metastore;
};

} // namespace

//===--------------------------------------------------------------------===//
// Options
//===--------------------------------------------------------------------===//
bool ThriftMetastoreOptions::IsThriftPath(const string &path) {
	return StringUtil::StartsWith(StringUtil::Lower(path), "thrift://");
}

void ThriftMetastoreOptions::SetPath(const string &path_p) {
	path = path_p;
	auto address = path.substr(string("thrift://").size());
	StringUtil::RTrim(address, "/");
	string port_text;
	if (StringUtil::StartsWith(address, "[")) {
		// an IPv6 address: [::1]:9083
		auto close = address.find(']');
		if (close == string::npos) {
			throw InvalidConfigurationException("Invalid host in Hive metastore URI '%s'", path);
		}
		host = address.substr(1, close - 1);
		auto rest = address.substr(close + 1);
		if (!rest.empty()) {
			if (rest[0] != ':') {
				throw InvalidConfigurationException("Invalid host in Hive metastore URI '%s'", path);
			}
			port_text = rest.substr(1);
		}
	} else {
		auto colon = address.rfind(':');
		host = address.substr(0, colon);
		if (colon != string::npos) {
			port_text = address.substr(colon + 1);
		}
	}
	if (host.empty()) {
		throw InvalidConfigurationException("Hive metastore URI '%s' names no host: thrift://<host>:<port>", path);
	}
	if (!port_text.empty()) {
		std::size_t parsed = 0;
		try {
			port = std::stoi(port_text, &parsed);
		} catch (std::exception &) {
			parsed = 0;
		}
		if (parsed != port_text.size() || port <= 0 || port > 65535) {
			throw InvalidConfigurationException("Invalid port in Hive metastore URI '%s'", path);
		}
	}
}

bool ThriftMetastoreOptions::ParseOption(const string &name, const Value &value) {
	if (name == "timeout") {
		timeout_seconds = value.DefaultCastAs(LogicalType::INTEGER).GetValue<int32_t>();
		if (timeout_seconds <= 0) {
			throw InvalidConfigurationException("TIMEOUT must be a positive number of seconds");
		}
		return true;
	}
	return false;
}

//===--------------------------------------------------------------------===//
// Calls
//===--------------------------------------------------------------------===//
ThriftMetastore::ThriftMetastore(HiveCatalog &catalog, ThriftMetastoreOptions options_p)
    : HiveMetastore(catalog), options(std::move(options_p)),
      client(options.host, options.port, options.timeout_seconds) {
}

ThriftReply ThriftMetastore::Call(const string &method, const ThriftValue &arguments) {
	return client.Call(method, arguments);
}

void ThriftMetastore::Throw(const string &operation, const ThriftReply &reply) {
	throw MetastoreException(StringUtil::Format("Hive metastore %s failed: %s", operation, reply.exception_message),
	                         false);
}

void ThriftMetastore::VerifyConnection(ClientContext &context) {
	auto reply = Call("get_all_databases", ThriftArguments());
	if (reply.Failed()) {
		Throw("get_all_databases", reply);
	}
}

//===--------------------------------------------------------------------===//
// Databases
//===--------------------------------------------------------------------===//
vector<HiveDatabaseInfo> ThriftMetastore::GetDatabases(ClientContext &context) {
	auto reply = Call("get_all_databases", ThriftArguments());
	if (reply.Failed()) {
		Throw("get_all_databases", reply);
	}
	vector<HiveDatabaseInfo> result;
	for (auto &name : reply.success.elements) {
		HiveDatabaseInfo database;
		if (GetDatabase(context, name.str, database)) {
			result.push_back(std::move(database));
		}
	}
	return result;
}

bool ThriftMetastore::GetDatabase(ClientContext &context, const string &database_name, HiveDatabaseInfo &result,
                                  string *raw_json) {
	auto reply = Call("get_database", DatabaseArguments(database_name));
	// throws (1: NoSuchObjectException o1, 2: MetaException o2)
	if (reply.exception_id == 1) {
		return false;
	}
	if (reply.Failed()) {
		Throw("get_database '" + database_name + "'", reply);
	}
	result = DatabaseFromThrift(reply.success);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Database(result);
	}
	return true;
}

HiveCreateOutcome ThriftMetastore::CreateDatabase(ClientContext &context, const HiveDatabaseInfo &info) {
	MetastoreWrite write(context, catalog, "create_database");
	auto value = ThriftValue::Struct();
	value.SetField(database::NAME, ThriftValue::String(ThriftName(info.name)));
	value.SetField(database::DESCRIPTION, ThriftValue::String(info.description));
	if (!info.location_uri.empty()) {
		value.SetField(database::LOCATION_URI, ThriftValue::String(info.location_uri));
	}
	value.SetField(database::PARAMETERS, ThriftValue::StringMap(info.parameters));
	auto arguments = ThriftArguments();
	arguments.SetField(1, std::move(value));
	auto reply = Call("create_database", arguments);
	// throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2, 3: MetaException o3)
	if (reply.exception_id == 1) {
		return HiveCreateOutcome::ALREADY_EXISTED;
	}
	if (reply.Failed()) {
		Throw("create_database '" + info.name + "'", reply);
	}
	return HiveCreateOutcome::CREATED;
}

void ThriftMetastore::UpdateDatabase(ClientContext &context, const HiveDatabaseInfo &info) {
	MetastoreWrite write(context, catalog, "alter_database");
	auto current = Call("get_database", DatabaseArguments(info.name));
	if (current.Failed()) {
		Throw("get_database '" + info.name + "'", current);
	}
	auto value = current.success;
	value.SetField(database::DESCRIPTION, ThriftValue::String(info.description));
	value.SetField(database::LOCATION_URI, ThriftValue::String(info.location_uri));
	value.SetField(database::PARAMETERS, ThriftValue::StringMap(info.parameters));
	auto arguments = DatabaseArguments(info.name);
	arguments.SetField(2, std::move(value));
	auto reply = Call("alter_database", arguments);
	if (reply.Failed()) {
		Throw("alter_database '" + info.name + "'", reply);
	}
}

void ThriftMetastore::DeleteDatabase(ClientContext &context, const string &database_name) {
	MetastoreWrite write(context, catalog, "drop_database");
	auto arguments = DatabaseArguments(database_name);
	// keep the data files, drop the tables with the database
	arguments.SetField(2, ThriftValue::Bool(false));
	arguments.SetField(3, ThriftValue::Bool(true));
	auto reply = Call("drop_database", arguments);
	// throws (1: NoSuchObjectException o1, 2: InvalidOperationException o2, 3: MetaException o3)
	if (reply.exception_id == 1) {
		throw CatalogException("Hive metastore database \"%s\" does not exist", database_name);
	}
	if (reply.Failed()) {
		Throw("drop_database '" + database_name + "'", reply);
	}
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//
vector<HiveTableInfo> ThriftMetastore::GetTables(ClientContext &context, const string &database_name, bool single_page,
                                                 optional_ptr<bool> has_more, bool throw_if_denied) {
	if (has_more) {
		*has_more = false;
	}
	auto names = Call("get_all_tables", DatabaseArguments(database_name));
	if (names.Failed()) {
		// the database was dropped after it was listed: it has no tables to list
		HiveDatabaseInfo database;
		if (!GetDatabase(context, database_name, database)) {
			return {};
		}
		Throw("get_all_tables '" + database_name + "'", names);
	}
	vector<HiveTableInfo> result;
	if (names.success.elements.empty()) {
		return result;
	}
	auto list = ThriftValue::List(ThriftType::STRING);
	list.elements = names.success.elements;
	// GetTablesRequest: 1: dbName, 2: tblNames
	auto request = ThriftValue::Struct();
	request.SetField(1, ThriftValue::String(ThriftName(database_name)));
	request.SetField(2, list);
	auto arguments = ThriftArguments();
	arguments.SetField(1, std::move(request));
	ThriftReply reply;
	const ThriftValue *tables;
	try {
		reply = Call("get_table_objects_by_name_req", arguments);
		// GetTablesResult: 1: tables
		tables = &reply.success.Field(1);
	} catch (ThriftApplicationException &ex) {
		if (ex.type != ThriftApplicationException::UNKNOWN_METHOD) {
			throw;
		}
		auto legacy_arguments = DatabaseArguments(database_name);
		legacy_arguments.SetField(2, std::move(list));
		reply = Call("get_table_objects_by_name", legacy_arguments);
		tables = &reply.success;
	}
	if (reply.Failed()) {
		Throw("get_table_objects_by_name '" + database_name + "'", reply);
	}
	for (auto &table : tables->elements) {
		result.push_back(TableFromThrift(table, database_name));
	}
	return result;
}

bool ThriftMetastore::GetAnyTableName(ClientContext &context, const string &database_name, string &table_name) {
	auto names = Call("get_all_tables", DatabaseArguments(database_name));
	if (names.Failed()) {
		Throw("get_all_tables '" + database_name + "'", names);
	}
	if (names.success.elements.empty()) {
		return false;
	}
	table_name = names.success.elements[0].str;
	return true;
}

bool ThriftMetastore::FetchTable(const string &database_name, const string &table_name, ThriftValue &result) {
	// GetTableRequest: 1: dbName, 2: tblName; get_table, its predecessor, is gone since Hive 4
	auto request = ThriftValue::Struct();
	request.SetField(1, ThriftValue::String(ThriftName(database_name)));
	request.SetField(2, ThriftValue::String(ThriftName(table_name)));
	auto arguments = ThriftArguments();
	arguments.SetField(1, std::move(request));
	ThriftReply reply;
	bool legacy = false;
	try {
		reply = Call("get_table_req", arguments);
	} catch (ThriftApplicationException &ex) {
		if (ex.type != ThriftApplicationException::UNKNOWN_METHOD) {
			throw;
		}
		legacy = true;
		reply = Call("get_table", TableArguments(database_name, table_name));
	}
	// throws (1: MetaException o1, 2: NoSuchObjectException o2)
	if (reply.exception_id == 2) {
		return false;
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("get_table '%s.%s'", database_name, table_name), reply);
	}
	// GetTableResult: 1: table
	result = legacy ? std::move(reply.success) : reply.success.Field(1);
	return true;
}

bool ThriftMetastore::GetTable(ClientContext &context, const string &database_name, const string &table_name,
                               HiveTableInfo &result, string *raw_json) {
	ThriftValue table;
	if (!FetchTable(database_name, table_name, table)) {
		return false;
	}
	result = TableFromThrift(table, database_name);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Table(result);
	}
	return true;
}

optional_idx ThriftMetastore::GetTableCreateTime(const string &database_name, const string &table_name) {
	ThriftValue table;
	if (!FetchTable(database_name, table_name, table)) {
		return optional_idx();
	}
	return static_cast<idx_t>(table.GetInteger(table::CREATE_TIME)) * 1000;
}

HiveCreateOutcome ThriftMetastore::CreateHiveTable(ClientContext &context, const HiveTableInfo &info) {
	MetastoreWrite write(context, catalog, "create_table");
	if (info.location.empty()) {
		throw InvalidInputException("Can not create Hive table '%s.%s' without a location", info.schema_name,
		                            info.name);
	}
	auto arguments = ThriftArguments();
	arguments.SetField(1, TableValue(info.ForCreate()));
	auto reply = Call("create_table", arguments);
	// throws (1: AlreadyExistsException o1, 2: InvalidObjectException o2, 3: MetaException o3,
	// 4: NoSuchObjectException o4)
	if (reply.exception_id == 1) {
		return HiveCreateOutcome::ALREADY_EXISTED;
	}
	if (reply.exception_id == 4) {
		throw CatalogException("Hive metastore database \"%s\" does not exist", info.schema_name);
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("create_table '%s.%s' (location '%s')", info.schema_name, info.name, info.location),
		      reply);
	}
	return HiveCreateOutcome::CREATED;
}

HiveCreateOutcome ThriftMetastore::CreateView(ClientContext &context, const HiveViewInfo &view) {
	MetastoreWrite write(context, catalog, "create_table");
	auto arguments = ThriftArguments();
	arguments.SetField(1, ViewValue(view, {}));
	auto reply = Call("create_table", arguments);
	if (reply.exception_id == 1) {
		return HiveCreateOutcome::ALREADY_EXISTED;
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("create_table '%s.%s' (view)", view.database_name, view.name), reply);
	}
	return HiveCreateOutcome::CREATED;
}

void ThriftMetastore::AlterTable(const HiveTableInfo &bound, const std::function<void(ThriftValue &table)> &modify) {
	auto table = FetchedTable(bound);
	modify(table);
	auto arguments = TableArguments(bound.schema_name, bound.name);
	arguments.SetField(3, std::move(table));
	auto reply = Call("alter_table", arguments);
	if (reply.Failed()) {
		Throw(StringUtil::Format("alter_table '%s.%s'", bound.schema_name, bound.name), reply);
	}
}

void ThriftMetastore::UpdateView(ClientContext &context, const HiveViewInfo &view, const HiveTableInfo &bound) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound, [&](ThriftValue &table) {
		auto replaced = ViewValue(view, table.GetStringMap(table::PARAMETERS));
		for (auto id :
		     {table::SD, table::PARAMETERS, table::VIEW_ORIGINAL_TEXT, table::VIEW_EXPANDED_TEXT, table::TABLE_TYPE}) {
			table.SetField(id, replaced.Field(id));
		}
	});
}

void ThriftMetastore::UpdateTableColumns(ClientContext &context, const HiveTableInfo &bound,
                                         const vector<HiveColumn> &columns) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound, [&](ThriftValue &table) {
		auto &sd = table.fields[table::SD];
		sd.SetField(storage_descriptor::COLS, ToThriftColumns(columns));
	});
}

void ThriftMetastore::UpdateTablePartitionKeys(ClientContext &context, const HiveTableInfo &bound,
                                               const vector<HiveColumn> &partition_keys) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound,
	           [&](ThriftValue &table) { table.SetField(table::PARTITION_KEYS, ToThriftColumns(partition_keys)); });
}

void ThriftMetastore::UpdateTableDescription(ClientContext &context, const HiveTableInfo &bound,
                                             const string &description) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound, [&](ThriftValue &table) {
		auto parameters = table.GetStringMap(table::PARAMETERS);
		if (description.empty()) {
			parameters.erase("comment");
		} else {
			parameters["comment"] = description;
		}
		table.SetField(table::PARAMETERS, ThriftValue::StringMap(parameters));
	});
}

void ThriftMetastore::SetTableLocation(ClientContext &context, const HiveTableInfo &bound, const string &location) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound, [&](ThriftValue &table) {
		auto &sd = table.fields[table::SD];
		sd.SetField(storage_descriptor::LOCATION, ThriftValue::String(location));
	});
}

void ThriftMetastore::UpdateTableParameters(ClientContext &context, const HiveTableInfo &bound,
                                            const vector<pair<string, string>> &set, const vector<string> &unset) {
	MetastoreWrite write(context, catalog, "alter_table");
	AlterTable(bound, [&](ThriftValue &table) {
		auto parameters = table.GetStringMap(table::PARAMETERS);
		for (auto &key : unset) {
			parameters.erase(key);
		}
		for (auto &entry : set) {
			parameters[entry.first] = entry.second;
		}
		table.SetField(table::PARAMETERS, ThriftValue::StringMap(parameters));
	});
}

void ThriftMetastore::DropTable(const string &database_name, const string &table_name) {
	auto arguments = TableArguments(database_name, table_name);
	// keep the data files
	arguments.SetField(3, ThriftValue::Bool(false));
	auto reply = Call("drop_table", arguments);
	// throws (1: NoSuchObjectException o1, 2: MetaException o3)
	if (reply.exception_id == 1) {
		throw CatalogException("Table with name \"%s\" does not exist in Hive metastore database \"%s\"", table_name,
		                       database_name);
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("drop_table '%s.%s'", database_name, table_name), reply);
	}
}

void ThriftMetastore::DeleteTable(ClientContext &context, const string &database_name, const string &table_name) {
	MetastoreWrite write(context, catalog, "drop_table");
	DropTable(database_name, table_name);
}

unique_ptr<HiveMetastoreHandle> ThriftMetastore::GetHandle(ClientContext &context) {
	return make_uniq<ThriftMetastoreHandle>(*this);
}

//===--------------------------------------------------------------------===//
// Partitions
//===--------------------------------------------------------------------===//
bool ThriftMetastore::GetPartition(ClientContext &context, const string &database_name, const string &table_name,
                                   const vector<string> &values, HivePartitionDetails &result, string *raw_json) {
	auto reply = Call("get_partition", PartitionArguments(database_name, table_name, values));
	// throws (1: MetaException o1, 2: NoSuchObjectException o2)
	if (reply.exception_id == 2) {
		return false;
	}
	if (reply.Failed()) {
		Throw(
		    StringUtil::Format("get_partition '%s.%s' [%s]", database_name, table_name, StringUtil::Join(values, ", ")),
		    reply);
	}
	result = PartitionFromThrift(reply.success);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Partition(database_name, table_name, result);
	}
	return true;
}

vector<HivePartitionInfo> ThriftMetastore::GetPartitions(ClientContext &context, const string &database_name,
                                                         const string &table_name) {
	auto arguments = TableArguments(database_name, table_name);
	arguments.SetField(3, ThriftValue::I16(-1));
	auto reply = Call("get_partitions", arguments);
	// throws (1: NoSuchObjectException o1, 2: MetaException o2)
	if (reply.Failed()) {
		Throw(StringUtil::Format("get_partitions '%s.%s'", database_name, table_name), reply);
	}
	vector<HivePartitionInfo> result;
	for (auto &element : reply.success.elements) {
		result.push_back(PartitionFromThrift(element));
	}
	return result;
}

ThriftValue ThriftMetastore::NewPartition(const HiveTableInfo &table, const HivePartitionInput &input) {
	if (input.values.size() != table.partition_keys.size()) {
		throw InvalidInputException("Partition [%s] of Hive table '%s.%s' has %llu values, the table has %llu "
		                            "partition keys",
		                            StringUtil::Join(input.values, ", "), table.schema_name, table.name,
		                            input.values.size(), table.partition_keys.size());
	}
	auto fetched = dynamic_cast<const ThriftFetchedTable *>(table.fetched.get());
	auto sd = fetched ? fetched->raw.Field(table::SD) : StorageDescriptorValue(table);
	sd.SetField(storage_descriptor::LOCATION, ThriftValue::String(input.location));
	auto result = ThriftValue::Struct();
	result.SetField(partition::VALUES, ThriftValue::StringList(input.values));
	result.SetField(partition::DB_NAME, ThriftValue::String(ThriftName(table.schema_name)));
	result.SetField(partition::TABLE_NAME, ThriftValue::String(ThriftName(table.name)));
	result.SetField(partition::CREATE_TIME, ThriftValue::I32(0));
	result.SetField(partition::LAST_ACCESS_TIME, ThriftValue::I32(0));
	result.SetField(partition::SD, std::move(sd));
	result.SetField(partition::PARAMETERS, ThriftValue::StringMap({}));
	return result;
}

bool ThriftMetastore::CreatePartition(ClientContext &context, const HiveTableInfo &table,
                                      const HivePartitionInput &input, bool if_not_exists) {
	MetastoreWrite write(context, catalog, "add_partition");
	auto arguments = ThriftArguments();
	arguments.SetField(1, NewPartition(table, input));
	auto reply = Call("add_partition", arguments);
	// throws (1: InvalidObjectException o1, 2: AlreadyExistsException o2, 3: MetaException o3)
	if (reply.exception_id == 2) {
		if (if_not_exists) {
			return false;
		}
		throw CatalogException("Partition [%s] of Hive table '%s.%s' already exists",
		                       StringUtil::Join(input.values, ", "), table.schema_name, table.name);
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("add_partition '%s.%s' [%s]", table.schema_name, table.name,
		                         StringUtil::Join(input.values, ", ")),
		      reply);
	}
	return true;
}

vector<HivePartitionInput> ThriftMetastore::BatchCreatePartitions(ClientContext &context, const HiveTableInfo &table,
                                                                  const vector<HivePartitionInput> &partitions) {
	MetastoreWrite write(context, catalog, "add_partitions_req");
	vector<HivePartitionInput> existing;
	if (partitions.empty()) {
		return existing;
	}
	auto request = ThriftValue::Struct();
	request.SetField(add_partitions_request::DB_NAME, ThriftValue::String(ThriftName(table.schema_name)));
	request.SetField(add_partitions_request::TBL_NAME, ThriftValue::String(ThriftName(table.name)));
	auto parts = ThriftValue::List(ThriftType::STRUCT);
	for (auto &input : partitions) {
		parts.elements.push_back(NewPartition(table, input));
	}
	request.SetField(add_partitions_request::PARTS, std::move(parts));
	request.SetField(add_partitions_request::IF_NOT_EXISTS, ThriftValue::Bool(true));
	request.SetField(add_partitions_request::NEED_RESULT, ThriftValue::Bool(true));
	auto arguments = ThriftArguments();
	arguments.SetField(1, std::move(request));
	auto reply = Call("add_partitions_req", arguments);
	if (reply.Failed()) {
		Throw(StringUtil::Format("add_partitions_req '%s.%s'", table.schema_name, table.name), reply);
	}
	// the result lists the partitions that were added: the others existed
	set<vector<string>> added;
	for (auto &element : reply.success.Field(1).elements) {
		added.insert(element.GetStringList(partition::VALUES));
	}
	for (auto &input : partitions) {
		if (!added.count(input.values)) {
			existing.push_back(input);
		}
	}
	return existing;
}

bool ThriftMetastore::DeletePartition(ClientContext &context, const string &database_name, const string &table_name,
                                      const vector<string> &values) {
	MetastoreWrite write(context, catalog, "drop_partition");
	auto arguments = PartitionArguments(database_name, table_name, values);
	arguments.SetField(4, ThriftValue::Bool(false));
	auto reply = Call("drop_partition", arguments);
	// throws (1: NoSuchObjectException o1, 2: MetaException o2)
	if (reply.exception_id == 1) {
		return false;
	}
	if (reply.Failed()) {
		Throw(StringUtil::Format("drop_partition '%s.%s' [%s]", database_name, table_name,
		                         StringUtil::Join(values, ", ")),
		      reply);
	}
	return true;
}

void ThriftMetastore::BatchDeletePartitions(ClientContext &context, const string &database_name,
                                            const string &table_name, const vector<vector<string>> &partitions) {
	for (auto &values : partitions) {
		DeletePartition(context, database_name, table_name, values);
	}
}

map<vector<string>, string> ThriftMetastore::PartitionLocations(ClientContext &context, const string &database_name,
                                                                const string &table_name,
                                                                const vector<vector<string>> &partitions) {
	map<vector<string>, string> result;
	for (auto &values : partitions) {
		HivePartitionDetails partition;
		if (GetPartition(context, database_name, table_name, values, partition)) {
			result[values] = partition.location;
		}
	}
	return result;
}

void ThriftMetastore::SetPartitionLocation(ClientContext &context, const string &database_name,
                                           const string &table_name, const HivePartitionDetails &partition,
                                           const string &location) {
	MetastoreWrite write(context, catalog, "alter_partition");
	auto value = FetchedPartition(partition);
	value.fields[partition::SD].SetField(storage_descriptor::LOCATION, ThriftValue::String(location));
	auto arguments = TableArguments(database_name, table_name);
	arguments.SetField(3, std::move(value));
	auto reply = Call("alter_partition", arguments);
	if (reply.Failed()) {
		Throw(StringUtil::Format("alter_partition '%s.%s' [%s]", database_name, table_name,
		                         StringUtil::Join(partition.values, ", ")),
		      reply);
	}
}

string ThriftMetastore::RenamePartition(ClientContext &context, const string &database_name, const string &table_name,
                                        const HivePartitionDetails &partition, const vector<string> &new_values) {
	MetastoreWrite write(context, catalog, "rename_partition");
	auto value = FetchedPartition(partition);
	value.SetField(partition::VALUES, ThriftValue::StringList(new_values));
	auto arguments = PartitionArguments(database_name, table_name, partition.values);
	arguments.SetField(4, std::move(value));
	auto reply = Call("rename_partition", arguments);
	if (reply.Failed()) {
		Throw(StringUtil::Format("rename_partition '%s.%s' [%s]", database_name, table_name,
		                         StringUtil::Join(partition.values, ", ")),
		      reply);
	}
	return partition.location;
}

} // namespace duckdb
