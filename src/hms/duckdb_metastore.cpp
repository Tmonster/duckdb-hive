#include "hms/duckdb_metastore.hpp"

#include "duckdb/common/local_file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/query_result.hpp"

#include "hive/catalog/hive_catalog.hpp"
#include "hive/core/hive_table_format.hpp"
#include "hive/metastore/hive_metastore_json.hpp"
#include "hive/metastore/metastore_write.hpp"

#include <chrono>
#include <cstring>

namespace duckdb {

namespace {

const char *const SCHEMA_SQL = R"(
CREATE TABLE IF NOT EXISTS dbs (
	name VARCHAR PRIMARY KEY,
	description VARCHAR,
	location_uri VARCHAR,
	parameters MAP(VARCHAR, VARCHAR),
	create_time BIGINT
);
CREATE TABLE IF NOT EXISTS tbls (
	db_name VARCHAR,
	tbl_name VARCHAR,
	tbl_type VARCHAR,
	description VARCHAR,
	create_time BIGINT,
	version BIGINT,
	view_original_text VARCHAR,
	view_expanded_text VARCHAR,
	parameters MAP(VARCHAR, VARCHAR),
	location VARCHAR,
	input_format VARCHAR,
	output_format VARCHAR,
	serde_lib VARCHAR,
	serde_params MAP(VARCHAR, VARCHAR),
	cols STRUCT(name VARCHAR, type VARCHAR, comment VARCHAR)[],
	partition_keys STRUCT(name VARCHAR, type VARCHAR, comment VARCHAR)[],
	bucket_cols VARCHAR[],
	num_buckets INTEGER,
	sort_cols STRUCT(name VARCHAR, ascending BOOLEAN)[],
	PRIMARY KEY (db_name, tbl_name)
);
CREATE TABLE IF NOT EXISTS partitions (
	db_name VARCHAR,
	tbl_name VARCHAR,
	part_key VARCHAR,
	part_values VARCHAR[],
	create_time BIGINT,
	parameters MAP(VARCHAR, VARCHAR),
	location VARCHAR,
	input_format VARCHAR,
	output_format VARCHAR,
	serde_lib VARCHAR,
	serde_params MAP(VARCHAR, VARCHAR),
	cols STRUCT(name VARCHAR, type VARCHAR, comment VARCHAR)[],
	PRIMARY KEY (db_name, tbl_name, part_key)
);
)";

const char *const TABLE_COLUMNS =
    "tbl_name, db_name, tbl_type, description, create_time, version, view_original_text, view_expanded_text, "
    "parameters, location, input_format, output_format, serde_lib, serde_params, cols, partition_keys, bucket_cols, "
    "num_buckets, sort_cols";

const char *const PARTITION_COLUMNS = "part_values, location, input_format, serde_lib, serde_params, cols, parameters";

int64_t NowMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
	    .count();
}

LogicalType ColumnType() {
	child_list_t<LogicalType> children {
	    {"name", LogicalType::VARCHAR}, {"type", LogicalType::VARCHAR}, {"comment", LogicalType::VARCHAR}};
	return LogicalType::STRUCT(std::move(children));
}

LogicalType SortColumnType() {
	child_list_t<LogicalType> children {{"name", LogicalType::VARCHAR}, {"ascending", LogicalType::BOOLEAN}};
	return LogicalType::STRUCT(std::move(children));
}

Value ParametersValue(const unordered_map<string, string> &map) {
	vector<Value> keys;
	vector<Value> values;
	for (auto &entry : map) {
		keys.emplace_back(entry.first);
		values.emplace_back(entry.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

Value StringsValue(const vector<string> &strings) {
	vector<Value> values;
	for (auto &value : strings) {
		values.emplace_back(value);
	}
	return Value::LIST(LogicalType::VARCHAR, std::move(values));
}

Value ColumnsValue(const vector<HiveColumn> &columns) {
	vector<Value> values;
	for (auto &column : columns) {
		child_list_t<Value> children {
		    {"name", Value(column.name)}, {"type", Value(column.type)}, {"comment", Value(column.comment)}};
		values.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::LIST(ColumnType(), std::move(values));
}

Value SortColumnsValue(const vector<HiveColumn> &columns) {
	vector<Value> values;
	for (auto &column : columns) {
		child_list_t<Value> children {{"name", Value(column.name)},
		                              {"ascending", Value::BOOLEAN(column.sort_order != HiveSortOrder::DESCENDING)}};
		values.push_back(Value::STRUCT(std::move(children)));
	}
	return Value::LIST(SortColumnType(), std::move(values));
}

string StringOf(const Value &value) {
	return value.IsNull() ? string() : value.GetValue<string>();
}

unordered_map<string, string> MapOf(const Value &value) {
	unordered_map<string, string> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : MapValue::GetChildren(value)) {
		auto &key_value = StructValue::GetChildren(entry);
		result[StringOf(key_value[0])] = StringOf(key_value[1]);
	}
	return result;
}

vector<string> StringsOf(const Value &value) {
	vector<string> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : ListValue::GetChildren(value)) {
		result.push_back(StringOf(entry));
	}
	return result;
}

vector<HiveColumn> ColumnsOf(const Value &value) {
	vector<HiveColumn> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : ListValue::GetChildren(value)) {
		auto &fields = StructValue::GetChildren(entry);
		HiveColumn column;
		column.name = StringOf(fields[0]);
		column.type = StringOf(fields[1]);
		column.comment = StringOf(fields[2]);
		result.push_back(std::move(column));
	}
	return result;
}

vector<HiveColumn> SortColumnsOf(const Value &value) {
	vector<HiveColumn> result;
	if (value.IsNull()) {
		return result;
	}
	for (auto &entry : ListValue::GetChildren(value)) {
		auto &fields = StructValue::GetChildren(entry);
		HiveColumn column;
		column.name = StringOf(fields[0]);
		column.sort_order =
		    !fields[1].IsNull() && !fields[1].GetValue<bool>() ? HiveSortOrder::DESCENDING : HiveSortOrder::ASCENDING;
		result.push_back(std::move(column));
	}
	return result;
}

//! The key of a partition in the partitions table: its values in order
string PartitionKey(const vector<string> &values) {
	return StringUtil::Join(values, "\x1f");
}

string Lower(const string &name) {
	return StringUtil::Lower(name);
}

//! An existing file must be a DuckDB database: its header has the magic bytes "DUCK" at offset 8
void CheckDatabaseFile(const string &path) {
	LocalFileSystem fs;
	if (!fs.FileExists(path)) {
		return;
	}
	auto handle = fs.OpenFile(path, FileFlags::FILE_FLAGS_READ);
	char header[12];
	auto read = fs.Read(*handle, header, sizeof(header));
	if (read != static_cast<int64_t>(sizeof(header)) || memcmp(header + 8, "DUCK", 4) != 0) {
		throw IOException("Could not open the Hive metastore database '%s': it is not a DuckDB database file", path);
	}
}

class DuckDBMetastoreHandle : public HiveMetastoreHandle {
public:
	explicit DuckDBMetastoreHandle(DuckDBMetastore &metastore) : metastore(metastore) {
	}

	optional_idx GetTableCreateTime(const string &database_name, const string &table_name) override {
		return metastore.GetTableCreateTime(database_name, table_name);
	}
	void DeleteTable(ClientContext &context, const string &database_name, const string &table_name) override {
		metastore.DeleteTableRow(database_name, table_name);
	}

private:
	DuckDBMetastore &metastore;
};

} // namespace

DuckDBMetastore::DuckDBMetastore(HiveCatalog &catalog, string path_p)
    : HiveMetastore(catalog), path(std::move(path_p)) {
}

DuckDBMetastore::~DuckDBMetastore() {
}

void DuckDBMetastore::Open() {
	if (connection) {
		return;
	}
	DBConfig config;
	config.SetOptionByName("threads", Value::BIGINT(1));
	config.SetOptionByName("autoload_known_extensions", Value::BOOLEAN(false));
	config.SetOptionByName("autoinstall_known_extensions", Value::BOOLEAN(false));
	if (catalog.access_mode == AccessMode::READ_ONLY && path != ":memory:") {
		config.options.access_mode = AccessMode::READ_ONLY;
	}
	if (path != ":memory:") {
		CheckDatabaseFile(path);
	}
	try {
		database = make_uniq<DuckDB>(path == ":memory:" ? nullptr : path.c_str(), &config);
	} catch (std::exception &ex) {
		throw IOException("Could not open the Hive metastore database '%s': %s", path, ErrorData(ex).RawMessage());
	}
	connection = make_uniq<Connection>(*database);
	if (catalog.access_mode != AccessMode::READ_ONLY || path == ":memory:") {
		auto result = connection->Query(SCHEMA_SQL);
		if (result->HasError()) {
			result->ThrowError("Could not create the Hive metastore tables: ");
		}
	}
}

unique_ptr<QueryResult> DuckDBMetastore::Run(const string &sql, vector<Value> values) {
	Open();
	auto statement = connection->Prepare(sql);
	if (statement->HasError()) {
		throw IOException("Hive metastore query failed: %s", statement->GetError());
	}
	auto result = statement->Execute(values);
	if (result->HasError()) {
		if (result->GetErrorType() == ExceptionType::CONSTRAINT) {
			throw ConstraintException(result->GetError());
		}
		throw IOException("Hive metastore query failed: %s", result->GetError());
	}
	return result;
}

vector<vector<Value>> DuckDBMetastore::Rows(const string &sql, vector<Value> values) {
	auto result = Run(sql, std::move(values));
	vector<vector<Value>> rows;
	while (auto chunk = result->Fetch()) {
		for (idx_t row = 0; row < chunk->size(); row++) {
			vector<Value> row_values;
			for (idx_t column = 0; column < chunk->ColumnCount(); column++) {
				row_values.push_back(chunk->GetValue(column, row));
			}
			rows.push_back(std::move(row_values));
		}
	}
	return rows;
}

idx_t DuckDBMetastore::Changed(const string &sql, vector<Value> values) {
	auto rows = Rows(sql, std::move(values));
	return rows.empty() ? 0 : rows[0][0].GetValue<idx_t>();
}

void DuckDBMetastore::VerifyConnection(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	Open();
}

//===--------------------------------------------------------------------===//
// Databases
//===--------------------------------------------------------------------===//
static HiveDatabaseInfo DatabaseFromRow(const vector<Value> &row) {
	HiveDatabaseInfo database;
	database.name = StringOf(row[0]);
	database.description = StringOf(row[1]);
	database.location_uri = StringOf(row[2]);
	database.parameters = MapOf(row[3]);
	return database;
}

vector<HiveDatabaseInfo> DuckDBMetastore::GetDatabases(ClientContext &context) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	vector<HiveDatabaseInfo> result;
	for (auto &row : Rows("SELECT name, description, location_uri, parameters FROM dbs ORDER BY name", {})) {
		result.push_back(DatabaseFromRow(row));
	}
	return result;
}

bool DuckDBMetastore::GetDatabase(ClientContext &context, const string &database_name, HiveDatabaseInfo &result,
                                  string *raw_json) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto rows = Rows("SELECT name, description, location_uri, parameters FROM dbs WHERE name = $1",
	                 {Value(Lower(database_name))});
	if (rows.empty()) {
		return false;
	}
	result = DatabaseFromRow(rows[0]);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Database(result);
	}
	return true;
}

HiveCreateOutcome DuckDBMetastore::CreateDatabase(ClientContext &context, const HiveDatabaseInfo &database) {
	MetastoreWrite write(context, catalog, "CreateDatabase");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto inserted = Changed("INSERT INTO dbs VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
	                        {Value(Lower(database.name)), Value(database.description), Value(database.location_uri),
	                         ParametersValue(database.parameters), Value::BIGINT(NowMs())});
	return inserted ? HiveCreateOutcome::CREATED : HiveCreateOutcome::ALREADY_EXISTED;
}

void DuckDBMetastore::UpdateDatabase(ClientContext &context, const HiveDatabaseInfo &database) {
	MetastoreWrite write(context, catalog, "UpdateDatabase");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto updated = Changed("UPDATE dbs SET description = $2, location_uri = $3, parameters = $4 WHERE name = $1",
	                       {Value(Lower(database.name)), Value(database.description), Value(database.location_uri),
	                        ParametersValue(database.parameters)});
	if (!updated) {
		throw CatalogException("Hive metastore database \"%s\" does not exist", database.name);
	}
}

void DuckDBMetastore::DeleteDatabase(ClientContext &context, const string &database_name) {
	MetastoreWrite write(context, catalog, "DeleteDatabase");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto name = Value(Lower(database_name));
	if (!Changed("DELETE FROM dbs WHERE name = $1", {name})) {
		throw CatalogException("Hive metastore database \"%s\" does not exist", database_name);
	}
	Run("DELETE FROM partitions WHERE db_name = $1", {name});
	Run("DELETE FROM tbls WHERE db_name = $1", {name});
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//
HiveTableInfo DuckDBMetastore::TableFromRow(const vector<Value> &row) const {
	HiveTableInfo table;
	table.name = StringOf(row[0]);
	table.schema_name = StringOf(row[1]);
	table.hive_database_name = table.schema_name;
	table.hive_table_type = StringOf(row[2]);
	table.table_type = HiveTableTypeFromString(table.hive_table_type);
	table.description = StringOf(row[3]);
	table.create_time_ms = row[4].IsNull() ? 0 : row[4].GetValue<int64_t>();
	table.version_id = StringOf(row[5]);
	table.view_original_text = StringOf(row[6]);
	table.view_expanded_text = StringOf(row[7]);
	table.parameters = MapOf(row[8]);
	table.location = StringOf(row[9]);
	table.input_format = StringOf(row[10]);
	table.output_format = StringOf(row[11]);
	table.serde_library = StringOf(row[12]);
	table.serde_parameters = MapOf(row[13]);
	table.columns = ColumnsOf(row[14]);
	table.partition_keys = ColumnsOf(row[15]);
	table.bucket_columns = StringsOf(row[16]);
	table.number_of_buckets = row[17].IsNull() ? -1 : row[17].GetValue<int32_t>();
	table.sort_columns = SortColumnsOf(row[18]);
	return table;
}

vector<HiveTableInfo> DuckDBMetastore::GetTables(ClientContext &context, const string &database_name, bool single_page,
                                                 optional_ptr<bool> has_more, bool throw_if_denied) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (has_more) {
		*has_more = false;
	}
	vector<HiveTableInfo> result;
	for (auto &row : Rows(StringUtil::Format("SELECT %s FROM tbls WHERE db_name = $1 ORDER BY tbl_name", TABLE_COLUMNS),
	                      {Value(Lower(database_name))})) {
		result.push_back(TableFromRow(row));
	}
	return result;
}

bool DuckDBMetastore::GetAnyTableName(ClientContext &context, const string &database_name, string &table_name) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto rows =
	    Rows("SELECT tbl_name FROM tbls WHERE db_name = $1 ORDER BY tbl_name LIMIT 1", {Value(Lower(database_name))});
	if (rows.empty()) {
		return false;
	}
	table_name = StringOf(rows[0][0]);
	return true;
}

bool DuckDBMetastore::GetTable(ClientContext &context, const string &database_name, const string &table_name,
                               HiveTableInfo &result, string *raw_json) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto rows = Rows(StringUtil::Format("SELECT %s FROM tbls WHERE db_name = $1 AND tbl_name = $2", TABLE_COLUMNS),
	                 {Value(Lower(database_name)), Value(Lower(table_name))});
	if (rows.empty()) {
		return false;
	}
	result = TableFromRow(rows[0]);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Table(result);
	}
	return true;
}

bool DuckDBMetastore::InsertTable(const HiveTableInfo &table) {
	if (Rows("SELECT 1 FROM dbs WHERE name = $1", {Value(Lower(table.schema_name))}).empty()) {
		throw CatalogException("Hive metastore database \"%s\" does not exist", table.schema_name);
	}
	auto inserted = Changed(
	    StringUtil::Format("INSERT INTO tbls (%s) VALUES ($1, $2, $3, $4, $5, 1, $6, $7, $8, $9, $10, $11, $12, $13, "
	                       "$14, $15, $16, $17, $18) ON CONFLICT DO NOTHING",
	                       TABLE_COLUMNS),
	    {Value(Lower(table.name)), Value(Lower(table.schema_name)), Value(table.hive_table_type),
	     Value(table.description), Value::BIGINT(NowMs()), Value(table.view_original_text),
	     Value(table.view_expanded_text), ParametersValue(table.parameters), Value(table.location),
	     Value(table.input_format), Value(table.output_format), Value(table.serde_library),
	     ParametersValue(table.serde_parameters), ColumnsValue(table.columns), ColumnsValue(table.partition_keys),
	     StringsValue(table.bucket_columns), Value::INTEGER(table.number_of_buckets),
	     SortColumnsValue(table.sort_columns)});
	return inserted > 0;
}

void DuckDBMetastore::ReplaceTable(const HiveTableInfo &bound, const HiveTableInfo &table) {
	auto updated = Changed(
	    "UPDATE tbls SET version = version + 1, tbl_type = $4, description = $5, view_original_text = $6, "
	    "view_expanded_text = $7, parameters = $8, location = $9, input_format = $10, output_format = $11, "
	    "serde_lib = $12, serde_params = $13, cols = $14, partition_keys = $15, bucket_cols = $16, num_buckets = $17, "
	    "sort_cols = $18 WHERE db_name = $1 AND tbl_name = $2 AND CAST(version AS VARCHAR) = $3",
	    {Value(Lower(bound.schema_name)), Value(Lower(bound.name)), Value(bound.version_id),
	     Value(table.hive_table_type), Value(table.description), Value(table.view_original_text),
	     Value(table.view_expanded_text), ParametersValue(table.parameters), Value(table.location),
	     Value(table.input_format), Value(table.output_format), Value(table.serde_library),
	     ParametersValue(table.serde_parameters), ColumnsValue(table.columns), ColumnsValue(table.partition_keys),
	     StringsValue(table.bucket_columns), Value::INTEGER(table.number_of_buckets),
	     SortColumnsValue(table.sort_columns)});
	if (!updated) {
		throw TransactionException("Hive table \"%s.%s\" was changed by another client since it was read, run the "
		                           "statement again",
		                           bound.schema_name, bound.name);
	}
}

HiveCreateOutcome DuckDBMetastore::CreateHiveTable(ClientContext &context, const HiveTableInfo &table) {
	MetastoreWrite write(context, catalog, "CreateTable");
	if (table.location.empty()) {
		throw InvalidInputException("Can not create Hive table '%s.%s' without a location", table.schema_name,
		                            table.name);
	}
	annotated_lock_guard<annotated_mutex> guard(lock);
	return InsertTable(table.ForCreate()) ? HiveCreateOutcome::CREATED : HiveCreateOutcome::ALREADY_EXISTED;
}

static HiveTableInfo ViewTable(const HiveViewInfo &view, HiveTableInfo table) {
	table.name = view.name;
	table.schema_name = view.database_name;
	table.hive_table_type = "VIRTUAL_VIEW";
	table.table_type = HiveTableType::VIRTUAL_VIEW;
	table.view_original_text = view.sql;
	table.view_expanded_text = view.expanded_sql;
	table.columns = view.columns;
	table.location.clear();
	view.SetParameters(table.parameters);
	return table;
}

HiveCreateOutcome DuckDBMetastore::CreateView(ClientContext &context, const HiveViewInfo &view) {
	MetastoreWrite write(context, catalog, "CreateView");
	annotated_lock_guard<annotated_mutex> guard(lock);
	return InsertTable(ViewTable(view, HiveTableInfo())) ? HiveCreateOutcome::CREATED
	                                                     : HiveCreateOutcome::ALREADY_EXISTED;
}

void DuckDBMetastore::UpdateView(ClientContext &context, const HiveViewInfo &view, const HiveTableInfo &bound) {
	MetastoreWrite write(context, catalog, "UpdateView");
	annotated_lock_guard<annotated_mutex> guard(lock);
	ReplaceTable(bound, ViewTable(view, bound));
}

void DuckDBMetastore::UpdateTableColumns(ClientContext &context, const HiveTableInfo &bound,
                                         const vector<HiveColumn> &columns) {
	MetastoreWrite write(context, catalog, "UpdateTable");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto table = bound;
	table.columns = columns;
	ReplaceTable(bound, table);
}

void DuckDBMetastore::UpdateTablePartitionKeys(ClientContext &context, const HiveTableInfo &bound,
                                               const vector<HiveColumn> &partition_keys) {
	MetastoreWrite write(context, catalog, "UpdateTable");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto table = bound;
	for (idx_t i = 0; i < table.partition_keys.size() && i < partition_keys.size(); i++) {
		table.partition_keys[i].name = partition_keys[i].name;
		table.partition_keys[i].type = partition_keys[i].type;
		table.partition_keys[i].comment = partition_keys[i].comment;
	}
	ReplaceTable(bound, table);
}

void DuckDBMetastore::UpdateTableDescription(ClientContext &context, const HiveTableInfo &bound,
                                             const string &description) {
	MetastoreWrite write(context, catalog, "UpdateTable");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto table = bound;
	table.description = description;
	if (description.empty()) {
		table.parameters.erase("comment");
	} else {
		table.parameters["comment"] = description;
	}
	ReplaceTable(bound, table);
}

void DuckDBMetastore::SetTableLocation(ClientContext &context, const HiveTableInfo &bound, const string &location) {
	MetastoreWrite write(context, catalog, "UpdateTable");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto table = bound;
	table.location = location;
	ReplaceTable(bound, table);
}

void DuckDBMetastore::UpdateTableParameters(ClientContext &context, const HiveTableInfo &bound,
                                            const vector<pair<string, string>> &set, const vector<string> &unset) {
	MetastoreWrite write(context, catalog, "UpdateTable");
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto table = bound;
	for (auto &key : unset) {
		table.parameters.erase(key);
	}
	for (auto &entry : set) {
		table.parameters[entry.first] = entry.second;
	}
	ReplaceTable(bound, table);
}

void DuckDBMetastore::DeleteTableRow(const string &database_name, const string &table_name) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto db = Value(Lower(database_name));
	auto table = Value(Lower(table_name));
	if (!Changed("DELETE FROM tbls WHERE db_name = $1 AND tbl_name = $2", {db, table})) {
		throw CatalogException("Table with name \"%s\" does not exist in Hive metastore database \"%s\"", table_name,
		                       database_name);
	}
	Run("DELETE FROM partitions WHERE db_name = $1 AND tbl_name = $2", {db, table});
}

void DuckDBMetastore::DeleteTable(ClientContext &context, const string &database_name, const string &table_name) {
	MetastoreWrite write(context, catalog, "DeleteTable");
	DeleteTableRow(database_name, table_name);
}

optional_idx DuckDBMetastore::GetTableCreateTime(const string &database_name, const string &table_name) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto rows = Rows("SELECT create_time FROM tbls WHERE db_name = $1 AND tbl_name = $2",
	                 {Value(Lower(database_name)), Value(Lower(table_name))});
	if (rows.empty()) {
		return optional_idx();
	}
	return rows[0][0].IsNull() ? 0 : rows[0][0].GetValue<idx_t>();
}

unique_ptr<HiveMetastoreHandle> DuckDBMetastore::GetHandle(ClientContext &context) {
	return make_uniq<DuckDBMetastoreHandle>(*this);
}

//===--------------------------------------------------------------------===//
// Partitions
//===--------------------------------------------------------------------===//
HivePartitionDetails DuckDBMetastore::PartitionFromRow(const vector<Value> &row) const {
	HivePartitionDetails partition;
	partition.values = StringsOf(row[0]);
	partition.location = StringOf(row[1]);
	partition.input_format = StringOf(row[2]);
	partition.serde_library = StringOf(row[3]);
	partition.serde_parameters = MapOf(row[4]);
	partition.partition_columns = ColumnsOf(row[5]);
	partition.parameters = MapOf(row[6]);
	return partition;
}

bool DuckDBMetastore::GetPartition(ClientContext &context, const string &database_name, const string &table_name,
                                   const vector<string> &values, HivePartitionDetails &result, string *raw_json) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	auto rows = Rows(StringUtil::Format("SELECT %s FROM partitions WHERE db_name = $1 AND tbl_name = $2 AND "
	                                    "part_key = $3",
	                                    PARTITION_COLUMNS),
	                 {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(values))});
	if (rows.empty()) {
		return false;
	}
	result = PartitionFromRow(rows[0]);
	if (raw_json) {
		*raw_json = HiveMetastoreJSON::Partition(database_name, table_name, result);
	}
	return true;
}

vector<HivePartitionInfo> DuckDBMetastore::GetPartitions(ClientContext &context, const string &database_name,
                                                         const string &table_name) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	vector<HivePartitionInfo> result;
	for (auto &row : Rows(StringUtil::Format("SELECT %s FROM partitions WHERE db_name = $1 AND tbl_name = $2 ORDER BY "
	                                         "create_time, part_key",
	                                         PARTITION_COLUMNS),
	                      {Value(Lower(database_name)), Value(Lower(table_name))})) {
		result.push_back(PartitionFromRow(row));
	}
	return result;
}

bool DuckDBMetastore::InsertPartition(const HiveTableInfo &table, const HivePartitionInput &partition) {
	if (partition.values.size() != table.partition_keys.size()) {
		throw InvalidInputException("Partition [%s] of Hive table '%s.%s' has %llu values, the table has %llu "
		                            "partition keys",
		                            StringUtil::Join(partition.values, ", "), table.schema_name, table.name,
		                            partition.values.size(), table.partition_keys.size());
	}
	auto inserted = Changed(
	    "INSERT INTO partitions VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT DO NOTHING",
	    {Value(Lower(table.schema_name)), Value(Lower(table.name)), Value(PartitionKey(partition.values)),
	     StringsValue(partition.values), Value::BIGINT(NowMs()), ParametersValue({}), Value(partition.location),
	     Value(table.input_format), Value(table.output_format), Value(table.serde_library),
	     ParametersValue(table.serde_parameters), ColumnsValue(table.columns)});
	return inserted > 0;
}

bool DuckDBMetastore::CreatePartition(ClientContext &context, const HiveTableInfo &table,
                                      const HivePartitionInput &partition, bool if_not_exists) {
	MetastoreWrite write(context, catalog, "CreatePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (InsertPartition(table, partition)) {
		return true;
	}
	if (if_not_exists) {
		return false;
	}
	throw CatalogException("Partition [%s] of Hive table '%s.%s' already exists",
	                       StringUtil::Join(partition.values, ", "), table.schema_name, table.name);
}

vector<HivePartitionInput> DuckDBMetastore::BatchCreatePartitions(ClientContext &context, const HiveTableInfo &table,
                                                                  const vector<HivePartitionInput> &partitions) {
	MetastoreWrite write(context, catalog, "BatchCreatePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	vector<HivePartitionInput> existing;
	for (auto &partition : partitions) {
		if (!InsertPartition(table, partition)) {
			existing.push_back(partition);
		}
	}
	return existing;
}

bool DuckDBMetastore::DeletePartition(ClientContext &context, const string &database_name, const string &table_name,
                                      const vector<string> &values) {
	MetastoreWrite write(context, catalog, "DeletePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	return Changed("DELETE FROM partitions WHERE db_name = $1 AND tbl_name = $2 AND part_key = $3",
	               {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(values))}) > 0;
}

void DuckDBMetastore::BatchDeletePartitions(ClientContext &context, const string &database_name,
                                            const string &table_name, const vector<vector<string>> &partitions) {
	MetastoreWrite write(context, catalog, "BatchDeletePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	for (auto &values : partitions) {
		Run("DELETE FROM partitions WHERE db_name = $1 AND tbl_name = $2 AND part_key = $3",
		    {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(values))});
	}
}

map<vector<string>, string> DuckDBMetastore::PartitionLocations(ClientContext &context, const string &database_name,
                                                                const string &table_name,
                                                                const vector<vector<string>> &partitions) {
	annotated_lock_guard<annotated_mutex> guard(lock);
	map<vector<string>, string> result;
	for (auto &values : partitions) {
		auto rows = Rows("SELECT location FROM partitions WHERE db_name = $1 AND tbl_name = $2 AND part_key = $3",
		                 {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(values))});
		if (!rows.empty()) {
			result[values] = StringOf(rows[0][0]);
		}
	}
	return result;
}

void DuckDBMetastore::SetPartitionLocation(ClientContext &context, const string &database_name,
                                           const string &table_name, const HivePartitionDetails &partition,
                                           const string &location) {
	MetastoreWrite write(context, catalog, "UpdatePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!Changed("UPDATE partitions SET location = $4 WHERE db_name = $1 AND tbl_name = $2 AND part_key = $3",
	             {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(partition.values)),
	              Value(location)})) {
		throw CatalogException("Partition [%s] of Hive table '%s.%s' does not exist",
		                       StringUtil::Join(partition.values, ", "), database_name, table_name);
	}
}

string DuckDBMetastore::RenamePartition(ClientContext &context, const string &database_name, const string &table_name,
                                        const HivePartitionDetails &partition, const vector<string> &new_values) {
	MetastoreWrite write(context, catalog, "UpdatePartition");
	annotated_lock_guard<annotated_mutex> guard(lock);
	if (!Changed("UPDATE partitions SET part_key = $4, part_values = $5 WHERE db_name = $1 AND tbl_name = $2 AND "
	             "part_key = $3",
	             {Value(Lower(database_name)), Value(Lower(table_name)), Value(PartitionKey(partition.values)),
	              Value(PartitionKey(new_values)), StringsValue(new_values)})) {
		throw CatalogException("Partition [%s] of Hive table '%s.%s' does not exist",
		                       StringUtil::Join(partition.values, ", "), database_name, table_name);
	}
	return partition.location;
}

} // namespace duckdb
