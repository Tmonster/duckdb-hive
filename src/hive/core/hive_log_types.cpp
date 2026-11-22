#include "hive/core/hive_log_types.hpp"

#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

constexpr LogLevel HiveScanLogType::LEVEL;

static Value OptionalCount(optional_idx value) {
	return value.IsValid() ? Value::UBIGINT(value.GetIndex()) : Value(LogicalType::UBIGINT);
}

static Value OptionalString(const string &value) {
	return value.empty() ? Value(LogicalType::VARCHAR) : Value(value);
}

HiveScanLogType::HiveScanLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

LogicalType HiveScanLogType::GetLogType() {
	child_list_t<LogicalType> children = {
	    {"table", LogicalType::VARCHAR},           {"scan_id", LogicalType::UBIGINT},
	    {"event", LogicalType::VARCHAR},           {"partitions", LogicalType::UBIGINT},
	    {"partitions_read", LogicalType::UBIGINT}, {"listings", LogicalType::UBIGINT},
	    {"files", LogicalType::UBIGINT},           {"duration_ms", LogicalType::DOUBLE},
	    {"detail", LogicalType::VARCHAR}};
	return LogicalType::STRUCT(children);
}

string HiveScanLogType::ConstructLogMessage(const HiveScanEvent &event) {
	child_list_t<Value> children = {
	    {"table", Value(event.table)},
	    {"scan_id", Value::UBIGINT(event.scan_id)},
	    {"event", Value(event.event)},
	    {"partitions", OptionalCount(event.partitions)},
	    {"partitions_read", OptionalCount(event.partitions_read)},
	    {"listings", OptionalCount(event.listings)},
	    {"files", OptionalCount(event.files)},
	    {"duration_ms", event.duration_ms < 0 ? Value(LogicalType::DOUBLE) : Value::DOUBLE(event.duration_ms)},
	    {"detail", OptionalString(event.detail)}};
	return Value::STRUCT(std::move(children)).ToString();
}

void RegisterHiveLogTypes(DatabaseInstance &db) {
	auto &log_manager = db.GetLogManager();
	log_manager.RegisterLogType(make_uniq<HiveScanLogType>());
}

} // namespace duckdb
