//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hive/metastore/hive_metastore_json.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hive/core/hive_info.hpp"

namespace duckdb {

//! The metastore objects (Database, Table, Partition) as JSON, for the raw_json of hive_get_*_response
struct HiveMetastoreJSON {
	static string Database(const HiveDatabaseInfo &database);
	static string Table(const HiveTableInfo &table);
	static string Partition(const string &database_name, const string &table_name,
	                        const HivePartitionDetails &partition);
};

} // namespace duckdb
