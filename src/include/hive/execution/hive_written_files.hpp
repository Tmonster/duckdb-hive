#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class ClientContext;

//! With hive_store_written_files, the data files an INSERT or CREATE TABLE AS into a Hive table wrote are added to the
//! temporary table hive_written_files once the write's partitions are registered
struct HiveWrittenFiles {
	static constexpr const char *SETTING = "hive_store_written_files";
	static constexpr const char *TABLE = "hive_written_files";

	static bool IsEnabled(ClientContext &context);
	static void Append(ClientContext &context, const string &table, const vector<string> &files);
};

} // namespace duckdb
