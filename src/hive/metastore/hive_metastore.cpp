#include "hive/metastore/hive_metastore.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

bool HiveMetastore::GetPartition(ClientContext &context, const string &database_name, const string &table_name,
                                 const vector<string> &values, HivePartitionInfo &result) {
	HivePartitionDetails details;
	if (!GetPartition(context, database_name, table_name, values, details)) {
		return false;
	}
	result = std::move(static_cast<HivePartitionInfo &>(details));
	return true;
}

set<vector<string>> HiveMetastore::ExistingPartitions(ClientContext &context, const string &database_name,
                                                      const string &table_name,
                                                      const vector<vector<string>> &partitions) {
	set<vector<string>> result;
	for (auto &entry : PartitionLocations(context, database_name, table_name, partitions)) {
		result.insert(entry.first);
	}
	return result;
}

void HiveMetastore::GetPartitionsOfTables(ClientContext &context, vector<HivePartitionLoad> &tables) {
	for (auto &table : tables) {
		try {
			table.partitions = make_shared_ptr<const vector<HivePartitionInfo>>(
			    GetPartitions(context, table.database_name, table.table_name));
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT) {
				throw;
			}
			auto metastore_error = dynamic_cast<MetastoreException *>(&ex);
			table.throttled = metastore_error && metastore_error->throttled;
			table.error = std::move(error);
			return;
		}
	}
}

} // namespace duckdb
