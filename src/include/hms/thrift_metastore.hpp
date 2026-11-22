//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms/thrift_metastore.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hive/metastore/hive_metastore.hpp"
#include "hms/thrift_client.hpp"

namespace duckdb {

struct ThriftMetastoreOptions {
	string path;
	string host;
	int32_t port = 9083;
	//! Socket send / receive timeout
	int32_t timeout_seconds = 60;

public:
	//! 'thrift://<host>[:<port>]'
	static bool IsThriftPath(const string &path);
	void SetPath(const string &path);
	//! TIMEOUT (seconds); false for any other option
	bool ParseOption(const string &name, const Value &value);
};

//! A Hive Metastore, reached through its Thrift API (ThriftHiveMetastore) without SASL. Tables and partitions keep
//! the Thrift structs they were read as, so an update sends back everything the metastore stored, fields this
//! extension does not know included.
class ThriftMetastore : public HiveMetastore {
public:
	ThriftMetastore(HiveCatalog &catalog, ThriftMetastoreOptions options);

public:
	string GetCatalogType() const override {
		return "hive";
	}
	string ServiceName() const override {
		return "Hive metastore";
	}
	string GetPath() const override {
		return options.path;
	}
	//! get_all_databases
	void VerifyConnection(ClientContext &context) override;
	string Identity(ClientContext &context) override {
		return "thrift:" + options.host + ":" + to_string(options.port);
	}

	vector<HiveDatabaseInfo> GetDatabases(ClientContext &context) override;
	bool GetDatabase(ClientContext &context, const string &database_name, HiveDatabaseInfo &result,
	                 string *raw_json = nullptr) override;
	vector<HiveTableInfo> GetTables(ClientContext &context, const string &database_name, bool single_page = false,
	                                optional_ptr<bool> has_more = nullptr, bool throw_if_denied = false) override;
	bool GetAnyTableName(ClientContext &context, const string &database_name, string &table_name) override;
	bool GetTable(ClientContext &context, const string &database_name, const string &table_name, HiveTableInfo &result,
	              string *raw_json = nullptr) override;
	HiveCreateOutcome CreateDatabase(ClientContext &context, const HiveDatabaseInfo &database) override;
	void UpdateDatabase(ClientContext &context, const HiveDatabaseInfo &database) override;
	void DeleteDatabase(ClientContext &context, const string &database_name) override;
	HiveCreateOutcome CreateHiveTable(ClientContext &context, const HiveTableInfo &table) override;
	HiveCreateOutcome CreateView(ClientContext &context, const HiveViewInfo &view) override;
	void UpdateView(ClientContext &context, const HiveViewInfo &view, const HiveTableInfo &bound) override;
	void UpdateTableColumns(ClientContext &context, const HiveTableInfo &bound,
	                        const vector<HiveColumn> &columns) override;
	void UpdateTablePartitionKeys(ClientContext &context, const HiveTableInfo &bound,
	                              const vector<HiveColumn> &partition_keys) override;
	void UpdateTableDescription(ClientContext &context, const HiveTableInfo &bound, const string &description) override;
	void SetTableLocation(ClientContext &context, const HiveTableInfo &bound, const string &location) override;
	void UpdateTableParameters(ClientContext &context, const HiveTableInfo &bound,
	                           const vector<pair<string, string>> &set, const vector<string> &unset) override;
	void DeleteTable(ClientContext &context, const string &database_name, const string &table_name) override;
	unique_ptr<HiveMetastoreHandle> GetHandle(ClientContext &context) override;

	using HiveMetastore::GetPartition;
	bool GetPartition(ClientContext &context, const string &database_name, const string &table_name,
	                  const vector<string> &values, HivePartitionDetails &result, string *raw_json = nullptr) override;
	void SetPartitionLocation(ClientContext &context, const string &database_name, const string &table_name,
	                          const HivePartitionDetails &partition, const string &location) override;
	bool CreatePartition(ClientContext &context, const HiveTableInfo &table, const HivePartitionInput &partition,
	                     bool if_not_exists) override;
	bool DeletePartition(ClientContext &context, const string &database_name, const string &table_name,
	                     const vector<string> &values) override;
	void BatchDeletePartitions(ClientContext &context, const string &database_name, const string &table_name,
	                           const vector<vector<string>> &partitions) override;
	map<vector<string>, string> PartitionLocations(ClientContext &context, const string &database_name,
	                                               const string &table_name,
	                                               const vector<vector<string>> &partitions) override;
	string RenamePartition(ClientContext &context, const string &database_name, const string &table_name,
	                       const HivePartitionDetails &partition, const vector<string> &new_values) override;
	vector<HivePartitionInfo> GetPartitions(ClientContext &context, const string &database_name,
	                                        const string &table_name) override;
	vector<HivePartitionInput> BatchCreatePartitions(ClientContext &context, const HiveTableInfo &table,
	                                                 const vector<HivePartitionInput> &partitions) override;

	//! drop_table, without the metadata cache or the read-only check
	void DropTable(const string &database_name, const string &table_name);
	optional_idx GetTableCreateTime(const string &database_name, const string &table_name);

private:
	//! Calls 'method', throws a MetastoreException for an exception the metastore threw
	ThriftReply Call(const string &method, const ThriftValue &arguments);
	[[noreturn]] void Throw(const string &operation, const ThriftReply &reply);
	//! get_table: false if the table does not exist
	bool FetchTable(const string &database_name, const string &table_name, ThriftValue &result);
	//! alter_table with the Table 'bound' was read as, changed by 'modify'
	void AlterTable(const HiveTableInfo &bound, const std::function<void(ThriftValue &table)> &modify);
	//! A Partition with the storage descriptor of 'table' and the partition's location
	ThriftValue NewPartition(const HiveTableInfo &table, const HivePartitionInput &partition);

private:
	ThriftMetastoreOptions options;
	ThriftClient client;
};

} // namespace duckdb
