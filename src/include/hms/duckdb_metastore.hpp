//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms/duckdb_metastore.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/value.hpp"

#include "hive/metastore/hive_metastore.hpp"

namespace duckdb {
class Connection;
class DuckDB;
class QueryResult;

//! A Hive metastore kept in a DuckDB database of its own (a file, or ':memory:'): the tables dbs, tbls and
//! partitions hold what a Hive Metastore keeps in its relational backend (DBS, TBLS + SDS + SERDES, PARTITIONS),
//! one row per object with the storage descriptor inline. Changes commit one by one, like the other metastores.
class DuckDBMetastore : public HiveMetastore {
public:
	DuckDBMetastore(HiveCatalog &catalog, string path);
	~DuckDBMetastore() override;

public:
	string GetCatalogType() const override {
		return "hive";
	}
	string ServiceName() const override {
		return "Hive metastore";
	}
	string GetPath() const override {
		return path;
	}
	//! Opens the database and creates the metastore tables in it
	void VerifyConnection(ClientContext &context) override;
	string Identity(ClientContext &context) override {
		return "duckdb:" + path;
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

	//! Delete a table without the metadata cache or the read-only check, as a rollback does
	void DeleteTableRow(const string &database_name, const string &table_name);
	optional_idx GetTableCreateTime(const string &database_name, const string &table_name);

private:
	//! Runs 'sql' with 'values' on the metastore database, which is opened first if need be
	unique_ptr<QueryResult> Run(const string &sql, vector<Value> values) DUCKDB_REQUIRES(lock);
	//! The rows 'sql' returns
	vector<vector<Value>> Rows(const string &sql, vector<Value> values) DUCKDB_REQUIRES(lock);
	//! The number of rows a statement changed
	idx_t Changed(const string &sql, vector<Value> values) DUCKDB_REQUIRES(lock);
	void Open() DUCKDB_REQUIRES(lock);
	//! Insert a table row: false if a table of that name exists
	bool InsertTable(const HiveTableInfo &table) DUCKDB_REQUIRES(lock);
	//! Replace the definition of 'table' if it is still the version 'bound' was read as
	void ReplaceTable(const HiveTableInfo &bound, const HiveTableInfo &table) DUCKDB_REQUIRES(lock);
	//! Insert a partition of 'table' with the table's storage descriptor and the partition's location
	bool InsertPartition(const HiveTableInfo &table, const HivePartitionInput &partition) DUCKDB_REQUIRES(lock);
	HiveTableInfo TableFromRow(const vector<Value> &row) const;
	HivePartitionDetails PartitionFromRow(const vector<Value> &row) const;

private:
	string path;
	annotated_mutex lock;
	unique_ptr<DuckDB> database DUCKDB_GUARDED_BY(lock);
	unique_ptr<Connection> connection DUCKDB_GUARDED_BY(lock);
};

} // namespace duckdb
