//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hive/metastore/hive_metastore.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/string_util.hpp"

#include "hive/core/hive_info.hpp"

namespace duckdb {
class ClientContext;
class HiveCatalog;

//! A metastore call failed. 'access_denied' and 'throttled' let the catalog tell a refused permission or a rate
//! limit apart from other failures
class MetastoreException : public IOException {
public:
	MetastoreException(const string &message, bool access_denied, bool throttled = false)
	    : IOException(message), access_denied(access_denied), throttled(throttled) {
	}

	bool access_denied;
	bool throttled;
};

//! One table of HiveMetastore::GetPartitionsOfTables
struct HivePartitionLoad {
	string database_name;
	string table_name;
	//! Null unless every request for the table's partitions succeeded
	shared_ptr<const vector<HivePartitionInfo>> partitions;
	//! The error of the table's first failed request. Unset with no partitions when another table failed first
	ErrorData error;
	//! Whether the metastore refused that request for the request rate
	bool throttled = false;
};

//! How a create call ended: the object exists afterwards either way, another client may have created it first
enum class HiveCreateOutcome : uint8_t { CREATED, ALREADY_EXISTED };

//! The metastore calls a rollback makes after its transaction ended, when credentials can no longer be looked up:
//! taken while the statement still runs
class HiveMetastoreHandle {
public:
	virtual ~HiveMetastoreHandle() = default;

	//! The create time of a table in milliseconds: invalid if the table does not exist, 0 if the metastore gave none
	virtual optional_idx GetTableCreateTime(const string &database_name, const string &table_name) = 0;
	//! Delete a table (the data files are left in place); leaves the metadata cache alone
	virtual void DeleteTable(ClientContext &context, const string &database_name, const string &table_name) = 0;
};

//! The metadata service behind a HiveCatalog: a Hive Metastore (Thrift) or a DuckDB database. It speaks
//! the Hive metastore's model (databases, tables with a storage descriptor, partitions) and knows nothing of how the
//! data files are read or written. Mutations are refused for a read-only attach and clear the metadata cache.
class HiveMetastore {
public:
	explicit HiveMetastore(HiveCatalog &catalog) : catalog(catalog) {
	}
	virtual ~HiveMetastore() = default;

public:
	//! The catalog type, e.g. 'hive'
	virtual string GetCatalogType() const = 0;
	//! How errors name the service, e.g. 'Hive metastore'
	virtual string ServiceName() const = 0;
	//! The ATTACH path
	virtual string GetPath() const = 0;
	//! Fail early when the metastore can not be reached
	virtual void VerifyConnection(ClientContext &context) = 0;
	//! Identifies the metastore behind this attach: attaches of the same one share its request rate
	virtual string Identity(ClientContext &context) = 0;
	//! Whether an Iceberg catalog over 'warehouse' reads this metastore's Iceberg tables
	virtual bool IsIcebergWarehouse(const string &warehouse) {
		return false;
	}
	//! Throw for a location ('function' names the caller in errors) the metastore does not store tables at
	virtual void CheckLocation(const string &function, const string &location) const {
	}
	virtual vector<HiveDatabaseInfo> GetDatabases(ClientContext &context) = 0;
	//! Returns false if the database does not exist. 'raw_json' (optional) receives the database as JSON
	virtual bool GetDatabase(ClientContext &context, const string &database_name, HiveDatabaseInfo &result,
	                         string *raw_json = nullptr) = 0;
	//! All tables of a database; none when the database no longer exists or its tables may not be listed. With
	//! 'single_page' only the first page is fetched and 'has_more' reports whether there are more. With 'single_page'
	//! or 'throw_if_denied', a refused permission throws instead of returning none.
	virtual vector<HiveTableInfo> GetTables(ClientContext &context, const string &database_name,
	                                        bool single_page = false, optional_ptr<bool> has_more = nullptr,
	                                        bool throw_if_denied = false) = 0;
	//! Returns false if the database holds no tables (or views), otherwise true with the name of one of them
	virtual bool GetAnyTableName(ClientContext &context, const string &database_name, string &table_name) = 0;
	//! Returns false if the table does not exist. 'raw_json' (optional) receives the table as JSON
	virtual bool GetTable(ClientContext &context, const string &database_name, const string &table_name,
	                      HiveTableInfo &result, string *raw_json = nullptr) = 0;

	virtual HiveCreateOutcome CreateDatabase(ClientContext &context, const HiveDatabaseInfo &database) = 0;
	//! Replace the description, location and parameters of a database
	virtual void UpdateDatabase(ClientContext &context, const HiveDatabaseInfo &database) = 0;
	//! Delete a database and all of its tables, throws a CatalogException if it does not exist
	virtual void DeleteDatabase(ClientContext &context, const string &database_name) = 0;
	//! Create a Hive table in 'table.file_format' at 'table.location' with the columns and partition keys of 'table'
	virtual HiveCreateOutcome CreateHiveTable(ClientContext &context, const HiveTableInfo &table) = 0;
	virtual HiveCreateOutcome CreateView(ClientContext &context, const HiveViewInfo &view) = 0;
	//! Replace the definition of the view 'bound' (as fetched)
	virtual void UpdateView(ClientContext &context, const HiveViewInfo &view, const HiveTableInfo &bound) = 0;
	//! The updates below rewrite the table from 'bound' (as fetched), keeping the rest of its definition
	virtual void UpdateTableColumns(ClientContext &context, const HiveTableInfo &bound,
	                                const vector<HiveColumn> &columns) = 0;
	virtual void UpdateTablePartitionKeys(ClientContext &context, const HiveTableInfo &bound,
	                                      const vector<HiveColumn> &partition_keys) = 0;
	virtual void UpdateTableDescription(ClientContext &context, const HiveTableInfo &bound,
	                                    const string &description) = 0;
	virtual void SetTableLocation(ClientContext &context, const HiveTableInfo &bound, const string &location) = 0;
	//! The 'set' parameters are added or overwritten and the 'unset' keys removed; the parameters the table format is
	//! derived from (MetastoreTableFormat::IsParameter) are refused
	virtual void UpdateTableParameters(ClientContext &context, const HiveTableInfo &bound,
	                                   const vector<pair<string, string>> &set, const vector<string> &unset) = 0;
	//! Delete a table (the data files are left in place), throws a CatalogException if it does not exist
	virtual void DeleteTable(ClientContext &context, const string &database_name, const string &table_name) = 0;
	//! What a rollback needs after the transaction ended
	virtual unique_ptr<HiveMetastoreHandle> GetHandle(ClientContext &context) = 0;

	virtual bool GetPartition(ClientContext &context, const string &database_name, const string &table_name,
	                          const vector<string> &values, HivePartitionInfo &result);
	//! With the partition's storage descriptor and parameters. 'raw_json' (optional) receives the partition as JSON
	virtual bool GetPartition(ClientContext &context, const string &database_name, const string &table_name,
	                          const vector<string> &values, HivePartitionDetails &result,
	                          string *raw_json = nullptr) = 0;
	//! Point the partition (as fetched) at another location
	virtual void SetPartitionLocation(ClientContext &context, const string &database_name, const string &table_name,
	                                  const HivePartitionDetails &partition, const string &location) = 0;
	//! Register one partition with the storage descriptor of 'table' (as fetched) and the partition's location.
	//! Returns false if it already exists and 'if_not_exists' is set, throws a CatalogException otherwise.
	virtual bool CreatePartition(ClientContext &context, const HiveTableInfo &table,
	                             const HivePartitionInput &partition, bool if_not_exists) = 0;
	//! Unregister a partition (the data files are left in place), returns false if it does not exist
	virtual bool DeletePartition(ClientContext &context, const string &database_name, const string &table_name,
	                             const vector<string> &values) = 0;
	//! Unregister partitions; those that do not exist are skipped
	virtual void BatchDeletePartitions(ClientContext &context, const string &database_name, const string &table_name,
	                                   const vector<vector<string>> &partitions) = 0;
	//! Which of 'partitions' exist
	virtual set<vector<string>> ExistingPartitions(ClientContext &context, const string &database_name,
	                                               const string &table_name, const vector<vector<string>> &partitions);
	//! The locations of those of 'partitions' that exist, by their values
	virtual map<vector<string>, string> PartitionLocations(ClientContext &context, const string &database_name,
	                                                       const string &table_name,
	                                                       const vector<vector<string>> &partitions) = 0;
	//! Change the values of the partition (as fetched), keeping its location; returns the location
	virtual string RenamePartition(ClientContext &context, const string &database_name, const string &table_name,
	                               const HivePartitionDetails &partition, const vector<string> &new_values) = 0;
	//! All partitions of a table
	virtual vector<HivePartitionInfo> GetPartitions(ClientContext &context, const string &database_name,
	                                                const string &table_name) = 0;
	//! GetPartitions of several tables. The first failed request stops the others and is kept on its table; only an
	//! interrupt is thrown. One table after the other unless the metastore can do better.
	virtual void GetPartitionsOfTables(ClientContext &context, vector<HivePartitionLoad> &tables);
	//! Register partitions with the storage descriptor of 'table' (as fetched) and each partition's location.
	//! Partitions that already exist are skipped and returned.
	virtual vector<HivePartitionInput> BatchCreatePartitions(ClientContext &context, const HiveTableInfo &table,
	                                                         const vector<HivePartitionInput> &partitions) = 0;

	template <class TARGET>
	TARGET &Cast() {
		DynamicCastCheck<TARGET>(this);
		return reinterpret_cast<TARGET &>(*this);
	}

public:
	HiveCatalog &catalog;
};

} // namespace duckdb
