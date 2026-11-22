#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/on_entry_not_found.hpp"
#include "duckdb/parser/parsed_expression.hpp"

#include "hive/core/hive_info.hpp"
#include "hive/catalog/hive_table_set.hpp"

namespace duckdb {
struct CreateTableInfo;
struct SetColumnCommentInfo;
struct SetCommentInfo;

//! Options accepted in CREATE TABLE ... WITH (...) for Hive tables
struct HiveCreateTableOptions {
	//! Optional explicit S3 location of the table
	string location;
	//! Optional description of the table (comment = '...'), stored as the table's description
	string description;
	//! The file format of the table (format = 'parquet' | 'csv' | 'json' | 'avro'), parquet by default
	HiveFileFormat format = HiveFileFormat::PARQUET;
	//! csv only: delimiter, quote and escape (single characters; quote and escape empty unless given)
	string csv_delimiter = ",";
	string csv_quote;
	string csv_escape;
	//! BucketColumns / NumberOfBuckets / SortColumns, named as in the Hive storage descriptor
	vector<string> bucket_columns;
	int32_t number_of_buckets = -1;
	vector<HiveColumn> sort_columns;
	//! CREATE TABLE AS only: delete the data files already at the location (overwrite = true)
	bool overwrite = false;
	//! Every other option is stored as a table parameter in the metastore
	unordered_map<string, string> parameters;
};

//! A Hive database, exposed as a DuckDB schema
class HiveSchemaEntry : public SchemaCatalogEntry {
public:
	//! 'schema_info_loaded' false: only the name of 'database_info' is known, GetSchemaInfo asks the metastore for the
	//! rest
	HiveSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, HiveDatabaseInfo database_info,
	                bool schema_info_loaded = true);
	~HiveSchemaEntry() override;

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	//! Create a table from a definition whose options were already resolved while planning CTAS. This avoids evaluating
	//! location, format or dialect expressions a second time when execution begins.
	optional_ptr<CatalogEntry> CreateTableFromInfo(CatalogTransaction transaction, BoundCreateTableInfo &info,
	                                               const HiveTableInfo &table_info);
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	//! Serves a single-name equality on the entry name with one table lookup instead of listing every table
	void Scan(ClientContext &context, SchemaScanInfo &info) override;
	//! Suggests from the tables this attach has already seen
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	//! Build and validate a Hive table definition without touching the metastore. CTAS resolves it once while planning,
	//! then carries the same value into execution for both catalog creation and file-writer behavior.
	HiveTableInfo BuildTableInfo(ClientContext &context, const CreateTableInfo &create_info);
	//! Bind and evaluate the constant expressions of a WITH (<key> = <value>, ...) option list
	static vector<pair<string, Value>>
	EvaluateOptions(ClientContext &context, const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options,
	                const string &statement);
	static HiveCreateTableOptions ParseCreateTableOptions(ClientContext &context, const CreateTableInfo &create_info);
	//! BucketColumns, NumberOfBuckets or SortColumns (case-insensitive)
	static bool IsBucketingOption(const string &key);
	//! Type changes Hive can read back from the existing parquet files: widening only
	static bool IsAllowedHiveTypeChange(const LogicalType &from, const LogicalType &to);
	//! Replace the cached entry of an altered table with what the metastore stored
	HiveTable &RefreshTable(ClientContext &context, const string &table_name);
	//! The definition a write rewrites: the bound entry's. Under a connection-scoped metadata cache the entry may
	//! predate a change by another connection, so it is fetched again first.
	const HiveTableInfo &DefinitionForWrite(ClientContext &context, const CatalogEntry &entry);
	const string &GetDatabaseName() const {
		return database_info.name;
	}
	//! The database definition, asked from the metastore first if the entry was created from the name alone
	const HiveDatabaseInfo &GetSchemaInfo(ClientContext &context);
	//! Take the definition of a database known by name alone from a listing; no-op once it is loaded
	void SetSchemaInfo(const HiveDatabaseInfo &database);

private:
	optional_ptr<CatalogEntry> CreateTableInternal(CatalogTransaction transaction, BoundCreateTableInfo &info,
	                                               const HiveTableInfo *resolved_table_info);
	static bool CatalogTypeIsSupported(CatalogType type);
	//! The ALTER TABLE forms below rewrite the table from 'table', the bound entry's definition
	void AlterTableProperties(ClientContext &context, const HiveTableInfo &table, AlterTableInfo &alter_table);
	//! COMMENT ON COLUMN: the metastore keeps the comment with the column
	void SetColumnComment(ClientContext &context, const HiveTableInfo &table, SetColumnCommentInfo &info);
	//! COMMENT ON TABLE: the metastore stores it as the table's description
	void SetTableComment(ClientContext &context, const HiveTableInfo &table, SetCommentInfo &info);

public:
	HiveTableSet tables;

private:
	//! Only the name is known until GetSchemaInfo loads the rest for an entry created from a table lookup
	HiveDatabaseInfo database_info;
	//! Guards the fields of 'database_info' other than the name, which never changes
	mutex schema_info_lock;
	bool schema_info_loaded;
};

} // namespace duckdb
