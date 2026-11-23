#include "hive/catalog/hive_catalog.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/schema_scan_filters.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/alter_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/storage/database_size.hpp"

#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_file_credentials.hpp"
#include "hive/catalog/hive_schema_entry.hpp"
#include "hive/catalog/hive_table.hpp"
#include "hive/catalog/hive_transaction.hpp"
#include "hive/catalog/hive_view.hpp"
#include "hive/catalog/scan_filter_equality.hpp"
#include "hive/planning/hive_insert.hpp"

namespace duckdb {

HiveCatalog::HiveCatalog(AttachedDatabase &db_p, AccessMode access_mode, HiveAttachOptions options_p)
    : Catalog(db_p), access_mode(access_mode), options(std::move(options_p)), schemas(*this) {
}

HiveCatalog::~HiveCatalog() {
}

void HiveCatalog::SetMetastore(unique_ptr<HiveMetastore> metastore_p) {
	metastore = std::move(metastore_p);
}

HiveMetastore &HiveCatalog::Metastore() const {
	if (!metastore) {
		throw InternalException("Hive catalog \"%s\" has no metastore", options.name);
	}
	return *metastore;
}

string HiveCatalog::GetCatalogType() {
	return Metastore().GetCatalogType();
}

void HiveCatalog::Initialize(bool load_builtin) {
}

ErrorData HiveCatalog::SupportsCreateTable(BoundCreateTableInfo &info) {
	auto &base = info.Base().Cast<CreateTableInfo>();
	// PARTITIONED BY is accepted here and validated in HiveSchemaEntry::BuildTableInfo (Hive tables only, plain
	// column references)
	if (!base.sort_keys.empty()) {
		return ErrorData(
		    ExceptionType::CATALOG,
		    StringUtil::Format("SORTED BY is not supported for tables in a %s catalog, the sort order of a "
		                       "bucketed table is the SortColumns option",
		                       Metastore().ServiceName()));
	}
	return ErrorData();
}

//! Hive databases do not nest: a nested path would otherwise create or drop the database of its last name
static string NestedSchemaError(const Catalog &catalog, vector<Identifier> schema_path) {
	return StringUtil::Format("Catalog %s does not support nested schemas: %s", catalog.GetName(),
	                          QualifiedName::FromPath(std::move(schema_path)).ToString());
}

ErrorData HiveCatalog::SupportsCreateSchema(CreateSchemaInfo &info) {
	if (info.IsNested()) {
		auto schema_path = info.ParentSchemas();
		schema_path.push_back(info.SchemaName());
		return ErrorData(ExceptionType::BINDER, NestedSchemaError(*this, std::move(schema_path)));
	}
	return ErrorData();
}

optional<Identifier> HiveCatalog::GetDefaultSchema() const {
	if (options.default_schema.empty()) {
		return nullopt;
	}
	return options.default_schema;
}

//===--------------------------------------------------------------------===//
// Schemas
//===--------------------------------------------------------------------===//
HiveSchemaSet &HiveCatalog::GetSchemas() {
	return schemas;
}

static optional_ptr<CatalogEntry> OnExistingSchema(const CreateSchemaInfo &info, const string &schema_name,
                                                   const string &catalog_name, const string &service_name) {
	switch (info.on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return nullptr;
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("Schema with name \"%s\" already exists in %s catalog \"%s\"", schema_name, service_name,
		                       catalog_name);
	default:
		throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported for %s catalogs", service_name);
	}
}

optional_ptr<CatalogEntry> HiveCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &context = transaction.GetContext();
	ThrowIfInExplicitTransaction(context);
	auto schema_name = info.SchemaName().GetIdentifierName();

	auto existing = schemas.GetEntry(context, schema_name);
	if (existing) {
		return OnExistingSchema(info, schema_name, GetName().GetIdentifierName(), Metastore().ServiceName());
	}

	HiveDatabaseInfo database;
	database.name = schema_name;
	database.location_uri = GetDatabaseLocation(schema_name);
	for (auto &option : HiveSchemaEntry::EvaluateOptions(context, info.options, "CREATE SCHEMA")) {
		SetDatabaseOption(database, option.first, option.second);
	}
	if (Metastore().CreateDatabase(context, database) == HiveCreateOutcome::ALREADY_EXISTED) {
		// another client created the database since the lookup above: take its entry
		if (!schemas.GetEntry(context, schema_name)) {
			throw CatalogException("%s database \"%s\" exists but could not be fetched", Metastore().ServiceName(),
			                       schema_name);
		}
		return OnExistingSchema(info, schema_name, GetName().GetIdentifierName(), Metastore().ServiceName());
	}

	// re-fetch so the cached entry reflects what the metastore stored
	HiveDatabaseInfo created;
	if (!Metastore().GetDatabase(context, schema_name, created)) {
		throw CatalogException("%s database \"%s\" was created but could not be fetched afterwards",
		                       Metastore().ServiceName(), schema_name);
	}
	return schemas.CreateEntry(schemas.CreateSchemaEntry(created));
}

void HiveCatalog::OnDetach(ClientContext &context) {
	HiveTransactionCache::OnDetach(context, *this);
}

void HiveCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	// the path is [catalog, parent schemas..., schema]
	auto &path = info.GetQualifiedName().Path();
	vector<Identifier> schema_path;
	for (idx_t i = path.size() > 1 ? 1 : 0; i < path.size(); i++) {
		if (!path[i].empty()) {
			schema_path.push_back(path[i]);
		}
	}
	if (schema_path.size() > 1) {
		throw BinderException(NestedSchemaError(*this, std::move(schema_path)));
	}
	ThrowIfInExplicitTransaction(context);
	auto schema_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto existing = schemas.GetEntry(context, schema_name);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("Schema with name \"%s\" does not exist in %s catalog \"%s\"", schema_name,
		                       Metastore().ServiceName(), GetName().GetIdentifierName());
	}
	// The metastore deletes every table of the database along with it, so without CASCADE only an empty database is
	// dropped
	string table_name;
	if (!info.cascade && Metastore().GetAnyTableName(context, schema_name, table_name)) {
		throw DependencyException("Cannot drop %s database \"%s\" because it still contains tables (e.g. \"%s\"). "
		                          "Use DROP SCHEMA ... CASCADE to drop the database together with all of its tables",
		                          Metastore().ServiceName(), schema_name, table_name);
	}
	Metastore().DeleteDatabase(context, schema_name);
	schemas.RemoveEntry(schema_name);
}

static void EraseDatabaseParameter(HiveDatabaseInfo &database, const string &key) {
	for (auto it = database.parameters.begin(); it != database.parameters.end();) {
		if (StringUtil::CIEquals(it->first, key)) {
			it = database.parameters.erase(it);
		} else {
			++it;
		}
	}
}

void HiveCatalog::SetDatabaseOption(HiveDatabaseInfo &database, const string &key, const Value &value) const {
	auto string_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
	if (StringUtil::CIEquals(key, "comment")) {
		database.description = string_value;
	} else if (StringUtil::CIEquals(key, "location")) {
		StringUtil::RTrim(string_value, "/");
		if (string_value.empty()) {
			throw InvalidInputException("The location of %s database \"%s\" must not be empty",
			                            Metastore().ServiceName(), database.name);
		}
		database.location_uri = string_value;
	} else {
		// everything else is a database property, stored in the database parameters (like Hive's DBPROPERTIES)
		EraseDatabaseParameter(database, key);
		database.parameters[key] = string_value;
	}
}

void HiveCatalog::AlterSchema(CatalogTransaction transaction, SchemaCatalogEntry &schema, AlterSchemaInfo &info) {
	auto &context = transaction.GetContext();
	ThrowIfInExplicitTransaction(context);
	auto schema_name = schema.Cast<HiveSchemaEntry>().GetDatabaseName();
	// work on the current metastore definition, not the cached one
	HiveDatabaseInfo database;
	if (!Metastore().GetDatabase(context, schema_name, database)) {
		throw CatalogException("Schema with name \"%s\" does not exist in %s catalog \"%s\"", schema_name,
		                       Metastore().ServiceName(), GetName().GetIdentifierName());
	}
	switch (info.alter_schema_type) {
	case AlterSchemaType::SET_SCHEMA_OPTIONS: {
		auto &set_info = info.Cast<SetSchemaOptionsInfo>();
		for (auto &option : HiveSchemaEntry::EvaluateOptions(context, set_info.options, "ALTER SCHEMA")) {
			SetDatabaseOption(database, option.first, option.second);
		}
		break;
	}
	case AlterSchemaType::RESET_SCHEMA_OPTIONS: {
		auto &reset_info = info.Cast<ResetSchemaOptionsInfo>();
		for (auto &option : reset_info.options) {
			auto key = option.GetIdentifierName();
			if (StringUtil::CIEquals(key, "comment")) {
				database.description.clear();
			} else if (StringUtil::CIEquals(key, "location")) {
				database.location_uri.clear();
			} else {
				EraseDatabaseParameter(database, key);
			}
		}
		break;
	}
	default:
		throw InternalException("Unrecognized alter schema type!");
	}
	Metastore().UpdateDatabase(context, database);

	HiveDatabaseInfo updated;
	if (!Metastore().GetDatabase(context, schema_name, updated)) {
		throw CatalogException("%s database \"%s\" was updated but could not be fetched afterwards",
		                       Metastore().ServiceName(), schema_name);
	}
	schemas.CreateEntry(schemas.CreateSchemaEntry(updated));
}

string HiveCatalog::GetDatabaseLocation(const string &database_name) const {
	if (options.default_location.empty()) {
		return string();
	}
	return options.default_location + "/" + database_name;
}

string HiveCatalog::GetTableLocation(const HiveDatabaseInfo &database, const string &table_name) const {
	// DEFAULT_LOCATION on ATTACH wins over the LocationUri of the Hive database, so that everything a session creates
	// without an explicit location lands under one prefix
	if (!options.default_location.empty()) {
		return GetDatabaseLocation(database.name) + "/" + table_name;
	}
	if (!database.location_uri.empty()) {
		auto location = database.location_uri;
		StringUtil::RTrim(location, "/");
		return location + "/" + table_name;
	}
	throw InvalidConfigurationException(
	    "No location for table \"%s\": %s database \"%s\" has no LocationUri and the catalog was attached without "
	    "DEFAULT_LOCATION. Provide one with CREATE TABLE ... WITH (location = 's3://...') or attach with "
	    "DEFAULT_LOCATION",
	    table_name, Metastore().ServiceName(), database.name);
}

void HiveCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<HiveSchemaEntry>()); });
}

vector<reference<SchemaCatalogEntry>> HiveCatalog::GetSchemasForScan(ClientContext &context,
                                                                     const SchemaScanFilters &filters) {
	optional_ptr<CatalogEntry> entry;
	string name;
	idx_t oid;
	if (TryGetOidEquality(filters.GetSchemaOid(), "HiveCatalog::GetSchemasForScan", oid)) {
		entry = schemas.GetEntryByOid(context, oid);
	} else if (!filters.GetSchemaOid() &&
	           TryGetNameEquality(filters.GetSchemaName(), "HiveCatalog::GetSchemasForScan", name)) {
		entry = schemas.GetEntry(context, name);
	} else {
		return Catalog::GetSchemasForScan(context, filters);
	}
	vector<reference<SchemaCatalogEntry>> result;
	if (entry) {
		result.push_back(entry->Cast<SchemaCatalogEntry>());
	}
	return result;
}

optional_ptr<SchemaCatalogEntry> HiveCatalog::LookupSchema(CatalogTransaction transaction,
                                                           const EntryLookupInfo &schema_lookup,
                                                           OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	// Hive databases are not nested: "x".db.t with an unknown catalog x arrives as [x, db]
	vector<string> path;
	for (auto &component : schema_lookup.GetQualifiedName().Path()) {
		if (!component.empty()) {
			path.push_back(component.GetIdentifierName());
		}
	}
	if (path.size() > 1 && StringUtil::CIEquals(path[0], GetName().GetIdentifierName())) {
		path.erase(path.begin());
	}
	if (path.size() > 1) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException(
		    schema_lookup.GetErrorContext(), "%s database \"%s\" does not exist: %s databases are not nested",
		    Metastore().ServiceName(), schema_lookup.GetQualifiedName().ToString(), Metastore().ServiceName());
	}
	auto &context = transaction.GetContext();
	auto entry = schemas.GetEntry(context, schema_name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw CatalogException(schema_lookup.GetErrorContext(), "%s database with name \"%s\" does not exist",
		                       Metastore().ServiceName(), schema_name);
	}
	return &entry->Cast<SchemaCatalogEntry>();
}

CatalogEntryLookup HiveCatalog::TryLookupEntryInternal(CatalogTransaction transaction,
                                                       const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetAtClause() && !SupportsTimeTravel()) {
		return {nullptr, nullptr, ErrorData(BinderException("Catalog type does not support time travel"))};
	}
	CatalogEntryLookup result {nullptr, nullptr, ErrorData()};
	auto &catalog = lookup_info.GetCatalog();
	auto &database_name = lookup_info.GetSchema().GetIdentifierName();
	// Hive databases are not nested: "x".db.t with an unknown catalog x arrives as [x, db, t]
	if (lookup_info.GetQualifiedName().Path().size() > 3 || database_name.empty() ||
	    (!catalog.empty() && !StringUtil::CIEquals(catalog.GetIdentifierName(), GetName().GetIdentifierName()))) {
		return result;
	}
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY) {
		switch (TryLookupTableBeforeSchema(transaction, lookup_info, database_name, result)) {
		case TableLookup::TABLE_FOUND:
		case TableLookup::TABLE_NOT_FOUND:
		case TableLookup::SCHEMA_NOT_FOUND:
			// 'result' holds the table and its schema, the schema alone, or neither
			return ViewForReading(transaction, result);
		case TableLookup::SKIPPED:
			break;
		}
	}
	auto schema = schemas.GetEntry(transaction.GetContext(), database_name);
	if (schema) {
		result.schema = &schema->Cast<SchemaCatalogEntry>();
		result.entry = result.schema->LookupEntry(transaction, lookup_info);
	}
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY) {
		return ViewForReading(transaction, result);
	}
	return result;
}

bool HiveCatalog::IsIcebergCatalog(Catalog &catalog) {
	auto &iceberg_catalog_name = options.iceberg_catalog;
	if (iceberg_catalog_name.empty() || catalog.GetName() != iceberg_catalog_name) {
		return false;
	}
	if (catalog.GetCatalogType() != "iceberg") {
		throw BinderException("ICEBERG_CATALOG \"%s\" of %s catalog \"%s\" is a %s catalog, not an Iceberg catalog",
		                      iceberg_catalog_name.GetIdentifierName(), Metastore().ServiceName(),
		                      GetName().GetIdentifierName(), catalog.GetCatalogType());
	}
	auto warehouse = catalog.GetDBPath();
	if (!Metastore().IsIcebergWarehouse(warehouse)) {
		throw BinderException("ICEBERG_CATALOG \"%s\" reads warehouse '%s', not %s catalog \"%s\" ('%s')",
		                      iceberg_catalog_name.GetIdentifierName(), warehouse, Metastore().ServiceName(),
		                      GetName().GetIdentifierName(), Metastore().GetPath());
	}
	return true;
}

bool HiveCatalog::IsIcebergTable(ClientContext &context, const Identifier &schema_name, const Identifier &table_name) {
	auto schema = schemas.GetEntry(context, schema_name.GetIdentifierName());
	if (!schema) {
		return false;
	}
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(table_name));
	auto entry = schema->Cast<SchemaCatalogEntry>().LookupEntry(GetCatalogTransaction(context), lookup);
	return entry && entry->type == CatalogType::TABLE_ENTRY &&
	       entry->Cast<HiveTable>().table_info.GetFormat().format == MetastoreFormat::ICEBERG;
}

CatalogEntryLookup HiveCatalog::ViewForReading(CatalogTransaction transaction, CatalogEntryLookup result) {
	if (result.entry && result.entry->type == CatalogType::VIEW_ENTRY) {
		result.entry = &result.entry->Cast<HiveView>().ForReading(transaction.GetContext());
	}
	return result;
}

TableLookup HiveCatalog::TryLookupTableBeforeSchema(CatalogTransaction transaction, const EntryLookupInfo &lookup_info,
                                                    const string &database_name, CatalogEntryLookup &result) {
	// The metastore keeps database names in lower case, as GetDatabase would name the schema
	HiveDatabaseInfo database;
	database.name = StringUtil::Lower(database_name);
	if (schemas.GetCachedEntry(database.name)) {
		return TableLookup::SKIPPED;
	}
	auto schema = schemas.CreateSchemaEntry(database, false);
	auto entry = schema->LookupEntry(transaction, lookup_info);
	if (!entry) {
		// the table or its schema is missing: GetDatabase tells which, without asking for the table again
		auto loaded = schemas.GetEntry(transaction.GetContext(), database.name);
		if (!loaded) {
			return TableLookup::SCHEMA_NOT_FOUND;
		}
		result.schema = &loaded->Cast<SchemaCatalogEntry>();
		return TableLookup::TABLE_NOT_FOUND;
	}
	// The set's lock is not held across the GetTable above, so another lookup can have added the schema since the
	// GetCachedEntry check. Then that entry is kept and the regular lookup finds the table in it, from the cache.
	auto added = schemas.AddEntryIfAbsent(std::move(schema));
	if (!added) {
		return TableLookup::SKIPPED;
	}
	result.schema = &added->Cast<SchemaCatalogEntry>();
	result.entry = entry;
	return TableLookup::TABLE_FOUND;
}

bool HiveCatalog::IsHiveCatalog(Catalog &catalog) {
	return dynamic_cast<HiveCatalog *>(&catalog) != nullptr;
}

void HiveCatalog::CheckWritable(const string &operation) const {
	if (access_mode == AccessMode::READ_ONLY) {
		throw InvalidInputException("Cannot execute %s %s on database %s which is attached in read-only mode!",
		                            Metastore().ServiceName(), operation, GetName());
	}
}

void HiveCatalog::ThrowIfInExplicitTransaction(ClientContext &context) {
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException("This connection is currently in a transaction. Transaction support is not "
		                           "provided for Hive catalogs. Please call COMMIT, ABORT or ROLLBACK before trying "
		                           "the query again");
	}
}

void HiveCatalog::ThrowIfInExplicitTransaction(ClientContext &context, const char *function) {
	if (!context.transaction.IsAutoCommit()) {
		throw TransactionException(
		    "%s changes the metastore, which has no transactions, so it can not run inside a transaction: "
		    "run it as a statement of its own, CALL %s(...), outside BEGIN ... COMMIT",
		    function, function);
	}
}

HiveTable &HiveCatalog::GetHiveTableForDML(ClientContext &context, TableCatalogEntry &table, const char *statement,
                                           HiveTableInfo &table_info) {
	auto &hive_table = table.Cast<HiveTable>();
	auto service_name = hive_table.catalog.Cast<HiveCatalog>().Metastore().ServiceName();
	table_info = hive_table.schema.Cast<HiveSchemaEntry>().DefinitionForWrite(context, hive_table);
	auto format = table_info.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("%s on %s table '%s' with type %s is not supported, only Hive tables can be "
		                              "written",
		                              statement, service_name, table.name.GetIdentifierName(),
		                              format.Describe(table_info));
	}
	// an InputFormat of another engine over a Parquet or text SerDe is a layout DuckDB does not know
	auto file_format = table_info.GetFileFormat();
	if (!format.IsWritable(table_info, file_format)) {
		throw NotImplementedException("%s into %s table '%s' is not supported: its InputFormat '%s' is not one Hive "
		                              "uses for %s tables, so DuckDB does not know how the files below the table "
		                              "location are laid out",
		                              statement, service_name, table.name.GetIdentifierName(), table_info.input_format,
		                              HiveFileFormatToString(file_format));
	}
	// the partitions an INSERT registers are ignored by Athena, and the files may belong under a location template
	if (table_info.IsPartitionProjected()) {
		throw NotImplementedException("%s into %s table '%s' is not supported: the table uses Athena partition "
		                              "projection (projection.enabled), so the partitions it would register are "
		                              "ignored by Athena. Set projection.enabled to false and register the partitions "
		                              "to write it",
		                              statement, service_name, table.name.GetIdentifierName());
	}
	// Hive and Spark bucket with different hash functions and file names; neither is implemented
	if (table_info.IsBucketed()) {
		throw NotImplementedException("%s into %s table '%s' is not supported: the table is %s, and DuckDB does not "
		                              "write a bucketed layout.",
		                              statement, service_name, table.name.GetIdentifierName(),
		                              table_info.DescribeBucketing());
	}
	// Athena writes JSON only with the Hive JsonSerDe; OpenX properties (mapping.*, dots.in.keys) would not find the
	// keys DuckDB writes
	if (table_info.GetSerDe() == HiveSerDe::OPENX_JSON) {
		throw NotImplementedException("%s into %s table '%s' is not supported: the table uses the OpenX JSON SerDe "
		                              "'%s', DuckDB writes JSON tables only with the Hive JsonSerDe "
		                              "(org.apache.hive.hcatalog.data.JsonSerDe)",
		                              statement, service_name, table.name.GetIdentifierName(),
		                              table_info.serde_library);
	}
	return hive_table;
}

//===--------------------------------------------------------------------===//
// Planning
//===--------------------------------------------------------------------===//
PhysicalOperator &HiveCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                          optional_ptr<PhysicalOperator> plan) {
	ThrowIfInExplicitTransaction(context);
	HiveTableInfo table_info;
	auto &hive_table = GetHiveTableForDML(context, op.table, "INSERT", table_info);
	// an INSERT writes by path, without the credentials the table's scans read with
	// TODO: support INSERT into a table with credentials, by writing its files with them
	if (!HiveFileCredentialsCallback::Get(context, hive_table).empty()) {
		throw NotImplementedException("INSERT into %s table '%s', which is read with file credentials, is not "
		                              "supported",
		                              Metastore().ServiceName(), op.table.name.GetIdentifierName());
	}
	return HiveInsert::PlanInsert(context, planner, op, hive_table, table_info, plan);
}

PhysicalOperator &HiveCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalCreateTable &op, PhysicalOperator &plan) {
	return HiveInsert::PlanCreateTableAs(context, planner, op, plan);
}

PhysicalOperator &HiveCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("DELETE is not supported for %s tables", Metastore().ServiceName());
}

PhysicalOperator &HiveCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                          PhysicalOperator &plan) {
	throw NotImplementedException("UPDATE is not supported for %s tables", Metastore().ServiceName());
}

PhysicalOperator &HiveCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                             LogicalMergeInto &op, PhysicalOperator &plan) {
	throw NotImplementedException("MERGE INTO is not supported for %s tables", Metastore().ServiceName());
}

unique_ptr<LogicalOperator> HiveCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                         TableCatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("Indexes are not supported for %s catalogs", Metastore().ServiceName());
}

//===--------------------------------------------------------------------===//
// Misc
//===--------------------------------------------------------------------===//
DatabaseSize HiveCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	return size;
}

bool HiveCatalog::InMemory() {
	return false;
}

string HiveCatalog::GetDBPath() {
	return options.path;
}

} // namespace duckdb
