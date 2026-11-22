#include "hive/catalog/hive_schema_entry.hpp"
#include "hive/catalog/hive_view.hpp"
#include "hive/catalog/hive_view_names.hpp"
#include "hive/catalog/scan_filter_equality.hpp"

#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/catalog/entry_scan_filters.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/parsed_data/comment_on_column_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/enum_util.hpp"

#include <algorithm>
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/table_function_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"

#include "hive/core/hive_types.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "hive/catalog/hive_view.hpp"
#include "hive/metastore/hive_metastore.hpp"
#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"

namespace duckdb {

HiveSchemaEntry::HiveSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, HiveDatabaseInfo database_info_p,
                                 bool schema_info_loaded)
    : SchemaCatalogEntry(catalog, info), tables(*this), database_info(std::move(database_info_p)),
      schema_info_loaded(schema_info_loaded) {
}

HiveSchemaEntry::~HiveSchemaEntry() {
}

bool HiveSchemaEntry::CatalogTypeIsSupported(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
	case CatalogType::VIEW_ENTRY:
		return true;
	default:
		return false;
	}
}

//===--------------------------------------------------------------------===//
// Create / Drop / Alter
//===--------------------------------------------------------------------===//
vector<pair<string, Value>>
HiveSchemaEntry::EvaluateOptions(ClientContext &context,
                                 const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options,
                                 const string &statement) {
	vector<pair<string, Value>> result;
	if (options.empty()) {
		return result;
	}
	auto binder = Binder::CreateBinder(context);
	TableFunctionBinder option_binder(*binder, context, statement + " options");
	for (auto &option : options) {
		auto expr_copy = option.second->Copy();
		auto bound_expr = option_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		if (value.IsNull()) {
			throw BinderException("NULL is not a valid value for %s option '%s'", statement, option.first);
		}
		result.emplace_back(option.first, std::move(value));
	}
	return result;
}

bool HiveSchemaEntry::IsBucketingOption(const string &key) {
	return StringUtil::CIEquals(key, "BucketColumns") || StringUtil::CIEquals(key, "NumberOfBuckets") ||
	       StringUtil::CIEquals(key, "SortColumns");
}

static string LookupSpelling(const case_insensitive_map_t<string> &spellings, const string &key) {
	auto entry = spellings.find(key);
	return entry == spellings.end() ? string() : entry->second;
}

//! The bucketing option another spelling stands for, empty if the key is none of them
static string BucketingOptionFor(const string &key) {
	static const case_insensitive_map_t<string> SPELLINGS = {
	    {"bucket_columns", "BucketColumns"}, {"bucketed_by", "BucketColumns"},
	    {"clustered_by", "BucketColumns"},   {"number_of_buckets", "NumberOfBuckets"},
	    {"bucket_count", "NumberOfBuckets"}, {"sort_columns", "SortColumns"},
	    {"sorted_by", "SortColumns"}};
	return LookupSpelling(SPELLINGS, key);
}

//! The spelling to use for another engine's CREATE TABLE option, empty if the key is none of them
static string OtherEngineOptionFor(const string &key) {
	static constexpr const char *PARTITIONED_BY_CLAUSE = "a PARTITIONED BY (...) clause";
	static const case_insensitive_map_t<string> SPELLINGS = {
	    {"partitioned_by", PARTITIONED_BY_CLAUSE},  {"partitioning", PARTITIONED_BY_CLAUSE},
	    {"partition_by", PARTITIONED_BY_CLAUSE},    {"partition_columns", PARTITIONED_BY_CLAUSE},
	    {"partition_keys", PARTITIONED_BY_CLAUSE},  {"PartitionKeys", PARTITIONED_BY_CLAUSE},
	    {"external_location", "option 'location'"}, {"field_delimiter", "option 'delimiter'"}};
	return LookupSpelling(SPELLINGS, key);
}

static void CheckBucketedTablesEnabled(ClientContext &context, const string &key) {
	Value enabled;
	if (context.TryGetCurrentSetting("hive_create_bucketed_tables", enabled) && !enabled.IsNull() &&
	    enabled.GetValue<bool>()) {
		return;
	}
	throw BinderException("CREATE TABLE option '%s' creates a bucketed Hive table, which DuckDB can read but not "
	                      "write (INSERT into it is refused). SET hive_create_bucketed_tables = true to create one",
	                      key);
}

static optional<int32_t> ToInteger(const Value &value) {
	if (!value.type().IsIntegral()) {
		return nullopt;
	}
	auto integer = value.DefaultTryCastAs(LogicalType::INTEGER);
	if (!integer) {
		return nullopt;
	}
	return integer->GetValue<int32_t>();
}

static vector<string> ParseBucketColumns(const string &key, const Value &value) {
	if (value.type().id() != LogicalTypeId::LIST) {
		throw BinderException("CREATE TABLE option '%s' must be a list of column names, e.g. ['id'], got '%s'", key,
		                      value.ToString());
	}
	vector<string> result;
	for (auto &entry : ListValue::GetChildren(value)) {
		if (entry.IsNull()) {
			throw BinderException("CREATE TABLE option '%s' can not contain NULL", key);
		}
		result.push_back(entry.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
	}
	if (result.empty()) {
		throw BinderException("CREATE TABLE option '%s' needs at least one column", key);
	}
	return result;
}

static int32_t ParseNumberOfBuckets(const string &key, const Value &value) {
	auto count = ToInteger(value);
	if (!count || *count <= 0) {
		throw BinderException("CREATE TABLE option '%s' must be a positive integer, got '%s'", key, value.ToString());
	}
	return *count;
}

static HiveSortOrder ParseSortOrder(const string &key, const Value &value) {
	// Hive's sort order is 1 for ascending, 0 for descending
	auto sort_order = ToInteger(value);
	if (sort_order && *sort_order == 1) {
		return HiveSortOrder::ASCENDING;
	}
	if (sort_order && *sort_order == 0) {
		return HiveSortOrder::DESCENDING;
	}
	throw BinderException("SortOrder in CREATE TABLE option '%s' must be 1 (ascending) or 0 (descending), got '%s'",
	                      key, value.ToString());
}

static vector<HiveColumn> ParseSortColumns(const string &key, const Value &value) {
	auto &type = value.type();
	if (type.id() != LogicalTypeId::LIST || ListType::GetChildType(type).id() != LogicalTypeId::STRUCT) {
		throw BinderException(
		    "CREATE TABLE option '%s' must be a list of {'Column': name, 'SortOrder': 1 or 0}, got '%s'", key,
		    value.ToString());
	}
	vector<HiveColumn> result;
	for (auto &entry : ListValue::GetChildren(value)) {
		if (entry.IsNull()) {
			throw BinderException("CREATE TABLE option '%s' can not contain NULL", key);
		}
		auto &fields = StructType::GetChildTypes(entry.type());
		auto &values = StructValue::GetChildren(entry);
		HiveColumn sort_column;
		for (idx_t i = 0; i < fields.size(); i++) {
			auto &field = fields[i].first.GetIdentifierName();
			auto is_column = StringUtil::CIEquals(field, "Column");
			if (!is_column && !StringUtil::CIEquals(field, "SortOrder")) {
				throw BinderException("CREATE TABLE option '%s' takes structs of 'Column' and 'SortOrder', got '%s'",
				                      key, field);
			}
			if (values[i].IsNull()) {
				continue;
			}
			if (is_column) {
				sort_column.name = values[i].DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
			} else {
				sort_column.sort_order = ParseSortOrder(key, values[i]);
			}
		}
		if (sort_column.name.empty() || sort_column.sort_order == HiveSortOrder::UNSORTED) {
			throw BinderException(
			    "Every entry of CREATE TABLE option '%s' needs a 'Column' and a 'SortOrder', got '%s'", key,
			    entry.ToString());
		}
		result.push_back(std::move(sort_column));
	}
	if (result.empty()) {
		throw BinderException("CREATE TABLE option '%s' needs at least one column", key);
	}
	return result;
}

static void CheckNoDuplicates(const char *option, const vector<string> &names) {
	for (idx_t i = 0; i < names.size(); i++) {
		for (idx_t j = 0; j < i; j++) {
			if (StringUtil::CIEquals(names[i], names[j])) {
				throw BinderException("%s lists column '%s' twice", option, names[i]);
			}
		}
	}
}

//! Hive's CLUSTERED BY (...) [SORTED BY (...)] INTO n BUCKETS, over columns that are not partition columns
static void ResolveBucketing(HiveCreateTableOptions &options, const ColumnList &columns,
                             const vector<string> &partition_columns, const string &table_name) {
	if (options.bucket_columns.empty()) {
		if (options.number_of_buckets != -1 || !options.sort_columns.empty()) {
			throw BinderException("CREATE TABLE options NumberOfBuckets and SortColumns need BucketColumns");
		}
		return;
	}
	if (options.number_of_buckets == -1) {
		throw BinderException("CREATE TABLE option BucketColumns needs NumberOfBuckets");
	}
	auto resolve = [&](const char *option, const string &name) -> string {
		for (auto &partition_column : partition_columns) {
			if (StringUtil::CIEquals(partition_column, name)) {
				throw BinderException("%s column '%s' is a partition column of table '%s'", option, name, table_name);
			}
		}
		for (auto &column : columns.Physical()) {
			if (StringUtil::CIEquals(column.Name().GetIdentifierName(), name)) {
				return column.Name().GetIdentifierName();
			}
		}
		throw BinderException("%s column '%s' is not a column of table '%s'", option, name, table_name);
	};
	for (auto &name : options.bucket_columns) {
		name = resolve("BucketColumns", name);
	}
	vector<string> sort_names;
	for (auto &sort_column : options.sort_columns) {
		sort_column.name = resolve("SortColumns", sort_column.name);
		sort_names.push_back(sort_column.name);
	}
	CheckNoDuplicates("BucketColumns", options.bucket_columns);
	CheckNoDuplicates("SortColumns", sort_names);
}

HiveCreateTableOptions HiveSchemaEntry::ParseCreateTableOptions(ClientContext &context,
                                                                const CreateTableInfo &create_info) {
	HiveCreateTableOptions result;
	for (auto &option : EvaluateOptions(context, create_info.options, "CREATE TABLE")) {
		auto &key = option.first;
		auto &value = option.second;
		auto string_value = value.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();

		if (StringUtil::CIEquals(key, "type")) {
			// only Hive tables can be created
			if (StringUtil::Upper(string_value) != "HIVE") {
				throw BinderException("Unknown Hive table type '%s' for option 'type', only 'HIVE' is supported",
				                      string_value);
			}
		} else if (StringUtil::CIEquals(key, "format")) {
			result.format = HiveFileFormatFromString(string_value);
		} else if (StringUtil::CIEquals(key, "location")) {
			result.location = string_value;
			StringUtil::RTrim(result.location, "/");
		} else if (StringUtil::CIEquals(key, "comment")) {
			// Stored as the table's description
			result.description = string_value;
		} else if (StringUtil::CIEquals(key, "delimiter") || StringUtil::CIEquals(key, "quote") ||
		           StringUtil::CIEquals(key, "escape")) {
			// the csv dialect: Hive SerDes take single characters
			if (string_value.size() != 1) {
				throw BinderException("CREATE TABLE option '%s' must be a single character, got '%s'", key,
				                      string_value);
			}
			if (StringUtil::CIEquals(key, "delimiter")) {
				result.csv_delimiter = string_value;
			} else if (StringUtil::CIEquals(key, "quote")) {
				result.csv_quote = string_value;
			} else {
				result.csv_escape = string_value;
			}
		} else if (StringUtil::CIEquals(key, "overwrite")) {
			result.overwrite = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
		} else if (StringUtil::CIEquals(key, "header")) {
			// csv files with a header line: Hive's "TBLPROPERTIES ('skip.header.line.count' = '1')"
			if (value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>()) {
				result.parameters["skip.header.line.count"] = "1";
			}
		} else if (IsBucketingOption(key)) {
			CheckBucketedTablesEnabled(context, key);
			if (StringUtil::CIEquals(key, "BucketColumns")) {
				result.bucket_columns = ParseBucketColumns(key, value);
			} else if (StringUtil::CIEquals(key, "NumberOfBuckets")) {
				result.number_of_buckets = ParseNumberOfBuckets(key, value);
			} else {
				result.sort_columns = ParseSortColumns(key, value);
			}
		} else if (!BucketingOptionFor(key).empty()) {
			// stored as a table parameter this would silently leave the table unbucketed
			throw BinderException("Unknown CREATE TABLE option '%s', a bucketed Hive table is created with option '%s'",
			                      key, BucketingOptionFor(key));
		} else if (!OtherEngineOptionFor(key).empty()) {
			throw BinderException("Unknown CREATE TABLE option '%s', use %s instead", key, OtherEngineOptionFor(key));
		} else {
			// everything else is a table property, stored in the table parameters (like Hive / Trino do)
			result.parameters[key] = string_value;
		}
	}
	return result;
}

static string ServiceName(Catalog &catalog) {
	return catalog.Cast<HiveCatalog>().Metastore().ServiceName();
}

static void ThrowIfCollated(const LogicalType &type, const string &column_name, const string &service_name) {
	auto collation = StringType::GetCollation(type);
	if (!collation.empty()) {
		throw NotImplementedException("Collation \"%s\" of column \"%s\" is not supported for tables in a %s catalog",
		                              collation, column_name, service_name);
	}
}

//! A Hive column is a name and a type: refuse what DuckDB would otherwise drop when the column is stored
static void ThrowIfUnsupportedColumn(const ColumnDefinition &column, const string &service_name) {
	auto &name = column.Name().GetIdentifierName();
	if (column.Generated()) {
		throw NotImplementedException("Generated column \"%s\" is not supported for tables in a %s catalog", name,
		                              service_name);
	}
	if (column.HasDefaultValue()) {
		throw NotImplementedException("Default value of column \"%s\" is not supported for tables in a %s catalog",
		                              name, service_name);
	}
	ThrowIfCollated(column.Type(), name, service_name);
}

//! Tables and views share one catalog set (as in DuckDB's own schema), so a lookup by name returns either; the
//! statements that mean one of the two check it here
static void CheckEntryType(optional_ptr<CatalogEntry> existing, CatalogType expected, const string &name,
                           const char *action) {
	if (existing && existing->type != expected) {
		throw CatalogException("Existing object %s is of type %s, trying to %s type %s", name,
		                       CatalogTypeToString(existing->type), action, CatalogTypeToString(expected));
	}
}

static optional_ptr<CatalogEntry> OnExistingTable(const CreateTableInfo &base, const string &database_name,
                                                  const string &table_name, const string &service_name) {
	switch (base.on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return nullptr;
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("Table with name \"%s\" already exists in %s database \"%s\"", table_name, service_name,
		                       database_name);
	default:
		throw NotImplementedException(
		    "CREATE OR REPLACE TABLE is not supported for %s catalogs, use separate DROP and CREATE statements",
		    service_name);
	}
}

//! Whether CREATE VIEW goes on to replace the existing view: not for IF NOT EXISTS, an error for a plain CREATE
static bool ReplacesExistingView(const CreateViewInfo &info, const HiveView &existing, const string &database_name,
                                 const string &view_name, const string &service_name) {
	switch (info.on_conflict) {
	case OnCreateConflict::IGNORE_ON_CONFLICT:
		return false;
	case OnCreateConflict::ERROR_ON_CONFLICT:
		throw CatalogException("View with name \"%s\" already exists in %s database \"%s\"", view_name, service_name,
		                       database_name);
	default:
		if (!existing.IsDuckDBView()) {
			throw CatalogException("%s view \"%s\" was not written by DuckDB; replace it from the engine that wrote it",
			                       service_name, view_name);
		}
		return true;
	}
}

HiveTableInfo HiveSchemaEntry::BuildTableInfo(ClientContext &context, const CreateTableInfo &base) {
	if (!base.constraints.empty()) {
		throw NotImplementedException("Constraints are not supported when creating tables in a %s catalog",
		                              ServiceName(catalog));
	}
	for (auto &column : base.columns.Logical()) {
		ThrowIfUnsupportedColumn(column, ServiceName(catalog));
	}
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto table_name = base.GetTableName().GetIdentifierName();
	auto options = ParseCreateTableOptions(context, base);
	// Hive partitions are columns: PARTITIONED BY must name columns of the table, which become the PartitionKeys
	// (in the given order) and are stored in the directory names rather than in the data files.
	vector<string> partition_columns;
	for (auto &key : base.partition_keys) {
		if (key->GetExpressionType() != ExpressionType::COLUMN_REF) {
			throw BinderException("PARTITIONED BY for Hive tables only supports column names, got '%s'",
			                      key->ToString());
		}
		auto &column_ref = key->Cast<ColumnRefExpression>();
		if (column_ref.IsQualified()) {
			throw BinderException("PARTITIONED BY for Hive tables only supports plain column names, got '%s'",
			                      key->ToString());
		}
		auto &column_name = column_ref.GetColumnName().GetIdentifierName();
		if (!base.columns.ColumnExists(column_ref.GetColumnName())) {
			throw BinderException("PARTITIONED BY column '%s' is not a column of table '%s'", column_name, table_name);
		}
		for (auto &existing : partition_columns) {
			if (StringUtil::CIEquals(existing, column_name)) {
				throw BinderException("PARTITIONED BY column '%s' is listed twice for table '%s'", column_name,
				                      table_name);
			}
		}
		partition_columns.push_back(column_name);
	}
	auto is_partition_column = [&](const string &name) {
		for (auto &partition_column : partition_columns) {
			if (StringUtil::CIEquals(partition_column, name)) {
				return true;
			}
		}
		return false;
	};
	ResolveBucketing(options, base.columns, partition_columns, table_name);

	HiveTableInfo table;
	table.name = table_name;
	table.schema_name = database_info.name;
	// Table description: `WITH (comment = '...')` is the primary spelling (DuckDB's parser accepts a trailing
	// `COMMENT '...'` only on CREATE SCHEMA, not CREATE TABLE); CreateInfo::comment is a Value and set by a
	// future parser or an engine wrapper, so honor it too and let the WITH value override when both are given.
	if (!base.comment.IsNull()) {
		table.description = base.comment.ToString();
	}
	if (!options.description.empty()) {
		table.description = options.description;
	}
	if (options.location.empty()) {
		// the database's LocationUri only matters without DEFAULT_LOCATION, which wins over it
		auto &database = hive_catalog.options.default_location.empty() ? GetSchemaInfo(context) : database_info;
		table.location = hive_catalog.GetTableLocation(database, table_name);
	} else {
		table.location = options.location;
	}
	table.parameters = options.parameters;
	table.file_format = options.format;
	if (table.file_format == HiveFileFormat::CSV) {
		// with a quote or escape character OpenCSVSerde (which quotes), without both LazySimpleSerDe (which does not)
		if (options.csv_quote.empty() && options.csv_escape.empty()) {
			table.serde_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
			table.serde_parameters["field.delim"] = options.csv_delimiter;
			table.serde_parameters["serialization.format"] = options.csv_delimiter;
		} else {
			// the escape character defaults to the quote character, like DuckDB's COPY
			string quote = options.csv_quote.empty() ? "\"" : options.csv_quote;
			table.serde_library = "org.apache.hadoop.hive.serde2.OpenCSVSerde";
			table.serde_parameters["separatorChar"] = options.csv_delimiter;
			table.serde_parameters["quoteChar"] = quote;
			table.serde_parameters["escapeChar"] = options.csv_escape.empty() ? quote : options.csv_escape;
		}
	}
	table.bucket_columns = options.bucket_columns;
	table.number_of_buckets = options.number_of_buckets;
	table.sort_columns = options.sort_columns;
	for (auto &column : base.columns.Physical()) {
		if (is_partition_column(column.Name().GetIdentifierName())) {
			continue;
		}
		HiveColumn hive_column;
		hive_column.name = column.Name().GetIdentifierName();
		hive_column.type = HiveTypes::FromLogicalType(column.Type());
		if (!column.Comment().IsNull()) {
			hive_column.comment = column.Comment().ToString();
		}
		table.columns.push_back(std::move(hive_column));
	}
	for (auto &partition_column : partition_columns) {
		auto &column = base.columns.GetColumn(Identifier(partition_column));
		HiveColumn hive_column;
		hive_column.name = column.Name().GetIdentifierName();
		hive_column.type = HiveTypes::FromLogicalType(column.Type());
		if (!column.Comment().IsNull()) {
			hive_column.comment = column.Comment().ToString();
		}
		table.partition_keys.push_back(std::move(hive_column));
	}
	if (table.columns.empty()) {
		throw BinderException("Table '%s' needs at least one column that is not a partition column", table_name);
	}
	return table;
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	return CreateTableInternal(transaction, info, nullptr);
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateTableFromInfo(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info,
                                                                const HiveTableInfo &table_info) {
	return CreateTableInternal(transaction, info, &table_info);
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateTableInternal(CatalogTransaction transaction,
                                                                BoundCreateTableInfo &info,
                                                                const HiveTableInfo *resolved_table_info) {
	auto &context = transaction.GetContext();
	HiveCatalog::ThrowIfInExplicitTransaction(context);
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto &base = info.Base();
	auto table_name = base.GetTableName().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	CheckEntryType(existing, CatalogType::TABLE_ENTRY, table_name, "create");
	if (existing) {
		return OnExistingTable(base, database_info.name, table_name, ServiceName(catalog));
	}
	// CREATE TABLE AS creates its table through CreateTableFromInfo
	if (!resolved_table_info && base.options.find("overwrite") != base.options.end()) {
		throw BinderException("CREATE TABLE option 'overwrite' is only for CREATE TABLE ... AS: a table created "
		                      "without a query writes no files");
	}
	auto table = resolved_table_info ? *resolved_table_info : BuildTableInfo(context, base);
	D_ASSERT(table.name == table_name);
	D_ASSERT(table.schema_name == database_info.name);
	if (hive_catalog.Metastore().CreateHiveTable(context, table) == HiveCreateOutcome::ALREADY_EXISTED) {
		// another client created the table since the lookup above: take its entry
		auto raced = tables.GetEntry(context, lookup);
		if (!raced) {
			throw CatalogException("%s table \"%s.%s\" exists but could not be fetched", ServiceName(catalog),
			                       database_info.name, table_name);
		}
		CheckEntryType(raced, CatalogType::TABLE_ENTRY, table_name, "create");
		return OnExistingTable(base, database_info.name, table_name, ServiceName(catalog));
	}

	// re-fetch so the entry reflects what the metastore stored
	HiveTableInfo created;
	if (!hive_catalog.Metastore().GetTable(context, database_info.name, table_name, created)) {
		throw CatalogException("%s table \"%s.%s\" was created but could not be fetched afterwards",
		                       ServiceName(catalog), database_info.name, table_name);
	}
	return tables.CreateEntry(tables.CreateEntry(created));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	throw BinderException("%s databases do not support creating functions", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                        TableCatalogEntry &table) {
	throw BinderException("%s databases do not support creating indexes", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto &context = transaction.GetContext();
	HiveCatalog::ThrowIfInExplicitTransaction(context);
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto view_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::VIEW_ENTRY, QualifiedName(Identifier(view_name)));
	auto existing = tables.GetEntry(context, lookup);
	CheckEntryType(existing, CatalogType::VIEW_ENTRY, view_name, "create");
	if (existing &&
	    !ReplacesExistingView(info, existing->Cast<HiveView>(), database_info.name, view_name, ServiceName(catalog))) {
		return existing;
	}
	if (info.binding_mode == CreateViewBindingMode::SKIP_BINDING) {
		throw NotImplementedException("%s views can not be created WITH (DEFER_BINDING) yet", ServiceName(catalog));
	}

	HiveViewInfo view;
	view.database_name = database_info.name;
	view.name = view_name;
	// a DuckDB view's ViewOriginalText: the user's SQL; ViewExpandedText: tables of this catalog as db.t, of other
	// catalogs as "external:<type>:<alias>"
	view.sql = HiveView::RenderViewSql(info);
	auto expanded = unique_ptr_cast<SQLStatement, SelectStatement>(info.query->Copy());
	HiveViewNames::QualifyForStorage(context, hive_catalog, name, *expanded->node);
	view.expanded_sql = expanded->ToString();
	// names were resolved by lookups, not the binder: check they bind to the same column types (not names: an
	// unaliased expression is named after its text, which the rewrite changes)
	auto check = unique_ptr_cast<SQLStatement, SelectStatement>(expanded->Copy());
	auto aliases = HiveViewNames::AttachedAliases(hive_catalog, HiveViewNames::ReferencedCatalogIds(*check->node));
	HiveViewNames::ResolveForReading(hive_catalog, database_info.name + "." + view_name, aliases, *check->node);
	HiveViewNames::PinToCatalog(*check->node, catalog.GetName(), name);
	HiveViewNames::MapIcebergTables(context, hive_catalog, database_info.name + "." + view_name, *check->node);
	vector<LogicalType> types;
	vector<Identifier> names;
	// the entries the query reads in this catalog, through other views too: the view being replaced is among them
	// if its new query would read itself under this alias (not when a path reaches it under another alias)
	LogicalDependencyList reads;
	Binder::BindView(context, *check, catalog.GetName(), GetSchemaPath(), &reads, info.aliases, types, names);
	if (existing && reads.Contains(*existing)) {
		throw BinderException("%s view \"%s.%s\" can not be created: its query reads the view itself",
		                      ServiceName(catalog), database_info.name, view_name);
	}
	if (types != info.types) {
		throw BinderException("The ViewExpandedText of %s view \"%s\" binds to other column types than the view "
		                      "query:\n%s",
		                      ServiceName(catalog), view_name, view.expanded_sql);
	}
	for (auto &text : {view.sql, view.expanded_sql}) {
		try {
			// what is stored must read back: never write a view DuckDB itself could not parse
			auto parser = Parser::GetBuiltinParser();
			CreateViewInfo::ParseSelect(parser, text);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			throw InternalException("The SQL rendered for %s view \"%s\" does not parse: %s\n%s", ServiceName(catalog),
			                        view_name, error.RawMessage(), text);
		}
	}
	view.secure = info.security_type == ViewSecurityType::SECURE_VIEW;
	view.has_column_alias_list = !info.aliases.empty();
	// the stored columns carry the column alias list of CREATE VIEW: an alias replaces the query's own name
	for (idx_t i = 0; i < info.types.size(); i++) {
		HiveColumn column;
		auto aliased = i < info.aliases.size() && !info.aliases[i].GetIdentifierName().empty();
		column.name = (aliased ? info.aliases[i] : info.names[i]).GetIdentifierName();
		column.type = HiveTypes::FromLogicalTypeForView(info.types[i]);
		view.columns.push_back(std::move(column));
	}
	// re-fetch so the entry reflects what the metastore stored
	auto fetch_stored = [&]() {
		tables.RemoveEntry(view_name);
		HiveTableInfo stored;
		if (!hive_catalog.Metastore().GetTable(context, database_info.name, view_name, stored)) {
			throw CatalogException("%s view \"%s.%s\" was created but could not be fetched afterwards",
			                       ServiceName(catalog), database_info.name, view_name);
		}
		return tables.CreateEntry(tables.CreateEntry(stored));
	};
	if (!existing) {
		if (hive_catalog.Metastore().CreateView(context, view) == HiveCreateOutcome::CREATED) {
			return fetch_stored();
		}
		// another client created the view since the lookup above: take its entry
		existing = tables.GetEntry(context, lookup);
		if (!existing) {
			throw CatalogException("%s view \"%s.%s\" exists but could not be fetched", ServiceName(catalog),
			                       database_info.name, view_name);
		}
		CheckEntryType(existing, CatalogType::VIEW_ENTRY, view_name, "create");
		if (!ReplacesExistingView(info, existing->Cast<HiveView>(), database_info.name, view_name,
		                          ServiceName(catalog))) {
			return existing;
		}
	}
	hive_catalog.Metastore().UpdateView(context, view, DefinitionForWrite(context, *existing));
	return fetch_stored();
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	throw BinderException("%s databases do not support creating sequences", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                CreateTableFunctionInfo &info) {
	throw BinderException("%s databases do not support creating table functions", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                               CreateCopyFunctionInfo &info) {
	throw BinderException("%s databases do not support creating copy functions", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                 CreatePragmaFunctionInfo &info) {
	throw BinderException("%s databases do not support creating pragma functions", ServiceName(catalog));
}

optional_ptr<CatalogEntry> HiveSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw BinderException("%s databases do not support creating types", ServiceName(catalog));
}

bool HiveSchemaEntry::IsAllowedHiveTypeChange(const LogicalType &from, const LogicalType &to) {
	if (from == to) {
		return true;
	}
	auto rank = [](LogicalTypeId id) -> int {
		switch (id) {
		case LogicalTypeId::TINYINT:
			return 1;
		case LogicalTypeId::SMALLINT:
			return 2;
		case LogicalTypeId::INTEGER:
			return 3;
		case LogicalTypeId::BIGINT:
			return 4;
		default:
			return 0;
		}
	};
	if (rank(from.id()) > 0 && rank(to.id()) > 0) {
		return rank(to.id()) > rank(from.id());
	}
	if (from.id() == LogicalTypeId::FLOAT && to.id() == LogicalTypeId::DOUBLE) {
		return true;
	}
	if (to.id() == LogicalTypeId::VARCHAR) {
		// everything can be read as a string
		return true;
	}
	return false;
}

//! The parameters HiveTableInfo::GetFormat() derives the table format from can not be set or reset: changing
//! table_type on a Hive table would relabel it as Iceberg or Delta without a metadata file behind it.
static void CheckTablePropertyChangeable(const string &key) {
	if (key.empty()) {
		throw InvalidInputException("A table property needs a name");
	}
	if (MetastoreTableFormat::IsParameter(key)) {
		throw InvalidInputException("Table property '%s' decides how the table is read and can not be changed "
		                            "with ALTER TABLE",
		                            key);
	}
}

//! ALTER TABLE t SET (key = value, ...) / RESET (key, ...): Hive's SET / UNSET TBLPROPERTIES
void HiveSchemaEntry::AlterTableProperties(ClientContext &context, const HiveTableInfo &table,
                                           AlterTableInfo &alter_table) {
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	vector<pair<string, string>> set;
	vector<string> unset;
	if (alter_table.alter_table_type == AlterTableType::SET_TABLE_OPTIONS) {
		auto &options = alter_table.Cast<SetTableOptionsInfo>();
		for (auto &option : EvaluateOptions(context, options.table_options, "ALTER TABLE SET")) {
			CheckTablePropertyChangeable(option.first);
			// The metastore stores every parameter as a string
			set.emplace_back(option.first, option.second.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
		}
	} else {
		for (auto &option : alter_table.Cast<ResetTableOptionsInfo>().table_options) {
			auto key = option.GetIdentifierName();
			CheckTablePropertyChangeable(key);
			unset.push_back(std::move(key));
		}
	}
	hive_catalog.Metastore().UpdateTableParameters(context, table, set, unset);
	RefreshTable(context, table.name);
}

//! The metastore keeps BucketColumns and SortColumns as they are, so a column they name can not be dropped or renamed
static void CheckNotClustering(const HiveTableInfo &table, const string &table_name, const string &name,
                               const char *action) {
	for (auto &bucket_column : table.bucket_columns) {
		if (StringUtil::CIEquals(bucket_column, name)) {
			throw CatalogException("Column \"%s\" is a bucket column of table \"%s\" and can not be %s", name,
			                       table_name, action);
		}
	}
	for (auto &sort_column : table.sort_columns) {
		if (StringUtil::CIEquals(sort_column.name, name)) {
			throw CatalogException("Column \"%s\" is a sort column of table \"%s\" and can not be %s", name, table_name,
			                       action);
		}
	}
}

void HiveSchemaEntry::SetColumnComment(ClientContext &context, const HiveTableInfo &table, SetColumnCommentInfo &info) {
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto &table_name = table.name;
	auto &name = info.column_name.GetIdentifierName();
	auto by_name = [&](const HiveColumn &column) {
		return StringUtil::CIEquals(column.name, name);
	};
	auto partition_key = std::find_if(table.partition_keys.begin(), table.partition_keys.end(), by_name);
	if (partition_key != table.partition_keys.end()) {
		auto partition_keys = table.partition_keys;
		// NULL removes the comment
		partition_keys[partition_key - table.partition_keys.begin()].comment =
		    info.comment_value.IsNull() ? string() : info.comment_value.ToString();
		hive_catalog.Metastore().UpdateTablePartitionKeys(context, table, partition_keys);
		RefreshTable(context, table_name);
		return;
	}
	auto columns = table.columns;
	auto column = std::find_if(columns.begin(), columns.end(), by_name);
	if (column == columns.end()) {
		throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
	}
	// NULL removes the comment
	column->comment = info.comment_value.IsNull() ? string() : info.comment_value.ToString();
	hive_catalog.Metastore().UpdateTableColumns(context, table, columns);
	RefreshTable(context, table_name);
}

void HiveSchemaEntry::SetTableComment(ClientContext &context, const HiveTableInfo &table, SetCommentInfo &info) {
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	// NULL removes the description
	auto description = info.comment_value.IsNull() ? string() : info.comment_value.ToString();
	hive_catalog.Metastore().UpdateTableDescription(context, table, description);
	RefreshTable(context, table.name);
}

void HiveSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	auto &context = transaction.GetContext();
	HiveCatalog::ThrowIfInExplicitTransaction(context);
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)));
	auto entry = tables.GetEntry(context, lookup);
	if (!entry) {
		throw CatalogException("Table with name \"%s\" does not exist in %s database \"%s\"", table_name,
		                       ServiceName(catalog), database_info.name);
	}
	if (entry->type == CatalogType::VIEW_ENTRY) {
		if (info.type == AlterType::ALTER_VIEW) {
			throw NotImplementedException(
			    "%s cannot rename a view; use CREATE OR REPLACE VIEW under the new name and DROP "
			    "VIEW the old one",
			    ServiceName(catalog));
		}
		if (info.type == AlterType::SET_COMMENT || info.type == AlterType::SET_COLUMN_COMMENT) {
			throw NotImplementedException("Comments on %s views are not supported yet", ServiceName(catalog));
		}
		throw BinderException("\"%s\" is a view", table_name);
	}
	auto &hive_table = entry->Cast<HiveTable>();
	auto &current = DefinitionForWrite(context, hive_table);
	auto format = current.GetFormat();
	if (!format.IsHive()) {
		throw NotImplementedException("ALTER TABLE is only supported for Hive tables in a %s catalog, '%s' is a %s "
		                              "table",
		                              ServiceName(catalog), table_name, format.Describe(current));
	}
	if (info.type == AlterType::SET_COLUMN_COMMENT) {
		SetColumnComment(context, current, info.Cast<SetColumnCommentInfo>());
		return;
	}
	if (info.type == AlterType::SET_COMMENT) {
		SetTableComment(context, current, info.Cast<SetCommentInfo>());
		return;
	}
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("Only ALTER TABLE is supported for %s tables", ServiceName(catalog));
	}
	auto &alter_table = info.Cast<AlterTableInfo>();
	if (alter_table.alter_table_type == AlterTableType::SET_TABLE_OPTIONS ||
	    alter_table.alter_table_type == AlterTableType::RESET_TABLE_OPTIONS) {
		AlterTableProperties(context, current, alter_table);
		return;
	}

	auto find_column = [](vector<HiveColumn> &columns, const string &name) -> optional_ptr<HiveColumn> {
		for (auto &column : columns) {
			if (StringUtil::CIEquals(column.name, name)) {
				return &column;
			}
		}
		return nullptr;
	};
	auto is_partition_key = [&](const string &name) {
		return std::any_of(current.partition_keys.begin(), current.partition_keys.end(),
		                   [&](const HiveColumn &column) { return StringUtil::CIEquals(column.name, name); });
	};

	auto columns = current.columns;
	switch (alter_table.alter_table_type) {
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter_table.Cast<AddColumnInfo>();
		auto &name = add.new_column.Name().GetIdentifierName();
		if (find_column(columns, name) || is_partition_key(name)) {
			if (add.if_column_not_exists) {
				return;
			}
			throw CatalogException("Column with name \"%s\" already exists in table \"%s\"", name, table_name);
		}
		ThrowIfUnsupportedColumn(add.new_column, ServiceName(catalog));
		HiveColumn column;
		column.name = name;
		column.type = HiveTypes::FromLogicalType(add.new_column.Type());
		columns.push_back(std::move(column));
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove = alter_table.Cast<RemoveColumnInfo>();
		auto &name = remove.removed_column.GetIdentifierName();
		if (is_partition_key(name)) {
			throw CatalogException("Column \"%s\" is a partition key of table \"%s\" and can not be dropped", name,
			                       table_name);
		}
		if (!find_column(columns, name)) {
			if (remove.if_column_exists) {
				return;
			}
			throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
		}
		CheckNotClustering(current, table_name, name, "dropped");
		if (current.GetFileFormat() == HiveFileFormat::CSV) {
			throw CatalogException("Can not drop column \"%s\" from table \"%s\": DROP COLUMN is not "
			                       "supported for csv (text) tables",
			                       name, table_name);
		}

		if (columns.size() == 1) {
			throw CatalogException("Can not drop column \"%s\": table \"%s\" needs at least one column", name,
			                       table_name);
		}
		columns.erase(std::remove_if(columns.begin(), columns.end(),
		                             [&](const HiveColumn &column) { return StringUtil::CIEquals(column.name, name); }),
		              columns.end());
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename = alter_table.Cast<RenameColumnInfo>();
		auto &name = rename.old_name.GetIdentifierName();
		auto &new_name = rename.new_name.GetIdentifierName();
		auto column = find_column(columns, name);
		if (!column) {
			if (is_partition_key(name)) {
				throw CatalogException("Column \"%s\" is a partition key of table \"%s\" and can not be renamed: the "
				                       "existing partition directories are named after it (%s=...)",
				                       name, table_name, name);
			}
			throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
		}
		auto existing = find_column(columns, new_name);
		if ((existing && existing.get() != column.get()) || is_partition_key(new_name)) {
			throw CatalogException("Column with name \"%s\" already exists in table \"%s\"", new_name, table_name);
		}
		// csv files are read by position; the other formats match their columns by name
		if (current.GetFileFormat() != HiveFileFormat::CSV) {
			throw NotImplementedException(
			    "Can not rename column \"%s\" of table \"%s\": its data files are matched to the columns by name, so "
			    "the existing data would read as NULL; use hive_replace_columns to replace the column deliberately",
			    name, table_name);
		}
		CheckNotClustering(current, table_name, column->name, "renamed");
		column->name = new_name;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change = alter_table.Cast<ChangeColumnTypeInfo>();
		if (change.column_path.size() > 1) {
			throw NotImplementedException("Changing the type of a nested field is not yet supported");
		}
		auto &name = change.column_path[0].GetIdentifierName();
		if (is_partition_key(name)) {
			throw CatalogException("Column \"%s\" is a partition key of table \"%s\" and its type can not be "
			                       "changed",
			                       name, table_name);
		}
		auto column = find_column(columns, name);
		if (!column) {
			throw CatalogException("Table \"%s\" does not have a column with name \"%s\"", table_name, name);
		}
		ThrowIfCollated(change.target_type, name, ServiceName(catalog));
		// without USING the binder fills in CAST(column AS type)
		CastExpression plain_cast(change.target_type, make_uniq<ColumnRefExpression>(change.column_path));
		if (change.expression && !change.expression->Equals(plain_cast)) {
			throw NotImplementedException("ALTER COLUMN ... TYPE ... USING is not supported for Hive tables in a %s "
			                              "catalog: existing data files can not be rewritten",
			                              ServiceName(catalog));
		}

		auto from = HiveTypes::ToLogicalType(column->type);
		if (!IsAllowedHiveTypeChange(from, change.target_type)) {
			throw CatalogException("Can not change column \"%s\" of table \"%s\" from %s to %s: existing parquet "
			                       "files keep their types, only widening changes (e.g. INTEGER to BIGINT, FLOAT to "
			                       "DOUBLE, anything to VARCHAR) are supported for Hive tables",
			                       name, table_name, from.ToString(), change.target_type.ToString());
		}
		column->type = HiveTypes::FromLogicalType(change.target_type);
		break;
	}
	default:
		throw NotImplementedException("ALTER TABLE %s is not supported for %s tables",
		                              EnumUtil::ToString(alter_table.alter_table_type), ServiceName(catalog));
	}

	hive_catalog.Metastore().UpdateTableColumns(context, current, columns);
	RefreshTable(context, table_name);
}

HiveTable &HiveSchemaEntry::RefreshTable(ClientContext &context, const string &table_name) {
	HiveTableInfo updated;
	if (!catalog.Cast<HiveCatalog>().Metastore().GetTable(context, database_info.name, table_name, updated)) {
		throw CatalogException("Table \"%s.%s\" was altered but could not be fetched afterwards", database_info.name,
		                       table_name);
	}
	return tables.CreateEntry(tables.CreateEntry(updated))->Cast<HiveTable>();
}

const HiveTableInfo &HiveSchemaEntry::DefinitionForWrite(ClientContext &context, const CatalogEntry &entry) {
	auto &bound = HiveTableSet::GetTableInfo(entry);
	if (!HiveTransactionCache::ConnectionScoped(context)) {
		return bound;
	}
	HiveTableInfo current;
	if (!catalog.Cast<HiveCatalog>().Metastore().GetTable(context, database_info.name, bound.name, current)) {
		throw CatalogException("Table with name \"%s\" does not exist in %s database \"%s\"", bound.name,
		                       ServiceName(catalog), database_info.name);
	}
	if (bound.IsSameVersion(current)) {
		return bound;
	}
	return HiveTableSet::GetTableInfo(*tables.CreateEntry(tables.CreateEntry(current)));
}

void HiveSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (!CatalogTypeIsSupported(info.type)) {
		throw NotImplementedException("%s databases only support dropping tables", ServiceName(catalog));
	}
	HiveCatalog::ThrowIfInExplicitTransaction(context);
	auto &hive_catalog = catalog.Cast<HiveCatalog>();
	auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

	EntryLookupInfo lookup(info.type, QualifiedName(Identifier(table_name)));
	auto existing = tables.GetEntry(context, lookup);
	if (!existing) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("%s with name \"%s\" does not exist in %s database \"%s\"",
		                       info.type == CatalogType::VIEW_ENTRY ? "View" : "Table", table_name,
		                       ServiceName(catalog), database_info.name);
	}
	CheckEntryType(existing, info.type, table_name, "drop");
	if (existing->type == CatalogType::VIEW_ENTRY && !existing->Cast<HiveView>().IsDuckDBView()) {
		throw CatalogException("%s view \"%s\" was not written by DuckDB; drop it from the engine that wrote it",
		                       ServiceName(catalog), table_name);
	}
	hive_catalog.Metastore().DeleteTable(context, database_info.name, table_name);
	tables.RemoveEntry(table_name);
}

//===--------------------------------------------------------------------===//
// Scan / Lookup
//===--------------------------------------------------------------------===//
void HiveSchemaEntry::Scan(ClientContext &context, CatalogType type,
                           const std::function<void(CatalogEntry &)> &callback) {
	if (!CatalogTypeIsSupported(type)) {
		return;
	}
	tables.Scan(context, callback);
}

void HiveSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported for %s databases", ServiceName(catalog));
}

void HiveSchemaEntry::Scan(ClientContext &context, SchemaScanInfo &info) {
	string name;
	if (!CatalogTypeIsSupported(info.type) || !info.filter || info.filter->GetOid() ||
	    !TryGetNameEquality(info.filter->GetName(), "HiveSchemaEntry::Scan", name)) {
		SchemaCatalogEntry::Scan(context, info);
		return;
	}
	// tables and views share the set, so the lookup returns whichever the metastore holds under the name; the caller
	// keeps the entries of the type it asked for
	EntryLookupInfo lookup(info.type, QualifiedName(Identifier(name)));
	optional_ptr<CatalogEntry> entry;
	try {
		entry = tables.GetEntry(context, lookup);
	} catch (const MetastoreException &ex) {
		// like the listing this filter replaces, a table we may not describe is not listed
		if (!ex.access_denied) {
			throw;
		}
		ErrorData error(ex);
		DUCKDB_LOG_WARNING(context, "Not listing %s table '%s.%s': %s", ServiceName(catalog), database_info.name, name,
		                   error.RawMessage());
		return;
	}
	if (entry) {
		info.callback(*entry);
	}
}

SimilarCatalogEntry HiveSchemaEntry::GetSimilarEntry(CatalogTransaction transaction,
                                                     const EntryLookupInfo &lookup_info) {
	SimilarCatalogEntry result;
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return result;
	}
	tables.ScanLoaded([&](CatalogEntry &entry) {
		auto score = StringUtil::SimilarityRating(entry.name.GetIdentifierName(), lookup_info.GetEntryName());
		if (score > result.score) {
			result.score = score;
			result.name = Identifier(entry.name.GetIdentifierName());
		}
	});
	return result;
}

const HiveDatabaseInfo &HiveSchemaEntry::GetSchemaInfo(ClientContext &context) {
	lock_guard<mutex> guard(schema_info_lock);
	if (!schema_info_loaded) {
		HiveDatabaseInfo database;
		if (!catalog.Cast<HiveCatalog>().Metastore().GetDatabase(context, database_info.name, database)) {
			throw CatalogException("%s database \"%s\" no longer exists", ServiceName(catalog), database_info.name);
		}
		database_info.description = std::move(database.description);
		database_info.location_uri = std::move(database.location_uri);
		database_info.parameters = std::move(database.parameters);
		schema_info_loaded = true;
	}
	return database_info;
}

void HiveSchemaEntry::SetSchemaInfo(const HiveDatabaseInfo &database) {
	lock_guard<mutex> guard(schema_info_lock);
	if (schema_info_loaded) {
		return;
	}
	database_info.description = database.description;
	database_info.location_uri = database.location_uri;
	database_info.parameters = database.parameters;
	schema_info_loaded = true;
}

optional_ptr<CatalogEntry> HiveSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                        const EntryLookupInfo &lookup_info) {
	if (!CatalogTypeIsSupported(lookup_info.GetCatalogType())) {
		return nullptr;
	}
	auto &context = transaction.GetContext();
	return tables.GetEntry(context, lookup_info);
}

} // namespace duckdb
