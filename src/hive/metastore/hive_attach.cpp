#include "hive/metastore/hive_attach.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "hive/catalog/hive_catalog.hpp"
#include "hive/metastore/hive_metastore.hpp"

namespace duckdb {

bool HiveAttach::ParseOption(const string &name, const Value &value, HiveAttachOptions &options) {
	if (name == "type" || name == "read_only") {
		return true;
	}
	if (name == "default_location") {
		options.default_location = value.ToString();
		StringUtil::RTrim(options.default_location, "/");
	} else if (name == "default_schema") {
		options.default_schema = Identifier(value.ToString());
	} else if (name == "verify_connection") {
		options.verify_connection = value.DefaultCastAs(LogicalType::BOOLEAN).GetValue<bool>();
	} else if (name == "iceberg_catalog") {
		options.iceberg_catalog = Identifier(value.ToString());
	} else {
		return false;
	}
	return true;
}

void HiveAttach::CheckOptions(const HiveAttachOptions &options, const string &service) {
	if (!options.iceberg_catalog.empty() &&
	    StringUtil::CIEquals(options.iceberg_catalog.GetIdentifierName(), options.name)) {
		throw InvalidConfigurationException("ICEBERG_CATALOG must name an Iceberg catalog, not the %s catalog \"%s\" "
		                                    "itself",
		                                    service, options.name);
	}
}

void HiveAttach::Connect(ClientContext &context, HiveCatalog &catalog) {
	auto &metastore = catalog.Metastore();
	if (catalog.options.verify_connection) {
		metastore.VerifyConnection(context);
	}
	if (!catalog.options.default_schema.empty()) {
		HiveDatabaseInfo database;
		if (!metastore.GetDatabase(context, catalog.options.default_schema.GetIdentifierName(), database)) {
			throw InvalidConfigurationException("default_schema '%s' does not exist in %s catalog '%s'",
			                                    catalog.options.default_schema.GetIdentifierName(),
			                                    metastore.ServiceName(), catalog.options.path);
		}
	}
}
} // namespace duckdb
