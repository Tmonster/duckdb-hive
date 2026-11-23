#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! The ATTACH options every Hive catalog takes, whatever its metastore
struct HiveAttachOptions {
	//! The name of the attached database
	string name;
	//! The raw ATTACH path, which names the metastore
	string path;
	//! Optional S3 prefix for new databases, and for tables created without a location in databases that have no
	//! LocationUri. Empty means: databases are created without a LocationUri and tables must be given a location
	string default_location;
	//! Optional default schema
	Identifier default_schema;
	//! Reach the metastore while attaching (default true); false defers the first metastore call
	bool verify_connection = true;
	//! The attached Iceberg catalog that reads this catalog's Iceberg tables in views
	Identifier iceberg_catalog;
};

} // namespace duckdb
