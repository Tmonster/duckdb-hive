//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hive/metastore/hive_attach.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "hive/catalog/hive_options.hpp"

namespace duckdb {
class HiveCatalog;

struct HiveAttach {
	//! ATTACH 'thrift://<host>:<port>' (TYPE hive) for a Hive Metastore, ATTACH '<file>.duckdb' or ':memory:'
	//! (TYPE hive) for a metastore kept in a DuckDB database
	static unique_ptr<Catalog> Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
	                                  AttachedDatabase &db, const string &name, AttachInfo &info,
	                                  AttachOptions &options);

	//! Take an option every Hive catalog has (lower case 'name'); false when it is not one of them
	static bool ParseOption(const string &name, const Value &value, HiveAttachOptions &options);
	//! Throw for options that contradict each other; 'service' names the metastore in errors
	static void CheckOptions(const HiveAttachOptions &options, const string &service);
	//! Reach the metastore unless VERIFY_CONNECTION is false, and check that DEFAULT_SCHEMA exists
	static void Connect(ClientContext &context, HiveCatalog &catalog);
};

} // namespace duckdb
