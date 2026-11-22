//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hive/catalog/hive_file_credentials.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/extension_callback.hpp"

namespace duckdb {

class HiveTable;

//! Gives the scans of a Hive table the credentials to read its files with, as the name of a secret that every listed
//! file names in its options (ExtendedOpenFileInfo::SECRET_OPTION). An extension registers one as an
//! ExtensionCallback; without one, every file is read with the secret that matches its path.
//! The callback is found with dynamic_cast, so the extension must be linked into the same binary as this one: one
//! LOADed as a separate library does not share the type and is not found.
class HiveFileCredentialsCallback : public ExtensionCallback {
public:
	//! The name of the secret for a read of `table`, or "" to read its files by path. Asked each time a scan of the
	//! table is bound, and before an INSERT or hive_repair_table of it. A secret of the transaction
	//! (SecretPersistType::TRANSACTION) also serves the listings, which are made by path, and ends with the
	//! transaction.
	virtual string GetFileSecret(ClientContext &context, const HiveTable &table) = 0;

	//! The registered callback, if any. Throws when more than one is.
	static optional_ptr<HiveFileCredentialsCallback> Find(ClientContext &context);
	//! What the registered callback returns; "" when none is registered
	static string Get(ClientContext &context, const HiveTable &table);
};

} // namespace duckdb
