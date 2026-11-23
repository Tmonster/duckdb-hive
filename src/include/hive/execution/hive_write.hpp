#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/string.hpp"

#include "hive/metastore/hive_metastore.hpp"

namespace duckdb {
class ClientContext;
class HiveCatalog;

//! What an INSERT or CREATE TABLE AS into a Hive table changed, so that a failed statement can be taken back
struct HiveWrite {
	string write_id;
	string database_name;
	string table_name;
	//! The files of the write are named <write_id>_..., below this location
	string location;
	//! CREATE TABLE AS created the table, and the metastore handle to drop it with (the rollback runs when the
	//! transaction has ended, too late to look up credentials)
	bool created_table = false;
	shared_ptr<HiveMetastoreHandle> metastore;
	//! The CreateTime of the table it created, if it could be fetched: a table with another CreateTime was created
	//! again by someone else, and is not dropped. The metastore keeps whole seconds, so a table dropped and created
	//! again within the same second is not told apart
	optional_idx created_table_time;
	//! The statement deleted its files itself (a failed partition registration does), so the undo need not look
	bool files_deleted = false;

public:
	//! Undo a write whose statement failed: delete its files and, for CREATE TABLE AS, drop the table it created.
	//! Best effort: a step that fails is logged
	void Undo(ClientContext &context, HiveCatalog &catalog);
};

} // namespace duckdb
