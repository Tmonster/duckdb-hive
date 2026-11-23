//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hive/metastore/metastore_write.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "hive/catalog/hive_catalog.hpp"
#include "hive/catalog/hive_transaction.hpp"

namespace duckdb {

//! Wraps a metastore change: refused on a read-only attach, and the metadata cache is cleared when it returns or
//! throws
class MetastoreWrite {
public:
	MetastoreWrite(ClientContext &context, HiveCatalog &catalog, const string &operation)
	    : invalidate(context, catalog) {
		catalog.CheckWritable(operation);
	}

private:
	HiveTransactionCache::InvalidateOnExit invalidate;
};

} // namespace duckdb
