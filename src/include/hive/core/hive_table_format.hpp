#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {
struct HiveTableInfo;
enum class HiveFileFormat : uint8_t;

//! The (open) table format a Hive table is stored in
enum class MetastoreFormat : uint8_t { ICEBERG, DELTA, HUDI, SYMLINK, HIVE, UNKNOWN };

//! What the format was derived from
enum class MetastoreFormatSource : uint8_t {
	//! table_type, spark.sql.sources.provider or metadata_location
	PARAMETERS,
	//! another engine's InputFormat class (Hudi, symlink manifests, the Iceberg and Delta Hive connectors)
	INPUT_FORMAT,
	//! a storage descriptor with an InputFormat or location, and nothing announcing another format: a Hive table
	STORAGE_DESCRIPTOR,
	NONE
};

//! The format of a Hive table and where it was read from
struct MetastoreTableFormat {
	MetastoreFormat format = MetastoreFormat::UNKNOWN;
	MetastoreFormatSource source = MetastoreFormatSource::NONE;

public:
	//! Detect the format from the table definition: the table parameters first, then the InputFormat class, then any
	//! storage descriptor (a Hive table)
	static MetastoreTableFormat Detect(const HiveTableInfo &info);
	//! Whether 'key' is one of the table parameters the format is derived from
	static bool IsParameter(const string &key);

	bool IsHive() const {
		return format == MetastoreFormat::HIVE;
	}
	//! A Hive table whose InputFormat is one Hive uses for 'file_format' (or none): a layout DuckDB writes
	bool IsWritable(const HiveTableInfo &info, HiveFileFormat file_format) const;
	//! ICEBERG, DELTA, HUDI, SYMLINK, HIVE or UNKNOWN
	string ToString() const;
	//! ToString() plus what decided it, for error messages
	string Describe(const HiveTableInfo &info) const;
};

} // namespace duckdb
