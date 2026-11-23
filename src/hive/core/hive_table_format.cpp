#include "hive/core/hive_table_format.hpp"

#include "duckdb/common/string_util.hpp"

#include "hive/core/hive_info.hpp"

namespace duckdb {

//! The format the table parameters announce, UNKNOWN when they announce none
static MetastoreFormat FromParameters(const HiveTableInfo &info) {
	// Open table formats register themselves through the 'table_type' parameter
	auto table_type = StringUtil::Upper(info.GetParameter("table_type"));
	if (table_type == "ICEBERG") {
		return MetastoreFormat::ICEBERG;
	}
	if (table_type == "DELTA") {
		return MetastoreFormat::DELTA;
	}
	if (table_type == "HUDI") {
		return MetastoreFormat::HUDI;
	}
	// Spark registers Delta tables through the data source provider
	auto provider = StringUtil::Lower(info.GetParameter("spark.sql.sources.provider"));
	if (provider == "delta") {
		return MetastoreFormat::DELTA;
	}
	if (provider == "iceberg") {
		return MetastoreFormat::ICEBERG;
	}
	if (provider == "hudi" || provider == "org.apache.hudi") {
		return MetastoreFormat::HUDI;
	}
	if (!info.GetMetadataLocation().empty()) {
		return MetastoreFormat::ICEBERG;
	}
	return MetastoreFormat::UNKNOWN;
}

//! The format another engine's InputFormat class reveals, UNKNOWN for the InputFormats of plain Hive tables
static MetastoreFormat FromInputFormat(const string &input_format) {
	auto lower = StringUtil::Lower(input_format);
	auto class_name = lower.substr(lower.find_last_of('.') + 1);
	// org.apache.hudi.hadoop.HoodieParquetInputFormat, HoodieParquetRealtimeInputFormat, legacy com.uber.hoodie
	if (StringUtil::StartsWith(lower, "org.apache.hudi.") || StringUtil::StartsWith(lower, "com.uber.hoodie.") ||
	    StringUtil::StartsWith(class_name, "hoodie")) {
		return MetastoreFormat::HUDI;
	}
	// org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat: the location holds manifests listing the data files
	if (class_name == "symlinktextinputformat") {
		return MetastoreFormat::SYMLINK;
	}
	// the Hive connectors: org.apache.iceberg.mr.hive.HiveIcebergInputFormat, io.delta.hive.DeltaInputFormat
	if (StringUtil::StartsWith(lower, "org.apache.iceberg.")) {
		return MetastoreFormat::ICEBERG;
	}
	if (StringUtil::StartsWith(lower, "io.delta.")) {
		return MetastoreFormat::DELTA;
	}
	return MetastoreFormat::UNKNOWN;
}

MetastoreTableFormat MetastoreTableFormat::Detect(const HiveTableInfo &info) {
	MetastoreTableFormat result;
	result.format = FromParameters(info);
	if (result.format != MetastoreFormat::UNKNOWN) {
		result.source = MetastoreFormatSource::PARAMETERS;
		return result;
	}
	result.format = FromInputFormat(info.input_format);
	if (result.format != MetastoreFormat::UNKNOWN) {
		result.source = MetastoreFormatSource::INPUT_FORMAT;
		return result;
	}
	if (!info.input_format.empty() || !info.location.empty()) {
		result.format = MetastoreFormat::HIVE;
		result.source = MetastoreFormatSource::STORAGE_DESCRIPTOR;
	}
	return result;
}

bool MetastoreTableFormat::IsParameter(const string &key) {
	return StringUtil::CIEquals(key, "table_type") || StringUtil::CIEquals(key, "spark.sql.sources.provider") ||
	       StringUtil::CIEquals(key, "metadata_location");
}

bool MetastoreTableFormat::IsWritable(const HiveTableInfo &info, HiveFileFormat file_format) const {
	if (!IsHive()) {
		return false;
	}
	if (info.input_format.empty()) {
		return true;
	}
	auto lower = StringUtil::Lower(info.input_format);
	switch (file_format) {
	case HiveFileFormat::PARQUET:
		return lower == "org.apache.hadoop.hive.ql.io.parquet.mapredparquetinputformat" ||
		       lower == "org.apache.parquet.hadoop.parquetinputformat" ||
		       lower == "parquet.hadoop.parquetinputformat" || lower == "parquet.hive.deprecatedparquetinputformat";
	case HiveFileFormat::CSV:
	case HiveFileFormat::JSON:
		return lower == "org.apache.hadoop.mapred.textinputformat" ||
		       lower == "org.apache.hadoop.mapred.lib.combinetextinputformat";
	case HiveFileFormat::AVRO:
		return lower == "org.apache.hadoop.hive.ql.io.avro.avrocontainerinputformat";
	}
	return false;
}

string MetastoreTableFormat::ToString() const {
	switch (format) {
	case MetastoreFormat::ICEBERG:
		return "ICEBERG";
	case MetastoreFormat::DELTA:
		return "DELTA";
	case MetastoreFormat::HUDI:
		return "HUDI";
	case MetastoreFormat::SYMLINK:
		return "SYMLINK";
	case MetastoreFormat::HIVE:
		return "HIVE";
	default:
		return "UNKNOWN";
	}
}

string MetastoreTableFormat::Describe(const HiveTableInfo &info) const {
	switch (source) {
	case MetastoreFormatSource::INPUT_FORMAT:
	case MetastoreFormatSource::STORAGE_DESCRIPTOR:
		return StringUtil::Format("%s (input format '%s')", ToString(), info.input_format);
	case MetastoreFormatSource::NONE:
		if (!info.hive_table_type.empty()) {
			return StringUtil::Format("UNKNOWN (table type '%s')", info.hive_table_type);
		}
		return ToString();
	case MetastoreFormatSource::PARAMETERS:
		return ToString();
	}
	return ToString();
}

} // namespace duckdb
