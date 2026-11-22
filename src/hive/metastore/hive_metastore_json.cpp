#include "hive/metastore/hive_metastore_json.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

string Quote(const string &value) {
	string result = "\"";
	for (auto c : value) {
		switch (c) {
		case '"':
			result += "\\\"";
			break;
		case '\\':
			result += "\\\\";
			break;
		case '\n':
			result += "\\n";
			break;
		case '\r':
			result += "\\r";
			break;
		case '\t':
			result += "\\t";
			break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				result += StringUtil::Format("\\u%04x", static_cast<int>(c));
			} else {
				result += c;
			}
		}
	}
	return result + "\"";
}

//! A JSON object built field by field
class JSONObject {
public:
	JSONObject &Add(const string &key, const string &raw_value) {
		fields.push_back(Quote(key) + ": " + raw_value);
		return *this;
	}
	JSONObject &String(const string &key, const string &value) {
		return Add(key, Quote(value));
	}
	//! Leaves out an empty value, as the metastore does for fields that are not set
	JSONObject &StringIfSet(const string &key, const string &value) {
		return value.empty() ? *this : String(key, value);
	}
	string Build() const {
		return "{" + StringUtil::Join(fields, ", ") + "}";
	}

private:
	vector<string> fields;
};

string Map(const unordered_map<string, string> &values) {
	map<string, string> sorted(values.begin(), values.end());
	JSONObject result;
	for (auto &entry : sorted) {
		result.String(entry.first, entry.second);
	}
	return result.Build();
}

string Strings(const vector<string> &values) {
	vector<string> quoted;
	for (auto &value : values) {
		quoted.push_back(Quote(value));
	}
	return "[" + StringUtil::Join(quoted, ", ") + "]";
}

string Columns(const vector<HiveColumn> &columns) {
	vector<string> result;
	for (auto &column : columns) {
		result.push_back(JSONObject()
		                     .String("Name", column.name)
		                     .String("Type", column.type)
		                     .StringIfSet("Comment", column.comment)
		                     .Build());
	}
	return "[" + StringUtil::Join(result, ", ") + "]";
}

string StorageDescriptor(const vector<HiveColumn> &columns, const string &location, const string &input_format,
                         const string &output_format, const string &serde_library,
                         const unordered_map<string, string> &serde_parameters) {
	auto serde =
	    JSONObject().StringIfSet("SerializationLibrary", serde_library).Add("Parameters", Map(serde_parameters));
	return JSONObject()
	    .Add("Columns", Columns(columns))
	    .String("Location", location)
	    .StringIfSet("InputFormat", input_format)
	    .StringIfSet("OutputFormat", output_format)
	    .Add("SerdeInfo", serde.Build())
	    .Build();
}

} // namespace

string HiveMetastoreJSON::Database(const HiveDatabaseInfo &database) {
	return JSONObject()
	    .String("Name", database.name)
	    .StringIfSet("Description", database.description)
	    .StringIfSet("LocationUri", database.location_uri)
	    .Add("Parameters", Map(database.parameters))
	    .Build();
}

string HiveMetastoreJSON::Table(const HiveTableInfo &table) {
	JSONObject result;
	result.String("Name", table.name)
	    .String("DatabaseName", table.hive_database_name.empty() ? table.schema_name : table.hive_database_name)
	    .StringIfSet("Description", table.description)
	    .StringIfSet("TableType", table.hive_table_type)
	    .StringIfSet("ViewOriginalText", table.view_original_text)
	    .StringIfSet("ViewExpandedText", table.view_expanded_text);
	auto storage_descriptor = StorageDescriptor(table.columns, table.location, table.input_format, table.output_format,
	                                            table.serde_library, table.serde_parameters);
	if (!table.bucket_columns.empty() || table.number_of_buckets >= 0) {
		// the bucketing goes inside the storage descriptor object
		storage_descriptor.pop_back();
		storage_descriptor += ", " + Quote("BucketColumns") + ": " + Strings(table.bucket_columns) + ", " +
		                      Quote("NumberOfBuckets") + ": " + to_string(table.number_of_buckets) + "}";
	}
	result.Add("StorageDescriptor", storage_descriptor)
	    .Add("PartitionKeys", Columns(table.partition_keys))
	    .Add("Parameters", Map(table.parameters))
	    .StringIfSet("VersionId", table.version_id);
	return result.Build();
}

string HiveMetastoreJSON::Partition(const string &database_name, const string &table_name,
                                    const HivePartitionDetails &partition) {
	return JSONObject()
	    .Add("Values", Strings(partition.values))
	    .String("DatabaseName", database_name)
	    .String("TableName", table_name)
	    .Add("StorageDescriptor",
	         StorageDescriptor(partition.partition_columns, partition.location, partition.input_format, string(),
	                           partition.serde_library, partition.serde_parameters))
	    .Add("Parameters", Map(partition.parameters))
	    .Build();
}

} // namespace duckdb
