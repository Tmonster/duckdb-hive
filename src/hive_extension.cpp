#define DUCKDB_EXTENSION_MAIN

#include "hive_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void HiveScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "...........🦆 " + name.GetString());
	});
}

inline void HiveOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Hive " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto hive_scalar_function =
	    ScalarFunction("hive", {LogicalType::VARCHAR}, LogicalType::VARCHAR, HiveScalarFun);

	loader.RegisterFunction(hive_scalar_function);

	// Register another scalar function
	auto hive_openssl_version_scalar_function = ScalarFunction("hive_openssl_version", {LogicalType::VARCHAR},
	                                                             LogicalType::VARCHAR, HiveOpenSSLVersionScalarFun);
	loader.RegisterFunction(hive_openssl_version_scalar_function);
}

void HiveExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string HiveExtension::Name() {
	return "hive";
}

std::string HiveExtension::Version() const {
#ifdef EXT_VERSION_HIVE
	return EXT_VERSION_HIVE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hive, loader) {
	duckdb::LoadInternal(loader);
}
}
