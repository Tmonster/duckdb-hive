#include "hive/catalog/hive_file_credentials.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

optional_ptr<HiveFileCredentialsCallback> HiveFileCredentialsCallback::Find(ClientContext &context) {
	optional_ptr<HiveFileCredentialsCallback> found;
	for (auto &callback : ExtensionCallback::Iterate(context)) {
		auto credentials_callback = dynamic_cast<HiveFileCredentialsCallback *>(callback.get());
		if (!credentials_callback) {
			continue;
		}
		if (found) {
			throw InvalidConfigurationException("More than one extension registered a HiveFileCredentialsCallback");
		}
		found = credentials_callback;
	}
	return found;
}

string HiveFileCredentialsCallback::Get(ClientContext &context, const HiveTable &table) {
	auto found = Find(context);
	return found ? found->GetFileSecret(context, table) : string();
}

} // namespace duckdb
