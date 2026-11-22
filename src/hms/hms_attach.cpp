#include "hive/metastore/hive_attach.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"

#include "hive/catalog/hive_catalog.hpp"
#include "hms/duckdb_metastore.hpp"
#include "hms/thrift_metastore.hpp"

namespace duckdb {

unique_ptr<Catalog> HiveAttach::Attach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &options) {
	HiveAttachOptions attach_options;
	attach_options.name = name;
	attach_options.path = info.path;
	ThriftMetastoreOptions thrift_options;
	auto is_thrift = ThriftMetastoreOptions::IsThriftPath(info.path);
	for (auto &entry : info.options) {
		auto lower_name = StringUtil::Lower(entry.first);
		if (ParseOption(lower_name, entry.second, attach_options)) {
			continue;
		}
		if (is_thrift && thrift_options.ParseOption(lower_name, entry.second)) {
			continue;
		}
		throw InvalidConfigurationException("Unrecognized option for Hive attach: '%s'", entry.first);
	}
	if (!attach_options.iceberg_catalog.empty()) {
		throw NotImplementedException("ICEBERG_CATALOG is not supported for Hive metastore catalogs");
	}
	auto catalog = make_uniq<HiveCatalog>(db, options.access_mode, std::move(attach_options));
	if (is_thrift) {
		thrift_options.SetPath(info.path);
		catalog->SetMetastore(make_uniq<ThriftMetastore>(*catalog, std::move(thrift_options)));
	} else {
		catalog->SetMetastore(make_uniq<DuckDBMetastore>(*catalog, info.path));
	}
	Connect(context, *catalog);
	return std::move(catalog);
}

} // namespace duckdb
