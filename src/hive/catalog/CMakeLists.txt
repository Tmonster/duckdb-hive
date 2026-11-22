add_library_unity(
  hive_catalog
  OBJECT
  hive_catalog.cpp
  hive_file_credentials.cpp
  hive_schema_entry.cpp
  hive_schema_set.cpp
  hive_table.cpp
  hive_table_set.cpp
  hive_transaction.cpp
  hive_transaction_manager.cpp
  hive_view.cpp
  hive_view_names.cpp
  scan_filter_equality.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_catalog>
    PARENT_SCOPE)
