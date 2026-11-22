add_library_unity(
  hms
  OBJECT
  duckdb_metastore.cpp
  hms_attach.cpp
  thrift_client.cpp
  thrift_metastore.cpp
  thrift_protocol.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hms>
    PARENT_SCOPE)
