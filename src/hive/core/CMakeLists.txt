add_subdirectory(readers)

add_library_unity(
  hive_core
  OBJECT
  helpers.cpp
  hive_info.cpp
  hive_location.cpp
  hive_log_types.cpp
  hive_table_format.cpp
  hive_timestamp_formats.cpp
  hive_types.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_core>
    PARENT_SCOPE)
