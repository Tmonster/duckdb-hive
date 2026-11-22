add_subdirectory(stats)

add_library_unity(
  hive_planning
  OBJECT
  hive_field_conversion.cpp
  hive_insert.cpp
  hive_multi_file_reader.cpp
  hive_partition_cache.cpp
  hive_query_cache.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_planning>
    PARENT_SCOPE)
