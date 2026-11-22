add_library_unity(
  hive_functions OBJECT hive_functions.cpp hive_partition_functions.cpp
  hive_repair_table.cpp hive_scan_function.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_functions>
    PARENT_SCOPE)
