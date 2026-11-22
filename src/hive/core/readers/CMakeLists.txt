add_library_unity(hive_core_readers OBJECT hive_csv_options.cpp
                  hive_json_options.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_core_readers>
    PARENT_SCOPE)
