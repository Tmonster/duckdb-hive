add_library_unity(hive_metastore OBJECT hive_attach.cpp hive_metastore.cpp
                  hive_metastore_json.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_metastore>
    PARENT_SCOPE)
