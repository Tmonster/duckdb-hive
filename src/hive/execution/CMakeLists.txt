add_library_unity(hive_execution OBJECT hive_copy.cpp hive_write.cpp
                  hive_written_files.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_execution>
    PARENT_SCOPE)
