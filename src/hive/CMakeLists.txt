add_subdirectory(catalog)
add_subdirectory(core)
add_subdirectory(execution)
add_subdirectory(functions)
add_subdirectory(grammar)
add_subdirectory(metastore)
add_subdirectory(planning)

set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES}
    PARENT_SCOPE)
