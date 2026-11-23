add_subdirectory(hive)
add_subdirectory(hms)

add_library_unity(hive_main OBJECT hive_extension.cpp)

set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_main>
    PARENT_SCOPE)
