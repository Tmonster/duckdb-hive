add_library_unity(hive_planning_stats OBJECT hive_stats.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_planning_stats>
    PARENT_SCOPE)
