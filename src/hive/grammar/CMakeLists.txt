add_library_unity(hive_grammar OBJECT hive_grammar.cpp)
set(HIVE_EXTENSION_FILES
    ${HIVE_EXTENSION_FILES} $<TARGET_OBJECTS:hive_grammar>
    PARENT_SCOPE)
