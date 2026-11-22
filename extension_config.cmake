# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(hive
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# S3 for the tables of the Hive Metastore tests. Pinned and patched as the duckdb submodule's
# .github/config/extensions/httpfs.cmake, until duckdb-httpfs main builds against duckdb main
duckdb_extension_load(httpfs
    GIT_URL https://github.com/duckdb/duckdb-httpfs
    GIT_TAG a2485c0fd240cbddc0d467019f30201c9a46cf06
    APPLY_PATCHES
)

# Avro backed Hive tables (AvroSerDe) are read with read_avro from the avro extension, which the hive extension loads
# when it reads such a table. Pinned to the hash the duckdb submodule pins in .github/config/extensions/avro.cmake
if (NOT MINGW)
    duckdb_extension_load(avro
        GIT_URL https://github.com/duckdb/duckdb-avro
        GIT_TAG 5ef585162ca04de48f2fbfce939dac3322091836
        SUBMODULES "third_party/avro-c"
    )
endif()
