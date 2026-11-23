# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(hive
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
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
