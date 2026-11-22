# AGENTS.md

This file provides guidance to coding agents when working with code in this repository. For DuckDB internals
(parser/planner/optimizer/execution, Vector API, generated files) see `duckdb/AGENTS.md`.

## What this is

A DuckDB extension (`hive`) that exposes a Hive metastore as a DuckDB catalog: a Hive Metastore over Thrift
(`ATTACH 'thrift://host:9083' (TYPE hive)`) or a metastore kept in a DuckDB file (`ATTACH 'meta.duckdb' (TYPE hive)`).
Only Hive tables are readable/writable (parquet, csv, json, avro SerDes); Iceberg/Delta/Hudi/symlink-manifest tables
are listed but scans and DML throw. `README.md` is the user-facing spec; keep it in sync when behavior changes. The code
was split out of the Glue extension (`duckdb-aws-glue`), which still carries its own copy of the Hive layer.

## Build

Extension template build (`extension-ci-tools`); the `duckdb` and `extension-ci-tools` submodules must be checked out.
`duckdb` is pinned to the DuckDB main commit the Glue extension uses, not a release: the code needs its APIs.

```sh
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make release   # or make relassert
```

A `relassert` build is ASAN-instrumented: run it with `ASAN_OPTIONS=detect_container_overflow=0`. Each `src/`
subdirectory has its own `CMakeLists.txt` (an `add_library_unity` object library appended to `HIVE_EXTENSION_FILES`):
add new `.cpp` files there, and a new directory with `add_subdirectory` in its parent. `extension_config.cmake` also
builds `httpfs` (S3) and `avro` (loaded on demand for AvroSerDe tables).

Run `make format-fix` (clang-format over `src` and `test`, via the duckdb submodule) before committing.

## Tests

SQLLogicTests under `test/sql/hive_metastore/` attach `HIVE_METASTORE_URI` with their tables below `HIVE_WAREHOUSE`;
without a `--test-config` they are skipped (`require-env HIVE_METASTORE_URI`).

- `test/configs/local_hive_duckdb.json`: a DuckDB-file metastore in the test directory, tables on the local file system,
  no servers (`make test-hive-duckdb`).
- `test/configs/local_hms.json`: Apache Hive's metastore + SeaweedFS from `scripts/hive_metastore/docker-compose.yml`
  (`make hive-fixture`, `make test-hms`, `make hive-fixture-down`). The compose project is `duckdb-hive-metastore`
  (ports 9083/9200). The metastore keeps its state between runs: tests `DROP SCHEMA IF EXISTS ... CASCADE` their
  databases first. Tests only for one backend use `require-env HIVE_TEST_CONFIG duckdb|thrift`.

Test targets don't build first (`TEST_BUILD=release` by default). Test format is DuckDB's sqllogictest. Do not add
`PRAGMA enable_verification`. Test error paths, not just the happy path.

## Architecture

- **Entry point** `src/hive_extension.cpp`: registers the `hive` StorageExtension, the table functions, the `hive_ddl`
  grammar extension and the settings.
- **Metastore interface** `src/include/hive/metastore/hive_metastore.hpp`: `HiveMetastore`, whose model is the Hive
  metastore's (databases, tables with a storage descriptor, partitions) as plain structs (`HiveTableInfo`,
  `HivePartitionInfo`, ... in `hive/core/hive_info.hpp`). Backends live in `src/hms/`: `ThriftMetastore` (a
  hand-written TBinaryProtocol codec, `ThriftValue` keeps every field so an update sends back what it read; field ids of
  `hive_metastore.thrift`; Hive 4 dropped `get_table`, so the `_req` methods are tried first) and `DuckDBMetastore`
  (its own `DuckDB` instance over the file, tables `dbs`, `tbls`, `partitions`). `src/hms/hms_attach.cpp` picks one by
  the ATTACH path; `HiveAttach` (`hive/metastore/hive_attach.cpp`) parses the shared options.
- **Catalog layer** `src/hive/catalog/`: `HiveCatalog` → `HiveSchemaSet` → `HiveSchemaEntry` (a metastore database) →
  `HiveTableSet` → `HiveTable`. Every metastore call goes through `catalog.Metastore()`; mutations are wrapped in
  `MetastoreWrite` (read-only check, metadata cache invalidation). Tables and partitions keep the object they were
  fetched as (`HiveFetchedTable`, subclassed per backend) because updates rewrite the whole definition.
  `HiveTableInfo::ForCreate` is the storage descriptor of a table DuckDB creates.
- **Reading** `src/hive/planning/hive_multi_file_reader.cpp`: `HiveScanInfo` + `HiveMultiFileReader` + a lazy
  `HiveMultiFileList` over `read_parquet`/`read_csv`/`read_json`/`read_avro`; partition filters are applied to the
  partition values before any listing. `hive_scan` (`src/hive/functions/hive_scan_function.cpp`) reuses the reader.
- **Writing** `src/hive/planning/hive_insert.cpp`: INSERT and CTAS plan a `PhysicalCopyToFile` (`HiveCopy`) with hive
  partition directories and register new partitions in Finalize; a failed statement's write is undone by
  `HiveTransaction::Rollback` (`HiveWrite::Undo`), using the `HiveMetastoreHandle` taken while the statement ran.
- **Partition DDL** `src/hive/functions/hive_partition_functions.cpp` (`hive_partitions`, `hive_add_partition`, ...,
  `hive_alter_table`); `src/hive/grammar/hive_grammar.cpp` rewrites Hive partition SQL into `CALL hive_alter_table(...)`.

## Coding guidelines

Follow DuckDB's style (`duckdb/AGENTS.md`):

- **Comments are kept to a minimum.** Code should be self-descriptive; a comment is one short line, and only where the
  code can't say it (e.g. a metastore quirk). No comments describing the change, the bug that was fixed, or the history
  of the code — that belongs in the commit message or PR. Don't add doc comments restating a function's name.
- Tabs for indentation, 120 columns. Always use braces for `if` and loops. Range-based for loops where possible.
- `[u]int(8|16|32|64)_t` instead of `int`/`long`; `idx_t` for offsets, indices and counts.
- `unique_ptr` for ownership, `shared_ptr` only when necessary, `optional_ptr`/`reference` instead of raw pointers.
  Never `const_cast`. Pass non-trivial objects by `const &`.
- Naming: files `snake_case`, types and functions `PascalCase`, variables `snake_case`.
- Class layout: `public:` section(s) before `private:`, methods and member variables in separate sections.
- Exceptions for query-terminating errors (`BinderException`, `NotImplementedException`, `IOException`, ...);
  `D_ASSERT` only for programmer errors, never for anything user input or metastore responses can trigger.
- When reading or writing vectors, use `Vector::Values<T>()` / `FlatVector::Writer<T>()` rather than `ToUnifiedFormat`
  loops.

## Change checklist

- C++ change: `make format-check`, build, run `test/sql/hive_metastore` with `local_hive_duckdb.json`, and with
  `local_hms.json` when it touches `src/hms/thrift_*` or the metastore calls.
- Build change: read both the root `Makefile` and `extension-ci-tools/makefiles/duckdb_extension.Makefile`.

## Repo notes

- `duckdb/` and `extension-ci-tools/` are submodules; don't edit or bump them unless the task requires it.
- Don't commit generated/runtime directories (`build/`, `.cache/`, `duckdb_unittest_tempdir/`).
