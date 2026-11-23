# DuckDB Hive extension

**Experimental** extension that exposes a Hive metastore as a DuckDB catalog and reads and writes its Hive tables
(parquet, csv, json and avro SerDes). Tables of other formats the metastore registers (Iceberg, Delta, Hudi, symlink
manifests) are listed but can not be read or written.

```sql
-- a Hive Metastore (Thrift, binary protocol, no SASL / Kerberos)
ATTACH 'thrift://metastore-host:9083' AS hms (TYPE hive, DEFAULT_LOCATION 's3://bucket/warehouse');
-- a metastore kept in a DuckDB database file (or ':memory:'), with the tables wherever their locations point
ATTACH 'metastore.duckdb' AS lake (TYPE hive, DEFAULT_LOCATION '/data/warehouse');

CREATE SCHEMA lake.sales;
CREATE TABLE lake.sales.orders (id INTEGER, amount DOUBLE, dt VARCHAR) PARTITIONED BY (dt);
INSERT INTO lake.sales.orders VALUES (1, 10.5, '2024-01-01');
SELECT * FROM lake.sales.orders WHERE dt = '2024-01-01';
```

The Hive layer was split out of the Glue extension (`duckdb-aws-glue`), whose
`docs/README.md` describes how Hive tables are read and written (SerDes and their properties, partitions, statistics,
the rollback of a failed write). The same rules apply here, with `hive_*` in place of the `glue_*` names.

## Attach options

| option              | description                                                                                   |
|---------------------|-----------------------------------------------------------------------------------------------|
| `DEFAULT_LOCATION`  | the prefix of new databases (`<prefix>/<database>`) and of tables created without a location   |
| `DEFAULT_SCHEMA`    | the database to use as the default schema                                                      |
| `READ_ONLY`         | refuse every change: DML, DDL and the partition functions that change the metastore            |
| `VERIFY_CONNECTION` | reach the metastore while attaching (default true)                                             |
| `TIMEOUT`           | `thrift://` only: the socket timeout in seconds (default 60)                                   |

- Thrift: a database or table is created with the location given, and the metastore creates its directory, so it must
  reach that file system (S3A for `s3://`). Hive 4 metastores are called with the request-style methods
  (`get_table_req`, `get_table_objects_by_name_req`), older ones with the plain ones. A table's comment is its `comment`
  parameter; a changed table is told apart by its `transient_lastDdlTime`.
- DuckDB: the database holds the tables `dbs`, `tbls` and `partitions`, one row per object with the storage descriptor
  inline (columns as `STRUCT(name, type, comment)[]`, parameters as `MAP(VARCHAR, VARCHAR)`), the shape of a Hive
  Metastore's relational backend. Any DuckDB client can read or edit them; the catalog opens the file in a database
  instance of its own, so attach it as a Hive catalog or as a plain DuckDB database, not both at once. Locations may be
  local paths: the directories of a table are created as it is written.

## Functions

| function | |
|----------|---|
| `hive_partitions('cat.db.t')` | `SHOW PARTITIONS`: a typed column per partition key, `location` and `partition_values` |
| `CALL hive_add_partition('cat.db.t', {dt: '2024-01-01'}, location := '...', if_not_exists := false)` | `ALTER TABLE ADD PARTITION` |
| `CALL hive_drop_partition('cat.db.t', {dt: '2024-01-01'}, if_exists := false)` | `ALTER TABLE DROP PARTITION`, the files stay |
| `CALL hive_rename_partition('cat.db.t', {dt: '...'}, {dt: '...'})` | `ALTER TABLE ... PARTITION ... RENAME TO PARTITION` |
| `CALL hive_set_partition_location(...)`, `CALL hive_set_table_location(...)` | `ALTER TABLE ... SET LOCATION` |
| `CALL hive_replace_columns(...)` | `ALTER TABLE ... REPLACE COLUMNS` |
| `CALL hive_repair_table('cat.db.t')` | `MSCK REPAIR TABLE`: register the partition directories found below the table |
| `CALL hive_alter_table(...)` | the partition DDL of one `ALTER TABLE`; `SET active_grammar_extensions = ['hive_ddl']` turns Hive's partition SQL into it |
| `hive_get_table_response`, `hive_get_database_response`, `hive_get_partition_response` | the metastore's objects, as columns and as VARIANT |
| `hive_scan('s3://root', schema := {...}, partitions := [...])` | a Hive table without a catalog |

## Building

```sh
VCPKG_TOOLCHAIN_PATH='<vcpkg>/scripts/buildsystems/vcpkg.cmake' GEN=ninja make release
```

The `duckdb` submodule is pinned to a DuckDB main commit (the one the Glue extension builds against): the code uses
APIs that DuckDB v2.0.0 does not have.

## Testing

The tests under `test/sql/hive_metastore/` attach `HIVE_METASTORE_URI` with their tables below `HIVE_WAREHOUSE`; without
a `--test-config` they are skipped.

- `test/configs/local_hive_duckdb.json`: a metastore kept in a DuckDB file in the test directory and tables on the local
  file system, no servers: `make test-hive-duckdb`.
- `test/configs/local_hms.json`: Apache Hive's metastore (4.0.1, Derby inside) from
  `scripts/hive_metastore/docker-compose.yml`: `make hive-fixture`, `make test-hms`, `make hive-fixture-down`. The
  tables live on the local file system below `/tmp/duckdb-hive-warehouse`, which is mounted at the same path in the
  container, as the metastore creates the directories of what it registers. The metastore keeps its state between
  runs, so the tests drop their databases first.

CI (`.github/workflows/LocalHiveTests.yml`) runs each config in a job of its own; only the Hive Metastore job starts
containers.

`TEST_BUILD=relassert` runs the tests of a `make relassert` build.
