# Testing this extension

The SQLLogicTests under `sql/hive_metastore` attach `HIVE_METASTORE_URI` with their tables below `HIVE_WAREHOUSE`,
which a `--test-config` sets; without one they are skipped.

```sh
make test-hive-duckdb                 # a metastore kept in a DuckDB file, tables on the local file system
make hive-fixture && make test-hms    # Apache Hive's metastore from scripts/hive_metastore/docker-compose.yml
```
