PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=hive
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Extensions needed for testing: parquet and json for the tables
CORE_EXTENSIONS='parquet;json'

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# The tests of test/sql/hive_metastore: against a metastore kept in a DuckDB database with the tables on the local file
# system (no servers), and against the Hive Metastore of scripts/hive_metastore/docker-compose.yml (start it with
# `make hive-fixture` first)
HIVE_COMPOSE=docker compose -f scripts/hive_metastore/docker-compose.yml
TEST_BUILD ?= release
# the metastore creates directories below the warehouse, DuckDB writes files into them: both need to write there
HIVE_WAREHOUSE_DIR=/tmp/duckdb-hive-warehouse
hive-fixture:
	mkdir -p $(HIVE_WAREHOUSE_DIR) && chmod 777 $(HIVE_WAREHOUSE_DIR)
	$(HIVE_COMPOSE) up -d --wait
hive-fixture-down:
	$(HIVE_COMPOSE) down -v --remove-orphans
test-hive-duckdb:
	ASAN_OPTIONS=detect_container_overflow=0 ./build/$(TEST_BUILD)/test/unittest --test-config test/configs/local_hive_duckdb.json 'test/sql/*'
test-hms:
	ASAN_OPTIONS=detect_container_overflow=0 ./build/$(TEST_BUILD)/test/unittest --test-config test/configs/local_hms.json 'test/sql/*'
