#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "hive/core/hive_info.hpp"

namespace duckdb {
class HiveCatalog;
class HiveSchemaEntry;
class HiveTable;

//! Writes rows into a Hive table registered in the metastore: a COPY into the table location (one <key=value> directory
//! level per partition key, or the location of an existing partition), then the new partition directories are
//! registered in the metastore.
class HiveInsert : public PhysicalOperator {
public:
	HiveInsert(PhysicalPlan &physical_plan, LogicalOperator &op, HiveCatalog &catalog, HiveTableInfo table_info);

	//! INSERT INTO <hive table>
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                                    HiveTable &table, const HiveTableInfo &table_info,
	                                    optional_ptr<PhysicalOperator> plan);
	//! CREATE TABLE <hive table> AS <query>: the copy creates the table in the metastore when it starts, then writes
	//! the result
	static PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
	                                           LogicalCreateTable &op, PhysicalOperator &plan);

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
	InsertionOrderPreservingMap<string> ParamsToString() const override;

private:
	//! Plan the COPY child of 'insert': INSERT passes the existing table (whose partitions may live at their own
	//! locations), CTAS the schema and the bound statement of the table the copy creates when it starts
	static void PlanWrite(ClientContext &context, PhysicalPlanGenerator &planner, LogicalOperator &op,
	                      HiveInsert &insert, optional_ptr<HiveTable> existing_table, const HiveTableInfo &table_info,
	                      PhysicalOperator &plan, const vector<Identifier> &names, const vector<LogicalType> &types,
	                      optional_ptr<HiveSchemaEntry> create_schema, unique_ptr<BoundCreateTableInfo> create_info);
	//! Adds the files of a write whose partitions are registered to the hive_written_files temporary table
	void RecordWrittenFiles(ClientContext &context, const vector<string> &files) const;
	//! Throws when a partition registration skipped as existing is registered at another location than its files
	void CheckExistingPartitionLocations(ClientContext &context,
	                                     const vector<HivePartitionInput> &already_existing) const;

public:
	//! The target catalog (for partition registration after the file write).
	HiveCatalog &catalog;
	//! The one resolved table definition used by both CTAS catalog creation and file-writer planning.
	HiveTableInfo table_info;
	//! Where each partition that existed when the INSERT was planned is registered, by its metastore values.
	map<vector<string>, string> planned_locations;
	//! The directories of existing partitions not laid out as <key=value>, with their metastore values.
	unordered_map<string, vector<string>> partition_directories;
	//! Names the files of this write, and its entry in the Hive transaction
	string write_id;
};

} // namespace duckdb
