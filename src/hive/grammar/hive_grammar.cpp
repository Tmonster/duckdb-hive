#include "hive/grammar/hive_grammar.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/grammar_change.hpp"
#include "duckdb/parser/peg/parsed_grammar.hpp"
#include "duckdb/parser/peg/transformer/parse_result.hpp"
#include "duckdb/parser/peg/transformer/peg_transformer.hpp"
#include "duckdb/parser/statement/call_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

namespace duckdb {

namespace {

bool IsRule(const ParseResult &parse_result, const char *name) {
	return StringUtil::CIEquals(parse_result.Name(), name);
}

unique_ptr<ParsedExpression> Constant(const Value &value) {
	return ConstantExpression::FromValue(value);
}

unique_ptr<ParsedExpression> Call(const char *function_name, vector<FunctionArgument> arguments) {
	return make_uniq<FunctionExpression>(Identifier(function_name), std::move(arguments));
}

//! HivePartitionSpec <- 'PARTITION' Parens(List(HivePartitionValue))
//! HivePartitionValue <- ColumnName '=' Expression
//! -> list_value(struct_pack(key := 'dt', value := CAST(<expr> AS VARCHAR)), ...)
unique_ptr<ParsedExpression> TransformPartitionSpec(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &spec = parse_result.Cast<ListParseResult>();
	auto entries = PEGTransformerFactory::ExtractParseResultsFromList(
	    PEGTransformerFactory::ExtractResultFromParens(spec.GetChild(1)));
	vector<FunctionArgument> pairs;
	for (auto &entry : entries) {
		auto &key_value = entry.get().Cast<ListParseResult>();
		// ColumnName is matched as a bare identifier: read it, it has no transform of its own
		auto &key_result = key_value.GetChild(0);
		auto &key_identifier = key_result.type == ParseResultType::IDENTIFIER
		                           ? key_result.Cast<IdentifierParseResult>()
		                           : key_result.Cast<ListParseResult>().Child<IdentifierParseResult>(0);
		auto key = key_identifier.identifier;
		auto value = transformer.Transform<unique_ptr<ParsedExpression>>(key_value.GetChild(2));
		vector<FunctionArgument> fields;
		fields.emplace_back(Identifier("key"), Constant(Value(key.GetIdentifierName())));
		fields.emplace_back(Identifier("value"), make_uniq<CastExpression>(LogicalType::VARCHAR, std::move(value)));
		pairs.emplace_back(Call("struct_pack", std::move(fields)));
	}
	return Call("list_value", std::move(pairs));
}

//! HiveLocation <- 'LOCATION' StringLiteral
unique_ptr<ParsedExpression> TransformLocation(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &location = parse_result.Cast<ListParseResult>();
	return Constant(Value(transformer.Transform<string>(location.GetChild(1))));
}

unique_ptr<ParsedExpression> MakeAction(const char *action, bool if_not_exists, bool if_exists,
                                        unique_ptr<ParsedExpression> partition,
                                        unique_ptr<ParsedExpression> new_partition,
                                        unique_ptr<ParsedExpression> location) {
	vector<FunctionArgument> fields;
	fields.emplace_back(Identifier("action"), Constant(Value(action)));
	fields.emplace_back(Identifier("if_not_exists"), Constant(Value::BOOLEAN(if_not_exists)));
	fields.emplace_back(Identifier("if_exists"), Constant(Value::BOOLEAN(if_exists)));
	fields.emplace_back(Identifier("partition"), partition ? std::move(partition) : Constant(Value()));
	fields.emplace_back(Identifier("new_partition"), new_partition ? std::move(new_partition) : Constant(Value()));
	fields.emplace_back(Identifier("location"), location ? std::move(location) : Constant(Value()));
	return Call("struct_pack", std::move(fields));
}

void TransformAction(PEGTransformer &transformer, ParseResult &action_result, vector<FunctionArgument> &actions) {
	// HivePartitionAction <- HiveAddPartitions / HiveDropPartitions / HiveRenamePartition / ...
	auto &action = action_result.Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
	auto &list = action.Cast<ListParseResult>();
	if (IsRule(action, "HiveAddPartitions")) {
		// 'ADD' IfNotExists? HivePartitionWithLocation+
		bool if_not_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : list.Child<RepeatParseResult>(2).GetChildren()) {
			// HivePartitionWithLocation <- HivePartitionSpec HiveLocation?
			auto &with_location = entry.get().Cast<ListParseResult>();
			auto partition = TransformPartitionSpec(transformer, with_location.GetChild(0));
			unique_ptr<ParsedExpression> location;
			auto &optional_location = with_location.Child<OptionalParseResult>(1);
			if (optional_location.HasResult()) {
				location = TransformLocation(transformer, optional_location.GetResult());
			}
			actions.emplace_back(
			    MakeAction("add", if_not_exists, false, std::move(partition), nullptr, std::move(location)));
		}
		return;
	}
	if (IsRule(action, "HiveDropPartitions")) {
		// 'DROP' IfExists? List(HivePartitionSpec)
		bool if_exists = list.Child<OptionalParseResult>(1).HasResult();
		for (auto &entry : PEGTransformerFactory::ExtractParseResultsFromList(list.GetChild(2))) {
			auto partition = TransformPartitionSpec(transformer, entry.get());
			actions.emplace_back(MakeAction("drop", false, if_exists, std::move(partition), nullptr, nullptr));
		}
		return;
	}
	if (IsRule(action, "HiveRenamePartition")) {
		// HivePartitionSpec 'RENAME' 'TO' HivePartitionSpec
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto new_partition = TransformPartitionSpec(transformer, list.GetChild(3));
		actions.emplace_back(
		    MakeAction("rename", false, false, std::move(partition), std::move(new_partition), nullptr));
		return;
	}
	if (IsRule(action, "HivePartitionSetLocation")) {
		// HivePartitionSpec 'SET' HiveLocation
		auto partition = TransformPartitionSpec(transformer, list.GetChild(0));
		auto location = TransformLocation(transformer, list.GetChild(2));
		actions.emplace_back(
		    MakeAction("set_partition_location", false, false, std::move(partition), nullptr, std::move(location)));
		return;
	}
	if (IsRule(action, "HiveTableSetLocation")) {
		// 'SET' HiveLocation
		auto location = TransformLocation(transformer, list.GetChild(1));
		actions.emplace_back(MakeAction("set_table_location", false, false, nullptr, nullptr, std::move(location)));
		return;
	}
	throw InternalException("Unknown Hive partition action rule '%s'", action.Name());
}

//! HiveAlterTableStatement <- 'ALTER' 'TABLE' BaseTableName HivePartitionAction+
//! -> CALL hive_alter_table('<table>', [actions])
arena_ptr<TransformResultValue> FinalizeHiveAlterTable(PEGTransformer &transformer, ParseResult &parse_result) {
	auto &list = parse_result.Cast<ListParseResult>();
	auto table = transformer.Transform<unique_ptr<BaseTableRef>>(list.GetChild(2));
	vector<FunctionArgument> actions;
	for (auto &action : list.Child<RepeatParseResult>(3).GetChildren()) {
		TransformAction(transformer, action.get(), actions);
	}
	vector<FunctionArgument> arguments;
	arguments.emplace_back(Constant(Value(table->GetQualifiedName().ToString())));
	arguments.emplace_back(Call("list_value", std::move(actions)));
	auto statement = make_uniq<CallStatement>();
	statement->function = Call("hive_alter_table", std::move(arguments));
	unique_ptr<SQLStatement> result = std::move(statement);
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(result));
}

arena_ptr<TransformProcess> StartHiveAlterTableTransform(PEGTransformer &transformer, ParseResult &parse_result) {
	return transformer.MakeProcess<FinalizeTransformProcess>(transformer, parse_result, FinalizeHiveAlterTable);
}

class HiveHiveDDLGrammar final : public GrammarExtension {
public:
	HiveHiveDDLGrammar()
	    : GrammarExtension("hive_ddl", "Hive partition DDL for Hive tables: ALTER TABLE ... "
	                                   "ADD / DROP PARTITION, RENAME PARTITION, SET LOCATION") {
	}

	vector<GrammarChange> GetChanges() const override {
		vector<GrammarChange> changes;
		changes.push_back(
		    GrammarChange::AddRule("HiveAlterTableStatement <- 'ALTER' 'TABLE' BaseTableName HivePartitionAction+",
		                           StartHiveAlterTableTransform));
		changes.push_back(GrammarChange::AddRule(
		    "HivePartitionAction <- HiveAddPartitions / HiveDropPartitions / HiveRenamePartition / "
		    "HivePartitionSetLocation / HiveTableSetLocation"));
		changes.push_back(GrammarChange::AddRule("HiveAddPartitions <- 'ADD' IfNotExists? HivePartitionWithLocation+"));
		changes.push_back(GrammarChange::AddRule("HivePartitionWithLocation <- HivePartitionSpec HiveLocation?"));
		changes.push_back(GrammarChange::AddRule("HiveDropPartitions <- 'DROP' IfExists? List(HivePartitionSpec)"));
		changes.push_back(
		    GrammarChange::AddRule("HiveRenamePartition <- HivePartitionSpec 'RENAME' 'TO' HivePartitionSpec"));
		changes.push_back(GrammarChange::AddRule("HivePartitionSetLocation <- HivePartitionSpec 'SET' HiveLocation"));
		changes.push_back(GrammarChange::AddRule("HiveTableSetLocation <- 'SET' HiveLocation"));
		changes.push_back(GrammarChange::AddRule("HivePartitionSpec <- 'PARTITION' Parens(List(HivePartitionValue))"));
		changes.push_back(GrammarChange::AddRule("HivePartitionValue <- ColumnName '=' Expression"));
		changes.push_back(GrammarChange::AddRule("HiveLocation <- 'LOCATION' StringLiteral"));
		// tried before the built-in ALTER statement; it fails on anything that is not a partition action, so the
		// built-in ALTER TABLE forms are unaffected
		changes.push_back(GrammarChange::PrependChoice("Statement", "HiveAlterTableStatement"));
		return changes;
	}
};

} // namespace

void RegisterHiveGrammarExtension(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<HiveHiveDDLGrammar>());
}

} // namespace duckdb
