//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms/thrift_protocol.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/map.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

enum class ThriftType : uint8_t {
	STOP = 0,
	BOOL = 2,
	BYTE = 3,
	DOUBLE = 4,
	I16 = 6,
	I32 = 8,
	I64 = 10,
	STRING = 11,
	STRUCT = 12,
	MAP = 13,
	SET = 14,
	LIST = 15
};

enum class ThriftMessageType : uint8_t { CALL = 1, REPLY = 2, EXCEPTION = 3, ONEWAY = 4 };

//! A decoded Thrift value of any type. Structs keep every field, known or not, so a struct read from the metastore
//! is written back unchanged except for the fields that were set.
struct ThriftValue {
	ThriftType type = ThriftType::STOP;
	int64_t integer = 0;
	double floating = 0;
	string str;
	//! STRUCT: the fields by id, in id order
	map<int16_t, ThriftValue> fields;
	//! LIST / SET: the elements; MAP: keys and values alternating
	ThriftType element_type = ThriftType::STOP;
	ThriftType value_type = ThriftType::STOP;
	vector<ThriftValue> elements;

public:
	static ThriftValue Bool(bool value);
	static ThriftValue I16(int16_t value);
	static ThriftValue I32(int32_t value);
	static ThriftValue I64(int64_t value);
	static ThriftValue String(const string &value);
	static ThriftValue Struct();
	static ThriftValue List(ThriftType element_type);
	static ThriftValue StringList(const vector<string> &values);
	static ThriftValue StringMap(const unordered_map<string, string> &values);

	bool Has(int16_t id) const;
	//! The field 'id' of a struct, or an empty value (STOP) when it is not set
	const ThriftValue &Field(int16_t id) const;
	ThriftValue &SetField(int16_t id, ThriftValue value);
	void ClearField(int16_t id);

	string GetString(int16_t id) const;
	int64_t GetInteger(int16_t id, int64_t fallback = 0) const;
	bool GetBool(int16_t id, bool fallback = false) const;
	vector<string> GetStringList(int16_t id) const;
	unordered_map<string, string> GetStringMap(int16_t id) const;
};

//! TBinaryProtocol (strict) encoding of messages and values
class ThriftWriter {
public:
	void WriteMessageBegin(const string &name, ThriftMessageType type, int32_t sequence_id);
	//! A struct's fields followed by STOP
	void WriteStruct(const ThriftValue &value);
	void WriteValue(const ThriftValue &value);

	string &Data() {
		return buffer;
	}

private:
	void WriteByte(uint8_t value);
	void WriteI16(int16_t value);
	void WriteI32(int32_t value);
	void WriteI64(int64_t value);
	void WriteBinary(const string &value);

private:
	string buffer;
};

class ThriftReader {
public:
	//! Pulls more bytes when the buffer runs out; returns false at the end of the stream
	using fill_t = std::function<bool(string &buffer)>;
	explicit ThriftReader(fill_t fill);

	void ReadMessageBegin(string &name, ThriftMessageType &type, int32_t &sequence_id);
	ThriftValue ReadStruct();
	ThriftValue ReadValue(ThriftType type);

private:
	void Need(idx_t count);
	uint8_t ReadByte();
	int16_t ReadI16();
	int32_t ReadI32();
	int64_t ReadI64();
	string ReadBinary();
	idx_t ReadSize();

private:
	fill_t fill;
	string buffer;
	idx_t position = 0;
	idx_t depth = 0;
};

} // namespace duckdb
