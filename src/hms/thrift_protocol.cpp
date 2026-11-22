#include "hms/thrift_protocol.hpp"

#include "duckdb/common/exception.hpp"

#include <cstring>

namespace duckdb {

static constexpr uint32_t THRIFT_VERSION_1 = 0x80010000;
static constexpr uint32_t THRIFT_VERSION_MASK = 0xffff0000;
//! Larger strings or collections than any metastore object holds: a stream out of sync, not data
static constexpr idx_t THRIFT_MAX_SIZE = 512ULL * 1024ULL * 1024ULL;
static constexpr idx_t THRIFT_MAX_DEPTH = 64;

ThriftValue ThriftValue::Bool(bool value) {
	ThriftValue result;
	result.type = ThriftType::BOOL;
	result.integer = value ? 1 : 0;
	return result;
}

ThriftValue ThriftValue::I16(int16_t value) {
	ThriftValue result;
	result.type = ThriftType::I16;
	result.integer = value;
	return result;
}

ThriftValue ThriftValue::I32(int32_t value) {
	ThriftValue result;
	result.type = ThriftType::I32;
	result.integer = value;
	return result;
}

ThriftValue ThriftValue::I64(int64_t value) {
	ThriftValue result;
	result.type = ThriftType::I64;
	result.integer = value;
	return result;
}

ThriftValue ThriftValue::String(const string &value) {
	ThriftValue result;
	result.type = ThriftType::STRING;
	result.str = value;
	return result;
}

ThriftValue ThriftValue::Struct() {
	ThriftValue result;
	result.type = ThriftType::STRUCT;
	return result;
}

ThriftValue ThriftValue::List(ThriftType element_type) {
	ThriftValue result;
	result.type = ThriftType::LIST;
	result.element_type = element_type;
	return result;
}

ThriftValue ThriftValue::StringList(const vector<string> &values) {
	auto result = List(ThriftType::STRING);
	for (auto &value : values) {
		result.elements.push_back(String(value));
	}
	return result;
}

ThriftValue ThriftValue::StringMap(const unordered_map<string, string> &values) {
	ThriftValue result;
	result.type = ThriftType::MAP;
	result.element_type = ThriftType::STRING;
	result.value_type = ThriftType::STRING;
	// sorted, so that the same map always encodes the same
	map<string, string> sorted(values.begin(), values.end());
	for (auto &entry : sorted) {
		result.elements.push_back(String(entry.first));
		result.elements.push_back(String(entry.second));
	}
	return result;
}

bool ThriftValue::Has(int16_t id) const {
	return fields.find(id) != fields.end();
}

const ThriftValue &ThriftValue::Field(int16_t id) const {
	static const ThriftValue EMPTY;
	auto entry = fields.find(id);
	return entry == fields.end() ? EMPTY : entry->second;
}

ThriftValue &ThriftValue::SetField(int16_t id, ThriftValue value) {
	auto &slot = fields[id];
	slot = std::move(value);
	return slot;
}

void ThriftValue::ClearField(int16_t id) {
	fields.erase(id);
}

string ThriftValue::GetString(int16_t id) const {
	return Field(id).str;
}

int64_t ThriftValue::GetInteger(int16_t id, int64_t fallback) const {
	auto &field = Field(id);
	return field.type == ThriftType::STOP ? fallback : field.integer;
}

bool ThriftValue::GetBool(int16_t id, bool fallback) const {
	auto &field = Field(id);
	return field.type == ThriftType::STOP ? fallback : field.integer != 0;
}

vector<string> ThriftValue::GetStringList(int16_t id) const {
	vector<string> result;
	for (auto &element : Field(id).elements) {
		result.push_back(element.str);
	}
	return result;
}

unordered_map<string, string> ThriftValue::GetStringMap(int16_t id) const {
	unordered_map<string, string> result;
	auto &elements = Field(id).elements;
	for (idx_t i = 0; i + 1 < elements.size(); i += 2) {
		result[elements[i].str] = elements[i + 1].str;
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Writer
//===--------------------------------------------------------------------===//
void ThriftWriter::WriteByte(uint8_t value) {
	buffer.push_back(static_cast<char>(value));
}

void ThriftWriter::WriteI16(int16_t value) {
	auto bits = static_cast<uint16_t>(value);
	WriteByte(static_cast<uint8_t>(bits >> 8));
	WriteByte(static_cast<uint8_t>(bits));
}

void ThriftWriter::WriteI32(int32_t value) {
	auto bits = static_cast<uint32_t>(value);
	for (int32_t shift = 24; shift >= 0; shift -= 8) {
		WriteByte(static_cast<uint8_t>(bits >> shift));
	}
}

void ThriftWriter::WriteI64(int64_t value) {
	auto bits = static_cast<uint64_t>(value);
	for (int32_t shift = 56; shift >= 0; shift -= 8) {
		WriteByte(static_cast<uint8_t>(bits >> shift));
	}
}

void ThriftWriter::WriteBinary(const string &value) {
	WriteI32(static_cast<int32_t>(value.size()));
	buffer += value;
}

void ThriftWriter::WriteMessageBegin(const string &name, ThriftMessageType type, int32_t sequence_id) {
	WriteI32(static_cast<int32_t>(THRIFT_VERSION_1 | static_cast<uint32_t>(type)));
	WriteBinary(name);
	WriteI32(sequence_id);
}

void ThriftWriter::WriteStruct(const ThriftValue &value) {
	for (auto &field : value.fields) {
		WriteByte(static_cast<uint8_t>(field.second.type));
		WriteI16(field.first);
		WriteValue(field.second);
	}
	WriteByte(static_cast<uint8_t>(ThriftType::STOP));
}

void ThriftWriter::WriteValue(const ThriftValue &value) {
	switch (value.type) {
	case ThriftType::BOOL:
	case ThriftType::BYTE:
		WriteByte(static_cast<uint8_t>(value.integer));
		break;
	case ThriftType::I16:
		WriteI16(static_cast<int16_t>(value.integer));
		break;
	case ThriftType::I32:
		WriteI32(static_cast<int32_t>(value.integer));
		break;
	case ThriftType::I64:
		WriteI64(value.integer);
		break;
	case ThriftType::DOUBLE: {
		int64_t bits;
		memcpy(&bits, &value.floating, sizeof(bits));
		WriteI64(bits);
		break;
	}
	case ThriftType::STRING:
		WriteBinary(value.str);
		break;
	case ThriftType::STRUCT:
		WriteStruct(value);
		break;
	case ThriftType::MAP:
		WriteByte(static_cast<uint8_t>(value.element_type));
		WriteByte(static_cast<uint8_t>(value.value_type));
		WriteI32(static_cast<int32_t>(value.elements.size() / 2));
		for (auto &element : value.elements) {
			WriteValue(element);
		}
		break;
	case ThriftType::SET:
	case ThriftType::LIST:
		WriteByte(static_cast<uint8_t>(value.element_type));
		WriteI32(static_cast<int32_t>(value.elements.size()));
		for (auto &element : value.elements) {
			WriteValue(element);
		}
		break;
	default:
		throw InternalException("Thrift: can not write a value of type %d", static_cast<int>(value.type));
	}
}

//===--------------------------------------------------------------------===//
// Reader
//===--------------------------------------------------------------------===//
ThriftReader::ThriftReader(fill_t fill_p) : fill(std::move(fill_p)) {
}

void ThriftReader::Need(idx_t count) {
	while (buffer.size() - position < count) {
		if (position > 0) {
			buffer.erase(0, position);
			position = 0;
		}
		if (!fill(buffer)) {
			throw IOException("Hive metastore closed the connection in the middle of a response");
		}
	}
}

uint8_t ThriftReader::ReadByte() {
	Need(1);
	return static_cast<uint8_t>(buffer[position++]);
}

int16_t ThriftReader::ReadI16() {
	Need(2);
	auto bits = static_cast<uint16_t>((static_cast<uint8_t>(buffer[position]) << 8) |
	                                  static_cast<uint8_t>(buffer[position + 1]));
	position += 2;
	return static_cast<int16_t>(bits);
}

int32_t ThriftReader::ReadI32() {
	Need(4);
	uint32_t bits = 0;
	for (idx_t i = 0; i < 4; i++) {
		bits = (bits << 8) | static_cast<uint8_t>(buffer[position + i]);
	}
	position += 4;
	return static_cast<int32_t>(bits);
}

int64_t ThriftReader::ReadI64() {
	Need(8);
	uint64_t bits = 0;
	for (idx_t i = 0; i < 8; i++) {
		bits = (bits << 8) | static_cast<uint8_t>(buffer[position + i]);
	}
	position += 8;
	return static_cast<int64_t>(bits);
}

idx_t ThriftReader::ReadSize() {
	auto size = ReadI32();
	if (size < 0 || static_cast<idx_t>(size) > THRIFT_MAX_SIZE) {
		throw IOException("Hive metastore sent an invalid Thrift size %d", size);
	}
	return static_cast<idx_t>(size);
}

string ThriftReader::ReadBinary() {
	auto size = ReadSize();
	Need(size);
	auto result = buffer.substr(position, size);
	position += size;
	return result;
}

void ThriftReader::ReadMessageBegin(string &name, ThriftMessageType &type, int32_t &sequence_id) {
	auto header = static_cast<uint32_t>(ReadI32());
	if ((header & THRIFT_VERSION_MASK) != THRIFT_VERSION_1) {
		throw IOException("Hive metastore sent a message that is not in Thrift's binary protocol: is the metastore "
		                  "running with SASL or the HTTP transport?");
	}
	type = static_cast<ThriftMessageType>(header & 0xff);
	name = ReadBinary();
	sequence_id = ReadI32();
}

ThriftValue ThriftReader::ReadStruct() {
	if (++depth > THRIFT_MAX_DEPTH) {
		throw IOException("Hive metastore sent Thrift structs nested too deep");
	}
	auto result = ThriftValue::Struct();
	while (true) {
		auto type = static_cast<ThriftType>(ReadByte());
		if (type == ThriftType::STOP) {
			break;
		}
		auto id = ReadI16();
		result.fields[id] = ReadValue(type);
	}
	depth--;
	return result;
}

ThriftValue ThriftReader::ReadValue(ThriftType type) {
	ThriftValue result;
	result.type = type;
	switch (type) {
	case ThriftType::BOOL:
	case ThriftType::BYTE:
		result.integer = ReadByte();
		break;
	case ThriftType::I16:
		result.integer = ReadI16();
		break;
	case ThriftType::I32:
		result.integer = ReadI32();
		break;
	case ThriftType::I64:
		result.integer = ReadI64();
		break;
	case ThriftType::DOUBLE: {
		auto bits = ReadI64();
		memcpy(&result.floating, &bits, sizeof(bits));
		break;
	}
	case ThriftType::STRING:
		result.str = ReadBinary();
		break;
	case ThriftType::STRUCT:
		return ReadStruct();
	case ThriftType::MAP: {
		result.element_type = static_cast<ThriftType>(ReadByte());
		result.value_type = static_cast<ThriftType>(ReadByte());
		auto size = ReadSize();
		for (idx_t i = 0; i < size; i++) {
			result.elements.push_back(ReadValue(result.element_type));
			result.elements.push_back(ReadValue(result.value_type));
		}
		break;
	}
	case ThriftType::SET:
	case ThriftType::LIST: {
		result.element_type = static_cast<ThriftType>(ReadByte());
		auto size = ReadSize();
		for (idx_t i = 0; i < size; i++) {
			result.elements.push_back(ReadValue(result.element_type));
		}
		break;
	}
	default:
		throw IOException("Hive metastore sent an unknown Thrift type %d", static_cast<int>(type));
	}
	return result;
}

} // namespace duckdb
