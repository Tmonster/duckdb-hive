//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hms/thrift_client.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/mutex.hpp"

#include "hms/thrift_protocol.hpp"

namespace duckdb {

//! The reply of a Thrift call: the result struct's field 0, or the exception (field 'exception_id') it threw
struct ThriftReply {
	ThriftValue success;
	//! The id of the result field that holds the exception, 0 when the call succeeded
	int16_t exception_id = 0;
	string exception_message;

public:
	bool Failed() const {
		return exception_id != 0;
	}
};

//! The metastore refused a call as such (Thrift's TApplicationException), e.g. a method it does not have
class ThriftApplicationException : public IOException {
public:
	static constexpr int32_t UNKNOWN_METHOD = 1;

	ThriftApplicationException(const string &message, int32_t type) : IOException(message), type(type) {
	}

	int32_t type;
};

//! One TCP connection to a Thrift service, binary protocol over a buffered (unframed) transport, as the Hive
//! Metastore serves it without SASL
class ThriftConnection {
public:
	ThriftConnection(const string &host, int32_t port, int32_t timeout_seconds);
	~ThriftConnection();

	ThriftReply Call(const string &method, const ThriftValue &arguments);
	//! Whether the last call got any byte of a reply: a call that did not may be sent again on a new connection
	bool ReceivedAny() const {
		return received_any;
	}

private:
	void SendAll(const string &data);
	bool Receive(string &buffer);

private:
	string host;
	int32_t port;
	int socket_fd = -1;
	int32_t sequence_id = 0;
	bool received_any = false;
};

//! The connections to one Thrift service: a call takes an idle one or opens a new one, and gives it back unless it
//! failed
class ThriftClient {
public:
	ThriftClient(string host, int32_t port, int32_t timeout_seconds);

	ThriftReply Call(const string &method, const ThriftValue &arguments);

	const string &Host() const {
		return host;
	}
	int32_t Port() const {
		return port;
	}

private:
	string host;
	int32_t port;
	int32_t timeout_seconds;
	mutex lock;
	vector<unique_ptr<ThriftConnection>> idle;
};

} // namespace duckdb
