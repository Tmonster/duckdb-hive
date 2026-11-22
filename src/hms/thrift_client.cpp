#include "hms/thrift_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
#include <cerrno>
#include <cstring>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>
#define DUCKDB_HMS_SOCKETS
#endif

namespace duckdb {

#ifdef DUCKDB_HMS_SOCKETS

ThriftConnection::ThriftConnection(const string &host_p, int32_t port_p, int32_t timeout_seconds)
    : host(host_p), port(port_p) {
	addrinfo hints {};
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
	addrinfo *addresses = nullptr;
	auto service = to_string(port);
	auto status = getaddrinfo(host.c_str(), service.c_str(), &hints, &addresses);
	if (status != 0) {
		throw IOException("Could not resolve Hive metastore host '%s': %s", host, gai_strerror(status));
	}
	string error;
	for (auto address = addresses; address; address = address->ai_next) {
		auto fd = socket(address->ai_family, address->ai_socktype, address->ai_protocol);
		if (fd < 0) {
			error = strerror(errno);
			continue;
		}
		timeval timeout {};
		timeout.tv_sec = timeout_seconds;
		setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
		setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
		int one = 1;
		setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
		if (connect(fd, address->ai_addr, address->ai_addrlen) == 0) {
			socket_fd = fd;
			break;
		}
		error = strerror(errno);
		close(fd);
	}
	freeaddrinfo(addresses);
	if (socket_fd < 0) {
		throw IOException("Could not connect to the Hive metastore at %s:%d: %s", host, port, error);
	}
}

ThriftConnection::~ThriftConnection() {
	if (socket_fd >= 0) {
		close(socket_fd);
	}
}

void ThriftConnection::SendAll(const string &data) {
	idx_t sent = 0;
	while (sent < data.size()) {
		auto count = send(socket_fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
		if (count < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("Could not send to the Hive metastore at %s:%d: %s", host, port, strerror(errno));
		}
		sent += static_cast<idx_t>(count);
	}
}

bool ThriftConnection::Receive(string &buffer) {
	char data[65536];
	while (true) {
		auto count = recv(socket_fd, data, sizeof(data), 0);
		if (count < 0) {
			if (errno == EINTR) {
				continue;
			}
			throw IOException("Could not read from the Hive metastore at %s:%d: %s", host, port, strerror(errno));
		}
		if (count == 0) {
			return false;
		}
		buffer.append(data, static_cast<idx_t>(count));
		received_any = true;
		return true;
	}
}

#else

ThriftConnection::ThriftConnection(const string &host_p, int32_t port_p, int32_t timeout_seconds)
    : host(host_p), port(port_p) {
	throw NotImplementedException("Connecting to a Hive metastore is not supported on this platform");
}

ThriftConnection::~ThriftConnection() {
}

void ThriftConnection::SendAll(const string &data) {
}

bool ThriftConnection::Receive(string &buffer) {
	return false;
}

#endif

ThriftReply ThriftConnection::Call(const string &method, const ThriftValue &arguments) {
	ThriftWriter writer;
	received_any = false;
	auto id = ++sequence_id;
	writer.WriteMessageBegin(method, ThriftMessageType::CALL, id);
	writer.WriteStruct(arguments);
	SendAll(writer.Data());

	ThriftReader reader([&](string &buffer) { return Receive(buffer); });
	string name;
	ThriftMessageType type;
	int32_t reply_id;
	reader.ReadMessageBegin(name, type, reply_id);
	auto body = reader.ReadStruct();
	if (type == ThriftMessageType::EXCEPTION) {
		throw ThriftApplicationException(
		    StringUtil::Format("Hive metastore call %s failed: %s", method, body.GetString(1)),
		    static_cast<int32_t>(body.GetInteger(2)));
	}
	if (type != ThriftMessageType::REPLY || name != method || reply_id != id) {
		throw IOException("Hive metastore sent an unexpected reply '%s' to %s", name, method);
	}
	ThriftReply reply;
	for (auto &field : body.fields) {
		if (field.first == 0) {
			reply.success = field.second;
		} else {
			reply.exception_id = field.first;
			reply.exception_message = field.second.GetString(1);
		}
	}
	return reply;
}

ThriftClient::ThriftClient(string host_p, int32_t port_p, int32_t timeout_seconds_p)
    : host(std::move(host_p)), port(port_p), timeout_seconds(timeout_seconds_p) {
}

ThriftReply ThriftClient::Call(const string &method, const ThriftValue &arguments) {
	unique_ptr<ThriftConnection> connection;
	{
		lock_guard<mutex> guard(lock);
		if (!idle.empty()) {
			connection = std::move(idle.back());
			idle.pop_back();
		}
	}
	bool reused = connection != nullptr;
	if (!connection) {
		connection = make_uniq<ThriftConnection>(host, port, timeout_seconds);
	}
	ThriftReply reply;
	try {
		reply = connection->Call(method, arguments);
	} catch (ThriftApplicationException &) {
		lock_guard<mutex> guard(lock);
		idle.push_back(std::move(connection));
		throw;
	} catch (IOException &) {
		if (!reused || connection->ReceivedAny()) {
			throw;
		}
		// the metastore may have closed an idle connection: one more try on a new one
		connection = make_uniq<ThriftConnection>(host, port, timeout_seconds);
		reply = connection->Call(method, arguments);
	}
	lock_guard<mutex> guard(lock);
	idle.push_back(std::move(connection));
	return reply;
}

} // namespace duckdb
