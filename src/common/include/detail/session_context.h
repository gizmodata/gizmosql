// src/common/include/session_context.h
#pragma once
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <duckdb.hpp>

namespace gizmosql::ddb {
class DuckDBFlightSqlServer; // forward declare
}

struct ClientSession {
  std::weak_ptr<gizmosql::ddb::DuckDBFlightSqlServer> server;
  std::shared_ptr<duckdb::Connection> connection;
  std::mutex connection_mutex;  // Serializes access to connection per session
  std::string session_id; // from session middleware
  std::string username; // from bearer auth middleware (JWT sub/email/etc.)
  std::string role; // from JWT claims (e.g. "role") or header
  std::string peer; // client ip:port (ctx.peer())
  std::optional<std::string> active_sql_handle;
  std::optional<int32_t> query_timeout = std::nullopt;
  std::optional<arrow::util::ArrowLogLevel> query_log_level = std::nullopt;
};

// Inline utility for safe access
inline std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer>
GetServer(const ClientSession& session) {
  return session.server.lock();
}
