// src/common/include/session_context.h
#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <vector>
#include <duckdb.hpp>
#include <arrow/util/logging.h>

#include "request_ctx.h"  // For CatalogAccessRule, CatalogAccessLevel

namespace gizmosql::ddb {
class DuckDBFlightSqlServer;  // forward declare
class SessionInstrumentation;  // forward declare
}

namespace gizmosql {

struct ClientSession {
  std::weak_ptr<gizmosql::ddb::DuckDBFlightSqlServer> server;
  std::shared_ptr<duckdb::Connection> connection;
  std::string instance_id; // server instance UUID (for multi-instance log correlation)
  std::string session_id;  // from session middleware
  std::string username;    // from bearer auth middleware (JWT sub/email/etc.)
  std::string role;        // from JWT claims (e.g. "role") or header
  std::string peer;        // client ip:port (ctx.peer())
  std::string peer_identity;  // mTLS client certificate identity (empty if not using mTLS)
  std::string auth_method; // authentication method (e.g. "Basic", "BootstrapToken")
  std::string user_agent;  // user-agent header from client (for client type detection)
  std::string connection_protocol;  // "plaintext", "tls", or "mtls"
  std::optional<std::string> active_sql_handle;
  std::optional<int32_t> query_timeout = std::nullopt;
  std::optional<arrow::util::ArrowLogLevel> query_log_level = std::nullopt;

  // Catalog-level access controls from JWT token claims (Enterprise feature)
  // If empty, full access is granted (backward compatible)
  // Rules are evaluated in order; first match wins
  // Access checking is done via enterprise::HasReadAccess/HasWriteAccess
  std::vector<CatalogAccessRule> catalog_access;

  // Instrumentation for session lifecycle tracking
  std::unique_ptr<gizmosql::ddb::SessionInstrumentation> instrumentation;

  // Flag for KILL SESSION support - when set, the session should be terminated
  std::atomic<bool> kill_requested{false};
};

// Inline utility for safe access
inline std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> GetServer(
    const ClientSession& session) {
  return session.server.lock();
}

}  // namespace gizmosql
