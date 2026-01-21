// src/common/include/session_context.h
#pragma once
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <optional>
#include <vector>
#include <duckdb.hpp>

#include "request_ctx.h"  // For CatalogAccessRule, CatalogAccessLevel

namespace gizmosql::ddb {
class DuckDBFlightSqlServer;  // forward declare
class SessionInstrumentation;  // forward declare
}

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

  // Catalog-level access controls from JWT token claims
  // If empty, full access is granted (backward compatible)
  // Rules are evaluated in order; first match wins
  std::vector<CatalogAccessRule> catalog_access;

  // Instrumentation for session lifecycle tracking
  std::unique_ptr<gizmosql::ddb::SessionInstrumentation> instrumentation;

  // Flag for KILL SESSION support - when set, the session should be terminated
  std::atomic<bool> kill_requested{false};

  // Check if this session has the required access level for a catalog
  // Returns the access level granted, considering rules in order (first match wins)
  // Special handling for _gizmosql_instr: only admins can read, no one can write via client
  CatalogAccessLevel GetCatalogAccess(const std::string& catalog_name) const {
    // The instrumentation database is special: system-managed, read-only for admins
    // Token-based catalog_access rules do NOT override this protection
    if (catalog_name == "_gizmosql_instr") {
      return (role == "admin") ? CatalogAccessLevel::kRead : CatalogAccessLevel::kNone;
    }

    // If no catalog_access rules defined, grant full access (backward compatible)
    if (catalog_access.empty()) {
      return CatalogAccessLevel::kWrite;
    }

    // Check rules in order - first match wins
    for (const auto& rule : catalog_access) {
      if (rule.catalog == catalog_name || rule.catalog == "*") {
        return rule.access;
      }
    }

    // No matching rule - deny access
    return CatalogAccessLevel::kNone;
  }

  // Convenience methods
  bool HasReadAccess(const std::string& catalog_name) const {
    return GetCatalogAccess(catalog_name) >= CatalogAccessLevel::kRead;
  }

  bool HasWriteAccess(const std::string& catalog_name) const {
    return GetCatalogAccess(catalog_name) >= CatalogAccessLevel::kWrite;
  }
};

// Inline utility for safe access
inline std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> GetServer(
    const ClientSession& session) {
  return session.server.lock();
}
