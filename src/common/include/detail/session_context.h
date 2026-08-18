// src/common/include/session_context.h
#pragma once
#include <atomic>
#include <cctype>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <optional>
#include <vector>
#include <duckdb.hpp>
#include <arrow/util/logging.h>

#include "request_ctx.h"  // For CatalogAccessRule, CatalogAccessLevel
#include "tracked_duckdb_connection.h"

namespace gizmosql::ddb {
class DuckDBFlightSqlServer;  // forward declare
class DuckDBStatement;        // forward declare
#ifdef GIZMOSQL_ENTERPRISE
class SessionInstrumentation;  // forward declare
#endif
}

namespace gizmosql {

// Controls whether DuckDB query profiling is captured into the instrumentation
// `sql_executions.query_profile` column (Enterprise feature). Settable at the
// server level (--capture-query-profile / GIZMOSQL_CAPTURE_QUERY_PROFILE) and
// overridable per-session or globally via `SET gizmosql.capture_query_profile`.
//   kOff      - no profiling captured (default; zero overhead)
//   kStandard - per-operator profile (DuckDB `enable_profiling`)
//   kDetailed - additionally times each expression (DuckDB `profiling_mode=detailed`)
enum class QueryProfileMode { kOff, kStandard, kDetailed };

// Parse a capture-query-profile string. Throws std::invalid_argument on an
// unrecognized value so callers (CLI/library resolution and the SET handler)
// can surface a precise error.
inline QueryProfileMode query_profile_mode_from_string(const std::string& s) {
  std::string v;
  v.reserve(s.size());
  for (char c : s) v += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  if (v == "off" || v == "none" || v == "false" || v == "0") return QueryProfileMode::kOff;
  if (v == "standard" || v == "on" || v == "true" || v == "1") return QueryProfileMode::kStandard;
  if (v == "detailed") return QueryProfileMode::kDetailed;
  throw std::invalid_argument("Invalid capture_query_profile value '" + s +
                              "' (expected off, standard, or detailed)");
}

inline std::string query_profile_mode_to_string(QueryProfileMode mode) {
  switch (mode) {
    case QueryProfileMode::kStandard:
      return "standard";
    case QueryProfileMode::kDetailed:
      return "detailed";
    case QueryProfileMode::kOff:
    default:
      return "off";
  }
}

struct ClientSession {
  std::weak_ptr<gizmosql::ddb::DuckDBFlightSqlServer> server;
  std::shared_ptr<TrackedDuckDBConnection> connection;
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
  // Per-session override for query profile capture (Enterprise). nullopt => use
  // the server default. Set via `SET gizmosql.capture_query_profile`.
  std::optional<QueryProfileMode> capture_query_profile = std::nullopt;
  // Statement-queue overrides (Enterprise). bypass_queue: skip the queue for this
  // session (admin-only to enable). max_queue_wait: per-session override of the
  // server's default queue wait. Both nullopt => fall through to server defaults.
  std::optional<bool> bypass_queue = std::nullopt;
  std::optional<int32_t> max_queue_wait = std::nullopt;
  std::string session_tag;  // JSON-formatted session tag (Enterprise feature, set via SET gizmosql.session_tag)
  std::string query_tag;    // JSON-formatted query tag (Enterprise feature, set via SET gizmosql.query_tag)

  // Catalog-level access controls from JWT token claims (Enterprise feature)
  // If empty, full access is granted (backward compatible)
  // Rules are evaluated in order; first match wins
  // Access checking is done via enterprise::HasReadAccess/HasWriteAccess
  std::vector<CatalogAccessRule> catalog_access;

#ifdef GIZMOSQL_ENTERPRISE
  // Instrumentation for session lifecycle tracking (Enterprise feature)
  std::unique_ptr<gizmosql::ddb::SessionInstrumentation> instrumentation;
#endif

  // Flag for KILL SESSION support - when set, the session should be terminated
  std::atomic<bool> kill_requested{false};

  // Last user-SQL activity for --session-idle-timeout (0 = unset until TouchSqlActivity).
  // Touched on statement create and on each user-SQL FetchResult (row download).
  std::atomic<int64_t> last_sql_activity_ns{0};

  // Sweeper-facing "busy executing?" count. Do not read active_sql_handle for
  // this (plain string; racy from other threads). Maintained exclusively by
  // ScopedSqlInFlight so no code path can leak a session into a permanently
  // "busy" (never-evictable) state; a count (not a bool) so concurrent
  // statements on one session cannot clear each other's busy state.
  std::atomic<int32_t> sql_in_flight{0};

  // Prepared statements owned by this session
  std::map<std::string, std::shared_ptr<gizmosql::ddb::DuckDBStatement>> prepared_statements;
  mutable std::shared_mutex statements_mutex;

  void TouchSqlActivity() {
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        std::chrono::steady_clock::now().time_since_epoch())
                        .count();
    last_sql_activity_ns.store(ns, std::memory_order_relaxed);
  }

  std::chrono::steady_clock::time_point LastSqlActivity() const {
    return std::chrono::steady_clock::time_point(
        std::chrono::nanoseconds(last_sql_activity_ns.load(std::memory_order_relaxed)));
  }

  bool HasInFlightSql() const {
    return sql_in_flight.load(std::memory_order_relaxed) > 0;
  }

  // Destructor handles session cleanup:
  // 1. Interrupts any in-flight query on the DuckDB connection
  // 2. Clears prepared statements (releasing DuckDB handles before connection closes)
  // 3. TrackedDuckDBConnection destructor decrements the open connection counter
  ~ClientSession();
};

// RAII marker for "this session is executing SQL right now", read by the
// idle-session sweeper via ClientSession::HasInFlightSql(). Scope it to the
// execution only (not statement lifetime): a statement that is created but
// never executed, or an execute that errors out on any path, must not leave
// the session permanently "busy" and therefore never evictable.
class ScopedSqlInFlight {
 public:
  explicit ScopedSqlInFlight(std::shared_ptr<ClientSession> session)
      : session_(std::move(session)) {
    session_->sql_in_flight.fetch_add(1, std::memory_order_relaxed);
  }
  ~ScopedSqlInFlight() {
    session_->sql_in_flight.fetch_sub(1, std::memory_order_relaxed);
  }
  ScopedSqlInFlight(const ScopedSqlInFlight&) = delete;
  ScopedSqlInFlight& operator=(const ScopedSqlInFlight&) = delete;

 private:
  std::shared_ptr<ClientSession> session_;
};

// Inline utility for safe access
inline std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> GetServer(
    const ClientSession& session) {
  return session.server.lock();
}

}  // namespace gizmosql
