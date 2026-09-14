// src/common/include/session_context.h
#pragma once
#include <atomic>
#include <cctype>
#include <chrono>
#include <map>
#include <limits>
#include <memory>
#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <optional>
#include <vector>
#include <duckdb.hpp>
#include <arrow/record_batch.h>
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

#ifdef GIZMOSQL_ENTERPRISE
namespace enterprise {
class MetricsRegistry;
}
#endif

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

// Lock-free std::optional replacement for small trivially convertible values
// (ints, bools, enums) that SET writes on one request thread while other
// requests on the same session read them concurrently. A plain std::optional
// would be a data race; here the value and its presence are one atomic word.
// load() returns a consistent snapshot; callers must not re-read expecting the
// same value.
template <typename T>
class AtomicOptional {
 public:
  AtomicOptional() = default;
  AtomicOptional(const AtomicOptional& other) : raw_(other.raw_.load()) {}
  AtomicOptional& operator=(const AtomicOptional& other) {
    raw_.store(other.raw_.load());
    return *this;
  }
  AtomicOptional& operator=(std::optional<T> value) {
    store(value);
    return *this;
  }
  AtomicOptional& operator=(T value) {
    store(value);
    return *this;
  }
  AtomicOptional& operator=(std::nullopt_t) {
    store(std::nullopt);
    return *this;
  }
  std::optional<T> load() const {
    const int64_t raw = raw_.load(std::memory_order_relaxed);
    if (raw == kNull) return std::nullopt;
    return static_cast<T>(raw);
  }
  void store(std::optional<T> value) {
    raw_.store(value ? static_cast<int64_t>(*value) : kNull, std::memory_order_relaxed);
  }

 private:
  static constexpr int64_t kNull = std::numeric_limits<int64_t>::min();
  std::atomic<int64_t> raw_{kNull};
};

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
  // Handle of the statement currently executing on this session. Written by
  // the executing request and read by CancelFlightInfo / teardown on other
  // threads, so it is swapped atomically as a shared_ptr rather than mutated
  // as a string (no lock, no data race). Empty pointer == nothing active.
  std::shared_ptr<const std::string> active_sql_handle;
  void SetActiveSqlHandle(std::string handle) {
    std::atomic_store(&active_sql_handle,
                      handle.empty()
                          ? std::shared_ptr<const std::string>{}
                          : std::make_shared<const std::string>(std::move(handle)));
  }
  std::shared_ptr<const std::string> ActiveSqlHandle() const {
    return std::atomic_load(&active_sql_handle);
  }
  static void SetTag(std::shared_ptr<const std::string>& slot, std::string value) {
    std::atomic_store(&slot, value.empty()
                                 ? std::shared_ptr<const std::string>{}
                                 : std::make_shared<const std::string>(std::move(value)));
  }
  static std::string Tag(const std::shared_ptr<const std::string>& slot) {
    const auto value = std::atomic_load(&slot);
    return value ? *value : std::string{};
  }
  // Per-session overrides written by SET on one request thread and read by
  // other requests on the same session, so they are lock-free atomics rather
  // than std::optional (see AtomicOptional). load() returns a snapshot.
  AtomicOptional<int32_t> query_timeout;
  AtomicOptional<arrow::util::ArrowLogLevel> query_log_level;
  // Per-session override for query profile capture (Enterprise). nullopt => use
  // the server default. Set via `SET gizmosql.capture_query_profile`.
  AtomicOptional<QueryProfileMode> capture_query_profile;
  // Statement-queue overrides (Enterprise). bypass_queue: skip the queue for this
  // session (admin-only to enable). max_queue_wait: per-session override of the
  // server's default queue wait. Both nullopt => fall through to server defaults.
  AtomicOptional<bool> bypass_queue;
  AtomicOptional<int32_t> max_queue_wait;
  // JSON-formatted tags (Enterprise feature, set via SET gizmosql.session_tag /
  // gizmosql.query_tag). Strings written by SET and read by statement creation
  // and settings queries on other request threads, so they are swapped
  // atomically as shared_ptrs; the accessors return snapshots (empty == unset).
  std::shared_ptr<const std::string> session_tag;
  std::shared_ptr<const std::string> query_tag;
  void SetSessionTag(std::string value) { SetTag(session_tag, std::move(value)); }
  void SetQueryTag(std::string value) { SetTag(query_tag, std::move(value)); }
  std::string SessionTag() const { return Tag(session_tag); }
  std::string QueryTag() const { return Tag(query_tag); }

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
#ifdef GIZMOSQL_ENTERPRISE
  // 0 = autocommit, 1 = transaction, 2 = transaction invalidated by an error.
  std::atomic<int> metrics_transaction_state{0};
  // Immutable after session creation: queries need no global registry lock.
  std::shared_ptr<enterprise::MetricsRegistry> metrics;
#endif

  // Immutable, session-owned results of eager executions. Tickets identify an
  // execution, never SQL or a reusable prepared statement. Cache misses fail
  // closed rather than re-executing. Bounded to avoid retaining abandoned tickets.
  struct CompletedExecution {
    std::shared_ptr<arrow::Schema> schema;
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::chrono::steady_clock::time_point created;
  };
  std::map<std::string, CompletedExecution> completed_executions;
  // Insertion order == age order (steady_clock is monotonic), so expiry and
  // capacity eviction pop from the front in O(1) instead of scanning the map.
  std::deque<std::string> completed_execution_order;
  std::mutex completed_executions_mutex;

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
