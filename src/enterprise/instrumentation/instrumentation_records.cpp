// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "instrumentation_records.h"

#include "instrumentation_manager.h"

#include <algorithm>
#include <stdexcept>

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace gizmosql::ddb {

namespace {

std::string GenerateUUID() {
  return boost::uuids::to_string(boost::uuids::random_generator()());
}

// Execute one instrumentation write. The result is materialized
// (allow_stream_result=false) so a DML error is observed here, rather than
// deferred to an unfetched streaming result and then lost when it is destroyed.
// On any error this THROWS: the writer thread runs each write inside an explicit
// transaction (WriterTxnGuard), so the throw unwinds through the guard's
// destructor (rolling back this write) and is logged by the writer loop's
// handler. Throwing — rather than logging-and-returning — makes the rollback
// explicit and backend-independent, instead of relying on DuckDB silently
// discarding an aborted transaction on COMMIT (which is not guaranteed for
// writes routed to PostgreSQL via the postgres extension). Instrumentation is
// best-effort, so the writer never lets the throw escape the thread.
void ExecuteInstrumentationWrite(duckdb::Connection& conn, const char* operation,
                                 const std::string& sql,
                                 duckdb::vector<duckdb::Value> params) {
  auto stmt = conn.Prepare(sql);
  if (stmt->HasError()) {
    throw std::runtime_error(std::string("instrumentation write [") + operation +
                             "] failed to prepare: " + stmt->GetError());
  }
  auto result = stmt->Execute(params, /*allow_stream_result=*/false);
  if (result->HasError()) {
    throw std::runtime_error(std::string("instrumentation write [") + operation +
                             "] failed: " + result->GetError());
  }
}

}  // namespace

// ============================================================================
// InstanceInstrumentation
// ============================================================================

InstanceInstrumentation::InstanceInstrumentation(
    std::shared_ptr<InstrumentationManager> manager, const InstanceConfig& config)
    : manager_(std::move(manager)), instance_id_(config.instance_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([config](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record instance start",
        "INSERT INTO instances (instance_id, gizmosql_version, gizmosql_edition, "
        "duckdb_version, arrow_version, hostname, hostname_arg, server_ip, port, database_path, "
        "tls_enabled, tls_cert_path, tls_key_path, mtls_required, mtls_ca_cert_path, readonly, "
        "os_platform, os_name, os_version, cpu_arch, cpu_model, cpu_count, memory_total_bytes, "
        "start_time, status, instance_tag, cluster_id) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, "
        "$17, $18, $19, $20, $21, $22, $23, now(), 'running', $24, $25)",
        {
            duckdb::Value::UUID(config.instance_id),
            duckdb::Value(config.gizmosql_version),
            duckdb::Value(config.gizmosql_edition),
            duckdb::Value(config.duckdb_version),
            duckdb::Value(config.arrow_version),
            config.hostname.empty() ? duckdb::Value() : duckdb::Value(config.hostname),
            config.hostname_arg.empty() ? duckdb::Value() : duckdb::Value(config.hostname_arg),
            config.server_ip.empty() ? duckdb::Value() : duckdb::Value(config.server_ip),
            duckdb::Value(config.port),
            duckdb::Value(config.database_path),
            duckdb::Value::BOOLEAN(config.tls_enabled),
            config.tls_cert_path.empty() ? duckdb::Value() : duckdb::Value(config.tls_cert_path),
            config.tls_key_path.empty() ? duckdb::Value() : duckdb::Value(config.tls_key_path),
            duckdb::Value::BOOLEAN(config.mtls_required),
            config.mtls_ca_cert_path.empty() ? duckdb::Value() : duckdb::Value(config.mtls_ca_cert_path),
            duckdb::Value::BOOLEAN(config.readonly),
            config.os_platform.empty() ? duckdb::Value() : duckdb::Value(config.os_platform),
            config.os_name.empty() ? duckdb::Value() : duckdb::Value(config.os_name),
            config.os_version.empty() ? duckdb::Value() : duckdb::Value(config.os_version),
            config.cpu_arch.empty() ? duckdb::Value() : duckdb::Value(config.cpu_arch),
            config.cpu_model.empty() ? duckdb::Value() : duckdb::Value(config.cpu_model),
            duckdb::Value(config.cpu_count),
            duckdb::Value::BIGINT(config.memory_total_bytes),
            config.instance_tag.empty() ? duckdb::Value() : duckdb::Value(config.instance_tag),
            config.cluster_id.empty() ? duckdb::Value()
                                      : duckdb::Value::UUID(config.cluster_id),
        });
  });
}

InstanceInstrumentation::~InstanceInstrumentation() {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([id = instance_id_, reason = stop_reason_](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record instance stop",
        "UPDATE instances SET stop_time = now(), status = 'stopped', stop_reason = $2 "
        "WHERE instance_id = $1",
        {duckdb::Value::UUID(id), duckdb::Value(reason)});
  });
}

void InstanceInstrumentation::SetStopReason(const std::string& reason) {
  stop_reason_ = reason;
}

// ============================================================================
// SessionInstrumentation
// ============================================================================

SessionInstrumentation::SessionInstrumentation(
    std::shared_ptr<InstrumentationManager> manager, const std::string& instance_id,
    const std::string& session_id, const std::string& username, const std::string& role,
    const std::string& auth_method, const std::string& peer, const std::string& peer_identity,
    const std::string& user_agent, const std::string& connection_protocol)
    : manager_(std::move(manager)), instance_id_(instance_id), session_id_(session_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([instance_id, session_id, username, role, auth_method,
                        peer, peer_identity, user_agent,
                        connection_protocol](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record session start",
        "INSERT INTO sessions (session_id, instance_id, username, role, "
        "auth_method, peer, peer_identity, user_agent, connection_protocol, start_time, status) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), 'active')",
        {duckdb::Value::UUID(session_id), duckdb::Value::UUID(instance_id),
         duckdb::Value(username), duckdb::Value(role), duckdb::Value(auth_method),
         duckdb::Value(peer),
         peer_identity.empty() ? duckdb::Value() : duckdb::Value(peer_identity),
         user_agent.empty() ? duckdb::Value() : duckdb::Value(user_agent),
         duckdb::Value(connection_protocol)});
  });
}

SessionInstrumentation::~SessionInstrumentation() {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  // Map stop_reason to session_status enum value
  // Valid values: 'active', 'closed', 'killed', 'timeout', 'error'
  std::string status = stop_reason_;
  if (status != "closed" && status != "killed" && status != "timeout" && status != "error") {
    status = "closed";  // Default to 'closed' for normal termination
  }

  manager_->QueueWrite([id = session_id_, status, reason = stop_reason_](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record session stop",
        "UPDATE sessions SET stop_time = now(), status = $2, stop_reason = $3 "
        "WHERE session_id = $1",
        {duckdb::Value::UUID(id), duckdb::Value(status), duckdb::Value(reason)});
  });
}

void SessionInstrumentation::SetStopReason(const std::string& reason) {
  stop_reason_ = reason;
}

void SessionInstrumentation::UpdateSessionTag(const std::string& tag) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([id = session_id_, tag](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "update session tag",
        "UPDATE sessions SET session_tag = $2 WHERE session_id = $1",
        {duckdb::Value::UUID(id),
         tag.empty() ? duckdb::Value() : duckdb::Value(tag)});
  });
}

// ============================================================================
// StatementInstrumentation
// ============================================================================

StatementInstrumentation::StatementInstrumentation(
    std::shared_ptr<InstrumentationManager> manager, const std::string& statement_id,
    const std::string& session_id, const std::string& sql_text,
    const std::string& flight_method, bool is_internal, const std::string& prepare_error,
    const std::string& query_tag)
    : manager_(std::move(manager)), statement_id_(statement_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  bool prepare_success = prepare_error.empty();
  manager_->QueueWrite([statement_id, session_id, sql_text, flight_method,
                        is_internal, prepare_success, prepare_error,
                        query_tag](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record statement",
        "INSERT INTO sql_statements (statement_id, session_id, sql_text, "
        "flight_method, is_internal, prepare_success, prepare_error, created_time, query_tag) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, now(), $8)",
        {duckdb::Value::UUID(statement_id), duckdb::Value::UUID(session_id),
         duckdb::Value(sql_text),
         flight_method.empty() ? duckdb::Value() : duckdb::Value(flight_method),
         duckdb::Value::BOOLEAN(is_internal),
         duckdb::Value::BOOLEAN(prepare_success),
         prepare_error.empty() ? duckdb::Value() : duckdb::Value(prepare_error),
         query_tag.empty() ? duckdb::Value() : duckdb::Value(query_tag)});
  });
}

// ============================================================================
// ExecutionInstrumentation
// ============================================================================

ExecutionInstrumentation::ExecutionInstrumentation(
    std::shared_ptr<InstrumentationManager> manager, const std::string& execution_id,
    const std::string& statement_id, const std::string& bind_parameters)
    : manager_(std::move(manager)),
      execution_id_(execution_id),
      statement_id_(statement_id),
      start_timestamp_(std::chrono::system_clock::now()) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([execution_id, statement_id, bind_parameters](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record execution start",
        "INSERT INTO sql_executions (execution_id, statement_id, "
        "bind_parameters, execution_start_time, status, rows_fetched) "
        "VALUES ($1, $2, $3, now(), 'executing', 0)",
        {duckdb::Value::UUID(execution_id), duckdb::Value::UUID(statement_id),
         bind_parameters.empty() ? duckdb::Value() : duckdb::Value(bind_parameters)});
  });
}

ExecutionInstrumentation::~ExecutionInstrumentation() {
  if (!finalized_) {
    Finalize();
  }
}

void ExecutionInstrumentation::SetCompleted() {
  end_timestamp_ = std::chrono::system_clock::now();
  status_ = "success";
  // Don't finalize here - let the destructor handle it after all rows have been fetched
  // via IncrementRowsFetched() calls in the batch reader
}

void ExecutionInstrumentation::SetError(const std::string& error_message) {
  end_timestamp_ = std::chrono::system_clock::now();
  error_message_ = error_message;
  status_ = "error";
  Finalize();
}

void ExecutionInstrumentation::SetTimeout() {
  end_timestamp_ = std::chrono::system_clock::now();
  status_ = "timeout";
  Finalize();
}

void ExecutionInstrumentation::SetCancelled() {
  end_timestamp_ = std::chrono::system_clock::now();
  status_ = "cancelled";
  Finalize();
}

void ExecutionInstrumentation::SetQueued() {
  status_ = "queued";
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }
  manager_->QueueWrite([exec_id = execution_id_](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record execution queued",
        "UPDATE sql_executions SET status = 'queued', enqueue_time = now() "
        "WHERE execution_id = $1",
        {duckdb::Value::UUID(exec_id)});
  });
}

void ExecutionInstrumentation::SetRunning() {
  status_ = "executing";
  // The statement just left the queue; (re)start the execution clock so the
  // recorded duration excludes time spent waiting for a slot.
  start_timestamp_ = std::chrono::system_clock::now();
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }
  manager_->QueueWrite([exec_id = execution_id_](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record execution running",
        "UPDATE sql_executions SET status = 'executing', execution_start_time = now() "
        "WHERE execution_id = $1 AND status = 'queued'",
        {duckdb::Value::UUID(exec_id)});
  });
}

void ExecutionInstrumentation::IncrementRowsFetched(int64_t count) {
  rows_fetched_.fetch_add(count, std::memory_order_relaxed);
  const int64_t now_us = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::system_clock::now().time_since_epoch())
                             .count();
  int64_t expected = 0;
  first_fetch_us_.compare_exchange_strong(expected, now_us, std::memory_order_relaxed);
  last_fetch_us_.store(now_us, std::memory_order_relaxed);
}

void ExecutionInstrumentation::SetQueryProfile(std::string query_profile_json) {
  query_profile_ = std::move(query_profile_json);
}

void ExecutionInstrumentation::Finalize() {
  if (finalized_) {
    return;
  }
  finalized_ = true;

  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  // If end_timestamp_ wasn't set (destructor path), capture it now
  if (end_timestamp_.time_since_epoch().count() == 0) {
    end_timestamp_ = std::chrono::system_clock::now();
  }

  // The execution timeline is recorded as five timestamps, all derived from
  // wall-clock offsets against start_timestamp_ (captured synchronously, so
  // async write-queue latency can never skew them):
  //  * execution_end_time / duration_ms — when the ENGINE finished executing
  //    (end_timestamp_, stamped by SetCompleted/SetError/etc.).
  //  * fetch_start_time — first result batch handed to the client (NULL if
  //    none were, e.g. DML or an error).
  //  * fetch_end_time / total_duration_ms — when result DELIVERY finished: the
  //    last batch the client fetched, or the engine end if later (results
  //    stream on the prepared path, so Execute() returning is not the end of
  //    the work).
  //  * cursor_close_time — when the client released the statement/stream
  //    (i.e. now, at finalize). A client can hold a drained statement open for
  //    hours; that idle time shows up here and only here — it is deliberately
  //    excluded from both durations.
  const auto us_to_tp = [](int64_t us) {
    return std::chrono::system_clock::time_point{
        std::chrono::duration_cast<std::chrono::system_clock::duration>(
            std::chrono::microseconds(us))};
  };
  const auto ms_since = [this](std::chrono::system_clock::time_point end) {
    return std::max<int64_t>(0, std::chrono::duration_cast<std::chrono::milliseconds>(
                                    end - start_timestamp_)
                                    .count());
  };

  const int64_t first_fetch_us = first_fetch_us_.load(std::memory_order_relaxed);
  const int64_t last_fetch_us = last_fetch_us_.load(std::memory_order_relaxed);
  auto fetch_end = end_timestamp_;
  if (last_fetch_us > 0 && us_to_tp(last_fetch_us) > fetch_end) {
    fetch_end = us_to_tp(last_fetch_us);
  }

  const int64_t final_duration_ms = ms_since(end_timestamp_);
  const int64_t total_duration_ms = ms_since(fetch_end);
  // NULL (typed, so to_milliseconds() can bind it) when no batch was delivered.
  const duckdb::Value fetch_start_offset =
      first_fetch_us > 0 ? duckdb::Value::BIGINT(ms_since(us_to_tp(first_fetch_us)))
                         : duckdb::Value(duckdb::LogicalType::BIGINT);
  const int64_t cursor_close_offset_ms = ms_since(std::chrono::system_clock::now());

  manager_->QueueWrite([exec_id = execution_id_, rows = rows_fetched_.load(),
                        status = status_, error_msg = error_message_,
                        profile = query_profile_, final_duration_ms,
                        total_duration_ms, fetch_start_offset,
                        cursor_close_offset_ms](duckdb::Connection& conn) {
    // All timestamps are derived from execution_start_time (stamped by this
    // same writer connection) rather than now(), so that end - start always
    // equals the corresponding duration regardless of write-queue latency.
    ExecuteInstrumentationWrite(
        conn, "record execution end",
        "UPDATE sql_executions SET "
        "execution_end_time = execution_start_time + to_milliseconds($5), "
        "fetch_start_time = execution_start_time + to_milliseconds($8), "
        "fetch_end_time = execution_start_time + to_milliseconds($7), "
        "cursor_close_time = execution_start_time + to_milliseconds($9), "
        "rows_fetched = $2, status = $3, error_message = $4, "
        "duration_ms = $5, query_profile = $6, total_duration_ms = $7 "
        "WHERE execution_id = $1",
        {duckdb::Value::UUID(exec_id),
         duckdb::Value::BIGINT(rows), duckdb::Value(status),
         error_msg.empty() ? duckdb::Value() : duckdb::Value(error_msg),
         duckdb::Value::BIGINT(final_duration_ms),
         profile.empty() ? duckdb::Value() : duckdb::Value(profile),
         duckdb::Value::BIGINT(total_duration_ms), fetch_start_offset,
         duckdb::Value::BIGINT(cursor_close_offset_ms)});
  });
}

}  // namespace gizmosql::ddb
