// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "instrumentation_records.h"

#include "instrumentation_manager.h"

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

void ExecutionInstrumentation::SetCompleted(int64_t duration_ms) {
  end_timestamp_ = std::chrono::system_clock::now();
  duration_ms_ = duration_ms;
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

  // Calculate duration from wall-clock times if not explicitly provided
  int64_t final_duration_ms = duration_ms_;
  if (final_duration_ms == 0) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_timestamp_ - start_timestamp_);
    final_duration_ms = duration.count();
  }

  manager_->QueueWrite([exec_id = execution_id_, rows = rows_fetched_.load(),
                        status = status_, error_msg = error_message_,
                        profile = query_profile_,
                        final_duration_ms](duckdb::Connection& conn) {
    ExecuteInstrumentationWrite(
        conn, "record execution end",
        "UPDATE sql_executions SET execution_end_time = now(), "
        "rows_fetched = $2, status = $3, error_message = $4, "
        "duration_ms = $5, query_profile = $6 WHERE execution_id = $1",
        {duckdb::Value::UUID(exec_id),
         duckdb::Value::BIGINT(rows), duckdb::Value(status),
         error_msg.empty() ? duckdb::Value() : duckdb::Value(error_msg),
         duckdb::Value::BIGINT(final_duration_ms),
         profile.empty() ? duckdb::Value() : duckdb::Value(profile)});
  });
}

}  // namespace gizmosql::ddb
