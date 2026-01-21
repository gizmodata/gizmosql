// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "instrumentation_records.h"

#include "instrumentation_manager.h"

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

namespace gizmosql::ddb {

namespace {

std::string GenerateUUID() {
  return boost::uuids::to_string(boost::uuids::random_generator()());
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
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.instances (instance_id, gizmosql_version, gizmosql_edition, "
        "duckdb_version, arrow_version, hostname, hostname_arg, server_ip, port, database_path, "
        "tls_enabled, tls_cert_path, tls_key_path, mtls_required, mtls_ca_cert_path, readonly) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)");
    stmt->Execute(
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
        duckdb::Value::BOOLEAN(config.readonly));
  });
}

InstanceInstrumentation::~InstanceInstrumentation() {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([id = instance_id_, reason = stop_reason_](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "UPDATE _gizmosql_instr.instances SET stop_time = now(), status = 'stopped', stop_reason = $2 "
        "WHERE instance_id = $1");
    stmt->Execute(duckdb::Value::UUID(id), duckdb::Value(reason));
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
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sessions (session_id, instance_id, username, role, "
        "auth_method, peer, peer_identity, user_agent, connection_protocol) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::_gizmosql_instr.connection_protocol)");
    stmt->Execute(duckdb::Value::UUID(session_id), duckdb::Value::UUID(instance_id),
                  duckdb::Value(username), duckdb::Value(role), duckdb::Value(auth_method),
                  duckdb::Value(peer),
                  peer_identity.empty() ? duckdb::Value() : duckdb::Value(peer_identity),
                  user_agent.empty() ? duckdb::Value() : duckdb::Value(user_agent),
                  duckdb::Value(connection_protocol));
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
    auto stmt = conn.Prepare(
        "UPDATE _gizmosql_instr.sessions SET stop_time = now(), status = $2::_gizmosql_instr.session_status, stop_reason = $3 "
        "WHERE session_id = $1");
    stmt->Execute(duckdb::Value::UUID(id), duckdb::Value(status), duckdb::Value(reason));
  });
}

void SessionInstrumentation::SetStopReason(const std::string& reason) {
  stop_reason_ = reason;
}

// ============================================================================
// StatementInstrumentation
// ============================================================================

StatementInstrumentation::StatementInstrumentation(
    std::shared_ptr<InstrumentationManager> manager, const std::string& statement_id,
    const std::string& session_id, const std::string& sql_text,
    const std::string& flight_method, bool is_internal, const std::string& prepare_error)
    : manager_(std::move(manager)), statement_id_(statement_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  bool prepare_success = prepare_error.empty();
  manager_->QueueWrite([statement_id, session_id, sql_text, flight_method,
                        is_internal, prepare_success, prepare_error](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sql_statements (statement_id, session_id, sql_text, "
        "flight_method, is_internal, prepare_success, prepare_error) VALUES ($1, $2, $3, $4, $5, $6, $7)");
    stmt->Execute(duckdb::Value::UUID(statement_id), duckdb::Value::UUID(session_id),
                  duckdb::Value(sql_text),
                  flight_method.empty() ? duckdb::Value() : duckdb::Value(flight_method),
                  duckdb::Value::BOOLEAN(is_internal),
                  duckdb::Value::BOOLEAN(prepare_success),
                  prepare_error.empty() ? duckdb::Value() : duckdb::Value(prepare_error));
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

  // Convert wall-clock time to DuckDB timestamp (microseconds since epoch)
  auto start_us = std::chrono::duration_cast<std::chrono::microseconds>(
                      start_timestamp_.time_since_epoch())
                      .count();

  manager_->QueueWrite([execution_id, statement_id, bind_parameters,
                        start_us](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sql_executions (execution_id, statement_id, "
        "bind_parameters, execution_start_time) VALUES ($1, $2, $3, make_timestamp($4))");
    stmt->Execute(duckdb::Value::UUID(execution_id), duckdb::Value::UUID(statement_id),
                  bind_parameters.empty() ? duckdb::Value() : duckdb::Value(bind_parameters),
                  duckdb::Value::BIGINT(start_us));
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

void ExecutionInstrumentation::IncrementRowsFetched(int64_t count) {
  rows_fetched_.fetch_add(count, std::memory_order_relaxed);
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

  // Convert wall-clock end time to DuckDB timestamp (microseconds since epoch)
  auto end_us = std::chrono::duration_cast<std::chrono::microseconds>(
                    end_timestamp_.time_since_epoch())
                    .count();

  // Calculate duration from wall-clock times if not explicitly provided
  int64_t final_duration_ms = duration_ms_;
  if (final_duration_ms == 0) {
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_timestamp_ - start_timestamp_);
    final_duration_ms = duration.count();
  }

  manager_->QueueWrite([exec_id = execution_id_, rows = rows_fetched_.load(),
                        status = status_, error_msg = error_message_,
                        end_us, final_duration_ms](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "UPDATE _gizmosql_instr.sql_executions SET execution_end_time = make_timestamp($2), "
        "rows_fetched = $3, status = $4::_gizmosql_instr.execution_status, error_message = $5, "
        "duration_ms = $6 WHERE execution_id = $1");
    stmt->Execute(duckdb::Value::UUID(exec_id), duckdb::Value::BIGINT(end_us),
                  duckdb::Value::BIGINT(rows), duckdb::Value(status),
                  error_msg.empty() ? duckdb::Value() : duckdb::Value(error_msg),
                  duckdb::Value::BIGINT(final_duration_ms));
  });
}

}  // namespace gizmosql::ddb
