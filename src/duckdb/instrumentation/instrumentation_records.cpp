// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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
    std::shared_ptr<InstrumentationManager> manager, const std::string& gizmosql_version,
    const std::string& duckdb_version, const std::string& hostname, int port,
    const std::string& database_path)
    : manager_(std::move(manager)), instance_id_(GenerateUUID()) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([id = instance_id_, gizmosql_version, duckdb_version, hostname,
                        port, database_path](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.instances (instance_id, gizmosql_version, duckdb_version, "
        "hostname, port, database_path) VALUES ($1, $2, $3, $4, $5, $6)");
    stmt->Execute(duckdb::Value::UUID(id), duckdb::Value(gizmosql_version),
                  duckdb::Value(duckdb_version), duckdb::Value(hostname),
                  duckdb::Value(port), duckdb::Value(database_path));
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
    const std::string& peer)
    : manager_(std::move(manager)), instance_id_(instance_id), session_id_(session_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([instance_id, session_id, username, role,
                        peer](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sessions (session_id, instance_id, username, role, peer) "
        "VALUES ($1, $2, $3, $4, $5)");
    stmt->Execute(duckdb::Value::UUID(session_id), duckdb::Value::UUID(instance_id),
                  duckdb::Value(username), duckdb::Value(role), duckdb::Value(peer));
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
    const std::string& session_id, const std::string& sql_text)
    : manager_(std::move(manager)), statement_id_(statement_id) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([statement_id, session_id, sql_text](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sql_statements (statement_id, session_id, sql_text) "
        "VALUES ($1, $2, $3)");
    stmt->Execute(duckdb::Value::UUID(statement_id), duckdb::Value::UUID(session_id),
                  duckdb::Value(sql_text));
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

void ExecutionInstrumentation::SetCompleted(int64_t rows_fetched, int64_t duration_ms) {
  end_timestamp_ = std::chrono::system_clock::now();
  rows_fetched_ = rows_fetched;
  duration_ms_ = duration_ms;
  status_ = "success";
  Finalize();
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
