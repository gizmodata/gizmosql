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
    std::shared_ptr<InstrumentationManager> manager, const std::string& session_id,
    const std::string& sql_text, const std::string& statement_handle)
    : manager_(std::move(manager)),
      statement_id_(GenerateUUID()),
      session_id_(session_id),
      start_time_(std::chrono::steady_clock::now()) {
  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  manager_->QueueWrite([stmt_id = statement_id_, session_id, sql_text,
                        statement_handle](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "INSERT INTO _gizmosql_instr.sql_statements (statement_id, session_id, sql_text, "
        "statement_handle, execution_start_time) VALUES ($1, $2, $3, $4, now())");
    stmt->Execute(duckdb::Value::UUID(stmt_id), duckdb::Value::UUID(session_id),
                  duckdb::Value(sql_text), duckdb::Value(statement_handle));
  });
}

StatementInstrumentation::~StatementInstrumentation() {
  if (!finalized_) {
    Finalize();
  }
}

void StatementInstrumentation::SetCompleted(int64_t rows_fetched, int64_t duration_ms) {
  rows_fetched_ = rows_fetched;
  duration_ms_ = duration_ms;
  status_ = "success";
  Finalize();
}

void StatementInstrumentation::SetError(const std::string& error_message) {
  error_message_ = error_message;
  status_ = "error";
  Finalize();
}

void StatementInstrumentation::SetTimeout() {
  status_ = "timeout";
  Finalize();
}

void StatementInstrumentation::SetCancelled() {
  status_ = "cancelled";
  Finalize();
}

void StatementInstrumentation::IncrementRowsFetched(int64_t count) {
  rows_fetched_.fetch_add(count, std::memory_order_relaxed);
}

void StatementInstrumentation::Finalize() {
  if (finalized_) {
    return;
  }
  finalized_ = true;

  if (!manager_ || !manager_->IsEnabled()) {
    return;
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
  int64_t final_duration_ms = duration_ms_ > 0 ? duration_ms_ : duration.count();

  manager_->QueueWrite([stmt_id = statement_id_, rows = rows_fetched_.load(),
                        status = status_, error_msg = error_message_,
                        final_duration_ms](duckdb::Connection& conn) {
    auto stmt = conn.Prepare(
        "UPDATE _gizmosql_instr.sql_statements SET execution_end_time = now(), rows_fetched = $2, "
        "status = $3::_gizmosql_instr.sql_statement_status, error_message = $4, duration_ms = $5 WHERE statement_id = $1");
    stmt->Execute(duckdb::Value::UUID(stmt_id), duckdb::Value::BIGINT(rows),
                  duckdb::Value(status),
                  error_msg.empty() ? duckdb::Value() : duckdb::Value(error_msg),
                  duckdb::Value::BIGINT(final_duration_ms));
  });
}

}  // namespace gizmosql::ddb
