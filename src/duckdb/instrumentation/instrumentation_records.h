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

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <string>

namespace gizmosql::ddb {

class InstrumentationManager;

class InstanceInstrumentation {
 public:
  InstanceInstrumentation(std::shared_ptr<InstrumentationManager> manager,
                          const std::string& gizmosql_version,
                          const std::string& duckdb_version,
                          const std::string& hostname, int port,
                          const std::string& database_path);

  ~InstanceInstrumentation();

  std::string GetInstanceId() const { return instance_id_; }

  void SetStopReason(const std::string& reason);

 private:
  std::shared_ptr<InstrumentationManager> manager_;
  std::string instance_id_;
  std::string stop_reason_{"graceful"};
};

class SessionInstrumentation {
 public:
  SessionInstrumentation(std::shared_ptr<InstrumentationManager> manager,
                         const std::string& instance_id,
                         const std::string& session_id, const std::string& username,
                         const std::string& role, const std::string& peer);

  ~SessionInstrumentation();

  std::string GetSessionId() const { return session_id_; }

  void SetStopReason(const std::string& reason);

 private:
  std::shared_ptr<InstrumentationManager> manager_;
  std::string instance_id_;
  std::string session_id_;
  std::string stop_reason_{"closed"};
};

/// Tracks prepared statement creation (once per statement)
class StatementInstrumentation {
 public:
  /// Creates a statement instrumentation record.
  /// @param manager The instrumentation manager
  /// @param statement_id The unique ID for this statement (used in logging and DB)
  /// @param session_id The session this statement belongs to
  /// @param sql_text The SQL text of the statement
  StatementInstrumentation(std::shared_ptr<InstrumentationManager> manager,
                           const std::string& statement_id,
                           const std::string& session_id,
                           const std::string& sql_text);

  std::string GetStatementId() const { return statement_id_; }

 private:
  std::shared_ptr<InstrumentationManager> manager_;
  std::string statement_id_;
};

/// Tracks individual executions of a statement (once per Execute call)
class ExecutionInstrumentation {
 public:
  /// Creates an execution instrumentation record.
  /// @param manager The instrumentation manager
  /// @param execution_id The unique ID for this execution (used in logging and DB)
  /// @param statement_id The statement being executed
  /// @param bind_parameters JSON-formatted bind parameters (empty string if none)
  ExecutionInstrumentation(std::shared_ptr<InstrumentationManager> manager,
                           const std::string& execution_id,
                           const std::string& statement_id,
                           const std::string& bind_parameters);

  ~ExecutionInstrumentation();

  std::string GetExecutionId() const { return execution_id_; }

  void SetCompleted(int64_t rows_fetched, int64_t duration_ms);
  void SetError(const std::string& error_message);
  void SetTimeout();
  void SetCancelled();
  void IncrementRowsFetched(int64_t count);

 private:
  void Finalize();

  std::shared_ptr<InstrumentationManager> manager_;
  std::string execution_id_;
  std::string statement_id_;
  std::atomic<int64_t> rows_fetched_{0};
  std::string status_{"executing"};
  std::string error_message_;
  int64_t duration_ms_{0};
  // Wall-clock timestamps captured synchronously (not affected by async queue delays)
  std::chrono::system_clock::time_point start_timestamp_;
  std::chrono::system_clock::time_point end_timestamp_;
  bool finalized_{false};
};

}  // namespace gizmosql::ddb
