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

class StatementInstrumentation {
 public:
  StatementInstrumentation(std::shared_ptr<InstrumentationManager> manager,
                           const std::string& session_id, const std::string& sql_text,
                           const std::string& statement_handle);

  ~StatementInstrumentation();

  std::string GetStatementId() const { return statement_id_; }

  void SetCompleted(int64_t rows_fetched, int64_t duration_ms);
  void SetError(const std::string& error_message);
  void SetTimeout();
  void SetCancelled();
  void IncrementRowsFetched(int64_t count);

 private:
  void Finalize();

  std::shared_ptr<InstrumentationManager> manager_;
  std::string statement_id_;
  std::string session_id_;
  std::atomic<int64_t> rows_fetched_{0};
  std::string status_{"executing"};
  std::string error_message_;
  int64_t duration_ms_{0};
  std::chrono::steady_clock::time_point start_time_;
  bool finalized_{false};
};

}  // namespace gizmosql::ddb
