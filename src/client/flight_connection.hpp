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
#include <memory>
#include <string>
#include <vector>

#include <arrow/flight/sql/api.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "client_config.hpp"

namespace gizmosql::client {

struct QueryResult {
  std::shared_ptr<arrow::Table> table;
  int64_t total_rows;  // -1 if unknown
};

arrow::Status ReadPEMFile(const std::string& path, std::string& contents);

class FlightConnection {
 public:
  FlightConnection() = default;
  ~FlightConnection() { Disconnect(); }

  // Moveable but not copyable (std::atomic is not copyable)
  FlightConnection(FlightConnection&& other) noexcept
      : client_(std::move(other.client_)),
        call_options_(std::move(other.call_options_)),
        cancel_client_(std::move(other.cancel_client_)),
        cancel_call_options_(std::move(other.cancel_call_options_)),
        cancel_requested_(other.cancel_requested_.load()) {}
  FlightConnection& operator=(FlightConnection&&) = delete;
  FlightConnection(const FlightConnection&) = delete;
  FlightConnection& operator=(const FlightConnection&) = delete;

  arrow::Status Connect(const ClientConfig& config);

  arrow::Result<QueryResult> ExecuteQuery(const std::string& sql,
                                           int64_t row_limit = 0);
  arrow::Result<int64_t> ExecuteUpdate(const std::string& sql);

  arrow::Result<std::shared_ptr<arrow::Table>> GetTables(
      const std::string& catalog_pattern = "",
      const std::string& schema_pattern = "",
      const std::string& table_pattern = "");
  arrow::Result<std::shared_ptr<arrow::Table>> GetDbSchemas(
      const std::string& catalog_pattern = "",
      const std::string& schema_pattern = "");
  arrow::Result<std::shared_ptr<arrow::Table>> GetCatalogs();

  arrow::Result<std::shared_ptr<arrow::Table>> GetSqlInfo(
      const std::vector<int>& info);

  void Disconnect();

  /// Request cancellation (async-signal-safe — only sets an atomic flag).
  void RequestCancel() { cancel_requested_.store(true); }

  /// Send CancelFlightInfo to the server (called from cancel watcher thread).
  void SendCancelToServer();

  bool IsConnected() const { return client_ != nullptr; }

 private:
  arrow::Result<std::shared_ptr<arrow::Table>> CollectResults(
      const std::unique_ptr<arrow::flight::FlightInfo>& info);

  std::unique_ptr<arrow::flight::sql::FlightSqlClient> client_;
  arrow::flight::FlightCallOptions call_options_;

  // Separate client used exclusively for sending CancelFlightInfo from the
  // cancel watcher thread. Using the main client_ concurrently is not safe.
  std::unique_ptr<arrow::flight::FlightClient> cancel_client_;
  arrow::flight::FlightCallOptions cancel_call_options_;

  std::atomic<bool> cancel_requested_{false};
};

}  // namespace gizmosql::client
