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

#include <memory>
#include <string>
#include <vector>

#include <arrow/flight/sql/api.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include "client_config.hpp"

namespace gizmosql::client {

arrow::Status ReadPEMFile(const std::string& path, std::string& contents);

class FlightConnection {
 public:
  arrow::Status Connect(const ClientConfig& config);

  arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQuery(const std::string& sql);
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

  bool IsConnected() const { return client_ != nullptr; }

 private:
  arrow::Result<std::shared_ptr<arrow::Table>> CollectResults(
      const std::unique_ptr<arrow::flight::FlightInfo>& info);

  std::unique_ptr<arrow::flight::sql::FlightSqlClient> client_;
  arrow::flight::FlightCallOptions call_options_;
};

}  // namespace gizmosql::client
