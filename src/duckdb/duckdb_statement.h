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

#include <duckdb.hpp>

#include <memory>
#include <string>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/type_fwd.h>

#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"
#include "session_context.h"
#include <chrono>

using Clock = std::chrono::steady_clock;

namespace gizmosql::ddb {
std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType duckdb_type);

/// \brief Create an object ColumnMetadata using the column type and
///        table name.
/// \param column_type  The DuckDB type.
/// \param table        The table name.
/// \return             A Column Metadata object.
flight::sql::ColumnMetadata GetColumnMetadata(int column_type, const char* table);

class DuckDBStatement {
 public:
  static arrow::Result<std::shared_ptr<DuckDBStatement>> Create(
      std::shared_ptr<ClientSession> client_session, const std::string& handle,
      const std::string& sql,
      const arrow::util::ArrowLogLevel& log_level =
          arrow::util::ArrowLogLevel::ARROW_INFO,
      const bool& log_queries = false, const int32_t& query_timeout = 0,
      const std::shared_ptr<arrow::Schema>& override_schema = nullptr);

  // Convenience method to generate a handle for the caller
  static arrow::Result<std::shared_ptr<DuckDBStatement>> Create(
      std::shared_ptr<ClientSession> client_session, const std::string& sql,
      const arrow::util::ArrowLogLevel& log_level =
          arrow::util::ArrowLogLevel::ARROW_INFO,
      const bool& log_queries = false, const int32_t& query_timeout = 0,
      const std::shared_ptr<arrow::Schema>& override_schema = nullptr);

  ~DuckDBStatement();

  /// \brief Creates an Arrow Schema based on the results of this statement.
  /// \return              The resulting Schema.
  arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const;

  arrow::Result<int> Execute();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchResult();
  // arrow::Result<std::shared_ptr<Schema>> GetArrowSchema();

  std::shared_ptr<duckdb::PreparedStatement> GetDuckDBStmt() const;

  /// \brief Executes an UPDATE, INSERT or DELETE statement.
  /// \return              The number of rows changed by execution.
  arrow::Result<int64_t> ExecuteUpdate();

  long GetLastExecutionDurationMs() const;

  duckdb::vector<duckdb::Value> bind_parameters;

 private:
  std::shared_ptr<ClientSession> client_session_;
  std::string handle_;
  std::shared_ptr<duckdb::PreparedStatement> stmt_;
  duckdb::unique_ptr<duckdb::QueryResult> query_result_;
  arrow::util::ArrowLogLevel log_level_;
  bool log_queries_;
  int32_t query_timeout_;
  std::shared_ptr<arrow::Schema> override_schema_;
  std::chrono::steady_clock::time_point start_time_;
  std::chrono::steady_clock::time_point end_time_;

  // Support for direct query execution (fallback for statements that can't be prepared)
  std::string sql_;            // Original SQL for direct execution
  bool use_direct_execution_;  // Flag to indicate whether to use direct query execution

  DuckDBStatement(std::shared_ptr<ClientSession> client_session,
                  const std::string& handle,
                  std::shared_ptr<duckdb::PreparedStatement> stmt,
                  const arrow::util::ArrowLogLevel& log_level, const bool& log_queries,
                  const int32_t& query_timeout,
                  std::shared_ptr<arrow::Schema> override_schema) {
    client_session_ = client_session;
    handle_ = handle;
    stmt_ = stmt;
    use_direct_execution_ = false;
    log_level_ = log_level;
    log_queries_ = log_queries;
    start_time_ = std::chrono::steady_clock::now();
    query_timeout_ = query_timeout;
    override_schema_ = override_schema;
  }

  // Constructor for direct execution mode
  DuckDBStatement(std::shared_ptr<ClientSession> client_session,
                  const std::string& handle, const std::string& sql,
                  const arrow::util::ArrowLogLevel& log_level, const bool& log_queries,
                  const int32_t& query_timeout,
                  std::shared_ptr<arrow::Schema> override_schema) {
    client_session_ = client_session;
    handle_ = handle;
    sql_ = sql;
    use_direct_execution_ = true;
    stmt_ = nullptr;
    log_level_ = log_level;
    log_queries_ = log_queries;
    start_time_ = std::chrono::steady_clock::now();
    query_timeout_ = query_timeout;
    override_schema_ = override_schema;
  }
};
}  // namespace gizmosql::ddb