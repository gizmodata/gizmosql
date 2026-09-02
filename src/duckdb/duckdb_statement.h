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
#include <mutex>
#include <string>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/type_fwd.h>

#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"
#include "session_context.h"
#include <chrono>
#include <arrow/record_batch.h>

using Clock = std::chrono::steady_clock;

namespace arrow::flight {
class ServerCallContext;
}

namespace gizmosql::ddb {

/// Returns true if a catalog named `catalog_name` is attached on `connection`.
/// Answered from duckdb_databases() so it never opens connections to remote
/// catalog metadata stores (see the definition for why this matters).
bool CatalogExistsOnConnection(duckdb::Connection& connection,
                               const std::string& catalog_name);

#ifdef GIZMOSQL_ENTERPRISE
class StatementInstrumentation;
class ExecutionInstrumentation;
#endif

/// \brief True when DuckDB could not resolve the type at prepare time — an
/// untyped placeholder such as `SELECT ? AS x`. DuckDB resolves such types
/// at execute time from the bound values.
bool IsUnresolvedDuckDBType(const duckdb::LogicalType& type);

std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType& duckdb_type);

/// \brief Create an object ColumnMetadata using the column type and
///        table name.
/// \param column_type  The DuckDB type.
/// \param table        The table name.
/// \return             A Column Metadata object.
flight::sql::ColumnMetadata GetColumnMetadata(int column_type, const char* table);

class DuckDBStatement {
 public:
  static arrow::Result<std::shared_ptr<DuckDBStatement>> Create(
      const std::shared_ptr<ClientSession>& client_session, const std::string& handle,
      const std::string& sql,
      const std::optional<arrow::util::ArrowLogLevel>& log_level = std::nullopt,
      const bool& log_queries = false,
      const std::shared_ptr<arrow::Schema>& override_schema = nullptr,
      const std::string& flight_method = "",
      bool is_internal = false);

  // Convenience method to generate a handle for the caller
  static arrow::Result<std::shared_ptr<DuckDBStatement>> Create(
      const std::shared_ptr<ClientSession>& client_session, const std::string& sql,
      const std::optional<arrow::util::ArrowLogLevel>& log_level = std::nullopt,
      const bool& log_queries = false,
      const std::shared_ptr<arrow::Schema>& override_schema = nullptr,
      const std::string& flight_method = "",
      bool is_internal = false);

  ~DuckDBStatement();

  /// \brief Creates an Arrow Schema based on the results of this statement.
  /// \return              The resulting Schema.
  arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema();

  /// \brief True when the prepare-time result schema contains placeholder types
  /// (untyped parameters such as `SELECT ? AS x`). GetSchema() then reports
  /// VARCHAR for those columns until Execute() has run with bound parameters,
  /// after which it returns the real schema of the result.
  bool HasUnresolvedSchema() const { return schema_unresolved_; }

  arrow::Result<int> Execute();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchResult();

  /// \brief Attach the Flight call that is driving the next Execute().
  ///
  /// While the statement runs, the execute-wait loop polls the call's
  /// is_cancelled() and interrupts DuckDB if the client goes away (process
  /// killed, connection dropped, DoGet cancelled, client deadline hit) —
  /// otherwise such a query would run to completion or the query timeout.
  /// The pointer is only dereferenced inside Execute() and is cleared when
  /// Execute() returns, so it must outlive that call (Flight keeps the
  /// ServerCallContext alive for the whole RPC, including streaming the
  /// FlightDataStream a DoGet handler returns).
  void SetCallContext(const arrow::flight::ServerCallContext* context) {
    call_context_ = context;
  }

  std::shared_ptr<duckdb::PreparedStatement> GetDuckDBStmt() const;

  /// \brief Executes an UPDATE, INSERT or DELETE statement.
  /// \return              The number of rows changed by execution.
  arrow::Result<int64_t> ExecuteUpdate();

  long GetLastExecutionDurationMs() const;

  std::string GetSessionId() const;

  duckdb::vector<duckdb::Value> bind_parameters;

#ifdef GIZMOSQL_ENTERPRISE
  StatementInstrumentation* GetInstrumentation() const { return instrumentation_.get(); }
  ExecutionInstrumentation* GetExecutionInstrumentation() const {
    return execution_instrumentation_.get();
  }
#endif

 private:
#ifdef GIZMOSQL_ENTERPRISE
  std::unique_ptr<StatementInstrumentation> instrumentation_;
  std::unique_ptr<ExecutionInstrumentation> execution_instrumentation_;
  // Resolved query-profile capture mode for the current Execute() call. Set when
  // profiling is enabled on the connection before execution, read when harvesting
  // the profile JSON afterwards.
  gizmosql::QueryProfileMode query_profile_mode_ = gizmosql::QueryProfileMode::kOff;
#endif
  std::weak_ptr<ClientSession> client_session_;
  std::string session_id_;  // cached for use after session expires
  std::string statement_id_;
  std::shared_ptr<duckdb::PreparedStatement> stmt_;
  duckdb::unique_ptr<duckdb::QueryResult> query_result_;
  std::optional<arrow::util::ArrowLogLevel> log_level_;
  std::shared_ptr<arrow::Schema> override_schema_;
  std::chrono::steady_clock::time_point start_time_;
  std::chrono::steady_clock::time_point end_time_;

  // Support for direct query execution (fallback for statements that can't be prepared)
  std::string sql_;  // Original SQL for direct execution
  bool log_queries_;
  std::string logged_sql_;     // Redacted SQL safe for logging
  bool use_direct_execution_;  // Flag to indicate whether to use direct query execution
  bool is_gizmosql_admin_ =
      false;  // Flag to indicate whether the statement is a GizmoSQL administrative command
  bool is_internal_ = false;  // Flag to indicate whether the statement is an internal query
  std::string flight_method_;  // The Flight RPC method that created this statement
  duckdb::shared_ptr<duckdb::ClientContext> client_context_;
  // Flight call driving the current Execute(); see SetCallContext().
  const arrow::flight::ServerCallContext* call_context_ = nullptr;
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  std::string creation_trace_id_;
  std::string creation_span_id_;
#endif
  // Memoized result schema (nullptr until computed). Guarded by schema_mutex_.
  std::shared_ptr<arrow::Schema> cached_schema_;
  std::mutex schema_mutex_;
  // Set when the prepare-time schema had unresolved placeholder types; see
  // HasUnresolvedSchema().
  bool schema_unresolved_ = false;
  std::shared_ptr<arrow::RecordBatch> synthetic_result_batch_;

  DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                  const std::string& handle,
                  const std::shared_ptr<duckdb::PreparedStatement>& stmt,
                  const std::optional<arrow::util::ArrowLogLevel>& log_level,
                  const bool& log_queries,
                  const std::shared_ptr<arrow::Schema>& override_schema,
                  bool is_internal = false,
                  std::string flight_method = "");

  // Constructor for direct execution mode
  DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                  const std::string& handle, const std::string& sql,
                  const std::optional<arrow::util::ArrowLogLevel>& log_level,
                  const bool& log_queries,
                  const std::shared_ptr<arrow::Schema>& override_schema,
                  bool is_internal = false,
                  std::string flight_method = "");

  arrow::Status HandleGizmoSQLSet();

  arrow::Result<std::shared_ptr<arrow::Schema>> ComputeSchema();

  arrow::Result<int32_t> GetQueryTimeout() const;

  arrow::Result<arrow::util::ArrowLogLevel> GetLogLevel() const;

  arrow::Result<std::shared_ptr<ClientSession>> GetSession() const;
};
}  // namespace gizmosql::ddb