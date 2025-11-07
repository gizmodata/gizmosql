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

#include "duckdb_statement.h"

#include <duckdb.h>
#include <duckdb/main/client_context.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <iostream>
#include <future>
#include <chrono>

#include <boost/algorithm/string.hpp>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/util/logging.h>
#include <arrow/c/bridge.h>
#include "duckdb_server.h"
#include "session_context.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using arrow::Status;
using duckdb::QueryResult;

namespace gizmosql::ddb {
std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType duckdb_type) {
  const duckdb::LogicalTypeId column_type_id = duckdb_type.id();
  switch (column_type_id) {
    case duckdb::LogicalTypeId::INTEGER:
      return arrow::int32();
    case duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width = 0;
      uint8_t scale = 0;
      bool dec_properties = duckdb_type.GetDecimalProperties(width, scale);
      return arrow::smallest_decimal(scale, width);
    }
    case duckdb::LogicalTypeId::FLOAT:
      return arrow::float32();
    case duckdb::LogicalTypeId::DOUBLE:
      return arrow::float64();
    case duckdb::LogicalTypeId::CHAR:
    case duckdb::LogicalTypeId::VARCHAR:
      return arrow::utf8();
    case duckdb::LogicalTypeId::BLOB:
      return arrow::binary();
    case duckdb::LogicalTypeId::TINYINT:
      return arrow::int8();
    case duckdb::LogicalTypeId::SMALLINT:
      return arrow::int16();
    case duckdb::LogicalTypeId::BIGINT:
      return arrow::int64();
    case duckdb::LogicalTypeId::BOOLEAN:
      return arrow::boolean();
    case duckdb::LogicalTypeId::DATE:
      return arrow::date32();
    case duckdb::LogicalTypeId::TIME:
    case duckdb::LogicalTypeId::TIMESTAMP_MS:
      return timestamp(arrow::TimeUnit::MILLI);
    case duckdb::LogicalTypeId::TIMESTAMP:
      return timestamp(arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::TIMESTAMP_SEC:
      return timestamp(arrow::TimeUnit::SECOND);
    case duckdb::LogicalTypeId::TIMESTAMP_NS:
      return timestamp(arrow::TimeUnit::NANO);
    case duckdb::LogicalTypeId::INTERVAL:
      return duration(
          arrow::TimeUnit::MICRO);  // ASSUMING MICRO AS DUCKDB's DOCS DOES NOT SPECIFY
    case duckdb::LogicalTypeId::UTINYINT:
      return arrow::uint8();
    case duckdb::LogicalTypeId::USMALLINT:
      return arrow::uint16();
    case duckdb::LogicalTypeId::UINTEGER:
      return arrow::uint32();
    case duckdb::LogicalTypeId::UBIGINT:
      return arrow::int64();
    case duckdb::LogicalTypeId::INVALID:
    case duckdb::LogicalTypeId::SQLNULL:
    case duckdb::LogicalTypeId::UNKNOWN:
    case duckdb::LogicalTypeId::ANY:
    case duckdb::LogicalTypeId::USER:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
    case duckdb::LogicalTypeId::TIME_TZ:
    case duckdb::LogicalTypeId::HUGEINT:
      return arrow::decimal128(38, 0);
    case duckdb::LogicalTypeId::POINTER:
    case duckdb::LogicalTypeId::VALIDITY:
    case duckdb::LogicalTypeId::UUID:
    case duckdb::LogicalTypeId::STRUCT:
    case duckdb::LogicalTypeId::LIST:
    case duckdb::LogicalTypeId::MAP:
    case duckdb::LogicalTypeId::TABLE:
    case duckdb::LogicalTypeId::ENUM:
    default:
      return arrow::null();
  }
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    std::shared_ptr<ClientSession> client_session, const std::string& handle,
    const std::string& sql, const arrow::util::ArrowLogLevel& log_level,
    const bool& log_queries, const int32_t& query_timeout,
    const std::shared_ptr<arrow::Schema>& override_schema) {
  client_session->active_sql_handle = handle;
  auto logged_sql = redact_sql_for_logs(sql);
  if (log_queries) {
    GIZMOSQL_LOGKV_DYNAMIC(
        log_level, "Client is attempting to run a SQL command",
        {"peer", client_session->peer}, {"kind", "sql"}, {"status", "attempt"},
        {"session_id", client_session->session_id}, {"user", client_session->username},
        {"role", client_session->role}, {"statement_handle", handle}, {"sql", logged_sql},
        {"query_timeout", std::to_string(query_timeout)});
  }
  std::shared_ptr<duckdb::PreparedStatement> stmt =
      client_session->connection->Prepare(sql);

  if (not stmt->success) {
    std::string error_message = stmt->error.Message();

    // Check if this is the multiple statements error that can be resolved with direct execution
    if (error_message.find("Cannot prepare multiple statements at once") !=
        std::string::npos) {
      // Fallback to direct query execution for statements like PIVOT that get rewritten to multiple statements
      std::shared_ptr<DuckDBStatement> result(
          new DuckDBStatement(client_session, handle, sql, log_level, log_queries,
                              query_timeout, override_schema));
      return result;
    }

    // Other preparation errors are still fatal
    std::string err_msg =
        "Can't prepare statement: '" + logged_sql + "' - Error: " + error_message;

    if (log_queries) {
      GIZMOSQL_LOGKV(WARNING, "Client SQL command failed preparation",
                     {"peer", client_session->peer}, {"kind", "sql"},
                     {"status", "failure"}, {"session_id", client_session->session_id},
                     {"user", client_session->username}, {"role", client_session->role},
                     {"statement_handle", handle}, {"error", err_msg},
                     {"sql", logged_sql},
                     {"query_timeout", std::to_string(query_timeout)});
    }

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<DuckDBStatement> result(
      new DuckDBStatement(client_session, handle, stmt, log_level, log_queries,
                          query_timeout, override_schema));

  return result;
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    std::shared_ptr<ClientSession> client_session, const std::string& sql,
    const arrow::util::ArrowLogLevel& log_level, const bool& log_queries,
    const int32_t& query_timeout, const std::shared_ptr<arrow::Schema>& override_schema) {
  std::string handle = boost::uuids::to_string(boost::uuids::random_generator()());
  return DuckDBStatement::Create(client_session, handle, sql, log_level, log_queries,
                                 query_timeout, override_schema);
}

DuckDBStatement::~DuckDBStatement() {}

arrow::Result<int> DuckDBStatement::Execute() {
  std::string logged_sql;

  // Launch execution in a separate thread
  auto future =
      std::async(std::launch::async, [this, &logged_sql]() -> arrow::Result<int> {
        if (use_direct_execution_) {
          logged_sql = redact_sql_for_logs(sql_);

          if (!bind_parameters.empty()) {
            client_session_->active_sql_handle = "";
            return arrow::Status::Invalid(
                "Direct query execution does not support bind parameters");
          }

          auto result = client_session_->connection->Query(sql_);
          client_session_->active_sql_handle = "";

          if (result->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV(
                  WARNING, "Client SQL command failed direct execution",
                  {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "failure"},
                  {"session_id", client_session_->session_id},
                  {"user", client_session_->username}, {"role", client_session_->role},
                  {"statement_handle", handle_}, {"error", result->GetError()},
                  {"sql", logged_sql}, {"query_timeout", std::to_string(query_timeout_)});
            }
            return arrow::Status::ExecutionError("Direct query execution error: ",
                                                 result->GetError());
          }

          query_result_ = std::move(result);
        } else {
          logged_sql = redact_sql_for_logs(stmt_->query);

          if (log_queries_ && !bind_parameters.empty()) {
            std::stringstream params_str;
            params_str << "[";
            for (size_t i = 0; i < bind_parameters.size(); i++) {
              if (i > 0) params_str << ", ";
              params_str << "'" << bind_parameters[i].ToString() << "'";
            }
            params_str << "]";

            GIZMOSQL_LOGKV_DYNAMIC(
                log_level_, "Executing prepared statement with bind parameters",
                {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "executing"},
                {"session_id", client_session_->session_id},
                {"user", client_session_->username}, {"role", client_session_->role},
                {"statement_handle", handle_}, {"bind_parameters", params_str.str()},
                {"param_count", std::to_string(bind_parameters.size())},
                {"query_timeout", std::to_string(query_timeout_)});
          }

          query_result_ = stmt_->Execute(bind_parameters);
          client_session_->active_sql_handle = "";

          if (query_result_->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV(
                  WARNING, "Client SQL command failed execution",
                  {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "failure"},
                  {"session_id", client_session_->session_id},
                  {"user", client_session_->username}, {"role", client_session_->role},
                  {"statement_handle", handle_}, {"error", query_result_->GetError()},
                  {"sql", logged_sql}, {"query_timeout", std::to_string(query_timeout_)});
            }
            return arrow::Status::ExecutionError("An execution error has occurred: ",
                                                 query_result_->GetError());
          }
        }

        return 0;  // Success
      });

  std::future_status status;
  // Define timeout duration
  auto timeout_duration = std::chrono::seconds(query_timeout_);

  if (query_timeout_ == 0) {
    future.wait();  // Blocks until ready
    status = std::future_status::ready;
  } else {
    status = future.wait_for(timeout_duration);
  }

  if (status == std::future_status::timeout) {
    // Timeout occurred - interrupt the query
    client_session_->connection->Interrupt();
    client_session_->active_sql_handle = "";

    if (log_queries_) {
      GIZMOSQL_LOGKV(
          WARNING, "Client SQL command timed out", {"peer", client_session_->peer},
          {"kind", "sql"}, {"status", "timeout"},
          {"session_id", client_session_->session_id},
          {"user", client_session_->username}, {"role", client_session_->role},
          {"statement_handle", handle_},
          {"timeout_seconds", std::to_string(query_timeout_)},
          {"sql", redact_sql_for_logs(use_direct_execution_ ? sql_ : stmt_->query)});
    }

    return arrow::Status::ExecutionError("Query execution timed out after ",
                                         std::to_string(timeout_duration.count()),
                                         " seconds");
  }

  // Get the result from the future
  auto result = future.get();

  end_time_ = std::chrono::steady_clock::now();
  if (log_queries_ && result.ok()) {
    GIZMOSQL_LOGKV_DYNAMIC(
        log_level_, "Client SQL command execution succeeded",
        {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "success"},
        {"session_id", client_session_->session_id}, {"user", client_session_->username},
        {"role", client_session_->role}, {"statement_handle", handle_},
        {"direct_execution", use_direct_execution_ ? "true" : "false"},
        {"duration_ms", GetLastExecutionDurationMs()},
        {"sql", redact_sql_for_logs(use_direct_execution_ ? sql_ : stmt_->query)});
  }

  return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DuckDBStatement::FetchResult() {
  std::shared_ptr<arrow::RecordBatch> record_batch;
  ArrowArray res_arr;
  ArrowSchema res_schema;

  // Get client context - handle both prepared statement and direct execution modes
  duckdb::shared_ptr<duckdb::ClientContext> client_context;
  if (use_direct_execution_) {
    // For direct execution, get context from the connection
    client_context = client_session_->connection->context;
  } else {
    // For prepared statements, get context from the statement
    client_context = stmt_->context;
  }

  auto res_options = client_context->GetClientProperties();
  res_options.time_zone = query_result_->client_properties.time_zone;

  if (override_schema_) {
    ARROW_RETURN_NOT_OK(arrow::ExportSchema(*override_schema_, &res_schema));
  } else {
    duckdb::ArrowConverter::ToArrowSchema(&res_schema, query_result_->types,
                                          query_result_->names, res_options);
  }

  duckdb::unique_ptr<duckdb::DataChunk> data_chunk;
  duckdb::ErrorData fetch_error;
  auto fetch_success = query_result_->TryFetch(data_chunk, fetch_error);
  if (!fetch_success) {
    return arrow::Status::ExecutionError(fetch_error.Message());
  }

  if (data_chunk != nullptr) {
    duckdb::unordered_map<idx_t, const duckdb::shared_ptr<duckdb::ArrowTypeExtensionData>>
        extension_type_cast;
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &res_arr, res_options,
                                         extension_type_cast);
    ARROW_ASSIGN_OR_RAISE(record_batch, arrow::ImportRecordBatch(&res_arr, &res_schema));

    GIZMOSQL_LOGKV(
        DEBUG, "Client RecordBatch Fetch", {"peer", client_session_->peer},
        {"kind", "fetch"}, {"status", "success"},
        {"session_id", client_session_->session_id}, {"user", client_session_->username},
        {"role", client_session_->role}, {"statement_handle", handle_},
        {"num_rows", std::to_string(record_batch->num_rows())},
        {"num_columns", std::to_string(record_batch->num_columns())},
        {"sql", redact_sql_for_logs(use_direct_execution_ ? sql_ : stmt_->query)});
  }

  return record_batch;
}

std::shared_ptr<duckdb::PreparedStatement> DuckDBStatement::GetDuckDBStmt() const {
  if (use_direct_execution_) {
    // Direct execution mode doesn't have a prepared statement
    return nullptr;
  }
  return stmt_;
}

arrow::Result<int64_t> DuckDBStatement::ExecuteUpdate() {
  ARROW_RETURN_NOT_OK(Execute());
  ARROW_ASSIGN_OR_RAISE(auto result_batch, FetchResult());

  if (!result_batch) {
    return 0;
  }

  // For DML statements, DuckDB returns a single BIGINT value with the number of
  // affected rows. This is represented as a RecordBatch with one row and one
  // column.
  if (result_batch->num_rows() == 1 && result_batch->num_columns() == 1 &&
      result_batch->column(0)->type_id() == arrow::Type::INT64) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, result_batch->column(0)->GetScalar(0));
    if (scalar->is_valid) {
      return std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
    }
  }

  // Fallback to previous behavior for other cases.
  return result_batch->num_rows();
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::GetSchema() const {
  if (override_schema_) {
    return override_schema_;
  }

  if (use_direct_execution_) {
    // For direct execution, we need to execute the query to get schema information
    // This is a temporary execution just to get the schema
    auto temp_result = client_session_->connection->Query(sql_);
    if (temp_result->HasError()) {
      return arrow::Status::ExecutionError("Failed to get schema for direct query: ",
                                           temp_result->GetError());
    }

    auto& context = client_session_->connection->context;
    auto client_properties = context->GetClientProperties();

    ArrowSchema arrow_schema;
    duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, temp_result->types,
                                          temp_result->names, client_properties);

    auto return_value = arrow::ImportSchema(&arrow_schema);
    return return_value;
  } else {
    // Traditional prepared statement schema retrieval
    auto names = stmt_->GetNames();
    auto types = stmt_->GetTypes();

    auto& context = stmt_->context;
    auto client_properties = context->GetClientProperties();

    ArrowSchema arrow_schema;
    duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);

    auto return_value = arrow::ImportSchema(&arrow_schema);

    return return_value;
  }
}

long DuckDBStatement::GetLastExecutionDurationMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_)
      .count();
}
}  // namespace gizmosql::ddb