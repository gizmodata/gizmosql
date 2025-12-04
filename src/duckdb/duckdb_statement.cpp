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
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/common/enums/set_scope.hpp>
#include "duckdb/parser/expression/constant_expression.hpp"
#include <future>
#include <chrono>

#include <boost/algorithm/string.hpp>

#include <arrow/api.h>
#include <arrow/util/logging.h>
#include <arrow/c/bridge.h>
#include "duckdb_server.h"
#include "session_context.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using arrow::Status;
using duckdb::QueryResult;

namespace {

bool IsLikelyGizmoSQLSet(const std::string& sql) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  std::string upper = boost::to_upper_copy(trimmed);
  if (upper.rfind("SET ", 0) != 0 && upper.rfind("SET\t", 0) != 0 &&
      upper.rfind("SET\n", 0) != 0) {
    return false;
  }
  return upper.find("GIZMOSQL.") != std::string::npos;
}

}  // namespace

namespace gizmosql::ddb {
std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case duckdb::LogicalTypeId::INTEGER:
      return arrow::int32();
    case duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width = 0;
      uint8_t scale = 0;
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
    const std::shared_ptr<ClientSession>& client_session, const std::string& handle,
    const std::string& sql, const arrow::util::ArrowLogLevel& log_level,
    const bool& log_queries, const int32_t& query_timeout,
    const std::shared_ptr<arrow::Schema>& override_schema) {
  std::string status;
  auto logged_sql = redact_sql_for_logs(sql);

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Create", status, {"peer", client_session->peer},
      {"session_id", client_session->session_id}, {"user", client_session->username},
      {"role", client_session->role}, {"statement_handle", handle},
      {"timeout_seconds", std::to_string(query_timeout)});

  client_session->active_sql_handle = handle;
  if (log_queries) {
    GIZMOSQL_LOGKV_DYNAMIC(
        log_level, "Client is attempting to run a SQL command",
        {"peer", client_session->peer}, {"kind", "sql"}, {"status", "attempt"},
        {"session_id", client_session->session_id}, {"user", client_session->username},
        {"role", client_session->role}, {"statement_handle", handle}, {"sql", logged_sql},
        {"query_timeout", std::to_string(query_timeout)});
  }

  if (IsLikelyGizmoSQLSet(sql)) {
    std::shared_ptr<DuckDBStatement> result(
        new DuckDBStatement(client_session, handle, sql, log_level, log_queries,
                            query_timeout, override_schema));
    result->is_gizmosql_admin_ = true;
    if (log_queries) {
      GIZMOSQL_LOGKV_DYNAMIC(
          log_level, "Detected GizmoSQL admin SET command",
          {"peer", client_session->peer}, {"kind", "sql"}, {"status", "admin"},
          {"session_id", client_session->session_id}, {"user", client_session->username},
          {"role", client_session->role}, {"statement_handle", handle},
          {"sql", logged_sql});
    }
    return result;
  }

  std::shared_ptr<duckdb::PreparedStatement> stmt =
      client_session->connection->Prepare(sql);

  if (not stmt->success) {
    std::string error_message = stmt->error.Message();

    // Check if this is the multiple statements error that can be resolved with direct execution
    if (error_message.find("Cannot prepare multiple statements at once") !=
        std::string::npos) {
      // Fallback to direct query execution for statements like PIVOT that get rewritten to multiple statements
      if (log_queries) {
        GIZMOSQL_LOGKV_DYNAMIC(
            log_level,
            "SQL command cannot run as a prepared statement, falling back to direct "
            "query execution",
            {"peer", client_session->peer}, {"kind", "sql"}, {"status", "fallback"},
            {"session_id", client_session->session_id},
            {"user", client_session->username}, {"role", client_session->role},
            {"statement_handle", handle}, {"sql", logged_sql},
            {"query_timeout", std::to_string(query_timeout)});
      }

      std::shared_ptr<DuckDBStatement> result(
          new DuckDBStatement(client_session, handle, sql, log_level, log_queries,
                              query_timeout, override_schema));
      status = "success";
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

  status = "success";
  return result;
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& sql,
    const arrow::util::ArrowLogLevel& log_level, const bool& log_queries,
    const int32_t& query_timeout, const std::shared_ptr<arrow::Schema>& override_schema) {
  std::string handle = boost::uuids::to_string(boost::uuids::random_generator()());
  return DuckDBStatement::Create(client_session, handle, sql, log_level, log_queries,
                                 query_timeout, override_schema);
}

arrow::Status DuckDBStatement::HandleGizmoSQLSet() {
  duckdb::Parser parser;
  try {
    parser.ParseQuery(sql_);
  } catch (const std::exception& ex) {
    return arrow::Status::Invalid("Failed to parse GizmoSQL SET command: " + sql_ +
                                  " - " + ex.what());
  }

  if (parser.statements.empty() ||
      parser.statements[0]->type != duckdb::StatementType::SET_STATEMENT) {
    return arrow::Status::Invalid("Expected SET statement: " + sql_);
  }

  auto& set_stmt = (duckdb::SetVariableStatement&)*parser.statements[0];

  const std::string& name = set_stmt.name;
  auto scope = set_stmt.scope;

  auto* const_expr = dynamic_cast<duckdb::ConstantExpression*>(set_stmt.value.get());
  if (!const_expr) return Status::Invalid("SET value is not a constant");

  auto val = const_expr->value.ToString();  // duckdb::Value

  if (!boost::istarts_with(name, "gizmosql.")) {
    return arrow::Status::Invalid("Unsupported GizmoSQL parameter: " + name);
  }

  if (name == "gizmosql.query_timeout") {
    int timeout_seconds = 0;
    try {
      timeout_seconds = std::stoi(val);
    } catch (...) {
      return arrow::Status::Invalid("Invalid value for query_timeout: " + val);
    }

    if (scope == duckdb::SetScope::SESSION) {
      client_session_->query_timeout = timeout_seconds;
    } else if (scope == duckdb::SetScope::GLOBAL) {
      if (auto server = GetServer(*client_session_)) {
        ARROW_RETURN_NOT_OK(server->SetQueryTimeout(client_session_, timeout_seconds));
      }
    }

    std::string scope_str = (scope == duckdb::SetScope::GLOBAL) ? "global" : "session";
    std::string msg =
        "GizmoSQL " + scope_str + " parameter '" + name + "' successfully set to: " + val;

    auto schema = arrow::schema({arrow::field("result", arrow::utf8())});
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Append(msg));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    synthetic_result_batch_ = arrow::RecordBatch::Make(
        schema, 1, std::vector<std::shared_ptr<arrow::Array>>{array});
    return arrow::Status::OK();
  }

  return arrow::Status::Invalid("Unknown GizmoSQL configuration parameter: " + name);
}

DuckDBStatement::~DuckDBStatement() {}

arrow::Result<int> DuckDBStatement::Execute() {
  std::string execute_status;

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Execute", execute_status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_handle", handle_},
      {"timeout_seconds", std::to_string(query_timeout_)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    ARROW_RETURN_NOT_OK(HandleGizmoSQLSet());
    return 0;
  }

  // Launch execution in a separate thread
  auto future = std::async(std::launch::async, [this]() -> arrow::Result<int> {
    if (use_direct_execution_) {
      // The statement may have already been executed from the ComputeSchema() method - if so, just skip execution
      if (query_result_ != nullptr) {
        if (log_queries_) {
          GIZMOSQL_LOGKV_DYNAMIC(
              log_level_,
              "Direct execution of the SQL command has already occurred, skipping "
              "re-execution",
              {"peer", client_session_->peer}, {"kind", "sql"},
              {"status", "already-executed"}, {"session_id", client_session_->session_id},
              {"user", client_session_->username}, {"role", client_session_->role},
              {"statement_handle", handle_},
              {"query_timeout", std::to_string(query_timeout_)});
        }
        return 0;  // Success
      }
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
              {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout_)});
        }
        return arrow::Status::ExecutionError("Direct query execution error: ",
                                             result->GetError());
      }

      query_result_ = std::move(result);
    } else {
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
              {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout_)});
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

    // Now wait for the background thread to finish cleanly
    future.wait();

    client_session_->active_sql_handle = "";

    if (log_queries_) {
      GIZMOSQL_LOGKV(WARNING, "Client SQL command timed out",
                     {"peer", client_session_->peer}, {"kind", "sql"},
                     {"status", "timeout"}, {"session_id", client_session_->session_id},
                     {"user", client_session_->username}, {"role", client_session_->role},
                     {"statement_handle", handle_},
                     {"timeout_seconds", std::to_string(query_timeout_)},
                     {"sql", logged_sql_});
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
        {"duration_ms", GetLastExecutionDurationMs()}, {"sql", logged_sql_});
  }

  execute_status = "success";
  return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DuckDBStatement::FetchResult() {
  std::string status;

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::FetchResult", status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_handle", handle_},
      {"timeout_seconds", std::to_string(query_timeout_)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (synthetic_result_batch_) {
    status = "success";
    auto batch = synthetic_result_batch_;
    synthetic_result_batch_.reset();
    return batch;
  }

  std::shared_ptr<arrow::RecordBatch> record_batch;

  if (!query_result_) {
    // There is nothing to fetch...
    status = "success";
    return record_batch;
  }

  ArrowArray res_arr;
  ArrowSchema res_schema;

  auto res_options = client_context_->GetClientProperties();
  res_options.time_zone = query_result_->client_properties.time_zone;

  ARROW_ASSIGN_OR_RAISE(auto schema, GetSchema());
  ARROW_RETURN_NOT_OK(arrow::ExportSchema(*schema, &res_schema));

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

    GIZMOSQL_LOGKV(DEBUG, "Client RecordBatch Fetch", {"peer", client_session_->peer},
                   {"kind", "fetch"}, {"status", "success"},
                   {"session_id", client_session_->session_id},
                   {"user", client_session_->username}, {"role", client_session_->role},
                   {"statement_handle", handle_},
                   {"num_rows", std::to_string(record_batch->num_rows())},
                   {"num_columns", std::to_string(record_batch->num_columns())},
                   {"sql", logged_sql_});
  }

  status = "success";
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
  std::string status;

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ExecuteUpdate", status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_handle", handle_},
      {"timeout_seconds", std::to_string(query_timeout_)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"}, );

  ARROW_RETURN_NOT_OK(Execute());
  ARROW_ASSIGN_OR_RAISE(auto result_batch, FetchResult());

  if (!result_batch) {
    status = "success";
    return 0;
  }

  // For DML statements, DuckDB returns a single BIGINT value with the number of
  // affected rows. This is represented as a RecordBatch with one row and one
  // column.
  if (result_batch->num_rows() == 1 && result_batch->num_columns() == 1 &&
      result_batch->column(0)->type_id() == arrow::Type::INT64) {
    ARROW_ASSIGN_OR_RAISE(auto scalar, result_batch->column(0)->GetScalar(0));
    if (scalar->is_valid) {
      status = "success";
      return std::static_pointer_cast<arrow::Int64Scalar>(scalar)->value;
    }
  }

  // Fallback to previous behavior for other cases.
  status = "success";
  return result_batch->num_rows();
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::GetSchema() {
  std::string status;

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::GetSchema", status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_handle", handle_},
      {"timeout_seconds", std::to_string(query_timeout_)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  // If there is an override schema - just return it and avoid computation...
  if (override_schema_) {
    status = "success";
    return override_schema_;
  }

  // Lazily compute & memoize schema exactly once
  std::call_once(schema_once_flag_, [this] {
    cached_schema_ = ComputeSchema();  // Store the Result<>
  });

  status = "success";
  return cached_schema_;
}

long DuckDBStatement::GetLastExecutionDurationMs() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_)
      .count();
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::ComputeSchema() {
  std::string status;

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ComputeSchema", status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_handle", handle_},
      {"timeout_seconds", std::to_string(query_timeout_)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    status = "success";
    return arrow::schema({arrow::field("result", arrow::utf8())});
  }

  if (use_direct_execution_) {
    // For direct execution, we need to execute the query to get schema information
    ARROW_RETURN_NOT_OK(Execute());
    auto client_properties = client_context_->GetClientProperties();

    ArrowSchema arrow_schema;
    duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, query_result_->types,
                                          query_result_->names, client_properties);

    auto return_value = arrow::ImportSchema(&arrow_schema);
    status = "success";
    return return_value;
  }

  // Traditional prepared statement schema retrieval
  auto names = stmt_->GetNames();
  auto types = stmt_->GetTypes();

  auto client_properties = client_context_->GetClientProperties();

  ArrowSchema arrow_schema;
  duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);

  auto return_value = arrow::ImportSchema(&arrow_schema);
  status = "success";
  return return_value;
}

}  // namespace gizmosql::ddb