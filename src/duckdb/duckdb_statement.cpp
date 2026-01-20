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
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/common/enums/set_scope.hpp>
#include "duckdb/parser/expression/constant_expression.hpp"
#include <future>
#include <chrono>
#include <regex>

#include <boost/algorithm/string.hpp>

#include <arrow/api.h>
#include <arrow/util/logging.h>
#include <arrow/c/bridge.h>
#include "duckdb_server.h"
#include "session_context.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "instrumentation/instrumentation_manager.h"
#include "instrumentation/instrumentation_records.h"
#include "version.h"

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

bool IsDetachInstrumentationDb(const std::string& sql) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Case-insensitive check for DETACH commands targeting instrumentation DB
  std::string upper = boost::to_upper_copy(trimmed);
  if (upper.find("DETACH") == std::string::npos) {
    return false;
  }
  // Check for instrumentation DB aliases/names
  return upper.find("_GIZMOSQL_INSTR") != std::string::npos ||
         upper.find("GIZMOSQL_INSTRUMENTATION") != std::string::npos;
}

bool IsKillSessionCommand(const std::string& sql, std::string& target_session_id) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Match: KILL SESSION 'uuid' or KILL SESSION "uuid" or KILL SESSION uuid
  std::regex kill_pattern(R"(^\s*KILL\s+SESSION\s+['\"]?([0-9a-fA-F-]+)['\"]?\s*;?\s*$)",
                          std::regex_constants::icase);
  std::smatch match;
  if (std::regex_match(trimmed, match, kill_pattern)) {
    target_session_id = match[1].str();
    return true;
  }
  return false;
}

// Replace GizmoSQL pseudo-functions with actual values:
//   GIZMOSQL_CURRENT_SESSION() -> current session UUID
//   GIZMOSQL_CURRENT_INSTANCE() -> current server instance UUID
//   GIZMOSQL_VERSION() -> GizmoSQL version string
// Only replaces occurrences that are NOT within quoted strings.
// When in a SELECT expression context, adds an alias matching the function name.

// Check if we should add an alias based on context (what precedes and follows).
// Returns true if in a SELECT expression context, false if in a condition context.
bool ShouldAddAlias(const std::string& sql, size_t func_start, size_t func_end) {
  // First check what precedes the function - if preceded by comparison operator, don't alias
  if (func_start > 0) {
    size_t prev_pos = func_start - 1;
    // Skip whitespace backwards
    while (prev_pos > 0 && std::isspace(sql[prev_pos])) {
      prev_pos--;
    }

    if (prev_pos < sql.size()) {
      char prev_char = sql[prev_pos];
      // If preceded by comparison operators, we're in a condition context
      if (prev_char == '=' || prev_char == '<' || prev_char == '>' || prev_char == '!') {
        return false;
      }
      // Check for multi-char operators like !=, <>, <=, >=
      if (prev_pos > 0) {
        std::string two_char = sql.substr(prev_pos - 1, 2);
        if (two_char == "!=" || two_char == "<>" || two_char == "<=" || two_char == ">=") {
          return false;
        }
      }
    }
  }

  // Check what follows the function
  size_t pos = func_end;
  // Skip whitespace
  while (pos < sql.size() && std::isspace(sql[pos])) {
    pos++;
  }

  if (pos >= sql.size()) {
    return true;  // End of string - likely SELECT expression
  }

  char next_char = sql[pos];

  // These characters indicate end of a SELECT expression
  if (next_char == ',' || next_char == ';') {
    return true;
  }

  // Closing paren could be either context - check what follows it
  if (next_char == ')') {
    return true;  // Typically fine to alias inside parens in SELECT
  }

  // Check for user-provided AS alias
  std::string rest = sql.substr(pos);
  std::string upper_rest = boost::to_upper_copy(rest);
  if (upper_rest.rfind("AS ", 0) == 0 || upper_rest.rfind("AS\t", 0) == 0 ||
      upper_rest.rfind("AS\n", 0) == 0 || upper_rest.rfind("AS\r", 0) == 0) {
    return false;  // User providing their own alias
  }

  // Check for SQL keywords that follow SELECT expressions
  static const std::vector<std::string> select_terminators = {
      "FROM ", "FROM\t", "FROM\n", "FROM\r",
      "WHERE ", "WHERE\t", "WHERE\n", "WHERE\r",
      "ORDER ", "ORDER\t", "ORDER\n", "ORDER\r",
      "GROUP ", "GROUP\t", "GROUP\n", "GROUP\r",
      "HAVING ", "HAVING\t", "HAVING\n", "HAVING\r",
      "LIMIT ", "LIMIT\t", "LIMIT\n", "LIMIT\r",
      "UNION ", "UNION\t", "UNION\n", "UNION\r",
      "EXCEPT ", "EXCEPT\t", "EXCEPT\n", "EXCEPT\r",
      "INTERSECT ", "INTERSECT\t", "INTERSECT\n", "INTERSECT\r",
  };

  for (const auto& terminator : select_terminators) {
    if (upper_rest.rfind(terminator, 0) == 0) {
      return true;
    }
  }

  // Check for condition keywords/operators that indicate we're NOT in a SELECT expression
  static const std::vector<std::string> condition_indicators = {
      "AND ", "AND\t", "AND\n", "AND\r",
      "OR ", "OR\t", "OR\n", "OR\r",
      "IN ", "IN\t", "IN\n", "IN\r", "IN(",
      "LIKE ", "LIKE\t", "LIKE\n", "LIKE\r",
      "BETWEEN ", "BETWEEN\t", "BETWEEN\n", "BETWEEN\r",
      "IS ", "IS\t", "IS\n", "IS\r",
  };

  for (const auto& indicator : condition_indicators) {
    if (upper_rest.rfind(indicator, 0) == 0) {
      return false;
    }
  }

  // If followed by comparison operators, we're in a condition
  if (next_char == '=' || next_char == '<' || next_char == '>' || next_char == '!') {
    return false;
  }

  // Default: don't add alias to be safe
  return false;
}

std::string ReplaceGizmoSQLFunctions(const std::string& sql,
                                     const std::string& session_id,
                                     const std::string& instance_id) {
  std::string result;
  result.reserve(sql.size() * 2);  // Extra space for potential aliases

  size_t i = 0;
  while (i < sql.size()) {
    // Skip single-quoted strings
    if (sql[i] == '\'') {
      result += sql[i++];
      while (i < sql.size() && sql[i] != '\'') {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          result += sql[i++];
        }
        if (i < sql.size()) {
          result += sql[i++];
        }
      }
      if (i < sql.size()) {
        result += sql[i++];  // closing quote
      }
      continue;
    }

    // Skip double-quoted strings (identifiers in DuckDB)
    if (sql[i] == '"') {
      result += sql[i++];
      while (i < sql.size() && sql[i] != '"') {
        if (sql[i] == '\\' && i + 1 < sql.size()) {
          result += sql[i++];
        }
        if (i < sql.size()) {
          result += sql[i++];
        }
      }
      if (i < sql.size()) {
        result += sql[i++];  // closing quote
      }
      continue;
    }

    // Check for GIZMOSQL_CURRENT_SESSION() at this position
    constexpr size_t kSessionFuncLen = 26;  // length of "GIZMOSQL_CURRENT_SESSION()"
    if (i + kSessionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kSessionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_SESSION()") {
        result += '\'';
        result += session_id;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kSessionFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_SESSION()\"";
        }
        i += kSessionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_CURRENT_INSTANCE() at this position
    constexpr size_t kInstanceFuncLen = 27;  // length of "GIZMOSQL_CURRENT_INSTANCE()"
    if (i + kInstanceFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstanceFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_INSTANCE()") {
        if (instance_id.empty()) {
          result += "NULL";
        } else {
          result += '\'';
          result += instance_id;
          result += '\'';
        }
        if (ShouldAddAlias(sql, i, i + kInstanceFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_INSTANCE()\"";
        }
        i += kInstanceFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_VERSION() at this position
    constexpr size_t kVersionFuncLen = 18;  // length of "GIZMOSQL_VERSION()"
    if (i + kVersionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kVersionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_VERSION()") {
        result += '\'';
        result += PROJECT_VERSION;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kVersionFuncLen)) {
          result += " AS \"GIZMOSQL_VERSION()\"";
        }
        i += kVersionFuncLen;
        continue;
      }
    }

    result += sql[i++];
  }

  return result;
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

arrow::Result<arrow::util::ArrowLogLevel> GetSessionOrServerLogLevel(
    std::shared_ptr<ClientSession> client_session) {
  // Fall-back to the session query log level if the statement log level is not set...
  if (client_session->query_log_level.has_value()) {
    return client_session->query_log_level.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*client_session)) {
    return server->GetQueryLogLevel(client_session);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& handle,
    const std::string& sql, const std::optional<arrow::util::ArrowLogLevel>& log_level,
    const bool& log_queries, const std::shared_ptr<arrow::Schema>& override_schema,
    const std::string& flight_method, bool is_internal) {
  std::string status;
  auto logged_sql = redact_sql_for_logs(sql);
  arrow::util::ArrowLogLevel effective_log_level;
  if (!log_level.has_value()) {
    ARROW_ASSIGN_OR_RAISE(effective_log_level,
                          GetSessionOrServerLogLevel(client_session));
  } else {
    effective_log_level = log_level.value();
  }

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Create", status, {"peer", client_session->peer},
      {"session_id", client_session->session_id}, {"user", client_session->username},
      {"role", client_session->role}, {"statement_id", handle});

  client_session->active_sql_handle = handle;

  // Get instance_id from server for GIZMOSQL_CURRENT_INSTANCE()
  std::string instance_id;
  if (auto server = GetServer(*client_session)) {
    instance_id = server->GetInstanceId();
  }

  // Replace GIZMOSQL_CURRENT_SESSION() and GIZMOSQL_CURRENT_INSTANCE() with actual values
  std::string effective_sql =
      ReplaceGizmoSQLFunctions(sql, client_session->session_id, instance_id);

  if (log_queries) {
    GIZMOSQL_LOGKV_DYNAMIC(
        effective_log_level, "Client is attempting to run a SQL command",
        {"peer", client_session->peer}, {"kind", "sql"}, {"status", "attempt"},
        {"session_id", client_session->session_id}, {"user", client_session->username},
        {"role", client_session->role}, {"statement_id", handle},
        {"sql", logged_sql});
  }

  // Prevent DETACH of instrumentation database
  if (IsDetachInstrumentationDb(sql)) {
    GIZMOSQL_LOGKV(WARNING, "Client attempted to DETACH instrumentation database",
                   {"peer", client_session->peer}, {"kind", "sql"},
                   {"status", "rejected"}, {"session_id", client_session->session_id},
                   {"user", client_session->username}, {"role", client_session->role},
                   {"statement_id", handle}, {"sql", logged_sql});
    std::string error_msg = "Cannot DETACH the instrumentation database";
    // Record the rejected DETACH attempt
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                 flight_method, is_internal, error_msg);
      }
    }
    return Status::Invalid(error_msg);
  }

  // Handle KILL SESSION command
  std::string target_session_id;
  if (IsKillSessionCommand(sql, target_session_id)) {
    // Helper to record KILL SESSION attempts (both successful and failed)
    auto record_kill_session = [&](const std::string& error_msg = "") {
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
    };

    // Only admin users can kill sessions
    if (client_session->role != "admin") {
      GIZMOSQL_LOGKV(WARNING, "Non-admin user attempted KILL SESSION",
                     {"peer", client_session->peer}, {"kind", "sql"},
                     {"status", "rejected"}, {"session_id", client_session->session_id},
                     {"user", client_session->username}, {"role", client_session->role},
                     {"target_session_id", target_session_id});
      std::string error_msg = "Only admin users can execute KILL SESSION";
      record_kill_session(error_msg);
      return Status::Invalid(error_msg);
    }

    // Cannot kill own session
    if (target_session_id == client_session->session_id) {
      std::string error_msg = "Cannot kill your own session";
      record_kill_session(error_msg);
      return Status::Invalid(error_msg);
    }

    // Find and kill the target session
    auto server = GetServer(*client_session);
    if (!server) {
      std::string error_msg = "Unable to get server instance for KILL SESSION";
      record_kill_session(error_msg);
      return Status::Invalid(error_msg);
    }

    auto target = server->FindSession(target_session_id);
    if (!target) {
      std::string error_msg = "Session not found: " + target_session_id;
      record_kill_session(error_msg);
      return Status::KeyError(error_msg);
    }

    // Mark session for termination and interrupt its connection
    target->kill_requested = true;
    target->connection->Interrupt();

    // Update instrumentation stop reason
    if (target->instrumentation) {
      target->instrumentation->SetStopReason("killed");
    }

    // Remove from session map and mark as killed to prevent reconnection
    ARROW_RETURN_NOT_OK(server->RemoveSession(target_session_id, /*was_killed=*/true));

    GIZMOSQL_LOGKV(INFO, "Session killed successfully",
                   {"peer", client_session->peer}, {"kind", "sql"},
                   {"status", "success"}, {"session_id", client_session->session_id},
                   {"user", client_session->username}, {"role", client_session->role},
                   {"target_session_id", target_session_id});

    // Return a synthetic result for the KILL SESSION command
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, effective_log_level, log_queries, override_schema));
    result->is_gizmosql_admin_ = true;

    // Create statement instrumentation for successful KILL SESSION
    if (auto mgr = server->GetInstrumentationManager()) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal);
    }

    // Create a synthetic result batch with success message
    auto schema = arrow::schema({arrow::field("result", arrow::utf8())});
    arrow::StringBuilder builder;
    ARROW_RETURN_NOT_OK(builder.Append("Session " + target_session_id + " killed successfully"));
    std::shared_ptr<arrow::Array> array;
    ARROW_RETURN_NOT_OK(builder.Finish(&array));
    result->synthetic_result_batch_ = arrow::RecordBatch::Make(
        schema, 1, std::vector<std::shared_ptr<arrow::Array>>{array});

    return result;
  }

  if (IsLikelyGizmoSQLSet(sql)) {
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, effective_log_level, log_queries, override_schema));
    result->is_gizmosql_admin_ = true;

    // Create statement instrumentation (admin commands are still tracked)
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        result->instrumentation_ = std::make_unique<StatementInstrumentation>(
            mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal);
      }
    }

    if (log_queries) {
      GIZMOSQL_LOGKV_DYNAMIC(
          effective_log_level, "Detected GizmoSQL admin SET command",
          {"peer", client_session->peer}, {"kind", "sql"}, {"status", "admin"},
          {"session_id", client_session->session_id}, {"user", client_session->username},
          {"role", client_session->role}, {"statement_id", handle},
          {"sql", logged_sql});
    }
    return result;
  }

  std::shared_ptr<duckdb::PreparedStatement> stmt =
      client_session->connection->Prepare(effective_sql);

  if (stmt->success) {
    // Check catalog-level write access for all databases the statement will modify
    // modified_databases is a map of catalog_name -> CatalogIdentity
    const auto& modified_dbs = stmt->data->properties.modified_databases;
    for (const auto& [catalog_name, catalog_identity] : modified_dbs) {
      if (!client_session->HasWriteAccess(catalog_name)) {
        GIZMOSQL_LOGKV(WARNING, "Access denied: user lacks write access to catalog",
                       {"peer", client_session->peer}, {"kind", "sql"},
                       {"status", "rejected"}, {"session_id", client_session->session_id},
                       {"user", client_session->username}, {"role", client_session->role},
                       {"catalog", catalog_name}, {"statement_id", handle}, {"sql", logged_sql});
        std::string error_msg =
            "Access denied: You do not have write access to catalog '" + catalog_name + "'.";
        // Record the rejected modification attempt
        if (auto server = GetServer(*client_session)) {
          if (auto mgr = server->GetInstrumentationManager()) {
            StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                     flight_method, is_internal, error_msg);
          }
        }
        return Status::Invalid(error_msg);
      }
    }

    // Check catalog-level read access for all databases the statement will read
    // read_databases is a map of catalog_name -> CatalogIdentity
    const auto& read_dbs = stmt->data->properties.read_databases;
    for (const auto& [catalog_name, catalog_identity] : read_dbs) {
      if (!client_session->HasReadAccess(catalog_name)) {
        GIZMOSQL_LOGKV(WARNING, "Access denied: user lacks read access to catalog",
                       {"peer", client_session->peer}, {"kind", "sql"},
                       {"status", "rejected"}, {"session_id", client_session->session_id},
                       {"user", client_session->username}, {"role", client_session->role},
                       {"catalog", catalog_name}, {"statement_id", handle}, {"sql", logged_sql});
        std::string error_msg =
            "Access denied: You do not have read access to catalog '" + catalog_name + "'.";
        // Record the rejected read attempt
        if (auto server = GetServer(*client_session)) {
          if (auto mgr = server->GetInstrumentationManager()) {
            StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                     flight_method, is_internal, error_msg);
          }
        }
        return Status::Invalid(error_msg);
      }
    }

    // Check for readonly role trying to modify data (legacy support)
    if (!stmt->data->properties.IsReadOnly() && client_session->role == "readonly") {
      std::string error_msg =
          "User '" + client_session->username +
          "' has a readonly session and cannot run statements that modify state.";
      // Record the rejected modification attempt by readonly user
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
      return Status::ExecutionError(error_msg);
    }
  }

  if (not stmt->success) {
    std::string error_message = stmt->error.Message();

    // Check if this is the multiple statements error that can be resolved with direct execution
    if (error_message.find("Cannot prepare multiple statements at once") !=
        std::string::npos) {
      // Fallback to direct query execution for statements like PIVOT that get rewritten to multiple statements
      if (log_queries) {
        GIZMOSQL_LOGKV_DYNAMIC(
            effective_log_level,
            "SQL command cannot run as a prepared statement, falling back to direct "
            "query execution",
            {"peer", client_session->peer}, {"kind", "sql"}, {"status", "fallback"},
            {"session_id", client_session->session_id},
            {"user", client_session->username}, {"role", client_session->role},
            {"statement_id", handle}, {"sql", logged_sql});
      }

      std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
          client_session, handle, sql, log_level, log_queries, override_schema));

      // Create statement instrumentation for direct execution
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          result->instrumentation_ = std::make_unique<StatementInstrumentation>(
              mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal);
        }
      }

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
                     {"statement_id", handle}, {"error", err_msg},
                     {"sql", logged_sql});
    }

    // Record the failed statement in instrumentation with the error message
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        // Create instrumentation record for the failed statement (fire and forget)
        StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                 flight_method, is_internal, error_message);
      }
    }

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
      client_session, handle, stmt, log_level, log_queries, override_schema));

  // Create statement instrumentation for prepared statement
  if (auto server = GetServer(*client_session)) {
    if (auto mgr = server->GetInstrumentationManager()) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal);
    }
  }

  status = "success";
  return result;
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& sql,
    const std::optional<arrow::util::ArrowLogLevel>& log_level, const bool& log_queries,
    const std::shared_ptr<arrow::Schema>& override_schema,
    const std::string& flight_method, bool is_internal) {
  std::string handle = boost::uuids::to_string(boost::uuids::random_generator()());
  return DuckDBStatement::Create(client_session, handle, sql, log_level, log_queries,
                                 override_schema, flight_method, is_internal);
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

    if (scope == duckdb::SetScope::SESSION || scope == duckdb::SetScope::AUTOMATIC) {
      client_session_->query_timeout = timeout_seconds;
    } else if (scope == duckdb::SetScope::GLOBAL) {
      if (auto server = GetServer(*client_session_)) {
        ARROW_RETURN_NOT_OK(server->SetQueryTimeout(client_session_, timeout_seconds));
      }
    }

  } else if (name == "gizmosql.query_log_level") {
    arrow::util::ArrowLogLevel query_log_level;
    try {
      query_log_level = log_level_string_to_arrow_log_level(val);
    } catch (...) {
      return arrow::Status::Invalid("Invalid value for query_log_level: " + val);
    }

    if (scope == duckdb::SetScope::SESSION || scope == duckdb::SetScope::AUTOMATIC) {
      client_session_->query_log_level = query_log_level;
    } else if (scope == duckdb::SetScope::GLOBAL) {
      if (auto server = GetServer(*client_session_)) {
        ARROW_RETURN_NOT_OK(server->SetQueryLogLevel(client_session_, query_log_level));
      }
    }
  } else {
    return arrow::Status::Invalid("Unknown GizmoSQL configuration parameter: " + name);
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

DuckDBStatement::~DuckDBStatement() {}

DuckDBStatement::DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                                 const std::string& handle,
                                 const std::shared_ptr<duckdb::PreparedStatement>& stmt,
                                 const std::optional<arrow::util::ArrowLogLevel>& log_level,
                                 const bool& log_queries,
                                 const std::shared_ptr<arrow::Schema>& override_schema) {
  client_session_ = client_session;
  statement_id_ = handle;
  stmt_ = stmt;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(stmt->query);
  use_direct_execution_ = false;
  log_level_ = log_level;
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = stmt->context;
}

DuckDBStatement::DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                                 const std::string& handle, const std::string& sql,
                                 const std::optional<arrow::util::ArrowLogLevel>& log_level,
                                 const bool& log_queries,
                                 const std::shared_ptr<arrow::Schema>& override_schema) {
  client_session_ = client_session;
  statement_id_ = handle;
  sql_ = sql;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(sql);
  use_direct_execution_ = true;
  stmt_ = nullptr;
  log_level_ = log_level;
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = client_session->connection->context;
}

arrow::Result<int> DuckDBStatement::Execute() {
  std::string execute_status;

  ARROW_ASSIGN_OR_RAISE(auto query_timeout, GetQueryTimeout());
  ARROW_ASSIGN_OR_RAISE(auto log_level, GetLogLevel());

  // Generate execution ID for tracing (matches instrumentation table)
  std::string execution_id = boost::uuids::to_string(boost::uuids::random_generator()());

  // Serialize bind parameters for instrumentation
  std::string bind_params_str;
  if (!bind_parameters.empty()) {
    std::stringstream params_ss;
    params_ss << "[";
    for (size_t i = 0; i < bind_parameters.size(); i++) {
      if (i > 0) params_ss << ", ";
      params_ss << "\"" << bind_parameters[i].ToString() << "\"";
    }
    params_ss << "]";
    bind_params_str = params_ss.str();
  }

  // Create execution instrumentation
  if (instrumentation_) {
    if (auto server = GetServer(*client_session_)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        execution_instrumentation_ = std::make_unique<ExecutionInstrumentation>(
            mgr, execution_id, statement_id_, bind_params_str);
      }
    }
  }

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Execute", execute_status, {"peer", client_session_->peer},
      {"session_id", client_session_->session_id}, {"user", client_session_->username},
      {"role", client_session_->role}, {"statement_id", statement_id_},
      {"execution_id", execution_id},
      {"timeout_seconds", std::to_string(query_timeout)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    // If we have a synthetic result (e.g., from KILL SESSION), execution is already done
    if (synthetic_result_batch_) {
      // Mark execution as complete for admin commands
      if (execution_instrumentation_) {
        execution_instrumentation_->SetCompleted(0);
      }
      return 0;
    }
    auto set_status = HandleGizmoSQLSet();
    if (!set_status.ok()) {
      // Mark execution as failed for SET command errors
      if (execution_instrumentation_) {
        execution_instrumentation_->SetError(set_status.ToString());
      }
      return set_status;
    }
    // Mark execution as complete for successful SET commands
    if (execution_instrumentation_) {
      execution_instrumentation_->SetCompleted(0);
    }
    return 0;
  }

  // Launch execution in a separate thread
  auto future = std::async(
      std::launch::async, [this, query_timeout, log_level]() -> arrow::Result<int> {
        if (use_direct_execution_) {
          // The statement may have already been executed from the ComputeSchema() method - if so, just skip execution
          if (query_result_ != nullptr) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_DYNAMIC(
                  log_level,
                  "Direct execution of the SQL command has already occurred, skipping "
                  "re-execution",
                  {"peer", client_session_->peer}, {"kind", "sql"},
                  {"status", "already-executed"},
                  {"session_id", client_session_->session_id},
                  {"user", client_session_->username}, {"role", client_session_->role},
                  {"statement_id", statement_id_}, {"query_timeout", query_timeout});
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
                  {"statement_id", statement_id_}, {"error", result->GetError()},
                  {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout)});
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
                log_level, "Executing prepared statement with bind parameters",
                {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "executing"},
                {"session_id", client_session_->session_id},
                {"user", client_session_->username}, {"role", client_session_->role},
                {"statement_id", statement_id_}, {"bind_parameters", params_str.str()},
                {"param_count", std::to_string(bind_parameters.size())},
                {"query_timeout", std::to_string(query_timeout)});
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
                  {"statement_id", statement_id_}, {"error", query_result_->GetError()},
                  {"sql", logged_sql_}, {"query_timeout", std::to_string(query_timeout)});
            }
            return arrow::Status::ExecutionError("An execution error has occurred: ",
                                                 query_result_->GetError());
          }
        }

        return 0;  // Success
      });

  std::future_status status;
  // Define timeout duration
  auto timeout_duration = std::chrono::seconds(query_timeout);

  if (query_timeout == 0) {
    future.wait();  // Blocks until ready
    status = std::future_status::ready;
  } else {
    status = future.wait_for(timeout_duration);
  }

  if (status == std::future_status::timeout) {
    if (log_queries_) {
      GIZMOSQL_LOGKV(WARNING, "Client SQL command timed out - begin statement interruption",
                     {"peer", client_session_->peer}, {"kind", "sql"},
                     {"status", "timeout"}, {"session_id", client_session_->session_id},
                     {"interruption_status", "begin"},
                     {"user", client_session_->username}, {"role", client_session_->role},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

    // Timeout occurred - interrupt the query
    client_session_->connection->Interrupt();

    // Now wait for the background thread to finish cleanly
    future.wait();

    client_session_->active_sql_handle = "";

    if (log_queries_) {
      GIZMOSQL_LOGKV(WARNING, "Client SQL command timed out - completed statement interruption",
                     {"peer", client_session_->peer}, {"kind", "sql"},
                     {"status", "timeout"}, {"session_id", client_session_->session_id},
                     {"interruption_status", "end"},
                     {"user", client_session_->username}, {"role", client_session_->role},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

    // Record timeout in instrumentation
    if (execution_instrumentation_) {
      execution_instrumentation_->SetTimeout();
    }

    return arrow::Status::ExecutionError("Query execution timed out after ",
                                         std::to_string(timeout_duration.count()),
                                         " seconds");
  }

  // Get the result from the future
  auto result = future.get();

  end_time_ = std::chrono::steady_clock::now();

  // Record success or error in instrumentation
  // Note: rows_fetched will be updated incrementally during FetchResult calls in the batch reader,
  // and the final record will be written when the ExecutionInstrumentation is destroyed
  if (execution_instrumentation_) {
    if (result.ok()) {
      execution_instrumentation_->SetCompleted(GetLastExecutionDurationMs());
    } else {
      execution_instrumentation_->SetError(result.status().ToString());
    }
  }

  if (log_queries_ && result.ok()) {
    GIZMOSQL_LOGKV_DYNAMIC(
        log_level, "Client SQL command execution succeeded",
        {"peer", client_session_->peer}, {"kind", "sql"}, {"status", "success"},
        {"session_id", client_session_->session_id}, {"user", client_session_->username},
        {"role", client_session_->role}, {"statement_id", statement_id_},
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
      {"role", client_session_->role}, {"statement_id", statement_id_},
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
                   {"statement_id", statement_id_},
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
      {"role", client_session_->role}, {"statement_id", statement_id_},
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
      {"role", client_session_->role}, {"statement_id", statement_id_},
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
      {"role", client_session_->role}, {"statement_id", statement_id_},
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

arrow::Result<int32_t> DuckDBStatement::GetQueryTimeout() const {
  // First, try getting the value from the user's session
  if (client_session_->query_timeout.has_value()) {
    return client_session_->query_timeout.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*client_session_)) {
    return server->GetQueryTimeout(client_session_);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

arrow::Result<arrow::util::ArrowLogLevel> DuckDBStatement::GetLogLevel() const {
  if (log_level_.has_value()) {
    return log_level_.value();
  }
  return GetSessionOrServerLogLevel(client_session_);
}

}  // namespace gizmosql::ddb
