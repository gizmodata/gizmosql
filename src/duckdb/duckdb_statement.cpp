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
#include "system_catalog.h"

#include <duckdb.h>
#include <duckdb/main/client_config.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/main/query_profiler.hpp>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <duckdb/function/table/arrow/arrow_duck_schema.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/common/enums/set_scope.hpp>
#include "duckdb/parser/expression/constant_expression.hpp"
#include <future>
#include <chrono>
#include <cctype>
#include <optional>
#include <regex>
#include <functional>
#include <unordered_map>

#include <boost/algorithm/string.hpp>

#include <arrow/api.h>
#include <arrow/util/logging.h>
#include <arrow/c/bridge.h>
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/context/runtime_context.h>
#endif
#include "duckdb_server.h"
#include "session_context.h"
#include "admin_command_guard.h"
#include "gizmosql_telemetry.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/instrumentation/instrumentation_manager.h"
#include "enterprise/instrumentation/instrumentation_records.h"
#include "enterprise/kill_session/kill_session_handler.h"
#include "enterprise/catalog_permissions/catalog_permissions_handler.h"
#include "enterprise/enterprise_features.h"
#endif
#include <nlohmann/json.hpp>
#include "version.h"
#include "gizmosql_library.h"  // GIZMOSQL_SERVER_VERSION (channel-aware)

using arrow::Status;
using duckdb::QueryResult;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
namespace context_api = opentelemetry::context;
#endif

namespace {

/// Returns true if the string is valid JSON (object, array, or primitive).
bool IsValidJSON(const std::string& s) {
  auto parsed = nlohmann::json::parse(s, nullptr, false);
  return !parsed.is_discarded();
}

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

std::string GetSqlOperationForMetrics(const std::string& sql) {
  std::string trimmed = sql;
  boost::algorithm::trim_left(trimmed);
  if (trimmed.empty()) return "UNKNOWN";

  const auto token_end = trimmed.find_first_of(" \t\r\n(;)");
  std::string operation = trimmed.substr(0, token_end);
  while (!operation.empty() &&
         !std::isalpha(static_cast<unsigned char>(operation.front()))) {
    operation.erase(operation.begin());
  }
  if (operation.empty()) return "UNKNOWN";

  boost::algorithm::to_upper(operation);
  return operation;
}

int64_t GetArrayDataSize(const std::shared_ptr<arrow::ArrayData>& data) {
  if (!data) return 0;

  int64_t total_size = 0;
  for (const auto& buffer : data->buffers) {
    if (buffer) total_size += static_cast<int64_t>(buffer->size());
  }
  for (const auto& child : data->child_data) {
    total_size += GetArrayDataSize(child);
  }
  if (data->dictionary) {
    total_size += GetArrayDataSize(data->dictionary);
  }

  return total_size;
}

int64_t GetRecordBatchSizeBytes(const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!batch) return 0;

  int64_t total_size = 0;
  for (int i = 0; i < batch->num_columns(); ++i) {
    total_size += GetArrayDataSize(batch->column(i)->data());
  }
  return total_size;
}

bool IsDetachInstrumentationDb(const std::string& sql, const std::string& instrumentation_catalog = "") {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Case-insensitive check for DETACH commands targeting instrumentation DB
  std::string upper = boost::to_upper_copy(trimmed);
  if (upper.find("DETACH") == std::string::npos) {
    return false;
  }
  // Check for default instrumentation DB aliases/names
  if (upper.find("_GIZMOSQL_INSTR") != std::string::npos ||
      upper.find("GIZMOSQL_INSTRUMENTATION") != std::string::npos) {
    return true;
  }
  // Check for external instrumentation catalog name (e.g., DuckLake catalog)
  if (!instrumentation_catalog.empty()) {
    std::string upper_catalog = boost::to_upper_copy(instrumentation_catalog);
    if (upper.find(upper_catalog) != std::string::npos) {
      return true;
    }
  }
  return false;
}

// Generic DETACH guard: true if `sql` is a DETACH that mentions `catalog`
// (same substring heuristic as IsDetachInstrumentationDb). Used to also protect
// the catalog-logging catalog from being detached out from under the sink.
bool SqlDetachesCatalog(const std::string& sql, const std::string& catalog) {
  if (catalog.empty()) return false;
  std::string upper = boost::to_upper_copy(sql);
  if (upper.find("DETACH") == std::string::npos) {
    return false;
  }
  return upper.find(boost::to_upper_copy(catalog)) != std::string::npos;
}

#ifndef GIZMOSQL_ENTERPRISE
// Core edition: local implementation for detection only
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
#endif

// These helpers are used by both Core and Enterprise editions
std::string UnquoteIdentifier(const std::string& identifier) {
  if (identifier.size() >= 2 && identifier.front() == '"' && identifier.back() == '"') {
    std::string unquoted = identifier.substr(1, identifier.size() - 2);
    boost::replace_all(unquoted, "\"\"", "\"");
    return unquoted;
  }
  return identifier;
}

std::optional<std::string> TryExtractUseCatalogName(const std::string& sql) {
  static const std::regex use_pattern(
      R"(^\s*USE\s+((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*))(?:\s*\.\s*((?:"(?:[^"]|"")+"|[A-Za-z_][A-Za-z0-9_]*)))?\s*;?\s*$)",
      std::regex_constants::icase);

  std::smatch match;
  if (!std::regex_match(sql, match, use_pattern)) {
    return std::nullopt;
  }

  const std::string first_identifier = UnquoteIdentifier(match[1].str());
  if (match[2].matched) {
    return first_identifier;
  }

  return first_identifier;
}

bool CatalogExistsOnConnection(duckdb::Connection& connection,
                               const std::string& catalog_name) {
  auto stmt = connection.Prepare(
      "SELECT 1 FROM information_schema.schemata WHERE catalog_name = ? LIMIT 1");
  if (!stmt || !stmt->success) {
    return false;
  }

  duckdb::vector<duckdb::Value> bind_parameters;
  bind_parameters.emplace_back(catalog_name);
  auto result = stmt->Execute(bind_parameters);
  if (!result || result->HasError()) {
    return false;
  }

  auto row = result->Fetch();
  return row != nullptr && row->size() > 0;
}

// Replace GizmoSQL pseudo-functions with actual values:
//   GIZMOSQL_CURRENT_SESSION() -> current session UUID
//   GIZMOSQL_CURRENT_INSTANCE() -> current server instance UUID
//   GIZMOSQL_CURRENT_CLUSTER() -> current cluster UUID (NULL if --cluster-id unset)
//   GIZMOSQL_VERSION() -> GizmoSQL version string
//   GIZMOSQL_USER() -> current username
//   GIZMOSQL_ROLE() -> current user's role
//   GIZMOSQL_EDITION() -> edition name ("Core" or "Enterprise")
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
                                     const std::string& instance_id,
                                     const std::string& cluster_id,
                                     const std::string& username,
                                     const std::string& role,
                                     const std::string& edition,
                                     bool instrumentation_enabled,
                                     const std::string& instrumentation_catalog,
                                     const std::string& instrumentation_schema) {
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

    // Check for GIZMOSQL_CURRENT_CLUSTER() at this position
    constexpr size_t kClusterFuncLen = 26;  // length of "GIZMOSQL_CURRENT_CLUSTER()"
    if (i + kClusterFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kClusterFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_CURRENT_CLUSTER()") {
        if (cluster_id.empty()) {
          result += "NULL";
        } else {
          result += '\'';
          result += cluster_id;
          result += '\'';
        }
        if (ShouldAddAlias(sql, i, i + kClusterFuncLen)) {
          result += " AS \"GIZMOSQL_CURRENT_CLUSTER()\"";
        }
        i += kClusterFuncLen;
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
        // Use the channel-aware version string so SELECT GIZMOSQL_VERSION()
        // on an LTS server returns e.g. "v1.25.0-LTS" rather than the bare
        // git tag "v1.25.0". Stable builds remain unchanged.
        result += GIZMOSQL_SERVER_VERSION;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kVersionFuncLen)) {
          result += " AS \"GIZMOSQL_VERSION()\"";
        }
        i += kVersionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_USER() at this position
    constexpr size_t kUserFuncLen = 15;  // length of "GIZMOSQL_USER()"
    if (i + kUserFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kUserFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_USER()") {
        result += '\'';
        result += username;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kUserFuncLen)) {
          result += " AS \"GIZMOSQL_USER()\"";
        }
        i += kUserFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_ROLE() at this position
    constexpr size_t kRoleFuncLen = 15;  // length of "GIZMOSQL_ROLE()"
    if (i + kRoleFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kRoleFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_ROLE()") {
        result += '\'';
        result += role;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kRoleFuncLen)) {
          result += " AS \"GIZMOSQL_ROLE()\"";
        }
        i += kRoleFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_EDITION() at this position
    constexpr size_t kEditionFuncLen = 18;  // length of "GIZMOSQL_EDITION()"
    if (i + kEditionFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kEditionFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_EDITION()") {
        result += '\'';
        result += edition;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kEditionFuncLen)) {
          result += " AS \"GIZMOSQL_EDITION()\"";
        }
        i += kEditionFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_ENABLED() at this position
    constexpr size_t kInstrEnabledFuncLen = 34;  // length of "GIZMOSQL_INSTRUMENTATION_ENABLED()"
    if (i + kInstrEnabledFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrEnabledFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_ENABLED()") {
        result += instrumentation_enabled ? "true" : "false";
        if (ShouldAddAlias(sql, i, i + kInstrEnabledFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_ENABLED()\"";
        }
        i += kInstrEnabledFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_CATALOG() at this position
    constexpr size_t kInstrCatalogFuncLen = 34;  // length of "GIZMOSQL_INSTRUMENTATION_CATALOG()"
    if (i + kInstrCatalogFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrCatalogFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_CATALOG()") {
        result += '\'';
        result += instrumentation_catalog;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kInstrCatalogFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_CATALOG()\"";
        }
        i += kInstrCatalogFuncLen;
        continue;
      }
    }

    // Check for GIZMOSQL_INSTRUMENTATION_SCHEMA() at this position
    constexpr size_t kInstrSchemaFuncLen = 33;  // length of "GIZMOSQL_INSTRUMENTATION_SCHEMA()"
    if (i + kInstrSchemaFuncLen <= sql.size()) {
      std::string candidate = sql.substr(i, kInstrSchemaFuncLen);
      std::string upper_candidate = boost::to_upper_copy(candidate);
      if (upper_candidate == "GIZMOSQL_INSTRUMENTATION_SCHEMA()") {
        result += '\'';
        result += instrumentation_schema;
        result += '\'';
        if (ShouldAddAlias(sql, i, i + kInstrSchemaFuncLen)) {
          result += " AS \"GIZMOSQL_INSTRUMENTATION_SCHEMA()\"";
        }
        i += kInstrSchemaFuncLen;
        continue;
      }
    }

    result += sql[i++];
  }

  return result;
}

}  // namespace

namespace gizmosql::ddb {

namespace {
// Defined with the GizmoSQL settings registry below; forward-declared here because
// DuckDBStatement::Create() uses it for the gizmosql_settings() table function.
std::string RewriteGizmoSettings(const std::string& sql, const ClientSession& session,
                                 DuckDBFlightSqlServer* server,
                                 duckdb::vector<duckdb::Value>& binds);
}  // namespace
std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType& duckdb_type) {
  switch (duckdb_type.id()) {
    case duckdb::LogicalTypeId::INTEGER:
      return arrow::int32();
    case duckdb::LogicalTypeId::DECIMAL: {
      uint8_t width = duckdb::DecimalType::GetWidth(duckdb_type);
      uint8_t scale = duckdb::DecimalType::GetScale(duckdb_type);
      // Always emit Decimal128 (or Decimal256 when the precision exceeds 38).
      // DuckDB reports width=0 for parameters whose precision wasn't resolved
      // during parse (e.g. `?` in `INSERT INTO t(dec) VALUES (?)`) — use the
      // widest decimal in that case. Also, the Arrow Java JDBC client only
      // supports Decimal128/Decimal256, so we must not hand it a Decimal32/64
      // (which is what arrow::smallest_decimal would pick for width<=18).
      if (width == 0 || width > 38) {
        return arrow::decimal256(width == 0 ? 38 : width, scale);
      }
      return arrow::decimal128(width, scale);
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
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return arrow::timestamp(arrow::TimeUnit::MICRO, "UTC");
    case duckdb::LogicalTypeId::TIME_TZ:
      return arrow::time64(arrow::TimeUnit::MICRO);
    case duckdb::LogicalTypeId::HUGEINT:
      return arrow::decimal128(38, 0);
    case duckdb::LogicalTypeId::INVALID:
    case duckdb::LogicalTypeId::SQLNULL:
    case duckdb::LogicalTypeId::UNKNOWN:
    case duckdb::LogicalTypeId::ANY:
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
    // DuckDB v1.4.x still carries LogicalTypeId::USER as a distinct enum
    // value; v1.5+ folded it into UNBOUND, so the case is omitted there.
    case duckdb::LogicalTypeId::USER:
#endif
      return arrow::null();
    case duckdb::LogicalTypeId::LIST: {
      auto child_type = duckdb::ListType::GetChildType(duckdb_type);
      return arrow::list(GetDataTypeFromDuckDbType(child_type));
    }
    case duckdb::LogicalTypeId::STRUCT: {
      auto& child_types = duckdb::StructType::GetChildTypes(duckdb_type);
      arrow::FieldVector fields;
      for (auto& child : child_types) {
        fields.push_back(
            arrow::field(child.first, GetDataTypeFromDuckDbType(child.second)));
      }
      return arrow::struct_(fields);
    }
    case duckdb::LogicalTypeId::MAP: {
      auto key_type = duckdb::MapType::KeyType(duckdb_type);
      auto value_type = duckdb::MapType::ValueType(duckdb_type);
      return arrow::map(GetDataTypeFromDuckDbType(key_type),
                        GetDataTypeFromDuckDbType(value_type));
    }
    case duckdb::LogicalTypeId::ARRAY: {
      auto child_type = duckdb::ArrayType::GetChildType(duckdb_type);
      auto array_size = duckdb::ArrayType::GetSize(duckdb_type);
      return arrow::fixed_size_list(GetDataTypeFromDuckDbType(child_type), array_size);
    }
#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
    case duckdb::LogicalTypeId::VARIANT:
      // VARIANT is self-describing typed binary data (DuckDB v1.5.0+).
      // DuckDB's Arrow exporter does not yet support VARIANT natively, so
      // clients should cast to VARCHAR or JSON before querying:
      //   SELECT v::VARCHAR FROM t;
      // Not present in the LTS channel (v1.4.x).
      return arrow::binary();
#endif
    case duckdb::LogicalTypeId::POINTER:
    case duckdb::LogicalTypeId::VALIDITY:
    case duckdb::LogicalTypeId::UUID:
    case duckdb::LogicalTypeId::TABLE:
    case duckdb::LogicalTypeId::ENUM:
    default:
      return arrow::null();
  }
}

arrow::Result<arrow::util::ArrowLogLevel> GetSessionOrServerLogLevel(
    const std::shared_ptr<ClientSession>& client_session) {
  // Fall-back to the session query log level if the statement log level is not set...
  if (client_session->query_log_level.has_value()) {
    return client_session->query_log_level.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*client_session)) {
    return server->GetQueryLogLevel(*client_session);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

// Resolve the effective query-profile-capture mode for a session: the per-session
// override wins, otherwise fall back to the server default. Returns kOff if the
// server instance is gone (capture is a best-effort, never-fail feature).
QueryProfileMode GetSessionOrServerCaptureProfile(
    const std::shared_ptr<ClientSession>& client_session) {
  if (client_session->capture_query_profile.has_value()) {
    return client_session->capture_query_profile.value();
  }
  if (auto server = GetServer(*client_session)) {
    return server->GetCaptureQueryProfile(*client_session);
  }
  return QueryProfileMode::kOff;
}

arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
    const std::shared_ptr<ClientSession>& client_session, const std::string& handle,
    const std::string& sql, const std::optional<arrow::util::ArrowLogLevel>& log_level,
    const bool& log_queries, const std::shared_ptr<arrow::Schema>& override_schema,
    const std::string& flight_method, bool is_internal) {
  std::string status;
  auto logged_sql = redact_sql_for_logs(sql);
  // Threshold: session/server log level gates whether messages are emitted
  ARROW_ASSIGN_OR_RAISE(auto log_threshold,
                        GetSessionOrServerLogLevel(client_session));
  // Display severity: statement's own level, defaulting to INFO for user queries
  auto display_severity =
      log_level.value_or(arrow::util::ArrowLogLevel::ARROW_INFO);

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Create", status, {"peer", client_session->peer},
      {"session_id", client_session->session_id}, {"user", client_session->username},
      {"role", client_session->role}, {"statement_id", handle});

  client_session->active_sql_handle = handle;

  // Rudimentary admin-command gate (Core): block dangerous filesystem- and
  // instance-level commands (ATTACH/DETACH, SET GLOBAL, INSTALL/LOAD, CHECKPOINT,
  // COPY/EXPORT to local files, read_* of local files, duckdb_secrets()) for
  // non-admin sessions, ahead of the full RBAC model (GizmoSQL 2.0). Basic-auth
  // users are always "admin", so this only affects token deployments with
  // non-admin roles. Internal/system queries are exempt. Admins and internal
  // queries skip the parse entirely (fast path).
  if (!is_internal && client_session->role != "admin") {
    if (auto gated_category = gizmosql::ddb::ClassifyGatedCommand(sql)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Client attempted a gated admin command",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"gated", *gated_category}, {"statement_id", handle},
                             {"sql", logged_sql});
#ifdef GIZMOSQL_ENTERPRISE
      std::string gate_error_msg =
          "Permission denied: " + *gated_category +
          " requires the 'admin' role. This GizmoSQL instance restricts "
          "filesystem- and instance-level commands to admin users.";
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, gate_error_msg);
        }
      }
#endif
      return gizmosql::ddb::CheckNonAdminCommandAllowed(sql);
    }
  }

  // Get instance_id + cluster_id from the server for the
  // GIZMOSQL_CURRENT_INSTANCE() / GIZMOSQL_CURRENT_CLUSTER() pseudo-functions.
  std::string instance_id;
  std::string cluster_id;
  if (auto server = GetServer(*client_session)) {
    instance_id = server->GetInstanceId();
    cluster_id = server->GetClusterId();
  }

  // Get edition name for GIZMOSQL_EDITION()
#ifdef GIZMOSQL_ENTERPRISE
  std::string edition = enterprise::EnterpriseFeatures::Instance().GetEditionName();
#else
  std::string edition = "Core";
#endif

  // Get instrumentation state for GIZMOSQL_INSTRUMENTATION_*() functions
  bool instrumentation_enabled = false;
  std::string instrumentation_catalog;
  std::string instrumentation_schema;
#ifdef GIZMOSQL_ENTERPRISE
  if (auto server = GetServer(*client_session)) {
    if (auto mgr = server->GetInstrumentationManager()) {
      instrumentation_enabled = true;
      instrumentation_catalog = mgr->GetCatalog();
      instrumentation_schema = mgr->GetSchema();
    }
  }
#endif

  // Replace GIZMOSQL_* pseudo-functions with actual values
  std::string effective_sql = ReplaceGizmoSQLFunctions(
      sql, client_session->session_id, instance_id, cluster_id, client_session->username,
      client_session->role, edition, instrumentation_enabled,
      instrumentation_catalog, instrumentation_schema);

  // gizmosql_settings() table function: rewrite to a bind-parameterized VALUES so
  // its rows (incl. JSON tags / descriptions) are passed as bound parameters. The
  // collected binds are attached to the prepared statement below.
  duckdb::vector<duckdb::Value> settings_binds;
  if (boost::icontains(effective_sql, "gizmosql_settings()")) {
    auto settings_server = GetServer(*client_session);
    effective_sql = RewriteGizmoSettings(effective_sql, *client_session,
                                         settings_server.get(), settings_binds);
  }

#ifdef GIZMOSQL_ENTERPRISE
  std::shared_ptr<InstrumentationManager> instr_mgr;
  std::string log_catalog;
  if (auto server = GetServer(*client_session)) {
    instr_mgr = server->GetInstrumentationManager();
    log_catalog = server->GetLogCatalog();
  }

  if (!is_internal) {
    if (auto use_catalog_name = TryExtractUseCatalogName(sql)) {
      if (CatalogExistsOnConnection(client_session->connection->Get(), *use_catalog_name)) {
        ARROW_RETURN_NOT_OK(gizmosql::enterprise::EnsureCatalogReadAccess(
            client_session, *use_catalog_name, instr_mgr, handle, logged_sql,
            flight_method, is_internal, log_catalog));
      }
    }
  }

  // Catalog visibility filtering: rewrite metadata queries to hide unauthorized catalogs
  if (!client_session->catalog_access.empty() &&
      enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    auto allowed = enterprise::GetAllowedCatalogs(
        *client_session, client_session->connection->Get(), instr_mgr, log_catalog);
    if (!allowed.empty()) {
      auto filter_in = enterprise::BuildCatalogFilterIN(allowed);
      std::string rewritten;
      if (enterprise::RewriteShowCommand(effective_sql, filter_in, rewritten)) {
        GIZMOSQL_LOGKV_SESSION(DEBUG, client_session,
                               "Catalog visibility filter rewrote SHOW command",
                               {"kind", "sql"}, {"original_sql", effective_sql},
                               {"rewritten_sql", rewritten});
        effective_sql = std::move(rewritten);
      } else {
        auto filtered = enterprise::FilterMetadataReferences(effective_sql, filter_in);
        if (filtered != effective_sql) {
          GIZMOSQL_LOGKV_SESSION(DEBUG, client_session,
                                 "Catalog visibility filter rewrote metadata references",
                                 {"kind", "sql"}, {"original_sql", effective_sql},
                                 {"rewritten_sql", filtered});
          effective_sql = std::move(filtered);
        }
      }
    }
  }
#endif

  if (log_queries) {
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
        log_threshold, display_severity,
        client_session, "Client is attempting to run a SQL command",
        {"kind", "sql"}, {"status", "attempt"}, {"statement_id", handle},
        {"sql", logged_sql}, {"is_internal", is_internal ? "true" : "false"},
        {"flight_method", flight_method});
  }

  // Prevent DETACH of the system-managed catalogs — instrumentation and
  // catalog-logging — including externally-attached ones (e.g. DuckLake/PostgreSQL).
#ifdef GIZMOSQL_ENTERPRISE
  {
    std::string instr_catalog;
    std::string log_catalog;
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        instr_catalog = mgr->GetCatalog();
      }
      log_catalog = server->GetLogCatalog();
    }
    const char* detach_target = nullptr;
    if (IsDetachInstrumentationDb(sql, instr_catalog)) {
      detach_target = "instrumentation";
    } else if (SqlDetachesCatalog(sql, log_catalog)) {
      detach_target = "catalog-logging";
    }
    if (detach_target != nullptr) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                     "Client attempted to DETACH a system-managed database",
                     {"kind", "sql"}, {"status", "rejected"}, {"catalog", detach_target},
                     {"statement_id", handle}, {"sql", logged_sql});
      std::string error_msg =
          std::string("Cannot DETACH the ") + detach_target + " database";
      // Record the rejected DETACH attempt
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
      return Status::Invalid(error_msg);
    }
  }
#endif

  // Handle KILL SESSION command
  std::string target_session_id;
#ifdef GIZMOSQL_ENTERPRISE
  if (gizmosql::enterprise::IsKillSessionCommand(sql, target_session_id)) {
    auto server = GetServer(*client_session);
    std::shared_ptr<InstrumentationManager> instr_mgr;
    if (server) {
      instr_mgr = server->GetInstrumentationManager();
    }

    auto kill_status = gizmosql::enterprise::HandleKillSession(
        client_session, target_session_id, server.get(), instr_mgr,
        handle, logged_sql, flight_method, is_internal);

    if (!kill_status.ok()) {
      return kill_status;
    }

    // Return a synthetic result for the successful KILL SESSION command
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, display_severity, log_queries, override_schema,
        is_internal, flight_method));
    result->is_gizmosql_admin_ = true;

    // Create statement instrumentation for successful KILL SESSION
    if (instr_mgr) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          instr_mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
          "", client_session->query_tag);
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
#else
  // Core edition: KILL SESSION requires enterprise license
  if (IsKillSessionCommand(sql, target_session_id)) {
    std::string error_msg = "KILL SESSION is a commercially licensed enterprise feature. "
                            "Please provide a valid license key file via --license-key-file "
                            "or contact GizmoData sales at sales@gizmodata.com to obtain a license.";
    GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "KILL SESSION attempted without enterprise license",
                   {"kind", "sql"}, {"status", "rejected"},
                   {"target_session_id", target_session_id});
    return Status::Invalid(error_msg);
  }
#endif

  if (IsLikelyGizmoSQLSet(sql)) {
    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
        client_session, handle, sql, display_severity, log_queries, override_schema,
        is_internal, flight_method));
    result->is_gizmosql_admin_ = true;

#ifdef GIZMOSQL_ENTERPRISE
    // Create statement instrumentation (admin commands are still tracked)
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        result->instrumentation_ = std::make_unique<StatementInstrumentation>(
            mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
            "", client_session->query_tag);
      }
    }
#endif

    if (log_queries) {
      GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
          log_threshold, display_severity,
          client_session, "Detected GizmoSQL admin SET command",
          {"kind", "sql"}, {"status", "admin"}, {"statement_id", handle},
          {"sql", logged_sql}, {"is_internal", is_internal ? "true" : "false"},
          {"flight_method", flight_method});
    }
    return result;
  }

  std::shared_ptr<duckdb::PreparedStatement> stmt =
      client_session->connection->Get().Prepare(effective_sql);

  if (stmt->success) {
    // Block writes to the GizmoSQL system catalog regardless of role or
    // licensing — it is a process-local in-memory catalog that hosts
    // server-managed metadata views, and clients must not be able to
    // mutate them. Enforced in both Core and Enterprise builds. The
    // analogous protection for the instrumentation catalog
    // (_gizmosql_instr) lives in CheckCatalogWriteAccess and only runs
    // in Enterprise builds because that catalog only exists there.
    for (const auto& [catalog_name, _ignored] :
         stmt->data->properties.modified_databases) {
      if (gizmosql::IsSystemCatalog(catalog_name)) {
        std::string error_msg =
            "Access denied: The GizmoSQL system catalog '" + catalog_name +
            "' is read-only.";
        GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                               "Access denied: system catalog is read-only",
                               {"kind", "sql"}, {"status", "rejected"},
                               {"catalog", catalog_name},
                               {"statement_id", handle},
                               {"sql", logged_sql});
#ifdef GIZMOSQL_ENTERPRISE
        if (auto server = GetServer(*client_session)) {
          if (auto mgr = server->GetInstrumentationManager()) {
            StatementInstrumentation(mgr, handle, client_session->session_id,
                                     logged_sql, flight_method, is_internal,
                                     error_msg);
          }
        }
#endif
        return arrow::Status::Invalid(error_msg);
      }
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Check catalog-level access permissions (Enterprise feature)
    // These checks enforce per-catalog read/write permissions from JWT token claims
    std::shared_ptr<InstrumentationManager> instr_mgr;
    std::string log_catalog;
    if (auto server = GetServer(*client_session)) {
      instr_mgr = server->GetInstrumentationManager();
      log_catalog = server->GetLogCatalog();
    }

    // Check write access for all catalogs the statement will modify
    auto write_status = gizmosql::enterprise::CheckCatalogWriteAccess(
        client_session, stmt->data->properties.modified_databases,
        instr_mgr, handle, logged_sql, flight_method, is_internal, log_catalog);
    if (!write_status.ok()) {
      return write_status;
    }

    // Check read access for all catalogs the statement will read
    auto read_status = gizmosql::enterprise::CheckCatalogReadAccess(
        client_session, stmt->data->properties.read_databases,
        instr_mgr, handle, logged_sql, flight_method, is_internal, log_catalog);
    if (!read_status.ok()) {
      return read_status;
    }
#endif

    // Check for readonly role trying to modify data (legacy support)
    if (!stmt->data->properties.IsReadOnly() && client_session->role == "readonly") {
      std::string error_msg =
          "User '" + client_session->username +
          "' has a readonly session and cannot run statements that modify state.";
#ifdef GIZMOSQL_ENTERPRISE
      // Record the rejected modification attempt by readonly user
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                   flight_method, is_internal, error_msg);
        }
      }
#endif
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
        GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
            log_threshold, display_severity,
            client_session,
            "SQL command cannot run as a prepared statement, falling back to direct "
            "query execution",
            {"kind", "sql"}, {"status", "fallback"},
            {"statement_id", handle}, {"sql", logged_sql},
            {"is_internal", is_internal ? "true" : "false"},
            {"flight_method", flight_method});
      }

      std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
          client_session, handle, effective_sql, log_level, log_queries, override_schema,
          is_internal, flight_method));

#ifdef GIZMOSQL_ENTERPRISE
      // Create statement instrumentation for direct execution
      if (auto server = GetServer(*client_session)) {
        if (auto mgr = server->GetInstrumentationManager()) {
          result->instrumentation_ = std::make_unique<StatementInstrumentation>(
              mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
              "", client_session->query_tag);
        }
      }
#endif

      status = "success";
      return result;
    }

    // Other preparation errors are still fatal
    std::string err_msg =
        "Can't prepare statement: '" + logged_sql + "' - Error: " + error_message;

    if (log_queries) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "Client SQL command failed preparation",
                     {"kind", "sql"}, {"status", "failure"},
                     {"statement_id", handle}, {"error", err_msg},
                     {"sql", logged_sql});
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Record the failed statement in instrumentation with the error message
    if (auto server = GetServer(*client_session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        // Create instrumentation record for the failed statement (fire and forget)
        StatementInstrumentation(mgr, handle, client_session->session_id, logged_sql,
                                 flight_method, is_internal, error_message);
      }
    }
#endif

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(
      client_session, handle, stmt, log_level, log_queries, override_schema, is_internal,
      flight_method));

  // Bind the gizmosql_settings() values (if this statement referenced it).
  if (!settings_binds.empty()) {
    result->bind_parameters = std::move(settings_binds);
  }

#ifdef GIZMOSQL_ENTERPRISE
  // Create statement instrumentation for prepared statement
  if (auto server = GetServer(*client_session)) {
    if (auto mgr = server->GetInstrumentationManager()) {
      result->instrumentation_ = std::make_unique<StatementInstrumentation>(
          mgr, handle, client_session->session_id, logged_sql, flight_method, is_internal,
          "", client_session->query_tag);
    }
  }
#endif

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

namespace {

// ---- GizmoSQL settings registry --------------------------------------------
// Single source of truth for the gizmosql.* session/server settings. Each
// descriptor carries metadata plus the mutator hooks invoked by
// SettingsRegistry::Apply(), which centralizes the cross-cutting checks:
// unknown-name, enterprise license, scope validity, and the admin gate for
// GLOBAL writes. Adding a setting = one descriptor.
enum class SetScopeKind { kSessionOnly, kGlobalOnly, kSessionOrGlobal };

struct GizmoSetting {
  std::string name;
  SetScopeKind scope;
  bool enterprise = false;
  const char* enterprise_feature = nullptr;  // mirrors enterprise::kFeature* values
  std::string input_type;                    // INTEGER / BOOLEAN / VARCHAR (display)
  std::string env_var;                       // "" if none
  std::string default_value;
  std::string description;
  // Value accessors for gizmosql_settings() (canonical strings; nullopt = unset /
  // not-applicable-at-this-scope).
  std::function<std::optional<std::string>(const ClientSession&)> get_session;
  std::function<std::optional<std::string>(DuckDBFlightSqlServer&, const ClientSession&)>
      get_global;
  std::function<arrow::Status(ClientSession&, const std::string&)> set_session;
  std::function<arrow::Status(DuckDBFlightSqlServer&, ClientSession&,
                              const std::string&)>
      set_global;
};

// Parse a SET value into a bool, tolerating the forms DuckDB's parser produces:
// integer/string constants and the `true`/`false` cast-literal text.
arrow::Result<bool> ParseSetBool(const std::string& val, const std::string& setting) {
  const std::string lowered = boost::algorithm::to_lower_copy(val);
  if (lowered == "true" || lowered == "1" || lowered == "on" || lowered == "yes" ||
      lowered == "t" || lowered == "'true'" || lowered == "cast('t' as boolean)") {
    return true;
  }
  if (lowered == "false" || lowered == "0" || lowered == "off" || lowered == "no" ||
      lowered == "f" || lowered == "'false'" || lowered == "cast('f' as boolean)") {
    return false;
  }
  return arrow::Status::Invalid("Invalid value for " + setting + ": " + val);
}

arrow::Result<int32_t> ParseSetInt(const std::string& val, const std::string& setting) {
  try {
    return static_cast<int32_t>(std::stoi(val));
  } catch (...) {
    return arrow::Status::Invalid("Invalid value for " + setting + ": " + val);
  }
}

class SettingsRegistry {
 public:
  static const SettingsRegistry& Instance() {
    static const SettingsRegistry kRegistry;
    return kRegistry;
  }

  const GizmoSetting* Find(const std::string& name) const {
    auto it = by_name_.find(name);
    return it == by_name_.end() ? nullptr : &settings_[it->second];
  }

  const std::vector<GizmoSetting>& All() const { return settings_; }

  // Dispatch `SET [SESSION|GLOBAL] <name> = <val>` after centralized checks.
  arrow::Status Apply(ClientSession& session, DuckDBFlightSqlServer* server,
                      const std::string& name, duckdb::SetScope scope,
                      const std::string& val) const {
    const GizmoSetting* d = Find(name);
    if (!d) {
      return arrow::Status::Invalid("Unknown GizmoSQL configuration parameter: " + name);
    }

    // Enterprise license gate (centralized).
    if (d->enterprise) {
#ifdef GIZMOSQL_ENTERPRISE
      if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsFeatureAvailable(
              d->enterprise_feature)) {
        return arrow::Status::Invalid(
            gizmosql::enterprise::EnterpriseFeatures::GetLicenseRequiredError("SET " +
                                                                              d->name));
      }
#else
      return arrow::Status::Invalid(
          "SET " + d->name +
          " is a commercially licensed enterprise feature. Please provide a valid "
          "license key file via --license-key-file or contact GizmoData sales at "
          "sales@gizmodata.com to obtain a license.");
#endif
    }

    // Resolve effective scope. A bare SET (AUTOMATIC) on a global-only setting
    // means GLOBAL; otherwise a non-GLOBAL scope means SESSION.
    const bool to_global = scope == duckdb::SetScope::GLOBAL ||
                           (scope != duckdb::SetScope::GLOBAL &&
                            d->scope == SetScopeKind::kGlobalOnly);
    if (to_global && d->scope == SetScopeKind::kSessionOnly) {
      return arrow::Status::Invalid(d->name + " can only be set at SESSION scope");
    }
    if (!to_global && d->scope == SetScopeKind::kGlobalOnly) {
      return arrow::Status::Invalid(d->name + " can only be set with SET GLOBAL");
    }

    if (to_global) {
      if (session.role != "admin") {
        return arrow::Status::Invalid("Only admin users can SET GLOBAL " + d->name);
      }
      if (!server || !d->set_global) {
        return arrow::Status::Invalid(d->name + " cannot be set at GLOBAL scope");
      }
      return d->set_global(*server, session, val);
    }
    if (!d->set_session) {
      return arrow::Status::Invalid(d->name + " cannot be set at SESSION scope");
    }
    return d->set_session(session, val);
  }

 private:
  SettingsRegistry();
  std::vector<GizmoSetting> settings_;
  std::unordered_map<std::string, size_t> by_name_;
};

SettingsRegistry::SettingsRegistry() {
  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_timeout",
      .scope = SetScopeKind::kSessionOrGlobal,
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_QUERY_TIMEOUT",
      .default_value = "0",
      .description = "Per-statement timeout in seconds (0 = no timeout).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_timeout ? std::optional(std::to_string(*s.query_timeout))
                               : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        auto r = srv.GetQueryTimeout(s);
        return r.ok() ? std::optional(std::to_string(*r)) : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "query_timeout"));
        s.query_timeout = n;
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "query_timeout"));
        return srv.SetQueryTimeout(s, n);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_log_level",
      .scope = SetScopeKind::kSessionOrGlobal,
      .input_type = "VARCHAR",
      .env_var = "GIZMOSQL_QUERY_LOG_LEVEL",
      .default_value = "INFO",
      .description = "Query-execution log level (DEBUG/INFO/WARNING/ERROR).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_log_level ? std::optional(log_level_arrow_log_level_to_string(
                                       *s.query_log_level))
                                 : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        auto r = srv.GetQueryLogLevel(s);
        return r.ok() ? std::optional(log_level_arrow_log_level_to_string(*r))
                      : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        try {
          s.query_log_level = log_level_string_to_arrow_log_level(val);
        } catch (...) {
          return arrow::Status::Invalid("Invalid value for query_log_level: " + val);
        }
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        arrow::util::ArrowLogLevel lvl;
        try {
          lvl = log_level_string_to_arrow_log_level(val);
        } catch (...) {
          return arrow::Status::Invalid("Invalid value for query_log_level: " + val);
        }
        return srv.SetQueryLogLevel(s, lvl);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.capture_query_profile",
      .scope = SetScopeKind::kSessionOrGlobal,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .env_var = "GIZMOSQL_CAPTURE_QUERY_PROFILE",
      .default_value = "off",
      .description =
          "Capture DuckDB query profiles into instrumentation "
          "(off/standard/detailed).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.capture_query_profile
                   ? std::optional(query_profile_mode_to_string(*s.capture_query_profile))
                   : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession& s) -> std::optional<std::string> {
        return query_profile_mode_to_string(srv.GetCaptureQueryProfile(s));
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        try {
          s.capture_query_profile = query_profile_mode_from_string(val);
        } catch (const std::invalid_argument& ex) {
          return arrow::Status::Invalid(ex.what());
        }
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession& s,
                       const std::string& val) -> arrow::Status {
        QueryProfileMode mode;
        try {
          mode = query_profile_mode_from_string(val);
        } catch (const std::invalid_argument& ex) {
          return arrow::Status::Invalid(ex.what());
        }
        return srv.SetCaptureQueryProfile(s, mode);
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.bypass_queue",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "BOOLEAN",
      .default_value = "false",
      .description = "Skip the statement queue for this session (admin only to enable).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.bypass_queue
                   ? std::optional(std::string(*s.bypass_queue ? "true" : "false"))
                   : std::nullopt;
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(bool b, ParseSetBool(val, "bypass_queue"));
        // Only admins may *enable* bypass; a non-admin must not be able to lift
        // their own queueing (privilege-escalation footgun).
        if (b && s.role != "admin") {
          return arrow::Status::Invalid(
              "Only admin users may set gizmosql.bypass_queue = true");
        }
        s.bypass_queue = b;
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.session_tag",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .description = "JSON session tag recorded in instrumentation.",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.session_tag.empty() ? std::nullopt : std::optional(s.session_tag);
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        if (!val.empty() && !IsValidJSON(val)) {
          return arrow::Status::Invalid("Invalid JSON for session_tag: " + val);
        }
        s.session_tag = val;
#ifdef GIZMOSQL_ENTERPRISE
        if (s.instrumentation) {
          s.instrumentation->UpdateSessionTag(val);
        }
#endif
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.query_tag",
      .scope = SetScopeKind::kSessionOnly,
      .enterprise = true,
      .enterprise_feature = "instrumentation",
      .input_type = "VARCHAR",
      .description = "JSON query tag recorded in instrumentation.",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.query_tag.empty() ? std::nullopt : std::optional(s.query_tag);
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        if (!val.empty() && !IsValidJSON(val)) {
          return arrow::Status::Invalid("Invalid JSON for query_tag: " + val);
        }
        s.query_tag = val;
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_concurrent_statements",
      .scope = SetScopeKind::kGlobalOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_CONCURRENT_STATEMENTS",
      .default_value = "0",
      .description = "Max concurrently executing statements (0 = unlimited).",
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(std::to_string(srv.GetAdmissionController().Limit()));
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_concurrent_statements"));
        if (n < 0) return arrow::Status::Invalid("max_concurrent_statements must be >= 0");
        srv.GetAdmissionController().SetLimit(n);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_queued_statements",
      .scope = SetScopeKind::kGlobalOnly,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_QUEUED_STATEMENTS",
      .default_value = "0",
      .description = "Max statements that may wait for a slot (0 = unbounded).",
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(std::to_string(srv.GetAdmissionController().MaxQueued()));
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queued_statements"));
        srv.GetAdmissionController().SetMaxQueued(n);
        return arrow::Status::OK();
      },
  });

  settings_.push_back(GizmoSetting{
      .name = "gizmosql.max_queue_wait",
      .scope = SetScopeKind::kSessionOrGlobal,
      .enterprise = true,
      .enterprise_feature = "statement_queue",
      .input_type = "INTEGER",
      .env_var = "GIZMOSQL_MAX_QUEUE_WAIT",
      .default_value = "300",
      .description =
          "Seconds a statement may wait in the queue before rejection (0 = forever).",
      .get_session = [](const ClientSession& s) -> std::optional<std::string> {
        return s.max_queue_wait ? std::optional(std::to_string(*s.max_queue_wait))
                                : std::nullopt;
      },
      .get_global = [](DuckDBFlightSqlServer& srv,
                       const ClientSession&) -> std::optional<std::string> {
        return std::optional(
            std::to_string(srv.GetAdmissionController().DefaultMaxQueueWaitSeconds()));
      },
      .set_session = [](ClientSession& s, const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queue_wait"));
        s.max_queue_wait = n;
        return arrow::Status::OK();
      },
      .set_global = [](DuckDBFlightSqlServer& srv, ClientSession&,
                       const std::string& val) -> arrow::Status {
        ARROW_ASSIGN_OR_RAISE(int n, ParseSetInt(val, "max_queue_wait"));
        srv.GetAdmissionController().SetDefaultMaxQueueWaitSeconds(n);
        return arrow::Status::OK();
      },
  });

  for (size_t i = 0; i < settings_.size(); ++i) {
    by_name_[settings_[i].name] = i;
  }
}

const char* ScopeToString(SetScopeKind scope) {
  switch (scope) {
    case SetScopeKind::kSessionOnly:
      return "SESSION";
    case SetScopeKind::kGlobalOnly:
      return "GLOBAL";
    case SetScopeKind::kSessionOrGlobal:
      return "SESSION_OR_GLOBAL";
  }
  return "";
}

// Build the bind-parameterized VALUES that replaces a gizmosql_settings() call:
// "(VALUES (CAST(? AS ...),...),...) AS gizmosql_settings(<cols>)". Each value is
// appended to `binds` (row-major) and passed as a bound parameter — never
// interpolated — so JSON tags / descriptions can't break or inject SQL. The
// explicit CASTs fix the column types so GetSchema() works before binding.
std::string BuildGizmoSettingsValues(const ClientSession& session,
                                     DuckDBFlightSqlServer* server,
                                     duckdb::vector<duckdb::Value>& binds) {
  static constexpr const char* kRow =
      "(CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),"
      "CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),CAST(? AS VARCHAR),"
      "CAST(? AS BOOLEAN),CAST(? AS VARCHAR))";

  auto push = [&](const std::string& v) { binds.push_back(duckdb::Value(v)); };
  auto push_opt = [&](const std::optional<std::string>& v) {
    binds.push_back(v ? duckdb::Value(*v) : duckdb::Value());  // NULL when unset
  };

  std::string rows;
  const auto& settings = SettingsRegistry::Instance().All();
  for (size_t r = 0; r < settings.size(); ++r) {
    const GizmoSetting& d = settings[r];
    std::optional<std::string> sess = d.get_session ? d.get_session(session) : std::nullopt;
    std::optional<std::string> glob =
        (d.get_global && server) ? d.get_global(*server, session) : std::nullopt;
    std::string effective = sess.value_or(glob.value_or(d.default_value));

    push(d.name);                  // name
    push(effective);               // value (effective: session > global > default)
    push_opt(sess);                // session_value
    push_opt(glob);                // global_value
    push(ScopeToString(d.scope));  // scope
    push(d.input_type);            // input_type
    push(d.default_value);         // default_value
    push_opt(d.env_var.empty() ? std::optional<std::string>()
                               : std::optional<std::string>(d.env_var));  // env_var
    binds.push_back(duckdb::Value::BOOLEAN(d.enterprise));                // enterprise
    push(d.description);                                                  // description

    if (r) rows += ", ";
    rows += kRow;
  }

  return "(VALUES " + rows +
         ") AS gizmosql_settings(name, value, session_value, global_value, scope, "
         "input_type, default_value, env_var, enterprise, description)";
}

// Case-insensitively replace each "gizmosql_settings()" token with the
// bind-parameterized VALUES, appending its binds (in left-to-right order).
std::string RewriteGizmoSettings(const std::string& sql, const ClientSession& session,
                                 DuckDBFlightSqlServer* server,
                                 duckdb::vector<duckdb::Value>& binds) {
  static const std::string token = "gizmosql_settings()";
  const std::string lower = boost::algorithm::to_lower_copy(sql);
  std::string out;
  size_t pos = 0;
  while (true) {
    const size_t hit = lower.find(token, pos);
    if (hit == std::string::npos) {
      out += sql.substr(pos);
      break;
    }
    out += sql.substr(pos, hit - pos);
    out += BuildGizmoSettingsValues(session, server, binds);
    pos = hit + token.size();
  }
  return out;
}

}  // namespace

arrow::Status DuckDBStatement::HandleGizmoSQLSet() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

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

  if (!set_stmt.value) {
    return Status::Invalid("SET requires a value");
  }
  std::string val;
  if (auto* const_expr =
          dynamic_cast<duckdb::ConstantExpression*>(set_stmt.value.get())) {
    val = const_expr->value.ToString();
  } else {
    // Bare keyword/identifier values (notably `= true` / `= false`) are not
    // represented as a ConstantExpression in DuckDB's grammar; fall back to the
    // parsed expression's textual form so booleans work without quoting.
    val = set_stmt.value->ToString();
  }

  if (!boost::istarts_with(name, "gizmosql.")) {
    return arrow::Status::Invalid("Unsupported GizmoSQL parameter: " + name);
  }

  // Dispatch through the settings registry (single source of truth). It performs
  // the enterprise-license, scope-validity, and GLOBAL-admin checks centrally and
  // invokes the per-setting mutator hook.
  ARROW_RETURN_NOT_OK(SettingsRegistry::Instance().Apply(
      *session, GetServer(*session).get(), name, scope, val));

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
                                 const std::shared_ptr<arrow::Schema>& override_schema,
                                 bool is_internal,
                                 std::string flight_method) {
  client_session_ = client_session;
  session_id_ = client_session->session_id;
  statement_id_ = handle;
  stmt_ = stmt;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(stmt->query);
  use_direct_execution_ = false;
  log_level_ = log_level;
  is_internal_ = is_internal;
  flight_method_ = std::move(flight_method);
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = stmt->context;
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (auto trace_ids = GetCurrentTraceCorrelationIds()) {
    creation_trace_id_ = trace_ids->trace_id;
    creation_span_id_ = trace_ids->span_id;
  }
#endif
}

DuckDBStatement::DuckDBStatement(const std::shared_ptr<ClientSession>& client_session,
                                 const std::string& handle, const std::string& sql,
                                 const std::optional<arrow::util::ArrowLogLevel>& log_level,
                                 const bool& log_queries,
                                 const std::shared_ptr<arrow::Schema>& override_schema,
                                 bool is_internal,
                                 std::string flight_method) {
  client_session_ = client_session;
  session_id_ = client_session->session_id;
  statement_id_ = handle;
  sql_ = sql;
  log_queries_ = log_queries;
  logged_sql_ = redact_sql_for_logs(sql);
  use_direct_execution_ = true;
  stmt_ = nullptr;
  log_level_ = log_level;
  is_internal_ = is_internal;
  flight_method_ = std::move(flight_method);
  start_time_ = std::chrono::steady_clock::now();
  override_schema_ = override_schema;
  query_result_ = nullptr;
  client_context_ = client_session->connection->Get().context;
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (auto trace_ids = GetCurrentTraceCorrelationIds()) {
    creation_trace_id_ = trace_ids->trace_id;
    creation_span_id_ = trace_ids->span_id;
  }
#endif
}

arrow::Result<int> DuckDBStatement::Execute() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string execute_status;

  ARROW_ASSIGN_OR_RAISE(auto query_timeout, GetQueryTimeout());
  // Threshold: session/server log level gates whether messages are emitted
  ARROW_ASSIGN_OR_RAISE(auto log_threshold, GetSessionOrServerLogLevel(session));
  // Display severity: statement's own level, defaulting to INFO for user queries
  auto log_level =
      log_level_.value_or(arrow::util::ArrowLogLevel::ARROW_INFO);
  start_time_ = std::chrono::steady_clock::now();

  const std::string metric_operation =
      is_gizmosql_admin_
          ? "ADMIN"
          : GetSqlOperationForMetrics(
                use_direct_execution_ ? sql_ : (stmt_ ? stmt_->query : sql_));
  auto record_query_metric = [this, &metric_operation](const std::string& status_label) {
    end_time_ = std::chrono::steady_clock::now();
    ::gizmosql::metrics::RecordQueryExecution(
        metric_operation, status_label, static_cast<double>(GetLastExecutionDurationMs()));
  };

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation execute_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  // Generate execution ID for tracing (matches instrumentation table)
  std::string execution_id = boost::uuids::to_string(boost::uuids::random_generator()());

#ifdef GIZMOSQL_ENTERPRISE
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
    if (auto server = GetServer(*session)) {
      if (auto mgr = server->GetInstrumentationManager()) {
        execution_instrumentation_ = std::make_unique<ExecutionInstrumentation>(
            mgr, execution_id, statement_id_, bind_params_str);
      }
    }
  }

  // Configure DuckDB query profiling on this connection's ClientContext. We only
  // bother when there is an instrumentation record to store the profile into.
  // Profiling state is per-ClientContext and re-applied every Execute(), so a
  // session toggling the setting on/off is self-correcting. Capture is harvested
  // synchronously after execution (below), before the next statement on this
  // connection clobbers the profiler.
  //
  // Cross-version note (LTS v1.4 + stable v1.5): we touch only the ClientConfig
  // flags that exist in both channels and never reassign profiler_settings or call
  // MetricsUtils (whose API differs by version) — the default metric set is
  // version-correct, and enable_detailed_profiling is what enriches the tree.
  query_profile_mode_ = QueryProfileMode::kOff;
  if (execution_instrumentation_ && client_context_) {
    query_profile_mode_ = GetSessionOrServerCaptureProfile(session);
    auto& cfg = duckdb::ClientConfig::GetConfig(*client_context_);
    if (query_profile_mode_ == QueryProfileMode::kOff) {
      cfg.enable_profiler = false;
      cfg.enable_detailed_profiling = false;
    } else {
      cfg.enable_profiler = true;
      cfg.emit_profiler_output = false;  // no console/file output; harvested via ToJSON()
      cfg.profiler_print_format = duckdb::ProfilerPrintFormat::JSON;
      cfg.enable_detailed_profiling =
          (query_profile_mode_ == QueryProfileMode::kDetailed);
    }
  }
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::Execute", execute_status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"execution_id", execution_id},
      {"timeout_seconds", std::to_string(query_timeout)},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (is_gizmosql_admin_) {
    // If we have a synthetic result (e.g., from KILL SESSION), execution is already done
    if (synthetic_result_batch_) {
#ifdef GIZMOSQL_ENTERPRISE
      // Mark execution as complete for admin commands
      if (execution_instrumentation_) {
        execution_instrumentation_->SetCompleted(0);
      }
#endif
      record_query_metric("OK");
      execute_status = "success";
      return 0;
    }
    auto set_status = HandleGizmoSQLSet();
    if (!set_status.ok()) {
#ifdef GIZMOSQL_ENTERPRISE
      // Mark execution as failed for SET command errors
      if (execution_instrumentation_) {
        execution_instrumentation_->SetError(set_status.ToString());
      }
#endif
      record_query_metric(set_status.CodeAsString());
      execute_status = "failure";
      return set_status;
    }
#ifdef GIZMOSQL_ENTERPRISE
    // Mark execution as complete for successful SET commands
    if (execution_instrumentation_) {
      execution_instrumentation_->SetCompleted(0);
    }
#endif
    record_query_metric("OK");
    execute_status = "success";
    return 0;
  }

  // ---- Statement-queue admission control (Enterprise; fails open) ----------
  // Acquire a concurrency slot for the duration of execution. DuckDB materializes
  // results inside Execute(), so this scope covers the heavy CPU/memory work; the
  // cheap FetchResult() iteration afterward needs no slot. Internal/metadata
  // queries are exempt. Without an enterprise license (or when the cap is 0),
  // Acquire() returns an inert handle (unlimited concurrency). The slot is held
  // until this function returns, covering the success, timeout, and error paths.
  // admission_server is declared first so it outlives admission_slot (the slot's
  // destructor releases back into the controller the server owns).
  std::shared_ptr<DuckDBFlightSqlServer> admission_server;
  gizmosql::AdmissionSlot admission_slot;
  {
    bool enforce_queue = false;
#ifdef GIZMOSQL_ENTERPRISE
    enforce_queue =
        !is_internal_ && !session->bypass_queue.value_or(false) &&
        gizmosql::enterprise::EnterpriseFeatures::Instance().IsFeatureAvailable(
            gizmosql::enterprise::kFeatureStatementQueue);
#endif
    if (enforce_queue) {
      admission_server = GetServer(*session);
      if (admission_server) {
        auto& controller = admission_server->GetAdmissionController();
        const int32_t max_queue_wait =
            session->max_queue_wait.value_or(controller.DefaultMaxQueueWaitSeconds());
#ifdef GIZMOSQL_ENTERPRISE
        // Record the queued phase (status='queued', enqueue_time) so the admin
        // SQL-monitor can show in-flight queued statements and the queue wait.
        if (execution_instrumentation_) {
          execution_instrumentation_->SetQueued();
        }
#endif
        // Pass an abort predicate so that if this session is killed while the
        // statement is queued, KILL SESSION's WakeWaiters() lets it abandon the
        // queue immediately (Cancelled) instead of waiting for a slot it will
        // never use.
        auto slot_result = controller.Acquire(
            /*enforce=*/true, max_queue_wait,
            /*is_aborted=*/[kr = &session->kill_requested] { return kr->load(); });
        if (!slot_result.ok()) {
          // Cancelled => the session was killed while we were queued: record a
          // cancellation and surface a Flight CANCELLED. Otherwise the queue was
          // full or the wait elapsed: surface a retriable Flight UNAVAILABLE so
          // clients can back off and retry.
          const bool killed =
              slot_result.status().code() == arrow::StatusCode::Cancelled;
#ifdef GIZMOSQL_ENTERPRISE
          if (execution_instrumentation_) {
            if (killed) {
              execution_instrumentation_->SetCancelled();
            } else {
              execution_instrumentation_->SetError(slot_result.status().message());
            }
          }
#endif
          return arrow::flight::MakeFlightError(
              killed ? arrow::flight::FlightStatusCode::Cancelled
                     : arrow::flight::FlightStatusCode::Unavailable,
              slot_result.status().message());
        }
        admission_slot = std::move(slot_result).ValueOrDie();
        // A kill can race the slot grant: if we were killed just as we were
        // admitted, don't execute the doomed statement — record it cancelled and
        // bail (the slot releases via admission_slot's destructor on return).
        if (session->kill_requested.load()) {
#ifdef GIZMOSQL_ENTERPRISE
          if (execution_instrumentation_) {
            execution_instrumentation_->SetCancelled();
          }
#endif
          return arrow::flight::MakeFlightError(
              arrow::flight::FlightStatusCode::Cancelled,
              "Statement cancelled: session was killed");
        }
#ifdef GIZMOSQL_ENTERPRISE
        // Slot acquired: queued -> executing (restarts the execution clock).
        if (execution_instrumentation_) {
          execution_instrumentation_->SetRunning();
        }
#endif
      }
    }
  }

  // Capture the current runtime context so trace/log correlation is preserved
  // when statement execution runs on the async worker thread.
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  auto telemetry_context = opentelemetry::context::RuntimeContext::GetCurrent();
#endif

  // Launch execution in a separate thread
  auto future = std::async(
      std::launch::async, [this, session, query_timeout, log_threshold, log_level
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
                           ,
                           telemetry_context,
                           statement_trace_id = creation_trace_id_,
                           statement_span_id = creation_span_id_
#endif
  ]() -> arrow::Result<int> {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
        ScopedLogCorrelation async_log_correlation(statement_trace_id, statement_span_id);
        auto telemetry_context_token =
            opentelemetry::context::RuntimeContext::Attach(telemetry_context);
        (void)telemetry_context_token;
#endif
        if (use_direct_execution_) {
          // The statement may have already been executed from the ComputeSchema() method - if so, just skip execution
          if (query_result_ != nullptr) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
                  log_threshold, log_level,
                  session,
                  "Direct execution of the SQL command has already occurred, skipping "
                  "re-execution",
                  {"kind", "sql"}, {"status", "already-executed"},
                  {"statement_id", statement_id_}, {"query_timeout", query_timeout},
                  {"is_internal", is_internal_ ? "true" : "false"},
                  {"flight_method", flight_method_});
            }
            return 0;  // Success
          }
          if (!bind_parameters.empty()) {
            session->active_sql_handle = "";
            return arrow::Status::Invalid(
                "Direct query execution does not support bind parameters");
          }

          auto result = session->connection->Get().Query(sql_);

          session->active_sql_handle = "";

          if (result->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION(
                  WARNING, session, "Client SQL command failed direct execution",
                  {"kind", "sql"}, {"status", "failure"},
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

            GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
                log_threshold, log_level,
                session, "Executing prepared statement with bind parameters",
                {"kind", "sql"}, {"status", "executing"},
                {"statement_id", statement_id_}, {"bind_parameters", params_str.str()},
                {"param_count", std::to_string(bind_parameters.size())},
                {"query_timeout", std::to_string(query_timeout)},
                {"is_internal", is_internal_ ? "true" : "false"},
                {"flight_method", flight_method_});
          }

          query_result_ = stmt_->Execute(bind_parameters);

          session->active_sql_handle = "";

          if (query_result_->HasError()) {
            if (log_queries_) {
              GIZMOSQL_LOGKV_SESSION(
                  WARNING, session, "Client SQL command failed execution",
                  {"kind", "sql"}, {"status", "failure"},
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
      GIZMOSQL_LOGKV_SESSION(WARNING, session, "Client SQL command timed out - begin statement interruption",
                     {"kind", "sql"}, {"status", "timeout"},
                     {"interruption_status", "begin"},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

    // Timeout occurred - interrupt the query
    session->connection->Get().Interrupt();

    // Now wait for the background thread to finish cleanly
    future.wait();

    session->active_sql_handle = "";

    if (log_queries_) {
      GIZMOSQL_LOGKV_SESSION(WARNING, session, "Client SQL command timed out - completed statement interruption",
                     {"kind", "sql"}, {"status", "timeout"},
                     {"interruption_status", "end"},
                     {"statement_id", statement_id_},
                     {"timeout_seconds", std::to_string(query_timeout)},
                     {"sql", logged_sql_});
    }

#ifdef GIZMOSQL_ENTERPRISE
    // Record timeout in instrumentation
    if (execution_instrumentation_) {
      execution_instrumentation_->SetTimeout();
    }
#endif
    record_query_metric("TIMEOUT");
    execute_status = "timeout";

    return arrow::Status::ExecutionError("Query execution timed out after ",
                                         std::to_string(timeout_duration.count()),
                                         " seconds");
  }

  // Get the result from the future
  auto result = future.get();

  end_time_ = std::chrono::steady_clock::now();

#ifdef GIZMOSQL_ENTERPRISE
  // Record success or error in instrumentation
  // Note: rows_fetched will be updated incrementally during FetchResult calls in the batch reader,
  // and the final record will be written when the ExecutionInstrumentation is destroyed
  if (execution_instrumentation_) {
    if (result.ok()) {
      // Harvest the DuckDB query profile while we still hold this connection and
      // before the next statement clobbers the per-ClientContext profiler. The
      // JSON is DuckDB's native profiling format, stored verbatim in
      // sql_executions.query_profile. Best-effort: never fail a query because
      // profiling threw, and only when capture is enabled for this execution.
      if (query_profile_mode_ != QueryProfileMode::kOff && client_context_) {
        try {
          std::string profile_json =
              duckdb::QueryProfiler::Get(*client_context_).ToJSON();
          execution_instrumentation_->SetQueryProfile(std::move(profile_json));
        } catch (const std::exception& ex) {
          GIZMOSQL_LOGKV_SESSION(WARNING, session,
                                 "Failed to capture query profile",
                                 {"statement_id", statement_id_},
                                 {"execution_id", execution_id}, {"error", ex.what()});
        }
      }
      execution_instrumentation_->SetCompleted(GetLastExecutionDurationMs());
    } else if (session->kill_requested) {
      // The query was interrupted by KILL SESSION, not a genuine execution error.
      execution_instrumentation_->SetCancelled();
    } else {
      execution_instrumentation_->SetError(result.status().ToString());
    }
  }
#endif

  if (log_queries_ && result.ok()) {
    GIZMOSQL_LOGKV_SESSION_DYNAMIC_AT(
        log_threshold, log_level,
        session, "Client SQL command execution succeeded",
        {"kind", "sql"}, {"status", "success"}, {"statement_id", statement_id_},
        {"direct_execution", use_direct_execution_ ? "true" : "false"},
        {"duration_ms", GetLastExecutionDurationMs()}, {"sql", logged_sql_},
        {"is_internal", is_internal_ ? "true" : "false"},
        {"flight_method", flight_method_});
  }

  record_query_metric(result.ok() ? "OK" : result.status().CodeAsString());
  execute_status = result.ok() ? "success" : "failure";
  return result;
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DuckDBStatement::FetchResult() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation fetch_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::FetchResult", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
      {"direct_execution", use_direct_execution_ ? "true" : "false"});

  if (synthetic_result_batch_) {
    status = "success";
    auto batch = synthetic_result_batch_;
    synthetic_result_batch_.reset();
    if (batch && ::gizmosql::IsTelemetryEnabled()) {
      const auto batch_size_bytes = GetRecordBatchSizeBytes(batch);
      ::gizmosql::metrics::RecordBytesTransferred("outbound", batch_size_bytes);
      ::gizmosql::metrics::RecordRowsTransferred("outbound", batch->num_rows());
    }
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

  // RAII guard: release the exported ArrowSchema if we exit before
  // ImportRecordBatch() takes ownership (which consumes both res_schema and
  // res_arr via the C Data Interface).
  auto schema_guard = ::gizmosql::MakeScopeGuard([&res_schema]() {
    if (res_schema.release) {
      res_schema.release(&res_schema);
    }
  });

  duckdb::unique_ptr<duckdb::DataChunk> data_chunk;
  duckdb::ErrorData fetch_error;
  auto fetch_success = query_result_->TryFetch(data_chunk, fetch_error);
  if (!fetch_success) {
    return arrow::Status::ExecutionError(fetch_error.Message());
  }

  if (data_chunk != nullptr) {
    auto extension_type_cast = duckdb::ArrowTypeExtensionData::GetExtensionTypes(
        *client_context_, query_result_->types);
    duckdb::ArrowConverter::ToArrowArray(*data_chunk, &res_arr, res_options,
                                         extension_type_cast);
    ARROW_ASSIGN_OR_RAISE(record_batch, arrow::ImportRecordBatch(&res_arr, &res_schema));
    schema_guard.Dismiss();  // ownership transferred to record_batch

    GIZMOSQL_LOGKV_SESSION(DEBUG, session, "Client RecordBatch Fetch",
                   {"kind", "fetch"}, {"status", "success"},
                   {"statement_id", statement_id_},
                   {"num_rows", std::to_string(record_batch->num_rows())},
                   {"num_columns", std::to_string(record_batch->num_columns())},
                   {"sql", logged_sql_});
    if (::gizmosql::IsTelemetryEnabled()) {
      const auto record_batch_size_bytes = GetRecordBatchSizeBytes(record_batch);
      ::gizmosql::metrics::RecordBytesTransferred("outbound", record_batch_size_bytes);
      ::gizmosql::metrics::RecordRowsTransferred("outbound", record_batch->num_rows());
    }
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
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation execute_update_log_correlation(creation_trace_id_,
                                                      creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ExecuteUpdate", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
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
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation get_schema_log_correlation(creation_trace_id_, creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::GetSchema", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
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

std::string DuckDBStatement::GetSessionId() const {
  return session_id_;
}

arrow::Result<std::shared_ptr<arrow::Schema>> DuckDBStatement::ComputeSchema() {
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  std::string status;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  ScopedLogCorrelation compute_schema_log_correlation(creation_trace_id_,
                                                      creation_span_id_);
#endif

  GIZMOSQL_LOG_SCOPE_STATUS(
      DEBUG, "DuckDBStatement::ComputeSchema", status, {"peer", session->peer},
      {"session_id", session->session_id}, {"user", session->username},
      {"role", session->role}, {"statement_id", statement_id_},
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
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());

  // First, try getting the value from the user's session
  if (session->query_timeout.has_value()) {
    return session->query_timeout.value();
  }
  // Fall-back to the server's setting if the session setting is not set...
  if (auto server = GetServer(*session)) {
    return server->GetQueryTimeout(*session);
  }
  return arrow::Status::Invalid("Unable to get server instance");
}

arrow::Result<arrow::util::ArrowLogLevel> DuckDBStatement::GetLogLevel() const {
  if (log_level_.has_value()) {
    return log_level_.value();
  }
  ARROW_ASSIGN_OR_RAISE(auto session, GetSession());
  return GetSessionOrServerLogLevel(session);
}

arrow::Result<std::shared_ptr<ClientSession>> DuckDBStatement::GetSession() const {
  auto session = client_session_.lock();
  if (!session) return arrow::Status::Invalid("Session expired");
  return session;
}

}  // namespace gizmosql::ddb
