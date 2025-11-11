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

#include <cstdint>
#include <vector>
#include <string>
#include <unordered_map>

#include <duckdb.hpp>
#include <arrow/flight/sql/types.h>
#include <arrow/util/config.h>

#include "duckdb_sql_info.h"
#include "duckdb_server.h"
#include "flight_sql_fwd.h"

namespace sql = flight::sql;

namespace gizmosql::ddb {
// Dynamic query versions that use the server instance (emulating DuckDB Java behavior)
std::vector<std::string> GetDynamicKeywords(const DuckDBFlightSqlServer* server) {
  auto result = server->ExecuteSqlAndGetStringVector(
      "SELECT keyword_name FROM duckdb_keywords() ORDER BY keyword_name");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  // If query fails, return empty vector like Java version would
  return {};
}

std::vector<std::string> GetDynamicNumericFunctions(const DuckDBFlightSqlServer* server) {
  auto result = server->ExecuteSqlAndGetStringVector(
      "SELECT function_name FROM duckdb_functions() WHERE function_type = 'scalar' "
      "AND return_type SIMILAR TO '%(INTEGER|BIGINT|DOUBLE|FLOAT|DECIMAL|NUMERIC|REAL)%' "
      "ORDER BY function_name");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  // If query fails, return empty vector like Java version would
  return {};
}

std::vector<std::string> GetDynamicStringFunctions(const DuckDBFlightSqlServer* server) {
  auto result = server->ExecuteSqlAndGetStringVector(
      "SELECT function_name FROM duckdb_functions() WHERE function_type = 'scalar' "
      "AND return_type SIMILAR TO '%(VARCHAR|TEXT|STRING)%' "
      "ORDER BY function_name");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  // If query fails, return empty vector like Java version would
  return {};
}

std::vector<std::string> GetDynamicSystemFunctions(const DuckDBFlightSqlServer* server) {
  auto result = server->ExecuteSqlAndGetStringVector(
      "SELECT function_name FROM duckdb_functions() WHERE function_type = 'scalar' "
      "AND (function_name LIKE '%CURRENT%' OR function_name LIKE '%SESSION%' OR "
      "function_name LIKE '%USER%' OR function_name = 'VERSION') "
      "ORDER BY function_name");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  // If query fails, return empty vector like Java version would
  return {};
}

std::vector<std::string> GetDynamicDateTimeFunctions(
    const DuckDBFlightSqlServer* server) {
  auto result = server->ExecuteSqlAndGetStringVector(
      "SELECT function_name FROM duckdb_functions() WHERE function_type = 'scalar' "
      "AND return_type SIMILAR TO '%(DATE|TIME|TIMESTAMP|TIMESTAMPTZ|INTERVAL)%' "
      "ORDER BY function_name");
  if (result.ok()) {
    return result.ValueOrDie();
  }
  // If query fails, return empty vector like Java version would
  return {};
}

// Static functions for original GetSqlInfoResultMap (backward compatibility)
std::vector<std::string> GetStaticNumericFunctions() {
  return {"ABS",       "ACOS",      "ASIN",  "ATAN",    "ATAN2",     "CEIL",
          "CEILING",   "COS",       "COT",   "DEGREES", "EXP",       "FLOOR",
          "GREATEST",  "LEAST",     "LN",    "LOG",     "LOG10",     "LOG2",
          "MOD",       "PI",        "POW",   "POWER",   "RADIANS",   "RANDOM",
          "ROUND",     "SIGN",      "SIN",   "SQRT",    "TAN",       "TRUNC",
          "CBRT",      "FACTORIAL", "GAMMA", "LGAMMA",  "NEXTAFTER", "SETSEED",
          "BIT_COUNT", "CHR",       "EVEN",  "XOR",     "@"};
}

std::vector<std::string> GetStaticStringFunctions() {
  return {"ASCII",
          "BIT_LENGTH",
          "CHAR_LENGTH",
          "CHARACTER_LENGTH",
          "CHR",
          "CONCAT",
          "CONCAT_WS",
          "FORMAT",
          "INITCAP",
          "INSTR",
          "LCASE",
          "LEFT",
          "LENGTH",
          "LIKE_ESCAPE",
          "LOWER",
          "LPAD",
          "LTRIM",
          "MD5",
          "NFC_NORMALIZE",
          "ORD",
          "POSITION",
          "PREFIX",
          "PRINTF",
          "REGEXP_EXTRACT",
          "REGEXP_FULL_MATCH",
          "REGEXP_MATCHES",
          "REGEXP_REPLACE",
          "REGEXP_SPLIT_TO_ARRAY",
          "REPEAT",
          "REPLACE",
          "REVERSE",
          "RIGHT",
          "RPAD",
          "RTRIM",
          "STRPOS",
          "SUBSTR",
          "SUBSTRING",
          "SUFFIX",
          "TRIM",
          "UCASE",
          "UNICODE",
          "UPPER",
          "BASE64",
          "FROM_BASE64",
          "TO_BASE64",
          "STRIP_ACCENTS",
          "STR_SPLIT",
          "STR_SPLIT_REGEX",
          "STRING_SPLIT",
          "STRING_SPLIT_REGEX",
          "STRING_TO_ARRAY",
          "EDITDIST3",
          "HAMMING",
          "JACCARD",
          "LEVENSHTEIN",
          "MISMATCHES",
          "CONTAINS",
          "NOT_LIKE_ESCAPE",
          "ARRAY_EXTRACT",
          "ARRAY_SLICE",
          "LIST_ELEMENT",
          "LIST_EXTRACT",
          "STRLEN"};
}

std::vector<std::string> GetStaticSystemFunctions() {
  return {"CURRENT_CATALOG", "CURRENT_DATABASE", "CURRENT_SCHEMA",
          "CURRENT_USER",    "SESSION_USER",     "USER",
          "VERSION"};
}

std::vector<std::string> GetStaticDateTimeFunctions() {
  return {
      "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME",  "LOCALTIMESTAMP",
      "NOW",          "TODAY",        "YESTERDAY",         "TOMORROW",   "DATE_PART",
      "DATE_TRUNC",   "EXTRACT",      "STRFTIME",          "STRPTIME",   "AGE",
      "DATE_ADD",     "DATE_SUB",     "DATEDIFF",          "DATEADD",    "DATESUB",
      "LAST_DAY",     "MONTHNAME",    "DAYNAME",           "DAYOFWEEK",  "DAYOFYEAR",
      "ISODOW",       "ISOYEAR",      "WEEKDAY",           "WEEKOFYEAR", "YEARWEEK"};
}

// clang-format off
/// \brief Gets the mapping from SQL info ids to SqlInfoResult instances.
/// Uses dynamic queries to DuckDB like the Java driver.
/// \param server The DuckDB server instance to use for dynamic queries
/// \return the cache.
sql::SqlInfoResultMap GetSqlInfoResultMap(const DuckDBFlightSqlServer* server,
  const int32_t& query_timeout_seconds) {
  using SqlInfo = sql::SqlInfoOptions::SqlInfo;
  using SqlInfoOptions = sql::SqlInfoOptions;
  using SqlInfoResult = sql::SqlInfoResult;

  // Create the base static metadata map
  sql::SqlInfoResultMap result_map = {
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_NAME,
       SqlInfoResult(std::string("gizmosql"))},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_VERSION,
       SqlInfoResult(std::string("duckdb " + std::string(duckdb_library_version())))},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_ARROW_VERSION,
       SqlInfoResult(std::string(ARROW_VERSION_STRING))},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_READ_ONLY, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SQL, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_SUBSTRAIT, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION,
       SqlInfoResult(SqlInfoOptions::SqlSupportedTransaction::
                         SQL_SUPPORTED_TRANSACTION_TRANSACTION)},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_CANCEL, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_DDL_CATALOG, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_DDL_SCHEMA, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_DDL_TABLE, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_CASE,
       SqlInfoResult(static_cast<int64_t>(SqlInfoOptions::SqlSupportedCaseSensitivity::
                                 SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
      {SqlInfoOptions::SqlInfo::SQL_IDENTIFIER_QUOTE_CHAR,
       SqlInfoResult(std::string("\""))},
      {SqlInfoOptions::SqlInfo::SQL_QUOTED_IDENTIFIER_CASE,
       SqlInfoResult(static_cast<int64_t>(SqlInfoOptions::SqlSupportedCaseSensitivity::
                                 SQL_CASE_SENSITIVITY_CASE_INSENSITIVE))},
      {SqlInfoOptions::SqlInfo::SQL_ALL_TABLES_ARE_SELECTABLE, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_NULL_ORDERING,
       SqlInfoResult(static_cast<int64_t>(SqlInfoOptions::SqlNullOrdering::SQL_NULLS_SORTED_AT_END))},
      // Default static values for functions - will be overridden by dynamic queries
      {SqlInfoOptions::SqlInfo::SQL_KEYWORDS, SqlInfoResult(std::vector<std::string>{})},
      {SqlInfoOptions::SqlInfo::SQL_NUMERIC_FUNCTIONS, SqlInfoResult(GetStaticNumericFunctions())},
      {SqlInfoOptions::SqlInfo::SQL_STRING_FUNCTIONS, SqlInfoResult(GetStaticStringFunctions())},
      {SqlInfoOptions::SqlInfo::SQL_SYSTEM_FUNCTIONS, SqlInfoResult(GetStaticSystemFunctions())},
      {SqlInfoOptions::SqlInfo::SQL_DATETIME_FUNCTIONS, SqlInfoResult(GetStaticDateTimeFunctions())},
      // Additional metadata for better Metabase compatibility
      {SqlInfoOptions::SqlInfo::SQL_SEARCH_STRING_ESCAPE, SqlInfoResult(std::string("\\"))},
      {SqlInfoOptions::SqlInfo::SQL_EXTRA_NAME_CHARACTERS, SqlInfoResult(std::string("$"))},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_COLUMN_ALIASING, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_NULL_PLUS_NULL_IS_NULL, SqlInfoResult(true)},
      // Type conversion support
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_CONVERT,
       SqlInfoResult(std::unordered_map<int32_t, std::vector<int32_t>>(
           {{SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_BIGINT,
             std::vector<int32_t>({
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_INTEGER,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_SMALLINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_DECIMAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_NUMERIC,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_REAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_FLOAT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_VARCHAR,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_CHAR})},
            {SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_INTEGER,
             std::vector<int32_t>({
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_BIGINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_SMALLINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_DECIMAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_NUMERIC,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_REAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_FLOAT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_VARCHAR,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_CHAR})},
            {SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_VARCHAR,
             std::vector<int32_t>({
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_CHAR,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_BIGINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_INTEGER,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_SMALLINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_DECIMAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_NUMERIC,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_REAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_FLOAT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_DATE,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_TIME,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_TIMESTAMP})},
            {SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_FLOAT,
             std::vector<int32_t>({
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_BIGINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_INTEGER,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_SMALLINT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_DECIMAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_NUMERIC,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_REAL,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_FLOAT,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_VARCHAR,
                 SqlInfoOptions::SqlSupportsConvert::SQL_CONVERT_CHAR})}}))},
      // Additional server capability information
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_BULK_INGESTION, SqlInfoResult(false)}, // DuckDB supports bulk insert, but we don't yet support it
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED, SqlInfoResult(true)}, // DuckDB supports transactions
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT, SqlInfoResult(query_timeout_seconds)},
      {SqlInfoOptions::SqlInfo::FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT, SqlInfoResult(static_cast<int32_t>(0))},
      // Critical transaction isolation level metadata for Metabase compatibility
      {SqlInfoOptions::SqlInfo::SQL_DEFAULT_TRANSACTION_ISOLATION,
       SqlInfoResult(static_cast<int64_t>(2))}, // TRANSACTION_READ_COMMITTED
      {SqlInfoOptions::SqlInfo::SQL_TRANSACTIONS_SUPPORTED, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS,
       SqlInfoResult(static_cast<int32_t>(14))}, // Support NONE, READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ (0b1110)
      {SqlInfoOptions::SqlInfo::SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED, SqlInfoResult(false)},

      // Database limit constants - matching DuckDB Java JDBC (all return 0 for no limit)
      {SqlInfoOptions::SqlInfo::SQL_MAX_BINARY_LITERAL_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_CHAR_LITERAL_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMN_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMNS_IN_GROUP_BY, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMNS_IN_INDEX, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMNS_IN_ORDER_BY, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMNS_IN_SELECT, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_COLUMNS_IN_TABLE, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_CONNECTIONS, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_CURSOR_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_INDEX_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_SCHEMA_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_PROCEDURE_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_CATALOG_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_ROW_SIZE, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_ROW_SIZE_INCLUDES_BLOBS, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_MAX_STATEMENT_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_STATEMENTS, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_TABLE_NAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_TABLES_IN_SELECT, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit
      {SqlInfoOptions::SqlInfo::SQL_MAX_USERNAME_LENGTH, SqlInfoResult(static_cast<int32_t>(0))}, // 0 means no limit

      // Additional schema and catalog metadata
      {SqlInfoOptions::SqlInfo::SQL_SCHEMA_TERM, SqlInfoResult(std::string("schema"))},
      {SqlInfoOptions::SqlInfo::SQL_CATALOG_TERM, SqlInfoResult(std::string("catalog"))},
      {SqlInfoOptions::SqlInfo::SQL_CATALOG_AT_START, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_PROCEDURE_TERM, SqlInfoResult(std::string("procedure"))},

      // Schema and catalog supported actions (bitmasks)
      {SqlInfoOptions::SqlInfo::SQL_SCHEMAS_SUPPORTED_ACTIONS, SqlInfoResult(static_cast<int32_t>(31))}, // All actions supported
      {SqlInfoOptions::SqlInfo::SQL_CATALOGS_SUPPORTED_ACTIONS, SqlInfoResult(static_cast<int32_t>(31))}, // All actions supported

      // Additional boolean support flags commonly required by JDBC tools
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_TABLE_CORRELATION_NAMES, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_ORDER_BY_UNRELATED, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_NON_NULLABLE_COLUMNS, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_SELECT_FOR_UPDATE_SUPPORTED, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_STORED_PROCEDURES_SUPPORTED, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_CORRELATED_SUBQUERIES_SUPPORTED, SqlInfoResult(true)},
      {SqlInfoOptions::SqlInfo::SQL_BATCH_UPDATES_SUPPORTED, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_SAVEPOINTS_SUPPORTED, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_NAMED_PARAMETERS_SUPPORTED, SqlInfoResult(false)},
      {SqlInfoOptions::SqlInfo::SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED, SqlInfoResult(false)},

      // Grammar and subquery support (integer bitmasks)
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_GROUP_BY, SqlInfoResult(static_cast<int32_t>(2))}, // GROUP_BY_BEYOND_SELECT
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_GRAMMAR, SqlInfoResult(static_cast<int32_t>(3))}, // ANSI92_FULL
      {SqlInfoOptions::SqlInfo::SQL_ANSI92_SUPPORTED_LEVEL, SqlInfoResult(static_cast<int32_t>(3))}, // ANSI92_FULL
      {SqlInfoOptions::SqlInfo::SQL_OUTER_JOINS_SUPPORT_LEVEL, SqlInfoResult(static_cast<int32_t>(3))}, // FULL_OUTER
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_SUBQUERIES, SqlInfoResult(static_cast<int32_t>(15))}, // All subquery types
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_UNIONS, SqlInfoResult(static_cast<int32_t>(3))}, // UNION and UNION ALL
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_RESULT_SET_TYPES, SqlInfoResult(static_cast<int32_t>(1))}, // FORWARD_ONLY
      {SqlInfoOptions::SqlInfo::SQL_SUPPORTED_POSITIONED_COMMANDS, SqlInfoResult(static_cast<int32_t>(0))}, // None supported

      // Additional keys required by Arrow Flight SQL JDBC to prevent NPEs
      {SqlInfoOptions::SqlInfo::SQL_LOCATORS_UPDATE_COPY, SqlInfoResult(false)}
  };

  // Override with dynamically queried values - emulating DuckDB Java behavior exactly
  if (server != nullptr) {
    // Dynamic keyword lookup
    result_map[SqlInfoOptions::SqlInfo::SQL_KEYWORDS] =
        SqlInfoResult(GetDynamicKeywords(server));

    // Dynamic numeric functions
    result_map[SqlInfoOptions::SqlInfo::SQL_NUMERIC_FUNCTIONS] =
        SqlInfoResult(GetDynamicNumericFunctions(server));

    // Dynamic string functions
    result_map[SqlInfoOptions::SqlInfo::SQL_STRING_FUNCTIONS] =
        SqlInfoResult(GetDynamicStringFunctions(server));

    // Dynamic system functions
    result_map[SqlInfoOptions::SqlInfo::SQL_SYSTEM_FUNCTIONS] =
        SqlInfoResult(GetDynamicSystemFunctions(server));

    // Dynamic datetime functions
    result_map[SqlInfoOptions::SqlInfo::SQL_DATETIME_FUNCTIONS] =
        SqlInfoResult(GetDynamicDateTimeFunctions(server));
  }

  return result_map;
}

// clang-format on
}  // namespace gizmosql::ddb