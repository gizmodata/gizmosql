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

// Bridges the DuckDB C++ API differences between the DuckDB versions GizmoSQL
// builds against: 1.x (the LTS channel, v1.4) and 2.x (the stable channel).
//
// DuckDB 2.0 made most PreparedStatement / QueryResult members private behind
// accessors, folded MaterializedQueryResult/StreamQueryResult into a single
// QueryResult, and turned catalog/column names into duckdb::Identifier (which
// does not implicitly convert to std::string). Code that must compile on both
// goes through these helpers instead of #if-ing every call site.

#ifndef GIZMOSQL_DUCKDB_MAJOR_VERSION
#error "GIZMOSQL_DUCKDB_MAJOR_VERSION must be defined (set by CMakeLists.txt)"
#endif

#include <memory>
#include <regex>
#include <string>
#include <unordered_map>

#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#if GIZMOSQL_DUCKDB_MAJOR_VERSION < 2
#include <duckdb/main/prepared_statement_data.hpp>
#endif
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
#include <duckdb/common/enums/query_result_state.hpp>
#include <duckdb/main/query_result_stream.hpp>
#endif

namespace gizmosql::ddb::compat {

// ---- names -----------------------------------------------------------------

inline const std::string& Str(const std::string& name) { return name; }
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
inline const std::string& Str(const duckdb::Identifier& name) {
  return name.GetIdentifierName();
}
#endif

// A name as DuckDB's API takes it: duckdb::Identifier on 2.x (explicit
// construction from a runtime string), std::string on 1.x.
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
using Name = duckdb::Identifier;
#else
using Name = std::string;
#endif

// Plain-string copy of a list of names (result columns, ...), as
// duckdb::ArrowConverter::ToArrowSchema takes them.
template <class NAME>
duckdb::vector<std::string> Strs(const duckdb::vector<NAME>& names) {
  duckdb::vector<std::string> result;
  result.reserve(names.size());
  for (const auto& name : names) result.push_back(Str(name));
  return result;
}

// Copy of a name-keyed map with plain std::string keys (DuckDB 2.x keys
// StatementProperties::modified_databases / read_databases by Identifier).
template <class MAP>
std::unordered_map<std::string, typename MAP::mapped_type> StringKeyed(const MAP& map) {
  std::unordered_map<std::string, typename MAP::mapped_type> result;
  for (const auto& [name, value] : map) result.emplace(Str(name), value);
  return result;
}

// ---- prepared statements ---------------------------------------------------

inline const std::string& Query(const duckdb::PreparedStatement& stmt) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return stmt.GetQuery();
#else
  return stmt.query;
#endif
}

inline duckdb::shared_ptr<duckdb::ClientContext> Context(
    const duckdb::PreparedStatement& stmt) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return stmt.TryGetContext();
#else
  return stmt.context;
#endif
}

inline duckdb::idx_t NamedParameterCount(const duckdb::PreparedStatement& stmt) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return stmt.GetNamedParameterMap().size();
#else
  return stmt.named_param_map.size();
#endif
}

// The type of a prepared statement's parameter (1-based position as a string,
// or a name). False when DuckDB has no type for it yet (untyped placeholder).
inline bool TryGetParameterType(duckdb::PreparedStatement& stmt, const std::string& identifier,
                                duckdb::LogicalType& type) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return stmt.TryGetParameterType(duckdb::Identifier(identifier), type);
#else
  return stmt.data->TryGetType(identifier, type);
#endif
}

// Executes a prepared statement with its result fully materialized, so that
// ResultValue() / ResultRowCount() can read it.
inline duckdb::unique_ptr<duckdb::QueryResult> ExecuteMaterialized(
    duckdb::PreparedStatement& stmt, duckdb::vector<duckdb::Value>& values) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  // Every 2.x QueryResult materializes on demand (Collection(), RowCount()).
  return stmt.Execute(values);
#else
  return stmt.Execute(values, /*allow_stream_result=*/false);
#endif
}

// ---- query results ---------------------------------------------------------

// Value at (column, row) of a materialized result: one from Connection::Query()
// or ExecuteMaterialized().
inline duckdb::Value ResultValue(duckdb::QueryResult& result, duckdb::idx_t column,
                                 duckdb::idx_t row) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return result.Collection().GetValue(column, row);
#else
  return result.Cast<duckdb::MaterializedQueryResult>().GetValue(column, row);
#endif
}

inline duckdb::idx_t ResultRowCount(duckdb::QueryResult& result) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return result.RowCount();
#else
  return result.Cast<duckdb::MaterializedQueryResult>().RowCount();
#endif
}

inline const duckdb::vector<duckdb::LogicalType>& ResultTypes(
    const duckdb::QueryResult& result) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return result.GetTypes();
#else
  return result.types;
#endif
}

inline duckdb::vector<std::string> ResultNames(const duckdb::QueryResult& result) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return Strs(result.GetNames());
#else
  return result.names;
#endif
}

// Fetches the next chunk (nullptr once exhausted). Returns false and fills
// `error` if the query failed.
inline bool TryFetch(duckdb::QueryResult& result, duckdb::unique_ptr<duckdb::DataChunk>& chunk,
                     duckdb::ErrorData& error) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  if (!result.TryFetchOrError(chunk, error)) {
    // An error result fails without setting `error`.
    if (!error.HasError() && result.HasError()) error = result.GetErrorObject();
    return false;
  }
  return true;
#else
  return result.TryFetch(chunk, error);
#endif
}

// ---- SQL text -------------------------------------------------------------

// DuckDB 2.x's grammar takes a single identifier as a setting name
// (`SettingName <- Identifier`), so `SET [GLOBAL] gizmosql.x = ...` no longer
// parses as written. Returns `sql` with a leading SET/RESET's dotted setting
// name quoted ("gizmosql.x"), which every grammar accepts; unchanged on 1.x
// and for anything else.
inline std::string QuoteDottedSettingName(const std::string& sql) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  static const std::regex kDottedSetting(
      R"(^(\s*(?:SET|RESET)\s+(?:(?:GLOBAL|SESSION|LOCAL)\s+)?)([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)+)(?=\s*(?:=|\bTO\b|;|$)))",
      std::regex_constants::icase);
  return std::regex_replace(sql, kDottedSetting, "$1\"$2\"",
                            std::regex_constants::format_first_only);
#else
  return sql;
#endif
}

// ---- streaming statement results -----------------------------------------

// The result of a statement GizmoSQL runs and then hands to the client chunk by
// chunk. It streams whenever DuckDB lets it, so a large SELECT is never fully
// buffered in server memory.
//
// DuckDB 1.x: PreparedStatement::Execute() streams by default.
// DuckDB 2.x: Execute() runs to completion and materializes the whole result;
// streaming needs Submit() plus a QueryResultStream, which DuckDB only allows
// for statements that do not complete before returning their result (DML,
// DDL, ... are FORCED and stay materialized).
class StatementResult {
 public:
  // Wraps an already-produced result (e.g. Connection::Query()).
  explicit StatementResult(duckdb::unique_ptr<duckdb::QueryResult> result)
      : result_(std::move(result)) {}

  static std::unique_ptr<StatementResult> Execute(duckdb::PreparedStatement& stmt,
                                                  duckdb::vector<duckdb::Value>& values) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto submitted = stmt.Submit(values);
    if (submitted && !submitted->HasError() &&
        submitted->GetStatementProperties().result_eagerness !=
            duckdb::ResultEagerness::FORCED &&
        submitted->HasBufferedData()) {
      auto streaming = std::unique_ptr<StatementResult>(new StatementResult(nullptr));
      streaming->types_ = submitted->GetTypes();
      streaming->names_ = Strs(submitted->GetNames());
      streaming->time_zone_ = submitted->client_properties.time_zone;
      streaming->stream_ =
          duckdb::make_uniq<duckdb::QueryResultStream<duckdb::ChunkFormat>>(
              std::move(submitted));
      // Like 1.x's Execute(), run the query here until its first chunk is
      // buffered (or it finishes / fails), so the heavy lifting of e.g. an
      // aggregate happens inside the caller's execute call — where GizmoSQL
      // polls for client cancellation and the query timeout and interrupts
      // the connection — rather than later inside a DoGet fetch. An
      // interrupt ends this loop with EXECUTION_ERROR (recorded on the stream).
      auto& stream = *streaming->stream_;
      while (true) {
        auto state = stream.ExecuteTask();
        if (duckdb::IsObservable(state)) break;
        if (state == duckdb::QueryResultState::BLOCKED ||
            state == duckdb::QueryResultState::NO_TASKS_AVAILABLE) {
          stream.WaitForTask();
        }
      }
      return streaming;
    }
    return std::make_unique<StatementResult>(std::move(submitted));
#else
    return std::make_unique<StatementResult>(stmt.Execute(values));
#endif
  }

  bool HasError() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) return stream_->HasError();
#endif
    return result_->HasError();
  }

  const std::string& GetError() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) return stream_->GetError();
#endif
    return result_->GetError();
  }

  const duckdb::vector<duckdb::LogicalType>& Types() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) return types_;
#endif
    return ResultTypes(*result_);
  }

  duckdb::vector<std::string> Names() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) return names_;
#endif
    return ResultNames(*result_);
  }

  // The session time zone the result's TIMESTAMPTZ values are rendered in.
  const std::string& TimeZone() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) return time_zone_;
#endif
    return result_->client_properties.time_zone;
  }

  // Next chunk, or nullptr once exhausted. False (with `error`) on failure.
  bool TryFetch(duckdb::unique_ptr<duckdb::DataChunk>& chunk, duckdb::ErrorData& error) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (stream_) {
      try {
        chunk = stream_->Fetch();
      } catch (std::exception& ex) {
        error = duckdb::ErrorData(ex);
        return false;
      }
      if (stream_->HasError()) {
        error = stream_->GetErrorObject();
        return false;
      }
      return true;
    }
#endif
    return compat::TryFetch(*result_, chunk, error);
  }

 private:
  duckdb::unique_ptr<duckdb::QueryResult> result_;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  duckdb::unique_ptr<duckdb::QueryResultStream<duckdb::ChunkFormat>> stream_;
  duckdb::vector<duckdb::LogicalType> types_;
  duckdb::vector<std::string> names_;
  std::string time_zone_;
#endif
};

// ---- parse tree ------------------------------------------------------------

// The value a parsed constant stands for. DuckDB 2.x keeps parsed constants as
// unbound literals (kind + text); 1.x stored the Value directly.
inline duckdb::Value ConstantValue(const duckdb::ConstantExpression& constant) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return constant.GetLiteral().ToValue();
#else
  return constant.value;
#endif
}

}  // namespace gizmosql::ddb::compat
