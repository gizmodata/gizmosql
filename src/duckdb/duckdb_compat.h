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
#include <mutex>
#include <regex>
#include <string>
#include <unordered_map>

#include <duckdb.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parser.hpp>
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

// ---- parsed statements -----------------------------------------------------

inline const std::string& TypeName(const duckdb::CreateTypeInfo& info) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return Str(info.GetTypeName());
#else
  return info.name;
#endif
}

// ---- parser ----------------------------------------------------------------

// A parser with DuckDB's built-in grammar and no client context. DuckDB 2.x
// (from v2.0.0-alpha46057) dropped Parser's default constructor for
// Parser::GetBuiltinParser().
inline duckdb::Parser StandaloneParser() {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return duckdb::Parser::GetBuiltinParser();
#else
  return duckdb::Parser();
#endif
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

// ---- settings --------------------------------------------------------------

// A memory_limit setting in bytes; an unlimited limit is the largest idx_t.
// DuckDB 2.x's ParseMemoryLimit returns an empty optional_idx for unlimited;
// 1.x returned DConstants::INVALID_INDEX (the same largest idx_t).
inline duckdb::idx_t ParseMemoryLimit(const std::string& setting) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return duckdb::DBConfig::ParseMemoryLimitOrMaximum(setting);
#else
  return duckdb::DBConfig::ParseMemoryLimit(setting);
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
// streaming needs Submit() plus a QueryResultStream. Only a SELECT that returns
// rows and writes nothing streams (duckdb::StreamsByDefault with
// reads_to_the_end = false): a client can stop reading a result early, and a
// statement abandoned mid-stream is rolled back, so DML, DDL and a SELECT that
// writes (e.g. a data-modifying CTE) stay materialized and run to completion.
//
// DuckDB 2.x also refuses any new statement on a connection while a stream is
// open on it ("connection has an open result"), where 1.x closed the earlier
// result. GizmoSQL keeps the 1.x behavior: a stream registers itself as its
// connection's open result, and EndOpenResult() (called before every new
// statement on a session connection, see TrackedDuckDBConnection::
// ForStatement()) closes it; a later fetch from it then fails.
class StatementResult;
inline void EndOpenResult(duckdb::Connection& connection);

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
namespace detail {
// A 2.x statement's result while it may hold its connection. Shared between
// the StatementResult and the open-result registry, so EndOpenResult() can
// close it without holding the registry lock (lock order: registry, then
// `mutex`, never both at once).
struct StatementStream {
  // Held while the statement is submitted and driven to its first chunk, and
  // while a chunk is fetched, so closing it waits for those.
  std::mutex mutex;
  duckdb::unique_ptr<duckdb::QueryResultStream<duckdb::ChunkFormat>> stream;
  bool closed = false;
};

struct OpenResultRegistry {
  std::mutex mutex;
  std::unordered_map<const duckdb::ClientContext*, std::weak_ptr<StatementStream>>
      by_context;
};

inline OpenResultRegistry& OpenResults() {
  static OpenResultRegistry registry;
  return registry;
}
}  // namespace detail
#endif

class StatementResult {
 public:
  // Wraps an already-produced result (e.g. Connection::Query()).
  explicit StatementResult(duckdb::unique_ptr<duckdb::QueryResult> result)
      : result_(std::move(result)) {}

  StatementResult(const StatementResult&) = delete;
  StatementResult& operator=(const StatementResult&) = delete;

  ~StatementResult() {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (!state_ || !context_) return;
    auto& registry = detail::OpenResults();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.by_context.find(context_);
    if (it != registry.by_context.end() && it->second.lock() == state_) {
      registry.by_context.erase(it);
    }
#endif
  }

  static std::unique_ptr<StatementResult> Execute(duckdb::PreparedStatement& stmt,
                                                  duckdb::vector<duckdb::Value>& values) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto result = std::unique_ptr<StatementResult>(new StatementResult(nullptr));
    result->state_ = std::make_shared<detail::StatementStream>();
    // Registered before Submit(): from then on the result may hold the
    // connection, and a later statement must be able to close it.
    if (auto context = stmt.TryGetContext()) {
      result->context_ = context.get();
      auto& registry = detail::OpenResults();
      std::lock_guard<std::mutex> lock(registry.mutex);
      registry.by_context[result->context_] = result->state_;
    }
    std::lock_guard<std::mutex> stream_lock(result->state_->mutex);
    auto submitted = stmt.Submit(values);
    if (submitted && !submitted->HasError() &&
        duckdb::StreamsByDefault(stmt.GetStatementType(), *submitted,
                                 /*reads_to_the_end=*/false) &&
        submitted->HasBufferedData()) {
      result->types_ = submitted->GetTypes();
      result->names_ = Strs(submitted->GetNames());
      result->time_zone_ = submitted->client_properties.time_zone;
      result->state_->stream =
          duckdb::make_uniq<duckdb::QueryResultStream<duckdb::ChunkFormat>>(
              std::move(submitted));
      // Like 1.x's Execute(), run the query here until its first chunk is
      // buffered (or it finishes / fails), so the heavy lifting of e.g. an
      // aggregate happens inside the caller's execute call — where GizmoSQL
      // polls for client cancellation and the query timeout and interrupts
      // the connection — rather than later inside a DoGet fetch. An
      // interrupt ends this loop with EXECUTION_ERROR (recorded on the stream).
      auto& stream = *result->state_->stream;
      while (true) {
        auto state = stream.ExecuteTask();
        if (duckdb::IsObservable(state)) break;
        if (state == duckdb::QueryResultState::BLOCKED ||
            state == duckdb::QueryResultState::NO_TASKS_AVAILABLE) {
          stream.WaitForTask();
        }
      }
      return result;
    }
    // Run it to completion now: a submitted result that is not drained holds
    // the connection (and, for DML, may not have run yet).
    if (submitted && !submitted->HasError()) submitted->Complete();
    result->result_ = std::move(submitted);
    return result;
#else
    return std::make_unique<StatementResult>(stmt.Execute(values));
#endif
  }

  bool HasError() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) return state_->stream->HasError();
#endif
    return result_->HasError();
  }

  const std::string& GetError() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) return state_->stream->GetError();
#endif
    return result_->GetError();
  }

  const duckdb::vector<duckdb::LogicalType>& Types() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) return types_;
#endif
    return ResultTypes(*result_);
  }

  duckdb::vector<std::string> Names() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) return names_;
#endif
    return ResultNames(*result_);
  }

  // The session time zone the result's TIMESTAMPTZ values are rendered in.
  const std::string& TimeZone() const {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) return time_zone_;
#endif
    return result_->client_properties.time_zone;
  }

  // Next chunk, or nullptr once exhausted. False (with `error`) on failure.
  bool TryFetch(duckdb::unique_ptr<duckdb::DataChunk>& chunk, duckdb::ErrorData& error) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (Streaming()) {
      std::lock_guard<std::mutex> lock(state_->mutex);
      if (state_->closed) {
        error = duckdb::ErrorData(
            duckdb::ExceptionType::INVALID_INPUT,
            "The result was closed because a later statement ran on the same session; a "
            "session streams one result at a time.");
        return false;
      }
      auto& stream = *state_->stream;
      try {
        chunk = stream.Fetch();
      } catch (std::exception& ex) {
        error = duckdb::ErrorData(ex);
        return false;
      }
      if (stream.HasError()) {
        error = stream.GetErrorObject();
        return false;
      }
      return true;
    }
#endif
    return compat::TryFetch(*result_, chunk, error);
  }

 private:
  friend void EndOpenResult(duckdb::Connection& connection);

  duckdb::unique_ptr<duckdb::QueryResult> result_;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  // A stream is set once Execute() returns, and never reset.
  bool Streaming() const { return state_ && state_->stream; }

  std::shared_ptr<detail::StatementStream> state_;
  // The connection this result is registered on (Execute() results only).
  const duckdb::ClientContext* context_ = nullptr;
  duckdb::vector<duckdb::LogicalType> types_;
  duckdb::vector<std::string> names_;
  std::string time_zone_;
#endif
};

// Closes the result an earlier statement left streaming on `connection`, so a
// new statement can run on it (DuckDB 1.x did this itself); a later fetch from
// that result fails. Call before running a statement on a session connection,
// never from Interrupt() paths: it waits for a submit or fetch in progress.
inline void EndOpenResult(duckdb::Connection& connection) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  std::shared_ptr<detail::StatementStream> state;
  {
    auto& registry = detail::OpenResults();
    std::lock_guard<std::mutex> lock(registry.mutex);
    auto it = registry.by_context.find(connection.context.get());
    if (it == registry.by_context.end()) return;
    state = it->second.lock();
    registry.by_context.erase(it);
  }
  if (!state) return;
  std::lock_guard<std::mutex> lock(state->mutex);
  // No stream: the statement ran to completion and holds nothing.
  if (state->stream && !state->closed) {
    state->stream->Close();
    state->closed = true;
  }
#else
  (void)connection;
#endif
}

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
