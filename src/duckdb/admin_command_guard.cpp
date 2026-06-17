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

#include "admin_command_guard.h"

#include <algorithm>
#include <array>
#include <functional>
#include <unordered_set>

#include <duckdb.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/statement/load_statement.hpp>
#include <duckdb/parser/statement/call_statement.hpp>
#include <duckdb/parser/statement/export_statement.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>

#include <arrow/flight/types.h>

#include "flight_sql_fwd.h"

namespace gizmosql::ddb {

namespace {

namespace dd = duckdb;

std::string ToLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

// A path is "proven remote" only if it begins with an object-storage / network
// scheme we recognize. Anything else — a bare path, a relative path, file://,
// or a non-literal/computed path — is treated as LOCAL (and therefore gated for
// non-admins). Fail closed: when we cannot prove a path is remote, we gate it.
bool IsProvenRemotePath(const std::string& path_lower) {
  static const std::array<const char*, 14> kRemoteSchemes = {
      "s3://",   "s3a://",  "s3n://", "gs://",  "gcs://",    "r2://",   "az://",
      "azure://","abfs://", "abfss://","http://","https://", "hf://",   "remote://"};
  for (const char* scheme : kRemoteSchemes) {
    if (path_lower.rfind(scheme, 0) == 0) return true;
  }
  return false;
}

// Heuristic for replacement scans: `SELECT * FROM '/path/file.parquet'`. A bare,
// unqualified table reference whose name looks like a filesystem path or a
// data-file name. Used only for non-admin BASE_TABLE refs.
bool LooksLikeLocalFilePath(const std::string& name) {
  if (name.empty()) return false;
  const std::string low = ToLower(name);
  if (IsProvenRemotePath(low)) return false;  // remote replacement scan is allowed
  // Path separators or explicit local prefixes are a strong signal.
  if (name.find('/') != std::string::npos || name.find('\\') != std::string::npos) {
    return true;
  }
  if (low.rfind("file://", 0) == 0 || name.rfind("~", 0) == 0) return true;
  // Otherwise require a recognized data-file extension to avoid flagging a real
  // single-identifier table name.
  static const std::array<const char*, 16> kExts = {
      ".csv",  ".tsv",     ".txt",   ".parquet", ".pq",   ".json",  ".ndjson",
      ".jsonl",".arrow",   ".feather",".gz",      ".zst",  ".bz2",   ".xz",
      ".orc",  ".avro"};
  for (const char* ext : kExts) {
    const std::string e(ext);
    if (low.size() >= e.size() && low.compare(low.size() - e.size(), e.size(), e) == 0) {
      return true;
    }
  }
  return false;
}

// Table functions that read the local filesystem when given a local path. Gated
// only when the path argument is not a proven-remote URL.
const std::unordered_set<std::string>& FsReadFunctions() {
  static const std::unordered_set<std::string> kFns = {
      "read_csv",        "read_csv_auto",   "read_parquet",    "parquet_scan",
      "read_json",       "read_json_auto",  "read_json_objects","read_ndjson",
      "read_ndjson_auto","read_ndjson_objects","read_text",    "read_blob",
      "glob",            "sniff_csv",       "parquet_metadata","parquet_schema",
      "parquet_file_metadata","parquet_kv_metadata","read_text_auto"};
  return kFns;
}

// Table functions gated for non-admins regardless of arguments.
const std::unordered_set<std::string>& AlwaysGatedFunctions() {
  static const std::unordered_set<std::string> kFns = {"duckdb_secrets"};
  return kFns;
}

// Dangerous DuckDB settings that affect the whole instance. `SET GLOBAL x` /
// `RESET GLOBAL x` is gated for ANY setting; these are ALSO gated in bare form
// (`SET x = y`, scope AUTOMATIC) because a bare SET of a global-only setting
// changes it for every session. Common session-scoped settings (timezone,
// search_path, ...) are intentionally NOT here, so non-admins can still tune
// their own session with a bare SET.
bool IsDangerousGlobalSetting(const std::string& name_lower) {
  static const std::unordered_set<std::string> kSettings = {
      // resource limits (DoS surface)
      "memory_limit", "max_memory", "threads", "worker_threads", "external_threads",
      "temp_directory", "max_temp_directory_size",
      // external access / extensions
      "enable_external_access", "allow_unsigned_extensions", "allow_community_extensions",
      "allow_extensions_metadata_query", "autoinstall_known_extensions",
      "autoload_known_extensions", "custom_extension_repository",
      "autoinstall_extension_repository", "extension_directory",
      "enable_external_file_cache",
      // secrets / filesystem
      "secret_directory", "allow_persistent_secrets", "disabled_filesystems",
      "allowed_directories", "allowed_paths", "home_directory", "lock_configuration"};
  return kSettings.count(name_lower) > 0;
}

// Extract the first positional string-literal argument of a function call (the
// path, for read_* functions). Returns nullopt if the first argument is not a
// plain string constant (e.g. a list, a column, or a computed expression).
std::optional<std::string> FirstStringLiteralArg(const dd::FunctionExpression& fe) {
  if (fe.children.empty() || !fe.children[0]) return std::nullopt;
  const auto& child = *fe.children[0];
  if (child.GetExpressionClass() != dd::ExpressionClass::CONSTANT) return std::nullopt;
  const auto& ce = child.Cast<dd::ConstantExpression>();
  if (ce.value.IsNull()) return std::nullopt;
  if (ce.value.type().id() != dd::LogicalTypeId::VARCHAR) return std::nullopt;
  return ce.value.GetValue<std::string>();
}

// Extract a literal path from a COPY/EXPORT target (info.file_path, or a string
// constant in info.file_path_expression). nullopt => non-literal/unknown.
std::optional<std::string> CopyPathLiteral(const dd::CopyInfo& info) {
  if (!info.file_path.empty()) return info.file_path;
  if (info.file_path_expression &&
      info.file_path_expression->GetExpressionClass() == dd::ExpressionClass::CONSTANT) {
    const auto& ce = info.file_path_expression->Cast<dd::ConstantExpression>();
    if (!ce.value.IsNull() && ce.value.type().id() == dd::LogicalTypeId::VARCHAR) {
      return ce.value.GetValue<std::string>();
    }
  }
  return std::nullopt;
}

// Searches a parsed query tree for a gated table function (read_*/duckdb_secrets)
// or a local-file replacement scan. DuckDB's ParsedExpressionIterator does the
// structural recursion (descending joins, subqueries, set-ops, and CTE
// definitions, invoking our ref-callback on every TableRef); our callbacks only
// CHECK — they must never re-invoke the enumerators on the same node, or they
// self-recurse forever (EnumerateTableRefChildren calls ref_callback on the ref
// itself). Subquery EXPRESSIONS are the one thing the node enumerator does not
// descend, so CheckExpr handles those. Sets `violation` to the first category
// found and short-circuits thereafter.
struct GatedFunctionWalker {
  std::optional<std::string> violation;

  // Drive a full walk of a query node. Safe to call recursively (only on
  // genuinely-nested subquery expressions, which are finite and acyclic).
  void WalkNode(dd::QueryNode& node) {
    if (violation) return;
    dd::ParsedExpressionIterator::EnumerateQueryNodeChildren(
        node,
        [this](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) CheckExpr(*child);
        },
        [this](dd::TableRef& ref) { CheckRef(ref); });
  }

  // Check one TableRef (called by the enumerator on every ref in the tree). No
  // recursion here.
  void CheckRef(dd::TableRef& ref) {
    if (violation) return;
    if (ref.type == dd::TableReferenceType::TABLE_FUNCTION) {
      auto& tf = ref.Cast<dd::TableFunctionRef>();
      CheckTableFunction(tf);
      // A table-in/out function's subquery argument is not enumerated for us.
      if (!violation && tf.subquery && tf.subquery->node) WalkNode(*tf.subquery->node);
    } else if (ref.type == dd::TableReferenceType::BASE_TABLE) {
      auto& bt = ref.Cast<dd::BaseTableRef>();
      if (bt.catalog_name.empty() && bt.schema_name.empty() &&
          LooksLikeLocalFilePath(bt.table_name)) {
        violation = "reading a local file via FROM '" + bt.table_name + "'";
      }
    }
  }

  // Check one expression, descending into any nested subquery expressions
  // (e.g. WHERE id IN (SELECT ... read_csv(...))).
  void CheckExpr(dd::ParsedExpression& expr) {
    if (violation) return;
    if (expr.GetExpressionClass() == dd::ExpressionClass::SUBQUERY) {
      auto& sub = expr.Cast<dd::SubqueryExpression>();
      if (sub.subquery && sub.subquery->node) WalkNode(*sub.subquery->node);
    }
    if (violation) return;
    dd::ParsedExpressionIterator::EnumerateChildren(
        expr, [this](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) CheckExpr(*child);
        });
  }

  void CheckTableFunction(const dd::TableFunctionRef& tf) {
    if (!tf.function || tf.function->GetExpressionClass() != dd::ExpressionClass::FUNCTION) {
      return;
    }
    const auto& fe = tf.function->Cast<dd::FunctionExpression>();
    const std::string name = ToLower(fe.function_name);
    if (AlwaysGatedFunctions().count(name)) {
      violation = name + "()";
      return;
    }
    if (FsReadFunctions().count(name)) {
      auto path = FirstStringLiteralArg(fe);
      if (path && IsProvenRemotePath(ToLower(*path))) return;  // remote read allowed
      violation = name + "() (local filesystem)";
    }
  }
};

// Walk the embedded query/select of a statement for gated table functions.
std::optional<std::string> WalkStatementForGatedFunctions(dd::SQLStatement& stmt) {
  GatedFunctionWalker walker;
  switch (stmt.type) {
    case dd::StatementType::SELECT_STATEMENT: {
      auto& s = stmt.Cast<dd::SelectStatement>();
      if (s.node) walker.WalkNode(*s.node);
      break;
    }
    case dd::StatementType::INSERT_STATEMENT: {
      auto& s = stmt.Cast<dd::InsertStatement>();
      if (s.select_statement && s.select_statement->node) {
        walker.WalkNode(*s.select_statement->node);
      }
      break;
    }
    case dd::StatementType::CREATE_STATEMENT: {
      auto& s = stmt.Cast<dd::CreateStatement>();
      if (s.info && s.info->type == dd::CatalogType::TABLE_ENTRY) {
        auto& ti = s.info->Cast<dd::CreateTableInfo>();
        if (ti.query && ti.query->node) walker.WalkNode(*ti.query->node);
      }
      break;
    }
    default:
      break;
  }
  return walker.violation;
}

}  // namespace

std::optional<std::string> ClassifyGatedCommand(const std::string& sql) {
  dd::Parser parser;
  try {
    parser.ParseQuery(sql);
  } catch (...) {
    // Unparseable here — let DuckDB surface the real parse error during
    // preparation. (The standalone parser uses the same core grammar as the
    // engine for the gated statement types.)
    return std::nullopt;
  }

  for (auto& stmt_ptr : parser.statements) {
    if (!stmt_ptr) continue;
    dd::SQLStatement& stmt = *stmt_ptr;

    switch (stmt.type) {
      case dd::StatementType::ATTACH_STATEMENT:
        return "ATTACH";
      case dd::StatementType::DETACH_STATEMENT:
        return "DETACH";
      case dd::StatementType::LOAD_STATEMENT: {
        auto& ls = stmt.Cast<dd::LoadStatement>();
        const bool is_load = ls.info && ls.info->load_type == dd::LoadType::LOAD;
        return is_load ? "LOAD extension" : "INSTALL extension";
      }
      case dd::StatementType::SET_STATEMENT: {
        auto& ss = stmt.Cast<dd::SetStatement>();
        const std::string verb =
            ss.set_type == dd::SetType::RESET ? "RESET GLOBAL " : "SET GLOBAL ";
        if (ss.scope == dd::SetScope::GLOBAL) {
          return verb + ss.name;  // explicit GLOBAL: any setting
        }
        // Bare SET/RESET (scope AUTOMATIC) of a dangerous global-only setting
        // changes it for the whole instance — gate it too. Explicit SESSION /
        // LOCAL, and bare SET of harmless session settings, are allowed.
        if (ss.scope == dd::SetScope::AUTOMATIC &&
            IsDangerousGlobalSetting(ToLower(ss.name))) {
          return verb + ss.name;
        }
        break;
      }
      case dd::StatementType::CALL_STATEMENT: {
        // CHECKPOINT / FORCE CHECKPOINT are parsed as CALL checkpoint() /
        // CALL force_checkpoint().
        auto& call = stmt.Cast<dd::CallStatement>();
        if (call.function &&
            call.function->GetExpressionClass() == dd::ExpressionClass::FUNCTION) {
          const std::string fname =
              ToLower(call.function->Cast<dd::FunctionExpression>().function_name);
          if (fname == "checkpoint" || fname == "force_checkpoint") return "CHECKPOINT";
        }
        break;  // other CALL functions allowed
      }
      case dd::StatementType::COPY_STATEMENT: {
        auto& cs = stmt.Cast<dd::CopyStatement>();
        if (cs.info) {
          auto path = CopyPathLiteral(*cs.info);
          const bool remote = path && IsProvenRemotePath(ToLower(*path));
          if (!remote) {
            return cs.info->is_from ? "COPY FROM (local filesystem)"
                                    : "COPY TO (local filesystem)";
          }
          // Remote COPY target is allowed, but a COPY (SELECT read_csv(...)) TO
          // may still read the local filesystem — inspect the embedded query.
          if (cs.info->select_statement) {
            GatedFunctionWalker walker;
            walker.WalkNode(*cs.info->select_statement);
            if (walker.violation) return walker.violation;
          }
        }
        break;
      }
      case dd::StatementType::EXPORT_STATEMENT: {
        auto& es = stmt.Cast<dd::ExportStatement>();
        auto path = es.info ? CopyPathLiteral(*es.info) : std::nullopt;
        const bool remote = path && IsProvenRemotePath(ToLower(*path));
        if (!remote) return "EXPORT DATABASE (local filesystem)";
        break;
      }
      default:
        break;
    }

    // Embedded read_*/duckdb_secrets in SELECT / CTAS / INSERT ... SELECT.
    if (auto v = WalkStatementForGatedFunctions(stmt)) return v;
  }

  return std::nullopt;
}

arrow::Status CheckNonAdminCommandAllowed(const std::string& sql) {
  auto category = ClassifyGatedCommand(sql);
  if (!category) return arrow::Status::OK();
  return flight::MakeFlightError(
      flight::FlightStatusCode::Unauthorized,
      "Permission denied: " + *category +
          " requires the 'admin' role. This GizmoSQL instance restricts "
          "filesystem- and instance-level commands to admin users.");
}

}  // namespace gizmosql::ddb
