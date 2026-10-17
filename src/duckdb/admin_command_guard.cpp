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
#include <cctype>
#include <functional>
#include <unordered_set>

#include <duckdb.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/tableref/table_function_ref.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/showref.hpp>
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/function/scalar_macro_function.hpp>
#include <duckdb/function/table_macro_function.hpp>
#include <duckdb/parser/statement/drop_statement.hpp>
#include <duckdb/parser/statement/explain_statement.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/copy_statement.hpp>
#include <duckdb/parser/statement/set_statement.hpp>
#include <duckdb/parser/statement/load_statement.hpp>
#include <duckdb/parser/statement/call_statement.hpp>
#include <duckdb/parser/statement/pragma_statement.hpp>
#include <duckdb/parser/statement/prepare_statement.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
#include <duckdb/parser/query_node/copy_query_node.hpp>
#include <duckdb/parser/query_node/delete_query_node.hpp>
#include <duckdb/parser/query_node/merge_query_node.hpp>
#include <duckdb/parser/query_node/update_query_node.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/merge_into_statement.hpp>
#include <duckdb/parser/statement/update_statement.hpp>
#include <duckdb/parser/query_node/insert_query_node.hpp>
#include <duckdb/parser/query_node/recursive_cte_node.hpp>
#include <duckdb/parser/query_node/set_operation_node.hpp>
#include <duckdb/parser/expression/type_expression.hpp>
#endif

#include "duckdb_compat.h"

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

using compat::Str;

// ---- parse-tree accessors (DuckDB 2.x encapsulated the parsed-tree members) --

std::string FunctionNameOf(const dd::FunctionExpression& fe) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return Str(fe.FunctionName());
#else
  return fe.function_name;
#endif
}

// The first argument of a call, if it is positional (nullptr otherwise).
const dd::ParsedExpression* FirstPositionalArg(const dd::FunctionExpression& fe) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto& args = fe.GetArguments();
  if (args.empty() || args[0].HasName()) return nullptr;
  return &args[0].GetExpression();
#else
  if (fe.children.empty()) return nullptr;
  return fe.children[0].get();
#endif
}

dd::SelectStatement* SubqueryOf(dd::SubqueryExpression& sub) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return sub.SubqueryMutable().get();
#else
  return sub.subquery.get();
#endif
}

// For a FROM 'path' replacement scan: the table name when it is unqualified.
std::optional<std::string> UnqualifiedTableName(const dd::BaseTableRef& bt) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto& name = bt.GetQualifiedName();
  if (name.Path().size() != 1) return std::nullopt;
  return Str(name.Name());
#else
  if (!bt.catalog_name.empty() || !bt.schema_name.empty()) return std::nullopt;
  return bt.table_name;
#endif
}

// A string constant's text; nullopt for anything else (NULL, numbers, ...).
std::optional<std::string> StringConstant(const dd::ParsedExpression& expr) {
  if (expr.GetExpressionClass() != dd::ExpressionClass::CONSTANT) return std::nullopt;
  const auto value = compat::ConstantValue(expr.Cast<dd::ConstantExpression>());
  if (value.IsNull() || value.type().id() != dd::LogicalTypeId::VARCHAR) return std::nullopt;
  return value.GetValue<std::string>();
}

// Strict mode for the classification running on this thread (sessions under
// access control): no path counts as remote. Set for one ClassifyGatedCommand call.
thread_local bool t_strict = false;

// Categories this classification may let through (see ClassifyGatedCommand).
thread_local const std::function<bool(const std::string&)>* t_permitted = nullptr;

// How deep query('<sql>') nesting the classification on this thread has gone.
thread_local int t_nested_sql_depth = 0;

// `category`, unless the session may run it after all.
std::optional<std::string> Gate(std::string category) {
  if (t_permitted && *t_permitted && (*t_permitted)(category)) return std::nullopt;
  return category;
}

// "(local filesystem)", or under strict mode "(files and URLs)".
std::string PathKind() { return t_strict ? " (files and URLs)" : " (local filesystem)"; }

// A path is "proven remote" only if it begins with an object-storage / network
// scheme we recognize. Anything else — a bare path, a relative path, file://,
// or a non-literal/computed path — is treated as LOCAL (and therefore gated for
// non-admins). Fail closed: when we cannot prove a path is remote, we gate it.
bool IsProvenRemotePath(const std::string& path_lower) {
  if (t_strict) return false;
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
      "parquet_file_metadata","parquet_kv_metadata","read_text_auto",
      // DuckDB 2.0
      "read_single_csv_file", "read_single_json_file"};
  return kFns;
}

// Table functions gated for non-admins regardless of arguments.
const std::unordered_set<std::string>& AlwaysGatedFunctions() {
  static const std::unordered_set<std::string> kFns = {
      "duckdb_secrets",
      // Runs a statement serialized by json_serialize_sql(), which the parser
      // here only sees as a string.
      "json_execute_serialized_sql",
      // DuckDB 2.0 external resources. Their callbacks are named by string and
      // run on an internal connection, outside every check on the client's
      // statement: destroy_external_resource('query', '<sql>') executes <sql>
      // and returns its error text. The metadata functions are instance-wide.
      "create_external_resource", "destroy_external_resource",
      "register_external_resource", "register_external_resource_type",
      "deregister_external_resource", "duckdb_external_resources",
      "duckdb_external_resource_types", "duckdb_extension_repositories"};
  return kFns;
}

// DuckDB's Quack remote protocol (autoloadable): quack_serve() opens a network
// server inside this process, quack_query()/quack_cancel() reach other
// servers. None of it is for non-admin clients.
bool IsQuackFunction(const std::string& name_lower) {
  return name_lower.rfind("quack_", 0) == 0;
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
      "allowed_directories", "allowed_paths", "home_directory", "lock_configuration",
      // DuckDB 2.0: extension repositories (their public keys become trusted
      // extension-signing keys), extension search paths, and HTTP log redaction
      // (false would put other sessions' HTTP headers into duckdb_logs)
      "allow_extension_repositories", "extension_repository_directory",
      "extension_directories", "redact_http_logs"};
  return kSettings.count(name_lower) > 0;
}

// DuckDB 2.0 settings that switch the session's SQL grammar. This gate parses
// with the built-in grammar and lets through what it cannot parse, so a session
// with its own grammar could send SQL the gate cannot read but the engine runs.
// Gated in every scope (SESSION too); RESET, back to the built-in grammar, is
// allowed.
// Settings that make DuckDB write to a path the statement names (a query log,
// a profile, an HTTP log): a file write anywhere the server can write, so they
// are gated in every scope, SESSION too. RESET is allowed.
bool IsFileWritingSetting(const std::string& name_lower) {
  static const std::unordered_set<std::string> kSettings = {
      "log_query_path", "profiling_output", "http_logging_output"};
  return kSettings.count(name_lower) > 0;
}

bool IsGrammarSetting(const std::string& name_lower) {
  static const std::unordered_set<std::string> kSettings = {
      "current_dialect", "active_grammar_extensions", "enable_legacy_parser"};
  return kSettings.count(name_lower) > 0;
}

// A string literal, or a list of them ('a' or ['a', 'b']), as its strings;
// nullopt for anything else.
std::optional<std::vector<std::string>> StringConstants(
    const dd::ParsedExpression& expr) {
  if (auto one = StringConstant(expr)) return std::vector<std::string>{*one};
  if (expr.GetExpressionClass() == dd::ExpressionClass::FUNCTION) {
    const auto& fe = expr.Cast<dd::FunctionExpression>();
    if (ToLower(FunctionNameOf(fe)) != "list_value") return std::nullopt;
    std::vector<std::string> out;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    for (const auto& arg : fe.GetArguments()) {
      auto text = StringConstant(arg.GetExpression());
#else
    for (const auto& child : fe.children) {
      auto text = child ? StringConstant(*child) : std::nullopt;
#endif
      if (!text) return std::nullopt;
      out.push_back(*text);
    }
    return out;
  }
  if (expr.GetExpressionClass() == dd::ExpressionClass::CONSTANT) {
    const auto value = compat::ConstantValue(expr.Cast<dd::ConstantExpression>());
    if (value.IsNull() || value.type().id() != dd::LogicalTypeId::LIST)
      return std::nullopt;
    std::vector<std::string> out;
    for (const auto& child : dd::ListValue::GetChildren(value)) {
      if (child.IsNull() || child.type().id() != dd::LogicalTypeId::VARCHAR)
        return std::nullopt;
      out.push_back(child.GetValue<std::string>());
    }
    return out;
  }
  return std::nullopt;
}

// A permission check that lets nothing through.
const std::function<bool(const std::string&)>& AllowNothing() {
  static const std::function<bool(const std::string&)> nothing;
  return nothing;
}

// Extract the first positional string-literal argument of a function call (the
// path, for read_* functions). Returns nullopt if the first argument is not a
// plain string constant (e.g. a list, a column, or a computed expression).
std::optional<std::string> FirstStringLiteralArg(const dd::FunctionExpression& fe) {
  const auto* first = FirstPositionalArg(fe);
  if (!first) return std::nullopt;
  return StringConstant(*first);
}

// Extract a literal path from a COPY/EXPORT target (info.file_path, or a string
// constant in info.file_path_expression). nullopt => non-literal/unknown.
std::optional<std::string> CopyPathLiteral(const dd::CopyInfo& info) {
  if (!info.file_path.empty()) return info.file_path;
  if (info.file_path_expression) return StringConstant(*info.file_path_expression);
  return std::nullopt;
}

// COPY whose target is not a proven-remote path (COPY TO a local file, or any
// COPY FROM the local filesystem). nullopt => remote target, allowed.
std::optional<std::string> LocalCopyViolation(const dd::CopyInfo& info) {
  auto path = CopyPathLiteral(info);
  if (path && IsProvenRemotePath(ToLower(*path))) return std::nullopt;
  return std::string(info.is_from ? "COPY FROM" : "COPY TO") + PathKind();
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

  // Record a gated operation, unless the session may run it.
  void Flag(std::string category) { violation = Gate(std::move(category)); }

  // Drive a full walk of a query node. Safe to call recursively (only on
  // genuinely-nested subquery expressions, which are finite and acyclic).
  void WalkNode(dd::QueryNode& node) {
    if (violation) return;
    CheckNodeTree(node);
    if (violation) return;
    dd::ParsedExpressionIterator::EnumerateQueryNodeChildren(
        node,
        [this](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) CheckExpr(*child);
        },
        [this](dd::TableRef& ref) { CheckRef(ref); });
  }

  // DuckDB 2.x parses COPY TO and DML as query nodes, and a CTE body may be
  // any of them, so a SELECT can carry a COPY:
  //   WITH x AS (COPY t TO '/some/local/file') SELECT 1
  // The enumerator descends into these nodes (CTE bodies, set-op children,
  // INSERT/COPY sources) without reporting them, so follow the node-to-node
  // edges here and gate every COPY found, exactly like a top-level COPY.
  // Subqueries reached through refs/expressions come back in via CheckRef /
  // CheckExpr. No-op on DuckDB 1.x, whose COPY is statement-only.
  void CheckNodeTree(dd::QueryNode& node) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (violation) return;
    switch (node.type) {
      case dd::QueryNodeType::COPY_QUERY_NODE: {
        auto& copy = node.Cast<dd::CopyQueryNode>();
        if (!copy.info) {
          Flag("COPY");
          return;
        }
        if (auto v = LocalCopyViolation(*copy.info)) {
          Flag(*v);
          if (violation) return;
        }
        if (copy.info->select_statement) CheckNodeTree(*copy.info->select_statement);
        break;
      }
      case dd::QueryNodeType::SET_OPERATION_NODE:
        for (auto& child : node.Cast<dd::SetOperationNode>().children) {
          if (child) CheckNodeTree(*child);
        }
        break;
      case dd::QueryNodeType::RECURSIVE_CTE_NODE: {
        auto& rcte = node.Cast<dd::RecursiveCTENode>();
        if (rcte.left) CheckNodeTree(*rcte.left);
        if (rcte.right) CheckNodeTree(*rcte.right);
        break;
      }
      case dd::QueryNodeType::INSERT_QUERY_NODE: {
        auto& insert = node.Cast<dd::InsertQueryNode>();
        if (insert.select_statement && insert.select_statement->node) {
          CheckNodeTree(*insert.select_statement->node);
        }
        break;
      }
      default:
        break;
    }
    for (auto& entry : node.cte_map.map) {
      if (entry.second && entry.second->query_node) CheckNodeTree(*entry.second->query_node);
    }
#else
    (void)node;
#endif
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
      auto table_name = UnqualifiedTableName(ref.Cast<dd::BaseTableRef>());
      if (table_name && LooksLikeLocalFilePath(*table_name)) {
        Flag("reading a local file via FROM '" + *table_name + "'");
      }
    } else if (ref.type == dd::TableReferenceType::SHOW_REF) {
      // DESCRIBE / SUMMARIZE / SHOW bind (and so read) their query or table
      // when the statement is bound, before any later check.
      auto& show = ref.Cast<dd::ShowRef>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      const std::string shown = Str(show.GetTableName());
#else
      const std::string shown = show.table_name;
#endif
      if (LooksLikeLocalFilePath(shown)) {
        Flag("reading a local file via FROM '" + shown + "'");
        return;
      }
      if (show.query) WalkNode(*show.query);
    } else if (ref.type == dd::TableReferenceType::SUBQUERY) {
      // Its children were enumerated already; only the node edges are left.
      auto& sq = ref.Cast<dd::SubqueryRef>();
      if (sq.subquery && sq.subquery->node) CheckNodeTree(*sq.subquery->node);
    }
  }

  // Check one expression, descending into any nested subquery expressions
  // (e.g. WHERE id IN (SELECT ... read_csv(...))).
  void CheckExpr(dd::ParsedExpression& expr) {
    if (violation) return;
    if (expr.GetExpressionClass() == dd::ExpressionClass::FUNCTION) {
      const std::string name = ToLower(FunctionNameOf(expr.Cast<dd::FunctionExpression>()));
      if (IsQuackFunction(name)) {
        Flag(name + "()");
        return;
      }
    }
    if (expr.GetExpressionClass() == dd::ExpressionClass::SUBQUERY) {
      auto* subquery = SubqueryOf(expr.Cast<dd::SubqueryExpression>());
      if (subquery && subquery->node) WalkNode(*subquery->node);
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
    const std::string name = ToLower(FunctionNameOf(fe));
    if (AlwaysGatedFunctions().count(name) || IsQuackFunction(name)) {
      Flag(name + "()");
      return;
    }
    // query('<sql>') / query_table('<name>') bind SQL from a string inside
    // DuckDB, where the remote reads it may contain happen before any check.
    if (t_strict && (name == "query" || name == "query_table")) {
      Flag(name + "()");
      return;
    }
    // Otherwise the SQL or table names inside are checked like the statement
    // around them: to the parser they are only string literals, but DuckDB runs
    // them. What is built at run time cannot be checked, so it is gated.
    if (name == "query") {
      const auto* first = FirstPositionalArg(fe);
      const auto inner = first ? StringConstant(*first) : std::nullopt;
      if (!inner || t_nested_sql_depth >= 8) {
        Flag("query() with SQL that is not a string literal");
        return;
      }
      ++t_nested_sql_depth;
      auto nested = ClassifyGatedCommand(*inner, t_strict,
                                         t_permitted ? *t_permitted : AllowNothing());
      --t_nested_sql_depth;
      if (nested) violation = nested;
      return;
    }
    if (name == "query_table") {
      const auto* first = FirstPositionalArg(fe);
      auto names = first ? StringConstants(*first) : std::nullopt;
      if (!names) {
        Flag("query_table() with names that are not string literals");
        return;
      }
      for (const auto& table : *names) {
        if (LooksLikeLocalFilePath(table)) {
          Flag("reading a local file via query_table('" + table + "')");
          return;
        }
      }
      return;
    }
    if (FsReadFunctions().count(name)) {
      auto path = FirstStringLiteralArg(fe);
      if (path && IsProvenRemotePath(ToLower(*path))) return;  // remote read allowed
      Flag(name + "()" + PathKind());
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
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      if (s.node) walker.WalkNode(*s.node);
#else
      if (s.select_statement && s.select_statement->node) {
        walker.WalkNode(*s.select_statement->node);
      }
#endif
      break;
    }
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    // 2.x parses these as query nodes, so their FROM / USING sources walk
    // like a SELECT's (UPDATE t SET ... FROM read_csv('/local/file')).
    case dd::StatementType::UPDATE_STATEMENT: {
      auto& s = stmt.Cast<dd::UpdateStatement>();
      if (s.node) walker.WalkNode(*s.node);
      break;
    }
    case dd::StatementType::DELETE_STATEMENT: {
      auto& s = stmt.Cast<dd::DeleteStatement>();
      if (s.node) walker.WalkNode(*s.node);
      break;
    }
    case dd::StatementType::MERGE_INTO_STATEMENT: {
      auto& s = stmt.Cast<dd::MergeIntoStatement>();
      if (s.node) walker.WalkNode(*s.node);
      break;
    }
#endif
    case dd::StatementType::CREATE_STATEMENT: {
      auto& s = stmt.Cast<dd::CreateStatement>();
      if (s.info && s.info->type == dd::CatalogType::TABLE_ENTRY) {
        auto& ti = s.info->Cast<dd::CreateTableInfo>();
        if (ti.query && ti.query->node) walker.WalkNode(*ti.query->node);
      } else if (s.info && s.info->type == dd::CatalogType::VIEW_ENTRY) {
        // CREATE VIEW binds its query (reading what it names) to learn the
        // view's columns.
        auto& vi = s.info->Cast<dd::CreateViewInfo>();
        if (vi.query && vi.query->node) walker.WalkNode(*vi.query->node);
      } else if (s.info && (s.info->type == dd::CatalogType::MACRO_ENTRY ||
                            s.info->type == dd::CatalogType::TABLE_MACRO_ENTRY)) {
        // A macro's body runs when it is called, under the caller's checks only
        // as far as its name: FROM m() is not a file read, so check the body
        // (and its default parameter values) when it is created.
        auto& mi = s.info->Cast<dd::CreateMacroInfo>();
        for (auto& macro : mi.macros) {
          if (!macro) continue;
          if (macro->type == dd::MacroType::TABLE_MACRO) {
            auto& table = macro->Cast<dd::TableMacroFunction>();
            if (table.query_node) walker.WalkNode(*table.query_node);
          } else if (macro->type == dd::MacroType::SCALAR_MACRO) {
            auto& scalar = macro->Cast<dd::ScalarMacroFunction>();
            if (scalar.expression) walker.CheckExpr(*scalar.expression);
          }
          for (auto& entry : macro->default_parameters) {
            if (entry.second) walker.CheckExpr(*entry.second);
          }
        }
      }
      break;
    }
    default:
      break;
  }
  return walker.violation;
}

// Classify a single statement. Recurses into PREPARE so that
// `PREPARE p AS SELECT * FROM read_csv('/etc/passwd')` is gated at prepare time
// — a non-admin therefore cannot stage a gated statement and EXECUTE it later
// (prepared statements are per-session/connection). Recurses into EXPLAIN too:
// EXPLAIN ANALYZE executes the wrapped statement (COPY TO writes its file,
// SET GLOBAL changes the setting, ...).
std::optional<std::string> ClassifyStatement(dd::SQLStatement& stmt) {
    switch (stmt.type) {
      case dd::StatementType::PREPARE_STATEMENT: {
        auto& ps = stmt.Cast<dd::PrepareStatement>();
        if (ps.statement) {
          if (auto v = ClassifyStatement(*ps.statement)) return v;
        }
        break;
      }
      case dd::StatementType::EXPLAIN_STATEMENT: {
        auto& es = stmt.Cast<dd::ExplainStatement>();
        if (es.stmt) {
          if (auto v = ClassifyStatement(*es.stmt)) return v;
        }
        break;
      }
      case dd::StatementType::ATTACH_STATEMENT:
        return Gate("ATTACH");
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      // DuckDB 2.0: CONNECT attaches its target (a file path or a postgres:/
      // sqlite:/quack: URI) and then forwards the session's raw SQL to it,
      // past these AST checks and catalog permissions — so it is ATTACH-class.
      // EXTERNAL RESOURCE provisions compute; PASSTHROUGH is raw SQL for a
      // CONNECTed target.
      case dd::StatementType::CONNECT_STATEMENT:
        return Gate("CONNECT");
      case dd::StatementType::DISCONNECT_STATEMENT:
        return Gate("DISCONNECT");
      case dd::StatementType::EXTERNAL_RESOURCE_STATEMENT:
        return Gate("EXTERNAL RESOURCE");
      case dd::StatementType::PASSTHROUGH_STATEMENT:
        return Gate("passthrough SQL");
#endif
      case dd::StatementType::DETACH_STATEMENT:
        return Gate("DETACH");
      case dd::StatementType::CREATE_STATEMENT: {
        // CREATE SECRET (incl. OR REPLACE / PERSISTENT / TEMPORARY — all parse
        // as a CreateStatement with a SECRET_ENTRY catalog type). Secrets hold
        // credentials, so creating them is admin-only. Other CREATE statements
        // (incl. CTAS) fall through to the gated-function walk below.
        auto& cs = stmt.Cast<dd::CreateStatement>();
        if (cs.info && cs.info->type == dd::CatalogType::SECRET_ENTRY) {
          return Gate("CREATE SECRET");
        }
        break;
      }
      case dd::StatementType::DROP_STATEMENT: {
        // DROP SECRET (incl. IF EXISTS / PERSISTENT / TEMPORARY — all parse as a
        // DropStatement with a SECRET_ENTRY catalog type). Other DROPs allowed.
        auto& ds = stmt.Cast<dd::DropStatement>();
        if (ds.info && ds.info->type == dd::CatalogType::SECRET_ENTRY) {
          return Gate("DROP SECRET");
        }
        break;
      }
      case dd::StatementType::LOAD_STATEMENT: {
        auto& ls = stmt.Cast<dd::LoadStatement>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
        // CREATE / DROP EXTENSION REPOSITORY parse as LOAD statements.
        if (ls.info && ls.info->load_type == dd::LoadType::CREATE_REPOSITORY) {
          return Gate("CREATE EXTENSION REPOSITORY");
        }
        if (ls.info && ls.info->load_type == dd::LoadType::DROP_REPOSITORY) {
          return Gate("DROP EXTENSION REPOSITORY");
        }
#endif
        const bool is_load = ls.info && ls.info->load_type == dd::LoadType::LOAD;
        return Gate(is_load ? "LOAD extension" : "INSTALL extension");
      }
      case dd::StatementType::SET_STATEMENT: {
        auto& ss = stmt.Cast<dd::SetStatement>();
        const std::string verb =
            ss.set_type == dd::SetType::RESET ? "RESET GLOBAL " : "SET GLOBAL ";
        if (ss.scope == dd::SetScope::GLOBAL) {
          return Gate(verb + Str(ss.name));  // explicit GLOBAL: any setting
        }
        // Bare SET/RESET (scope AUTOMATIC) of a dangerous global-only setting
        // changes it for the whole instance — gate it too. Explicit SESSION /
        // LOCAL, and bare SET of harmless session settings, are allowed.
        if (ss.scope == dd::SetScope::AUTOMATIC &&
            IsDangerousGlobalSetting(ToLower(Str(ss.name)))) {
          return Gate(verb + Str(ss.name));
        }
        if (ss.set_type != dd::SetType::RESET &&
            IsFileWritingSetting(ToLower(Str(ss.name)))) {
          return Gate("SET " + Str(ss.name));
        }
        if (ss.set_type != dd::SetType::RESET &&
            IsGrammarSetting(ToLower(Str(ss.name)))) {
          return Gate("SET " + Str(ss.name));
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
              ToLower(FunctionNameOf(call.function->Cast<dd::FunctionExpression>()));
          if (fname == "checkpoint" || fname == "force_checkpoint")
            return Gate("CHECKPOINT");
          if (IsQuackFunction(fname) || AlwaysGatedFunctions().count(fname)) {
            return Gate(fname + "()");
          }
        }
        break;  // other CALL functions allowed
      }
      case dd::StatementType::COPY_STATEMENT: {
        auto& cs = stmt.Cast<dd::CopyStatement>();
        if (cs.info) {
          if (auto v = LocalCopyViolation(*cs.info)) {
            if (auto gated = Gate(*v)) return gated;
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
      case dd::StatementType::EXPORT_STATEMENT:
        // EXPORT DATABASE dumps the ENTIRE database (every schema/table) to a
        // location — full-database egress, NOT bounded by object grants — so it
        // is gated unconditionally, local or remote.
        return Gate("EXPORT DATABASE");
      case dd::StatementType::PRAGMA_STATEMENT: {
        // IMPORT DATABASE is parsed as `PRAGMA import_database('dir')`. It runs
        // arbitrary DDL + DML from a dump (and reads the filesystem), so it is
        // gated unconditionally, local or remote. Other pragmas are allowed.
        auto& ps = stmt.Cast<dd::PragmaStatement>();
        if (ps.info && ToLower(Str(ps.info->name)) == "import_database") {
          return Gate("IMPORT DATABASE");
        }
        break;
      }
      default:
        break;
    }

    // Embedded read_*/duckdb_secrets in SELECT / CTAS / INSERT ... SELECT.
    if (auto v = WalkStatementForGatedFunctions(stmt)) return v;
    return std::nullopt;
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
// DuckDB 2.x keeps a cast's target as an unresolved type expression.
bool IsBooleanType(const dd::TypeExpression& type) {
  const auto& name = type.GetQualifiedName();
  if (name.Path().size() != 1) return false;  // a qualified (user) type
  const std::string lower = ToLower(Str(name.Name()));
  return lower == "boolean" || lower == "bool" || lower == "logical";
}
#else
bool IsBooleanType(const dd::LogicalType& type) {
#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
  // DuckDB 1.5+ leaves parsed type names unbound until binding.
  if (type.id() == dd::LogicalTypeId::UNBOUND) {
    try {
      return dd::UnboundType::TryDefaultBind(type).id() == dd::LogicalTypeId::BOOLEAN;
    } catch (...) {
      return false;
    }
  }
#endif
  return type.id() == dd::LogicalTypeId::BOOLEAN;
}
#endif

// True only when a SET value is provably false. DuckDB evaluates the value and
// casts it to BOOLEAN, so true, 1, 'yes', 't', NOT false, (true) all enable the
// setting; anything that is not a false constant is treated as an attempt to
// enable it (fail closed).
bool ExpressionIsFalse(const dd::ParsedExpression* value) {
  if (!value) return false;
  if (value->GetExpressionClass() == dd::ExpressionClass::CAST) {
    // DuckDB parses the keyword false as CAST('f' AS BOOLEAN).
    const auto& cast = value->Cast<dd::CastExpression>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    if (!IsBooleanType(cast.TargetType())) return false;
    return ExpressionIsFalse(&cast.Child());
#else
    if (!IsBooleanType(cast.cast_type)) return false;
    return ExpressionIsFalse(cast.child.get());
#endif
  }
  if (value->GetExpressionClass() != dd::ExpressionClass::CONSTANT) return false;
  const auto constant = compat::ConstantValue(value->Cast<dd::ConstantExpression>());
  if (constant.IsNull()) return false;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  auto cast = constant.DefaultTryCastAs(dd::LogicalType::BOOLEAN);
  if (!cast) return false;
  const dd::Value as_bool = *cast;
#else
  dd::Value as_bool;
  std::string error;
  if (!constant.DefaultTryCastAs(dd::LogicalType::BOOLEAN, as_bool, &error)) return false;
#endif
  return !as_bool.IsNull() && !as_bool.GetValue<bool>();
}

std::optional<std::string> ClassifyUnredactedSecretsStatement(dd::SQLStatement& stmt) {
  if (stmt.type == dd::StatementType::PREPARE_STATEMENT) {
    auto& ps = stmt.Cast<dd::PrepareStatement>();
    if (ps.statement) return ClassifyUnredactedSecretsStatement(*ps.statement);
    return std::nullopt;
  }
  if (stmt.type == dd::StatementType::EXPLAIN_STATEMENT) {
    // EXPLAIN ANALYZE executes the wrapped statement.
    auto& es = stmt.Cast<dd::ExplainStatement>();
    if (es.stmt) return ClassifyUnredactedSecretsStatement(*es.stmt);
    return std::nullopt;
  }
  if (stmt.type != dd::StatementType::SET_STATEMENT) return std::nullopt;
  auto& ss = stmt.Cast<dd::SetStatement>();
  if (ss.set_type == dd::SetType::RESET) return std::nullopt;
  if (ToLower(Str(ss.name)) != "allow_unredacted_secrets") return std::nullopt;
  auto& set_value = stmt.Cast<dd::SetVariableStatement>();
  if (ExpressionIsFalse(set_value.value.get())) return std::nullopt;
  return "SET allow_unredacted_secrets = true";
}

// A SET (or PRAGMA x = ...) of `name`, also inside PREPARE / EXPLAIN. RESET
// is not a match.
bool IsSetOf(dd::SQLStatement& stmt, const std::string& name) {
  if (stmt.type == dd::StatementType::PREPARE_STATEMENT) {
    auto& ps = stmt.Cast<dd::PrepareStatement>();
    return ps.statement && IsSetOf(*ps.statement, name);
  }
  if (stmt.type == dd::StatementType::EXPLAIN_STATEMENT) {
    auto& es = stmt.Cast<dd::ExplainStatement>();
    return es.stmt && IsSetOf(*es.stmt, name);
  }
  if (stmt.type != dd::StatementType::SET_STATEMENT) return false;
  auto& ss = stmt.Cast<dd::SetStatement>();
  return ss.set_type != dd::SetType::RESET && ToLower(Str(ss.name)) == name;
}

bool ContainsCaseInsensitive(const std::string& haystack,
                             const std::string& lower_needle) {
  return std::search(haystack.begin(), haystack.end(), lower_needle.begin(),
                     lower_needle.end(), [](unsigned char a, unsigned char b) {
                       return std::tolower(a) == b;
                     }) != haystack.end();
}

}  // namespace

std::optional<std::string> ClassifyGatedCommand(
    const std::string& sql, bool strict,
    const std::function<bool(const std::string& category)>& permitted) {
  struct StrictScope {
    StrictScope(bool on, const std::function<bool(const std::string&)>* allow)
        : previous(t_strict), previous_permitted(t_permitted) {
      t_strict = on;
      t_permitted = allow;
    }
    ~StrictScope() {
      t_strict = previous;
      t_permitted = previous_permitted;
    }
    bool previous;
    const std::function<bool(const std::string&)>* previous_permitted;
  } strict_scope(strict, &permitted);
  auto parser = compat::StandaloneParser();
  try {
    // A dotted setting name (SET GLOBAL gizmosql.x) must still classify on
    // DuckDB 2.x, whose grammar only parses it quoted.
    parser.ParseQuery(compat::QuoteDottedSettingName(sql));
  } catch (...) {
    // Unparseable here — let DuckDB surface the real parse error during
    // preparation. (The standalone parser uses the same core grammar as the
    // engine for the gated statement types.)
    return std::nullopt;
  }

  for (auto& stmt_ptr : parser.statements) {
    if (!stmt_ptr) continue;
    if (auto v = ClassifyStatement(*stmt_ptr)) return v;
  }
  return std::nullopt;
}

std::optional<std::string> ClassifyUnredactedSecretsSet(const std::string& sql) {
  // Every client statement comes through here, so skip the parse unless the
  // setting name appears. DuckDB's parser rejects U&"..." identifier escapes,
  // so the name cannot be spelled any other way.
  static const std::string kName = "allow_unredacted_secrets";
  auto it =
      std::search(sql.begin(), sql.end(), kName.begin(), kName.end(),
                  [](unsigned char a, unsigned char b) { return std::tolower(a) == b; });
  if (it == sql.end()) return std::nullopt;

  auto parser = compat::StandaloneParser();
  try {
    parser.ParseQuery(sql);
  } catch (...) {
    return std::nullopt;
  }
  for (auto& stmt_ptr : parser.statements) {
    if (!stmt_ptr) continue;
    if (auto v = ClassifyUnredactedSecretsStatement(*stmt_ptr)) return v;
  }
  return std::nullopt;
}

bool IsSecretDirectorySet(const std::string& sql) {
  static const std::string kName = "secret_directory";
  if (!ContainsCaseInsensitive(sql, kName)) return false;
  auto parser = compat::StandaloneParser();
  try {
    parser.ParseQuery(sql);
  } catch (...) {
    return false;
  }
  for (auto& stmt_ptr : parser.statements) {
    if (stmt_ptr && IsSetOf(*stmt_ptr, kName)) return true;
  }
  return false;
}

std::string GatedCommandDeniedMessage(const std::string& category) {
  return "Permission denied: GizmoSQL blocked " + category +
         ", which requires the 'admin' role. GizmoSQL confines filesystem- and "
         "instance-level commands to admins, so users sharing this server cannot read, "
         "write or reconfigure the host. (Server operators: roles come from the token's "
         "'role' claim)";
}

arrow::Status CheckNonAdminCommandAllowed(const std::string& sql) {
  auto category = ClassifyGatedCommand(sql);
  if (!category) return arrow::Status::OK();
  return flight::MakeFlightError(flight::FlightStatusCode::Unauthorized,
                                 GatedCommandDeniedMessage(*category));
}

bool IsFileReadingTableFunction(const std::string& name_lower) {
  return FsReadFunctions().count(name_lower) > 0;
}

}  // namespace gizmosql::ddb
