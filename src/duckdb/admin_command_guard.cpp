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
#include <unordered_map>
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
#include <duckdb/parser/tableref/subqueryref.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/function/macro_function.hpp>
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

// The catalog/schema qualifier of a call: `my_lake` in `my_lake.snapshots()`.
std::string FunctionQualifierOf(const dd::FunctionExpression& fe) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto& name = fe.GetQualifiedName();
  if (!Str(name.Catalog()).empty()) return Str(name.Catalog());
  return Str(name.Schema());
#else
  return fe.catalog.empty() ? fe.schema : fe.catalog;
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

// Paths that make the server itself open an outbound HTTP connection. Unlike
// object-storage URLs (which go through the credentials the operator mounted),
// these let a client use the server's network position: SSRF against internal
// endpoints and the cloud metadata service, and a data-exfiltration channel.
bool IsNetworkPath(const std::string& path_lower) {
  return path_lower.rfind("http://", 0) == 0 || path_lower.rfind("https://", 0) == 0 ||
         path_lower.rfind("hf://", 0) == 0;
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

// A string that plainly names a local file: an absolute, home-relative or
// dot-relative path, a file:// URL, a Windows drive or UNC path, or a name with a
// data-file extension. Narrower than LooksLikeLocalFilePath (a bare '/' inside
// the text is not enough: 'America/New_York' is an ordinary argument), because it
// is applied to the string arguments of every table function, whatever its name.
bool LooksLikeExplicitLocalPath(const std::string& text) {
  if (text.empty()) return false;
  const std::string low = ToLower(text);
  if (IsProvenRemotePath(low)) return false;
  for (const char* prefix : {"/", "~", "./", "../", ".\\", "..\\", "\\\\", "file:"}) {
    if (low.rfind(prefix, 0) == 0) return true;
  }
  if (low.size() >= 3 && std::isalpha(static_cast<unsigned char>(low[0])) &&
      low[1] == ':' && (low[2] == '\\' || low[2] == '/')) {
    return true;
  }
  static const std::array<const char*, 26> kExts = {
      ".csv",   ".tsv",   ".txt",     ".parquet", ".pq",  ".json",   ".ndjson",
      ".jsonl", ".arrow", ".feather", ".gz",      ".zst", ".bz2",    ".xz",
      ".orc",   ".avro",  ".xlsx",    ".xls",     ".db",  ".duckdb", ".sqlite",
      ".shp",   ".gpkg",  ".geojson", ".arrows",  ".ipc"};
  for (const char* ext : kExts) {
    const std::string e(ext);
    if (low.size() > e.size() && low.compare(low.size() - e.size(), e.size(), e) == 0) {
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

// Built-in table functions that only expand the values they are given, so a
// string argument is data, never a path or SQL (unnest(['report.csv'])).
bool IsValueTableFunction(const std::string& name_lower) {
  static const std::unordered_set<std::string> kFns = {
      "unnest",     "range",     "generate_series", "repeat",
      "repeat_row", "json_each", "json_tree",       "regexp_split_to_table"};
  return kFns.count(name_lower) > 0;
}

// Table functions gated for non-admins regardless of arguments.
const std::unordered_set<std::string>& AlwaysGatedFunctions() {
  // json_execute_serialized_sql runs serialized SQL the parser never sees as text.
  static const std::unordered_set<std::string> kFns = {"duckdb_secrets",
                                                       "json_execute_serialized_sql"};
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
      "allowed_directories", "allowed_paths", "home_directory", "lock_configuration"};
  return kSettings.count(name_lower) > 0;
}

// Extract the first positional string-literal argument of a function call (the
// path, for read_* functions). Returns nullopt if the first argument is not a
// plain string constant (e.g. a list, a column, or a computed expression).
std::optional<std::string> FirstStringLiteralArg(const dd::FunctionExpression& fe) {
  const auto* first = FirstPositionalArg(fe);
  if (!first) return std::nullopt;
  return StringConstant(*first);
}

// The first string literal among a call's arguments (positional or named,
// including strings inside a list) that names a network URL, if any.
std::optional<std::string> NetworkLiteralArg(const dd::FunctionExpression& fe) {
  std::optional<std::string> found;
  std::function<void(dd::ParsedExpression&)> visit = [&](dd::ParsedExpression& e) {
    if (found) return;
    if (auto lit = StringConstant(e)) {
      if (IsNetworkPath(ToLower(*lit))) found = *lit;
      return;
    }
    dd::ParsedExpressionIterator::EnumerateChildren(
        e, [&](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) visit(*child);
        });
  };
  // EnumerateChildren is the 1.x/2.x-neutral way to reach the call's arguments.
  dd::ParsedExpressionIterator::EnumerateChildren(
      const_cast<dd::FunctionExpression&>(fe),
      [&](dd::unique_ptr<dd::ParsedExpression>& child) {
        if (child) visit(*child);
      });
  return found;
}

// Extract a literal path from a COPY/EXPORT target (info.file_path, or a string
// constant in info.file_path_expression). nullopt => non-literal/unknown.
std::optional<std::string> CopyPathLiteral(const dd::CopyInfo& info) {
  if (!info.file_path.empty()) return info.file_path;
  if (info.file_path_expression) return StringConstant(*info.file_path_expression);
  return std::nullopt;
}

// COPY statements a non-admin may not run at the text level: local paths and
// network (HTTP) URLs. Object-storage COPY (s3://, gs://, ...) is allowed here;
// for a session whose token carries catalog_access rules the file system limits
// it to the data paths of that token's catalogs (RemotePathScope), whatever SQL
// names the path.
std::optional<std::string> LocalCopyViolation(const dd::CopyInfo& info) {
  auto path = CopyPathLiteral(info);
  const std::string lower = path ? ToLower(*path) : std::string();
  const bool remote = path && IsProvenRemotePath(lower);
  if (remote && !IsNetworkPath(lower)) return std::nullopt;
  if (remote) return info.is_from ? "COPY FROM (network URL)" : "COPY TO (network URL)";
  return info.is_from ? "COPY FROM (local filesystem)" : "COPY TO (local filesystem)";
}

// Functions that reach another database through the server's own connection
// settings (postgres_scanner / mysql_scanner / sqlite_scanner). They address an
// attached catalog by name and run as that catalog's database role, outside the
// token's catalog_access rules, so they are admin-only.
bool IsExternalDatabaseFunction(const std::string& name_lower) {
  return name_lower.rfind("postgres_", 0) == 0 || name_lower.rfind("mysql_", 0) == 0 ||
         name_lower == "sqlite_scan" || name_lower == "sqlite_attach" ||
         name_lower == "pg_clear_cache";
}

// DuckLake table functions / CALL procedures, spelled `ducklake_x('catalog', ...)`
// or `catalog.x(...)`.
const std::unordered_set<std::string>& DuckLakeReadMethods() {
  static const std::unordered_set<std::string> kFns = {
      "snapshots", "table_info", "list_files", "table_insertions", "table_deletions",
      "table_changes", "settings", "options", "get_option", "current_snapshot",
      "last_committed_snapshot",
      // Branch functions of customer
      // DuckLake builds (read-only ones;
      // any other ducklake_* is a write).
      "branches", "current_branch", "branch_table"};
  return kFns;
}

const std::unordered_set<std::string>& DuckLakeWriteMethods() {
  static const std::unordered_set<std::string> kFns = {
      "merge_adjacent_files", "expire_snapshots",
      "cleanup_old_files",    "delete_orphaned_files",
      "flush_inlined_data",   "rewrite_data_files",
      "set_option",           "add_data_files"};
  return kFns;
}

bool ContainsCaseInsensitive(const std::string& haystack, const std::string& needle);

// Extensions with their own object-store client (delta kernel, iceberg): they
// read s3:// / gs:// without going through DuckDB's file system, so the
// RemotePathScope cannot see them.
bool IsOwnStorageClientFunction(const std::string& name_lower) {
  return name_lower.rfind("delta_", 0) == 0 || name_lower.rfind("iceberg_", 0) == 0;
}

void AddOwnStorageRef(const std::string& function,
                      std::vector<DuckLakeFunctionRef>* refs) {
  if (!refs) return;
  const bool seen = std::any_of(refs->begin(), refs->end(), [&](const auto& r) {
    return r.own_storage_client && r.function == function;
  });
  if (seen) return;
  DuckLakeFunctionRef ref;
  ref.function = function;
  ref.own_storage_client = true;
  refs->push_back(std::move(ref));
}

// A macro body is not walked, so look for these functions by name.
void NoteMacroOwnStorageMentions(const std::string& sql,
                                 std::vector<DuckLakeFunctionRef>* refs) {
  for (const char* name : {"delta_", "iceberg_"}) {
    if (ContainsCaseInsensitive(sql, name)) {
      AddOwnStorageRef(std::string(name) + "*", refs);
    }
  }
}

// ---- which calls the checks have looked at ----------------------------------
//
// The walker below only reaches the parts of a statement it knows how to walk.
// SQL has many more places a call can sit (DESCRIBE / SUMMARIZE, PIVOT,
// SET VARIABLE, CREATE TYPE ... AS ENUM (SELECT ...), UPDATE / DELETE / MERGE on
// DuckDB 1.x, and whatever a later DuckDB adds), and a call the walker never saw
// would run unchecked. So every call to a function these checks care about is
// counted twice: once when the walker inspects it (CallAudit), once by reading
// the statement's tokens (ScanCallTokens). A statement that names more such
// calls than the walker inspected is refused (UninspectedCallViolation): the gate
// fails closed for statement forms it cannot read.

// The key a call is counted under, or nullopt if these checks do not care about
// the function. `qualified`: written `x.name(...)`.
std::optional<std::string> AuditedCallKey(const std::string& name_lower, bool qualified) {
  if (AlwaysGatedFunctions().count(name_lower) || FsReadFunctions().count(name_lower) ||
      name_lower == "query" || name_lower == "query_table" ||
      IsQuackFunction(name_lower) || IsExternalDatabaseFunction(name_lower) ||
      IsOwnStorageClientFunction(name_lower) || name_lower.rfind("ducklake_", 0) == 0) {
    return name_lower;
  }
  if (qualified && (DuckLakeReadMethods().count(name_lower) ||
                    DuckLakeWriteMethods().count(name_lower))) {
    return "." + name_lower;
  }
  return std::nullopt;
}

struct CallAudit {
  std::unordered_map<std::string, int> inspected;
  std::unordered_set<const dd::FunctionExpression*> seen;
};

// The audit of the SQL text being classified on this thread (a query('...')
// string gets one of its own).
thread_local CallAudit* current_call_audit = nullptr;

struct ScopedCallAudit {
  explicit ScopedCallAudit(CallAudit* audit) : previous(current_call_audit) {
    current_call_audit = audit;
  }
  ~ScopedCallAudit() { current_call_audit = previous; }
  CallAudit* previous;
};

void NoteInspectedCall(const dd::FunctionExpression& fe) {
  if (!current_call_audit) return;
  // A table function is visited both as a ref and as an expression.
  if (!current_call_audit->seen.insert(&fe).second) return;
  const auto key =
      AuditedCallKey(ToLower(FunctionNameOf(fe)), !FunctionQualifierOf(fe).empty());
  if (key) ++current_call_audit->inspected[*key];
}

// Checks one function call: returns a violation category if it is admin-only,
// and records any DuckLake catalog it addresses into `refs` (if given).
std::optional<std::string> CheckFunctionCall(const dd::FunctionExpression& fe,
                                             std::vector<DuckLakeFunctionRef>* refs) {
  NoteInspectedCall(fe);
  const std::string name = ToLower(FunctionNameOf(fe));
  if (IsQuackFunction(name) || IsExternalDatabaseFunction(name)) return name + "()";

  if (IsOwnStorageClientFunction(name)) AddOwnStorageRef(name, refs);

  const std::string qualifier = FunctionQualifierOf(fe);
  const bool prefixed = name.rfind("ducklake_", 0) == 0;
  const std::string method = prefixed ? name.substr(9) : name;
  if (!prefixed && qualifier.empty()) return std::nullopt;
  const bool is_write = DuckLakeWriteMethods().count(method) > 0;
  if (!prefixed && !is_write && DuckLakeReadMethods().count(method) == 0) {
    return std::nullopt;
  }
  // add_data_files registers arbitrary file paths in a catalog, so a user could
  // pull another tenant's data files into their own catalog.
  if (method == "add_data_files") return name + "()";
  if (refs) {
    DuckLakeFunctionRef ref;
    ref.function = name;
    ref.writes = is_write || (prefixed && DuckLakeReadMethods().count(method) == 0);
    ref.catalog =
        prefixed ? FirstStringLiteralArg(fe) : std::optional<std::string>(qualifier);
    ref.qualified = !prefixed;
    // A table function is visited both as a ref and as an expression.
    const bool seen = std::any_of(refs->begin(), refs->end(), [&](const auto& r) {
      return r.function == ref.function && r.catalog == ref.catalog &&
             r.writes == ref.writes;
    });
    if (!seen) refs->push_back(std::move(ref));
  }
  return std::nullopt;
}

std::optional<std::string> ClassifyNestedSql(
    const std::string& sql, std::vector<DuckLakeFunctionRef>* ducklake_refs);
std::optional<std::string> MacroMentionsGatedFunction(const std::string& sql);

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
  std::vector<DuckLakeFunctionRef>* ducklake_refs = nullptr;

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
          violation = "COPY";
          return;
        }
        if (auto v = LocalCopyViolation(*copy.info)) {
          violation = v;
          return;
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
      if (table_name && IsNetworkPath(ToLower(*table_name))) {
        violation = "reading a network URL via FROM '" + *table_name + "'";
      } else if (table_name && LooksLikeLocalFilePath(*table_name)) {
        violation = "reading a local file via FROM '" + *table_name + "'";
      }
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
      if (auto v =
              CheckFunctionCall(expr.Cast<dd::FunctionExpression>(), ducklake_refs)) {
        violation = v;
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
    CheckTableFunctionCall(tf.function->Cast<dd::FunctionExpression>());
  }

  // A call in table-function position: a FROM-clause table function, or the
  // function of a CALL statement (CALL read_csv('/etc/passwd') returns its rows).
  void CheckTableFunctionCall(const dd::FunctionExpression& fe) {
    if (violation) return;
    NoteInspectedCall(fe);
    const std::string name = ToLower(FunctionNameOf(fe));
    if (AlwaysGatedFunctions().count(name)) {
      violation = name + "()";
      return;
    }
    if (auto v = CheckFunctionCall(fe, ducklake_refs)) {
      violation = v;
      return;
    }
    // Any table function handed a network URL makes the server open an outbound
    // HTTP connection, whichever function it is.
    if (auto url = NetworkLiteralArg(fe)) {
      violation = name + "() (network URL)";
      return;
    }
    // query('<sql>') runs its argument as SQL: check that text like a statement
    // of its own, or the checks above are skipped for whatever it names.
    if (name == "query") {
      const auto nested = FirstStringLiteralArg(fe);
      if (!nested) {
        violation = "query() (SQL that is not a string literal)";
        return;
      }
      if (auto v = ClassifyNestedSql(*nested, ducklake_refs)) {
        violation = v;
        return;
      }
    }
    if (FsReadFunctions().count(name)) {
      auto path = FirstStringLiteralArg(fe);
      // Object storage is allowed here (and limited per session by the file
      // system); anything not provably remote is the local filesystem.
      if (!path || !IsProvenRemotePath(ToLower(*path))) {
        violation = name + "() (local filesystem)";
      }
      return;
    }
    if (name == "query") return;  // its SQL was classified above
    // query_table('<name>') resolves each name like a FROM clause does, so a file
    // path is read as a replacement scan.
    if (name == "query_table") {
      violation = QueryTableViolation(fe);
      return;
    }
    // Any other table function, including one these checks have never heard of
    // (an extension's reader, a user's table macro that passes its argument on
    // to read_csv() or query()): its string arguments must not plainly name a
    // local file, nor be SQL that the checks would refuse as a statement.
    if (FunctionQualifierOf(fe).empty() && IsValueTableFunction(name)) return;
    violation = OpaqueTableFunctionViolation(fe, name);
  }

  // query_table(): every table name must be a literal that is not a file.
  std::optional<std::string> QueryTableViolation(const dd::FunctionExpression& fe) {
    std::optional<std::string> found;
    std::function<void(dd::ParsedExpression&)> visit = [&](dd::ParsedExpression& e) {
      if (found) return;
      if (auto lit = StringConstant(e)) {
        const std::string low = ToLower(*lit);
        if (IsNetworkPath(low)) {
          found = "query_table() (network URL)";
        } else if (!IsProvenRemotePath(low) && LooksLikeLocalFilePath(*lit)) {
          found = "query_table() (local filesystem)";
        }
        return;
      }
      const auto expression_class = e.GetExpressionClass();
      if (expression_class == dd::ExpressionClass::CONSTANT) return;
      // A list of names (['a', 'b']) and the keywords true / false, which DuckDB
      // parses as casts of a constant.
      const bool container =
          expression_class == dd::ExpressionClass::CAST ||
          expression_class == dd::ExpressionClass::OPERATOR ||
          (expression_class == dd::ExpressionClass::FUNCTION &&
           ToLower(FunctionNameOf(e.Cast<dd::FunctionExpression>())) == "list_value");
      if (container) {
        dd::ParsedExpressionIterator::EnumerateChildren(
            e, [&](dd::unique_ptr<dd::ParsedExpression>& child) {
              if (child) visit(*child);
            });
        return;
      }
      found = "query_table() (table name that is not a string literal)";
    };
    dd::ParsedExpressionIterator::EnumerateChildren(
        const_cast<dd::FunctionExpression&>(fe),
        [&](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) visit(*child);
        });
    return found;
  }

  std::optional<std::string> OpaqueTableFunctionViolation(
      const dd::FunctionExpression& fe, const std::string& name) {
    std::optional<std::string> found;
    std::function<void(dd::ParsedExpression&)> visit = [&](dd::ParsedExpression& e) {
      if (found) return;
      if (auto lit = StringConstant(e)) {
        if (LooksLikeExplicitLocalPath(*lit)) {
          found = name + "() (local filesystem)";
        } else if (lit->find_first_of(" \t\r\n") != std::string::npos) {
          if (auto v = ClassifyNestedSql(*lit, ducklake_refs)) {
            found = "SQL passed to " + name + "(): " + *v;
          }
        }
        return;
      }
      dd::ParsedExpressionIterator::EnumerateChildren(
          e, [&](dd::unique_ptr<dd::ParsedExpression>& child) {
            if (child) visit(*child);
          });
    };
    dd::ParsedExpressionIterator::EnumerateChildren(
        const_cast<dd::FunctionExpression&>(fe),
        [&](dd::unique_ptr<dd::ParsedExpression>& child) {
          if (child) visit(*child);
        });
    return found;
  }
};

// Walk the embedded query/select of a statement for gated table functions.
std::optional<std::string> WalkStatementForGatedFunctions(
    dd::SQLStatement& stmt, std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  GatedFunctionWalker walker;
  walker.ducklake_refs = ducklake_refs;
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
        // A view would otherwise wrap a gated function for later use.
        auto& vi = s.info->Cast<dd::CreateViewInfo>();
        if (vi.query && vi.query->node) {
          // A DuckLake call in a view runs with the access of whoever selects from
          // it, not of whoever created it, so the caller refuses views that wrap
          // one (DuckLakeFunctionRef::in_view).
          std::vector<DuckLakeFunctionRef> view_refs;
          walker.ducklake_refs = ducklake_refs ? &view_refs : nullptr;
          walker.WalkNode(*vi.query->node);
          for (auto& ref : view_refs) {
            ref.in_view = true;
            ducklake_refs->push_back(std::move(ref));
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
std::optional<std::string> ClassifyStatement(
    dd::SQLStatement& stmt, std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  switch (stmt.type) {
    case dd::StatementType::PREPARE_STATEMENT: {
      auto& ps = stmt.Cast<dd::PrepareStatement>();
      if (ps.statement) {
        if (auto v = ClassifyStatement(*ps.statement, ducklake_refs)) return v;
      }
      break;
    }
    case dd::StatementType::EXPLAIN_STATEMENT: {
      auto& es = stmt.Cast<dd::ExplainStatement>();
      if (es.stmt) {
        if (auto v = ClassifyStatement(*es.stmt, ducklake_refs)) return v;
      }
      break;
    }
    case dd::StatementType::ATTACH_STATEMENT:
      return "ATTACH";
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    // DuckDB 2.0: CONNECT attaches its target (a file path or a postgres:/
    // sqlite:/quack: URI) and then forwards the session's raw SQL to it,
    // past these AST checks and catalog permissions — so it is ATTACH-class.
    // EXTERNAL RESOURCE provisions compute; PASSTHROUGH is raw SQL for a
    // CONNECTed target.
    case dd::StatementType::CONNECT_STATEMENT:
      return "CONNECT";
    case dd::StatementType::DISCONNECT_STATEMENT:
      return "DISCONNECT";
    case dd::StatementType::EXTERNAL_RESOURCE_STATEMENT:
      return "EXTERNAL RESOURCE";
    case dd::StatementType::PASSTHROUGH_STATEMENT:
      return "passthrough SQL";
#endif
    case dd::StatementType::DETACH_STATEMENT:
      return "DETACH";
    case dd::StatementType::CREATE_STATEMENT: {
      // CREATE SECRET (incl. OR REPLACE / PERSISTENT / TEMPORARY — all parse
      // as a CreateStatement with a SECRET_ENTRY catalog type). Secrets hold
      // credentials, so creating them is admin-only. Other CREATE statements
      // (incl. CTAS) fall through to the gated-function walk below.
      auto& cs = stmt.Cast<dd::CreateStatement>();
      if (cs.info && cs.info->type == dd::CatalogType::SECRET_ENTRY) {
        return "CREATE SECRET";
      }
      if (cs.info && (cs.info->type == dd::CatalogType::MACRO_ENTRY ||
                      cs.info->type == dd::CatalogType::TABLE_MACRO_ENTRY)) {
        // A macro runs its body for whoever calls it, with the caller's
        // arguments, so the body is held to the same rules as a statement. That
        // refuses a body that passes a parameter on as a path or as SQL
        // (read_csv(p), query(s)): neither is a string literal.
        auto& mi = cs.info->Cast<dd::CreateMacroInfo>();
        std::vector<DuckLakeFunctionRef> body_refs;
        GatedFunctionWalker walker;
        walker.ducklake_refs = ducklake_refs ? &body_refs : nullptr;
        for (auto& macro : mi.macros) {
          if (!macro) continue;
          for (auto& entry : macro->default_parameters) {
            if (entry.second) walker.CheckExpr(*entry.second);
          }
          if (macro->type == dd::MacroType::TABLE_MACRO) {
            auto& table_macro = macro->Cast<dd::TableMacroFunction>();
            if (table_macro.query_node) walker.WalkNode(*table_macro.query_node);
          } else {
            auto& scalar_macro = macro->Cast<dd::ScalarMacroFunction>();
            if (scalar_macro.expression) walker.CheckExpr(*scalar_macro.expression);
          }
        }
        if (walker.violation) return "CREATE MACRO wrapping " + *walker.violation;
        // Like a view, a macro runs with the access of whoever calls it.
        for (auto& ref : body_refs) {
          if (!ref.own_storage_client) ref.in_view = true;
          ducklake_refs->push_back(std::move(ref));
        }
        return "CREATE MACRO";  // refined to a pass-through by ClassifySqlText
      }
      break;
    }
    case dd::StatementType::DROP_STATEMENT: {
      // DROP SECRET (incl. IF EXISTS / PERSISTENT / TEMPORARY — all parse as a
      // DropStatement with a SECRET_ENTRY catalog type). Other DROPs allowed.
      auto& ds = stmt.Cast<dd::DropStatement>();
      if (ds.info && ds.info->type == dd::CatalogType::SECRET_ENTRY) {
        return "DROP SECRET";
      }
      break;
    }
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
        return verb + Str(ss.name);  // explicit GLOBAL: any setting
      }
      // Bare SET/RESET (scope AUTOMATIC) of a dangerous global-only setting
      // changes it for the whole instance — gate it too. Explicit SESSION /
      // LOCAL, and bare SET of harmless session settings, are allowed.
      if (ss.scope == dd::SetScope::AUTOMATIC &&
          IsDangerousGlobalSetting(ToLower(Str(ss.name)))) {
        return verb + Str(ss.name);
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
        if (fname == "checkpoint" || fname == "force_checkpoint") return "CHECKPOINT";
        // CALL f(...) returns f's rows like SELECT * FROM f(...), so f is held to
        // the table-function rules (CALL read_csv('/etc/passwd'), CALL
        // duckdb_secrets(), CALL query('...')).
        auto& fe = call.function->Cast<dd::FunctionExpression>();
        GatedFunctionWalker walker;
        walker.ducklake_refs = ducklake_refs;
        walker.CheckTableFunctionCall(fe);
        if (!walker.violation) {
          dd::ParsedExpressionIterator::EnumerateChildren(
              fe, [&](dd::unique_ptr<dd::ParsedExpression>& child) {
                if (child) walker.CheckExpr(*child);
              });
        }
        if (walker.violation) return walker.violation;
      }
      break;  // other CALL functions allowed
    }
    case dd::StatementType::COPY_STATEMENT: {
      auto& cs = stmt.Cast<dd::CopyStatement>();
      if (cs.info) {
        if (auto v = LocalCopyViolation(*cs.info)) return v;
        // Remote COPY target is allowed, but a COPY (SELECT read_csv(...)) TO
        // may still read the local filesystem — inspect the embedded query.
        if (cs.info->select_statement) {
          GatedFunctionWalker walker;
          walker.ducklake_refs = ducklake_refs;
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
      return "EXPORT DATABASE";
    case dd::StatementType::PRAGMA_STATEMENT: {
      // IMPORT DATABASE is parsed as `PRAGMA import_database('dir')`. It runs
      // arbitrary DDL + DML from a dump (and reads the filesystem), so it is
      // gated unconditionally, local or remote. Other pragmas are allowed.
      auto& ps = stmt.Cast<dd::PragmaStatement>();
      if (ps.info && ToLower(Str(ps.info->name)) == "import_database") {
        return "IMPORT DATABASE";
      }
      // PRAGMA json_execute_serialized_sql('...') is the same function as the
      // table function of that name.
      if (ps.info && AlwaysGatedFunctions().count(ToLower(Str(ps.info->name)))) {
        return ToLower(Str(ps.info->name)) + "()";
      }
      break;
    }
    default:
      break;
  }

  // Embedded read_*/duckdb_secrets in SELECT / CTAS / INSERT ... SELECT.
  if (auto v = WalkStatementForGatedFunctions(stmt, ducklake_refs)) return v;
  return std::nullopt;
}

std::optional<std::string> ClassifySqlText(
    const std::string& sql, const std::string& parse_text,
    std::vector<DuckLakeFunctionRef>* ducklake_refs);

// The SQL text a query('...') call runs (or a string handed to a table function
// that may pass it on to one), checked as statements of their own. Unparseable
// text is left for DuckDB to report; nesting is capped, so a query() inside a
// query() string cannot recurse without bound.
std::optional<std::string> ClassifyNestedSql(
    const std::string& sql, std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  static thread_local int depth = 0;
  constexpr int kMaxDepth = 4;
  if (depth >= kMaxDepth) return "query() nested too deeply";
  struct DepthGuard {
    DepthGuard() { ++depth; }
    ~DepthGuard() { --depth; }
  } guard;
  return ClassifySqlText(sql, sql, ducklake_refs);
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

// A CREATE MACRO / CREATE FUNCTION whose text names a function that is gated
// regardless of its arguments. Over-matches (it also trips on a string literal),
// which is the safe direction for a security gate.
std::optional<std::string> MacroMentionsGatedFunction(const std::string& sql) {
  static const std::array<const char*, 8> kNames = {
      "postgres_",      "mysql_", "sqlite_scan",    "sqlite_attach",
      "pg_clear_cache", "quack_", "duckdb_secrets", "ducklake_"};
  for (const char* name : kNames) {
    if (ContainsCaseInsensitive(sql, name)) {
      return "CREATE MACRO referencing " + std::string(name) + "*";
    }
  }
  return std::nullopt;
}

// ---- reading a statement's calls from its tokens ----------------------------

struct Lexeme {
  enum Kind { kIdentifier, kDot, kOpenParen, kString, kOther };
  Kind kind;
  std::string
      text;  // identifier: lower-case name; string: raw literal; other: the character
};

bool IsIdentifierStart(unsigned char c) {
  return std::isalpha(c) || c == '_' || c >= 0x80;
}
bool IsIdentifierPart(unsigned char c) {
  return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
}

// Lexes sql[pos, end) (text DuckDB's tokenizer did not mark as a string constant)
// into `out`. Comments are skipped. Strings are still recognized, in case a
// tokenizer reports one under another type.
void LexCode(const std::string& sql, size_t pos, size_t end, std::vector<Lexeme>& out) {
  while (pos < end) {
    const unsigned char c = static_cast<unsigned char>(sql[pos]);
    if (std::isspace(c)) {
      ++pos;
    } else if (c == '-' && pos + 1 < end && sql[pos + 1] == '-') {
      while (pos < end && sql[pos] != '\n') ++pos;
    } else if (c == '/' && pos + 1 < end && sql[pos + 1] == '*') {
      int depth = 1;
      pos += 2;
      while (pos < end && depth > 0) {
        if (sql[pos] == '/' && pos + 1 < end && sql[pos + 1] == '*') {
          ++depth;
          pos += 2;
        } else if (sql[pos] == '*' && pos + 1 < end && sql[pos + 1] == '/') {
          --depth;
          pos += 2;
        } else {
          ++pos;
        }
      }
    } else if (c == '"') {
      std::string name;
      ++pos;
      while (pos < end) {
        if (sql[pos] == '"') {
          if (pos + 1 < end && sql[pos + 1] == '"') {
            name += '"';
            pos += 2;
            continue;
          }
          ++pos;
          break;
        }
        name += sql[pos++];
      }
      out.push_back({Lexeme::kIdentifier, ToLower(name)});
    } else if (c == '\'') {
      // E'...' strings take backslash escapes.
      const bool escapes = !out.empty() && out.back().kind == Lexeme::kIdentifier &&
                           out.back().text == "e" && pos > 0 &&
                           (sql[pos - 1] == 'e' || sql[pos - 1] == 'E');
      const size_t begin = pos++;
      while (pos < end) {
        if (escapes && sql[pos] == '\\' && pos + 1 < end) {
          pos += 2;
        } else if (sql[pos] == '\'') {
          if (pos + 1 < end && sql[pos + 1] == '\'') {
            pos += 2;
            continue;
          }
          ++pos;
          break;
        } else {
          ++pos;
        }
      }
      if (escapes) out.pop_back();
      out.push_back(
          {Lexeme::kString, escapes ? std::string() : sql.substr(begin, pos - begin)});
    } else if (c == '$') {
      // $tag$ ... $tag$
      size_t tag_end = pos + 1;
      while (tag_end < end &&
             IsIdentifierPart(static_cast<unsigned char>(sql[tag_end])) &&
             sql[tag_end] != '$') {
        ++tag_end;
      }
      const bool is_tag =
          tag_end < end && sql[tag_end] == '$' &&
          (tag_end == pos + 1 || !std::isdigit(static_cast<unsigned char>(sql[pos + 1])));
      if (!is_tag) {
        out.push_back({Lexeme::kOther, "$"});
        ++pos;
        continue;
      }
      const std::string tag = sql.substr(pos, tag_end + 1 - pos);
      const auto close = sql.find(tag, tag_end + 1);
      pos = (close == std::string::npos || close >= end) ? end : close + tag.size();
      out.push_back({Lexeme::kString, std::string()});
    } else if (IsIdentifierStart(c)) {
      const size_t begin = pos;
      while (pos < end && IsIdentifierPart(static_cast<unsigned char>(sql[pos]))) ++pos;
      out.push_back({Lexeme::kIdentifier, ToLower(sql.substr(begin, pos - begin))});
    } else if (std::isdigit(c)) {
      while (pos < end && IsIdentifierPart(static_cast<unsigned char>(sql[pos]))) ++pos;
      out.push_back({Lexeme::kOther, "0"});
    } else {
      out.push_back({c == '.'   ? Lexeme::kDot
                     : c == '(' ? Lexeme::kOpenParen
                                : Lexeme::kOther,
                     std::string(1, static_cast<char>(c))});
      ++pos;
    }
  }
}

// The text of a plain '...' literal lexeme; nullopt for any other spelling
// (E'...', $$...$$), whose text is not decoded here.
std::optional<std::string> PlainLiteralValue(const Lexeme& lexeme) {
  const auto& raw = lexeme.text;
  if (lexeme.kind != Lexeme::kString || raw.size() < 2 || raw.front() != '\'') {
    return std::nullopt;
  }
  std::string value;
  for (size_t k = 1; k + 1 < raw.size(); ++k) {
    value += raw[k];
    if (raw[k] == '\'') ++k;  // '' is one quote
  }
  return value;
}

bool IsTableKeyword(const std::string& word) {
  static const std::unordered_set<std::string> kWords = {"from",     "join",      "using",
                                                         "describe", "summarize", "pivot",
                                                         "unpivot",  "table",     "show"};
  return kWords.count(word) > 0;
}

// Whether lexemes[i] stands where a table name does: right after FROM / JOIN /
// USING / DESCRIBE / SUMMARIZE / PIVOT / ..., or later in the same FROM list.
bool InTablePosition(const std::vector<Lexeme>& lexemes, size_t i) {
  if (i == 0) return false;
  const auto& previous = lexemes[i - 1];
  if (previous.kind == Lexeme::kIdentifier) return IsTableKeyword(previous.text);
  if (previous.kind != Lexeme::kOther || previous.text != ",") return false;
  // After a comma: walk back through this parenthesis level to the clause keyword.
  static const std::unordered_set<std::string> kOtherClauses = {
      "select", "where", "set",  "values",  "group",  "order",  "by",       "having",
      "limit",  "when",  "then", "qualify", "window", "offset", "returning"};
  int depth = 0;
  for (size_t j = i - 1; j-- > 0;) {
    const auto& lexeme = lexemes[j];
    if (lexeme.kind == Lexeme::kOther && lexeme.text == ")") {
      ++depth;
    } else if (lexeme.kind == Lexeme::kOpenParen) {
      if (depth == 0) return false;
      --depth;
    } else if (depth == 0 && lexeme.kind == Lexeme::kIdentifier) {
      if (lexeme.text == "from" || lexeme.text == "join" || lexeme.text == "using") {
        return true;
      }
      if (kOtherClauses.count(lexeme.text)) return false;
    }
  }
  return false;
}

// A call these checks care about, as written in the statement text.
struct CallToken {
  std::string key;   // AuditedCallKey
  std::string name;  // lower-case function name
  // The first argument, when it is a plain '...' literal and the whole argument.
  std::optional<std::string> first_literal;
};

// Every `name(` in the statement text whose name these checks care about. String
// constants and comments are not code and are skipped; DuckDB's own tokenizer
// says where the string constants are. nullopt: the text could not be tokenized.
std::optional<std::vector<CallToken>> ScanCallTokens(
    const std::string& sql, std::optional<std::string>* file_as_table) {
  dd::vector<dd::SimplifiedToken> tokens;
  try {
    tokens = dd::Parser::Tokenize(sql);
  } catch (...) {
    return std::nullopt;
  }
  std::vector<Lexeme> lexemes;
  for (size_t i = 0; i < tokens.size(); ++i) {
    const size_t begin = tokens[i].start;
    const size_t end = i + 1 < tokens.size() ? tokens[i + 1].start : sql.size();
    if (begin > end || end > sql.size()) return std::nullopt;
    const auto type = tokens[i].type;
    if (type == dd::SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT) continue;
    if (type == dd::SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT &&
        sql[begin] != '"') {
      // Keep the literal itself, without what follows it up to the next token.
      std::string literal;
      if (sql[begin] == '\'') {
        size_t pos = begin + 1;
        while (pos < end) {
          if (sql[pos] == '\'') {
            if (pos + 1 < end && sql[pos + 1] == '\'') {
              pos += 2;
              continue;
            }
            ++pos;
            break;
          }
          ++pos;
        }
        literal = sql.substr(begin, pos - begin);
      }
      lexemes.push_back({Lexeme::kString, std::move(literal)});
      continue;
    }
    LexCode(sql, begin, end, lexemes);
  }

  std::vector<CallToken> calls;
  for (size_t i = 0; i + 1 < lexemes.size(); ++i) {
    if (lexemes[i].kind != Lexeme::kIdentifier ||
        lexemes[i + 1].kind != Lexeme::kOpenParen) {
      continue;
    }
    const bool qualified = i >= 2 && lexemes[i - 1].kind == Lexeme::kDot &&
                           lexemes[i - 2].kind == Lexeme::kIdentifier;
    // CREATE MACRO name(...) defines `name`; it does not call it.
    size_t chain = i;
    while (chain >= 2 && lexemes[chain - 1].kind == Lexeme::kDot &&
           lexemes[chain - 2].kind == Lexeme::kIdentifier) {
      chain -= 2;
    }
    if (chain >= 1 && lexemes[chain - 1].kind == Lexeme::kIdentifier &&
        (lexemes[chain - 1].text == "macro" || lexemes[chain - 1].text == "function" ||
         lexemes[chain - 1].text == "exists")) {
      continue;
    }
    const auto key = AuditedCallKey(lexemes[i].text, qualified);
    if (!key) continue;
    CallToken call{*key, lexemes[i].text, std::nullopt};
    if (i + 3 < lexemes.size() && lexemes[i + 3].kind == Lexeme::kOther &&
        (lexemes[i + 3].text == "," || lexemes[i + 3].text == ")")) {
      call.first_literal = PlainLiteralValue(lexemes[i + 2]);
    }
    calls.push_back(std::move(call));
  }

  // FROM 'file.csv' (a replacement scan) names no function at all, so look for a
  // string or quoted name that stands where a table does and names a file or URL.
  for (size_t i = 0; i < lexemes.size() && !*file_as_table; ++i) {
    const auto& lexeme = lexemes[i];
    if (lexeme.kind != Lexeme::kString && lexeme.kind != Lexeme::kIdentifier) continue;
    if (i + 1 < lexemes.size() && (lexemes[i + 1].kind == Lexeme::kOpenParen ||
                                   lexemes[i + 1].kind == Lexeme::kDot)) {
      continue;  // a call, or the qualifier of a name
    }
    if (!InTablePosition(lexemes, i)) continue;
    if (lexeme.kind == Lexeme::kIdentifier) {
      const auto& name = lexeme.text;
      if (IsNetworkPath(name) || LooksLikeExplicitLocalPath(name)) *file_as_table = name;
      continue;
    }
    const auto value = PlainLiteralValue(lexeme);
    if (!value) {
      *file_as_table = "a string that is not a plain literal";
    } else if (IsNetworkPath(ToLower(*value)) || LooksLikeExplicitLocalPath(*value)) {
      *file_as_table = *value;
    }
  }
  return calls;
}

// Refuses a statement that names more calls of a function these checks care
// about than the walker inspected: the rest sit somewhere the walker does not
// reach. One exception keeps object-storage reads usable in those places
// (MERGE ... USING read_parquet('s3://...') on DuckDB 1.x, DESCRIBE SELECT ...
// FROM read_parquet('s3://...')): a file-reading function is fine when every
// call of it in the statement reads a literal object-storage path, which is all
// the walker would have required of it.
std::optional<std::string> UninspectedCallViolation(
    const std::string& sql, const CallAudit& audit,
    std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  std::optional<std::string> file_as_table;
  const auto calls = ScanCallTokens(sql, &file_as_table);
  if (!calls) return "SQL that cannot be tokenized";
  if (file_as_table) return "reading a file or URL via FROM '" + *file_as_table + "'";
  std::unordered_map<std::string, int> written;
  for (const auto& call : *calls) ++written[call.key];
  for (const auto& [key, count] : written) {
    const auto it = audit.inspected.find(key);
    if (count <= (it == audit.inspected.end() ? 0 : it->second)) continue;
    std::string name;
    bool literal_object_storage = true;
    for (const auto& call : *calls) {
      if (call.key != key) continue;
      name = call.name;
      const std::string low = call.first_literal ? ToLower(*call.first_literal) : "";
      if (!call.first_literal || !IsProvenRemotePath(low) || IsNetworkPath(low)) {
        literal_object_storage = false;
      }
    }
    const bool reader = FsReadFunctions().count(name) || IsOwnStorageClientFunction(name);
    if (reader && literal_object_storage && key == name) {
      if (IsOwnStorageClientFunction(name)) AddOwnStorageRef(name, ducklake_refs);
      continue;
    }
    return name + "() in a statement form GizmoSQL cannot inspect";
  }
  return std::nullopt;
}

// Classifies every statement in `sql` (parsed from `parse_text`), then checks
// that no call went uninspected.
std::optional<std::string> ClassifySqlText(
    const std::string& sql, const std::string& parse_text,
    std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  auto parser = compat::StandaloneParser();
  try {
    parser.ParseQuery(parse_text);
  } catch (...) {
    // Unparseable here — let DuckDB surface the real parse error during
    // preparation. (The standalone parser uses the same core grammar as the
    // engine for the gated statement types.)
    return std::nullopt;
  }

  CallAudit audit;
  ScopedCallAudit scoped_audit(&audit);
  for (auto& stmt_ptr : parser.statements) {
    if (!stmt_ptr) continue;
    auto v = ClassifyStatement(*stmt_ptr, ducklake_refs);
    if (v && *v == "CREATE MACRO") {
      // The body was walked (ClassifyStatement). On top of that, a macro that
      // mentions an admin-only family of functions anywhere in its text is
      // refused by name.
      NoteMacroOwnStorageMentions(sql, ducklake_refs);
      v = MacroMentionsGatedFunction(sql);
    }
    if (v) return v;
  }
  return UninspectedCallViolation(sql, audit, ducklake_refs);
}

}  // namespace

std::optional<std::string> ClassifyGatedCommand(
    const std::string& sql, std::vector<DuckLakeFunctionRef>* ducklake_refs) {
  // A dotted setting name (SET GLOBAL gizmosql.x) must still classify on
  // DuckDB 2.x, whose grammar only parses it quoted.
  return ClassifySqlText(sql, compat::QuoteDottedSettingName(sql), ducklake_refs);
}

std::optional<std::string> ClassifyGatedCommand(const std::string& sql) {
  return ClassifyGatedCommand(sql, nullptr);
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

}  // namespace gizmosql::ddb
