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

#include <optional>
#include <string>

#include <arrow/status.h>

// Rudimentary admin-command gate (Core).
//
// Ahead of the full RBAC model (GizmoSQL 2.0), this restricts a fixed set of
// dangerous, instance-level SQL commands to sessions whose role is "admin". It
// exists to stop a NON-admin token user from, e.g., detaching a DuckLake
// catalog, reading arbitrary local files, attaching databases, installing
// extensions, or changing global server settings.
//
// Gated for non-admins:
//   * ATTACH / DETACH (any database)
//   * SET GLOBAL / RESET GLOBAL (any setting, DuckDB or GizmoSQL)
//   * INSTALL / LOAD (extensions)
//   * CHECKPOINT / FORCE CHECKPOINT
//   * COPY ... TO/FROM a LOCAL file, and — unless the server allows non-admin
//     remote access (--allow-non-admin-remote-access) — a remote
//     object-storage/HTTP URL too: a non-admin cannot create secrets, so any
//     remote I/O it runs uses the SERVER's credentials and network position,
//     which on a shared cluster reach every tenant's storage prefix
//   * EXPORT DATABASE / IMPORT DATABASE (ANY destination — full-database
//     egress/ingress is not bounded by object grants, so it is always gated)
//   * read_*() / glob() / sniff_csv() table functions reading the LOCAL
//     filesystem (and remote URLs, as for COPY above), at any nesting
//   * External-database table functions — postgres_query / postgres_execute /
//     postgres_scan..., mysql_*, sqlite_* — and CALLs of them. They run raw SQL
//     against an attached database (e.g. a DuckLake catalog's metadata
//     Postgres) as that database's own role, outside catalog permissions
//   * DuckLake functions that take files or metadata by name rather than by
//     catalog grant: ducklake_add_data_files, ducklake_commit, and a direct
//     ducklake_scan(...) call. (The other ducklake_*() functions are checked
//     at bind time against the token's catalog access — see
//     plan_access_guard.h.)
//   * CREATE SECRET / DROP SECRET (all variants: OR REPLACE / PERSISTENT /
//     TEMPORARY — secrets hold credentials)
//   * duckdb_secrets() (always — it exposes secret material)
//
// Detection is parser-based (duckdb::Parser — no execution, no connection), so
// it is robust to whitespace/comments/case and to nesting (subqueries, CTEs,
// joins, CTAS, INSERT ... SELECT, COPY (SELECT ...) TO).
//
// This parse-time gate is the first of two layers. It refuses a statement
// before DuckDB binds it (some table functions do work at bind time, e.g.
// postgres_query prepares its SQL on the remote server). The second layer,
// plan_access_guard.h, checks the BOUND plan of every statement a non-admin
// session prepares, so it also sees what a view, a macro or a prepared-statement
// parameter supplies — which this parse-time walk cannot.
//
// KNOWN LIMITATIONS of this parse-time layer (covered at bind time):
//   * A view or macro that wraps a gated function is not inspected here.
//   * Replacement scans of string-literal paths (SELECT * FROM '/etc/passwd')
//     are caught by a path-shape heuristic, not by binding.
//   * Note: the statement-type gates (ATTACH/DETACH/SET GLOBAL/INSTALL/LOAD/
//     CHECKPOINT/COPY/EXPORT) have NO such gap — they are exact.
//
// Role note: basic-auth (username/password) users are always assigned the
// "admin" role, so this gate only affects token deployments that mint non-admin
// roles. Callers must apply the role/is_internal fast-path themselves (only call
// for non-admin, non-internal client statements).

namespace gizmosql::ddb {

/// Server policy that changes what the gate refuses.
struct GateOptions {
  /// Allow non-admin sessions to read and write remote object-storage / HTTP
  /// URLs (--allow-non-admin-remote-access). Off by default: such I/O runs with
  /// the server's credentials, not the session's.
  bool allow_remote_paths = false;
};

// ---- Classification helpers shared with the bind-time plan guard ----------

/// True if `path` starts with an object-storage / network scheme GizmoSQL
/// recognizes (s3://, gs://, https://, hf://, ...). Case-insensitive.
bool IsProvenRemotePath(const std::string& path);

/// Table functions that read files named by their first argument (read_csv,
/// read_parquet, glob, ...). `name_lower` must be lower-case.
bool IsFileReadFunction(const std::string& name_lower);

/// Table functions of the external-database scanners (postgres_*, mysql_*,
/// sqlite_*). They run SQL on, or scan, an attached external database with
/// that database's own credentials. `name_lower` must be lower-case.
bool IsExternalDatabaseFunction(const std::string& name_lower);

/// What a non-admin needs to call a ducklake_*() table function.
enum class DuckLakeFunctionAccess {
  kNotDuckLake,  // not a DuckLake function
  kScan,         // ducklake_scan: the scan behind every DuckLake table; a direct
                 // call with arguments reads arbitrary files -> admin only
  kRead,         // reads a catalog's metadata: read access to its first argument
  kWrite,        // changes a catalog: write access to its first argument
  kAdmin,        // admin only (ducklake_add_data_files, ducklake_commit)
};
DuckLakeFunctionAccess ClassifyDuckLakeFunction(const std::string& name_lower);

/// Classify `sql` for the non-admin admin-command gate. Returns a short, stable
/// human-readable category describing the FIRST gated operation found (e.g.
/// "ATTACH", "DETACH", "SET GLOBAL memory_limit", "INSTALL extension",
/// "CHECKPOINT", "COPY TO (local filesystem)", "EXPORT DATABASE (local
/// filesystem)", "read_csv() (local filesystem)", "duckdb_secrets()"), or
/// std::nullopt if the statement is permitted for a non-admin session. A SQL
/// string that fails to parse returns std::nullopt (DuckDB surfaces the real
/// parse error during preparation).
std::optional<std::string> ClassifyGatedCommand(const std::string& sql,
                                                const GateOptions& options = {});

/// `SET allow_unredacted_secrets = <value>` (or the PRAGMA form) for any role,
/// or std::nullopt. Any value that is not a false constant matches (true, 1,
/// 'yes', NOT false, ...). RESET and `= false` / `= 0` are not a match. A parse
/// failure returns std::nullopt.
std::optional<std::string> ClassifyUnredactedSecretsSet(const std::string& sql);

/// True for `SET secret_directory = ...` (or the PRAGMA form), also inside
/// PREPARE / EXPLAIN. With the sensitive-path guard on, clients may not move
/// DuckDB's persistent-secret directory somewhere unprotected.
bool IsSecretDirectorySet(const std::string& sql);

/// Client-facing error text for a gated command, e.g. category "ATTACH".
std::string GatedCommandDeniedMessage(const std::string& category);

/// Convenience wrapper: arrow::Status::OK() if permitted for a non-admin
/// session, otherwise a Flight PermissionDenied (Unauthorized) error naming the
/// gated category.
arrow::Status CheckNonAdminCommandAllowed(const std::string& sql,
                                          const GateOptions& options = {});

}  // namespace gizmosql::ddb
