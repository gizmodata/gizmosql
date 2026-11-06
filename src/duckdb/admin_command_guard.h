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
#include <vector>

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
//   * COPY ... TO/FROM a LOCAL file or a network URL
//   * EXPORT DATABASE / IMPORT DATABASE (ANY destination — full-database
//     egress/ingress is not bounded by object grants, so it is always gated)
//   * read_*() / glob() / sniff_csv() table functions reading the LOCAL
//     filesystem, at any nesting
//   * CREATE SECRET / DROP SECRET (all variants: OR REPLACE / PERSISTENT /
//     TEMPORARY — secrets hold credentials)
//   * duckdb_secrets() (always — it exposes secret material)
//   * postgres_* / mysql_* functions, sqlite_scan/sqlite_attach (always — they
//     reach attached databases outside the token's catalog access)
//   * reads and writes of http(s):// / hf:// URLs (server-side outbound HTTP) in
//     any table function / COPY / replacement scan
//   * query('<sql>'): its SQL is checked like a statement of its own; a query()
//     whose argument is not a string literal is refused. json_execute_serialized_sql
//     (always)
//   * Object storage (s3://, gs://, ...) is NOT decided here. For a session scoped
//     by catalog_access rules, SensitivePathGuardFileSystem limits remote paths to
//     the data paths of the token's catalogs (RemotePathScope), whatever SQL names
//     them (macros, query(), computed paths)
//   * delta_*()/iceberg_*() (and macros naming them) are reported in ducklake_refs
//     as own_storage_client: their extensions use their own object-store client,
//     so the caller refuses them for catalog-scoped sessions
//   * ducklake_add_data_files (always); other DuckLake functions are checked
//     against the token's catalog access by the caller (see ducklake_refs)
//
// Detection is parser-based (duckdb::Parser — no execution, no connection), so
// it is robust to whitespace/comments/case and to nesting (subqueries, CTEs,
// joins, CTAS, INSERT ... SELECT, COPY (SELECT ...) TO).
//
// KNOWN LIMITATIONS (documented; the airtight fix is the GizmoSQL 2.0 in-binder
// enforcement):
//   * A view that wraps a DuckLake function is refused at creation (Enterprise).
//     A view or macro that wraps any other gated function is not inspected —
//     selecting from it is not caught here, except that the file system still
//     applies to the paths it opens.
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

/// A DuckLake table function / CALL procedure found in a statement, with the
/// catalog it addresses. Used to check the call against the token's catalog
/// access (Enterprise): `ducklake_snapshots('lake')`, `CALL lake.set_option(...)`.
struct DuckLakeFunctionRef {
  std::string function;                // lower-case name, e.g. "ducklake_snapshots"
  std::optional<std::string> catalog;  // nullopt: not a string literal (cannot verify)
  bool writes = false;                 // maintenance/option changes need write access
  /// Spelled `catalog.method(...)` rather than `ducklake_method('catalog', ...)`.
  /// The qualifier may be any schema, so the caller only applies the check when
  /// it names an attached DuckLake catalog (`someschema.settings()` is a user function).
  bool qualified = false;
  /// Found inside a CREATE VIEW. A view runs with the access of whoever selects
  /// from it, not whoever created it, so the caller refuses such a view.
  bool in_view = false;
  /// A function of an extension that talks to object storage through its own
  /// client (delta_scan, iceberg_scan, ...), which the file system wrapper never
  /// sees. The caller refuses it for catalog-scoped sessions; `catalog` is unused.
  bool own_storage_client = false;
};

/// Classify `sql` for the non-admin admin-command gate. Returns a short, stable
/// human-readable category describing the FIRST gated operation found (e.g.
/// "ATTACH", "DETACH", "SET GLOBAL memory_limit", "INSTALL extension",
/// "CHECKPOINT", "COPY TO (local filesystem)", "EXPORT DATABASE (local
/// filesystem)", "read_csv() (local filesystem)", "duckdb_secrets()"), or
/// std::nullopt if the statement is permitted for a non-admin session. A SQL
/// string that fails to parse returns std::nullopt (DuckDB surfaces the real
/// parse error during preparation).
std::optional<std::string> ClassifyGatedCommand(const std::string& sql);

/// `SET allow_unredacted_secrets = <value>` (or the PRAGMA form) for any role,
/// or std::nullopt. Any value that is not a false constant matches (true, 1,
/// 'yes', NOT false, ...). RESET and `= false` / `= 0` are not a match. A parse
/// failure returns std::nullopt.
std::optional<std::string> ClassifyUnredactedSecretsSet(const std::string& sql);

/// True for `SET secret_directory = ...` (or the PRAGMA form), also inside
/// PREPARE / EXPLAIN. With the sensitive-path guard on, clients may not move
/// DuckDB's persistent-secret directory somewhere unprotected.
bool IsSecretDirectorySet(const std::string& sql);

/// As above, additionally collecting every DuckLake function reference found
/// (`ducklake_*()` and `catalog.snapshots()`-style calls) into `ducklake_refs`
/// when it is non-null. Only complete when the result is nullopt.
std::optional<std::string> ClassifyGatedCommand(
    const std::string& sql, std::vector<DuckLakeFunctionRef>* ducklake_refs);

/// Client-facing error text for a gated command, e.g. category "ATTACH".
std::string GatedCommandDeniedMessage(const std::string& category);

/// Convenience wrapper: arrow::Status::OK() if permitted for a non-admin
/// session, otherwise a Flight PermissionDenied (Unauthorized) error naming the
/// gated category.
arrow::Status CheckNonAdminCommandAllowed(const std::string& sql);

}  // namespace gizmosql::ddb
