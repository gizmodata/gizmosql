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
//   * COPY ... TO/FROM a LOCAL file (remote object-storage/HTTP paths allowed)
//   * EXPORT DATABASE / IMPORT DATABASE (ANY destination — full-database
//     egress/ingress is not bounded by object grants, so it is always gated)
//   * read_*() / glob() / sniff_csv() table functions reading the LOCAL
//     filesystem (remote object-storage/HTTP paths allowed), at any nesting
//   * CREATE SECRET / DROP SECRET (all variants: OR REPLACE / PERSISTENT /
//     TEMPORARY — secrets hold credentials)
//   * duckdb_secrets() (always — it exposes secret material)
//
// Detection is parser-based (duckdb::Parser — no execution, no connection), so
// it is robust to whitespace/comments/case and to nesting (subqueries, CTEs,
// joins, CTAS, INSERT ... SELECT, COPY (SELECT ...) TO).
//
// KNOWN LIMITATIONS (documented; the airtight fix is the GizmoSQL 2.0 in-binder
// enforcement):
//   * A view or macro that wraps a gated function is not inspected — selecting
//     from it is not caught here.
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

/// Classify `sql` for the non-admin admin-command gate. Returns a short, stable
/// human-readable category describing the FIRST gated operation found (e.g.
/// "ATTACH", "DETACH", "SET GLOBAL memory_limit", "INSTALL extension",
/// "CHECKPOINT", "COPY TO (local filesystem)", "EXPORT DATABASE (local
/// filesystem)", "read_csv() (local filesystem)", "duckdb_secrets()"), or
/// std::nullopt if the statement is permitted for a non-admin session. A SQL
/// string that fails to parse returns std::nullopt (DuckDB surfaces the real
/// parse error during preparation).
std::optional<std::string> ClassifyGatedCommand(const std::string& sql);

/// Convenience wrapper: arrow::Status::OK() if permitted for a non-admin
/// session, otherwise a Flight PermissionDenied (Unauthorized) error naming the
/// gated category.
arrow::Status CheckNonAdminCommandAllowed(const std::string& sql);

}  // namespace gizmosql::ddb
