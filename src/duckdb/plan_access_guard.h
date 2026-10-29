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

// Bind-time access guard for non-admin sessions (Core).
//
// The parse-time gate (admin_command_guard.h) sees only the SQL text the
// client sent. What a statement actually runs is decided at bind time: a view
// or macro can wrap postgres_query() or read_csv(), and a prepared statement's
// `?` parameter can supply the catalog that ducklake_snapshots(?) reads. This
// guard inspects the BOUND physical plan instead, where every table function
// call appears with its final, constant arguments however the SQL spelled it.
//
// It runs from a duckdb::ClientContextState registered on every non-admin
// session's connection for the session's whole life (see
// InstallPlanAccessGuard in duckdb_statement.h), so it covers statements
// prepared by Create(), the multi-statement direct-execution fallback, and
// re-binds DuckDB performs at execution time (parameter values, catalog
// changes) — DuckDB 2.x plans a statement with unbound parameters only then.
//
// For each table function call in the plan it refuses:
//   * an external-database scanner called with arguments (postgres_query,
//     postgres_execute, postgres_scan('dsn', ...), mysql_*, sqlite_*). Scans
//     of an ATTACHed catalog's tables use the same functions but carry no
//     arguments, and are governed by catalog permissions like any table.
//   * ducklake_scan called with arguments (a direct call reads arbitrary files)
//   * admin-only DuckLake functions (ducklake_add_data_files, ducklake_commit)
//   * any other ducklake_*() function unless the session has read (metadata
//     readers) or write (maintenance, options, tags) access to the catalog its
//     first argument names. These functions look the catalog up themselves, so
//     DuckDB does not report it in the statement's read/write databases.
//   * a file-reading function (read_csv, read_parquet, glob, ...) whose path is
//     local, or not a constant string (fail closed)
//   * any table function whose first argument is a remote URL, unless the
//     server allows non-admin remote access
//
// The check runs after binding and before execution. Binding has no lasting
// effect for these functions (postgres_query prepares and describes its SQL on
// the remote server but runs it only at execution), and the parse-time gate
// refuses direct calls before they are bound at all.

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include <duckdb.hpp>

#include "admin_command_guard.h"

namespace duckdb {
class ClientContext;
class PhysicalOperator;
struct PreparedStatementData;
}  // namespace duckdb

namespace gizmosql::ddb {

/// One table function call found in a bound physical plan.
struct PlanTableFunctionCall {
  std::string name_lower;
  duckdb::vector<duckdb::Value> parameters;  // positional arguments, as bound
};

/// Every table function scan in the plan rooted at `root`, depth first.
std::vector<PlanTableFunctionCall> CollectTableFunctionCalls(
    const duckdb::PhysicalOperator& root);

/// Decides catalog access for a ducklake_*() call: true if the session may
/// read (`need_write` false) or write (`need_write` true) `catalog`.
using CatalogAccessCheck =
    std::function<bool(const std::string& catalog, bool need_write)>;

/// Classify one call for a non-admin session. Returns a short category for the
/// first refused use (as GatedCommandDeniedMessage() expects), or nullopt.
std::optional<std::string> ClassifyTableFunctionCall(const PlanTableFunctionCall& call,
                                                     const GateOptions& options,
                                                     const CatalogAccessCheck& catalog_access);

/// The name of the attached catalog that `name` resolves to, as DuckDB spells
/// it (DuckDB resolves catalog names case-insensitively; catalog_access rules
/// match case-sensitively, so a rule must be checked against this spelling).
/// `name` itself when no attached catalog matches.
std::string CanonicalCatalogName(duckdb::ClientContext& context, const std::string& name);

/// Classify a prepared statement's bound plan for a non-admin session. A
/// statement with no physical plan yet (unbound parameters) returns nullopt:
/// DuckDB plans it again, with values, before it can execute.
std::optional<std::string> ClassifyPreparedPlan(duckdb::PreparedStatementData& prepared,
                                                const GateOptions& options,
                                                const CatalogAccessCheck& catalog_access);

}  // namespace gizmosql::ddb
