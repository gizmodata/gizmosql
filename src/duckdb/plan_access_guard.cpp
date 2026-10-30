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

#include "plan_access_guard.h"

#include <algorithm>
#include <cctype>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/execution/operator/scan/physical_table_scan.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/prepared_statement_data.hpp>

#include "duckdb_compat.h"

namespace gizmosql::ddb {

namespace {

namespace dd = duckdb;

std::string ToLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

std::string TableFunctionName(const dd::PhysicalTableScan& scan) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return compat::Str(scan.function.GetName());
#else
  return scan.function.name;
#endif
}

void Collect(const dd::PhysicalOperator& op, std::vector<PlanTableFunctionCall>& out) {
  if (op.type == dd::PhysicalOperatorType::TABLE_SCAN) {
    const auto& scan = op.Cast<dd::PhysicalTableScan>();
    out.push_back({ToLower(TableFunctionName(scan)), scan.parameters});
  }
  // GetChildren() also reports the extra children some operators keep apart
  // (CTE definitions, delim-join sides, ...).
  for (auto& child : op.GetChildren()) Collect(child.get(), out);
}

bool IsVarchar(const dd::Value& value) {
  return !value.IsNull() && value.type().id() == dd::LogicalTypeId::VARCHAR;
}

// The path(s) a file-reading call names: its first argument, a string or a
// list of strings. nullopt if any of it is not a constant string.
std::optional<std::vector<std::string>> PathArguments(const PlanTableFunctionCall& call) {
  if (call.parameters.empty()) return std::nullopt;
  const auto& first = call.parameters[0];
  if (IsVarchar(first)) return std::vector<std::string>{first.GetValue<std::string>()};
  if (first.IsNull() || first.type().id() != dd::LogicalTypeId::LIST) return std::nullopt;
  std::vector<std::string> paths;
  for (const auto& child : dd::ListValue::GetChildren(first)) {
    if (!IsVarchar(child)) return std::nullopt;
    paths.push_back(child.GetValue<std::string>());
  }
  if (paths.empty()) return std::nullopt;
  return paths;
}

}  // namespace

std::vector<PlanTableFunctionCall> CollectTableFunctionCalls(
    const duckdb::PhysicalOperator& root) {
  std::vector<PlanTableFunctionCall> calls;
  Collect(root, calls);
  return calls;
}

std::optional<std::string> ClassifyTableFunctionCall(const PlanTableFunctionCall& call,
                                                     const GateOptions& options,
                                                     const CatalogAccessCheck& catalog_access) {
  const std::string& name = call.name_lower;
  // DuckDB scans the tables of an attached catalog (DuckLake, Postgres,
  // SQLite, ...) with the same table functions a client could call, but never
  // with arguments: those scans are governed by catalog permissions.
  const bool called_directly = !call.parameters.empty();

  if (IsExternalDatabaseFunction(name)) {
    if (called_directly) return name + "() (external database access)";
    return std::nullopt;
  }

  switch (ClassifyDuckLakeFunction(name)) {
    case DuckLakeFunctionAccess::kNotDuckLake:
      break;
    case DuckLakeFunctionAccess::kScan:
      if (called_directly) return name + "()";
      return std::nullopt;
    case DuckLakeFunctionAccess::kAdmin:
      return name + "()";
    case DuckLakeFunctionAccess::kRead:
    case DuckLakeFunctionAccess::kWrite: {
      const bool need_write =
          ClassifyDuckLakeFunction(name) == DuckLakeFunctionAccess::kWrite;
      if (!called_directly || !IsVarchar(call.parameters[0])) {
        // Not naming a catalog we can check: fail closed.
        return name + "() without a constant catalog name";
      }
      const auto catalog = call.parameters[0].GetValue<std::string>();
      if (!catalog_access || !catalog_access(catalog, need_write)) {
        return name + "() on catalog '" + catalog + "' (no " +
               (need_write ? "write" : "read") + " access to that catalog)";
      }
      return std::nullopt;
    }
  }

  if (IsFileReadFunction(name)) {
    auto paths = PathArguments(call);
    if (!paths) return name + "() (local filesystem)";  // not provably remote
    for (const auto& path : *paths) {
      if (!IsProvenRemotePath(path)) return name + "() (local filesystem)";
      if (!options.allow_remote_paths) return name + "() (remote URL)";
    }
    return std::nullopt;
  }

  // Any other reader handed a URL (iceberg_scan, delta_scan, read_xlsx,
  // st_read, ... including autoloaded ones) reaches the network with the
  // server's credentials, like read_parquet would.
  if (!options.allow_remote_paths && called_directly) {
    if (auto paths = PathArguments(call)) {
      for (const auto& path : *paths) {
        if (IsProvenRemotePath(path)) return name + "() (remote URL)";
      }
    }
  }
  return std::nullopt;
}

std::string CanonicalCatalogName(duckdb::ClientContext& context, const std::string& name) {
  try {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto catalog = dd::Catalog::GetCatalogEntry(context, dd::Identifier(name));
#else
    auto catalog = dd::Catalog::GetCatalogEntry(context, name);
#endif
    if (catalog) return compat::Str(catalog->GetName());
  } catch (...) {
    // Not resolvable: check the name as given (it then matches no catalog).
  }
  return name;
}

std::optional<std::string> ClassifyPreparedPlan(duckdb::PreparedStatementData& prepared,
                                                const GateOptions& options,
                                                const CatalogAccessCheck& catalog_access) {
  if (!prepared.physical_plan) return std::nullopt;
  for (const auto& call : CollectTableFunctionCalls(prepared.physical_plan->Root())) {
    if (auto violation = ClassifyTableFunctionCall(call, options, catalog_access)) {
      return violation;
    }
  }
  return std::nullopt;
}

}  // namespace gizmosql::ddb
