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

// Views under access control read with their owner's rights.
//
// DuckDB inlines a view into the statement that reads it, so the plan the
// access check sees no longer says which tables were named by the caller and
// which by a view. A small patch to DuckDB's binder (third_party/
// patch_duckdb.cmake) calls a hook with the plan of every view as it is bound;
// for an access-controlled session the hook wraps that plan in a ViewMarker.
// The check reads the markers (who may read the view, and with whose rights
// its body is checked) and then removes them, before DuckDB optimizes the
// plan. A marker that is somehow left in place refuses to run.
//
// DuckDB 1.4 (the LTS channel) has no such hook: no markers are made, and a
// view's body is checked with the caller's rights.

#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include <duckdb.hpp>
#include <duckdb/planner/column_binding_map.hpp>
#include <duckdb/planner/operator/logical_extension_operator.hpp>

#include "rbac_types.h"

namespace gizmosql::enterprise::rbac {

class ViewMarker : public duckdb::LogicalExtensionOperator {
 public:
  ViewMarker(ObjectPath view, bool temporary, std::string definition_hash,
             std::vector<std::string> column_names, duckdb::unique_ptr<duckdb::LogicalOperator> plan);

  ObjectPath view;
  bool temporary;
  std::string definition_hash;
  // The view's output column names, by position (empty if unknown).
  std::vector<std::string> column_names;

  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() override;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  duckdb::TableIndex GetRootIndex() override;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const override;
#elif GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
  duckdb::idx_t GetRootIndex() override;
  duckdb::vector<duckdb::idx_t> GetTableIndex() const override;
#else
  duckdb::vector<duckdb::idx_t> GetTableIndex() const override;
#endif
  duckdb::PhysicalOperator& CreatePlan(duckdb::ClientContext& context,
                                       duckdb::PhysicalPlanGenerator& planner) override;
  std::string GetExtensionName() const override { return "gizmosql_view"; }
  std::string GetName() const override { return "GIZMOSQL_VIEW"; }

 protected:
  void ResolveTypes() override;
};

// Point DuckDB's view-binding hook at access control (once, at startup).
// Returns false when the linked DuckDB has no hook (DuckDB 1.4).
bool InstallViewHook();

// For each view marker in the plan: the view's columns the rest of the
// statement uses. All of them when that cannot be told apart.
std::unordered_map<const ViewMarker*, std::vector<std::string>> ViewColumnsUsed(
    duckdb::LogicalOperator& plan);

// Replace every view marker in the plan with the view's own plan.
void RemoveViewMarkers(duckdb::unique_ptr<duckdb::LogicalOperator>& plan);

}  // namespace gizmosql::enterprise::rbac
