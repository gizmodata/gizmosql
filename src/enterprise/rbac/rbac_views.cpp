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

#include "rbac_views.h"

#include <functional>

#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>

#include "duckdb_compat.h"
#include "rbac_duckdb.h"
#include "rbac_enforcement.h"

#if GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
namespace duckdb {
// Defined by third_party/patch_duckdb.cmake in DuckDB's binder.
extern void (*gizmosql_bound_view_hook)(ClientContext& context, ViewCatalogEntry& view,
                                        unique_ptr<LogicalOperator>& plan);
}  // namespace duckdb
#endif

namespace gizmosql::enterprise::rbac {

namespace dd = duckdb;
namespace compat = gizmosql::ddb::compat;
using OpType = dd::LogicalOperatorType;

ViewMarker::ViewMarker(ObjectPath view_p, bool temporary_p, std::string definition_hash_p,
                       std::vector<std::string> column_names_p,
                       dd::unique_ptr<dd::LogicalOperator> plan)
    : view(std::move(view_p)),
      temporary(temporary_p),
      definition_hash(std::move(definition_hash_p)),
      column_names(std::move(column_names_p)) {
  types = plan->types;
  children.push_back(std::move(plan));
}

dd::vector<dd::ColumnBinding> ViewMarker::GetColumnBindings() {
  return children[0]->GetColumnBindings();
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
dd::TableIndex ViewMarker::GetRootIndex() { return children[0]->GetRootIndex(); }
dd::vector<dd::TableIndex> ViewMarker::GetTableIndex() const { return children[0]->GetTableIndex(); }
#elif GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
dd::idx_t ViewMarker::GetRootIndex() { return children[0]->GetRootIndex(); }
dd::vector<dd::idx_t> ViewMarker::GetTableIndex() const { return children[0]->GetTableIndex(); }
#else
dd::vector<dd::idx_t> ViewMarker::GetTableIndex() const { return children[0]->GetTableIndex(); }
#endif

void ViewMarker::ResolveTypes() { types = children[0]->types; }

dd::PhysicalOperator& ViewMarker::CreatePlan(dd::ClientContext&, dd::PhysicalPlanGenerator&) {
  // The access check removes every marker before the plan is optimized; a
  // marker reaching execution means the check did not run on this plan.
  throw dd::PermissionException(
      "GizmoSQL blocked this statement: the view " + view.ToString() +
      " was not checked against your grants");
}

namespace {

#if GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
void OnViewBound(dd::ClientContext& context, dd::ViewCatalogEntry& view,
                 dd::unique_ptr<dd::LogicalOperator>& plan) {
  if (!plan) return;
  auto state = context.registered_state->Get<RbacSessionState>(kRbacSessionStateName);
  if (!state || state->unchecked()) return;  // only access-controlled statements carry markers
  ObjectPath path = PathOfEntry(view);
  const bool temporary = view.temporary || IdentifiersEqual(path.catalog, TEMP_CATALOG);
  plan = dd::make_uniq<ViewMarker>(std::move(path), temporary, ViewDefinitionHash(view.GetQuery()),
                                   ViewColumnNames(view), std::move(plan));
}
#endif

bool IsViewMarker(const dd::LogicalOperator& op) {
  return op.type == OpType::LOGICAL_EXTENSION_OPERATOR &&
         dynamic_cast<const ViewMarker*>(&op) != nullptr;
}

// Operators whose output carries their children's columns through unchanged
// (filters, sorts, limits, joins): what they pass on is "used" only if a
// later operator uses it. Projections and aggregates use exactly what their
// expressions name. Every other operator takes its children's columns by
// position (set operations, INSERT, CREATE TABLE AS, ...), so it uses them all.
bool PassesColumnsThrough(const dd::LogicalOperator& op) {
  switch (op.type) {
    case OpType::LOGICAL_FILTER:
    case OpType::LOGICAL_LIMIT:
    case OpType::LOGICAL_ORDER_BY:
    case OpType::LOGICAL_TOP_N:
    case OpType::LOGICAL_SAMPLE:
    case OpType::LOGICAL_DISTINCT:
    case OpType::LOGICAL_WINDOW:
    case OpType::LOGICAL_UNNEST:
    case OpType::LOGICAL_JOIN:
    case OpType::LOGICAL_DELIM_JOIN:
    case OpType::LOGICAL_COMPARISON_JOIN:
    case OpType::LOGICAL_ANY_JOIN:
    case OpType::LOGICAL_CROSS_PRODUCT:
    case OpType::LOGICAL_POSITIONAL_JOIN:
    case OpType::LOGICAL_ASOF_JOIN:
    case OpType::LOGICAL_DEPENDENT_JOIN:
    case OpType::LOGICAL_EXPLAIN:
    case OpType::LOGICAL_PREPARE:
    case OpType::LOGICAL_EXECUTE:
    case OpType::LOGICAL_PROJECTION:
    case OpType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      return true;
    case OpType::LOGICAL_EXTENSION_OPERATOR:
      return IsViewMarker(op);
    default:
      return false;
  }
}

void CollectColumnRefs(const dd::Expression& expr, dd::column_binding_set_t& used) {
  if (expr.GetExpressionClass() == dd::ExpressionClass::BOUND_COLUMN_REF) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    used.insert(expr.Cast<dd::BoundColumnRefExpression>().Binding());
#else
    used.insert(expr.Cast<dd::BoundColumnRefExpression>().binding);
#endif
  }
  dd::ExpressionIterator::EnumerateChildren(
      expr, [&](const dd::Expression& child) { CollectColumnRefs(child, used); });
}

// Every column binding that operators outside `skip`'s subtree use.
void CollectUses(dd::LogicalOperator& op, const dd::LogicalOperator* skip,
                 dd::column_binding_set_t& used) {
  if (&op == skip) return;
  dd::LogicalOperatorVisitor::EnumerateExpressions(op, [&](dd::unique_ptr<dd::Expression>* e) {
    if (e && *e) CollectColumnRefs(**e, used);
  });
  const bool positional = !PassesColumnsThrough(op);
  for (auto& child : op.children) {
    if (!child) continue;
    if (positional) {
      for (const auto& b : child->GetColumnBindings()) used.insert(b);
    }
    CollectUses(*child, skip, used);
  }
}

void FindMarkers(dd::LogicalOperator& op, std::vector<ViewMarker*>& markers) {
  if (IsViewMarker(op)) markers.push_back(&op.Cast<ViewMarker>());
  for (auto& child : op.children) {
    if (child) FindMarkers(*child, markers);
  }
}

}  // namespace

bool UsesChildColumnsByPosition(const dd::LogicalOperator& op) { return !PassesColumnsThrough(op); }

dd::vector<dd::ColumnBinding> ResultColumns(dd::LogicalOperator& plan) {
  dd::LogicalOperator* root = &plan;
  while ((root->type == OpType::LOGICAL_PREPARE || root->type == OpType::LOGICAL_EXECUTE ||
          root->type == OpType::LOGICAL_EXPLAIN) &&
         !root->children.empty() && root->children[0]) {
    root = root->children[0].get();
  }
  // Operators that only pass on (or compute) columns of the plan: their
  // bindings are real. Anything else reports a placeholder binding.
  if (root->type == OpType::LOGICAL_GET || root->type == OpType::LOGICAL_PROJECTION ||
      root->type == OpType::LOGICAL_AGGREGATE_AND_GROUP_BY || PassesColumnsThrough(*root)) {
    return root->GetColumnBindings();
  }
  return {};
}

bool InstallViewHook() {
#if GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
  dd::gizmosql_bound_view_hook = &OnViewBound;
  return true;
#else
  return false;
#endif
}

std::unordered_map<const ViewMarker*, std::vector<std::string>> ViewColumnsUsed(
    dd::LogicalOperator& plan) {
  std::unordered_map<const ViewMarker*, std::vector<std::string>> result;
  std::vector<ViewMarker*> markers;
  FindMarkers(plan, markers);
  for (auto* marker : markers) {
    std::vector<std::string> columns;
    try {
      dd::column_binding_set_t used;
      for (const auto& b : ResultColumns(plan)) used.insert(b);  // what the statement returns
      CollectUses(plan, marker, used);
      const auto bindings = marker->GetColumnBindings();
      if (bindings.size() != marker->column_names.size()) {
        columns = marker->column_names;  // cannot tell which: all of them
      } else {
        for (size_t i = 0; i < bindings.size(); ++i) {
          if (used.count(bindings[i])) columns.push_back(marker->column_names[i]);
        }
      }
    } catch (...) {
      columns = marker->column_names;
    }
    result[marker] = std::move(columns);
  }
  return result;
}

void RemoveViewMarkers(dd::unique_ptr<dd::LogicalOperator>& plan) {
  if (!plan) return;
  while (IsViewMarker(*plan)) {
    auto inner = std::move(plan->children[0]);
    plan = std::move(inner);
  }
  for (auto& child : plan->children) RemoveViewMarkers(child);
}

}  // namespace gizmosql::enterprise::rbac
