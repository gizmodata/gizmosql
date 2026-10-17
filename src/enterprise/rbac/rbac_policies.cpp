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

#include "rbac_policies.h"

#include <functional>
#include <unordered_set>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/column_binding_map.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/where_binder.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/logical_operator_visitor.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>

#include "duckdb_compat.h"
#include "rbac_duckdb.h"
#include "rbac_views.h"

namespace gizmosql::enterprise::rbac {

namespace dd = duckdb;
namespace compat = gizmosql::ddb::compat;
using OpType = dd::LogicalOperatorType;

namespace {

std::string Lower(std::string s) {
  for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

[[noreturn]] void Refuse(const std::string& what) {
  throw dd::PermissionException("GizmoSQL blocked this statement: " + what);
}

// ---- parse-tree helpers (DuckDB 1.x and 2.x) -------------------------------

std::string FunctionName(const dd::FunctionExpression& fe) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return Lower(compat::Str(fe.FunctionName()));
#else
  return Lower(fe.function_name);
#endif
}

bool HasSchema(const dd::FunctionExpression& fe) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return fe.GetQualifiedName().Path().size() > 1;
#else
  return !fe.schema.empty() || !fe.catalog.empty();
#endif
}

std::vector<const dd::ParsedExpression*> Arguments(const dd::FunctionExpression& fe) {
  std::vector<const dd::ParsedExpression*> args;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  for (const auto& a : fe.GetArguments()) args.push_back(&a.GetExpression());
#else
  for (const auto& c : fe.children) args.push_back(c.get());
#endif
  return args;
}

std::vector<std::string> ColumnNames(const dd::ColumnRefExpression& ref) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto names = compat::Strs(ref.ColumnNames());
  return std::vector<std::string>(names.begin(), names.end());
#else
  return ref.column_names;
#endif
}

dd::unique_ptr<dd::ParsedExpression> StringConstant(const std::string& s) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return dd::make_uniq<dd::ConstantExpression>(dd::Literal::String(s));
#else
  return dd::make_uniq<dd::ConstantExpression>(dd::Value(s));
#endif
}

dd::unique_ptr<dd::ParsedExpression> BoolConstant(bool b) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return dd::make_uniq<dd::ConstantExpression>(dd::Literal::Boolean(b));
#else
  return dd::make_uniq<dd::ConstantExpression>(dd::Value::BOOLEAN(b));
#endif
}

dd::unique_ptr<dd::ParsedExpression> NullConstant() {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return dd::make_uniq<dd::ConstantExpression>(dd::Literal::Null());
#else
  return dd::make_uniq<dd::ConstantExpression>(dd::Value());
#endif
}

// The constant string argument of a context function, or nullopt.
std::optional<std::string> ConstantStringArgument(const dd::FunctionExpression& fe) {
  const auto args = Arguments(fe);
  if (args.size() != 1 ||
      args[0]->GetExpressionClass() != dd::ExpressionClass::CONSTANT) {
    return std::nullopt;
  }
  const auto value = compat::ConstantValue(args[0]->Cast<dd::ConstantExpression>());
  if (value.IsNull() || value.type().id() != dd::LogicalTypeId::VARCHAR)
    return std::nullopt;
  return value.GetValue<std::string>();
}

// Context functions: names a policy may call, all resolved by GizmoSQL.
bool IsUserFunction(const std::string& n) {
  return n == "gizmosql_user" || n == "current_user" || n == "session_user" ||
         n == "user";
}
bool IsContextFunction(const std::string& n) {
  return IsUserFunction(n) || n == "is_role_active" || n == "session_attribute";
}

// What a policy must not read: anything the session can set, or anything that
// reaches outside the row.
bool IsForbiddenFunction(const std::string& n) {
  static const std::unordered_set<std::string> kNames = {
      "getvariable",   "current_setting", "query",   "query_table", "current_role",
      "gizmosql_role", "nextval",         "currval", "setseed"};
  return kNames.count(n) > 0;
}

arrow::Status Invalid(const std::string& what) {
  return arrow::Status::Invalid("GizmoSQL access control: " + what);
}

// Check one parsed policy expression (recursively).
arrow::Status CheckShape(const dd::ParsedExpression& expr,
                         const std::string& mask_parameter) {
  switch (expr.GetExpressionClass()) {
    case dd::ExpressionClass::SUBQUERY:
      return Invalid("a policy cannot contain a subquery");
    case dd::ExpressionClass::WINDOW:
      return Invalid("a policy cannot contain a window function");
    case dd::ExpressionClass::PARAMETER:
      return Invalid("a policy cannot contain parameters or variables");
    case dd::ExpressionClass::STAR:
    case dd::ExpressionClass::DEFAULT:
    case dd::ExpressionClass::POSITIONAL_REFERENCE:
      return Invalid("a policy must be a plain expression");
    case dd::ExpressionClass::FUNCTION: {
      const auto& fe = expr.Cast<dd::FunctionExpression>();
      const std::string name = FunctionName(fe);
      if (IsForbiddenFunction(name)) {
        return Invalid("a policy cannot call " + name + "()");
      }
      if (IsContextFunction(name)) {
        if (HasSchema(fe))
          return Invalid("context functions are called without a schema");
        if (IsUserFunction(name)) {
          if (!Arguments(fe).empty()) return Invalid(name + "() takes no arguments");
          return arrow::Status::OK();
        }
        if (!ConstantStringArgument(fe)) {
          return Invalid(name + "() takes one string constant");
        }
        return arrow::Status::OK();
      }
      break;
    }
    case dd::ExpressionClass::COLUMN_REF: {
      const auto names = ColumnNames(expr.Cast<dd::ColumnRefExpression>());
      if (names.size() == 1 && IsUserFunction(Lower(names[0])))
        return arrow::Status::OK();
      if (!mask_parameter.empty() &&
          !(names.size() == 1 && IdentifiersEqual(names[0], mask_parameter))) {
        return Invalid("a masking policy may read only its parameter " + mask_parameter);
      }
      break;
    }
    default:
      break;
  }
  arrow::Status status = arrow::Status::OK();
  dd::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](const dd::ParsedExpression& child) {
        if (status.ok()) status = CheckShape(child, mask_parameter);
      });
  return status;
}

arrow::Result<dd::unique_ptr<dd::ParsedExpression>> ParseOne(const std::string& sql) {
  dd::vector<dd::unique_ptr<dd::ParsedExpression>> list;
  try {
    auto parser = compat::StandaloneParser();
    list = parser.ParseExpressionList(sql);
  } catch (const std::exception& e) {
    return Invalid(std::string("the policy is not a valid expression: ") + e.what());
  }
  if (list.size() != 1) return Invalid("a policy must be exactly one expression");
  return std::move(list[0]);
}

// Replace context functions (and a masking parameter) inside `expr`.
void Substitute(dd::unique_ptr<dd::ParsedExpression>& expr, const PolicyContext& context,
                const std::string& mask_parameter,
                const dd::ParsedExpression* parameter_value) {
  if (expr->GetExpressionClass() == dd::ExpressionClass::FUNCTION) {
    const auto& fe = expr->Cast<dd::FunctionExpression>();
    const std::string name = FunctionName(fe);
    if (IsUserFunction(name)) {
      expr = StringConstant(context.user_name);
      return;
    }
    if (name == "is_role_active") {
      expr = BoolConstant(
          context.role_names.count(FoldIdentifier(*ConstantStringArgument(fe))) > 0);
      return;
    }
    if (name == "session_attribute") {
      auto it = context.attributes.find(*ConstantStringArgument(fe));
      expr = it == context.attributes.end() ? NullConstant() : StringConstant(it->second);
      return;
    }
  }
  if (expr->GetExpressionClass() == dd::ExpressionClass::COLUMN_REF) {
    const auto names = ColumnNames(expr->Cast<dd::ColumnRefExpression>());
    if (names.size() == 1 && IsUserFunction(Lower(names[0]))) {
      expr = StringConstant(context.user_name);
      return;
    }
    if (parameter_value && names.size() == 1 &&
        IdentifiersEqual(names[0], mask_parameter)) {
      expr = parameter_value->Copy();
      return;
    }
  }
  dd::ParsedExpressionIterator::EnumerateChildren(
      *expr, [&](dd::unique_ptr<dd::ParsedExpression>& child) {
        Substitute(child, context, mask_parameter, parameter_value);
      });
}

// A bound policy expression may not have reached anything the parse check
// could not see (a macro's body, say).
void CheckBound(const dd::Expression& expr) {
  if (expr.GetExpressionClass() == dd::ExpressionClass::BOUND_SUBQUERY) {
    Refuse("a row or masking policy on a table it reads contains a subquery");
  }
  if (expr.GetExpressionClass() == dd::ExpressionClass::BOUND_FUNCTION) {
    const auto& fn = expr.Cast<dd::BoundFunctionExpression>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    const std::string name = Lower(compat::Str(fn.Function().GetName()));
#else
    const std::string name = Lower(fn.function.name);
#endif
    if (IsForbiddenFunction(name)) {
      Refuse("a row or masking policy on a table it reads calls " + name + "()");
    }
  }
  dd::ExpressionIterator::EnumerateChildren(
      expr, [](const dd::Expression& child) { CheckBound(child); });
}

// ---- the plan rewrite --------------------------------------------------------

bool ReadsSettings(dd::LogicalOperator& op) {
  if (op.type == OpType::LOGICAL_GET) {
    auto& get = op.Cast<dd::LogicalGet>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    const std::string function = Lower(compat::Str(get.function.GetName()));
#else
    const std::string function = Lower(get.function.name);
#endif
    if (!get.GetTable() && function == "duckdb_settings") return true;
  }
  for (auto& child : op.children) {
    if (child && ReadsSettings(*child)) return true;
  }
  return false;
}

class PolicyRewriter {
 public:
  PolicyRewriter(dd::ClientContext& context, const Snapshot& snapshot,
                 const Principal& principal)
      : context_(context),
        snapshot_(snapshot),
        principal_(principal),
        policy_context_(PolicyContext::For(snapshot, principal)),
        tenant_user_(!snapshot.TenantOfPrincipal(principal).empty()),
        manage_config_(
            snapshot.HasSystemPrivilege(principal, SystemPrivilege::kManageConfig)) {}

  void Rewrite(dd::unique_ptr<dd::LogicalOperator>& plan) {
    Walk(plan);
    if (masks_.empty()) return;
    SubstituteMasks(*plan);
    for (const auto& b : ResultColumns(*plan)) {
      if (masks_.count(b))
        Refuse("a masked column is returned in a way GizmoSQL cannot mask");
    }
    CheckPositionalUse(*plan);
  }

 private:
  bool Governed(const ObjectPath& path) {
    return !snapshot_.RowPoliciesOn(path).empty() ||
           !snapshot_.ApplicableMasks(principal_, path).empty();
  }

  void RefuseWriteInto(dd::TableCatalogEntry& table, const char* verb) {
    const ObjectPath path = PathOfEntry(table);
    if (IdentifiersEqual(path.catalog, TEMP_CATALOG)) return;
    if (Governed(path)) {
      Refuse(std::string(verb) + " on " + path.ToString() +
             ", which has row or masking policies, is not available under access control "
             "yet");
    }
  }

  void Walk(dd::unique_ptr<dd::LogicalOperator>& op) {
    for (auto& child : op->children) {
      if (child) Walk(child);
    }
    switch (op->type) {
      case OpType::LOGICAL_INSERT:
        RefuseWriteInto(op->Cast<dd::LogicalInsert>().table, "INSERT");
        break;
      case OpType::LOGICAL_UPDATE:
        RefuseWriteInto(op->Cast<dd::LogicalUpdate>().table, "UPDATE");
        break;
      case OpType::LOGICAL_MERGE_INTO:
        RefuseWriteInto(op->Cast<dd::LogicalMergeInto>().table, "MERGE");
        break;
      case OpType::LOGICAL_DELETE: {
        // DELETE ... RETURNING hands back whole rows, masked columns included.
        auto& del = op->Cast<dd::LogicalDelete>();
        const ObjectPath path = PathOfEntry(del.table);
        if (del.return_chunk && !snapshot_.ApplicableMasks(principal_, path).empty()) {
          Refuse("DELETE ... RETURNING on " + path.ToString() +
                 ", which has masked columns");
        }
        break;
      }
      case OpType::LOGICAL_GET:
        RewriteGet(op);
        break;
      default:
        break;
    }
  }

  // The metadata table functions a tenant user may call list every attached
  // catalog; keep the rows of the catalogs the user can see. This covers
  // information_schema, SHOW and GizmoSQL's own helper views too, which are
  // built on them, however the statement reaches them.
  void RestrictMetadataRows(dd::unique_ptr<dd::LogicalOperator>& op,
                            const std::string& function) {
    std::vector<std::string> visible;
    for (const auto& db : dd::DatabaseManager::Get(context_).GetDatabases()) {
      const std::string name = compat::Str(db->GetName());
      if (snapshot_.CatalogVisible(principal_, name)) visible.push_back(name);
    }
    FilterByColumn(op, function, "database_name", visible, /*keep_listed=*/true,
                   "limited to user " + principal_.user_name + "'s catalogs");
  }

  // Keep (or drop) the rows of a table function scan whose `column` is one of
  // `values`.
  void FilterByColumn(dd::unique_ptr<dd::LogicalOperator>& op,
                      const std::string& function, const std::string& column_name,
                      const std::vector<std::string>& values, bool keep_listed,
                      const std::string& what) {
    auto& get = op->Cast<dd::LogicalGet>();
    dd::idx_t column = dd::DConstants::INVALID_INDEX;
    for (dd::idx_t i = 0; i < get.names.size(); ++i) {
      if (IdentifiersEqual(compat::Str(get.names[i]), column_name)) column = i;
    }
    if (column == dd::DConstants::INVALID_INDEX) {
      Refuse("the table function " + function + " cannot be " + what);
    }
    if (!keep_listed && values.empty()) return;
    auto& column_ids = get.GetMutableColumnIds();
    const size_t original_columns = column_ids.size();
    std::optional<size_t> position;
    for (size_t p = 0; p < original_columns; ++p) {
      if (!column_ids[p].IsRowIdColumn() && !column_ids[p].IsVirtualColumn() &&
          column_ids[p].GetPrimaryIndex() == column) {
        position = p;
      }
    }
    if (!position) {
      column_ids.emplace_back(column);
      position = column_ids.size() - 1;
    }
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    const dd::ColumnBinding binding(get.table_index, dd::ProjectionIndex(*position));
#else
    const dd::ColumnBinding binding(get.table_index, *position);
#endif
    dd::unique_ptr<dd::Expression> predicate;
    if (values.empty()) {
      predicate = dd::make_uniq<dd::BoundConstantExpression>(dd::Value::BOOLEAN(false));
    } else {
      auto in = dd::make_uniq<dd::BoundOperatorExpression>(
          keep_listed ? dd::ExpressionType::COMPARE_IN
                      : dd::ExpressionType::COMPARE_NOT_IN,
          dd::LogicalType::BOOLEAN);
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      auto& in_children = in->GetChildrenMutable();
#else
      auto& in_children = in->children;
#endif
      in_children.push_back(dd::make_uniq<dd::BoundColumnRefExpression>(
          get.returned_types[column], binding));
      for (const auto& v : values) {
        in_children.push_back(dd::make_uniq<dd::BoundConstantExpression>(dd::Value(v)));
      }
      predicate = std::move(in);
    }
    auto filter = dd::make_uniq<dd::LogicalFilter>(std::move(predicate));
    if (column_ids.size() > original_columns) {
      for (size_t i = 0; i < original_columns; ++i) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
        filter->projection_map.push_back(dd::ProjectionIndex(i));
#else
        filter->projection_map.push_back(i);
#endif
      }
    }
    filter->children.push_back(std::move(op));
    op = std::move(filter);
    policy_filters_.insert(op.get());
  }

  void RewriteGet(dd::unique_ptr<dd::LogicalOperator>& op) {
    auto& get = op->Cast<dd::LogicalGet>();
    auto table = get.GetTable();
    if (!table) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      const std::string function = Lower(compat::Str(get.function.GetName()));
#else
      const std::string function = Lower(get.function.name);
#endif
      static const std::unordered_set<std::string> kCatalogListing = {
          "duckdb_databases", "duckdb_tables",       "duckdb_views",
          "duckdb_columns",   "duckdb_constraints",  "duckdb_functions",
          "duckdb_indexes",   "duckdb_schemas",      "duckdb_sequences",
          "duckdb_types",     "pragma_database_size"};
      if (tenant_user_ && kCatalogListing.count(function))
        RestrictMetadataRows(op, function);
      // Credentials in settings (a proxy password, keys) are for MANAGE CONFIG.
      if (function == "duckdb_settings" && !manage_config_) {
        FilterByColumn(op, function, "name", CredentialSettingNames(context_),
                       /*keep_listed=*/false,
                       "filtered for user " + principal_.user_name);
      }
      return;
    }
    const ObjectPath path = PathOfEntry(*table);
    if (IdentifiersEqual(path.catalog, TEMP_CATALOG)) return;
    const auto policies = snapshot_.RowPoliciesOn(path);
    const auto masks = snapshot_.ApplicableMasks(principal_, path);
    if (policies.empty() && masks.empty()) return;

    // Bind policy SQL against this scan, as DuckDB's binder would: column
    // references resolve to the scan's columns (and add any it lacks).
    auto binder = dd::Binder::CreateBinder(context_);
    auto& column_ids = get.GetMutableColumnIds();
    const size_t original_columns = column_ids.size();
    const std::string alias = compat::Str(table->name);
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    dd::vector<dd::Identifier> names;
    dd::vector<dd::LogicalType> types;
    for (auto& col : table->GetColumns().Logical()) {
      names.push_back(col.Name());
      types.push_back(col.Type());
    }
    binder->bind_context.AddBaseTable(get.table_index, dd::Identifier(alias), names,
                                      types, column_ids, *table);
#else
    dd::vector<std::string> names;
    dd::vector<dd::LogicalType> types;
    for (auto& col : table->GetColumns().Logical()) {
      names.push_back(col.Name());
      types.push_back(col.Type());
    }
    binder->bind_context.AddBaseTable(get.table_index, alias, names, types, column_ids,
                                      *table);
#endif
    auto bind = [&](dd::unique_ptr<dd::ParsedExpression> parsed,
                    const dd::LogicalType& type) {
      dd::WhereBinder where_binder(*binder, context_);
      where_binder.target_type =
          type;  // a mask keeps its column's type; a predicate is BOOLEAN
      auto bound = where_binder.Bind(parsed);
      bound = dd::BoundCastExpression::AddCastToType(context_, std::move(bound), type);
      CheckBound(*bound);
      return bound;
    };

    // Masks: only for columns this scan reads.
    for (const auto& [column, policy] : masks) {
      if (!policy)
        Refuse("a masking policy on " + path.ToString() + "." + column + " is missing");
      dd::idx_t logical = dd::DConstants::INVALID_INDEX;
      dd::LogicalType column_type;
      for (auto& col : table->GetColumns().Logical()) {
        if (IdentifiersEqual(compat::Str(col.Name()), column)) {
          logical = col.Logical().index;
          column_type = col.Type();
        }
      }
      if (logical == dd::DConstants::INVALID_INDEX) {
        Refuse("the masked column " + column + " of " + path.ToString() +
               " no longer exists");
      }
      std::optional<size_t> position;
      for (size_t p = 0; p < original_columns; ++p) {
        if (!column_ids[p].IsRowIdColumn() && !column_ids[p].IsVirtualColumn() &&
            column_ids[p].GetPrimaryIndex() == logical) {
          position = p;
        }
      }
      if (!position) continue;  // not read by this statement
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      auto parameter = dd::make_uniq<dd::ColumnRefExpression>(dd::Identifier(column),
                                                              dd::Identifier(alias));
#else
      auto parameter = dd::make_uniq<dd::ColumnRefExpression>(column, alias);
#endif
      auto parsed = PreparePolicyExpression(policy->body, policy_context_,
                                            policy->parameter, parameter.get());
      if (!parsed.ok()) Refuse(parsed.status().message());
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      const dd::ColumnBinding binding(get.table_index, dd::ProjectionIndex(*position));
#else
      const dd::ColumnBinding binding(get.table_index, *position);
#endif
      masks_[binding] = bind(std::move(*parsed), column_type);
    }

    // Row policies: permissive ones OR-ed, restrictive ones AND-ed; nothing
    // permissive that applies means no rows.
    if (policies.empty()) return;
    std::vector<dd::unique_ptr<dd::ParsedExpression>> permissive, restrictive;
    for (const auto* p : snapshot_.ApplicableRowPolicies(principal_, path)) {
      auto parsed = PreparePolicyExpression(p->predicate, policy_context_);
      if (!parsed.ok()) Refuse(parsed.status().message());
      (p->permissive ? permissive : restrictive).push_back(std::move(*parsed));
    }
    dd::unique_ptr<dd::ParsedExpression> predicate;
    if (permissive.empty()) {
      predicate = BoolConstant(false);
    } else if (permissive.size() == 1) {
      predicate = std::move(permissive[0]);
    } else {
      dd::vector<dd::unique_ptr<dd::ParsedExpression>> children;
      for (auto& p : permissive) children.push_back(std::move(p));
      predicate = dd::make_uniq<dd::ConjunctionExpression>(
          dd::ExpressionType::CONJUNCTION_OR, std::move(children));
    }
    for (auto& r : restrictive) {
      predicate = dd::make_uniq<dd::ConjunctionExpression>(
          dd::ExpressionType::CONJUNCTION_AND, std::move(predicate), std::move(r));
    }
    auto filter = dd::make_uniq<dd::LogicalFilter>(
        bind(std::move(predicate), dd::LogicalType::BOOLEAN));
    if (column_ids.size() > original_columns) {
      // Columns the policy needed but the statement did not: keep them inside.
      for (size_t i = 0; i < original_columns; ++i) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
        filter->projection_map.push_back(dd::ProjectionIndex(i));
#else
        filter->projection_map.push_back(i);
#endif
      }
    }
    filter->children.push_back(std::move(op));
    op = std::move(filter);
    policy_filters_.insert(op.get());
  }

  // Every use of a masked column (outside the policy filters, which decide on
  // the real values) becomes the mask.
  void SubstituteMasks(dd::LogicalOperator& op) {
    if (!policy_filters_.count(&op)) {
      dd::LogicalOperatorVisitor::EnumerateExpressions(
          op, [&](dd::unique_ptr<dd::Expression>* e) {
            if (e && *e) SubstituteExpression(*e);
          });
    }
    for (auto& child : op.children) {
      if (child) SubstituteMasks(*child);
    }
  }

  void SubstituteExpression(dd::unique_ptr<dd::Expression>& expr) {
    if (expr->GetExpressionClass() == dd::ExpressionClass::BOUND_COLUMN_REF) {
      const auto& ref = expr->Cast<dd::BoundColumnRefExpression>();
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      auto it = masks_.find(ref.Binding());
#else
      auto it = masks_.find(ref.binding);
#endif
      if (it != masks_.end()) {
        auto alias = expr->GetAlias();
        expr = it->second->Copy();
        expr->SetAlias(alias);
        return;  // the mask's own reference to the column stays as it is
      }
    }
    dd::ExpressionIterator::EnumerateChildren(
        *expr,
        [&](dd::unique_ptr<dd::Expression>& child) { SubstituteExpression(child); });
  }

  // An operator that takes its child's columns by position (not through
  // expressions) would pass a masked column on unmasked.
  void CheckPositionalUse(dd::LogicalOperator& op) {
    if (UsesChildColumnsByPosition(op) && !policy_filters_.count(&op)) {
      for (auto& child : op.children) {
        if (!child) continue;
        for (const auto& b : child->GetColumnBindings()) {
          if (masks_.count(b))
            Refuse("a masked column is used in a way GizmoSQL cannot mask");
        }
      }
    }
    for (auto& child : op.children) {
      if (child) CheckPositionalUse(*child);
    }
  }

  dd::ClientContext& context_;
  const Snapshot& snapshot_;
  const Principal& principal_;
  PolicyContext policy_context_;
  dd::column_binding_map_t<dd::unique_ptr<dd::Expression>> masks_;
  std::unordered_set<const dd::LogicalOperator*> policy_filters_;
  const bool tenant_user_;
  const bool manage_config_;
};

}  // namespace

PolicyContext PolicyContext::For(const Snapshot& snapshot, const Principal& principal) {
  PolicyContext context;
  context.user_name = principal.user_name;
  for (const auto& id : snapshot.RolesOf(principal)) {
    if (const auto* role = snapshot.FindRoleById(id))
      context.role_names.insert(FoldIdentifier(role->name));
  }
  if (const auto* user = snapshot.FindUserById(principal.user_id))
    context.attributes = user->attributes;
  return context;
}

arrow::Status ValidatePolicySql(const std::string& sql,
                                const std::string& mask_parameter) {
  ARROW_ASSIGN_OR_RAISE(auto parsed, ParseOne(sql));
  return CheckShape(*parsed, mask_parameter);
}

arrow::Result<dd::unique_ptr<dd::ParsedExpression>> PreparePolicyExpression(
    const std::string& sql, const PolicyContext& context,
    const std::string& mask_parameter, const dd::ParsedExpression* parameter_value) {
  ARROW_ASSIGN_OR_RAISE(auto parsed, ParseOne(sql));
  ARROW_RETURN_NOT_OK(CheckShape(*parsed, mask_parameter));
  Substitute(parsed, context, mask_parameter, parameter_value);
  return parsed;
}

bool IsCredentialSetting(const std::string& name) {
  const std::string n = Lower(name);
  for (const char* marker :
       {"password", "secret", "token", "credential", "connection_string", "access_key",
        "account_key", "api_key", "private_key"}) {
    if (n.find(marker) != std::string::npos) return true;
  }
  return false;
}

std::vector<std::string> CredentialSettingNames(dd::ClientContext& context) {
  std::vector<std::string> names;
  for (dd::idx_t i = 0; i < dd::DBConfig::GetOptionCount(); ++i) {
    auto option = dd::DBConfig::GetOptionByIndex(i);
    if (option && IsCredentialSetting(option->name)) names.push_back(option->name);
  }
#if GIZMOSQL_DUCKDB_MAJOR_VERSION == 1 && GIZMOSQL_DUCKDB_MINOR_VERSION < 5
  for (const auto& [name, option] :
       dd::DBConfig::GetConfig(context).extension_parameters) {
    if (IsCredentialSetting(name)) names.push_back(name);
  }
#else
  for (const auto& [name, option] :
       dd::DBConfig::GetConfig(context).GetExtensionSettings()) {
    if (IsCredentialSetting(compat::Str(name))) names.push_back(compat::Str(name));
  }
#endif
  return names;
}

void ApplyPolicies(dd::ClientContext& context, const Snapshot& snapshot,
                   const Principal& principal,
                   dd::unique_ptr<dd::LogicalOperator>& plan) {
  if (!plan || principal.superuser) return;
  // Nothing to rewrite unless policies or tenants exist, or the statement
  // lists settings (credential rows are hidden).
  if (snapshot.row_policies().empty() && snapshot.mask_bindings().empty() &&
      snapshot.catalog_tenants().empty() && !ReadsSettings(*plan)) {
    return;
  }
  PolicyRewriter(context, snapshot, principal).Rewrite(plan);
}

}  // namespace gizmosql::enterprise::rbac
