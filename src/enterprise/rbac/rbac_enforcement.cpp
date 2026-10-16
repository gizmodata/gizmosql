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

#include "rbac_enforcement.h"

#include <set>
#include <unordered_set>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/optimizer/optimizer_extension.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/operator/logical_create.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_reset.hpp>
#include <duckdb/planner/operator/logical_set.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
#include <duckdb/planner/operator/logical_alter.hpp>
#include <duckdb/planner/operator/logical_drop.hpp>
#else
#include <duckdb/planner/operator/logical_simple.hpp>
#endif

#include "duckdb_compat.h"
#include "rbac_duckdb.h"
#include "rbac_views.h"

namespace gizmosql::enterprise::rbac {

namespace {

namespace dd = duckdb;
namespace compat = gizmosql::ddb::compat;
using OpType = dd::LogicalOperatorType;

// Table functions anyone may call under access control: they read no table
// and no file. Metadata functions list names, not data.
const std::unordered_set<std::string>& AllowedTableFunctions() {
  static const std::unordered_set<std::string> kNames = {
      "range", "generate_series", "unnest", "repeat", "repeat_row", "json_each",
      "json_tree", "test_all_types", "sql_auto_complete", "duckdb_tables",
      "duckdb_columns", "duckdb_views", "duckdb_schemas", "duckdb_databases",
      "duckdb_indexes", "duckdb_constraints", "duckdb_functions", "duckdb_types",
      "duckdb_keywords", "duckdb_settings", "duckdb_sequences", "duckdb_dependencies",
      "duckdb_extensions", "duckdb_optimizers", "pragma_table_info", "pragma_show",
      "pragma_version", "pragma_platform", "pragma_database_size",
      "pragma_collations", "pragma_user_agent"};
  return kNames;
}

// Session settings an access-controlled user may not change: turning the
// optimizer (and with it the plan check) off, or switching the SQL grammar.
const std::unordered_set<std::string>& BlockedSettings() {
  static const std::unordered_set<std::string> kNames = {
      "enable_optimizer", "disabled_optimizers", "current_dialect",
      "active_grammar_extensions", "enable_legacy_parser"};
  return kNames;
}

std::string Lower(std::string s) {
  for (auto& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

bool IsTempCatalog(const std::string& catalog) {
  return IdentifiersEqual(catalog, TEMP_CATALOG);
}

std::string TableFunctionName(const dd::LogicalGet& get) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return compat::Str(get.function.GetName());
#else
  return get.function.name;
#endif
}

// The parts of a (bound) qualified name: catalog, schema path..., name.
std::vector<std::string> NameParts(const dd::QualifiedName& name) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto parts = compat::Strs(name.Path());
  return std::vector<std::string>(parts.begin(), parts.end());
#else
  std::vector<std::string> parts;
  if (!name.catalog.empty()) parts.push_back(name.catalog);
  if (!name.schema.empty()) parts.push_back(name.schema);
  parts.push_back(name.name);
  return parts;
#endif
}

// Look an entry up inside the current transaction (the hook runs while the
// statement is being planned, so no new transaction or lock may be taken).
dd::optional_ptr<dd::CatalogEntry> LookUp(dd::ClientContext& context, dd::CatalogType type,
                                          const std::vector<std::string>& parts) {
  try {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    dd::vector<dd::Identifier> path;
    for (const auto& p : parts) path.emplace_back(p);
    return dd::Catalog::GetEntry(context,
                                 dd::EntryLookupInfo(type, dd::QualifiedName::FromPath(path)),
                                 dd::OnEntryNotFound::RETURN_NULL);
#else
    std::string catalog, schema, name;
    if (parts.size() == 1) {
      name = parts[0];
    } else if (parts.size() == 2) {
      schema = parts[0];
      name = parts[1];
    } else if (parts.size() == 3) {
      catalog = parts[0];
      schema = parts[1];
      name = parts[2];
    } else {
      return nullptr;
    }
    return dd::Catalog::GetEntry(context, catalog, schema, dd::EntryLookupInfo(type, name),
                                 dd::OnEntryNotFound::RETURN_NULL);
#endif
  } catch (...) {
    return nullptr;
  }
}

ObjectType ObjectTypeOf(const dd::CatalogEntry& entry) {
  if (entry.type == dd::CatalogType::VIEW_ENTRY) return ObjectType::kView;
  if (entry.type == dd::CatalogType::SCHEMA_ENTRY) return ObjectType::kSchema;
  return ObjectType::kTable;
}

std::string TypeWord(ObjectType type) {
  switch (type) {
    case ObjectType::kView:
      return "view";
    case ObjectType::kSchema:
      return "schema";
    case ObjectType::kTable:
      return "table";
  }
  return "object";
}

// A CREATE VIEW's definition hash (see OwnerRecord::definition_hash).
std::string DefinitionHashOf(dd::CreateInfo& info) {
  auto& view = info.Cast<dd::CreateViewInfo>();
  return view.query ? ViewDefinitionHash(*view.query) : "";
}

[[noreturn]] void Deny(const Principal& who, const std::string& what) {
  throw dd::PermissionException(
      "GizmoSQL blocked this statement: " + what + ". Access control on this server lets user " +
      who.user_name + " do only what their roles are granted (see SHOW GRANTS)");
}

// Walks one statement's bound plan and checks every access it makes.
//
// What the statement names itself is checked against the caller. What a view
// reads is checked against the view's owner (the caller needs SELECT on the
// view only), view inside view, each against its own owner. A view without a
// valid owner record reads with the caller's rights, in addition to the
// rights of any view it sits in.
class PlanChecker {
 public:
  using ViewColumns = std::unordered_map<const ViewMarker*, std::vector<std::string>>;

  PlanChecker(dd::ClientContext& context, const Snapshot& snapshot, const Principal& principal,
              const ViewColumns& view_columns)
      : context_(context),
        snapshot_(snapshot),
        principal_(principal),
        view_columns_(view_columns),
        rights_{Rights{principal, ""}} {}

  void Visit(dd::LogicalOperator& op, dd::optional_ptr<dd::TableCatalogEntry> dml_target = nullptr) {
    switch (op.type) {
      // ---- reads ----------------------------------------------------------
      case OpType::LOGICAL_GET:
        VisitGet(op.Cast<dd::LogicalGet>(), dml_target);
        break;

      // ---- writes ---------------------------------------------------------
      case OpType::LOGICAL_INSERT: {
        auto& insert = op.Cast<dd::LogicalInsert>();
        Require(insert.table, Privilege::kInsert, {});
        if (insert.return_chunk) RequireAllColumns(insert.table);
        VisitChildren(op, nullptr);
        return;
      }
      case OpType::LOGICAL_UPDATE: {
        auto& update = op.Cast<dd::LogicalUpdate>();
        Require(update.table, Privilege::kUpdate, {});
        if (update.return_chunk) RequireAllColumns(update.table);
        VisitChildren(op, &update.table);
        return;
      }
      case OpType::LOGICAL_DELETE: {
        auto& del = op.Cast<dd::LogicalDelete>();
        Require(del.table, Privilege::kDelete, {});
        if (del.return_chunk) RequireAllColumns(del.table);
        VisitChildren(op, &del.table);
        return;
      }
      case OpType::LOGICAL_MERGE_INTO: {
        auto& merge = op.Cast<dd::LogicalMergeInto>();
        // MERGE may insert, update and delete: require all three.
        Require(merge.table, Privilege::kInsert, {});
        Require(merge.table, Privilege::kUpdate, {});
        Require(merge.table, Privilege::kDelete, {});
        if (merge.return_chunk) RequireAllColumns(merge.table);
        VisitChildren(op, &merge.table);
        return;
      }

      // ---- DDL ------------------------------------------------------------
      case OpType::LOGICAL_CREATE_TABLE: {
        auto& create = op.Cast<dd::LogicalCreateTable>();
        CheckCreate(*create.info->base, ObjectType::kTable, Privilege::kCreateTable);
        VisitChildren(op, nullptr);  // CREATE TABLE AS reads its source
        return;
      }
      case OpType::LOGICAL_CREATE_VIEW: {
        auto& create = op.Cast<dd::LogicalCreate>();
        const size_t before = ownership_.size();
        CheckCreate(*create.info, ObjectType::kView, Privilege::kCreateView);
        if (ownership_.size() > before) ownership_.back().definition_hash = DefinitionHashOf(*create.info);
        return;
      }
      case OpType::LOGICAL_CREATE_SCHEMA: {
        auto& create = op.Cast<dd::LogicalCreate>();
        CheckCreate(*create.info, ObjectType::kSchema, Privilege::kCreateSchema);
        return;
      }
      case OpType::LOGICAL_CREATE_INDEX: {
        auto& index = op.Cast<dd::LogicalCreateIndex>();
        if (!IsTempCatalog(compat::Str(index.table.ParentCatalog().GetName()))) {
          Require(index.table, Privilege::kAlterTable, {});
        }
        VisitChildren(op, nullptr);
        return;
      }
      case OpType::LOGICAL_CREATE_SEQUENCE: {
        auto& create = op.Cast<dd::LogicalCreate>();
        if (create.info->temporary) return;  // session-local
        Deny(principal_, "creating sequences needs an administrator");
      }
      // Not even temporary ones: a session's macro or type is found before the
      // catalog's, also inside a view, and would change what the view computes.
      case OpType::LOGICAL_CREATE_MACRO:
        Deny(principal_, "creating macros needs an administrator");
      case OpType::LOGICAL_CREATE_TYPE:
        Deny(principal_, "creating types needs an administrator");
      case OpType::LOGICAL_DROP:
        CheckDrop(op);
        return;
      case OpType::LOGICAL_ALTER:
        CheckAlter(op);
        return;

      // ---- session statements --------------------------------------------
      case OpType::LOGICAL_SET: {
        const auto& set = op.Cast<dd::LogicalSet>();
        // A variable's value is computed by a query (which would otherwise go
        // unchecked here), and getvariable() hands it to any view that asks.
        if (set.scope == dd::SetScope::VARIABLE) {
          Deny(principal_, "SET VARIABLE is not available under access control");
        }
        if (BlockedSettings().count(Lower(compat::Str(set.name)))) {
          Deny(principal_, "changing " + compat::Str(set.name) + " is not allowed");
        }
        return;
      }
      case OpType::LOGICAL_RESET: {
        const auto& reset = op.Cast<dd::LogicalReset>();
        if (reset.scope != dd::SetScope::VARIABLE &&
            BlockedSettings().count(Lower(compat::Str(reset.name)))) {
          Deny(principal_, "changing " + compat::Str(reset.name) + " is not allowed");
        }
        return;
      }

      // ---- a view's plan, marked when it was bound -------------------------
      case OpType::LOGICAL_EXTENSION_OPERATOR:
        if (auto* marker = dynamic_cast<ViewMarker*>(&op)) {
          VisitView(*marker);
          return;
        }
        Deny(principal_, "extension operators are not available under access control");
      case OpType::LOGICAL_TRANSACTION:
        return;

      // ---- plain relational operators: check what they read --------------
      case OpType::LOGICAL_PROJECTION:
      case OpType::LOGICAL_FILTER:
      case OpType::LOGICAL_AGGREGATE_AND_GROUP_BY:
      case OpType::LOGICAL_WINDOW:
      case OpType::LOGICAL_UNNEST:
      case OpType::LOGICAL_LIMIT:
      case OpType::LOGICAL_ORDER_BY:
      case OpType::LOGICAL_TOP_N:
      case OpType::LOGICAL_DISTINCT:
      case OpType::LOGICAL_SAMPLE:
      case OpType::LOGICAL_PIVOT:
      case OpType::LOGICAL_CHUNK_GET:
      case OpType::LOGICAL_DELIM_GET:
      case OpType::LOGICAL_EXPRESSION_GET:
      case OpType::LOGICAL_DUMMY_SCAN:
      case OpType::LOGICAL_EMPTY_RESULT:
      case OpType::LOGICAL_CTE_REF:
      case OpType::LOGICAL_JOIN:
      case OpType::LOGICAL_DELIM_JOIN:
      case OpType::LOGICAL_COMPARISON_JOIN:
      case OpType::LOGICAL_ANY_JOIN:
      case OpType::LOGICAL_CROSS_PRODUCT:
      case OpType::LOGICAL_POSITIONAL_JOIN:
      case OpType::LOGICAL_ASOF_JOIN:
      case OpType::LOGICAL_DEPENDENT_JOIN:
      case OpType::LOGICAL_UNION:
      case OpType::LOGICAL_EXCEPT:
      case OpType::LOGICAL_INTERSECT:
      case OpType::LOGICAL_RECURSIVE_CTE:
      case OpType::LOGICAL_MATERIALIZED_CTE:
      case OpType::LOGICAL_EXPLAIN:
      // A prepared statement's plan, and the re-planned statement an EXECUTE
      // carries after a rebind (without one, the check made when it was
      // prepared, at the same grants version, still stands).
      case OpType::LOGICAL_PREPARE:
      case OpType::LOGICAL_EXECUTE:
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
      case OpType::LOGICAL_SECURE_VIEW:
      case OpType::LOGICAL_TRIGGER:
#endif
        VisitChildren(op, dml_target);
        return;

      // ---- everything else is denied -------------------------------------
      default:
        Deny(principal_, Lower(dd::LogicalOperatorToString(op.type)) +
                             " is not available under access control");
    }
  }

  std::vector<PendingOwnership> TakeOwnership() { return std::move(ownership_); }

 private:
  void VisitChildren(dd::LogicalOperator& op, dd::optional_ptr<dd::TableCatalogEntry> dml_target) {
    for (auto& child : op.children) {
      if (child) Visit(*child, dml_target);
    }
  }

  void VisitGet(dd::LogicalGet& get, dd::optional_ptr<dd::TableCatalogEntry> dml_target) {
    auto table = get.GetTable();
    if (!table) {
      const std::string name = Lower(TableFunctionName(get));
      if (OnlySuperusers()) return;  // in a view an administrator owns
      if (!AllowedTableFunctions().count(name)) {
        Deny(principal_, "the table function " + name + " is not available under access control");
      }
      return;
    }
    std::vector<std::string> columns;
    for (const auto& index : get.GetColumnIds()) {
      if (index.IsRowIdColumn() || index.IsVirtualColumn() || index.IsEmptyColumn()) continue;
      const auto& column = table->GetColumn(dd::LogicalIndex(index.GetPrimaryIndex()));
      columns.push_back(compat::Str(column.Name()));
    }
    // The row-locating scan under the statement's own UPDATE / DELETE target
    // needs no SELECT unless it reads columns (in WHERE or SET), as in Postgres.
    if (columns.empty() && dml_target && table.get() == dml_target.get()) return;
    Require(*table, Privilege::kSelect, columns);
  }

  ObjectPath PathOf(dd::CatalogEntry& entry) { return PathOfEntry(entry); }

  void Require(dd::CatalogEntry& entry, Privilege privilege, std::vector<std::string> columns) {
    RequireObject(PathOf(entry), ObjectTypeOf(entry), privilege, std::move(columns));
  }

  void RequireObject(const ObjectPath& path, ObjectType type, Privilege privilege,
                     std::vector<std::string> columns) {
    if (IsTempCatalog(path.catalog)) {
      // A session's temporary table or view is found before the catalog's
      // objects, inside a view too: it could stand in for a table the view's
      // owner reads (a list of allowed rows, say). Only the caller's own
      // statement may read one.
      if (in_owned_view_) {
        Deny(principal_, "the view " + owned_view_ + " cannot read a temporary " + TypeWord(type) +
                             " of this session");
      }
      return;
    }
    AccessRequest request{path, type, privilege, std::move(columns)};
    for (const auto& rights : rights_) {
      auto decision = snapshot_.Check(rights.principal, request);
      if (decision.allowed) continue;
      if (!rights.owner_of_view.empty()) {
        // Never name what the view reads: the caller may not know it.
        Deny(principal_, "the view " + rights.owner_of_view +
                             " cannot be read, because its owner lacks a privilege on an object "
                             "the view reads");
      }
      std::string what = "user " + principal_.user_name + " has no " + PrivilegeName(privilege) +
                         " privilege on " + TypeWord(request.type) + " " + path.ToString();
      if (privilege == Privilege::kSelect && decision.reason.rfind("no SELECT on column", 0) == 0) {
        what = "user " + principal_.user_name + " has " + decision.reason + " of " + path.ToString();
      }
      if (!unowned_view_.empty()) {
        what += " (read through the view " + unowned_view_ +
                ", which has no owner, so it reads with your rights)";
      }
      Deny(principal_, what);
    }
  }

  bool OnlySuperusers() const {
    for (const auto& rights : rights_) {
      if (!rights.principal.superuser) return false;
    }
    return true;
  }

  // The caller needs SELECT on the view (on the columns the statement uses);
  // the view's body is then checked with its owner's rights.
  void VisitView(ViewMarker& view) {
    const std::string name = view.view.ToString();
    if (view.temporary) {
      if (in_owned_view_) {
        Deny(principal_, "the view " + owned_view_ + " cannot read a temporary view of this session");
      }
      VisitChildren(view, nullptr);  // the session's own view: the caller's rights
      return;
    }
    auto used = view_columns_.find(&view);
    RequireObject(view.view, ObjectType::kView, Privilege::kSelect,
                  used != view_columns_.end() ? used->second : view.column_names);

    const auto saved_rights = rights_;
    const auto saved_in_owned = in_owned_view_;
    const auto saved_owned = owned_view_;
    const auto saved_unowned = unowned_view_;
    const OwnerRecord* owner = snapshot_.FindOwner(view.view);
    if (owner && owner->type == ObjectType::kView && !owner->definition_hash.empty() &&
        owner->definition_hash == view.definition_hash) {
      rights_ = {Rights{OwnerPrincipal(*owner, name), name}};
      in_owned_view_ = true;
      owned_view_ = name;
      unowned_view_.clear();
    } else {
      // No owner on record, or the record is of an earlier view of this name:
      // the caller's own rights apply as well.
      bool has_caller = false;
      for (const auto& r : rights_) has_caller = has_caller || r.owner_of_view.empty();
      if (!has_caller) rights_.push_back(Rights{principal_, ""});
      unowned_view_ = name;
    }
    VisitChildren(view, nullptr);
    rights_ = saved_rights;
    in_owned_view_ = saved_in_owned;
    owned_view_ = saved_owned;
    unowned_view_ = saved_unowned;
  }

  Principal OwnerPrincipal(const OwnerRecord& owner, const std::string& view_name) {
    switch (owner.owner_kind) {
      case OwnerKind::kSystem:
        return Principal::Superuser("administrator");
      case OwnerKind::kUser: {
        const UserRecord* user = snapshot_.FindUserById(owner.owner_id);
        if (!user || !user->enabled) {
          Deny(principal_, "the view " + view_name +
                               " cannot be read, because its owner is disabled or was dropped");
        }
        return Principal{false, user->id, user->name, ""};
      }
      case OwnerKind::kRole: {
        const RoleRecord* role = snapshot_.FindRoleById(owner.owner_id);
        if (!role) {
          Deny(principal_, "the view " + view_name + " cannot be read, because its owner was dropped");
        }
        return Principal::Role(role->id, role->name);
      }
    }
    Deny(principal_, "the view " + view_name + " has an owner record that is not understood");
  }

  void RequireAllColumns(dd::TableCatalogEntry& table) {
    std::vector<std::string> columns;
    for (const auto& name : table.GetColumns().GetColumnNames()) columns.push_back(name);
    Require(table, Privilege::kSelect, std::move(columns));
  }

  // CREATE needs the privilege on the container; CREATE OR REPLACE of an
  // existing object also needs the right to drop it; IF NOT EXISTS on an
  // existing object changes nothing and never takes its ownership.
  void CheckCreate(dd::CreateInfo& info, ObjectType type, Privilege privilege) {
    auto parts = NameParts(info.GetQualifiedName());
    if (info.temporary || (!parts.empty() && IsTempCatalog(parts[0]))) return;
    ObjectPath target;
    if (type == ObjectType::kSchema) {
      if (parts.size() < 2) Deny(principal_, "the new schema's catalog could not be determined");
      target.catalog = parts[0];
      target.schema_path.assign(parts.begin() + 1, parts.end());
    } else {
      if (parts.size() < 3) Deny(principal_, "the new object's schema could not be determined");
      target.catalog = parts[0];
      target.schema_path.assign(parts.begin() + 1, parts.end() - 1);
      target.object_name = parts.back();
    }
    const auto lookup_type =
        type == ObjectType::kSchema ? dd::CatalogType::SCHEMA_ENTRY : dd::CatalogType::TABLE_ENTRY;
    auto existing = LookUp(context_, lookup_type, parts);
    if (existing && info.on_conflict == dd::OnCreateConflict::IGNORE_ON_CONFLICT) return;
    if (existing && info.on_conflict == dd::OnCreateConflict::REPLACE_ON_CONFLICT) {
      CheckDropOf(*existing);
    }
    AccessRequest request{target, type, privilege, {}};
    if (!snapshot_.Check(principal_, request).allowed) {
      Deny(principal_, "user " + principal_.user_name + " has no " + PrivilegeName(privilege) +
                           " privilege on " +
                           (type == ObjectType::kSchema ? "catalog " + target.catalog
                                                        : "schema " + ContainerOf(target)));
    }
    ownership_.push_back({PendingOwnership::Kind::kCreate, target, type});
  }

  static std::string ContainerOf(const ObjectPath& object) {
    ObjectPath container = object;
    container.object_name.clear();
    return container.ToString();
  }

  // Dropping (or replacing) an object: its owner, or DROP TABLE / VIEW / SCHEMA.
  void CheckDropOf(dd::CatalogEntry& entry) {
    ObjectPath path = PathOf(entry);
    if (IsTempCatalog(path.catalog)) return;
    const ObjectType type = ObjectTypeOf(entry);
    const Privilege privilege = type == ObjectType::kView     ? Privilege::kDropView
                                : type == ObjectType::kSchema ? Privilege::kDropSchema
                                                              : Privilege::kDropTable;
    AccessRequest request{path, type, privilege, {}};
    if (!snapshot_.Check(principal_, request).allowed) {
      Deny(principal_, "user " + principal_.user_name + " may not drop " + TypeWord(type) + " " +
                           path.ToString() + " (only its owner, or a role granted " +
                           PrivilegeName(privilege) + ")");
    }
    ownership_.push_back({PendingOwnership::Kind::kDrop, path, type});
  }

  void CheckDrop(dd::LogicalOperator& op) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto& info = *op.Cast<dd::LogicalDrop>().info;
#else
    auto& simple = op.Cast<dd::LogicalSimple>();
    auto& info = simple.info->Cast<dd::DropInfo>();
#endif
    auto parts = NameParts(info.GetQualifiedName());
    if (!parts.empty() && IsTempCatalog(parts[0])) return;
    switch (info.type) {
      case dd::CatalogType::TABLE_ENTRY:
      case dd::CatalogType::VIEW_ENTRY:
      case dd::CatalogType::SCHEMA_ENTRY: {
        auto entry = LookUp(context_, info.type == dd::CatalogType::SCHEMA_ENTRY
                                          ? dd::CatalogType::SCHEMA_ENTRY
                                          : dd::CatalogType::TABLE_ENTRY,
                            parts);
        if (!entry) return;  // DROP ... IF EXISTS of nothing; DuckDB reports a missing object
        CheckDropOf(*entry);
        return;
      }
      default:
        Deny(principal_, "dropping this kind of object needs an administrator");
    }
  }

  void CheckAlter(dd::LogicalOperator& op) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto& info = *op.Cast<dd::LogicalAlter>().info;
#else
    auto& simple = op.Cast<dd::LogicalSimple>();
    auto& info = simple.info->Cast<dd::AlterInfo>();
#endif
    auto parts = NameParts(info.GetQualifiedName());
    if (!parts.empty() && IsTempCatalog(parts[0])) return;
    const auto catalog_type = info.GetCatalogType();
    if (catalog_type != dd::CatalogType::TABLE_ENTRY && catalog_type != dd::CatalogType::VIEW_ENTRY) {
      Deny(principal_, "altering this kind of object needs an administrator");
    }
    auto entry = LookUp(context_, dd::CatalogType::TABLE_ENTRY, parts);
    if (!entry) return;  // DuckDB reports the missing object
    ObjectPath path = PathOf(*entry);
    if (IsTempCatalog(path.catalog)) return;
    const ObjectType type = ObjectTypeOf(*entry);
    if (info.type == dd::AlterType::ALTER_TABLE || info.type == dd::AlterType::ALTER_VIEW) {
      // A rename moves the object out from under its owner record: owners only.
      AccessRequest request{path, type,
                            type == ObjectType::kView ? Privilege::kAlterView : Privilege::kAlterTable,
                            {}};
      if (!snapshot_.Check(principal_, request).allowed) {
        Deny(principal_, "user " + principal_.user_name + " may not alter " + TypeWord(type) + " " +
                             path.ToString() + " (only its owner, or a role granted " +
                             PrivilegeName(request.privilege) + ")");
      }
      return;
    }
    Deny(principal_, "this ALTER needs an administrator");
  }

  // Whose rights a read must satisfy, every one of them: the caller's, or a
  // view owner's (owner_of_view names the view).
  struct Rights {
    Principal principal;
    std::string owner_of_view;
  };

  dd::ClientContext& context_;
  const Snapshot& snapshot_;
  const Principal& principal_;
  const ViewColumns& view_columns_;
  std::vector<Rights> rights_;
  bool in_owned_view_ = false;
  std::string owned_view_;    // the innermost view read with its owner's rights
  std::string unowned_view_;  // the innermost view read with the caller's rights
  std::vector<PendingOwnership> ownership_;
};

void PreOptimize(dd::OptimizerExtensionInput& input, dd::unique_ptr<dd::LogicalOperator>& plan) {
  auto& context = input.context;
  auto state = context.registered_state->Get<RbacSessionState>(kRbacSessionStateName);
  if (!state) return;  // a GizmoSQL-internal connection
  auto engine = state->engine();
  if (!engine) {
    throw dd::PermissionException("GizmoSQL blocked this statement: access control is shutting down");
  }
  auto snapshot = engine->snapshot();
  if (state->unchecked() || !plan) {
    // Superusers and GizmoSQL's own statements are not checked, but what they
    // create or drop still updates ownership.
    std::vector<PendingOwnership> ownership;
    if (plan && !state->bypassed()) {
      // Record superuser-created objects as system-owned so a stale record of a
      // user's earlier object of the same name can never apply to them.
      std::function<void(dd::LogicalOperator&)> collect = [&](dd::LogicalOperator& op) {
        if (op.type == OpType::LOGICAL_CREATE_TABLE) {
          auto parts = NameParts(op.Cast<dd::LogicalCreateTable>().info->base->GetQualifiedName());
          if (parts.size() >= 3 && !IsTempCatalog(parts[0])) {
            ownership.push_back({PendingOwnership::Kind::kCreate,
                                 {parts[0], {parts.begin() + 1, parts.end() - 1}, parts.back()},
                                 ObjectType::kTable});
          }
        } else if (op.type == OpType::LOGICAL_CREATE_VIEW) {
          auto parts = NameParts(op.Cast<dd::LogicalCreate>().info->GetQualifiedName());
          if (parts.size() >= 3 && !IsTempCatalog(parts[0])) {
            ownership.push_back({PendingOwnership::Kind::kCreate,
                                 {parts[0], {parts.begin() + 1, parts.end() - 1}, parts.back()},
                                 ObjectType::kView,
                                 DefinitionHashOf(*op.Cast<dd::LogicalCreate>().info)});
          }
        }
        for (auto& child : op.children) {
          if (child) collect(*child);
        }
      };
      collect(*plan);
    }
    RemoveViewMarkers(plan);
    state->NoteChecked(snapshot->version(), std::move(ownership),
                       plan && plan->type == OpType::LOGICAL_PREPARE);
    return;
  }
  if (!engine->IsActive(state->principal())) {
    throw dd::PermissionException("GizmoSQL blocked this statement: user " +
                                  state->principal().user_name + " is disabled or was dropped");
  }
  const auto view_columns = ViewColumnsUsed(*plan);
  PlanChecker checker(context, *snapshot, state->principal(), view_columns);
  checker.Visit(*plan);
  RemoveViewMarkers(plan);
  state->NoteChecked(snapshot->version(), checker.TakeOwnership(),
                     plan->type == OpType::LOGICAL_PREPARE);
}

}  // namespace

// ---------------------------------------------------------------------------

RbacSessionState::RbacSessionState(std::shared_ptr<RbacEngine> engine, Principal principal)
    : engine_(engine), principal_(std::move(principal)) {}

RbacSessionState::BypassScope::BypassScope(dd::ClientContext& context)
    : state_(context.registered_state->Get<RbacSessionState>(kRbacSessionStateName)) {
  if (state_) ++state_->bypass_depth_;
}

RbacSessionState::BypassScope::~BypassScope() {
  if (state_) --state_->bypass_depth_;
}

void RbacSessionState::NoteChecked(int64_t snapshot_version,
                                   std::vector<PendingOwnership> ownership, bool prepare_only) {
  std::lock_guard<std::mutex> lock(mutex_);
  ++checked_pending_;
  last_checked_version_ = snapshot_version;
  planned_prepare_only_ = prepare_only;
  for (auto& o : ownership) planned_ownership_.push_back(std::move(o));
}

std::vector<PendingOwnership> RbacSessionState::TakePreparedOwnership() {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<PendingOwnership> taken;
  taken.swap(prepared_ownership_);
  return taken;
}

dd::RebindQueryInfo RbacSessionState::OnFinalizePrepare(dd::ClientContext&,
                                                        dd::PreparedStatementData& prepared,
                                                        dd::PreparedStatementMode mode) {
  std::lock_guard<std::mutex> lock(mutex_);
  // File the plan's ownership changes: applied at QueryEnd when it runs now,
  // or handed to GizmoSQL to apply after a later successful execution.
  const bool prepare_only =
      mode == dd::PreparedStatementMode::PREPARE_ONLY || planned_prepare_only_;
  planned_prepare_only_ = false;
  auto& target = prepare_only ? prepared_ownership_ : pending_ownership_;
  if (prepare_only) prepared_ownership_.clear();
  for (auto& o : planned_ownership_) target.push_back(std::move(o));
  planned_ownership_.clear();
  if (checked_pending_ > 0) {
    --checked_pending_;
  } else if (!unchecked() && prepared.statement_type == dd::StatementType::PREPARE_STATEMENT) {
    // DuckDB 2.x prepares a statement it must re-plan before every run (bound
    // parameters still open, or a catalog such as DuckLake that requires a
    // rebind) without optimizing it, so the check has not run yet. It runs on
    // that re-plan: the statement is not recorded as checked, so the execute
    // hook below forces the re-plan, and the check, before anything runs.
    return dd::RebindQueryInfo::DO_NOT_REBIND;
  } else if (!unchecked()) {
    // The plan never reached the check (the optimizer was skipped). Refuse it.
    pending_ownership_.clear();
    prepared_ownership_.clear();
    throw dd::PermissionException(
        "GizmoSQL blocked this statement: its plan could not be checked against user " +
        principal_.user_name + "'s grants");
  }
  if (prepared_versions_.size() > 4096) prepared_versions_.clear();
  prepared_versions_[&prepared] = last_checked_version_;
  return dd::RebindQueryInfo::DO_NOT_REBIND;
}

dd::RebindQueryInfo RbacSessionState::RebindIfGrantsChanged(const dd::PreparedStatementData& prepared) {
  if (unchecked()) return dd::RebindQueryInfo::DO_NOT_REBIND;
  auto engine = engine_.lock();
  if (!engine) return dd::RebindQueryInfo::ATTEMPT_TO_REBIND;
  const int64_t current = engine->snapshot()->version();
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = prepared_versions_.find(&prepared);
  if (it != prepared_versions_.end() && it->second == current) return dd::RebindQueryInfo::DO_NOT_REBIND;
  // Grants changed since this statement was checked (or it was never seen):
  // re-plan it, which runs the check again.
  return dd::RebindQueryInfo::ATTEMPT_TO_REBIND;
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
dd::RebindQueryInfo RbacSessionState::OnRebindPreparedStatement(
    dd::ClientContext&, dd::BindPreparedStatementCallbackInfo& info, dd::RebindQueryInfo) {
  return RebindIfGrantsChanged(info.prepared_statement);
}
#else
dd::RebindQueryInfo RbacSessionState::OnExecutePrepared(dd::ClientContext&,
                                                        dd::PreparedStatementCallbackInfo& info,
                                                        dd::RebindQueryInfo) {
  return RebindIfGrantsChanged(info.prepared_statement);
}
#endif

void RbacSessionState::QueryEnd(dd::ClientContext&, dd::optional_ptr<dd::ErrorData> error) {
  std::vector<PendingOwnership> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending.swap(pending_ownership_);
    planned_ownership_.clear();
    checked_pending_ = 0;
  }
  if (error || pending.empty()) return;
  if (auto engine = engine_.lock()) ApplyOwnership(*engine, principal_, pending);
}

void ApplyOwnership(RbacEngine& engine, const Principal& principal,
                    const std::vector<PendingOwnership>& changes) {
  for (const auto& p : changes) {
    if (p.kind == PendingOwnership::Kind::kCreate) {
      (void)engine.RecordOwner(p.object, p.type, principal, p.definition_hash);
    } else {
      (void)engine.ForgetOwner(p.object);
    }
  }
}

void RegisterRbacEnforcement(dd::DBConfig& config) {
  InstallViewHook();
  dd::OptimizerExtension extension;
  extension.pre_optimize_function = PreOptimize;
  dd::OptimizerExtension::Register(config, std::move(extension));
}

void AttachRbacSession(dd::ClientContext& context, std::shared_ptr<RbacEngine> engine,
                       Principal principal) {
  context.registered_state->Insert(
      kRbacSessionStateName,
      dd::make_shared_ptr<RbacSessionState>(std::move(engine), std::move(principal)));
}

}  // namespace gizmosql::enterprise::rbac
