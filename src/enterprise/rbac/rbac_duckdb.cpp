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

#include "rbac_duckdb.h"

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/planner/binder.hpp>

#include "duckdb_compat.h"

namespace gizmosql::enterprise::rbac {

namespace {

namespace dd = duckdb;
namespace compat = gizmosql::ddb::compat;

// Run `fn` inside a transaction on the session's context (catalog lookups need
// one), turning DuckDB exceptions into a GizmoSQL error.
template <typename Fn>
arrow::Status InTransaction(dd::Connection& connection, Fn&& fn) {
  arrow::Status status;
  try {
    connection.context->RunFunctionInTransaction([&] { status = fn(); });
  } catch (const std::exception& e) {
    return AccessControlError(dd::ErrorData(e).Message());
  }
  return status;
}

std::vector<std::string> SchemaPathOf(dd::SchemaCatalogEntry& schema) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const auto names = compat::Strs(schema.GetSchemaPath());
  return std::vector<std::string>(names.begin(), names.end());
#else
  return {schema.name};
#endif
}

dd::optional_ptr<dd::Catalog> CatalogNamed(dd::ClientContext& context, const std::string& name) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  return dd::Catalog::GetCatalogEntry(context, dd::Identifier(name));
#else
  return dd::Catalog::GetCatalogEntry(context, name);
#endif
}

}  // namespace

ObjectPath PathOfEntry(dd::CatalogEntry& entry) {
  ObjectPath path;
  path.catalog = compat::Str(entry.ParentCatalog().GetName());
  if (entry.type == dd::CatalogType::SCHEMA_ENTRY) {
    path.schema_path = SchemaPathOf(entry.Cast<dd::SchemaCatalogEntry>());
    return path;
  }
  path.schema_path = SchemaPathOf(entry.ParentSchema());
  path.object_name = compat::Str(entry.name);
  return path;
}

arrow::Result<ResolvedObject> SessionObjectResolver::ResolveObject(const QualifiedName& name) {
  ResolvedObject resolved;
  bool found = false;
  ARROW_RETURN_NOT_OK(InTransaction(connection_, [&]() -> arrow::Status {
    auto& context = *connection_.context;
    dd::optional_ptr<dd::CatalogEntry> entry;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    dd::vector<dd::Identifier> path;
    for (const auto& part : name.parts) path.emplace_back(part);
    entry = dd::Catalog::GetEntry(
        context,
        dd::EntryLookupInfo(dd::CatalogType::TABLE_ENTRY, dd::QualifiedName::FromPath(path)),
        dd::OnEntryNotFound::RETURN_NULL);
#else
    std::string catalog, schema, object;
    switch (name.parts.size()) {
      case 1:
        object = name.parts[0];
        break;
      case 2:
        schema = name.parts[0];
        object = name.parts[1];
        // The binder's rule: a lone qualifier may name a catalog.
        dd::Binder::BindSchemaOrCatalog(context, catalog, schema);
        break;
      case 3:
        catalog = name.parts[0];
        schema = name.parts[1];
        object = name.parts[2];
        break;
      default:
        return AccessControlError("too many name parts in " + name.ToString());
    }
    entry = dd::Catalog::GetEntry(context, catalog, schema,
                                  dd::EntryLookupInfo(dd::CatalogType::TABLE_ENTRY, object),
                                  dd::OnEntryNotFound::RETURN_NULL);
#endif
    if (!entry) return arrow::Status::OK();
    if (entry->type != dd::CatalogType::TABLE_ENTRY && entry->type != dd::CatalogType::VIEW_ENTRY) {
      return AccessControlError(name.ToString() + " is not a table or view");
    }
    found = true;
    resolved.path.catalog = compat::Str(entry->ParentCatalog().GetName());
    resolved.path.schema_path = SchemaPathOf(entry->ParentSchema());
    resolved.path.object_name = compat::Str(entry->name);
    if (entry->type == dd::CatalogType::VIEW_ENTRY) {
      resolved.type = ObjectType::kView;
      auto& view = entry->Cast<dd::ViewCatalogEntry>();
      view.BindView(context);
      if (auto info = view.GetColumnInfo()) {
        for (const auto& n : info->names) resolved.columns.push_back(compat::Str(n));
      }
    } else {
      resolved.type = ObjectType::kTable;
      auto& table = entry->Cast<dd::TableCatalogEntry>();
      for (const auto& n : table.GetColumns().GetColumnNames()) resolved.columns.push_back(n);
    }
    return arrow::Status::OK();
  }));
  if (!found) {
    return AccessControlError("table or view " + name.ToString() + " does not exist");
  }
  return resolved;
}

arrow::Result<ObjectPath> SessionObjectResolver::ResolveSchema(const QualifiedName& name) {
  ObjectPath resolved;
  bool found = false;
  ARROW_RETURN_NOT_OK(InTransaction(connection_, [&]() -> arrow::Status {
    auto& context = *connection_.context;
    // The first part is a catalog when one by that name is attached. An
    // unqualified schema is the session's current database's, never the
    // session-local temp catalog that heads the search path.
    std::string catalog;
    std::vector<std::string> schema_path = name.parts;
    if (name.parts.size() > 1 && CatalogNamed(context, name.parts[0])) {
      catalog = name.parts[0];
      schema_path.erase(schema_path.begin());
    } else {
      catalog = compat::Str(dd::DatabaseManager::GetDefaultDatabase(context));
    }
    dd::optional_ptr<dd::SchemaCatalogEntry> schema;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    dd::vector<dd::Identifier> path;
    for (const auto& part : schema_path) path.emplace_back(part);
    // An unqualified schema lives in the session's default catalog; DuckDB 2.x
    // spells "no catalog given" as InvalidCatalog(), not an empty identifier.
    schema = dd::Catalog::GetSchema(context, dd::Identifier(catalog), path,
                                    dd::OnEntryNotFound::RETURN_NULL);
#else
    if (schema_path.size() != 1) {
      return AccessControlError("schema " + name.ToString() +
                                " does not exist (nested schemas need DuckDB 2.x)");
    }
    schema = dd::Catalog::GetSchema(context, catalog, schema_path[0],
                                    dd::OnEntryNotFound::RETURN_NULL);
#endif
    if (!schema) return arrow::Status::OK();
    found = true;
    resolved.catalog = compat::Str(schema->ParentCatalog().GetName());
    resolved.schema_path = SchemaPathOf(*schema);
    return arrow::Status::OK();
  }));
  if (!found) return AccessControlError("schema " + name.ToString() + " does not exist");
  return resolved;
}

arrow::Result<std::string> SessionObjectResolver::ResolveCatalog(const std::string& name) {
  std::string resolved;
  ARROW_RETURN_NOT_OK(InTransaction(connection_, [&]() -> arrow::Status {
    auto catalog = CatalogNamed(*connection_.context, name);
    if (catalog) resolved = compat::Str(catalog->GetName());
    return arrow::Status::OK();
  }));
  if (resolved.empty()) return AccessControlError("catalog " + name + " does not exist");
  return resolved;
}

}  // namespace gizmosql::enterprise::rbac
