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

// DuckDB side of access control: resolving the names in GRANT / REVOKE the way
// the caller's session resolves them in a query.

#pragma once

#include "rbac_engine.h"

namespace duckdb {
class Connection;
class CatalogEntry;
class SelectStatement;
class ViewCatalogEntry;
}

namespace gizmosql::enterprise::rbac {

// Resolves names through DuckDB's own catalog lookup on one session's
// connection: its search path, its default catalog, and DuckDB's rule for
// whether the first part of a dotted name is a catalog.
class SessionObjectResolver : public ObjectResolver {
 public:
  explicit SessionObjectResolver(duckdb::Connection& connection) : connection_(connection) {}
  arrow::Result<ResolvedObject> ResolveObject(const QualifiedName& name) override;
  arrow::Result<ObjectPath> ResolveSchema(const QualifiedName& name) override;
  arrow::Result<std::string> ResolveCatalog(const std::string& name) override;

 private:
  duckdb::Connection& connection_;
};

// catalog, schema path and name of a catalog entry (a table, view or schema).
ObjectPath PathOfEntry(duckdb::CatalogEntry& entry);

// SHA-256 (hex) of a view's query as DuckDB prints it back: the same for the
// CREATE VIEW that made it and for the view DuckDB reads from any catalog.
std::string ViewDefinitionHash(const duckdb::SelectStatement& query);

// A bound view's output column names, as grants on the view name them (the
// view's own column aliases, else the names its query gives them). Empty when
// the view has not been bound.
std::vector<std::string> ViewColumnNames(const duckdb::ViewCatalogEntry& view);

}  // namespace gizmosql::enterprise::rbac
