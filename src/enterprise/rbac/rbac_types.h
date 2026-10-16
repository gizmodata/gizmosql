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

// Shared vocabulary for GizmoSQL role-based access control (Enterprise):
// privileges, object names and the case-folding rule every RBAC component uses.

#pragma once

#include <optional>
#include <string>
#include <vector>

namespace gizmosql::enterprise::rbac {

// Privileges are keyword phrases ("CREATE TABLE"), never underscored names, and
// are printed exactly as they are written in GRANT.
enum class Privilege {
  kSelect,
  kInsert,
  kUpdate,
  kDelete,
  kCreateTable,
  kCreateView,
  kCreateSchema,
  kDropTable,
  kDropView,
  kDropSchema,
  kAlterTable,
  kAlterView,
  kAll,  // every object privilege that applies to the target
};

// "SELECT", "CREATE TABLE", ...
std::string PrivilegeName(Privilege privilege);

// The object privileges ALL stands for on a table or view.
const std::vector<Privilege>& TableLevelPrivileges();

// What a GRANT is attached to. kObject is one table or view; kSchema and
// kCatalog grant on every present and future object in the container.
enum class GrantScope { kObject, kSchema, kCatalog };

// One identifier, compared case-insensitively (DuckDB's rule for both quoted
// and unquoted identifiers) and stored as the user spelled it.
std::string FoldIdentifier(const std::string& identifier);
bool IdentifiersEqual(const std::string& a, const std::string& b);

// A catalog object: catalog, schema path (one element on DuckDB 1.x; nested
// schemas on 2.x) and object name. Empty schema_path / object_name mean "every"
// for schema- and catalog-scope grants.
struct ObjectPath {
  std::string catalog;
  std::vector<std::string> schema_path;
  std::string object_name;

  // `cat.sch.obj`, each part quoted when needed.
  std::string ToString() const;
};

// Quote an identifier for display only when it needs it.
std::string QuoteIdentifierIfNeeded(const std::string& identifier);

}  // namespace gizmosql::enterprise::rbac
