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

// Parser for GizmoSQL's access-control statements (CREATE USER / ROLE, GRANT,
// REVOKE, SHOW GRANTS, ...). DuckDB has no grammar for these, so GizmoSQL reads
// them itself and they never reach the engine. One statement per call: any text
// after it other than a single trailing semicolon is an error, so a second
// statement can never ride along unparsed.

#pragma once

#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <arrow/result.h>

#include "rbac_types.h"

namespace gizmosql::enterprise::rbac {

// A name as written: `a`, `a.b`, `"a"."b".c`, ... (parts already unquoted).
struct QualifiedName {
  std::vector<std::string> parts;
  std::string ToString() const;
};

enum class GranteeKind { kUnspecified, kUser, kRole };

struct Grantee {
  GranteeKind kind = GranteeKind::kUnspecified;
  std::string name;
};

struct PrivilegeItem {
  Privilege privilege;
  // SELECT (a, b) — column-level; empty means every column.
  std::vector<std::string> columns;
};

struct CreateUserCommand {
  std::string name;
  std::optional<std::string> password;  // nullopt: token-only user
  bool if_not_exists = false;
};

struct AlterUserCommand {
  enum class Action { kSetPassword, kClearPassword, kEnable, kDisable, kSetAttribute,
                      kUnsetAttribute };
  std::string name;
  Action action;
  std::string password;   // kSetPassword
  std::string attribute;  // kSetAttribute / kUnsetAttribute
  std::string value;      // kSetAttribute
};

struct DropUserCommand {
  std::string name;
  bool if_exists = false;
};

struct CreateRoleCommand {
  std::string name;
  bool if_not_exists = false;
};

struct DropRoleCommand {
  std::string name;
  bool if_exists = false;
};

// GRANT role, ... TO grantee, ...   /   REVOKE role, ... FROM grantee, ...
struct RoleMembershipCommand {
  bool grant = true;
  std::vector<std::string> roles;
  std::vector<Grantee> grantees;
  bool with_admin_option = false;  // GRANT ... WITH ADMIN OPTION
};

// GRANT priv, ... ON [TABLE|VIEW] obj | SCHEMA s | CATALOG c TO role, ...
struct ObjectPrivilegeCommand {
  bool grant = true;
  std::vector<PrivilegeItem> privileges;
  GrantScope scope = GrantScope::kObject;
  QualifiedName target;
  std::vector<Grantee> grantees;
  bool with_grant_option = false;
  bool cascade = true;  // REVOKE ... [CASCADE | RESTRICT]
};

// GRANT sys_priv, ... TO role, ... [WITH ADMIN OPTION]
// REVOKE [ADMIN OPTION FOR] sys_priv, ... FROM role, ... [CASCADE | RESTRICT]
struct SystemPrivilegeCommand {
  bool grant = true;
  std::vector<SystemPrivilege> privileges;
  std::vector<Grantee> grantees;
  bool with_admin_option = false;  // REVOKE: only the admin option
  bool cascade = true;
};

// CREATE [OR REPLACE] ROW POLICY name ON table [AS PERMISSIVE | RESTRICTIVE]
//   [TO role, ...] USING (predicate)
struct CreateRowPolicyCommand {
  std::string name;
  bool or_replace = false;
  QualifiedName table;
  bool permissive = true;
  std::vector<std::string> roles;  // empty: every role
  std::string predicate;           // the SQL between the parentheses, as written
};

// DROP ROW POLICY [IF EXISTS] name ON table
struct DropRowPolicyCommand {
  std::string name;
  QualifiedName table;
  bool if_exists = false;
};

// CREATE [OR REPLACE] MASKING POLICY name AS (parameter type) RETURNS type -> body
struct CreateMaskingPolicyCommand {
  std::string name;
  bool or_replace = false;
  std::string parameter;
  std::string parameter_type;  // as written
  std::string return_type;     // as written
  std::string body;            // as written
};

// DROP MASKING POLICY [IF EXISTS] name
struct DropMaskingPolicyCommand {
  std::string name;
  bool if_exists = false;
};

// ALTER TABLE t MODIFY [COLUMN] c SET MASKING POLICY p [EXEMPT ROLE r, ...]
// ALTER TABLE t MODIFY [COLUMN] c UNSET MASKING POLICY
struct SetColumnMaskCommand {
  QualifiedName table;
  std::string column;
  std::optional<std::string> policy;  // nullopt: UNSET
  std::vector<std::string> exempt_roles;
};

// CREATE TENANT [IF NOT EXISTS] name / DROP TENANT [IF EXISTS] name
struct TenantCommand {
  bool create = true;
  std::string name;
  bool if_exists = false;  // IF NOT EXISTS for CREATE
};

// ALTER CATALOG c SET TENANT t | UNSET TENANT
struct CatalogTenantCommand {
  std::string catalog;
  std::optional<std::string> tenant;  // nullopt: UNSET
};

// ALTER TENANT t SET DEFAULT CATALOG c | UNSET DEFAULT CATALOG
struct TenantDefaultCatalogCommand {
  std::string tenant;
  std::string catalog;  // empty: UNSET
};

// SHOW ROW POLICIES [ON table] | SHOW MASKING POLICIES
struct ShowPoliciesCommand {
  bool masking = false;
  std::optional<QualifiedName> table;
};

// SHOW TENANTS
struct ShowTenantsCommand {};

// CREATE SCOPED TOKEN name [FOR USER user] ALLOW SELECT[, INSERT, UPDATE, DELETE]
//   [ON CATALOG c[, ...]] [VALID FOR '8 hours']
// DROP SCOPED TOKEN name [FOR USER user]
// SHOW SCOPED TOKENS
struct ScopedTokenCommand {
  enum class Kind { kCreate, kDrop, kShow };
  Kind kind = Kind::kShow;
  std::string name;
  std::optional<std::string> for_user;
  std::vector<Privilege> privileges;
  std::vector<std::string> catalogs;
  std::optional<std::string> valid_for;
};

// CREATE GROUP MAPPING 'group' [ISSUER 'issuer'] TO ROLE role
// DROP GROUP MAPPING 'group' [ISSUER 'issuer'] [TO ROLE role]
// SHOW GROUP MAPPINGS
struct GroupMappingCommand {
  enum class Kind { kCreate, kDrop, kShow };
  Kind kind = Kind::kShow;
  std::string group;
  std::string issuer;
  std::optional<std::string> role;
};

// SET ROLE role | SET ROLE ALL | SET ROLE NONE | RESET ROLE
// (no role: act with all of the user's roles again)
struct SetRoleCommand {
  std::optional<std::string> role;
};

// ALTER TABLE | VIEW obj OWNER TO principal
struct AlterOwnerCommand {
  bool is_view = false;
  QualifiedName target;
  Grantee new_owner;
};

// SHOW GRANTS [FOR | TO] [USER | ROLE] name ; bare SHOW GRANTS = the caller.
struct ShowGrantsCommand {
  std::optional<Grantee> principal;
};

using RbacCommand =
    std::variant<CreateUserCommand, AlterUserCommand, DropUserCommand, CreateRoleCommand,
                 DropRoleCommand, RoleMembershipCommand, ObjectPrivilegeCommand,
                 SystemPrivilegeCommand, AlterOwnerCommand, ShowGrantsCommand,
                 CreateRowPolicyCommand, DropRowPolicyCommand, CreateMaskingPolicyCommand,
                 DropMaskingPolicyCommand, SetColumnMaskCommand, ShowPoliciesCommand,
                 TenantCommand, CatalogTenantCommand, TenantDefaultCatalogCommand,
                 ShowTenantsCommand, SetRoleCommand, GroupMappingCommand,
                 ScopedTokenCommand>;

// True when `sql` begins like an access-control statement (GRANT, REVOKE,
// CREATE/ALTER/DROP USER or ROLE, ALTER TABLE|VIEW ... OWNER TO, SHOW GRANTS).
// Cheap; comments and whitespace before the first keyword are skipped. A true
// result means the statement is GizmoSQL's to parse (and reject if malformed);
// it is never forwarded to DuckDB.
bool LooksLikeRbacStatement(const std::string& sql);

// Parse one access-control statement. Errors are user-facing.
arrow::Result<RbacCommand> ParseRbacStatement(const std::string& sql);

// The SQL statement kind for logs and audit ("GRANT", "CREATE ROLE", ...).
std::string CommandKind(const RbacCommand& command);

}  // namespace gizmosql::enterprise::rbac
