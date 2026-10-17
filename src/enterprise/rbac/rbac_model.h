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

// The in-memory model of the access-control store: an immutable snapshot of
// users, roles, role memberships, object grants and owners, plus the
// authorization questions asked of it. A snapshot is replaced as a whole when
// the store changes, so a check never sees a half-applied GRANT or REVOKE.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "rbac_types.h"

namespace gizmosql::enterprise::rbac {

// Every user is implicitly a member of PUBLIC.
constexpr const char* kPublicRoleId = "public";
constexpr const char* kPublicRoleName = "PUBLIC";

struct UserRecord {
  std::string id;
  std::string name;
  std::optional<std::string> password_hash;  // nullopt: no password login
  bool enabled = true;
  // Set by the administrator (ALTER USER ... SET ATTRIBUTE); row and masking
  // policies read them with session_attribute('key').
  std::map<std::string, std::string> attributes;
};

struct RoleRecord {
  std::string id;
  std::string name;
  bool builtin = false;
};

enum class PrincipalKind { kUser, kRole };

// `member` (a user or a role) is granted `role_id`.
struct MembershipRecord {
  PrincipalKind member_kind = PrincipalKind::kUser;
  std::string member_id;
  std::string role_id;
  bool admin_option = false;
  std::string granted_by;  // user id of the grantor; empty for the administrator
};

struct ObjectGrantRecord {
  std::string id;
  std::string role_id;
  GrantScope scope = GrantScope::kObject;
  ObjectPath target;  // catalog [+ schema path [+ object]] per scope
  Privilege privilege = Privilege::kSelect;
  std::vector<std::string> columns;  // SELECT only; empty = every column
  bool grant_option = false;
  std::string granted_by;  // user id of the grantor; empty for the administrator
};

// CREATE ROW POLICY name ON table [AS PERMISSIVE | RESTRICTIVE] [TO roles] USING (predicate)
struct RowPolicyRecord {
  std::string id;
  std::string name;
  ObjectPath table;
  bool permissive = true;
  std::vector<std::string> role_ids;  // empty: every role
  std::string predicate;             // SQL, as written
};

// CREATE MASKING POLICY name AS (parameter type) RETURNS type -> body
struct MaskingPolicyRecord {
  std::string id;
  std::string name;
  std::string parameter;
  std::string parameter_type;
  std::string return_type;
  std::string body;  // SQL over `parameter`, as written
};

// ALTER TABLE t MODIFY COLUMN c SET MASKING POLICY p [EXEMPT ROLE r, ...]
struct MaskBindingRecord {
  ObjectPath table;
  std::string column;
  std::string policy_id;
  std::vector<std::string> exempt_role_ids;
};

// A system privilege a role holds (one record per grantor).
struct SystemPrivilegeRecord {
  std::string role_id;
  SystemPrivilege privilege = SystemPrivilege::kCheckpoint;
  bool admin_option = false;
  std::string granted_by;  // user id of the grantor; empty for the administrator
};

// kSchema: the object is the schema itself (catalog + schema path, no name).
enum class ObjectType { kTable, kView, kSchema };

enum class OwnerKind { kSystem, kUser, kRole };

struct OwnerRecord {
  ObjectPath object;
  ObjectType type = ObjectType::kTable;
  OwnerKind owner_kind = OwnerKind::kSystem;
  std::string owner_id;  // user or role id; empty for kSystem
  // Views: a hash of the view's definition when ownership was recorded. A view
  // reads with its owner's rights only while its definition still matches, so
  // a record left behind by a dropped view never applies to a new one.
  std::string definition_hash;
};

// Who is asking. The administrator (a superuser) is never looked up in the
// store and is allowed everything. A view owned by a role reads with that
// role's rights: a role principal has `role_id` set and no `user_id`.
struct Principal {
  bool superuser = false;
  std::string user_id;    // for a store user
  std::string user_name;  // for messages and audit (a role principal: the role's name)
  std::string role_id;    // for a role principal

  static Principal Superuser(std::string name) { return {true, "", std::move(name), ""}; }
  static Principal Role(std::string id, std::string name) {
    return {false, "", std::move(name), std::move(id)};
  }
  bool is_role() const { return !superuser && user_id.empty() && !role_id.empty(); }
};

// One required privilege on one object, as the enforcement layer derives it
// from a statement's plan.
struct AccessRequest {
  ObjectPath object;  // full path to a table or view; for CREATE: the new object
  ObjectType type = ObjectType::kTable;
  Privilege privilege = Privilege::kSelect;
  std::vector<std::string> columns;  // SELECT: the columns read
};

struct AccessDecision {
  bool allowed = false;
  std::string reason;            // which grant satisfied it, or what is missing
  std::string satisfying_role;   // role (or "owner") whose grant allowed it
};

class Snapshot {
 public:
  Snapshot(int64_t version, std::vector<UserRecord> users, std::vector<RoleRecord> roles,
           std::vector<MembershipRecord> memberships, std::vector<ObjectGrantRecord> grants,
           std::vector<OwnerRecord> owners,
           std::vector<SystemPrivilegeRecord> system_privileges = {},
           std::vector<RowPolicyRecord> row_policies = {},
           std::vector<MaskingPolicyRecord> masking_policies = {},
           std::vector<MaskBindingRecord> mask_bindings = {});

  int64_t version() const { return version_; }

  const UserRecord* FindUserByName(const std::string& name) const;
  const UserRecord* FindUserById(const std::string& id) const;
  const RoleRecord* FindRoleByName(const std::string& name) const;
  const RoleRecord* FindRoleById(const std::string& id) const;
  const std::vector<UserRecord>& users() const { return users_; }
  const std::vector<RoleRecord>& roles() const { return roles_; }
  const std::vector<MembershipRecord>& memberships() const { return memberships_; }
  const std::vector<ObjectGrantRecord>& grants() const { return grants_; }
  const std::vector<OwnerRecord>& owners() const { return owners_; }
  const std::vector<SystemPrivilegeRecord>& system_privileges() const { return system_privileges_; }
  const std::vector<RowPolicyRecord>& row_policies() const { return row_policies_; }
  const std::vector<MaskingPolicyRecord>& masking_policies() const { return masking_policies_; }
  const std::vector<MaskBindingRecord>& mask_bindings() const { return mask_bindings_; }
  const MaskingPolicyRecord* FindMaskingPolicyByName(const std::string& name) const;
  const MaskingPolicyRecord* FindMaskingPolicyById(const std::string& id) const;
  // Row policies on the table, and of those the ones that apply to the principal.
  std::vector<const RowPolicyRecord*> RowPoliciesOn(const ObjectPath& table) const;
  std::vector<const RowPolicyRecord*> ApplicableRowPolicies(const Principal& principal,
                                                             const ObjectPath& table) const;
  // Masked columns of the table the principal is not exempt from: column -> policy.
  std::vector<std::pair<std::string, const MaskingPolicyRecord*>> ApplicableMasks(
      const Principal& principal, const ObjectPath& table) const;

  // Does the principal hold the system privilege through one of its roles?
  // Always true for the administrator.
  bool HasSystemPrivilege(const Principal& principal, SystemPrivilege privilege) const;
  // May it grant and revoke it (it holds it WITH ADMIN OPTION)?
  bool CanGrantSystemPrivilege(const Principal& principal, SystemPrivilege privilege) const;

  // Every role the user holds: PUBLIC, direct roles, and roles those roles
  // were granted, transitively.
  std::set<std::string> EffectiveRoleIds(const std::string& user_id) const;
  // Every role `role_id` holds, itself included.
  std::set<std::string> RoleClosure(const std::string& role_id) const;
  // Would granting `role_id` to role `member_role_id` create a cycle?
  bool WouldCreateCycle(const std::string& role_id, const std::string& member_role_id) const;

  const OwnerRecord* FindOwner(const ObjectPath& object) const;

  // The question every statement asks. Superusers are always allowed.
  AccessDecision Check(const Principal& principal, const AccessRequest& request) const;

  // Is the principal the owner of the object (directly, or through a role)?
  bool IsOwner(const Principal& principal, const OwnerRecord& owner) const;

  // Every role whose grants apply to the principal (empty for an unknown or
  // disabled user, or an unknown role).
  std::set<std::string> RolesOf(const Principal& principal) const;

  // Everything the principal may do, as comparable strings (each grant through
  // its roles, each object it owns). A right in one snapshot's set that is
  // missing from a later one was taken away.
  std::set<std::string> RightsOf(const Principal& principal) const;
  // Did the principal lose a right between `before` and this snapshot? Owning
  // an object that has no owner at all any more (it was dropped) is not one.
  bool LostRightsSince(const Snapshot& before, const Principal& principal) const;

  // ---- delegation ----
  // May the principal give `privilege` on `target` (at `scope`, on `columns`
  // for SELECT) to others: as the owner of the object or of its schema, or
  // holding it WITH GRANT OPTION through one of its roles? Always true for
  // the administrator.
  bool CanGrant(const Principal& principal, GrantScope scope, const ObjectPath& target,
                Privilege privilege, const std::vector<std::string>& columns) const;
  // May the principal grant and revoke membership in the role (it holds the
  // role WITH ADMIN OPTION)? Always true for the administrator.
  bool CanAdminRole(const Principal& principal, const std::string& role_id) const;

  // Delegated grants and memberships that no longer stand, because their
  // grantor can no longer make them. What stands is worked out from what the
  // administrator and the owners granted, so grants made through each other
  // in a cycle do not keep one another alive.
  struct Fallen {
    std::vector<std::string> grant_ids;
    std::vector<MembershipRecord> memberships;
    std::vector<SystemPrivilegeRecord> system_privileges;
    bool empty() const {
      return grant_ids.empty() && memberships.empty() && system_privileges.empty();
    }
  };
  Fallen FallenDelegations() const;

 private:
  bool GrantCovers(const ObjectGrantRecord& grant, const AccessRequest& request) const;

  int64_t version_;
  std::vector<UserRecord> users_;
  std::vector<RoleRecord> roles_;
  std::vector<MembershipRecord> memberships_;
  std::vector<ObjectGrantRecord> grants_;
  std::vector<OwnerRecord> owners_;
  std::vector<SystemPrivilegeRecord> system_privileges_;
  std::vector<RowPolicyRecord> row_policies_;
  std::vector<MaskingPolicyRecord> masking_policies_;
  std::vector<MaskBindingRecord> mask_bindings_;
  std::map<std::string, size_t> user_by_key_, user_by_id_, role_by_key_, role_by_id_;
  std::map<std::string, std::vector<const ObjectGrantRecord*>> grants_by_role_;
};

// Case-folded key for an object path, used for owner lookup and dedupe.
std::string ObjectKey(const ObjectPath& path);

}  // namespace gizmosql::enterprise::rbac
