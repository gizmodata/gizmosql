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

#include "rbac_model.h"

#include <algorithm>
#include <deque>

namespace gizmosql::enterprise::rbac {

namespace {

bool PathEquals(const std::vector<std::string>& a, const std::vector<std::string>& b) {
  if (a.size() != b.size()) return false;
  for (size_t i = 0; i < a.size(); ++i) {
    if (!IdentifiersEqual(a[i], b[i])) return false;
  }
  return true;
}

// A schema grant covers the schema and every schema nested under it (DuckDB
// 2.x), so reorganizing objects into sub-schemas never silently drops access.
bool PathHasPrefix(const std::vector<std::string>& path, const std::vector<std::string>& prefix) {
  if (prefix.size() > path.size()) return false;
  for (size_t i = 0; i < prefix.size(); ++i) {
    if (!IdentifiersEqual(path[i], prefix[i])) return false;
  }
  return true;
}

bool IsTableLevel(Privilege p) {
  return p == Privilege::kSelect || p == Privilege::kInsert || p == Privilege::kUpdate ||
         p == Privilege::kDelete;
}

}  // namespace

std::string ObjectKey(const ObjectPath& path) {
  // \x1f cannot appear in an identifier, so distinct paths never collide.
  std::string key = FoldIdentifier(path.catalog);
  for (const auto& part : path.schema_path) key += "\x1f" + FoldIdentifier(part);
  key += "\x1e" + FoldIdentifier(path.object_name);
  return key;
}

Snapshot::Snapshot(int64_t version, std::vector<UserRecord> users, std::vector<RoleRecord> roles,
                   std::vector<MembershipRecord> memberships,
                   std::vector<ObjectGrantRecord> grants, std::vector<OwnerRecord> owners,
                   std::vector<SystemPrivilegeRecord> system_privileges,
                   std::vector<RowPolicyRecord> row_policies,
                   std::vector<MaskingPolicyRecord> masking_policies,
                   std::vector<MaskBindingRecord> mask_bindings,
                   std::vector<TenantRecord> tenants,
                   std::vector<PrincipalTenantRecord> principal_tenants,
                   std::vector<CatalogTenantRecord> catalog_tenants)
    : version_(version),
      users_(std::move(users)),
      roles_(std::move(roles)),
      memberships_(std::move(memberships)),
      grants_(std::move(grants)),
      owners_(std::move(owners)),
      system_privileges_(std::move(system_privileges)),
      row_policies_(std::move(row_policies)),
      masking_policies_(std::move(masking_policies)),
      mask_bindings_(std::move(mask_bindings)),
      tenants_(std::move(tenants)),
      principal_tenants_(std::move(principal_tenants)),
      catalog_tenants_(std::move(catalog_tenants)) {
  for (size_t i = 0; i < users_.size(); ++i) {
    user_by_key_[FoldIdentifier(users_[i].name)] = i;
    user_by_id_[users_[i].id] = i;
  }
  for (size_t i = 0; i < roles_.size(); ++i) {
    role_by_key_[FoldIdentifier(roles_[i].name)] = i;
    role_by_id_[roles_[i].id] = i;
  }
  for (const auto& grant : grants_) grants_by_role_[grant.role_id].push_back(&grant);
}

const UserRecord* Snapshot::FindUserByName(const std::string& name) const {
  auto it = user_by_key_.find(FoldIdentifier(name));
  return it == user_by_key_.end() ? nullptr : &users_[it->second];
}
const UserRecord* Snapshot::FindUserById(const std::string& id) const {
  auto it = user_by_id_.find(id);
  return it == user_by_id_.end() ? nullptr : &users_[it->second];
}
const RoleRecord* Snapshot::FindRoleByName(const std::string& name) const {
  auto it = role_by_key_.find(FoldIdentifier(name));
  return it == role_by_key_.end() ? nullptr : &roles_[it->second];
}
const RoleRecord* Snapshot::FindRoleById(const std::string& id) const {
  auto it = role_by_id_.find(id);
  return it == role_by_id_.end() ? nullptr : &roles_[it->second];
}

std::set<std::string> Snapshot::RoleClosure(const std::string& role_id) const {
  std::set<std::string> seen{role_id};
  std::deque<std::string> queue{role_id};
  while (!queue.empty()) {
    const std::string current = queue.front();
    queue.pop_front();
    for (const auto& m : memberships_) {
      if (m.member_kind == PrincipalKind::kRole && m.member_id == current &&
          seen.insert(m.role_id).second) {
        queue.push_back(m.role_id);
      }
    }
  }
  return seen;
}

std::set<std::string> Snapshot::EffectiveRoleIds(const std::string& user_id) const {
  // A tenant's users hold that tenant's PUBLIC, not the server-wide one.
  const std::string tenant = TenantOf(PrincipalKind::kUser, user_id);
  const RoleRecord* tenant_public = tenant.empty() ? nullptr : TenantRole(tenant, kTenantPublicSuffix);
  std::set<std::string> roles = RoleClosure(tenant_public ? tenant_public->id : kPublicRoleId);
  for (const auto& m : memberships_) {
    if (m.member_kind == PrincipalKind::kUser && m.member_id == user_id) {
      for (const auto& r : RoleClosure(m.role_id)) roles.insert(r);
    }
  }
  return roles;
}

bool Snapshot::WouldCreateCycle(const std::string& role_id,
                                const std::string& member_role_id) const {
  // Granting role_id to member_role_id makes member inherit role_id. That is a
  // cycle if role_id already (transitively) holds member_role_id.
  return RoleClosure(role_id).count(member_role_id) > 0;
}

const OwnerRecord* Snapshot::FindOwner(const ObjectPath& object) const {
  const std::string key = ObjectKey(object);
  for (const auto& owner : owners_) {
    if (ObjectKey(owner.object) == key) return &owner;
  }
  return nullptr;
}

std::set<std::string> Snapshot::RolesOf(const Principal& principal) const {
  if (principal.is_role()) {
    if (!FindRoleById(principal.role_id)) return {};
    std::set<std::string> roles = RoleClosure(kPublicRoleId);
    for (const auto& r : RoleClosure(principal.role_id)) roles.insert(r);
    return roles;
  }
  const UserRecord* user = FindUserById(principal.user_id);
  if (!user || !user->enabled) return {};
  return EffectiveRoleIds(principal.user_id);
}

bool Snapshot::IsOwner(const Principal& principal, const OwnerRecord& owner) const {
  if (principal.superuser) return owner.owner_kind == OwnerKind::kSystem;
  switch (owner.owner_kind) {
    case OwnerKind::kSystem:
      return false;
    case OwnerKind::kUser:
      return !principal.is_role() && owner.owner_id == principal.user_id;
    case OwnerKind::kRole:
      return RolesOf(principal).count(owner.owner_id) > 0;
  }
  return false;
}

bool Snapshot::GrantCovers(const ObjectGrantRecord& grant, const AccessRequest& request) const {
  // Privilege
  if (grant.privilege != request.privilege &&
      !(grant.privilege == Privilege::kAll && IsTableLevel(request.privilege))) {
    return false;
  }
  // Scope
  if (!IdentifiersEqual(grant.target.catalog, request.object.catalog)) return false;
  switch (grant.scope) {
    case GrantScope::kCatalog:
      return true;
    case GrantScope::kSchema:
      return PathHasPrefix(request.object.schema_path, grant.target.schema_path);
    case GrantScope::kObject:
      return PathEquals(request.object.schema_path, grant.target.schema_path) &&
             IdentifiersEqual(request.object.object_name, grant.target.object_name);
  }
  return false;
}

AccessDecision Snapshot::Check(const Principal& principal, const AccessRequest& request) const {
  if (principal.superuser) return {true, "superuser", "superuser"};
  const auto roles = RolesOf(principal);
  if (roles.empty()) {
    return {false, principal.is_role() ? "unknown role" : "unknown or disabled user", ""};
  }

  // The owner holds every privilege on the object (for a schema: on the schema
  // itself, which covers DROP SCHEMA).
  if (const OwnerRecord* owner = FindOwner(request.object)) {
    if (IsOwner(principal, *owner)) return {true, "owner", "owner"};
  }
  // The owner of a schema may create objects in it and drop them, as in Postgres.
  switch (request.privilege) {
    case Privilege::kCreateTable:
    case Privilege::kCreateView:
    case Privilege::kDropTable:
    case Privilege::kDropView: {
      ObjectPath schema = request.object;
      schema.object_name.clear();
      if (request.type != ObjectType::kSchema && !schema.schema_path.empty()) {
        if (const OwnerRecord* owner = FindOwner(schema)) {
          if (owner->type == ObjectType::kSchema && IsOwner(principal, *owner)) {
            return {true, "owner of the schema", "owner"};
          }
        }
      }
      break;
    }
    default:
      break;
  }

  std::set<std::string> covered_columns;
  bool all_columns = false;
  std::string satisfying_role;
  for (const auto& role_id : roles) {
    auto it = grants_by_role_.find(role_id);
    if (it == grants_by_role_.end()) continue;
    for (const ObjectGrantRecord* grant : it->second) {
      if (!GrantCovers(*grant, request)) continue;
      const RoleRecord* role = FindRoleById(role_id);
      if (request.privilege != Privilege::kSelect || grant->columns.empty()) {
        return {true, "granted " + PrivilegeName(grant->privilege),
                role ? role->name : role_id};
      }
      for (const auto& c : grant->columns) covered_columns.insert(FoldIdentifier(c));
      if (satisfying_role.empty() && role) satisfying_role = role->name;
    }
  }
  if (request.privilege == Privilege::kSelect && !covered_columns.empty() && !all_columns) {
    std::vector<std::string> missing;
    for (const auto& c : request.columns) {
      if (!covered_columns.count(FoldIdentifier(c))) missing.push_back(c);
    }
    if (missing.empty()) return {true, "granted SELECT on the columns read", satisfying_role};
    std::string list;
    for (size_t i = 0; i < missing.size(); ++i) list += (i ? ", " : "") + missing[i];
    return {false, "no SELECT on column(s) " + list, ""};
  }
  return {false, "no " + PrivilegeName(request.privilege) + " privilege", ""};
}

std::set<std::string> Snapshot::RightsOf(const Principal& principal) const {
  std::set<std::string> rights;
  for (const auto& role_id : RolesOf(principal)) {
    rights.insert("role\x1f" + role_id);
    auto it = grants_by_role_.find(role_id);
    if (it == grants_by_role_.end()) continue;
    for (const ObjectGrantRecord* g : it->second) {
      std::vector<std::string> columns;
      for (const auto& c : g->columns) columns.push_back(FoldIdentifier(c));
      std::sort(columns.begin(), columns.end());
      std::string key = "grant\x1f" + PrivilegeName(g->privilege) + "\x1f" +
                        std::to_string(static_cast<int>(g->scope)) + "\x1f" + ObjectKey(g->target);
      // Each column is its own right, so narrowing a column grant takes one away.
      if (columns.empty()) {
        rights.insert(key);
      } else {
        for (const auto& c : columns) rights.insert(key + "\x1f" + c);
      }
    }
  }
  for (const auto& o : owners_) {
    if (IsOwner(principal, o)) rights.insert("owner\x1f" + ObjectKey(o.object));
  }
  for (auto p : AllSystemPrivileges()) {
    if (!principal.superuser && HasSystemPrivilege(principal, p)) {
      rights.insert("system\x1f" + SystemPrivilegeName(p));
    }
  }
  return rights;
}

namespace {

// What the principal's row and masking policies are: any change to them can
// hide rows or mask columns that a running statement was planned to see.
std::string PolicyFingerprint(const Snapshot& snap, const Principal& principal) {
  std::set<std::string> parts;
  for (const auto& p : snap.row_policies()) {
    // Any policy makes its table protected (no rows unless one applies); the
    // ones that apply decide which rows.
    parts.insert("protected\x1f" + ObjectKey(p.table));
    for (const auto* applicable : snap.ApplicableRowPolicies(principal, p.table)) {
      if (applicable->id == p.id) {
        parts.insert("row\x1f" + ObjectKey(p.table) + "\x1f" + p.id + "\x1f" + p.predicate +
                     (p.permissive ? "\x1f" "p" : "\x1f" "r"));
      }
    }
  }
  for (const auto& b : snap.mask_bindings()) {
    const auto* policy = snap.FindMaskingPolicyById(b.policy_id);
    bool exempt = true;
    for (const auto& [column, applicable] : snap.ApplicableMasks(principal, b.table)) {
      if (IdentifiersEqual(column, b.column)) exempt = false;
    }
    parts.insert("mask\x1f" + ObjectKey(b.table) + "\x1f" + FoldIdentifier(b.column) + "\x1f" +
                 (policy ? policy->body : "") + (exempt ? "\x1f" "exempt" : ""));
  }
  if (const auto* user = snap.FindUserById(principal.user_id)) {
    for (const auto& [k, v] : user->attributes) parts.insert("attr\x1f" + k + "\x1f" + v);
  }
  std::string out;
  for (const auto& p : parts) out += p + "\x1e";
  return out;
}

}  // namespace

bool Snapshot::LostRightsSince(const Snapshot& before, const Principal& principal) const {
  if (PolicyFingerprint(before, principal) != PolicyFingerprint(*this, principal)) return true;
  const auto after = RightsOf(principal);
  static const std::string kOwnerPrefix = "owner\x1f";
  for (const auto& right : before.RightsOf(principal)) {
    if (after.count(right)) continue;
    if (right.rfind(kOwnerPrefix, 0) == 0) {
      const std::string key = right.substr(kOwnerPrefix.size());
      if (std::none_of(owners_.begin(), owners_.end(),
                       [&](const OwnerRecord& o) { return ObjectKey(o.object) == key; })) {
        continue;  // dropped: nothing left to protect
      }
    }
    return true;
  }
  return false;
}

bool Snapshot::HasSystemPrivilege(const Principal& principal, SystemPrivilege privilege) const {
  if (principal.superuser) return true;
  const auto roles = RolesOf(principal);
  for (const auto& r : system_privileges_) {
    if (r.privilege == privilege && roles.count(r.role_id)) return true;
  }
  return false;
}

bool Snapshot::CanGrantSystemPrivilege(const Principal& principal, SystemPrivilege privilege) const {
  if (principal.superuser) return true;
  const auto roles = RolesOf(principal);
  for (const auto& r : system_privileges_) {
    if (r.privilege == privilege && r.admin_option && roles.count(r.role_id)) return true;
  }
  return false;
}

const MaskingPolicyRecord* Snapshot::FindMaskingPolicyByName(const std::string& name) const {
  for (const auto& m : masking_policies_) {
    if (IdentifiersEqual(m.name, name)) return &m;
  }
  return nullptr;
}

const MaskingPolicyRecord* Snapshot::FindMaskingPolicyById(const std::string& id) const {
  for (const auto& m : masking_policies_) {
    if (m.id == id) return &m;
  }
  return nullptr;
}

std::vector<const RowPolicyRecord*> Snapshot::RowPoliciesOn(const ObjectPath& table) const {
  std::vector<const RowPolicyRecord*> found;
  const std::string key = ObjectKey(table);
  for (const auto& p : row_policies_) {
    if (ObjectKey(p.table) == key) found.push_back(&p);
  }
  return found;
}

std::vector<const RowPolicyRecord*> Snapshot::ApplicableRowPolicies(const Principal& principal,
                                                                   const ObjectPath& table) const {
  std::vector<const RowPolicyRecord*> found;
  const auto roles = RolesOf(principal);
  for (const auto* p : RowPoliciesOn(table)) {
    bool applies = p->role_ids.empty();
    for (const auto& r : p->role_ids) applies = applies || roles.count(r) > 0;
    if (applies) found.push_back(p);
  }
  return found;
}

std::vector<std::pair<std::string, const MaskingPolicyRecord*>> Snapshot::ApplicableMasks(
    const Principal& principal, const ObjectPath& table) const {
  std::vector<std::pair<std::string, const MaskingPolicyRecord*>> found;
  const std::string key = ObjectKey(table);
  const auto roles = RolesOf(principal);
  for (const auto& b : mask_bindings_) {
    if (ObjectKey(b.table) != key) continue;
    bool exempt = false;
    for (const auto& r : b.exempt_role_ids) exempt = exempt || roles.count(r) > 0;
    if (exempt) continue;
    // A binding whose policy is gone masks with nothing: fail closed in the rewrite.
    found.emplace_back(b.column, FindMaskingPolicyById(b.policy_id));
  }
  return found;
}

const TenantRecord* Snapshot::FindTenantByName(const std::string& name) const {
  for (const auto& t : tenants_) {
    if (IdentifiersEqual(t.name, name)) return &t;
  }
  return nullptr;
}

const TenantRecord* Snapshot::FindTenantById(const std::string& id) const {
  for (const auto& t : tenants_) {
    if (t.id == id) return &t;
  }
  return nullptr;
}

std::string Snapshot::TenantOf(PrincipalKind kind, const std::string& id) const {
  for (const auto& p : principal_tenants_) {
    if (p.kind == kind && p.principal_id == id) return p.tenant_id;
  }
  return "";
}

std::string Snapshot::TenantOfPrincipal(const Principal& principal) const {
  if (principal.superuser) return "";
  if (principal.is_role()) return TenantOf(PrincipalKind::kRole, principal.role_id);
  return TenantOf(PrincipalKind::kUser, principal.user_id);
}

std::string Snapshot::TenantOfCatalog(const std::string& catalog) const {
  for (const auto& c : catalog_tenants_) {
    if (IdentifiersEqual(c.catalog, catalog)) return c.tenant_id;
  }
  return "";
}

std::set<std::string> Snapshot::TenantCatalogs(const std::string& tenant_id) const {
  std::set<std::string> catalogs;
  for (const auto& c : catalog_tenants_) {
    if (c.tenant_id == tenant_id) catalogs.insert(FoldIdentifier(c.catalog));
  }
  return catalogs;
}

bool Snapshot::CatalogVisible(const Principal& principal, const std::string& catalog) const {
  const std::string tenant = TenantOfPrincipal(principal);
  if (tenant.empty()) return true;
  // GizmoSQL's helper catalog holds views for JDBC metadata calls; what they
  // list is limited to the user's catalogs like any metadata.
  if (IdentifiersEqual(catalog, "temp") || IdentifiersEqual(catalog, "system") ||
      IdentifiersEqual(catalog, "_gizmosql_system")) {
    return true;
  }
  return TenantOfCatalog(catalog) == tenant;
}

const RoleRecord* Snapshot::TenantRole(const std::string& tenant_id, const char* suffix) const {
  const TenantRecord* tenant = FindTenantById(tenant_id);
  return tenant ? FindRoleByName(tenant->name + suffix) : nullptr;
}

// ---------------------------------------------------------------------------
// Delegation
// ---------------------------------------------------------------------------

namespace {

// Does a grant held WITH GRANT OPTION cover passing on `wanted` (same
// privilege, a scope at least as wide, and every column wanted)?
bool HolderCovers(const ObjectGrantRecord& holder, GrantScope scope, const ObjectPath& target,
                  Privilege privilege, const std::vector<std::string>& columns) {
  if (!holder.grant_option) return false;
  if (holder.privilege != privilege &&
      !(holder.privilege == Privilege::kAll && IsTableLevel(privilege))) {
    return false;
  }
  if (!IdentifiersEqual(holder.target.catalog, target.catalog)) return false;
  switch (holder.scope) {
    case GrantScope::kCatalog:
      break;
    case GrantScope::kSchema:
      if (scope == GrantScope::kCatalog) return false;
      if (!PathHasPrefix(target.schema_path, holder.target.schema_path)) return false;
      break;
    case GrantScope::kObject:
      if (scope != GrantScope::kObject) return false;
      if (!PathEquals(target.schema_path, holder.target.schema_path) ||
          !IdentifiersEqual(target.object_name, holder.target.object_name)) {
        return false;
      }
      break;
  }
  if (!holder.columns.empty()) {
    if (columns.empty()) return false;  // a column grant cannot become a whole-table one
    for (const auto& c : columns) {
      if (std::none_of(holder.columns.begin(), holder.columns.end(),
                       [&](const std::string& h) { return IdentifiersEqual(h, c); })) {
        return false;
      }
    }
  }
  return true;
}

// The object, or for a schema-scope grant the schema, whose owner may grant on it.
ObjectPath OwnedTarget(GrantScope scope, const ObjectPath& target) {
  ObjectPath owned = target;
  if (scope == GrantScope::kSchema) owned.object_name.clear();
  return owned;
}

}  // namespace

bool Snapshot::CanGrant(const Principal& principal, GrantScope scope, const ObjectPath& target,
                        Privilege privilege, const std::vector<std::string>& columns) const {
  if (principal.superuser) return true;
  const auto roles = RolesOf(principal);
  if (roles.empty()) return false;
  if (scope != GrantScope::kCatalog) {
    if (const OwnerRecord* owner = FindOwner(OwnedTarget(scope, target))) {
      if (IsOwner(principal, *owner)) return true;
    }
    // The owner of the schema an object is in may grant on the object too.
    if (scope == GrantScope::kObject) {
      ObjectPath schema = target;
      schema.object_name.clear();
      if (const OwnerRecord* owner = FindOwner(schema)) {
        if (owner->type == ObjectType::kSchema && IsOwner(principal, *owner)) return true;
      }
    }
  }
  for (const auto& role_id : roles) {
    auto it = grants_by_role_.find(role_id);
    if (it == grants_by_role_.end()) continue;
    for (const ObjectGrantRecord* g : it->second) {
      if (HolderCovers(*g, scope, target, privilege, columns)) return true;
    }
  }
  return false;
}

bool Snapshot::CanAdminRole(const Principal& principal, const std::string& role_id) const {
  if (principal.superuser) return true;
  const auto roles = RolesOf(principal);
  if (roles.empty()) return false;
  for (const auto& m : memberships_) {
    if (m.role_id != role_id || !m.admin_option) continue;
    if (m.member_kind == PrincipalKind::kUser && !principal.is_role() &&
        m.member_id == principal.user_id) {
      return true;
    }
    if (m.member_kind == PrincipalKind::kRole && roles.count(m.member_id)) return true;
  }
  return false;
}

Snapshot::Fallen Snapshot::FallenDelegations() const {
  // Start from what the administrator granted, and grow what stands until it
  // stops changing.
  std::vector<bool> m_ok(memberships_.size()), g_ok(grants_.size()),
      s_ok(system_privileges_.size());
  for (size_t i = 0; i < memberships_.size(); ++i) m_ok[i] = memberships_[i].granted_by.empty();
  for (size_t i = 0; i < grants_.size(); ++i) g_ok[i] = grants_[i].granted_by.empty();
  for (size_t i = 0; i < system_privileges_.size(); ++i) {
    s_ok[i] = system_privileges_[i].granted_by.empty();
  }

  // A user's roles through standing memberships only.
  auto roles_of = [&](const std::string& user_id) {
    std::set<std::string> roles{kPublicRoleId};
    std::deque<std::string> queue{kPublicRoleId};
    for (size_t i = 0; i < memberships_.size(); ++i) {
      const auto& m = memberships_[i];
      if (m_ok[i] && m.member_kind == PrincipalKind::kUser && m.member_id == user_id &&
          roles.insert(m.role_id).second) {
        queue.push_back(m.role_id);
      }
    }
    while (!queue.empty()) {
      const std::string current = queue.front();
      queue.pop_front();
      for (size_t i = 0; i < memberships_.size(); ++i) {
        const auto& m = memberships_[i];
        if (m_ok[i] && m.member_kind == PrincipalKind::kRole && m.member_id == current &&
            roles.insert(m.role_id).second) {
          queue.push_back(m.role_id);
        }
      }
    }
    return roles;
  };
  auto owns = [&](const std::string& user_id, const std::set<std::string>& roles,
                  const ObjectPath& path, bool schema_only) {
    const OwnerRecord* owner = FindOwner(path);
    if (!owner || (schema_only && owner->type != ObjectType::kSchema)) return false;
    if (owner->owner_kind == OwnerKind::kUser) return owner->owner_id == user_id;
    if (owner->owner_kind == OwnerKind::kRole) return roles.count(owner->owner_id) > 0;
    return false;
  };

  bool changed = true;
  while (changed) {
    changed = false;
    for (size_t i = 0; i < memberships_.size(); ++i) {
      if (m_ok[i]) continue;
      const auto& m = memberships_[i];
      if (!FindUserById(m.granted_by)) continue;
      const auto roles = roles_of(m.granted_by);
      // A tenant admin administers its tenant's roles.
      const std::string tenant = TenantOf(PrincipalKind::kRole, m.role_id);
      if (!tenant.empty()) {
        const RoleRecord* admin = TenantRole(tenant, kTenantAdminSuffix);
        if (admin && roles.count(admin->id)) {
          m_ok[i] = changed = true;
          continue;
        }
      }
      for (size_t j = 0; j < memberships_.size(); ++j) {
        const auto& h = memberships_[j];
        if (!m_ok[j] || h.role_id != m.role_id || !h.admin_option) continue;
        if ((h.member_kind == PrincipalKind::kUser && h.member_id == m.granted_by) ||
            (h.member_kind == PrincipalKind::kRole && roles.count(h.member_id))) {
          m_ok[i] = changed = true;
          break;
        }
      }
    }
    for (size_t i = 0; i < grants_.size(); ++i) {
      if (g_ok[i]) continue;
      const auto& g = grants_[i];
      if (!FindUserById(g.granted_by)) continue;
      const auto roles = roles_of(g.granted_by);
      bool stands = false;
      if (g.scope != GrantScope::kCatalog) {
        stands = owns(g.granted_by, roles, OwnedTarget(g.scope, g.target), false);
        if (!stands && g.scope == GrantScope::kObject) {
          ObjectPath schema = g.target;
          schema.object_name.clear();
          stands = owns(g.granted_by, roles, schema, true);
        }
      }
      for (size_t j = 0; !stands && j < grants_.size(); ++j) {
        stands = j != i && g_ok[j] && roles.count(grants_[j].role_id) &&
                 HolderCovers(grants_[j], g.scope, g.target, g.privilege, g.columns);
      }
      if (stands) g_ok[i] = changed = true;
    }
    for (size_t i = 0; i < system_privileges_.size(); ++i) {
      if (s_ok[i]) continue;
      const auto& r = system_privileges_[i];
      if (!FindUserById(r.granted_by)) continue;
      const auto roles = roles_of(r.granted_by);
      for (size_t j = 0; j < system_privileges_.size(); ++j) {
        const auto& h = system_privileges_[j];
        if (j != i && s_ok[j] && h.privilege == r.privilege && h.admin_option &&
            roles.count(h.role_id)) {
          s_ok[i] = changed = true;
          break;
        }
      }
    }
  }

  Fallen fallen;
  for (size_t i = 0; i < memberships_.size(); ++i) {
    if (!m_ok[i]) fallen.memberships.push_back(memberships_[i]);
  }
  for (size_t i = 0; i < grants_.size(); ++i) {
    if (!g_ok[i]) fallen.grant_ids.push_back(grants_[i].id);
  }
  for (size_t i = 0; i < system_privileges_.size(); ++i) {
    if (!s_ok[i]) fallen.system_privileges.push_back(system_privileges_[i]);
  }
  return fallen;
}

}  // namespace gizmosql::enterprise::rbac
