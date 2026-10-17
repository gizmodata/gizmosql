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
                   std::vector<ObjectGrantRecord> grants, std::vector<OwnerRecord> owners)
    : version_(version),
      users_(std::move(users)),
      roles_(std::move(roles)),
      memberships_(std::move(memberships)),
      grants_(std::move(grants)),
      owners_(std::move(owners)) {
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
  std::set<std::string> roles = RoleClosure(kPublicRoleId);
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
  return rights;
}

bool Snapshot::LostRightsSince(const Snapshot& before, const Principal& principal) const {
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
  std::vector<bool> m_ok(memberships_.size()), g_ok(grants_.size());
  for (size_t i = 0; i < memberships_.size(); ++i) m_ok[i] = memberships_[i].granted_by.empty();
  for (size_t i = 0; i < grants_.size(); ++i) g_ok[i] = grants_[i].granted_by.empty();

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
  }

  Fallen fallen;
  for (size_t i = 0; i < memberships_.size(); ++i) {
    if (!m_ok[i]) fallen.memberships.push_back(memberships_[i]);
  }
  for (size_t i = 0; i < grants_.size(); ++i) {
    if (!g_ok[i]) fallen.grant_ids.push_back(grants_[i].id);
  }
  return fallen;
}

}  // namespace gizmosql::enterprise::rbac
