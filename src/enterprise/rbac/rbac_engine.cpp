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

#include "rbac_engine.h"

#include "rbac_policies.h"

#include <algorithm>
#include <set>

namespace gizmosql::enterprise::rbac {

arrow::Status AccessControlError(const std::string& message) {
  return arrow::Status::Invalid("GizmoSQL access control: " + message);
}

namespace {

CommandResult Message(const std::string& text) {
  CommandResult result;
  result.column_names = {"result"};
  result.rows = {{text}};
  return result;
}

std::string Join(const std::vector<std::string>& items, const std::string& sep = ", ") {
  std::string out;
  for (size_t i = 0; i < items.size(); ++i) out += (i ? sep : "") + items[i];
  return out;
}

std::string ScopeLabel(GrantScope scope) {
  switch (scope) {
    case GrantScope::kObject:
      return "TABLE";
    case GrantScope::kSchema:
      return "SCHEMA";
    case GrantScope::kCatalog:
      return "CATALOG";
  }
  return "TABLE";
}

bool SamePath(const ObjectPath& a, const ObjectPath& b) { return ObjectKey(a) == ObjectKey(b); }

// Privileges that one GRANT row stands for (ALL expands to the DML set).
std::vector<Privilege> Expand(Privilege p) {
  if (p == Privilege::kAll) return TableLevelPrivileges();
  return {p};
}

struct ResolvedGrantee {
  PrincipalKind kind;
  std::string id;
  std::string name;
};

// Users and roles share one namespace, so an unqualified name is unambiguous.
arrow::Result<ResolvedGrantee> ResolveGrantee(const Snapshot& snap, const Grantee& g) {
  if (g.kind != GranteeKind::kRole) {
    if (const auto* user = snap.FindUserByName(g.name)) {
      return ResolvedGrantee{PrincipalKind::kUser, user->id, user->name};
    }
    if (g.kind == GranteeKind::kUser) return AccessControlError("user " + g.name + " does not exist");
  }
  if (const auto* role = snap.FindRoleByName(g.name)) {
    return ResolvedGrantee{PrincipalKind::kRole, role->id, role->name};
  }
  return AccessControlError((g.kind == GranteeKind::kRole ? "role " : "user or role ") + g.name +
                            " does not exist");
}

arrow::Status CheckNameFree(const Snapshot& snap, const std::string& name) {
  if (IdentifiersEqual(name, kPublicRoleName)) {
    return AccessControlError("PUBLIC is a reserved role name");
  }
  if (snap.FindUserByName(name)) return AccessControlError("a user named " + name + " already exists");
  if (snap.FindRoleByName(name)) return AccessControlError("a role named " + name + " already exists");
  return arrow::Status::OK();
}

std::vector<std::string> OwnedObjects(const Snapshot& snap, OwnerKind kind, const std::string& id) {
  std::vector<std::string> owned;
  for (const auto& o : snap.owners()) {
    if (o.owner_kind == kind && o.owner_id == id) owned.push_back(o.object.ToString());
  }
  return owned;
}

}  // namespace

// ---------------------------------------------------------------------------

arrow::Result<std::shared_ptr<RbacEngine>> RbacEngine::Create(const Options& options) {
  std::shared_ptr<RbacEngine> engine(new RbacEngine(options));
  ARROW_ASSIGN_OR_RAISE(engine->dummy_hash_,
                        HashPassword("gizmosql-timing-equalizer", options.password_iterations));
  ARROW_ASSIGN_OR_RAISE(engine->store_, RbacStore::Open(options.store_uri));
  ARROW_ASSIGN_OR_RAISE(auto snapshot, engine->store_->Load());
  engine->Publish(std::move(snapshot));
  if (options.poll_interval.count() > 0) {
    engine->poller_ = std::thread([raw = engine.get()] { raw->PollLoop(); });
  }
  return engine;
}

RbacEngine::~RbacEngine() {
  {
    std::lock_guard<std::mutex> lock(poll_mutex_);
    stopping_ = true;
  }
  poll_cv_.notify_all();
  if (poller_.joinable()) poller_.join();
}

void RbacEngine::PollLoop() {
  std::unique_lock<std::mutex> lock(poll_mutex_);
  while (!stopping_) {
    poll_cv_.wait_for(lock, options_.poll_interval, [this] { return stopping_; });
    if (stopping_) break;
    lock.unlock();
    (void)Refresh();  // a store blip keeps the last good snapshot
    lock.lock();
  }
}

std::shared_ptr<const Snapshot> RbacEngine::snapshot() const {
  std::lock_guard<std::mutex> lock(snapshot_mutex_);
  return snapshot_;
}

void RbacEngine::Publish(std::shared_ptr<const Snapshot> snapshot) {
  bool replaced = false;
  {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    // Never go backwards (a slow reload must not undo a newer local write).
    if (!snapshot_ || snapshot->version() > snapshot_->version()) {
      snapshot_ = snapshot;
      replaced = true;
    }
  }
  if (!replaced) return;
  std::lock_guard<std::mutex> lock(listener_mutex_);
  if (listener_) listener_(*snapshot);
}

void RbacEngine::SetSnapshotListener(SnapshotListener listener) {
  std::lock_guard<std::mutex> lock(listener_mutex_);
  listener_ = std::move(listener);
}

arrow::Status RbacEngine::Refresh() {
  ARROW_ASSIGN_OR_RAISE(auto version, store_->CurrentVersion());
  if (version == snapshot()->version()) return arrow::Status::OK();
  ARROW_ASSIGN_OR_RAISE(auto fresh, store_->Load());
  Publish(std::move(fresh));
  return arrow::Status::OK();
}

arrow::Result<Principal> RbacEngine::AuthenticatePassword(const std::string& user_name,
                                                          const std::string& password) const {
  static const auto kFailure =
      AccessControlError("invalid user name or password");
  auto snap = snapshot();
  const UserRecord* user = snap->FindUserByName(user_name);
  if (!user || !user->password_hash) {
    // Spend the same time as a real check so a probe cannot tell which names exist.
    (void)VerifyPassword(password, dummy_hash_);
    return kFailure;
  }
  if (!VerifyPassword(password, *user->password_hash) || !user->enabled) return kFailure;
  return Principal{false, user->id, user->name};
}

arrow::Result<Principal> RbacEngine::PrincipalForUser(const std::string& user_name) const {
  auto snap = snapshot();
  const UserRecord* user = snap->FindUserByName(user_name);
  if (!user) {
    return AccessControlError("user " + user_name +
                              " is not defined on this server. An administrator creates it with "
                              "CREATE USER");
  }
  if (!user->enabled) return AccessControlError("user " + user->name + " is disabled");
  return Principal{false, user->id, user->name};
}

bool RbacEngine::IsActive(const Principal& principal) const {
  if (principal.superuser) return true;
  const UserRecord* user = snapshot()->FindUserById(principal.user_id);
  return user && user->enabled;
}

arrow::Status RbacEngine::RecordOwner(const ObjectPath& object, ObjectType type,
                                      const Principal& owner, const std::string& definition_hash) {
  ARROW_RETURN_NOT_OK(store_->Mutate([&](StoreTransaction& txn, const Snapshot&) {
    OwnerRecord record;
    record.object = object;
    record.type = type;
    record.owner_kind = owner.superuser ? OwnerKind::kSystem : OwnerKind::kUser;
    record.owner_id = owner.superuser ? "" : owner.user_id;
    record.definition_hash = definition_hash;
    return txn.UpsertOwner(record);
  }));
  ARROW_ASSIGN_OR_RAISE(auto snap, store_->Load());
  Publish(std::move(snap));
  return arrow::Status::OK();
}

arrow::Status RbacEngine::ForgetOwner(const ObjectPath& object) {
  if (!snapshot()->FindOwner(object)) return arrow::Status::OK();
  ARROW_RETURN_NOT_OK(store_->Mutate(
      [&](StoreTransaction& txn, const Snapshot&) { return txn.DeleteOwner(object); }));
  ARROW_ASSIGN_OR_RAISE(auto snap, store_->Load());
  Publish(std::move(snap));
  return arrow::Status::OK();
}

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

arrow::Result<CommandResult> RbacEngine::Execute(const RbacCommand& command,
                                                 const Principal& actor,
                                                 ObjectResolver& resolver) {
  // SHOW GRANTS is a read; everything else changes the store.
  if (const auto* show = std::get_if<ShowGrantsCommand>(&command)) {
    auto snap = snapshot();
    CommandResult result;
    result.column_names = {"privilege", "object_type", "object", "columns", "granted_to",
                           "grantee_type", "grant_option", "granted_by"};
    auto grantor_name = [&](const std::string& id) -> std::string {
      if (id.empty()) return "administrator";
      const auto* user = snap->FindUserById(id);
      return user ? user->name : id;
    };

    // Which roles to list: one role, or every role a user holds.
    std::vector<std::string> role_ids;
    if (!show->principal) {
      if (actor.superuser) {
        result.rows.push_back({"ALL", "SERVER", "*", "", actor.user_name, "SUPERUSER", "YES", ""});
        return result;
      }
      for (const auto& id : snap->EffectiveRoleIds(actor.user_id)) role_ids.push_back(id);
    } else {
      ARROW_ASSIGN_OR_RAISE(auto who, ResolveGrantee(*snap, *show->principal));
      if (who.kind == PrincipalKind::kUser) {
        if (!actor.superuser && who.id != actor.user_id) {
          return AccessControlError("GizmoSQL blocked SHOW GRANTS for another user. Users see "
                                    "their own grants; administrators see everyone's");
        }
        for (const auto& id : snap->EffectiveRoleIds(who.id)) role_ids.push_back(id);
      } else {
        if (!actor.superuser && !snap->EffectiveRoleIds(actor.user_id).count(who.id)) {
          return AccessControlError("GizmoSQL blocked SHOW GRANTS for role " + who.name +
                                    ", which you do not hold");
        }
        role_ids.push_back(who.id);
      }
      // Role memberships of the principal itself.
      for (const auto& m : snap->memberships()) {
        const bool member = (who.kind == PrincipalKind::kUser && m.member_kind == PrincipalKind::kUser &&
                             m.member_id == who.id) ||
                            (who.kind == PrincipalKind::kRole && m.member_kind == PrincipalKind::kRole &&
                             m.member_id == who.id);
        if (!member) continue;
        const auto* role = snap->FindRoleById(m.role_id);
        result.rows.push_back({"ROLE", "ROLE", role ? role->name : m.role_id, "", who.name,
                               who.kind == PrincipalKind::kUser ? "USER" : "ROLE",
                               m.admin_option ? "YES" : "NO", grantor_name(m.granted_by)});
      }
    }
    for (const auto& role_id : role_ids) {
      const auto* role = snap->FindRoleById(role_id);
      for (const auto& g : snap->grants()) {
        if (g.role_id != role_id) continue;
        result.rows.push_back({PrivilegeName(g.privilege), ScopeLabel(g.scope),
                               g.target.ToString(), Join(g.columns), role ? role->name : role_id,
                               "ROLE", g.grant_option ? "YES" : "NO", grantor_name(g.granted_by)});
      }
    }
    for (const auto& role_id : role_ids) {
      const auto* role = snap->FindRoleById(role_id);
      for (const auto& r : snap->system_privileges()) {
        if (r.role_id != role_id) continue;
        result.rows.push_back({SystemPrivilegeName(r.privilege), "SYSTEM", "", "",
                               role ? role->name : role_id, "ROLE", r.admin_option ? "YES" : "NO",
                               grantor_name(r.granted_by)});
      }
    }
    for (const auto& o : snap->owners()) {
      if (show->principal ? false : snap->IsOwner(actor, o)) {
        result.rows.push_back({"OWNERSHIP",
                               o.type == ObjectType::kView ? "VIEW"
                               : o.type == ObjectType::kSchema ? "SCHEMA"
                                                               : "TABLE",
                               o.object.ToString(), "", actor.user_name, "USER", "YES", ""});
      }
    }
    return result;
  }

  // Users and roles are the administrator's. Grants can be delegated: an owner
  // grants on what it owns, and WITH GRANT / ADMIN OPTION passes that on.
  const bool user_or_role_ddl =
      std::holds_alternative<CreateUserCommand>(command) ||
      std::holds_alternative<AlterUserCommand>(command) ||
      std::holds_alternative<DropUserCommand>(command) ||
      std::holds_alternative<CreateRoleCommand>(command) ||
      std::holds_alternative<DropRoleCommand>(command);
  if (!actor.superuser && user_or_role_ddl) {
    return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                              ": only administrators manage users and roles on this server");
  }

  CommandResult result;
  // Every change runs in one store transaction, which also removes the grants
  // and memberships the change left without a grantor able to make them
  // (REVOKE ... CASCADE, the default; RESTRICT refuses instead).
  size_t cascaded = 0;
  auto mutate_with = [&](bool restrict,
                         const std::function<arrow::Status(StoreTransaction&, const Snapshot&)>& fn)
      -> arrow::Status {
    ARROW_RETURN_NOT_OK(store_->Mutate([&](StoreTransaction& txn, const Snapshot& snap) {
      cascaded = 0;
      ARROW_RETURN_NOT_OK(fn(txn, snap));
      ARROW_ASSIGN_OR_RAISE(auto after, txn.Reload());
      const auto fallen = after->FallenDelegations();
      if (fallen.empty()) return arrow::Status::OK();
      if (restrict) {
        return AccessControlError(
            "other grants depend on this one; REVOKE ... CASCADE removes them too");
      }
      for (const auto& id : fallen.grant_ids) ARROW_RETURN_NOT_OK(txn.DeleteGrant(id));
      for (const auto& m : fallen.memberships) {
        ARROW_RETURN_NOT_OK(txn.DeleteMembership(m.member_kind, m.member_id, m.role_id));
      }
      for (const auto& r : fallen.system_privileges) {
        ARROW_RETURN_NOT_OK(txn.DeleteSystemPrivilege(r));
      }
      cascaded = fallen.grant_ids.size() + fallen.memberships.size() +
                 fallen.system_privileges.size();
      return arrow::Status::OK();
    }));
    ARROW_ASSIGN_OR_RAISE(auto snap, store_->Load());
    Publish(std::move(snap));
    return arrow::Status::OK();
  };
  auto mutate = [&](const std::function<arrow::Status(StoreTransaction&, const Snapshot&)>& fn) {
    return mutate_with(false, fn);
  };
  auto with_cascade_note = [&](std::string text) {
    if (cascaded > 0) {
      text += "; also revoked " + std::to_string(cascaded) +
              " grant(s) that were made through it";
    }
    return Message(text);
  };
  const std::string grantor = actor.superuser ? "" : actor.user_id;
  std::string message;

  // ---- CREATE USER -------------------------------------------------------
  if (const auto* c = std::get_if<CreateUserCommand>(&command)) {
    std::optional<std::string> hash;
    if (c->password) {
      ARROW_ASSIGN_OR_RAISE(auto h, HashPassword(*c->password, options_.password_iterations));
      hash = std::move(h);
    }
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      if (c->if_not_exists && snap.FindUserByName(c->name)) {
        message = "User " + c->name + " already exists";
        return arrow::Status::OK();
      }
      ARROW_RETURN_NOT_OK(CheckNameFree(snap, c->name));
      if (!options_.administrator_name.empty() &&
          IdentifiersEqual(c->name, options_.administrator_name)) {
        return AccessControlError(c->name + " is the server administrator's name");
      }
      message = "User " + c->name + " created";
      return txn.InsertUser({StoreTransaction::NewId(), c->name, hash, true});
    }));
    return Message(message);
  }

  // ---- ALTER USER --------------------------------------------------------
  if (const auto* c = std::get_if<AlterUserCommand>(&command)) {
    std::optional<std::string> hash;
    if (c->action == AlterUserCommand::Action::kSetPassword) {
      ARROW_ASSIGN_OR_RAISE(auto h, HashPassword(c->password, options_.password_iterations));
      hash = std::move(h);
    }
    std::string user_id;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* existing = snap.FindUserByName(c->name);
      if (!existing) return AccessControlError("user " + c->name + " does not exist");
      UserRecord user = *existing;
      user_id = user.id;
      switch (c->action) {
        case AlterUserCommand::Action::kSetPassword:
          user.password_hash = hash;
          message = "Password changed for user " + user.name;
          break;
        case AlterUserCommand::Action::kClearPassword:
          user.password_hash.reset();
          message = "Password login removed for user " + user.name;
          break;
        case AlterUserCommand::Action::kEnable:
          user.enabled = true;
          message = "User " + user.name + " enabled";
          break;
        case AlterUserCommand::Action::kDisable:
          user.enabled = false;
          message = "User " + user.name + " disabled";
          break;
        case AlterUserCommand::Action::kSetAttribute:
          message = "Attribute " + c->attribute + " set for user " + user.name;
          return txn.SetUserAttribute(user.id, c->attribute, c->value);
        case AlterUserCommand::Action::kUnsetAttribute:
          message = "Attribute " + c->attribute + " removed for user " + user.name;
          return txn.SetUserAttribute(user.id, c->attribute, std::nullopt);
      }
      return txn.UpdateUser(user);
    }));
    result = Message(message);
    if (c->action == AlterUserCommand::Action::kDisable ||
        c->action == AlterUserCommand::Action::kSetPassword ||
        c->action == AlterUserCommand::Action::kClearPassword) {
      result.terminate_user_ids.push_back(user_id);
    }
    return result;
  }

  // ---- DROP USER ---------------------------------------------------------
  if (const auto* c = std::get_if<DropUserCommand>(&command)) {
    std::string user_id;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* user = snap.FindUserByName(c->name);
      if (!user) {
        if (c->if_exists) {
          message = "User " + c->name + " does not exist";
          return arrow::Status::OK();
        }
        return AccessControlError("user " + c->name + " does not exist");
      }
      auto owned = OwnedObjects(snap, OwnerKind::kUser, user->id);
      if (!owned.empty()) {
        return AccessControlError("user " + user->name + " owns " + Join(owned) +
                                  ". Change the owner first (ALTER TABLE|VIEW ... OWNER TO)");
      }
      user_id = user->id;
      message = "User " + user->name + " dropped";
      return txn.DeleteUser(user->id);
    }));
    result = Message(message);
    if (!user_id.empty()) result.terminate_user_ids.push_back(user_id);
    return result;
  }

  // ---- CREATE ROLE -------------------------------------------------------
  if (const auto* c = std::get_if<CreateRoleCommand>(&command)) {
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      if (c->if_not_exists && snap.FindRoleByName(c->name)) {
        message = "Role " + c->name + " already exists";
        return arrow::Status::OK();
      }
      ARROW_RETURN_NOT_OK(CheckNameFree(snap, c->name));
      message = "Role " + c->name + " created";
      return txn.InsertRole({StoreTransaction::NewId(), c->name, false});
    }));
    return Message(message);
  }

  // ---- DROP ROLE ---------------------------------------------------------
  if (const auto* c = std::get_if<DropRoleCommand>(&command)) {
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* role = snap.FindRoleByName(c->name);
      if (!role) {
        if (c->if_exists) {
          message = "Role " + c->name + " does not exist";
          return arrow::Status::OK();
        }
        return AccessControlError("role " + c->name + " does not exist");
      }
      if (role->builtin) return AccessControlError("the built-in role " + role->name + " cannot be dropped");
      auto owned = OwnedObjects(snap, OwnerKind::kRole, role->id);
      if (!owned.empty()) {
        return AccessControlError("role " + role->name + " owns " + Join(owned) +
                                  ". Change the owner first (ALTER TABLE|VIEW ... OWNER TO)");
      }
      message = "Role " + role->name + " dropped";
      return txn.DeleteRole(role->id);
    }));
    return Message(message);
  }

  // ---- GRANT / REVOKE role -----------------------------------------------
  if (const auto* c = std::get_if<RoleMembershipCommand>(&command)) {
    std::vector<std::string> done;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      for (const auto& role_name : c->roles) {
        const auto* role = snap.FindRoleByName(role_name);
        if (!role) return AccessControlError("role " + role_name + " does not exist");
        if (role->id == kPublicRoleId) {
          return AccessControlError("every user already holds PUBLIC; it cannot be granted or revoked");
        }
        if (!snap.CanAdminRole(actor, role->id)) {
          return AccessControlError("GizmoSQL blocked " + CommandKind(command) + ": user " +
                                    actor.user_name + " may not grant or revoke role " +
                                    role->name + " (only an administrator, or a holder of it "
                                    "WITH ADMIN OPTION)");
        }
        for (const auto& g : c->grantees) {
          ARROW_ASSIGN_OR_RAISE(auto grantee, ResolveGrantee(snap, g));
          const MembershipRecord* existing = nullptr;
          for (const auto& m : snap.memberships()) {
            if (m.member_kind == grantee.kind && m.member_id == grantee.id && m.role_id == role->id) {
              existing = &m;
            }
          }
          if (c->grant) {
            if (grantee.kind == PrincipalKind::kRole) {
              if (grantee.id == role->id) return AccessControlError("a role cannot be granted to itself");
              if (snap.WouldCreateCycle(role->id, grantee.id)) {
                return AccessControlError("granting " + role->name + " to " + grantee.name +
                                          " would create a cycle of roles");
              }
            }
            if (existing) {
              if (c->with_admin_option && !existing->admin_option) {
                ARROW_RETURN_NOT_OK(txn.DeleteMembership(grantee.kind, grantee.id, role->id));
                MembershipRecord updated = *existing;
                updated.admin_option = true;
                updated.granted_by = grantor;
                ARROW_RETURN_NOT_OK(txn.InsertMembership(updated));
                done.push_back("Granted " + role->name + " to " + grantee.name +
                               " WITH ADMIN OPTION");
                continue;
              }
              done.push_back(grantee.name + " already holds " + role->name);
              continue;
            }
            ARROW_RETURN_NOT_OK(txn.InsertMembership(
                {grantee.kind, grantee.id, role->id, c->with_admin_option, grantor}));
            done.push_back("Granted " + role->name + " to " + grantee.name +
                           (c->with_admin_option ? " WITH ADMIN OPTION" : ""));
          } else {
            if (!existing) {
              done.push_back(grantee.name + " does not hold " + role->name);
              continue;
            }
            // Others revoke only what they granted; the administrator, anything.
            if (!actor.superuser && existing->granted_by != grantor) {
              return AccessControlError("GizmoSQL blocked REVOKE: " + grantee.name + " holds " +
                                        role->name + " from another grantor; only that grantor "
                                        "or an administrator can revoke it");
            }
            ARROW_RETURN_NOT_OK(txn.DeleteMembership(grantee.kind, grantee.id, role->id));
            if (c->with_admin_option) {  // REVOKE ADMIN OPTION FOR: keep the membership
              MembershipRecord updated = *existing;
              updated.admin_option = false;
              ARROW_RETURN_NOT_OK(txn.InsertMembership(updated));
              done.push_back("Revoked the admin option on " + role->name + " from " + grantee.name);
            } else {
              done.push_back("Revoked " + role->name + " from " + grantee.name);
            }
          }
        }
      }
      return arrow::Status::OK();
    }));
    return with_cascade_note(Join(done, "; "));
  }

  // ---- GRANT / REVOKE privileges -----------------------------------------
  if (const auto* c = std::get_if<ObjectPrivilegeCommand>(&command)) {
    ObjectPath target;
    std::vector<std::string> table_columns;
    switch (c->scope) {
      case GrantScope::kObject: {
        ARROW_ASSIGN_OR_RAISE(auto obj, resolver.ResolveObject(c->target));
        target = obj.path;
        table_columns = obj.columns;
        break;
      }
      case GrantScope::kSchema: {
        ARROW_ASSIGN_OR_RAISE(target, resolver.ResolveSchema(c->target));
        break;
      }
      case GrantScope::kCatalog: {
        ARROW_ASSIGN_OR_RAISE(target.catalog, resolver.ResolveCatalog(c->target.parts.at(0)));
        break;
      }
    }
    // Columns must exist, and are recorded as the table spells them.
    for (const auto& item : c->privileges) {
      for (const auto& col : item.columns) {
        auto it = std::find_if(table_columns.begin(), table_columns.end(),
                               [&](const std::string& t) { return IdentifiersEqual(t, col); });
        if (it == table_columns.end()) {
          return AccessControlError("column " + col + " does not exist in " + target.ToString());
        }
      }
    }
    std::vector<std::string> done;
    ARROW_RETURN_NOT_OK(mutate_with(!c->grant && !c->cascade,
                                    [&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      std::vector<ResolvedGrantee> grantees;
      for (const auto& g : c->grantees) {
        ARROW_ASSIGN_OR_RAISE(auto grantee, ResolveGrantee(snap, g));
        if (grantee.kind != PrincipalKind::kRole) {
          return AccessControlError("privileges are granted to roles, not users. Grant them to a "
                                    "role, then GRANT the role TO USER " + grantee.name);
        }
        grantees.push_back(std::move(grantee));
      }
      for (const auto& item : c->privileges) {
        for (Privilege p : Expand(item.privilege)) {
          const auto columns = p == Privilege::kSelect ? item.columns : std::vector<std::string>{};
          const std::string what = PrivilegeName(p) +
                                   (columns.empty() ? "" : " (" + Join(columns) + ")") + " on " +
                                   target.ToString();
          if (c->grant && !snap.CanGrant(actor, c->scope, target, p, columns)) {
            return AccessControlError("GizmoSQL blocked GRANT: user " + actor.user_name +
                                      " may not grant " + what +
                                      " (only its owner, or a role granted it WITH GRANT OPTION)");
          }
          for (const auto& grantee : grantees) {
            // One record per grantor, so each grant stands or falls with its own grantor.
            std::vector<const ObjectGrantRecord*> matching;
            const ObjectGrantRecord* own = nullptr;
            for (const auto& g : snap.grants()) {
              if (g.role_id == grantee.id && g.scope == c->scope && g.privilege == p &&
                  SamePath(g.target, target)) {
                matching.push_back(&g);
                if (g.granted_by == grantor) own = &g;
              }
            }
            if (c->grant) {
              if (own) {
                ObjectGrantRecord updated = *own;
                bool changed = false;
                if (c->with_grant_option && !own->grant_option) {
                  updated.grant_option = true;
                  changed = true;
                }
                if (!own->columns.empty() && columns.empty()) {
                  updated.columns.clear();  // widen a column grant to the whole table
                  changed = true;
                } else if (!own->columns.empty()) {
                  for (const auto& col : columns) {
                    if (std::none_of(updated.columns.begin(), updated.columns.end(),
                                     [&](const std::string& m) { return IdentifiersEqual(m, col); })) {
                      updated.columns.push_back(col);
                      changed = true;
                    }
                  }
                }
                if (changed) {
                  ARROW_RETURN_NOT_OK(txn.UpdateGrant(updated));
                  done.push_back("Granted " + what + " to " + grantee.name +
                                 (updated.grant_option ? " WITH GRANT OPTION" : ""));
                } else {
                  done.push_back(grantee.name + " already has " + what);
                }
                continue;
              }
              ObjectGrantRecord record;
              record.id = StoreTransaction::NewId();
              record.role_id = grantee.id;
              record.scope = c->scope;
              record.target = target;
              record.privilege = p;
              record.columns = columns;
              record.grant_option = c->with_grant_option;
              record.granted_by = grantor;
              ARROW_RETURN_NOT_OK(txn.InsertGrant(record));
              done.push_back("Granted " + what + " to " + grantee.name +
                             (c->with_grant_option ? " WITH GRANT OPTION" : ""));
              continue;
            }

            // REVOKE: the administrator's applies to every grantor's record;
            // anyone else's only to the records they granted.
            std::vector<const ObjectGrantRecord*> revocable;
            for (const auto* g : matching) {
              if (actor.superuser || g->granted_by == grantor) revocable.push_back(g);
            }
            if (revocable.empty()) {
              if (!matching.empty()) {
                return AccessControlError("GizmoSQL blocked REVOKE: " + grantee.name + " has " +
                                          what + " from another grantor; only that grantor or "
                                          "an administrator can revoke it");
              }
              done.push_back(grantee.name + " does not have " + what);
              continue;
            }
            for (const auto* existing : revocable) {
              if (c->with_grant_option) {  // REVOKE GRANT OPTION FOR: keep the privilege
                ObjectGrantRecord updated = *existing;
                updated.grant_option = false;
                ARROW_RETURN_NOT_OK(txn.UpdateGrant(updated));
                continue;
              }
              if (columns.empty()) {
                ARROW_RETURN_NOT_OK(txn.DeleteGrant(existing->id));
                continue;
              }
              // Revoke some columns: from a whole-table grant, keep every other column
              // (not a silent no-op as in Postgres).
              ObjectGrantRecord narrowed = *existing;
              std::vector<std::string> remaining;
              const auto& base = existing->columns.empty() ? table_columns : existing->columns;
              for (const auto& col : base) {
                if (std::none_of(columns.begin(), columns.end(),
                                 [&](const std::string& r) { return IdentifiersEqual(r, col); })) {
                  remaining.push_back(col);
                }
              }
              if (remaining.empty()) {
                ARROW_RETURN_NOT_OK(txn.DeleteGrant(existing->id));
              } else {
                narrowed.columns = remaining;
                ARROW_RETURN_NOT_OK(txn.UpdateGrant(narrowed));
              }
            }
            done.push_back((c->with_grant_option ? "Revoked the grant option for " : "Revoked ") +
                           what + " from " + grantee.name);
          }
        }
      }
      return arrow::Status::OK();
    }));
    return with_cascade_note(Join(done, "; "));
  }

  // ---- GRANT / REVOKE system privileges -----------------------------------
  if (const auto* c = std::get_if<SystemPrivilegeCommand>(&command)) {
    std::vector<std::string> done;
    ARROW_RETURN_NOT_OK(mutate_with(!c->grant && !c->cascade,
                                    [&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      for (SystemPrivilege p : c->privileges) {
        const std::string name = SystemPrivilegeName(p);
        if (c->grant && !snap.CanGrantSystemPrivilege(actor, p)) {
          return AccessControlError("GizmoSQL blocked GRANT: user " + actor.user_name +
                                    " may not grant " + name +
                                    " (only an administrator, or a holder of it WITH ADMIN OPTION)");
        }
        for (const auto& g : c->grantees) {
          ARROW_ASSIGN_OR_RAISE(auto grantee, ResolveGrantee(snap, g));
          if (grantee.kind != PrincipalKind::kRole) {
            return AccessControlError("privileges are granted to roles, not users. Grant them to a "
                                      "role, then GRANT the role TO USER " + grantee.name);
          }
          std::vector<SystemPrivilegeRecord> matching;
          const SystemPrivilegeRecord* own = nullptr;
          for (const auto& r : snap.system_privileges()) {
            if (r.role_id == grantee.id && r.privilege == p) {
              matching.push_back(r);
              if (r.granted_by == grantor) own = &r;
            }
          }
          if (c->grant) {
            SystemPrivilegeRecord record{grantee.id, p, c->with_admin_option, grantor};
            if (own && (own->admin_option || !c->with_admin_option)) {
              done.push_back(grantee.name + " already has " + name);
              continue;
            }
            ARROW_RETURN_NOT_OK(txn.UpsertSystemPrivilege(record));
            done.push_back("Granted " + name + " to " + grantee.name +
                           (c->with_admin_option ? " WITH ADMIN OPTION" : ""));
            continue;
          }
          std::vector<SystemPrivilegeRecord> revocable;
          for (const auto& r : matching) {
            if (actor.superuser || r.granted_by == grantor) revocable.push_back(r);
          }
          if (revocable.empty()) {
            if (!matching.empty()) {
              return AccessControlError("GizmoSQL blocked REVOKE: " + grantee.name + " has " + name +
                                        " from another grantor; only that grantor or an "
                                        "administrator can revoke it");
            }
            done.push_back(grantee.name + " does not have " + name);
            continue;
          }
          for (auto r : revocable) {
            if (c->with_admin_option) {
              r.admin_option = false;
              ARROW_RETURN_NOT_OK(txn.UpsertSystemPrivilege(r));
            } else {
              ARROW_RETURN_NOT_OK(txn.DeleteSystemPrivilege(r));
            }
          }
          done.push_back((c->with_admin_option ? "Revoked the admin option on " : "Revoked ") +
                         name + " from " + grantee.name);
        }
      }
      return arrow::Status::OK();
    }));
    return with_cascade_note(Join(done, "; "));
  }

  // ---- row policies --------------------------------------------------------
  if (std::holds_alternative<CreateRowPolicyCommand>(command) ||
      std::holds_alternative<DropRowPolicyCommand>(command) ||
      std::holds_alternative<CreateMaskingPolicyCommand>(command) ||
      std::holds_alternative<DropMaskingPolicyCommand>(command) ||
      std::holds_alternative<SetColumnMaskCommand>(command) ||
      std::holds_alternative<ShowPoliciesCommand>(command)) {
    if (!actor.superuser) {
      return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                                ": only administrators manage row and masking policies");
    }
  }
  if (const auto* c = std::get_if<CreateRowPolicyCommand>(&command)) {
    ARROW_ASSIGN_OR_RAISE(auto table, resolver.ResolveObject(c->table));
    if (table.type != ObjectType::kTable) {
      return AccessControlError("row policies go on tables; " + table.path.ToString() + " is a view");
    }
    ARROW_RETURN_NOT_OK(ValidatePolicySql(c->predicate));
    ARROW_RETURN_NOT_OK(resolver.CheckRowPredicate(table, c->predicate));
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      RowPolicyRecord record;
      for (const auto* p : snap.RowPoliciesOn(table.path)) {
        if (IdentifiersEqual(p->name, c->name)) {
          if (!c->or_replace) {
            return AccessControlError("row policy " + c->name + " already exists on " +
                                      table.path.ToString());
          }
          record.id = p->id;
        }
      }
      if (record.id.empty()) record.id = StoreTransaction::NewId();
      record.name = c->name;
      record.table = table.path;
      record.permissive = c->permissive;
      record.predicate = c->predicate;
      for (const auto& role_name : c->roles) {
        const auto* role = snap.FindRoleByName(role_name);
        if (!role) return AccessControlError("role " + role_name + " does not exist");
        record.role_ids.push_back(role->id);
      }
      message = "Row policy " + c->name + " on " + table.path.ToString() +
                (c->or_replace ? " created or replaced" : " created");
      return txn.UpsertRowPolicy(record);
    }));
    return Message(message);
  }
  if (const auto* c = std::get_if<DropRowPolicyCommand>(&command)) {
    ARROW_ASSIGN_OR_RAISE(auto table, resolver.ResolveObject(c->table));
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      for (const auto* p : snap.RowPoliciesOn(table.path)) {
        if (IdentifiersEqual(p->name, c->name)) {
          message = "Row policy " + p->name + " on " + table.path.ToString() + " dropped";
          return txn.DeleteRowPolicy(p->id);
        }
      }
      if (c->if_exists) {
        message = "Row policy " + c->name + " does not exist on " + table.path.ToString();
        return arrow::Status::OK();
      }
      return AccessControlError("row policy " + c->name + " does not exist on " +
                                table.path.ToString());
    }));
    return Message(message);
  }

  // ---- masking policies ----------------------------------------------------
  if (const auto* c = std::get_if<CreateMaskingPolicyCommand>(&command)) {
    ARROW_RETURN_NOT_OK(ValidatePolicySql(c->body, c->parameter));
    MaskingPolicyRecord candidate{"", c->name, c->parameter, c->parameter_type, c->return_type,
                                  c->body};
    ARROW_RETURN_NOT_OK(resolver.CheckMaskingPolicy(candidate));
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      MaskingPolicyRecord record = candidate;
      if (const auto* existing = snap.FindMaskingPolicyByName(c->name)) {
        if (!c->or_replace) return AccessControlError("masking policy " + c->name + " already exists");
        record.id = existing->id;
      } else {
        record.id = StoreTransaction::NewId();
      }
      message = "Masking policy " + c->name + (c->or_replace ? " created or replaced" : " created");
      return txn.UpsertMaskingPolicy(record);
    }));
    return Message(message);
  }
  if (const auto* c = std::get_if<DropMaskingPolicyCommand>(&command)) {
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* existing = snap.FindMaskingPolicyByName(c->name);
      if (!existing) {
        if (c->if_exists) {
          message = "Masking policy " + c->name + " does not exist";
          return arrow::Status::OK();
        }
        return AccessControlError("masking policy " + c->name + " does not exist");
      }
      for (const auto& b : snap.mask_bindings()) {
        if (b.policy_id == existing->id) {
          return AccessControlError("masking policy " + existing->name + " masks " +
                                    b.table.ToString() + "." + b.column +
                                    "; unset it there first (ALTER TABLE ... MODIFY COLUMN ... "
                                    "UNSET MASKING POLICY)");
        }
      }
      message = "Masking policy " + existing->name + " dropped";
      return txn.DeleteMaskingPolicy(existing->id);
    }));
    return Message(message);
  }
  if (const auto* c = std::get_if<SetColumnMaskCommand>(&command)) {
    ARROW_ASSIGN_OR_RAISE(auto table, resolver.ResolveObject(c->table));
    if (table.type != ObjectType::kTable) {
      return AccessControlError("masking policies go on table columns; " + table.path.ToString() +
                                " is a view");
    }
    auto column = std::find_if(table.columns.begin(), table.columns.end(),
                               [&](const std::string& t) { return IdentifiersEqual(t, c->column); });
    if (column == table.columns.end()) {
      return AccessControlError("column " + c->column + " does not exist in " + table.path.ToString());
    }
    const std::string column_name = *column;
    if (c->policy) {
      auto snap = snapshot();
      const auto* policy = snap->FindMaskingPolicyByName(*c->policy);
      if (!policy) return AccessControlError("masking policy " + *c->policy + " does not exist");
      ARROW_RETURN_NOT_OK(resolver.CheckMaskBinding(table, column_name, *policy));
    }
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      MaskBindingRecord binding;
      binding.table = table.path;
      binding.column = column_name;
      if (!c->policy) {
        message = "Masking policy removed from " + table.path.ToString() + "." + column_name;
        return txn.SetMaskBinding(binding, /*remove=*/true);
      }
      const auto* policy = snap.FindMaskingPolicyByName(*c->policy);
      if (!policy) return AccessControlError("masking policy " + *c->policy + " does not exist");
      binding.policy_id = policy->id;
      for (const auto& role_name : c->exempt_roles) {
        const auto* role = snap.FindRoleByName(role_name);
        if (!role) return AccessControlError("role " + role_name + " does not exist");
        binding.exempt_role_ids.push_back(role->id);
      }
      message = table.path.ToString() + "." + column_name + " is masked by " + policy->name;
      return txn.SetMaskBinding(binding, /*remove=*/false);
    }));
    return Message(message);
  }
  if (const auto* c = std::get_if<ShowPoliciesCommand>(&command)) {
    auto snap = snapshot();
    CommandResult result;
    auto role_names = [&](const std::vector<std::string>& ids) {
      std::vector<std::string> names;
      for (const auto& id : ids) {
        const auto* role = snap->FindRoleById(id);
        names.push_back(role ? role->name : id);
      }
      return Join(names);
    };
    if (c->masking) {
      result.column_names = {"policy", "parameter", "parameter_type", "return_type", "body",
                             "masks"};
      for (const auto& m : snap->masking_policies()) {
        std::vector<std::string> bound;
        for (const auto& b : snap->mask_bindings()) {
          if (b.policy_id == m.id) {
            bound.push_back(b.table.ToString() + "." + b.column +
                            (b.exempt_role_ids.empty() ? "" : " (exempt: " + role_names(b.exempt_role_ids) + ")"));
          }
        }
        result.rows.push_back({m.name, m.parameter, m.parameter_type, m.return_type, m.body,
                               Join(bound, "; ")});
      }
      return result;
    }
    std::optional<ObjectPath> only;
    if (c->table) {
      ARROW_ASSIGN_OR_RAISE(auto table, resolver.ResolveObject(*c->table));
      only = table.path;
    }
    result.column_names = {"policy", "table", "kind", "roles", "using"};
    for (const auto& p : snap->row_policies()) {
      if (only && ObjectKey(p.table) != ObjectKey(*only)) continue;
      result.rows.push_back({p.name, p.table.ToString(), p.permissive ? "PERMISSIVE" : "RESTRICTIVE",
                             p.role_ids.empty() ? "(all)" : role_names(p.role_ids), p.predicate});
    }
    return result;
  }

  // ---- ALTER TABLE | VIEW ... OWNER TO -------------------------------------
  if (const auto* c = std::get_if<AlterOwnerCommand>(&command)) {
    ARROW_ASSIGN_OR_RAISE(auto obj, resolver.ResolveObject(c->target));
    if ((obj.type == ObjectType::kView) != c->is_view) {
      return AccessControlError(obj.path.ToString() + " is a " +
                                (obj.type == ObjectType::kView ? "view" : "table") +
                                "; use ALTER " + (obj.type == ObjectType::kView ? "VIEW" : "TABLE"));
    }
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const OwnerRecord* current = snap.FindOwner(obj.path);
      OwnerRecord record;
      record.object = obj.path;
      record.type = obj.type;
      record.definition_hash = obj.definition_hash;
      const bool to_administrator =
          !options_.administrator_name.empty() && c->new_owner.kind != GranteeKind::kRole &&
          IdentifiersEqual(c->new_owner.name, options_.administrator_name);
      ResolvedGrantee owner{PrincipalKind::kUser, "", ""};
      if (to_administrator) {
        record.owner_kind = OwnerKind::kSystem;
      } else {
        ARROW_ASSIGN_OR_RAISE(owner, ResolveGrantee(snap, c->new_owner));
        record.owner_kind = owner.kind == PrincipalKind::kUser ? OwnerKind::kUser : OwnerKind::kRole;
        record.owner_id = owner.id;
      }
      if (!actor.superuser) {
        // An owner may hand the object to itself or to a role it holds; giving
        // it to anyone else could lend them, or take from them, the object's rights.
        const bool owns = current && snap.IsOwner(actor, *current);
        const bool to_self_or_role =
            !to_administrator &&
            ((owner.kind == PrincipalKind::kUser && owner.id == actor.user_id) ||
             (owner.kind == PrincipalKind::kRole && snap.RolesOf(actor).count(owner.id)));
        if (!owns || !to_self_or_role) {
          return AccessControlError(
              "GizmoSQL blocked ALTER OWNER: user " + actor.user_name + " may not change the "
              "owner of " + obj.path.ToString() + " (only its owner, to itself or a role it "
              "holds, or an administrator)");
        }
      }
      // Grants the previous owner made as owner stay in force: they become the
      // administrator's, rather than falling with the ownership.
      if (current && current->owner_kind != OwnerKind::kSystem) {
        for (const auto& g : snap.grants()) {
          if (g.granted_by.empty() || g.scope != GrantScope::kObject || !SamePath(g.target, obj.path)) {
            continue;
          }
          const UserRecord* by = snap.FindUserById(g.granted_by);
          if (by && snap.IsOwner(Principal{false, by->id, by->name, ""}, *current)) {
            ObjectGrantRecord kept = g;
            kept.granted_by.clear();
            ARROW_RETURN_NOT_OK(txn.UpdateGrant(kept));
          }
        }
      }
      message = obj.path.ToString() + " is now owned by " +
                (to_administrator ? std::string("the administrator") : owner.name);
      return txn.UpsertOwner(record);
    }));
    return with_cascade_note(message);
  }

  return AccessControlError("unsupported statement");
}

}  // namespace gizmosql::enterprise::rbac
