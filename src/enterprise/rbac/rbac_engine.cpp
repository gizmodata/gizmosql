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
#include <ctime>
#include <sstream>
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

// The tenant a user or role name belongs to: "tenant/name" names a tenant's
// principal, a plain name a system-scope one. "" for system scope.
arrow::Result<std::string> TenantForName(const Snapshot& snap, const std::string& name) {
  const auto slash = name.find('/');
  if (slash == std::string::npos) return std::string();
  const std::string tenant_name = name.substr(0, slash), rest = name.substr(slash + 1);
  if (tenant_name.empty() || rest.empty() || rest.find('/') != std::string::npos) {
    return AccessControlError("a tenant's users and roles are named tenant/name");
  }
  const auto* tenant = snap.FindTenantByName(tenant_name);
  if (!tenant) return AccessControlError("tenant " + tenant_name + " does not exist");
  return tenant->id;
}

// Does the principal hold the tenant's tenant_admin role?
bool IsTenantAdmin(const Snapshot& snap, const Principal& actor, const std::string& tenant_id) {
  if (tenant_id.empty() || actor.superuser) return false;
  const RoleRecord* admin = snap.TenantRole(tenant_id, kTenantAdminSuffix);
  return admin && snap.RolesOf(actor).count(admin->id) > 0;
}

std::string TenantName(const Snapshot& snap, const std::string& tenant_id) {
  if (tenant_id.empty()) return "the system scope";
  const auto* t = snap.FindTenantById(tenant_id);
  return "tenant " + (t ? t->name : tenant_id);
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

namespace {

// "8 hours", "30 minutes", "2 days", "45 seconds" -> seconds.
std::optional<int64_t> ParseDuration(const std::string& text) {
  std::istringstream in(text);
  int64_t n = 0;
  std::string unit, rest;
  if (!(in >> n) || n <= 0 || !(in >> unit) || (in >> rest)) return std::nullopt;
  for (auto& ch : unit) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
  if (!unit.empty() && unit.back() == 's') unit.pop_back();
  if (unit == "second") return n;
  if (unit == "minute") return n * 60;
  if (unit == "hour") return n * 3600;
  if (unit == "day") return n * 86400;
  if (unit == "week") return n * 7 * 86400;
  if (unit == "month") return n * 30 * 86400;
  if (unit == "year") return n * 365 * 86400;
  return std::nullopt;
}

std::string IsoTime(int64_t seconds) {
  std::time_t t = static_cast<std::time_t>(seconds);
  std::tm tm{};
#ifdef _WIN32
  gmtime_s(&tm, &t);
#else
  gmtime_r(&t, &tm);
#endif
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
  return buf;
}

int64_t NowSeconds() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

constexpr int64_t kMaxScopedTokenSeconds = 30 * 86400;

}  // namespace

arrow::Result<CommandResult> RbacEngine::ExecuteScopedToken(const ScopedTokenCommand& c,
                                                            const Principal& actor) {
  auto snap = snapshot();
  auto privilege_list = [](const std::vector<Privilege>& ps) {
    std::vector<std::string> names;
    for (auto p : ps) names.push_back(PrivilegeName(p));
    return Join(names);
  };
  if (c.kind == ScopedTokenCommand::Kind::kShow) {
    CommandResult result;
    result.column_names = {"name", "user", "allows", "catalogs", "expires_at", "minted_under"};
    for (const auto& t : snap->scoped_tokens()) {
      if (!actor.superuser && t.user_id != actor.user_id) continue;
      const auto* user = snap->FindUserById(t.user_id);
      const auto* parent = t.parent_id.empty() ? nullptr : snap->FindScopedToken(t.parent_id);
      result.rows.push_back({t.name, user ? user->name : t.user_id, privilege_list(t.privileges),
                             t.catalogs.empty() ? "(any)" : Join(t.catalogs), IsoTime(t.expires_at),
                             parent ? parent->name : ""});
    }
    return result;
  }
  // Whose token: your own, or (the administrator) the user named.
  const UserRecord* user = nullptr;
  if (actor.superuser) {
    if (!c.for_user) {
      return AccessControlError("the administrator names the user: " + CommandKind(c) + " " +
                                c.name + " FOR USER name ...");
    }
    user = snap->FindUserByName(*c.for_user);
    if (!user) return AccessControlError("user " + *c.for_user + " does not exist");
  } else {
    user = snap->FindUserById(actor.user_id);
    if (!user) return AccessControlError("user " + actor.user_name + " does not exist");
    if (c.for_user && !IdentifiersEqual(*c.for_user, user->name)) {
      return AccessControlError(actor.scope ? "a scoped token mints tokens only for the user it "
                                              "belongs to"
                                            : "users mint scoped tokens only for yourself; the "
                                              "administrator mints them for others");
    }
  }
  const ScopedTokenRecord* existing = nullptr;
  for (const auto& t : snap->scoped_tokens()) {
    if (t.user_id == user->id && IdentifiersEqual(t.name, c.name)) existing = &t;
  }

  if (c.kind == ScopedTokenCommand::Kind::kDrop) {
    if (!existing) {
      return AccessControlError("user " + user->name + " has no scoped token named " + c.name);
    }
    // The token and every token minted under it.
    std::set<std::string> doomed{existing->id};
    for (bool grew = true; grew;) {
      grew = false;
      for (const auto& t : snap->scoped_tokens()) {
        if (!t.parent_id.empty() && doomed.count(t.parent_id) && doomed.insert(t.id).second) grew = true;
      }
    }
    ARROW_RETURN_NOT_OK(store_->Mutate([&](StoreTransaction& txn, const Snapshot&) {
      for (const auto& id : doomed) ARROW_RETURN_NOT_OK(txn.DeleteScopedToken(id));
      return arrow::Status::OK();
    }));
    {
    ARROW_ASSIGN_OR_RAISE(auto fresh, store_->Load());
    Publish(std::move(fresh));
  }
    return Message("Scoped token " + existing->name + " of user " + user->name + " revoked" +
                   (doomed.size() > 1 ? " with " + std::to_string(doomed.size() - 1) +
                                            " token(s) minted under it"
                                      : ""));
  }

  // CREATE
  if (existing) {
    return AccessControlError("user " + user->name + " already has a scoped token named " + c.name);
  }
  int64_t seconds = 3600;
  if (c.valid_for) {
    auto parsed = ParseDuration(*c.valid_for);
    if (!parsed) {
      return AccessControlError("VALID FOR takes a duration such as '8 hours', '30 minutes' or "
                                "'7 days'");
    }
    seconds = *parsed;
  }
  if (seconds > kMaxScopedTokenSeconds) {
    return AccessControlError("a scoped token is valid for at most 30 days");
  }
  ScopedTokenRecord token;
  token.id = StoreTransaction::NewId();
  token.name = c.name;
  token.user_id = user->id;
  token.privileges = c.privileges;
  token.catalogs = c.catalogs;
  token.expires_at = NowSeconds() + seconds;
  token.created_by = actor.superuser ? "" : actor.user_id;
  if (actor.scope) {
    // Minted under a scoped token: never more than it.
    const auto& parent = *actor.scope;
    for (auto p : token.privileges) {
      if (!parent.privileges.count(p)) {
        return AccessControlError("a token minted under scoped token " + parent.name +
                                  " cannot allow " + PrivilegeName(p));
      }
    }
    if (!parent.catalogs.empty()) {
      if (token.catalogs.empty()) token.catalogs = parent.catalogs;
      for (const auto& cat : token.catalogs) {
        bool inside = false;
        for (const auto& pc : parent.catalogs) inside = inside || IdentifiersEqual(pc, cat);
        if (!inside) {
          return AccessControlError("a token minted under scoped token " + parent.name +
                                    " cannot reach catalog " + cat);
        }
      }
    }
    if (token.expires_at > parent.expires_at) {
      if (c.valid_for) {
        return AccessControlError("a token minted under scoped token " + parent.name +
                                  " cannot outlast it (it expires " + IsoTime(parent.expires_at) +
                                  ")");
      }
      token.expires_at = parent.expires_at;
    }
    token.parent_id = parent.id;
  }
  ARROW_RETURN_NOT_OK(store_->Mutate([&](StoreTransaction& txn, const Snapshot& now) {
    for (const auto& t : now.scoped_tokens()) {
      if (t.user_id == user->id && IdentifiersEqual(t.name, c.name)) {
        return AccessControlError("user " + user->name + " already has a scoped token named " +
                                  c.name);
      }
    }
    return txn.InsertScopedToken(token);
  }));
  {
    ARROW_ASSIGN_OR_RAISE(auto fresh, store_->Load());
    Publish(std::move(fresh));
  }
  CommandResult result;
  result.column_names = {"name", "user", "allows", "catalogs", "expires_at"};
  result.rows.push_back({token.name, user->name, privilege_list(token.privileges),
                         token.catalogs.empty() ? "(any)" : Join(token.catalogs),
                         IsoTime(token.expires_at)});
  result.minted = CommandResult::MintedToken{token.id, user->name, token.expires_at};
  return result;
}

arrow::Result<CommandResult> RbacEngine::Execute(const RbacCommand& command,
                                                 const Principal& actor,
                                                 ObjectResolver& resolver) {
  // A scoped token may read its grants, narrow itself and mint narrower
  // tokens; it never changes access control.
  if (actor.scope && !std::holds_alternative<ShowGrantsCommand>(command) &&
      !std::holds_alternative<SetRoleCommand>(command) &&
      !std::holds_alternative<ScopedTokenCommand>(command)) {
    return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                              ": a scoped token cannot change access control");
  }
  if (const auto* c = std::get_if<ScopedTokenCommand>(&command)) return ExecuteScopedToken(*c, actor);

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
      // The roles the session acts with (SET ROLE narrows them).
      for (const auto& id : snap->RolesOf(actor)) role_ids.push_back(id);
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
    // A tenant admin manages the users and roles of its own tenant.
    std::string name;
    if (const auto* c = std::get_if<CreateUserCommand>(&command)) name = c->name;
    if (const auto* c = std::get_if<AlterUserCommand>(&command)) name = c->name;
    if (const auto* c = std::get_if<DropUserCommand>(&command)) name = c->name;
    if (const auto* c = std::get_if<CreateRoleCommand>(&command)) name = c->name;
    if (const auto* c = std::get_if<DropRoleCommand>(&command)) name = c->name;
    auto snap = snapshot();
    auto tenant = TenantForName(*snap, name);
    if (!tenant.ok() || !IsTenantAdmin(*snap, actor, *tenant)) {
      return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                                ": only administrators manage users and roles on this server");
    }
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
      ARROW_ASSIGN_OR_RAISE(auto tenant, TenantForName(snap, c->name));
      message = "User " + c->name + " created";
      UserRecord user{StoreTransaction::NewId(), c->name, hash, true, {}};
      ARROW_RETURN_NOT_OK(txn.InsertUser(user));
      return txn.SetPrincipalTenant(PrincipalKind::kUser, user.id, tenant);
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
      ARROW_ASSIGN_OR_RAISE(auto tenant, TenantForName(snap, c->name));
      message = "Role " + c->name + " created";
      RoleRecord role{StoreTransaction::NewId(), c->name, false};
      ARROW_RETURN_NOT_OK(txn.InsertRole(role));
      return txn.SetPrincipalTenant(PrincipalKind::kRole, role.id, tenant);
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
        const std::string role_tenant = snap.TenantOf(PrincipalKind::kRole, role->id);
        if (!snap.CanAdminRole(actor, role->id) && !IsTenantAdmin(snap, actor, role_tenant)) {
          return AccessControlError("GizmoSQL blocked " + CommandKind(command) + ": user " +
                                    actor.user_name + " may not grant or revoke role " +
                                    role->name + " (only an administrator, or a holder of it "
                                    "WITH ADMIN OPTION)");
        }
        for (const auto& g : c->grantees) {
          ARROW_ASSIGN_OR_RAISE(auto grantee, ResolveGrantee(snap, g));
          if (snap.TenantOf(grantee.kind, grantee.id) != role_tenant) {
            return AccessControlError("role " + role->name + " belongs to " +
                                      TenantName(snap, role_tenant) + " and " + grantee.name +
                                      " does not: roles never cross tenants");
          }
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
            if (!actor.superuser && existing->granted_by != grantor &&
                !IsTenantAdmin(snap, actor, role_tenant)) {
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
        const std::string role_tenant = snap.TenantOf(PrincipalKind::kRole, grantee.id);
        if (!role_tenant.empty() && snap.TenantOfCatalog(target.catalog) != role_tenant) {
          return AccessControlError("role " + grantee.name + " belongs to " +
                                    TenantName(snap, role_tenant) + "; it can hold privileges "
                                    "only on that tenant's catalogs");
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
          if (c->grant && !snap.TenantOf(PrincipalKind::kRole, grantee.id).empty()) {
            return AccessControlError("system privileges reach the whole server; a tenant's role "
                                      "(" + grantee.name + ") cannot hold them");
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

  // ---- tenants ---------------------------------------------------------------
  if (std::holds_alternative<TenantCommand>(command) ||
      std::holds_alternative<CatalogTenantCommand>(command) ||
      std::holds_alternative<TenantDefaultCatalogCommand>(command)) {
    if (!actor.superuser) {
      return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                                ": only administrators manage tenants");
    }
  }
  if (const auto* c = std::get_if<TenantCommand>(&command)) {
    CommandResult result;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* existing = snap.FindTenantByName(c->name);
      if (c->create) {
        if (existing) {
          if (c->if_exists) {
            message = "Tenant " + c->name + " already exists";
            return arrow::Status::OK();
          }
          return AccessControlError("tenant " + c->name + " already exists");
        }
        if (c->name.find('/') != std::string::npos) return AccessControlError("a tenant name cannot contain /");
        TenantRecord tenant{StoreTransaction::NewId(), c->name, ""};
        ARROW_RETURN_NOT_OK(txn.UpsertTenant(tenant));
        for (const char* suffix : {kTenantPublicSuffix, kTenantAdminSuffix}) {
          ARROW_RETURN_NOT_OK(CheckNameFree(snap, c->name + suffix));
          RoleRecord role{StoreTransaction::NewId(), c->name + suffix, true};
          ARROW_RETURN_NOT_OK(txn.InsertRole(role));
          ARROW_RETURN_NOT_OK(txn.SetPrincipalTenant(PrincipalKind::kRole, role.id, tenant.id));
        }
        message = "Tenant " + c->name + " created, with roles " + c->name + kTenantPublicSuffix +
                  " and " + c->name + kTenantAdminSuffix;
        return arrow::Status::OK();
      }
      if (!existing) {
        if (c->if_exists) {
          message = "Tenant " + c->name + " does not exist";
          return arrow::Status::OK();
        }
        return AccessControlError("tenant " + c->name + " does not exist");
      }
      std::vector<std::string> members;
      for (const auto& u : snap.users()) {
        if (snap.TenantOf(PrincipalKind::kUser, u.id) == existing->id) members.push_back(u.name);
      }
      if (!members.empty()) {
        std::sort(members.begin(), members.end());
        return AccessControlError("tenant " + existing->name + " still has users (" +
                                  Join(members) + "); drop them first");
      }
      if (!snap.TenantCatalogs(existing->id).empty()) {
        return AccessControlError("tenant " + existing->name +
                                  " still has catalogs; ALTER CATALOG ... UNSET TENANT first");
      }
      for (const auto& r : snap.roles()) {
        if (snap.TenantOf(PrincipalKind::kRole, r.id) == existing->id) {
          ARROW_RETURN_NOT_OK(txn.DeleteRole(r.id));
        }
      }
      message = "Tenant " + existing->name + " dropped";
      return txn.DeleteTenant(existing->id);
    }));
    return Message(message);
  }
  if (const auto* c = std::get_if<CatalogTenantCommand>(&command)) {
    ARROW_ASSIGN_OR_RAISE(auto catalog, resolver.ResolveCatalog(c->catalog));
    CommandResult result;
    std::vector<std::string> affected_users;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const std::string current = snap.TenantOfCatalog(catalog);
      auto users_of = [&](const std::string& tenant_id) {
        for (const auto& u : snap.users()) {
          if (!tenant_id.empty() && snap.TenantOf(PrincipalKind::kUser, u.id) == tenant_id) {
            affected_users.push_back(u.id);
          }
        }
      };
      if (!c->tenant) {
        if (current.empty()) {
          message = "Catalog " + catalog + " belongs to no tenant";
          return arrow::Status::OK();
        }
        // The tenant's roles lose what they held there.
        for (const auto& g : snap.grants()) {
          if (IdentifiersEqual(g.target.catalog, catalog) &&
              snap.TenantOf(PrincipalKind::kRole, g.role_id) == current) {
            ARROW_RETURN_NOT_OK(txn.DeleteGrant(g.id));
          }
        }
        users_of(current);
        message = "Catalog " + catalog + " no longer belongs to " + TenantName(snap, current);
        return txn.SetCatalogTenant(catalog, "");
      }
      const auto* tenant = snap.FindTenantByName(*c->tenant);
      if (!tenant) return AccessControlError("tenant " + *c->tenant + " does not exist");
      if (!current.empty() && current != tenant->id) {
        return AccessControlError("catalog " + catalog + " belongs to " + TenantName(snap, current) +
                                  "; UNSET TENANT first");
      }
      ARROW_RETURN_NOT_OK(txn.SetCatalogTenant(catalog, tenant->id));
      // The tenant admin may do everything there, and grant it on.
      const RoleRecord* admin = snap.TenantRole(tenant->id, kTenantAdminSuffix);
      if (!admin) return AccessControlError("tenant " + tenant->name + " has no tenant_admin role");
      for (Privilege p : {Privilege::kSelect, Privilege::kInsert, Privilege::kUpdate, Privilege::kDelete,
                          Privilege::kCreateTable, Privilege::kCreateView, Privilege::kCreateSchema,
                          Privilege::kDropTable, Privilege::kDropView, Privilege::kDropSchema,
                          Privilege::kAlterTable, Privilege::kAlterView}) {
        bool have = false;
        for (const auto& g : snap.grants()) {
          have = have || (g.role_id == admin->id && g.scope == GrantScope::kCatalog &&
                          g.privilege == p && IdentifiersEqual(g.target.catalog, catalog) &&
                          g.granted_by.empty());
        }
        if (have) continue;
        ObjectGrantRecord grant;
        grant.id = StoreTransaction::NewId();
        grant.role_id = admin->id;
        grant.scope = GrantScope::kCatalog;
        grant.target.catalog = catalog;
        grant.privilege = p;
        grant.grant_option = true;
        ARROW_RETURN_NOT_OK(txn.InsertGrant(grant));
      }
      users_of(tenant->id);
      message = "Catalog " + catalog + " now belongs to tenant " + tenant->name;
      return arrow::Status::OK();
    }));
    result = Message(message);
    // What the tenant's users may see changed: their sessions start over.
    result.terminate_user_ids = affected_users;
    return result;
  }
  if (const auto* c = std::get_if<TenantDefaultCatalogCommand>(&command)) {
    std::string catalog;
    if (!c->catalog.empty()) {
      ARROW_ASSIGN_OR_RAISE(catalog, resolver.ResolveCatalog(c->catalog));
    }
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& snap) -> arrow::Status {
      const auto* tenant = snap.FindTenantByName(c->tenant);
      if (!tenant) return AccessControlError("tenant " + c->tenant + " does not exist");
      if (!catalog.empty() && snap.TenantOfCatalog(catalog) != tenant->id) {
        return AccessControlError("catalog " + catalog + " does not belong to tenant " + tenant->name);
      }
      TenantRecord updated = *tenant;
      updated.default_catalog = catalog;
      message = catalog.empty() ? "Tenant " + tenant->name + " has no default catalog"
                                : "Tenant " + tenant->name + "'s default catalog is " + catalog;
      return txn.UpsertTenant(updated);
    }));
    return Message(message);
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
  if (const auto* c = std::get_if<SetRoleCommand>(&command)) {
    if (actor.superuser) {
      return AccessControlError("GizmoSQL blocked SET ROLE: the administrator holds every right "
                                "already; SET ROLE is for store users");
    }
    auto snap = snapshot();
    CommandResult result;
    result.column_names = {"result"};
    if (!c->role) {
      result.active_role_id = "";
      result.rows.push_back({"All of " + actor.user_name + "'s roles are active"});
      return result;
    }
    // An unqualified name means the user's own tenant's role first.
    const RoleRecord* role = nullptr;
    const std::string tenant = snap->TenantOfPrincipal(actor);
    if (!tenant.empty() && c->role->find('/') == std::string::npos) {
      if (const auto* t = snap->FindTenantById(tenant)) role = snap->FindRoleByName(t->name + "/" + *c->role);
    }
    if (!role) role = snap->FindRoleByName(*c->role);
    if (!role || !snap->HeldRoleIds(actor).count(role->id)) {
      // Not telling a role that exists from one that does not.
      return AccessControlError("GizmoSQL blocked SET ROLE: user " + actor.user_name +
                                " does not hold role " + *c->role);
    }
    result.active_role_id = role->id;
    result.rows.push_back({"Active role: " + role->name});
    return result;
  }
  if (const auto* c = std::get_if<GroupMappingCommand>(&command)) {
    if (!actor.superuser) {
      return AccessControlError("GizmoSQL blocked " + CommandKind(command) +
                                ": only administrators manage group mappings");
    }
    auto snap = snapshot();
    auto role_name = [&](const std::string& id) {
      const auto* r = snap->FindRoleById(id);
      return r ? r->name : id;
    };
    if (c->kind == GroupMappingCommand::Kind::kShow) {
      CommandResult result;
      result.column_names = {"group", "issuer", "role"};
      for (const auto& m : snap->group_mappings()) {
        result.rows.push_back({m.group, m.issuer.empty() ? "(any)" : m.issuer, role_name(m.role_id)});
      }
      return result;
    }
    const RoleRecord* role = nullptr;
    if (c->role) {
      role = snap->FindRoleByName(*c->role);
      if (!role) return AccessControlError("role " + *c->role + " does not exist");
      if (role->id == kPublicRoleId) {
        return AccessControlError("everyone holds PUBLIC already; map the group to another role");
      }
    }
    std::string mapping_message;
    ARROW_RETURN_NOT_OK(mutate([&](StoreTransaction& txn, const Snapshot& now) -> arrow::Status {
      if (c->kind == GroupMappingCommand::Kind::kCreate) {
        for (const auto& m : now.group_mappings()) {
          if (m.group == c->group && m.issuer == c->issuer && m.role_id == role->id) {
            return AccessControlError("group '" + c->group + "' is already mapped to role " +
                                      role->name);
          }
        }
        mapping_message = "Users in group '" + c->group + "' hold role " + role->name;
        return txn.InsertGroupMapping({StoreTransaction::NewId(), c->issuer, c->group, role->id});
      }
      std::vector<std::string> ids;
      for (const auto& m : now.group_mappings()) {
        if (m.group == c->group && m.issuer == c->issuer && (!role || m.role_id == role->id)) {
          ids.push_back(m.id);
        }
      }
      if (ids.empty()) return AccessControlError("group '" + c->group + "' has no such mapping");
      for (const auto& id : ids) ARROW_RETURN_NOT_OK(txn.DeleteGroupMapping(id));
      mapping_message = "Removed " + std::to_string(ids.size()) + " mapping" +
                        (ids.size() == 1 ? "" : "s") + " of group '" + c->group + "'";
      return arrow::Status::OK();
    }));
    return Message(mapping_message);
  }
  if (std::holds_alternative<ShowTenantsCommand>(command)) {
    // The administrator sees every tenant; a tenant's users see their own.
    auto snap = snapshot();
    const std::string own = actor.superuser ? "" : snap->TenantOfPrincipal(actor);
    CommandResult result;
    result.column_names = {"tenant", "default_catalog", "catalogs", "users", "roles"};
    for (const auto& t : snap->tenants()) {
      if (!actor.superuser && t.id != own) continue;
      const auto tenant_catalogs = snap->TenantCatalogs(t.id);
      const std::vector<std::string> catalogs(tenant_catalogs.begin(), tenant_catalogs.end());
      int64_t users = 0, roles = 0;
      for (const auto& u : snap->users()) users += snap->TenantOf(PrincipalKind::kUser, u.id) == t.id;
      for (const auto& r : snap->roles()) roles += snap->TenantOf(PrincipalKind::kRole, r.id) == t.id;
      result.rows.push_back({t.name, t.default_catalog, Join(catalogs), std::to_string(users),
                             std::to_string(roles)});
    }
    return result;
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
        const std::string owner_tenant = snap.TenantOf(owner.kind, owner.id);
        if (!owner_tenant.empty() && snap.TenantOfCatalog(obj.path.catalog) != owner_tenant) {
          return AccessControlError(owner.name + " belongs to " + TenantName(snap, owner_tenant) +
                                    "; it can own objects only in that tenant's catalogs");
        }
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
