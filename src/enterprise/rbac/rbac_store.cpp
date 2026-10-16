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

#include "rbac_store.h"

#include <chrono>
#include <filesystem>
#include <regex>
#include <thread>

#include <duckdb.hpp>
#include <nlohmann/json.hpp>
#include <openssl/rand.h>

#include "duckdb_compat.h"

namespace gizmosql::enterprise::rbac {

namespace {

namespace dd = duckdb;
namespace compat = gizmosql::ddb::compat;
using json = nlohmann::json;

constexpr const char* kSchema = "rbac_store.gizmosql_rbac";

std::string T(const char* table) { return std::string(kSchema) + "." + table; }

arrow::Status StoreError(const std::string& what, const std::string& detail) {
  return arrow::Status::IOError("GizmoSQL access-control store: " + what + ": " + detail);
}

arrow::Status Exec(dd::Connection& conn, const std::string& sql) {
  auto result = conn.Query(sql);
  if (result->HasError()) return StoreError("statement failed", result->GetError());
  return arrow::Status::OK();
}

arrow::Result<dd::unique_ptr<dd::QueryResult>> Run(dd::Connection& conn, const std::string& sql,
                                                   dd::vector<dd::Value> params = {}) {
  auto stmt = conn.Prepare(sql);
  if (stmt->HasError()) return StoreError("prepare failed", stmt->GetError());
  auto result = compat::ExecuteMaterialized(*stmt, params);
  if (result->HasError()) return StoreError("statement failed", result->GetError());
  return result;
}

dd::Value Text(const std::string& s) { return dd::Value(s); }
dd::Value OptText(const std::optional<std::string>& s) {
  return s ? dd::Value(*s) : dd::Value(dd::LogicalType::VARCHAR);
}
dd::Value Bool(bool b) { return dd::Value::BOOLEAN(b); }

std::string ToJson(const std::vector<std::string>& values) { return json(values).dump(); }
std::vector<std::string> FromJson(const std::string& text) {
  if (text.empty()) return {};
  auto parsed = json::parse(text, nullptr, /*allow_exceptions=*/false);
  if (!parsed.is_array()) return {};
  std::vector<std::string> out;
  for (const auto& v : parsed) {
    if (v.is_string()) out.push_back(v.get<std::string>());
  }
  return out;
}

std::string GetString(dd::QueryResult& result, dd::idx_t col, dd::idx_t row) {
  auto v = compat::ResultValue(result, col, row);
  return v.IsNull() ? std::string() : v.ToString();
}
std::optional<std::string> GetOptString(dd::QueryResult& result, dd::idx_t col, dd::idx_t row) {
  auto v = compat::ResultValue(result, col, row);
  if (v.IsNull()) return std::nullopt;
  return v.ToString();
}
bool GetBool(dd::QueryResult& result, dd::idx_t col, dd::idx_t row) {
  auto v = compat::ResultValue(result, col, row);
  return !v.IsNull() && v.GetValue<bool>();
}

std::string ScopeName(GrantScope scope) {
  switch (scope) {
    case GrantScope::kObject:
      return "object";
    case GrantScope::kSchema:
      return "schema";
    case GrantScope::kCatalog:
      return "catalog";
  }
  return "object";
}
GrantScope ScopeFrom(const std::string& s) {
  if (s == "schema") return GrantScope::kSchema;
  if (s == "catalog") return GrantScope::kCatalog;
  return GrantScope::kObject;
}

std::optional<Privilege> PrivilegeFrom(const std::string& name) {
  for (auto p : {Privilege::kSelect, Privilege::kInsert, Privilege::kUpdate, Privilege::kDelete,
                 Privilege::kCreateTable, Privilege::kCreateView, Privilege::kCreateSchema,
                 Privilege::kDropTable, Privilege::kDropView, Privilege::kDropSchema,
                 Privilege::kAlterTable, Privilege::kAlterView, Privilege::kAll}) {
    if (PrivilegeName(p) == name) return p;
  }
  return std::nullopt;
}

std::string ObjectTypeName(ObjectType t) {
  switch (t) {
    case ObjectType::kTable:
      return "table";
    case ObjectType::kView:
      return "view";
    case ObjectType::kSchema:
      return "schema";
  }
  return "table";
}
ObjectType ObjectTypeFrom(const std::string& s) {
  if (s == "view") return ObjectType::kView;
  if (s == "schema") return ObjectType::kSchema;
  return ObjectType::kTable;
}

std::string OwnerKindName(OwnerKind k) {
  switch (k) {
    case OwnerKind::kSystem:
      return "system";
    case OwnerKind::kUser:
      return "user";
    case OwnerKind::kRole:
      return "role";
  }
  return "system";
}
OwnerKind OwnerKindFrom(const std::string& s) {
  if (s == "user") return OwnerKind::kUser;
  if (s == "role") return OwnerKind::kRole;
  return OwnerKind::kSystem;
}

bool IsRetryableConflict(const std::string& message) {
  return message.find("could not serialize") != std::string::npos ||
         message.find("onflict") != std::string::npos ||  // DuckDB write-write conflicts
         message.find("deadlock detected") != std::string::npos;
}

// DuckDB's connection errors quote the whole connection string, password
// included. Keep the useful part (libpq's reason) and none of the credentials:
// the string itself, any user:password@ authority and any password=... pair.
std::string RedactConnectionString(std::string message, const std::string& uri) {
  if (auto nl = message.find('\n'); nl != std::string::npos) message.resize(nl);
  for (size_t at; !uri.empty() && (at = message.find(uri)) != std::string::npos;) {
    message.replace(at, uri.size(), "<connection string>");
  }
  message = std::regex_replace(message, std::regex(R"((://[^:/@\s"']*:)[^@\s"']*@)"), "$1***@");
  message = std::regex_replace(message,
                               std::regex(R"((password\s*=\s*)('[^']*'|"[^"]*"|[^\s"']+))",
                                          std::regex::icase),
                               "$1***");
  return message;
}

bool LooksLikePostgres(const std::string& uri) {
  return uri.rfind("postgresql://", 0) == 0 || uri.rfind("postgres://", 0) == 0 ||
         uri.find("dbname=") != std::string::npos || uri.find("host=") != std::string::npos;
}

}  // namespace

// ---------------------------------------------------------------------------

arrow::Result<std::unique_ptr<RbacStore>> RbacStore::Open(const std::string& uri) {
  std::unique_ptr<RbacStore> store(new RbacStore());
  dd::DBConfig config;
  // The private instance must not pick up secrets from the host's DuckDB
  // profile (a default PostgreSQL secret there would change where the store
  // connects), so it gets an empty secret directory of its own.
  std::error_code ec;
  auto secret_dir = std::filesystem::temp_directory_path(ec) /
                    ("gizmosql-rbac-store-" + StoreTransaction::NewId());
  if (ec || !std::filesystem::create_directory(secret_dir, ec) || ec) {
    return StoreError("could not start", "no private secret directory");
  }
  std::filesystem::permissions(secret_dir, std::filesystem::perms::owner_all,
                               std::filesystem::perm_options::replace, ec);
  store->secret_dir_ = secret_dir.string();
  config.SetOptionByName("secret_directory", dd::Value(store->secret_dir_));
  try {
    store->db_ = std::make_unique<dd::DuckDB>(nullptr, &config);
    store->conn_ = std::make_unique<dd::Connection>(*store->db_);
  } catch (const std::exception& e) {
    return StoreError("could not start", e.what());
  }
  auto& conn = *store->conn_;

  if (uri == "memory") {
    store->description_ = "in-memory (tests only)";
    ARROW_RETURN_NOT_OK(Exec(conn, "ATTACH ':memory:' AS rbac_store"));
  } else if (LooksLikePostgres(uri)) {
    store->description_ = "PostgreSQL";
    auto load = conn.Query("LOAD postgres");
    if (load->HasError()) {
      ARROW_RETURN_NOT_OK(Exec(conn, "INSTALL postgres"));
      ARROW_RETURN_NOT_OK(Exec(conn, "LOAD postgres"));
    }
    // The connection string carries credentials: bind it, never log it.
    std::string quoted = "'";
    for (char c : uri) {
      if (c == '\'') quoted += '\'';
      quoted += c;
    }
    quoted += "'";
    auto attach = conn.Query("ATTACH " + quoted + " AS rbac_store (TYPE postgres)");
    if (attach->HasError()) {
      return StoreError("could not connect to the PostgreSQL store",
                        RedactConnectionString(attach->GetError(), uri));
    }
  } else {
    return arrow::Status::Invalid(
        "GizmoSQL access control: the store must be a PostgreSQL connection string "
        "(postgresql://user@host/db or 'host=... dbname=...')");
  }

  // Two replicas starting at once can race on CREATE ... IF NOT EXISTS.
  arrow::Status init;
  for (int attempt = 0; attempt < 5; ++attempt) {
    init = store->InitializeSchema();
    if (init.ok()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100 * (attempt + 1)));
  }
  ARROW_RETURN_NOT_OK(init);
  return store;
}

RbacStore::~RbacStore() {
  conn_.reset();
  db_.reset();
  if (!secret_dir_.empty()) {
    std::error_code ec;
    std::filesystem::remove_all(secret_dir_, ec);
  }
}

arrow::Status RbacStore::InitializeSchema() {
  auto& conn = *conn_;
  ARROW_RETURN_NOT_OK(Exec(conn, std::string("CREATE SCHEMA IF NOT EXISTS ") + kSchema));
  const std::string ddl[] = {
      "CREATE TABLE IF NOT EXISTS " + T("meta") +
          " (key VARCHAR PRIMARY KEY, value VARCHAR NOT NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("users") +
          " (user_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, name_key VARCHAR NOT NULL "
          "UNIQUE, password_hash VARCHAR, enabled BOOLEAN NOT NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("roles") +
          " (role_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, name_key VARCHAR NOT NULL "
          "UNIQUE, builtin BOOLEAN NOT NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("memberships") +
          " (member_kind VARCHAR NOT NULL, member_id VARCHAR NOT NULL, role_id VARCHAR NOT "
          "NULL, admin_option BOOLEAN NOT NULL, granted_by VARCHAR, PRIMARY KEY (member_kind, "
          "member_id, role_id))",
      "CREATE TABLE IF NOT EXISTS " + T("object_grants") +
          " (grant_id VARCHAR PRIMARY KEY, role_id VARCHAR NOT NULL, scope VARCHAR NOT NULL, "
          "catalog_name VARCHAR NOT NULL, schema_path VARCHAR NOT NULL, object_name VARCHAR "
          "NOT NULL, privilege VARCHAR NOT NULL, columns VARCHAR NOT NULL, grant_option "
          "BOOLEAN NOT NULL, granted_by VARCHAR)",
      "CREATE TABLE IF NOT EXISTS " + T("owners") +
          " (object_key VARCHAR PRIMARY KEY, catalog_name VARCHAR NOT NULL, schema_path "
          "VARCHAR NOT NULL, object_name VARCHAR NOT NULL, object_type VARCHAR NOT NULL, "
          "owner_kind VARCHAR NOT NULL, owner_id VARCHAR NOT NULL)",
  };
  for (const auto& sql : ddl) ARROW_RETURN_NOT_OK(Exec(conn, sql));
  ARROW_RETURN_NOT_OK(Run(conn,
                          "INSERT INTO " + T("meta") +
                              " SELECT 'data_version', '1' WHERE NOT EXISTS (SELECT 1 FROM " +
                              T("meta") + " WHERE key = 'data_version')")
                          .status());
  ARROW_RETURN_NOT_OK(Run(conn,
                          "INSERT INTO " + T("meta") +
                              " SELECT 'schema_version', '1' WHERE NOT EXISTS (SELECT 1 FROM " +
                              T("meta") + " WHERE key = 'schema_version')")
                          .status());
  ARROW_RETURN_NOT_OK(Run(conn,
                          "INSERT INTO " + T("roles") +
                              " SELECT $1, $2, $3, true WHERE NOT EXISTS (SELECT 1 FROM " +
                              T("roles") + " WHERE role_id = $1)",
                          {Text(kPublicRoleId), Text(kPublicRoleName),
                           Text(FoldIdentifier(kPublicRoleName))})
                          .status());
  return arrow::Status::OK();
}

arrow::Result<int64_t> RbacStore::CurrentVersion() {
  std::lock_guard<std::mutex> lock(mutex_);
  ARROW_ASSIGN_OR_RAISE(auto result,
                        Run(*conn_, "SELECT value FROM " + T("meta") + " WHERE key = 'data_version'"));
  if (compat::ResultRowCount(*result) != 1) return StoreError("corrupt store", "no data version");
  return std::stoll(GetString(*result, 0, 0));
}

arrow::Result<std::shared_ptr<const Snapshot>> RbacStore::Load() {
  std::lock_guard<std::mutex> lock(mutex_);
  return LoadLocked();
}

arrow::Result<std::shared_ptr<const Snapshot>> RbacStore::LoadLocked() {
  auto& conn = *conn_;
  ARROW_ASSIGN_OR_RAISE(auto version_result,
                        Run(conn, "SELECT value FROM " + T("meta") + " WHERE key = 'data_version'"));
  if (compat::ResultRowCount(*version_result) != 1) {
    return StoreError("corrupt store", "no data version");
  }
  const int64_t version = std::stoll(GetString(*version_result, 0, 0));

  std::vector<UserRecord> users;
  ARROW_ASSIGN_OR_RAISE(auto u, Run(conn, "SELECT user_id, name, password_hash, enabled FROM " +
                                              T("users") + " ORDER BY name_key"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*u); ++r) {
    users.push_back({GetString(*u, 0, r), GetString(*u, 1, r), GetOptString(*u, 2, r),
                     GetBool(*u, 3, r)});
  }

  std::vector<RoleRecord> roles;
  ARROW_ASSIGN_OR_RAISE(auto ro, Run(conn, "SELECT role_id, name, builtin FROM " + T("roles") +
                                               " ORDER BY name_key"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*ro); ++r) {
    roles.push_back({GetString(*ro, 0, r), GetString(*ro, 1, r), GetBool(*ro, 2, r)});
  }

  std::vector<MembershipRecord> memberships;
  ARROW_ASSIGN_OR_RAISE(auto m, Run(conn, "SELECT member_kind, member_id, role_id, admin_option, "
                                          "granted_by FROM " + T("memberships")));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*m); ++r) {
    memberships.push_back({GetString(*m, 0, r) == "role" ? PrincipalKind::kRole
                                                          : PrincipalKind::kUser,
                           GetString(*m, 1, r), GetString(*m, 2, r), GetBool(*m, 3, r),
                           GetString(*m, 4, r)});
  }

  std::vector<ObjectGrantRecord> grants;
  ARROW_ASSIGN_OR_RAISE(
      auto g, Run(conn, "SELECT grant_id, role_id, scope, catalog_name, schema_path, object_name, "
                        "privilege, columns, grant_option, granted_by FROM " +
                            T("object_grants") + " ORDER BY grant_id"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*g); ++r) {
    auto privilege = PrivilegeFrom(GetString(*g, 6, r));
    if (!privilege) continue;  // written by a newer version; never widens access
    ObjectGrantRecord grant;
    grant.id = GetString(*g, 0, r);
    grant.role_id = GetString(*g, 1, r);
    grant.scope = ScopeFrom(GetString(*g, 2, r));
    grant.target.catalog = GetString(*g, 3, r);
    grant.target.schema_path = FromJson(GetString(*g, 4, r));
    grant.target.object_name = GetString(*g, 5, r);
    grant.privilege = *privilege;
    grant.columns = FromJson(GetString(*g, 7, r));
    grant.grant_option = GetBool(*g, 8, r);
    grant.granted_by = GetString(*g, 9, r);
    grants.push_back(std::move(grant));
  }

  std::vector<OwnerRecord> owners;
  ARROW_ASSIGN_OR_RAISE(
      auto o, Run(conn, "SELECT catalog_name, schema_path, object_name, object_type, owner_kind, "
                        "owner_id FROM " + T("owners")));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*o); ++r) {
    OwnerRecord owner;
    owner.object.catalog = GetString(*o, 0, r);
    owner.object.schema_path = FromJson(GetString(*o, 1, r));
    owner.object.object_name = GetString(*o, 2, r);
    owner.type = ObjectTypeFrom(GetString(*o, 3, r));
    owner.owner_kind = OwnerKindFrom(GetString(*o, 4, r));
    owner.owner_id = GetString(*o, 5, r);
    owners.push_back(std::move(owner));
  }

  return std::make_shared<const Snapshot>(version, std::move(users), std::move(roles),
                                          std::move(memberships), std::move(grants),
                                          std::move(owners));
}

arrow::Status RbacStore::Mutate(
    const std::function<arrow::Status(StoreTransaction&, const Snapshot&)>& fn) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& conn = *conn_;
  arrow::Status last;
  for (int attempt = 0; attempt < 5; ++attempt) {
    ARROW_RETURN_NOT_OK(Exec(conn, "BEGIN TRANSACTION"));
    auto body = [&]() -> arrow::Status {
      // Bump the version first: the row lock serializes writers on every replica.
      ARROW_RETURN_NOT_OK(Run(conn, "UPDATE " + T("meta") +
                                        " SET value = CAST(CAST(value AS BIGINT) + 1 AS VARCHAR) "
                                        "WHERE key = 'data_version'")
                              .status());
      ARROW_ASSIGN_OR_RAISE(auto snapshot, LoadLocked());
      StoreTransaction txn(conn);
      ARROW_RETURN_NOT_OK(fn(txn, *snapshot));
      return Exec(conn, "COMMIT");
    };
    last = body();
    if (last.ok()) return last;
    (void)conn.Query("ROLLBACK");
    if (!IsRetryableConflict(last.message())) return last;
    std::this_thread::sleep_for(std::chrono::milliseconds(20 * (attempt + 1)));
  }
  return last;
}

// ---------------------------------------------------------------------------

std::string StoreTransaction::NewId() {
  unsigned char bytes[16];
  if (RAND_bytes(bytes, sizeof(bytes)) != 1) {
    // Fall back to a time-seeded id; uniqueness is still enforced by the store.
    auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    for (size_t i = 0; i < sizeof(bytes); ++i) bytes[i] = static_cast<unsigned char>(now >> (i % 8));
  }
  bytes[6] = static_cast<unsigned char>((bytes[6] & 0x0f) | 0x40);
  bytes[8] = static_cast<unsigned char>((bytes[8] & 0x3f) | 0x80);
  static const char* kHex = "0123456789abcdef";
  std::string id;
  for (size_t i = 0; i < sizeof(bytes); ++i) {
    if (i == 4 || i == 6 || i == 8 || i == 10) id += '-';
    id += kHex[bytes[i] >> 4];
    id += kHex[bytes[i] & 0x0f];
  }
  return id;
}

arrow::Status StoreTransaction::InsertUser(const UserRecord& user) {
  return Run(conn_, "INSERT INTO " + T("users") + " VALUES ($1, $2, $3, $4, $5)",
             {Text(user.id), Text(user.name), Text(FoldIdentifier(user.name)),
              OptText(user.password_hash), Bool(user.enabled)})
      .status();
}

arrow::Status StoreTransaction::UpdateUser(const UserRecord& user) {
  return Run(conn_, "UPDATE " + T("users") +
                        " SET password_hash = $2, enabled = $3 WHERE user_id = $1",
             {Text(user.id), OptText(user.password_hash), Bool(user.enabled)})
      .status();
}

arrow::Status StoreTransaction::DeleteUser(const std::string& user_id) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("memberships") +
                                     " WHERE member_kind = 'user' AND member_id = $1",
                          {Text(user_id)})
                          .status());
  return Run(conn_, "DELETE FROM " + T("users") + " WHERE user_id = $1", {Text(user_id)})
      .status();
}

arrow::Status StoreTransaction::InsertRole(const RoleRecord& role) {
  return Run(conn_, "INSERT INTO " + T("roles") + " VALUES ($1, $2, $3, $4)",
             {Text(role.id), Text(role.name), Text(FoldIdentifier(role.name)), Bool(role.builtin)})
      .status();
}

arrow::Status StoreTransaction::DeleteRole(const std::string& role_id) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("memberships") +
                                     " WHERE role_id = $1 OR (member_kind = 'role' AND member_id = $1)",
                          {Text(role_id)})
                          .status());
  ARROW_RETURN_NOT_OK(
      Run(conn_, "DELETE FROM " + T("object_grants") + " WHERE role_id = $1", {Text(role_id)})
          .status());
  return Run(conn_, "DELETE FROM " + T("roles") + " WHERE role_id = $1", {Text(role_id)})
      .status();
}

arrow::Status StoreTransaction::InsertMembership(const MembershipRecord& m) {
  return Run(conn_, "INSERT INTO " + T("memberships") + " VALUES ($1, $2, $3, $4, $5)",
             {Text(m.member_kind == PrincipalKind::kRole ? "role" : "user"), Text(m.member_id),
              Text(m.role_id), Bool(m.admin_option), Text(m.granted_by)})
      .status();
}

arrow::Status StoreTransaction::DeleteMembership(PrincipalKind member_kind,
                                                 const std::string& member_id,
                                                 const std::string& role_id) {
  return Run(conn_, "DELETE FROM " + T("memberships") +
                        " WHERE member_kind = $1 AND member_id = $2 AND role_id = $3",
             {Text(member_kind == PrincipalKind::kRole ? "role" : "user"), Text(member_id),
              Text(role_id)})
      .status();
}

arrow::Status StoreTransaction::InsertGrant(const ObjectGrantRecord& g) {
  return Run(conn_,
             "INSERT INTO " + T("object_grants") +
                 " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
             {Text(g.id), Text(g.role_id), Text(ScopeName(g.scope)), Text(g.target.catalog),
              Text(ToJson(g.target.schema_path)), Text(g.target.object_name),
              Text(PrivilegeName(g.privilege)), Text(ToJson(g.columns)), Bool(g.grant_option),
              Text(g.granted_by)})
      .status();
}

arrow::Status StoreTransaction::UpdateGrant(const ObjectGrantRecord& g) {
  return Run(conn_, "UPDATE " + T("object_grants") +
                        " SET columns = $2, grant_option = $3 WHERE grant_id = $1",
             {Text(g.id), Text(ToJson(g.columns)), Bool(g.grant_option)})
      .status();
}

arrow::Status StoreTransaction::DeleteGrant(const std::string& grant_id) {
  return Run(conn_, "DELETE FROM " + T("object_grants") + " WHERE grant_id = $1",
             {Text(grant_id)})
      .status();
}

arrow::Status StoreTransaction::UpsertOwner(const OwnerRecord& o) {
  ARROW_RETURN_NOT_OK(DeleteOwner(o.object));
  return Run(conn_, "INSERT INTO " + T("owners") + " VALUES ($1, $2, $3, $4, $5, $6, $7)",
             {Text(ObjectKey(o.object)), Text(o.object.catalog), Text(ToJson(o.object.schema_path)),
              Text(o.object.object_name), Text(ObjectTypeName(o.type)),
              Text(OwnerKindName(o.owner_kind)), Text(o.owner_id)})
      .status();
}

arrow::Status StoreTransaction::DeleteOwner(const ObjectPath& object) {
  return Run(conn_, "DELETE FROM " + T("owners") + " WHERE object_key = $1",
             {Text(ObjectKey(object))})
      .status();
}

}  // namespace gizmosql::enterprise::rbac
