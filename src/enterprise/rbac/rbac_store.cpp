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

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <random>
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
          "owner_kind VARCHAR NOT NULL, owner_id VARCHAR NOT NULL, definition_hash VARCHAR NOT "
          "NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("user_attributes") +
          " (user_id VARCHAR NOT NULL, attribute VARCHAR NOT NULL, value VARCHAR NOT NULL, "
          "PRIMARY KEY (user_id, attribute))",
      "CREATE TABLE IF NOT EXISTS " + T("row_policies") +
          " (policy_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, table_key VARCHAR NOT NULL, "
          "catalog_name VARCHAR NOT NULL, schema_path VARCHAR NOT NULL, object_name VARCHAR NOT "
          "NULL, permissive BOOLEAN NOT NULL, role_ids VARCHAR NOT NULL, predicate VARCHAR NOT "
          "NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("masking_policies") +
          " (policy_id VARCHAR PRIMARY KEY, name VARCHAR NOT NULL, name_key VARCHAR NOT NULL "
          "UNIQUE, parameter VARCHAR NOT NULL, parameter_type VARCHAR NOT NULL, return_type "
          "VARCHAR NOT NULL, body VARCHAR NOT NULL)",
      "CREATE TABLE IF NOT EXISTS " + T("mask_bindings") +
          " (table_key VARCHAR NOT NULL, column_key VARCHAR NOT NULL, catalog_name VARCHAR NOT "
          "NULL, schema_path VARCHAR NOT NULL, object_name VARCHAR NOT NULL, column_name VARCHAR "
          "NOT NULL, policy_id VARCHAR NOT NULL, exempt_role_ids VARCHAR NOT NULL, PRIMARY KEY "
          "(table_key, column_key))",
      "CREATE TABLE IF NOT EXISTS " + T("system_privileges") +
          " (role_id VARCHAR NOT NULL, privilege VARCHAR NOT NULL, admin_option BOOLEAN NOT NULL, "
          "granted_by VARCHAR NOT NULL, PRIMARY KEY (role_id, privilege, granted_by))",
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
                     GetBool(*u, 3, r), {}});
  }
  ARROW_ASSIGN_OR_RAISE(auto ua, Run(conn, "SELECT user_id, attribute, value FROM " +
                                               T("user_attributes")));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*ua); ++r) {
    const std::string id = GetString(*ua, 0, r);
    for (auto& user : users) {
      if (user.id == id) user.attributes[GetString(*ua, 1, r)] = GetString(*ua, 2, r);
    }
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
                        "owner_id, definition_hash FROM " + T("owners")));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*o); ++r) {
    OwnerRecord owner;
    owner.object.catalog = GetString(*o, 0, r);
    owner.object.schema_path = FromJson(GetString(*o, 1, r));
    owner.object.object_name = GetString(*o, 2, r);
    owner.type = ObjectTypeFrom(GetString(*o, 3, r));
    owner.owner_kind = OwnerKindFrom(GetString(*o, 4, r));
    owner.owner_id = GetString(*o, 5, r);
    owner.definition_hash = GetString(*o, 6, r);
    owners.push_back(std::move(owner));
  }

  std::vector<SystemPrivilegeRecord> system_privileges;
  ARROW_ASSIGN_OR_RAISE(
      auto sp, Run(conn, "SELECT role_id, privilege, admin_option, granted_by FROM " +
                             T("system_privileges") + " ORDER BY role_id, privilege, granted_by"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*sp); ++r) {
    auto privilege = SystemPrivilegeFromName(GetString(*sp, 1, r));
    if (!privilege) return StoreError("corrupt store", "unknown system privilege");
    system_privileges.push_back(
        {GetString(*sp, 0, r), *privilege, GetBool(*sp, 2, r), GetString(*sp, 3, r)});
  }

  std::vector<RowPolicyRecord> row_policies;
  ARROW_ASSIGN_OR_RAISE(
      auto rp, Run(conn, "SELECT policy_id, name, catalog_name, schema_path, object_name, "
                         "permissive, role_ids, predicate FROM " + T("row_policies") +
                             " ORDER BY policy_id"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*rp); ++r) {
    RowPolicyRecord p;
    p.id = GetString(*rp, 0, r);
    p.name = GetString(*rp, 1, r);
    p.table = {GetString(*rp, 2, r), FromJson(GetString(*rp, 3, r)), GetString(*rp, 4, r)};
    p.permissive = GetBool(*rp, 5, r);
    p.role_ids = FromJson(GetString(*rp, 6, r));
    p.predicate = GetString(*rp, 7, r);
    row_policies.push_back(std::move(p));
  }
  std::vector<MaskingPolicyRecord> masking_policies;
  ARROW_ASSIGN_OR_RAISE(auto mp, Run(conn, "SELECT policy_id, name, parameter, parameter_type, "
                                           "return_type, body FROM " + T("masking_policies") +
                                               " ORDER BY name_key"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*mp); ++r) {
    masking_policies.push_back({GetString(*mp, 0, r), GetString(*mp, 1, r), GetString(*mp, 2, r),
                                GetString(*mp, 3, r), GetString(*mp, 4, r), GetString(*mp, 5, r)});
  }
  std::vector<MaskBindingRecord> mask_bindings;
  ARROW_ASSIGN_OR_RAISE(auto mb, Run(conn, "SELECT catalog_name, schema_path, object_name, "
                                           "column_name, policy_id, exempt_role_ids FROM " +
                                               T("mask_bindings") + " ORDER BY table_key, column_key"));
  for (dd::idx_t r = 0; r < compat::ResultRowCount(*mb); ++r) {
    mask_bindings.push_back(
        {{GetString(*mb, 0, r), FromJson(GetString(*mb, 1, r)), GetString(*mb, 2, r)},
         GetString(*mb, 3, r), GetString(*mb, 4, r), FromJson(GetString(*mb, 5, r))});
  }

  return std::make_shared<const Snapshot>(
      version, std::move(users), std::move(roles), std::move(memberships), std::move(grants),
      std::move(owners), std::move(system_privileges), std::move(row_policies),
      std::move(masking_policies), std::move(mask_bindings));
}

arrow::Status RbacStore::Mutate(
    const std::function<arrow::Status(StoreTransaction&, const Snapshot&)>& fn) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& conn = *conn_;
  arrow::Status last;
  // Replicas writing at once conflict on the version row; the loser retries.
  // Under steady contention a few attempts are not enough, so keep trying for
  // a while, with jitter so two replicas do not retry in lockstep.
  thread_local std::minstd_rand jitter(std::random_device{}());
  constexpr int kAttempts = 40;
  for (int attempt = 0; attempt < kAttempts; ++attempt) {
    ARROW_RETURN_NOT_OK(Exec(conn, "BEGIN TRANSACTION"));
    auto body = [&]() -> arrow::Status {
      // Bump the version first: the row lock serializes writers on every replica.
      ARROW_RETURN_NOT_OK(Run(conn, "UPDATE " + T("meta") +
                                        " SET value = CAST(CAST(value AS BIGINT) + 1 AS VARCHAR) "
                                        "WHERE key = 'data_version'")
                              .status());
      ARROW_ASSIGN_OR_RAISE(auto snapshot, LoadLocked());
      StoreTransaction txn(*this, conn);
      ARROW_RETURN_NOT_OK(fn(txn, *snapshot));
      return Exec(conn, "COMMIT");
    };
    last = body();
    if (last.ok()) return last;
    (void)conn.Query("ROLLBACK");
    if (!IsRetryableConflict(last.message())) return last;
    const int backoff_ms = std::min(10 * (attempt + 1), 200) +
                           static_cast<int>(jitter() % 25);
    std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
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

arrow::Status StoreTransaction::SetUserAttribute(const std::string& user_id,
                                                const std::string& attribute,
                                                const std::optional<std::string>& value) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("user_attributes") +
                                     " WHERE user_id = $1 AND attribute = $2",
                          {Text(user_id), Text(attribute)})
                          .status());
  if (!value) return arrow::Status::OK();
  return Run(conn_, "INSERT INTO " + T("user_attributes") + " VALUES ($1, $2, $3)",
             {Text(user_id), Text(attribute), Text(*value)})
      .status();
}

arrow::Status StoreTransaction::UpsertRowPolicy(const RowPolicyRecord& p) {
  ARROW_RETURN_NOT_OK(DeleteRowPolicy(p.id));
  return Run(conn_, "INSERT INTO " + T("row_policies") +
                        " VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
             {Text(p.id), Text(p.name), Text(ObjectKey(p.table)), Text(p.table.catalog),
              Text(ToJson(p.table.schema_path)), Text(p.table.object_name), Bool(p.permissive),
              Text(ToJson(p.role_ids)), Text(p.predicate)})
      .status();
}

arrow::Status StoreTransaction::DeleteRowPolicy(const std::string& policy_id) {
  return Run(conn_, "DELETE FROM " + T("row_policies") + " WHERE policy_id = $1",
             {Text(policy_id)})
      .status();
}

arrow::Status StoreTransaction::UpsertMaskingPolicy(const MaskingPolicyRecord& m) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("masking_policies") + " WHERE policy_id = $1",
                          {Text(m.id)})
                          .status());
  return Run(conn_, "INSERT INTO " + T("masking_policies") + " VALUES ($1, $2, $3, $4, $5, $6, $7)",
             {Text(m.id), Text(m.name), Text(FoldIdentifier(m.name)), Text(m.parameter),
              Text(m.parameter_type), Text(m.return_type), Text(m.body)})
      .status();
}

arrow::Status StoreTransaction::DeleteMaskingPolicy(const std::string& policy_id) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("mask_bindings") + " WHERE policy_id = $1",
                          {Text(policy_id)})
                          .status());
  return Run(conn_, "DELETE FROM " + T("masking_policies") + " WHERE policy_id = $1",
             {Text(policy_id)})
      .status();
}

arrow::Status StoreTransaction::SetMaskBinding(const MaskBindingRecord& b, bool remove) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("mask_bindings") +
                                     " WHERE table_key = $1 AND column_key = $2",
                          {Text(ObjectKey(b.table)), Text(FoldIdentifier(b.column))})
                          .status());
  if (remove) return arrow::Status::OK();
  return Run(conn_, "INSERT INTO " + T("mask_bindings") + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
             {Text(ObjectKey(b.table)), Text(FoldIdentifier(b.column)), Text(b.table.catalog),
              Text(ToJson(b.table.schema_path)), Text(b.table.object_name), Text(b.column),
              Text(b.policy_id), Text(ToJson(b.exempt_role_ids))})
      .status();
}

arrow::Status StoreTransaction::DeleteUser(const std::string& user_id) {
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("memberships") +
                                     " WHERE member_kind = 'user' AND member_id = $1",
                          {Text(user_id)})
                          .status());
  ARROW_RETURN_NOT_OK(Run(conn_, "DELETE FROM " + T("user_attributes") + " WHERE user_id = $1",
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
  ARROW_RETURN_NOT_OK(
      Run(conn_, "DELETE FROM " + T("system_privileges") + " WHERE role_id = $1", {Text(role_id)})
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
                        " SET columns = $2, grant_option = $3, granted_by = $4 WHERE grant_id = $1",
             {Text(g.id), Text(ToJson(g.columns)), Bool(g.grant_option), Text(g.granted_by)})
      .status();
}

arrow::Status StoreTransaction::DeleteGrant(const std::string& grant_id) {
  return Run(conn_, "DELETE FROM " + T("object_grants") + " WHERE grant_id = $1",
             {Text(grant_id)})
      .status();
}

arrow::Status StoreTransaction::UpsertOwner(const OwnerRecord& o) {
  ARROW_RETURN_NOT_OK(DeleteOwner(o.object));
  return Run(conn_, "INSERT INTO " + T("owners") + " VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
             {Text(ObjectKey(o.object)), Text(o.object.catalog), Text(ToJson(o.object.schema_path)),
              Text(o.object.object_name), Text(ObjectTypeName(o.type)),
              Text(OwnerKindName(o.owner_kind)), Text(o.owner_id), Text(o.definition_hash)})
      .status();
}

arrow::Status StoreTransaction::UpsertSystemPrivilege(const SystemPrivilegeRecord& r) {
  ARROW_RETURN_NOT_OK(DeleteSystemPrivilege(r));
  return Run(conn_, "INSERT INTO " + T("system_privileges") + " VALUES ($1, $2, $3, $4)",
             {Text(r.role_id), Text(SystemPrivilegeName(r.privilege)), Bool(r.admin_option),
              Text(r.granted_by)})
      .status();
}

arrow::Status StoreTransaction::DeleteSystemPrivilege(const SystemPrivilegeRecord& r) {
  return Run(conn_, "DELETE FROM " + T("system_privileges") +
                        " WHERE role_id = $1 AND privilege = $2 AND granted_by = $3",
             {Text(r.role_id), Text(SystemPrivilegeName(r.privilege)), Text(r.granted_by)})
      .status();
}

arrow::Result<std::shared_ptr<const Snapshot>> StoreTransaction::Reload() {
  return store_.LoadLocked();
}

arrow::Status StoreTransaction::DeleteOwner(const ObjectPath& object) {
  return Run(conn_, "DELETE FROM " + T("owners") + " WHERE object_key = $1",
             {Text(ObjectKey(object))})
      .status();
}

}  // namespace gizmosql::enterprise::rbac
