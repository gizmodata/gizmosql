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

// Persistent store for users, roles, memberships, grants and owners.
//
// The store lives in PostgreSQL, so every replica of a cluster reads the same
// grants. It is reached through a PRIVATE in-process DuckDB instance (separate
// from the one serving clients) that attaches the store database: no client
// session can see, query or ATTACH it. Unit tests use an in-memory store.
//
// Every mutation runs in one store transaction that first bumps a data version
// row, which serializes writers across replicas; readers poll that version to
// know when to reload.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

#include "rbac_model.h"

namespace duckdb {
class DuckDB;
class Connection;
}  // namespace duckdb

namespace gizmosql::enterprise::rbac {

class StoreTransaction;

class RbacStore {
 public:
  // `uri`: a PostgreSQL connection string or URI ("postgresql://...",
  // "postgres://...", or libpq "host=... dbname=..."), or "memory" for an
  // in-process store (tests).
  static arrow::Result<std::unique_ptr<RbacStore>> Open(const std::string& uri);
  ~RbacStore();

  // Read everything into a snapshot.
  arrow::Result<std::shared_ptr<const Snapshot>> Load();

  // The store's data version (cheap; replicas poll it).
  arrow::Result<int64_t> CurrentVersion();

  // Run `fn` in one store transaction. The data version is bumped first (so
  // concurrent writers on any replica serialize), `fn` receives a snapshot read
  // inside the transaction and a handle to write with. Commits when `fn`
  // returns OK; rolls back otherwise. A serialization conflict with another
  // replica is retried.
  arrow::Status Mutate(
      const std::function<arrow::Status(StoreTransaction&, const Snapshot&)>& fn);

  const std::string& description() const { return description_; }

 private:
  RbacStore() = default;
  arrow::Status InitializeSchema();
  arrow::Result<std::shared_ptr<const Snapshot>> LoadLocked();

  std::unique_ptr<duckdb::DuckDB> db_;
  std::unique_ptr<duckdb::Connection> conn_;
  std::mutex mutex_;
  std::string description_;  // for logs; never contains credentials
  std::string secret_dir_;   // the private instance's (empty) secret directory

  friend class StoreTransaction;
};

// Writes inside RbacStore::Mutate. Only valid inside the callback.
class StoreTransaction {
 public:
  arrow::Status InsertUser(const UserRecord& user);
  arrow::Status UpdateUser(const UserRecord& user);
  arrow::Status DeleteUser(const std::string& user_id);
  arrow::Status InsertRole(const RoleRecord& role);
  arrow::Status DeleteRole(const std::string& role_id);
  arrow::Status InsertMembership(const MembershipRecord& membership);
  arrow::Status DeleteMembership(PrincipalKind member_kind, const std::string& member_id,
                                 const std::string& role_id);
  arrow::Status InsertGrant(const ObjectGrantRecord& grant);
  arrow::Status UpdateGrant(const ObjectGrantRecord& grant);
  arrow::Status DeleteGrant(const std::string& grant_id);
  arrow::Status UpsertOwner(const OwnerRecord& owner);
  // nullopt removes the attribute.
  arrow::Status SetUserAttribute(const std::string& user_id, const std::string& attribute,
                                 const std::optional<std::string>& value);
  arrow::Status UpsertTenant(const TenantRecord& tenant);
  // Also forgets the tenant's catalog and principal assignments.
  arrow::Status DeleteTenant(const std::string& tenant_id);
  // An empty tenant_id removes the assignment.
  arrow::Status SetPrincipalTenant(PrincipalKind kind, const std::string& id,
                                   const std::string& tenant_id);
  arrow::Status SetCatalogTenant(const std::string& catalog, const std::string& tenant_id);
  arrow::Status InsertGroupMapping(const GroupMappingRecord& mapping);
  arrow::Status InsertScopedToken(const ScopedTokenRecord& token);
  arrow::Status DeleteScopedToken(const std::string& token_id);
  arrow::Status DeleteGroupMapping(const std::string& mapping_id);
  arrow::Status UpsertRowPolicy(const RowPolicyRecord& policy);
  arrow::Status DeleteRowPolicy(const std::string& policy_id);
  arrow::Status UpsertMaskingPolicy(const MaskingPolicyRecord& policy);
  // Also removes the policy's column bindings.
  arrow::Status DeleteMaskingPolicy(const std::string& policy_id);
  arrow::Status SetMaskBinding(const MaskBindingRecord& binding, bool remove);
  // One record per (role, privilege, grantor).
  arrow::Status UpsertSystemPrivilege(const SystemPrivilegeRecord& record);
  arrow::Status DeleteSystemPrivilege(const SystemPrivilegeRecord& record);
  arrow::Status DeleteOwner(const ObjectPath& object);

  // A fresh random id (UUID v4 text).
  static std::string NewId();

  // The store as this transaction sees it, its own writes included.
  arrow::Result<std::shared_ptr<const Snapshot>> Reload();

 private:
  StoreTransaction(RbacStore& store, duckdb::Connection& conn) : store_(store), conn_(conn) {}
  RbacStore& store_;
  duckdb::Connection& conn_;
  friend class RbacStore;
};

}  // namespace gizmosql::enterprise::rbac
