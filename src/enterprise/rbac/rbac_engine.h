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

// The access-control engine: runs CREATE USER / ROLE, GRANT, REVOKE, ... against
// the store, keeps the current snapshot (reloading when another replica changes
// the store), authenticates store users, and answers authorization questions.

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "rbac_ddl.h"
#include "rbac_model.h"
#include "rbac_password.h"
#include "rbac_store.h"

namespace gizmosql::enterprise::rbac {

// A table or view as the engine (DuckDB) resolved it.
struct ResolvedObject {
  ObjectPath path;
  ObjectType type = ObjectType::kTable;
  std::vector<std::string> columns;
};

// Resolves names in GRANT / REVOKE / ALTER ... OWNER TO the way the caller's
// session would, so a grant names exactly the object a query would read.
class ObjectResolver {
 public:
  virtual ~ObjectResolver() = default;
  virtual arrow::Result<ResolvedObject> ResolveObject(const QualifiedName& name) = 0;
  // catalog.schema[.sub...] or schema (in the session's catalog)
  virtual arrow::Result<ObjectPath> ResolveSchema(const QualifiedName& name) = 0;
  virtual arrow::Result<std::string> ResolveCatalog(const std::string& name) = 0;
};

// The result of an access-control statement, returned to the client as a table.
struct CommandResult {
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> rows;
  // Users whose sessions must end now (ALTER USER ... DISABLE, DROP USER).
  std::vector<std::string> terminate_user_ids;
};

class RbacEngine {
 public:
  struct Options {
    std::string store_uri;
    int password_iterations = kDefaultPasswordIterations;
    // How often to look for changes made by other replicas. 0 disables polling.
    std::chrono::milliseconds poll_interval{1000};
  };

  static arrow::Result<std::shared_ptr<RbacEngine>> Create(const Options& options);
  ~RbacEngine();

  RbacEngine(const RbacEngine&) = delete;
  RbacEngine& operator=(const RbacEngine&) = delete;

  // The snapshot every check reads. Never null.
  std::shared_ptr<const Snapshot> snapshot() const;

  // Reload when the store's version moved (another replica wrote).
  arrow::Status Refresh();

  arrow::Result<CommandResult> Execute(const RbacCommand& command, const Principal& actor,
                                       ObjectResolver& resolver);

  // Password login for a store user. Returns the principal on success; any
  // failure (unknown user, wrong password, disabled) is the same error.
  arrow::Result<Principal> AuthenticatePassword(const std::string& user_name,
                                                const std::string& password) const;

  // The principal for a user identified by a token. Fails unless the user
  // exists in the store and is enabled.
  arrow::Result<Principal> PrincipalForUser(const std::string& user_name) const;

  // Is the principal still allowed to run anything (exists, enabled)?
  bool IsActive(const Principal& principal) const;

  // Ownership bookkeeping for objects created / dropped through GizmoSQL.
  arrow::Status RecordOwner(const ObjectPath& object, ObjectType type, const Principal& owner);
  arrow::Status ForgetOwner(const ObjectPath& object);

  const std::string& store_description() const { return store_->description(); }

 private:
  explicit RbacEngine(Options options) : options_(std::move(options)) {}
  void PollLoop();
  void Publish(std::shared_ptr<const Snapshot> snapshot);

  Options options_;
  std::string dummy_hash_;  // verified against for unknown users (equal timing)
  std::unique_ptr<RbacStore> store_;
  mutable std::mutex snapshot_mutex_;
  std::shared_ptr<const Snapshot> snapshot_;

  std::mutex poll_mutex_;
  std::condition_variable poll_cv_;
  bool stopping_ = false;
  std::thread poller_;
};

// "GizmoSQL access control: ..." — the prefix every RBAC error carries.
arrow::Status AccessControlError(const std::string& message);

}  // namespace gizmosql::enterprise::rbac
