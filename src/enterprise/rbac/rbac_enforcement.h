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

// Enforcement of grants on every statement, inside DuckDB.
//
// A DuckDB optimizer extension's pre-optimize hook receives the bound logical
// plan of every statement before any optimization, walks it, and checks each
// table read or written, each column referenced and each DDL object against the
// current snapshot. Anything it cannot classify is denied. A per-session DuckDB
// state then refuses any statement that reached the end of preparation without
// being checked (the optimizer, and with it the hook, can be skipped), and asks
// DuckDB to re-plan a prepared statement when grants changed since it was
// checked, so a REVOKE applies to the next execution.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <duckdb.hpp>
#include <duckdb/main/client_context_state.hpp>

#include "rbac_engine.h"

namespace gizmosql::enterprise::rbac {

constexpr const char* kRbacSessionStateName = "gizmosql_rbac";

// Ownership bookkeeping a statement asks for once it has run successfully.
struct PendingOwnership {
  enum class Kind { kCreate, kDrop };
  Kind kind;
  ObjectPath object;
  ObjectType type;
  std::string definition_hash;  // a created view's (OwnerRecord::definition_hash)
};

class RbacSessionState : public duckdb::ClientContextState {
 public:
  RbacSessionState(std::shared_ptr<RbacEngine> engine, Principal principal);

  const Principal& principal() const { return principal_; }
  std::shared_ptr<RbacEngine> engine() const { return engine_.lock(); }

  // GizmoSQL's own statements on the session connection (metadata queries for
  // the Flight SQL catalog calls, SET handling, ...) are not checked.
  class BypassScope {
   public:
    explicit BypassScope(duckdb::ClientContext& context);
    ~BypassScope();
    BypassScope(const BypassScope&) = delete;
    BypassScope& operator=(const BypassScope&) = delete;

   private:
    duckdb::shared_ptr<RbacSessionState> state_;
  };
  bool bypassed() const { return bypass_depth_.load() > 0; }
  bool unchecked() const { return principal_.superuser || bypassed(); }

  // Called by the pre-optimize hook once a plan passed (or needs no check).
  // `prepare_only`: the plan is a PREPARE (DuckDB 2.x prepares a client-side
  // statement by running one), so its ownership changes wait for a later run.
  void NoteChecked(int64_t snapshot_version, std::vector<PendingOwnership> ownership,
                   bool prepare_only);

  // Ownership changes of the statement most recently prepared without being
  // run (a client-side prepared statement). GizmoSQL takes them right after
  // preparing and applies them once the statement has run successfully.
  std::vector<PendingOwnership> TakePreparedOwnership();

  bool CanRequestRebind() override { return true; }
  duckdb::RebindQueryInfo OnFinalizePrepare(duckdb::ClientContext& context,
                                            duckdb::PreparedStatementData& prepared,
                                            duckdb::PreparedStatementMode mode) override;
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  duckdb::RebindQueryInfo OnRebindPreparedStatement(
      duckdb::ClientContext& context, duckdb::BindPreparedStatementCallbackInfo& info,
      duckdb::RebindQueryInfo current_rebind) override;
#else
  duckdb::RebindQueryInfo OnExecutePrepared(duckdb::ClientContext& context,
                                            duckdb::PreparedStatementCallbackInfo& info,
                                            duckdb::RebindQueryInfo current_rebind) override;
#endif
  void QueryEnd(duckdb::ClientContext& context, duckdb::optional_ptr<duckdb::ErrorData> error) override;

 private:
  duckdb::RebindQueryInfo RebindIfGrantsChanged(const duckdb::PreparedStatementData& prepared);

  std::weak_ptr<RbacEngine> engine_;
  Principal principal_;
  std::atomic<int> bypass_depth_{0};

  std::mutex mutex_;
  // Plans checked by the hook that have not reached OnFinalizePrepare yet.
  int checked_pending_ = 0;
  int64_t last_checked_version_ = 0;
  // Ownership changes of the plan just checked, until OnFinalizePrepare files
  // them under "runs now" (pending_ownership_) or "prepared" (prepared_ownership_).
  std::vector<PendingOwnership> planned_ownership_;
  bool planned_prepare_only_ = false;
  std::vector<PendingOwnership> prepared_ownership_;
  // Which snapshot version each prepared statement was checked against.
  std::unordered_map<const void*, int64_t> prepared_versions_;
  std::vector<PendingOwnership> pending_ownership_;
};

// Record what a statement that ran successfully created or dropped.
void ApplyOwnership(RbacEngine& engine, const Principal& principal,
                    const std::vector<PendingOwnership>& changes);

// Install the pre-optimize hook on a database configuration (once, before the
// database is opened).
void RegisterRbacEnforcement(duckdb::DBConfig& config);

// Give a session connection its principal. Every client session connection on
// an access-control server gets one; a connection without it is GizmoSQL's own.
void AttachRbacSession(duckdb::ClientContext& context, std::shared_ptr<RbacEngine> engine,
                       Principal principal);

}  // namespace gizmosql::enterprise::rbac
