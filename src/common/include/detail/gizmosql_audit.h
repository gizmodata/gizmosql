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

// Tamper-evident access-control audit records.
//
// Every access-control audit record a server writes (kind=access_control)
// joins a hash chain: it carries the chain's id, its sequence number, the
// previous record's digest and its own digest over its fields and message.
// Editing, removing, reordering or inserting a record breaks the chain, which
// `gizmosql_server --verify-audit-log FILE` reports. With
// GIZMOSQL_AUDIT_CHAIN_KEY set, digests are HMAC-SHA256 under that key, so
// nobody without the key can rebuild a consistent chain after editing; without
// it they are plain SHA-256 and only show accidental or partial changes.
//
// A server starts a new chain (a new id, sequence 1) when access control
// starts. The newest records of a chain can be cut off without a trace in the
// file itself; ship the log elsewhere as it is written to catch that.

#pragma once

#include <cstdint>
#include <istream>
#include <string>
#include <string_view>
#include <vector>

#include "gizmosql_logging.h"

namespace gizmosql {

// GIZMOSQL_AUDIT_CHAIN_KEY, or empty.
std::string AuditChainKeyFromEnvironment();

// Start a new chain (key empty: SHA-256) and write its first record.
void StartAuditChain(const std::string& key);

// Write an audit record: the chain fields are added, then it is logged as
// GIZMOSQL_LOGKV would (nothing is written, and the chain does not advance,
// when the level is below the logger's threshold).
void LogAuditRecord(arrow::util::ArrowLogLevel level, const char* file, int line,
                    std::string_view msg, FieldList fields);

struct AuditChainReport {
  int64_t records = 0;              // chained records found
  std::vector<std::string> chains;  // "id: records 1..N"
  std::vector<std::string> problems;
  bool ok() const { return records > 0 && problems.empty(); }
};

// Check the chained records in a JSON-format log (--log-format json); other
// lines are skipped. `key`: GIZMOSQL_AUDIT_CHAIN_KEY of the server that wrote
// it (empty for an unkeyed chain). `partial`: the log may start inside a
// chain (rotated logs), so a chain need not begin at sequence 1.
AuditChainReport VerifyAuditLog(std::istream& in, const std::string& key, bool partial = false);

}  // namespace gizmosql

// Session-aware audit record (see GIZMOSQL_LOGKV_SESSION).
#define GIZMOSQL_AUDIT_SESSION(SEV, SESSION, MSG, ...)                                       \
  ::gizmosql::LogAuditRecord(::arrow::util::ArrowLogLevel::ARROW_##SEV, __FILE__, __LINE__, \
                             MSG, ::gizmosql::FieldList{SESSION_KV_FIELDS(SESSION), __VA_ARGS__})
