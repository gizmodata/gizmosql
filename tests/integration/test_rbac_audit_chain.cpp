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

// The access-control audit records' hash chain (RBAC 1.3): a server's own log
// verifies, and every kind of edit to it is reported. Needs a license that
// includes RBAC.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "enterprise/enterprise_features.h"
#include "gizmosql_audit.h"
#include "gizmosql_logging.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kKey = "audit-chain-test-key";

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-audit-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-audit-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-5000-%012d", n);
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  arrow::Status Run(const std::string& sql) {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(co, sql));
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      (void)table;
    }
    return arrow::Status::OK();
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

std::vector<std::string> Lines(const std::string& text) {
  std::vector<std::string> lines;
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) lines.push_back(line);
  return lines;
}

std::string Join(const std::vector<std::string>& lines) {
  std::string out;
  for (const auto& l : lines) out += l + "\n";
  return out;
}

gizmosql::AuditChainReport Verify(const std::string& text, const std::string& key = kKey,
                                  bool partial = false) {
  std::istringstream in(text);
  return gizmosql::VerifyAuditLog(in, key, partial);
}

std::string Problems(const gizmosql::AuditChainReport& r) {
  std::string out;
  for (const auto& p : r.problems) out += p + "\n";
  return out;
}

bool Mentions(const gizmosql::AuditChainReport& r, const std::string& what) {
  return Problems(r).find(what) != std::string::npos;
}

class RbacAuditChainFixture
    : public gizmosql::testing::ServerTestFixture<RbacAuditChainFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_audit_chain_test.db",
        .port = 31780,
        .health_port = 31781,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  void SetUp() override {
    if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
      GTEST_SKIP() << "license does not include RBAC";
    }
    ASSERT_TRUE(IsServerReady());
    log_path_ =
        (std::filesystem::temp_directory_path() / "gizmosql_rbac_audit_chain_test.log")
            .string();
    std::filesystem::remove(log_path_);
    // The harness does not install GizmoSQL's logger; write JSON to a file, as
    // a server run with --log-format json --log-file does.
    gizmosql::LogConfig cfg;
    cfg.format = gizmosql::LogFormat::kJson;
    cfg.level = arrow::util::ArrowLogLevel::ARROW_INFO;
    cfg.file_path = log_path_;
    gizmosql::InitLogging(cfg);
    gizmosql::StartAuditChain(kKey);
  }
  void TearDown() override {
    gizmosql::LogConfig cfg;  // back to text on stderr
    cfg.level = arrow::util::ArrowLogLevel::ARROW_INFO;
    gizmosql::InitLogging(cfg);
  }

  std::string Log() const {
    std::ifstream in(log_path_);
    std::stringstream text;
    text << in.rdbuf();
    return text.str();
  }

  // A log with every kind of access-control record in it.
  std::string WriteSomeRecords() {
    Client admin(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {"CREATE TABLE ledger AS SELECT 1 AS id, 'x' AS v",
                              "CREATE TABLE vault AS SELECT 1 AS id",
                              "CREATE ROLE clerks", "GRANT SELECT ON ledger TO clerks",
                              "CREATE USER cleo", "GRANT clerks TO cleo"}) {
        auto st = admin.Run(sql);
        EXPECT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    Client cleo(GetPort(), "cleo", "user", 2);
    EXPECT_TRUE(cleo.Run("SELECT * FROM ledger").ok());              // allowed
    EXPECT_FALSE(cleo.Run("SELECT * FROM vault").ok());              // denied
    EXPECT_FALSE(cleo.Run("GRANT SELECT ON vault TO clerks").ok());  // refused statement
    EXPECT_TRUE(
        admin.Run("GRANT SELECT ON vault TO clerks").ok());  // access-control statement
    EXPECT_TRUE(admin.Run("REVOKE SELECT ON vault FROM clerks").ok());
    return Log();
  }

  std::string log_path_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacAuditChainFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacAuditChainFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RbacAuditChainFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacAuditChainFixture>::config_{};

TEST_F(RbacAuditChainFixture, TheServersOwnLogVerifies) {
  const auto log = WriteSomeRecords();
  const auto report = Verify(log);
  EXPECT_TRUE(report.ok()) << Problems(report);
  EXPECT_GE(report.records, 6);
  ASSERT_EQ(report.chains.size(), 1u);
  // Each kind of record is in the chain.
  for (const char* what :
       {"\"status\":\"allowed\"", "\"status\":\"denied\"", "\"status\":\"rejected\"",
        "\"status\":\"success\"", "Access-control audit chain started"}) {
    bool chained = false;
    for (const auto& line : Lines(log)) {
      chained = chained || (line.find(what) != std::string::npos &&
                            line.find("\"audit_hash\"") != std::string::npos);
    }
    EXPECT_TRUE(chained) << what << " is not in the chain:\n" << log;
  }
  // Lines in another order are still the same chain.
  auto lines = Lines(log);
  std::reverse(lines.begin(), lines.end());
  EXPECT_TRUE(Verify(Join(lines)).ok());
}

TEST_F(RbacAuditChainFixture, EveryKindOfEditIsReported) {
  const auto lines = Lines(WriteSomeRecords());
  std::vector<size_t> chained;
  for (size_t i = 0; i < lines.size(); ++i) {
    if (lines[i].find("\"audit_hash\"") != std::string::npos) chained.push_back(i);
  }
  ASSERT_GE(chained.size(), 6u);
  auto edit = [&](size_t index, const std::string& from, const std::string& to) {
    auto copy = lines;
    const auto at = copy[index].find(from);
    EXPECT_NE(at, std::string::npos) << from << " not in " << copy[index];
    if (at != std::string::npos) copy[index].replace(at, from.size(), to);
    return Join(copy);
  };
  // A denial rewritten as allowed.
  size_t denied = 0;
  for (auto i : chained) {
    if (lines[i].find("\"status\":\"denied\"") != std::string::npos) denied = i;
  }
  ASSERT_NE(denied, 0u);
  auto r = Verify(edit(denied, "\"status\":\"denied\"", "\"status\":\"allowed\""));
  EXPECT_FALSE(r.ok());
  EXPECT_TRUE(Mentions(r, "was changed after it was written")) << Problems(r);
  // The user's name changed.
  r = Verify(edit(denied, "\"user\":\"cleo\"", "\"user\":\"carl\""));
  EXPECT_TRUE(Mentions(r, "was changed after it was written")) << Problems(r);
  // A field removed from the record.
  r = Verify(edit(denied, ",\"user\":\"cleo\"", ""));
  EXPECT_TRUE(Mentions(r, "the field user is missing")) << Problems(r);
  // A field left out of what the digest covers.
  r = Verify(edit(denied, ",user,", ","));
  EXPECT_TRUE(Mentions(r, "was changed after it was written")) << Problems(r);
  // A record removed.
  {
    auto copy = lines;
    copy.erase(copy.begin() + static_cast<long>(chained[2]));
    r = Verify(Join(copy));
    EXPECT_TRUE(Mentions(r, "is missing")) << Problems(r);
    EXPECT_TRUE(Mentions(r, "does not follow the record before it")) << Problems(r);
  }
  // A record repeated.
  {
    auto copy = lines;
    copy.push_back(lines[chained[2]]);
    r = Verify(Join(copy));
    EXPECT_TRUE(Mentions(r, "twice")) << Problems(r);
  }
  // The first records cut off: reported, unless the log is said to be partial.
  {
    std::vector<std::string> copy;
    for (size_t i = chained[2]; i < lines.size(); ++i) copy.push_back(lines[i]);
    r = Verify(Join(copy));
    EXPECT_TRUE(Mentions(r, "its first records are missing")) << Problems(r);
    EXPECT_TRUE(Verify(Join(copy), kKey, /*partial=*/true).ok());
  }
  // The wrong key, or none.
  r = Verify(Join(lines), "another-key");
  EXPECT_TRUE(Mentions(r, "was changed after it was written")) << Problems(r);
  r = Verify(Join(lines), "");
  EXPECT_TRUE(Mentions(r, "sealed with a key")) << Problems(r);
  // Positive twin: the unedited log.
  EXPECT_TRUE(Verify(Join(lines)).ok()) << Problems(Verify(Join(lines)));
}

TEST_F(RbacAuditChainFixture, AChainWithoutTheKeyIsNotAcceptedForOne) {
  // Someone who rewrites the log without the key can only rebuild an unkeyed
  // chain; a check with the key refuses it.
  gizmosql::StartAuditChain("");
  WriteSomeRecords();
  const auto log = Log();
  auto unkeyed_start = log.rfind("Access-control audit chain started");
  ASSERT_NE(unkeyed_start, std::string::npos);
  const auto line_start = log.rfind('\n', unkeyed_start);
  const std::string unkeyed =
      log.substr(line_start == std::string::npos ? 0 : line_start + 1);
  EXPECT_TRUE(Verify(unkeyed, "").ok()) << Problems(Verify(unkeyed, ""));
  auto r = Verify(unkeyed, kKey);
  EXPECT_TRUE(Mentions(r, "not sealed with the key")) << Problems(r);
}

TEST_F(RbacAuditChainFixture, RecordsBelowTheLogLevelDoNotBreakTheChain) {
  gizmosql::SetLogLevel(arrow::util::ArrowLogLevel::ARROW_WARNING);
  WriteSomeRecords();  // the INFO records are not written
  gizmosql::SetLogLevel(arrow::util::ArrowLogLevel::ARROW_INFO);
  WriteSomeRecords();
  const auto report = Verify(Log());
  EXPECT_TRUE(report.ok()) << Problems(report);
}

TEST_F(RbacAuditChainFixture, ConcurrentWritersKeepOneChain) {
  std::vector<std::thread> threads;
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([t] {
      for (int i = 0; i < 200; ++i) {
        gizmosql::LogAuditRecord(arrow::util::ArrowLogLevel::ARROW_INFO, __FILE__,
                                 __LINE__, "Concurrent audit record",
                                 {{"kind", "access_control"},
                                  {"writer", std::to_string(t)},
                                  {"n", std::to_string(i)}});
      }
    });
  }
  for (auto& th : threads) th.join();
  const auto report = Verify(Log());
  EXPECT_TRUE(report.ok()) << Problems(report);
  EXPECT_EQ(report.records, 1 + 8 * 200);
}
