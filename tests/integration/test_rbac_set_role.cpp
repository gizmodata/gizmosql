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

// SET ROLE: a session narrows itself to one of its roles (with what that role
// inherits, and PUBLIC); RESET ROLE brings all of them back. Needs a license
// that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <set>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "enterprise/enterprise_features.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-set-role-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-set-role-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-7000-%012d", n);
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  arrow::flight::FlightCallOptions Options() const {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    return co;
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Fetch(
      arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> info_result) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto info, std::move(info_result));
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      tables.push_back(table);
    }
    if (tables.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto schema, info->GetSchema(nullptr));
      return arrow::Table::MakeEmpty(schema);
    }
    return arrow::ConcatenateTables(tables);
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
    return Fetch(client_->Execute(Options(), sql));
  }
  arrow::Result<std::shared_ptr<arrow::flight::sql::PreparedStatement>> Prepare(const std::string& sql) {
    return client_->Prepare(Options(), sql);
  }
  arrow::Result<std::shared_ptr<arrow::Table>> ExecutePrepared(arrow::flight::sql::PreparedStatement& stmt) {
    return Fetch(stmt.Execute(Options()));
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

std::string ErrorOf(const arrow::Status& st) { return st.ok() ? "" : st.ToString(); }

std::set<std::string> Column(const std::shared_ptr<arrow::Table>& t, int col = 0) {
  std::set<std::string> out;
  if (!t) return out;
  for (int64_t r = 0; r < t->num_rows(); ++r) {
    out.insert(t->column(col)->GetScalar(r).ValueOrDie()->ToString());
  }
  return out;
}

class RbacSetRoleFixture : public gizmosql::testing::ServerTestFixture<RbacSetRoleFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_set_role_test.db",
        .port = 31790,
        .health_port = 31791,
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
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {
               "CREATE TABLE t_a AS SELECT 'a' AS v", "CREATE TABLE t_b AS SELECT 'b' AS v",
               "CREATE TABLE t_pub AS SELECT 'pub' AS v",
               "CREATE TABLE t_rows AS SELECT * FROM (VALUES (1), (2)) t(id)",
               "CREATE ROLE ra", "CREATE ROLE rb", "CREATE ROLE rc", "CREATE ROLE spare",
               "GRANT SELECT ON t_a TO ra", "GRANT SELECT ON t_b TO rb", "GRANT ra TO rc",
               "GRANT SELECT ON t_pub TO PUBLIC", "GRANT SELECT ON t_rows TO ra, rb",
               "CREATE ROW POLICY only_rb ON t_rows USING (is_role_active('rb'))",
               "CREATE USER sam", "GRANT ra, rb, rc TO sam",
               "CREATE TENANT acme", "CREATE USER acme/ann", "CREATE ROLE acme/r",
               "GRANT acme/r TO acme/ann",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    sam_ = std::make_unique<Client>(GetPort(), "sam", "user", next_session_++);
  }

  std::string Error(Client& who, const std::string& sql) { return ErrorOf(who.Run(sql).status()); }
  void ExpectReads(Client& who, const std::string& table) {
    EXPECT_EQ(Error(who, "SELECT * FROM " + table), "") << "could not read " << table;
  }
  void ExpectRefused(Client& who, const std::string& table) {
    const auto error = Error(who, "SELECT * FROM " + table);
    EXPECT_NE(error.find("no SELECT privilege on table"), std::string::npos)
        << table << " -> " << (error.empty() ? "READ ALLOWED" : error);
  }

  static inline int next_session_ = 10;
  std::unique_ptr<Client> admin_, sam_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacSetRoleFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacSetRoleFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacSetRoleFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacSetRoleFixture>::config_{};

TEST_F(RbacSetRoleFixture, SetRoleNarrowsAndResetRoleRestores) {
  // All roles by default.
  ExpectReads(*sam_, "t_a");
  ExpectReads(*sam_, "t_b");
  ASSERT_EQ(Error(*sam_, "SET ROLE ra"), "");
  ExpectReads(*sam_, "t_a");
  ExpectRefused(*sam_, "t_b");
  // The refusal says the session narrowed itself.
  const auto why = Error(*sam_, "SELECT * FROM t_b");
  EXPECT_NE(why.find("acts with role ra only (SET ROLE); RESET ROLE"), std::string::npos) << why;
  ExpectReads(*sam_, "t_pub");  // PUBLIC still applies
  // SHOW GRANTS shows what the session acts with.
  auto grants = sam_->Run("SHOW GRANTS");
  ASSERT_TRUE(grants.ok()) << grants.status().ToString();
  const auto objects = Column(*grants, 2);
  EXPECT_TRUE(objects.count("rbac_set_role_test.main.t_a"));
  EXPECT_FALSE(objects.count("rbac_set_role_test.main.t_b"));
  // Another session of the same user is not narrowed.
  Client other(GetPort(), "sam", "user", next_session_++);
  ExpectReads(other, "t_b");
  for (const char* back : {"RESET ROLE", "SET ROLE ALL", "SET ROLE NONE"}) {
    ASSERT_EQ(Error(*sam_, "SET ROLE ra"), "");
    ExpectRefused(*sam_, "t_b");
    ASSERT_EQ(Error(*sam_, back), "") << back;
    ExpectReads(*sam_, "t_b");
  }
}

TEST_F(RbacSetRoleFixture, AnActiveRoleBringsWhatItInherits) {
  ASSERT_EQ(Error(*sam_, "SET ROLE rc"), "");
  ExpectReads(*sam_, "t_a");  // rc holds ra
  ExpectRefused(*sam_, "t_b");
}

TEST_F(RbacSetRoleFixture, OnlyARoleTheUserHolds) {
  const auto not_held = Error(*sam_, "SET ROLE spare");
  EXPECT_NE(not_held.find("does not hold role spare"), std::string::npos) << not_held;
  // A role that does not exist reads the same.
  const auto missing = Error(*sam_, "SET ROLE no_such_role");
  EXPECT_NE(missing.find("does not hold role no_such_role"), std::string::npos) << missing;
  // A refused SET ROLE changes nothing.
  ExpectReads(*sam_, "t_b");
  // The administrator has nothing to narrow to.
  const auto admin = Error(*admin_, "SET ROLE ra");
  EXPECT_NE(admin.find("the administrator holds every right"), std::string::npos) << admin;
}

TEST_F(RbacSetRoleFixture, RevokingTheActiveRoleLeavesNothingUntilAnotherIsSet) {
  ASSERT_EQ(Error(*admin_, "CREATE ROLE temp_role"), "");
  ASSERT_EQ(Error(*admin_, "GRANT SELECT ON t_a TO temp_role"), "");
  ASSERT_EQ(Error(*admin_, "GRANT temp_role TO sam"), "");
  ASSERT_EQ(Error(*sam_, "SET ROLE temp_role"), "");
  ExpectReads(*sam_, "t_a");
  ASSERT_EQ(Error(*admin_, "REVOKE temp_role FROM sam"), "");
  const auto error = Error(*sam_, "SELECT * FROM t_pub");
  EXPECT_NE(error.find("no longer holds role temp_role"), std::string::npos) << error;
  ASSERT_EQ(Error(*sam_, "RESET ROLE"), "");
  ExpectReads(*sam_, "t_a");
  ASSERT_EQ(Error(*admin_, "DROP ROLE temp_role"), "");
}

TEST_F(RbacSetRoleFixture, PreparedStatementsFollowTheActiveRole) {
  auto read_b = sam_->Prepare("SELECT * FROM t_b");
  ASSERT_TRUE(read_b.ok()) << read_b.status().ToString();
  auto read_a = sam_->Prepare("SELECT * FROM t_a");
  ASSERT_TRUE(read_a.ok()) << read_a.status().ToString();
  ASSERT_TRUE(sam_->ExecutePrepared(**read_b).ok());
  ASSERT_EQ(Error(*sam_, "SET ROLE ra"), "");
  auto narrowed = sam_->ExecutePrepared(**read_b);
  ASSERT_FALSE(narrowed.ok()) << "a statement prepared before SET ROLE kept the wider rights";
  EXPECT_NE(narrowed.status().ToString().find("no SELECT privilege on table"), std::string::npos)
      << narrowed.status().ToString();
  EXPECT_TRUE(sam_->ExecutePrepared(**read_a).ok());  // positive twin
  ASSERT_EQ(Error(*sam_, "RESET ROLE"), "");
  EXPECT_TRUE(sam_->ExecutePrepared(**read_b).ok());
}

TEST_F(RbacSetRoleFixture, PoliciesSeeTheActiveRole) {
  auto all = sam_->Run("SELECT count(*) FROM t_rows");
  ASSERT_TRUE(all.ok()) << all.status().ToString();
  EXPECT_EQ(Column(*all), (std::set<std::string>{"2"}));
  ASSERT_EQ(Error(*sam_, "SET ROLE ra"), "");
  auto narrowed = sam_->Run("SELECT count(*) FROM t_rows");
  ASSERT_TRUE(narrowed.ok()) << narrowed.status().ToString();
  EXPECT_EQ(Column(*narrowed), (std::set<std::string>{"0"})) << "is_role_active('rb') was true";
  ASSERT_EQ(Error(*sam_, "SET ROLE rb"), "");
  auto rb = sam_->Run("SELECT count(*) FROM t_rows");
  ASSERT_TRUE(rb.ok()) << rb.status().ToString();
  EXPECT_EQ(Column(*rb), (std::set<std::string>{"2"}));
}

TEST_F(RbacSetRoleFixture, ATenantsUserNamesItsRolesWithoutTheTenant) {
  Client ann(GetPort(), "acme/ann", "user", next_session_++);
  EXPECT_EQ(Error(ann, "SET ROLE r"), "");
  EXPECT_EQ(Error(ann, "SET ROLE acme/r"), "");
  const auto other = Error(ann, "SET ROLE ra");  // a server-wide role it does not hold
  EXPECT_NE(other.find("does not hold role ra"), std::string::npos) << other;
}
