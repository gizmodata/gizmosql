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

// Scoped tokens for agents and automation: a token a user mints for a program
// that may only ever do less than the user (some verbs, some catalogs, for a
// while), never more. A token minted under a scoped token is narrower again,
// and revoking a token revokes the ones minted from it. Needs a license that
// includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "enterprise/enterprise_features.h"
#include "enterprise/rbac/rbac_model.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kSecret = "test_secret_key_for_testing";

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-scoped-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-scoped-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{kSecret});
}

class Client {
 public:
  // A program logging in with a scoped token, as user "token" (like a login
  // token from an identity provider); the login happens on first use.
  Client(int port, std::string scoped_token) : scoped_token_(std::move(scoped_token)) {
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    flight_ = arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie();
  }
  static std::unique_ptr<Client> As(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-9100-%012d", n);
    auto c = std::make_unique<Client>(port, "");
    c->header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    return c;
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
    if (header_.first.empty()) {
      ARROW_ASSIGN_OR_RAISE(header_, flight_->AuthenticateBasicToken({}, "token", scoped_token_));
    }
    if (!client_) client_ = std::make_unique<FlightSqlClient>(std::move(flight_));
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(co, sql));
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
  std::string Error(const std::string& sql) {
    auto st = Run(sql).status();
    return st.ok() ? "" : st.ToString();
  }

 private:
  std::string scoped_token_;
  std::unique_ptr<arrow::flight::FlightClient> flight_;
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

// The value in the named column of the first row.
std::string Cell(const std::shared_ptr<arrow::Table>& t, const std::string& column) {
  auto c = t->GetColumnByName(column);
  if (!c || t->num_rows() == 0) return "";
  return c->GetScalar(0).ValueOrDie()->ToString();
}

class RbacScopedTokensFixture : public gizmosql::testing::ServerTestFixture<RbacScopedTokensFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_scoped_tokens_test.db",
        .port = 31810,
        .health_port = 31811,
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
    admin_ = Client::As(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {
               "ATTACH ':memory:' AS sales_db", "ATTACH ':memory:' AS hr_db",
               "CREATE TABLE sales_db.main.orders AS SELECT 1 AS id, 10 AS amount",
               "CREATE TABLE hr_db.main.staff AS SELECT 1 AS id, 'x' AS name",
               "CREATE ROLE worker",
               "GRANT SELECT, INSERT, UPDATE, DELETE ON CATALOG sales_db TO worker",
               "GRANT SELECT ON CATALOG hr_db TO worker",
               "GRANT CREATE TABLE ON SCHEMA sales_db.main TO worker",
               "CREATE USER wes", "GRANT worker TO wes", "CREATE USER ula",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    wes_ = Client::As(GetPort(), "wes", "user", next_session_++);
  }

  // Mint a token as `who`; returns the token, or fails the test.
  std::string Mint(Client& who, const std::string& sql) {
    auto result = who.Run(sql);
    EXPECT_TRUE(result.ok()) << sql << " -> " << result.status().ToString();
    if (!result.ok()) return "";
    const auto token = Cell(*result, "token");
    EXPECT_FALSE(token.empty()) << sql;
    return token;
  }

  static inline int next_session_ = 10;
  std::unique_ptr<Client> admin_, wes_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacScopedTokensFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacScopedTokensFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacScopedTokensFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacScopedTokensFixture>::config_{};

TEST_F(RbacScopedTokensFixture, ATokenDoesOnlyWhatItAllows) {
  const auto token = Mint(*wes_, "CREATE SCOPED TOKEN reports ALLOW SELECT ON CATALOG sales_db");
  Client agent(GetPort(), token);
  EXPECT_EQ(agent.Error("SELECT * FROM sales_db.main.orders"), "");
  // wes may write, read hr_db and create tables; the token may not.
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"INSERT INTO sales_db.main.orders VALUES (2, 20)", "allows only SELECT"},
           {"DELETE FROM sales_db.main.orders", "allows only SELECT"},
           {"SELECT * FROM hr_db.main.staff", "only on catalog sales_db"},
           {"CREATE TABLE sales_db.main.planted AS SELECT 1", "scoped token reports"},
       }) {
    const auto error = agent.Error(sql);
    EXPECT_NE(error.find("GizmoSQL blocked"), std::string::npos) << sql << " -> " << error;
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  // Positive twins: wes himself.
  EXPECT_EQ(wes_->Error("SELECT * FROM hr_db.main.staff"), "");
  EXPECT_EQ(wes_->Error("INSERT INTO sales_db.main.orders VALUES (3, 30)"), "");
  EXPECT_EQ(wes_->Error("DELETE FROM sales_db.main.orders WHERE id = 3"), "");
}

TEST_F(RbacScopedTokensFixture, ATokenNeverDoesMoreThanItsUser) {
  // A token that allows writes on a catalog wes may only read stays read-only there.
  const auto token = Mint(*wes_, "CREATE SCOPED TOKEN writer ALLOW SELECT, INSERT ON CATALOG hr_db");
  Client agent(GetPort(), token);
  EXPECT_EQ(agent.Error("SELECT * FROM hr_db.main.staff"), "");
  const auto error = agent.Error("INSERT INTO hr_db.main.staff VALUES (2, 'y')");
  EXPECT_NE(error.find("has no INSERT privilege"), std::string::npos) << error;
  // A grant taken from the user is taken from the token.
  ASSERT_EQ(admin_->Error("REVOKE SELECT ON CATALOG hr_db FROM worker"), "");
  EXPECT_NE(agent.Error("SELECT * FROM hr_db.main.staff").find("has no SELECT privilege"),
            std::string::npos);
  ASSERT_EQ(admin_->Error("GRANT SELECT ON CATALOG hr_db TO worker"), "");
}

TEST_F(RbacScopedTokensFixture, ATokenCannotChangeAccessControl) {
  Client agent(GetPort(), Mint(*wes_, "CREATE SCOPED TOKEN quiet ALLOW SELECT"));
  for (const char* sql : {"GRANT worker TO ula", "CREATE ROLE sneaky", "ALTER USER wes DISABLE"}) {
    const auto error = agent.Error(sql);
    EXPECT_NE(error.find("a scoped token cannot change access control"), std::string::npos)
        << sql << " -> " << error;
  }
  for (const char* sql : {"SET GLOBAL threads = 2", "ATTACH ':memory:' AS planted"}) {
    EXPECT_NE(agent.Error(sql).find("GizmoSQL blocked"), std::string::npos) << sql;
  }
  // It can read its own grants.
  EXPECT_EQ(agent.Error("SHOW GRANTS"), "");
}

TEST_F(RbacScopedTokensFixture, AChildTokenIsNarrowerAndDiesWithItsParent) {
  const auto parent = Mint(*wes_, "CREATE SCOPED TOKEN parent ALLOW SELECT, INSERT ON CATALOG sales_db, hr_db");
  Client parent_agent(GetPort(), parent);
  // Wider than the parent: refused.
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"CREATE SCOPED TOKEN wider ALLOW SELECT, DELETE", "cannot allow DELETE"},
           {"CREATE SCOPED TOKEN elsewhere ALLOW SELECT ON CATALOG other_db", "cannot reach catalog other_db"},
           {"CREATE SCOPED TOKEN longer ALLOW SELECT VALID FOR '30 days'", "cannot outlast"},
           {"CREATE SCOPED TOKEN other ALLOW SELECT FOR USER ula", "only for the user"},
       }) {
    const auto error = parent_agent.Error(sql);
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  const auto child = Mint(parent_agent, "CREATE SCOPED TOKEN child ALLOW SELECT ON CATALOG sales_db");
  Client child_agent(GetPort(), child);
  EXPECT_EQ(child_agent.Error("SELECT * FROM sales_db.main.orders"), "");
  EXPECT_NE(child_agent.Error("SELECT * FROM hr_db.main.staff").find("only on catalog sales_db"),
            std::string::npos);
  // Revoking the parent ends the child's sessions too, and new ones.
  ASSERT_EQ(wes_->Error("DROP SCOPED TOKEN parent"), "");
  for (Client* c : {&parent_agent, &child_agent}) {
    const auto error = c->Error("SELECT 1");
    EXPECT_NE(error, "") << "a revoked token still works";
  }
  Client fresh_child(GetPort(), child);
  const auto error = fresh_child.Error("SELECT 1");
  EXPECT_NE(error.find("revoked or has expired"), std::string::npos) << error;
}

TEST_F(RbacScopedTokensFixture, ExpiryAndBookkeeping) {
  const auto token = Mint(*wes_, "CREATE SCOPED TOKEN brief ALLOW SELECT VALID FOR '2 seconds'");
  Client agent(GetPort(), token);
  EXPECT_EQ(agent.Error("SELECT 1"), "");
  std::this_thread::sleep_for(std::chrono::seconds(4));
  Client later(GetPort(), token);
  EXPECT_NE(later.Error("SELECT 1"), "") << "an expired token still works";
  // A session that was open stops at the expiry too (its own token carries
  // the same expiry).
  const auto open_session = agent.Error("SELECT 1");
  EXPECT_NE(open_session.find("expired"), std::string::npos) << open_session;
  // SHOW SCOPED TOKENS lists a user's tokens and never the token itself.
  auto shown = wes_->Run("SHOW SCOPED TOKENS");
  ASSERT_TRUE(shown.ok()) << shown.status().ToString();
  const auto text = shown->get()->ToString();
  EXPECT_NE(text.find("brief"), std::string::npos) << text;
  EXPECT_EQ(text.find(token.substr(0, 20)), std::string::npos) << "a token was shown";
  // Others do not see wes's tokens; the administrator does.
  auto ula = Client::As(GetPort(), "ula", "user", next_session_++);
  auto ula_shown = ula->Run("SHOW SCOPED TOKENS");
  ASSERT_TRUE(ula_shown.ok());
  EXPECT_EQ(ula_shown->get()->ToString().find("brief"), std::string::npos);
  auto all = admin_->Run("SHOW SCOPED TOKENS");
  ASSERT_TRUE(all.ok());
  EXPECT_NE(all->get()->ToString().find("brief"), std::string::npos);
  // Mistakes.
  for (const auto& [who, sql, why] : std::vector<std::tuple<Client*, std::string, std::string>>{
           {wes_.get(), "CREATE SCOPED TOKEN brief ALLOW SELECT", "already has a scoped token named brief"},
           {wes_.get(), "CREATE SCOPED TOKEN x ALLOW SELECT VALID FOR '1 year'", "at most 30 days"},
           {wes_.get(), "CREATE SCOPED TOKEN x ALLOW SELECT VALID FOR 'soon'", "VALID FOR"},
           {wes_.get(), "CREATE SCOPED TOKEN x ALLOW CREATE TABLE", "SELECT, INSERT, UPDATE or DELETE"},
           {wes_.get(), "CREATE SCOPED TOKEN x ALLOW SELECT FOR USER ula", "only for yourself"},
           {admin_.get(), "CREATE SCOPED TOKEN x ALLOW SELECT", "FOR USER"},
           {wes_.get(), "DROP SCOPED TOKEN no_such", "no scoped token named no_such"},
       }) {
    const auto error = who->Error(sql);
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  // The administrator mints for a user, and can revoke it.
  const auto for_ula = Mint(*admin_, "CREATE SCOPED TOKEN ops FOR USER ula ALLOW SELECT");
  Client ula_agent(GetPort(), for_ula);
  EXPECT_EQ(ula_agent.Error("SELECT 1"), "");
  ASSERT_EQ(admin_->Error("DROP SCOPED TOKEN ops FOR USER ula"), "");
  EXPECT_NE(ula_agent.Error("SELECT 1"), "");
}

TEST_F(RbacScopedTokensFixture, ADisabledUsersTokensStopWorking) {
  ASSERT_EQ(admin_->Error("CREATE USER vic"), "");
  ASSERT_EQ(admin_->Error("GRANT worker TO vic"), "");
  auto vic = Client::As(GetPort(), "vic", "user", next_session_++);
  const auto token = Mint(*vic, "CREATE SCOPED TOKEN job ALLOW SELECT");
  ASSERT_EQ(admin_->Error("ALTER USER vic DISABLE"), "");
  Client agent(GetPort(), token);
  EXPECT_NE(agent.Error("SELECT 1"), "");
  // A token whose signature was tampered with is not a token.
  Client bad(GetPort(), token.substr(0, token.size() - 4) + "AAAA");
  EXPECT_NE(bad.Error("SELECT 1"), "");
}

// A token whose parent is gone (dropped while the child was being minted, say)
// no longer stands, nor one whose parent has expired.
TEST(RbacScopedTokenModel, ATokenStandsOnlyWhileEveryParentDoes) {
  using namespace gizmosql::enterprise::rbac;
  const int64_t now = 1'000'000;
  auto token = [](std::string id, std::string parent, int64_t expires) {
    ScopedTokenRecord t;
    t.id = std::move(id);
    t.name = t.id;
    t.user_id = "u1";
    t.parent_id = std::move(parent);
    t.privileges = {Privilege::kSelect};
    t.expires_at = expires;
    return t;
  };
  auto snapshot = [&](std::vector<ScopedTokenRecord> tokens) {
    return Snapshot(1, {UserRecord{"u1", "wes", std::nullopt, true, {}}}, {}, {}, {}, {}, {}, {}, {},
                    {}, {}, {}, {}, {}, std::move(tokens));
  };
  const auto whole = snapshot({token("parent", "", now + 100), token("child", "parent", now + 50)});
  EXPECT_TRUE(whole.ValidScope("child", "u1", now).has_value());
  EXPECT_FALSE(whole.ValidScope("child", "someone-else", now).has_value());
  EXPECT_FALSE(whole.ValidScope("child", "u1", now + 60).has_value()) << "expired";
  const auto orphan = snapshot({token("child", "parent", now + 50)});
  EXPECT_FALSE(orphan.ValidScope("child", "u1", now).has_value()) << "its parent is gone";
  const auto stale_parent =
      snapshot({token("parent", "", now - 1), token("child", "parent", now + 50)});
  EXPECT_FALSE(stale_parent.ValidScope("child", "u1", now).has_value()) << "its parent expired";
  // A loop of parents (never written by GizmoSQL) ends instead of spinning.
  const auto loop = snapshot({token("a", "b", now + 50), token("b", "a", now + 50)});
  EXPECT_FALSE(loop.ValidScope("a", "u1", now).has_value());
}
