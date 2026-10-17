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

// Identity-provider groups to roles: CREATE GROUP MAPPING gives a store user a
// role while their login token lists the group. The user must exist in the
// store; a group never creates one. Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <future>
#include <optional>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "enterprise/enterprise_features.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

using arrow::flight::sql::FlightSqlClient;

namespace gizmosql {
// gizmosql_security.h (which needs Boost) declares it.
std::optional<std::string> IdentityClaimJson(
    const jwt::decoded_jwt<jwt::traits::kazuho_picojson>& token);
}  // namespace gizmosql

namespace {

constexpr const char* kIssuer = "https://idp.example.test";

// A GizmoSQL session token, with the "idp" claim GizmoSQL copies from a
// login token that lists groups.
std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id, const std::vector<std::string>& groups,
                      const std::string& issuer) {
  auto builder = jwt::create()
                     .set_issuer("gizmosql")
                     .set_type("JWT")
                     .set_id("rbac-groups-" + session_id)
                     .set_issued_at(std::chrono::system_clock::now())
                     .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
                     .set_payload_claim("sub", jwt::claim(username))
                     .set_payload_claim("role", jwt::claim(role))
                     .set_payload_claim("auth_method", jwt::claim(std::string("BootstrapToken")))
                     .set_payload_claim("instance_id", jwt::claim(std::string("rbac-groups-instance")))
                     .set_payload_claim("session_id", jwt::claim(session_id));
  if (!groups.empty()) {
    picojson::array list;
    for (const auto& g : groups) list.emplace_back(g);
    picojson::object idp;
    idp["iss"] = picojson::value(issuer);
    idp["groups"] = picojson::value(list);
    builder.set_payload_claim("idp", jwt::claim(picojson::value(idp)));
  }
  return builder.sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, int n, const std::vector<std::string>& groups = {},
         const std::string& issuer = kIssuer, const std::string& role = "user") {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-8000-%012d", n);
    session_id_ = sid;
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid, groups, issuer)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
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
  const std::string& session_id() const { return session_id_; }

 private:
  std::string session_id_;
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

std::string ErrorOf(const arrow::Status& st) { return st.ok() ? "" : st.ToString(); }

class RbacGroupMappingFixture : public gizmosql::testing::ServerTestFixture<RbacGroupMappingFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_group_mapping_test.db",
        .port = 31800,
        .health_port = 31801,
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
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", 1, std::vector<std::string>{},
                                      kIssuer, "admin");
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {
               "CREATE TABLE sales AS SELECT 1 AS id, 100 AS amount",
               "CREATE TABLE payroll AS SELECT 1 AS id, 5000 AS salary",
               "CREATE ROLE analyst", "CREATE ROLE hr", "GRANT SELECT ON sales TO analyst",
               "GRANT SELECT ON payroll TO hr", "CREATE USER gina", "CREATE USER hank",
               "CREATE GROUP MAPPING 'okta-analysts' ISSUER 'https://idp.example.test' TO ROLE analyst",
               "CREATE GROUP MAPPING 'hr-team' TO ROLE hr",
               "CREATE TENANT acme", "CREATE USER acme/amy", "CREATE ROLE acme/r",
               "CREATE GROUP MAPPING 'acme-staff' TO ROLE acme/r",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
  }

  std::string Error(Client& who, const std::string& sql) { return ErrorOf(who.Run(sql).status()); }
  static void ExpectRefused(const std::string& error, const std::string& table) {
    EXPECT_NE(error.find("no SELECT privilege on table"), std::string::npos)
        << table << " -> " << (error.empty() ? "READ ALLOWED" : error);
  }

  static inline int next_session_ = 10;
  std::unique_ptr<Client> admin_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacGroupMappingFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacGroupMappingFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacGroupMappingFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacGroupMappingFixture>::config_{};

TEST_F(RbacGroupMappingFixture, AGroupInTheTokenGivesItsRole) {
  Client gina(GetPort(), "gina", next_session_++, {"okta-analysts", "unrelated"});
  EXPECT_EQ(Error(gina, "SELECT * FROM sales"), "");
  ExpectRefused(Error(gina, "SELECT * FROM payroll"), "payroll");
  // The same user without the group: nothing.
  Client plain(GetPort(), "gina", next_session_++);
  ExpectRefused(Error(plain, "SELECT * FROM sales"), "sales");
  // The group from another issuer does not count for a mapping that names one...
  Client elsewhere(GetPort(), "gina", next_session_++, {"okta-analysts"}, "https://other.example");
  ExpectRefused(Error(elsewhere, "SELECT * FROM sales"), "sales");
  // ...but does for a mapping that names none.
  Client hr(GetPort(), "hank", next_session_++, {"hr-team"}, "https://other.example");
  EXPECT_EQ(Error(hr, "SELECT * FROM payroll"), "");
  // Group names are exact.
  Client upper(GetPort(), "gina", next_session_++, {"OKTA-ANALYSTS"});
  ExpectRefused(Error(upper, "SELECT * FROM sales"), "sales");
  // A mapped role can be the active one.
  EXPECT_EQ(Error(gina, "SET ROLE analyst"), "");
  EXPECT_EQ(Error(gina, "SELECT * FROM sales"), "");
}

TEST_F(RbacGroupMappingFixture, AGroupNeverCreatesAUser) {
  Client stranger(GetPort(), "not_in_store", next_session_++, {"okta-analysts"});
  const auto error = Error(stranger, "SELECT 1");
  EXPECT_NE(error, "") << "a user missing from the store got in through a group";
  EXPECT_EQ(error.find("SELECT 1"), std::string::npos) << error;
}

TEST_F(RbacGroupMappingFixture, MappedRolesStayInTheirTenant) {
  // acme/r is acme's: a server-wide user with the group does not get it.
  Client outsider(GetPort(), "gina", next_session_++, {"acme-staff"});
  auto grants = outsider.Run("SHOW GRANTS");
  ASSERT_TRUE(grants.ok()) << grants.status().ToString();
  EXPECT_EQ(grants->get()->ToString().find("acme/r"), std::string::npos) << grants->get()->ToString();
  // acme's user does; and a server-wide role mapped to a group it lists does not.
  Client amy(GetPort(), "acme/amy", next_session_++, {"acme-staff", "okta-analysts"});
  EXPECT_EQ(Error(amy, "SET ROLE r"), "");
  const auto cross = Error(amy, "SET ROLE analyst");
  EXPECT_NE(cross.find("does not hold role analyst"), std::string::npos) << cross;
}

TEST_F(RbacGroupMappingFixture, OnlyTheAdministratorManagesMappings) {
  Client gina(GetPort(), "gina", next_session_++, {"okta-analysts"});
  for (const char* sql : {"CREATE GROUP MAPPING 'x' TO ROLE analyst", "DROP GROUP MAPPING 'okta-analysts'",
                          "SHOW GROUP MAPPINGS"}) {
    const auto error = Error(gina, sql);
    EXPECT_NE(error.find("only administrators manage group mappings"), std::string::npos)
        << sql << " -> " << error;
  }
  auto mappings = admin_->Run("SHOW GROUP MAPPINGS");
  ASSERT_TRUE(mappings.ok()) << mappings.status().ToString();
  const auto text = mappings->get()->ToString();
  EXPECT_NE(text.find("okta-analysts"), std::string::npos) << text;
  EXPECT_NE(text.find("(any)"), std::string::npos) << text;
  // Mistakes are refused with a reason.
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"CREATE GROUP MAPPING 'okta-analysts' ISSUER 'https://idp.example.test' TO ROLE analyst",
            "already mapped"},
           {"CREATE GROUP MAPPING 'g' TO ROLE no_such_role", "does not exist"},
           {"CREATE GROUP MAPPING 'g' TO ROLE PUBLIC", "everyone holds PUBLIC already"},
           {"CREATE GROUP MAPPING 'g'", "expected TO ROLE"},
           {"CREATE GROUP MAPPING '' TO ROLE analyst", "cannot be empty"},
           {"DROP GROUP MAPPING 'never-mapped'", "has no such mapping"},
       }) {
    const auto error = Error(*admin_, sql);
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
}

TEST_F(RbacGroupMappingFixture, DroppingAMappingTakesTheRoleAwayAtOnce) {
  ASSERT_EQ(Error(*admin_, "CREATE ROLE temp_reader"), "");
  ASSERT_EQ(Error(*admin_, "GRANT SELECT ON sales TO temp_reader"), "");
  ASSERT_EQ(Error(*admin_, "CREATE GROUP MAPPING 'temp-group' TO ROLE temp_reader"), "");
  Client hank(GetPort(), "hank", next_session_++, {"temp-group"});
  ASSERT_EQ(Error(hank, "SELECT * FROM sales"), "");
  // A statement running on the mapped role is stopped.
  Client runner(GetPort(), "hank", next_session_++, {"temp-group"});
  auto running = std::async(std::launch::async, [&] {
    return runner.Run("SELECT sum(s.amount * r.range) FROM sales s, range(1000000000000) r").status();
  });
  struct EndIfStillRunning {
    Client& admin;
    Client& runner;
    std::future<arrow::Status>& f;
    ~EndIfStillRunning() {
      if (f.valid() && f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        (void)admin.Run("KILL SESSION '" + runner.session_id() + "'");
        f.wait_for(std::chrono::seconds(60));
      }
    }
  } end_if_still_running{*admin_, runner, running};
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EQ(running.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  ASSERT_EQ(Error(*admin_, "DROP GROUP MAPPING 'temp-group' TO ROLE temp_reader"), "");
  ASSERT_EQ(running.wait_for(std::chrono::seconds(30)), std::future_status::ready)
      << "a statement kept running after its group mapping was dropped";
  EXPECT_FALSE(running.get().ok());
  ExpectRefused(Error(hank, "SELECT * FROM sales"), "sales");
  ASSERT_EQ(Error(*admin_, "DROP ROLE temp_reader"), "");
}

// The identity claim's issuer and groups.
std::pair<std::string, std::vector<std::string>> Parsed(const std::string& json) {
  picojson::value v;
  EXPECT_EQ(picojson::parse(v, json), "");
  const auto& obj = v.get<picojson::object>();
  std::vector<std::string> groups;
  for (const auto& g : obj.at("groups").get<picojson::array>()) groups.push_back(g.get<std::string>());
  return {obj.at("iss").get<std::string>(), groups};
}

TEST(RbacIdentityClaim, ReadsTheGroupsClaim) {
  auto decode = [](const auto& b) {
    return jwt::decode(b.sign(jwt::algorithm::hs256{"k"}));
  };
  auto base = [] { return jwt::create().set_issuer("https://idp.example.test"); };
  // No groups: no identity claim.
  EXPECT_FALSE(gizmosql::IdentityClaimJson(decode(base())).has_value());
  // An array (non-strings dropped), or one group as a string.
  picojson::array groups{picojson::value("a"), picojson::value(3.0), picojson::value("b")};
  auto list = gizmosql::IdentityClaimJson(
      decode(base().set_payload_claim("groups", jwt::claim(picojson::value(groups)))));
  ASSERT_TRUE(list.has_value());
  EXPECT_EQ(Parsed(*list), (std::pair<std::string, std::vector<std::string>>{
                             "https://idp.example.test", {"a", "b"}}));
  auto one = gizmosql::IdentityClaimJson(decode(base().set_payload_claim("groups", jwt::claim(std::string("solo")))));
  ASSERT_TRUE(one.has_value());
  EXPECT_EQ(Parsed(*one), (std::pair<std::string, std::vector<std::string>>{
                            "https://idp.example.test", {"solo"}}));
  // Another claim name, set by the operator.
  auto roles_claim = base().set_payload_claim("roles", jwt::claim(std::string("from-roles")));
  EXPECT_FALSE(gizmosql::IdentityClaimJson(decode(roles_claim)).has_value());
#ifdef _WIN32
  _putenv_s("GIZMOSQL_TOKEN_GROUPS_CLAIM", "roles");
#else
  setenv("GIZMOSQL_TOKEN_GROUPS_CLAIM", "roles", 1);
#endif
  auto renamed = gizmosql::IdentityClaimJson(decode(roles_claim));
#ifdef _WIN32
  _putenv_s("GIZMOSQL_TOKEN_GROUPS_CLAIM", "");
#else
  unsetenv("GIZMOSQL_TOKEN_GROUPS_CLAIM");
#endif
  ASSERT_TRUE(renamed.has_value());
  EXPECT_NE(renamed->find("from-roles"), std::string::npos);
}

// ---------------------------------------------------------------------------
// End to end: a login token from an identity provider (RS256, verified with
// its certificate) whose groups claim GizmoSQL carries into the session.
// ---------------------------------------------------------------------------

namespace {

struct IdpKeys {
  std::string public_pem, private_pem;
  std::string cert_path = "rbac_group_mapping_idp_pub.pem";
  IdpKeys() {
    EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
    EVP_PKEY_keygen_init(ctx);
    EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048);
    EVP_PKEY* pkey = nullptr;
    EVP_PKEY_keygen(ctx, &pkey);
    EVP_PKEY_CTX_free(ctx);
    auto pem = [&](bool is_private) {
      BIO* bio = BIO_new(BIO_s_mem());
      if (is_private) {
        PEM_write_bio_PrivateKey(bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
      } else {
        PEM_write_bio_PUBKEY(bio, pkey);
      }
      char* data = nullptr;
      const long len = BIO_get_mem_data(bio, &data);
      std::string out(data, len);
      BIO_free(bio);
      return out;
    };
    public_pem = pem(false);
    private_pem = pem(true);
    EVP_PKEY_free(pkey);
    std::ofstream(cert_path) << public_pem;
  }
};
IdpKeys& Keys() {
  static IdpKeys keys;
  return keys;
}

class RbacGroupLoginFixture : public gizmosql::testing::ServerTestFixture<RbacGroupLoginFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_group_login_test.db",
        .port = 31802,
        .health_port = 31803,
        .username = "admin_user",
        .password = "admin_pass",
        .token_allowed_issuer = kIssuer,
        .token_allowed_audience = "gizmosql-test",
        .token_signature_verify_cert_path = Keys().cert_path,
        .rbac_store_uri = "memory",
    };
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacGroupLoginFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacGroupLoginFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacGroupLoginFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacGroupLoginFixture>::config_{};

TEST_F(RbacGroupLoginFixture, TheLoginTokensGroupsReachTheSession) {
  if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
    GTEST_SKIP() << "license does not include RBAC";
  }
  ASSERT_TRUE(IsServerReady());
  auto connect = [&](const std::string& user, const std::string& password)
      -> arrow::Result<std::pair<std::unique_ptr<FlightSqlClient>, arrow::flight::FlightCallOptions>> {
    ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("127.0.0.1", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
    ARROW_ASSIGN_OR_RAISE(auto bearer, client->AuthenticateBasicToken({}, user, password));
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(bearer);
    return std::make_pair(std::make_unique<FlightSqlClient>(std::move(client)), co);
  };
  auto run = [](std::pair<std::unique_ptr<FlightSqlClient>, arrow::flight::FlightCallOptions>& c,
                const std::string& sql) -> arrow::Status {
    ARROW_ASSIGN_OR_RAISE(auto info, c.first->Execute(c.second, sql));
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, c.first->DoGet(c.second, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      (void)table;
    }
    return arrow::Status::OK();
  };
  ASSERT_OK_AND_ASSIGN(auto admin, connect("admin_user", "admin_pass"));
  for (const char* sql : {"CREATE TABLE sales AS SELECT 1 AS id", "CREATE ROLE analyst",
                          "GRANT SELECT ON sales TO analyst", "CREATE USER gina",
                          "CREATE GROUP MAPPING 'okta-analysts' ISSUER 'https://idp.example.test' "
                          "TO ROLE analyst"}) {
    ASSERT_TRUE(run(admin, sql).ok()) << sql;
  }
  auto login_token = [](const std::vector<std::string>& groups) {
    picojson::array list;
    for (const auto& g : groups) list.emplace_back(g);
    return jwt::create()
        .set_issuer(kIssuer)
        .set_type("JWT")
        .set_audience("gizmosql-test")
        .set_subject("gina")
        .set_issued_at(std::chrono::system_clock::now())
        .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{600})
        .set_payload_claim("role", jwt::claim(std::string("user")))
        .set_payload_claim("groups", jwt::claim(picojson::value(list)))
        .sign(jwt::algorithm::rs256("", Keys().private_pem, "", ""));
  };
  ASSERT_OK_AND_ASSIGN(auto with_group, connect("token", login_token({"okta-analysts"})));
  EXPECT_TRUE(run(with_group, "SELECT * FROM sales").ok()) << "the group's role did not apply";
  ASSERT_OK_AND_ASSIGN(auto without, connect("token", login_token({"other"})));
  const auto refused = run(without, "SELECT * FROM sales");
  ASSERT_FALSE(refused.ok());
  EXPECT_NE(refused.ToString().find("no SELECT privilege on table"), std::string::npos)
      << refused.ToString();
}
