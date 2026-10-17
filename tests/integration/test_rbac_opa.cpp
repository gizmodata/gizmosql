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

// An external policy service (Open Policy Agent's data API) that can refuse
// what grants allow, never allow what they refuse, and fails closed. A small
// HTTP server here plays OPA. Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <httplib.h>
#include <mutex>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "enterprise/enterprise_features.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr int kOpaPort = 31822;

// Plays OPA: POST /v1/data/gizmosql/allow with {"input": ...} -> {"result": bool}.
class FakeOpa {
 public:
  enum class Mode { kPolicy, kError500, kNotABool, kSlow };

  FakeOpa() {
    server_.Post("/v1/data/gizmosql/allow", [this](const httplib::Request& req,
                                                   httplib::Response& res) {
      const auto body = nlohmann::json::parse(req.body, nullptr, /*allow_exceptions=*/false);
      const auto input = body.is_object() && body.contains("input") ? body["input"] : nlohmann::json();
      {
        std::lock_guard<std::mutex> lock(mu_);
        if (input.value("action", "") == "statement") {
          ++statements_;
          last_statement_ = input;
        }
      }
      switch (mode_.load()) {
        case Mode::kError500:
          res.status = 500;
          res.set_content("boom", "text/plain");
          return;
        case Mode::kNotABool:
          res.set_content(R"({"result": "yes"})", "application/json");
          return;
        case Mode::kSlow:
          std::this_thread::sleep_for(std::chrono::seconds(3));
          res.set_content(R"({"result": true})", "application/json");
          return;
        case Mode::kPolicy:
          break;
      }
      bool allow = true;
      const std::string user = input.value("user", "");
      if (input.value("action", "") == "connect" && user == "mallory") allow = false;
      if (input.contains("objects")) {
        for (const auto& o : input["objects"]) {
          if (o.value("name", "") == "payroll" && user != "hr_bot") allow = false;
        }
      }
      res.set_content(nlohmann::json{{"result", allow}}.dump(), "application/json");
    });
    thread_ = std::thread([this] { server_.listen("127.0.0.1", kOpaPort); });
    server_.wait_until_ready();
  }
  ~FakeOpa() {
    server_.stop();
    if (thread_.joinable()) thread_.join();
  }
  void SetMode(Mode m) { mode_ = m; }
  int statements() {
    std::lock_guard<std::mutex> lock(mu_);
    return statements_;
  }
  nlohmann::json last_statement() {
    std::lock_guard<std::mutex> lock(mu_);
    return last_statement_;
  }

 private:
  httplib::Server server_;
  std::thread thread_;
  std::atomic<Mode> mode_{Mode::kPolicy};
  std::mutex mu_;
  int statements_ = 0;
  nlohmann::json last_statement_;
};

FakeOpa& Opa() {
  static FakeOpa opa;
  return opa;
}

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-opa-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-opa-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-9200-%012d", n);
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  std::string Error(const std::string& sql) {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    auto info = client_->Execute(co, sql);
    if (!info.ok()) return info.status().ToString();
    for (const auto& ep : (*info)->endpoints()) {
      auto reader = client_->DoGet(co, ep.ticket);
      if (!reader.ok()) return reader.status().ToString();
      auto table = (*reader)->ToTable();
      if (!table.ok()) return table.status().ToString();
    }
    return "";
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

class RbacOpaFixture : public gizmosql::testing::ServerTestFixture<RbacOpaFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    Opa();  // listening before the server starts
    // Read when the server starts; cleared in SetUp so no other suite sees it.
#ifdef _WIN32
    _putenv_s("GIZMOSQL_RBAC_OPA_URL", "http://127.0.0.1:31822/v1/data/gizmosql/allow");
    _putenv_s("GIZMOSQL_RBAC_OPA_TIMEOUT_MS", "800");
#else
    setenv("GIZMOSQL_RBAC_OPA_URL", "http://127.0.0.1:31822/v1/data/gizmosql/allow", 1);
    setenv("GIZMOSQL_RBAC_OPA_TIMEOUT_MS", "800", 1);
#endif
    return {
        .database_filename = "rbac_opa_test.db",
        .port = 31820,
        .health_port = 31821,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  void SetUp() override {
#ifdef _WIN32
    _putenv_s("GIZMOSQL_RBAC_OPA_URL", "");
    _putenv_s("GIZMOSQL_RBAC_OPA_TIMEOUT_MS", "");
#else
    unsetenv("GIZMOSQL_RBAC_OPA_URL");
    unsetenv("GIZMOSQL_RBAC_OPA_TIMEOUT_MS");
#endif
    if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
      GTEST_SKIP() << "license does not include RBAC";
    }
    ASSERT_TRUE(IsServerReady());
    Opa().SetMode(FakeOpa::Mode::kPolicy);
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {"CREATE TABLE orders AS SELECT 1 AS id, 10 AS amount",
                              "CREATE TABLE payroll AS SELECT 1 AS id, 5000 AS salary",
                              "CREATE TABLE secrets AS SELECT 1 AS id", "CREATE ROLE reader",
                              "GRANT SELECT ON orders TO reader", "GRANT SELECT ON payroll TO reader",
                              "CREATE USER ann", "CREATE USER hr_bot", "CREATE USER mallory",
                              "GRANT reader TO ann", "GRANT reader TO hr_bot",
                              "GRANT reader TO mallory"}) {
        ASSERT_EQ(admin_->Error(sql), "") << sql;
      }
      seeded = true;
    }
  }

  static inline int next_session_ = 10;
  std::unique_ptr<Client> admin_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacOpaFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacOpaFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacOpaFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig gizmosql::testing::ServerTestFixture<RbacOpaFixture>::config_{};

TEST_F(RbacOpaFixture, TheServiceSeesWhatAnAllowedStatementTouches) {
  Client ann(GetPort(), "ann", "user", next_session_++);
  ASSERT_EQ(ann.Error("SELECT id FROM orders"), "");
  const auto input = Opa().last_statement();
  EXPECT_EQ(input.value("user", ""), "ann");
  ASSERT_TRUE(input.contains("roles") && input["roles"].is_array()) << input.dump();
  EXPECT_NE(input["roles"].dump().find("reader"), std::string::npos) << input.dump();
  ASSERT_TRUE(input.contains("objects") && input["objects"].size() == 1) << input.dump();
  const auto& o = input["objects"][0];
  EXPECT_EQ(o.value("name", ""), "orders");
  EXPECT_EQ(o.value("schema", ""), "main");
  EXPECT_EQ(o.value("type", ""), "table");
  EXPECT_EQ(o.value("privilege", ""), "SELECT");
  EXPECT_EQ(o["columns"].dump(), R"(["id"])");
}

TEST_F(RbacOpaFixture, TheServiceCanRefuseWhatGrantsAllow) {
  Client ann(GetPort(), "ann", "user", next_session_++);
  const auto error = ann.Error("SELECT * FROM payroll");
  EXPECT_NE(error.find("the access policy service denied it"), std::string::npos) << error;
  // Positive twin: the service allows hr_bot.
  Client hr(GetPort(), "hr_bot", "user", next_session_++);
  EXPECT_EQ(hr.Error("SELECT * FROM payroll"), "");
}

TEST_F(RbacOpaFixture, TheServiceNeverAllowsWhatGrantsRefuse) {
  Client ann(GetPort(), "ann", "user", next_session_++);
  const int before = Opa().statements();
  const auto error = ann.Error("SELECT * FROM secrets");
  EXPECT_NE(error.find("no SELECT privilege on table"), std::string::npos) << error;
  EXPECT_EQ(Opa().statements(), before) << "a statement the grants refused was sent to the service";
}

TEST_F(RbacOpaFixture, NoAnswerMeansNo) {
  Client ann(GetPort(), "ann", "user", next_session_++);
  ASSERT_EQ(ann.Error("SELECT * FROM orders"), "");
  for (auto mode : {FakeOpa::Mode::kError500, FakeOpa::Mode::kNotABool, FakeOpa::Mode::kSlow}) {
    Opa().SetMode(mode);
    const auto error = ann.Error("SELECT * FROM orders");
    EXPECT_NE(error.find("could not get an answer from the access policy service"), std::string::npos)
        << static_cast<int>(mode) << " -> " << error;
  }
  // The administrator is not held up by the service (a way in when it is down).
  EXPECT_EQ(admin_->Error("SELECT * FROM orders"), "");
  Opa().SetMode(FakeOpa::Mode::kPolicy);
  EXPECT_EQ(ann.Error("SELECT * FROM orders"), "");
}

TEST_F(RbacOpaFixture, TheServiceCanRefuseASession) {
  Client mallory(GetPort(), "mallory", "user", next_session_++);
  const auto error = mallory.Error("SELECT 1");
  EXPECT_NE(error.find("the access policy service refused the session"), std::string::npos) << error;
  Client ann(GetPort(), "ann", "user", next_session_++);
  EXPECT_EQ(ann.Error("SELECT 1"), "");
}
