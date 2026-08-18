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

// Integration tests for --max-sessions / GIZMOSQL_MAX_SESSIONS.
// New non-admin sessions beyond the cap are rejected with Flight UNAVAILABLE
// (same pattern as graceful-drain rejects). Existing sessions keep working.
// Admin-role sessions skip the cap so an operator can still connect.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/types.h"
#include "arrow/testing/gtest_util.h"
#include "duckdb_server.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

struct HeldClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

arrow::Result<HeldClient> ConnectAndExecute(int port, const std::string& username,
                                            const std::string& password,
                                            const std::string& sql = "SELECT 1") {
  arrow::flight::FlightClientOptions opts;
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(location, opts));
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        flight_client->AuthenticateBasicToken({}, username, password));

  HeldClient held;
  held.call_options.headers.push_back(bearer);
  held.sql_client = std::make_unique<FlightSqlClient>(std::move(flight_client));
  ARROW_ASSIGN_OR_RAISE(auto info, held.sql_client->Execute(held.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          held.sql_client->DoGet(held.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return held;
}

// Username/password Basic auth always gets role "admin". Non-admin sessions
// (the ones the cap applies to) are minted JWTs with role "user".
// Session ids must be UUID hex: KILL SESSION only matches [0-9a-fA-F-]+, so a
// label like "max-sessions-user-N" is not recognized as a kill command.
std::string MintUserToken() {
  static std::atomic<int> n{0};
  std::ostringstream ss;
  ss << "aaaaaaaa-0000-4000-8000-" << std::hex << std::setw(12) << std::setfill('0')
     << ++n;
  const std::string session_id = ss.str();
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id(session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(std::string("analyst")))
      .set_payload_claim("role", jwt::claim(std::string("user")))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("max-sessions-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

arrow::Result<HeldClient> ConnectUserAndExecute(int port,
                                                const std::string& sql = "SELECT 1") {
  arrow::flight::FlightClientOptions opts;
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(location, opts));

  HeldClient held;
  held.call_options.headers.emplace_back("authorization", "Bearer " + MintUserToken());
  held.sql_client = std::make_unique<FlightSqlClient>(std::move(flight_client));
  ARROW_ASSIGN_OR_RAISE(auto info, held.sql_client->Execute(held.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          held.sql_client->DoGet(held.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return held;
}

arrow::Status ExecuteOn(HeldClient& held, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, held.sql_client->Execute(held.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          held.sql_client->DoGet(held.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return arrow::Status::OK();
}

void CloseHeld(HeldClient& held) {
  (void)held.sql_client->CloseSession(held.call_options,
                                      arrow::flight::CloseSessionRequest{});
}

arrow::Result<std::string> QueryScalarString(HeldClient& held, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, held.sql_client->Execute(held.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          held.sql_client->DoGet(held.call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto array = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
      return array->GetString(0);
    }
  }
  return arrow::Status::Invalid("no rows");
}

bool IsEnterpriseLicenseAvailable() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

}  // namespace

class MaxSessionsFixture
    : public gizmosql::testing::ServerTestFixture<MaxSessionsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "max_sessions_test.db",
        .port = 31420,
        .health_port = 31421,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        // Self-minted non-admin tokens use an arbitrary instance_id.
        .allow_cross_instance_tokens = true,
        .max_sessions = 2,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MaxSessionsFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<MaxSessionsFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MaxSessionsFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MaxSessionsFixture>::config_{};

TEST_F(MaxSessionsFixture, UnderCapSucceeds) {
  ASSERT_TRUE(IsServerReady());
  auto r = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(r.ok()) << r.status().ToString();
  CloseHeld(*r);
}

TEST_F(MaxSessionsFixture, RejectsBeyondCapWithUnavailable) {
  ASSERT_TRUE(IsServerReady());

  auto duckdb_server =
      std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_);
  ASSERT_NE(duckdb_server, nullptr);

  auto c1 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c1.ok()) << c1.status().ToString();
  auto c2 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c2.ok()) << c2.status().ToString();
  EXPECT_EQ(duckdb_server->GetActiveSessionCount(), 2u);

  auto overflow = ConnectUserAndExecute(GetPort());
  ASSERT_FALSE(overflow.ok()) << "third non-admin session should be rejected at max-sessions=2";
  const std::string err = overflow.status().ToString();
  EXPECT_NE(err.find("max-sessions"), std::string::npos) << err;
  EXPECT_NE(err.find("Unavailable"), std::string::npos) << err;

  // Existing session still works after the reject.
  ASSERT_TRUE(ExecuteOn(*c1, "SELECT 2").ok());

  // Close one session explicitly; a new client should be admitted.
  ASSERT_OK(c2->sql_client->CloseSession(c2->call_options,
                                         arrow::flight::CloseSessionRequest{})
                .status());
  EXPECT_LT(duckdb_server->GetActiveSessionCount(), 2u);

  auto c3 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c3.ok()) << c3.status().ToString();
  CloseHeld(*c1);
  CloseHeld(*c3);
}

TEST_F(MaxSessionsFixture, AdminBypassesCapAndKillFreesSlot) {
  ASSERT_TRUE(IsServerReady());

  auto c1 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c1.ok()) << c1.status().ToString();
  auto c2 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c2.ok()) << c2.status().ToString();

  auto overflow = ConnectUserAndExecute(GetPort());
  ASSERT_FALSE(overflow.ok()) << "non-admin should be rejected at cap";

  // Username/password Basic auth is role "admin" and must still get in.
  auto admin = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(admin.ok()) << admin.status().ToString();
  ASSERT_TRUE(ExecuteOn(*admin, "SELECT 1").ok());

  if (!IsEnterpriseLicenseAvailable()) {
    CloseHeld(*c1);
    CloseHeld(*c2);
    CloseHeld(*admin);
    GTEST_SKIP() << "Enterprise license required to prove KILL SESSION frees a slot. "
                 << "Set GIZMOSQL_LICENSE_KEY_FILE.";
  }

  auto session_id = QueryScalarString(*c1, "SELECT GIZMOSQL_CURRENT_SESSION()");
  ASSERT_TRUE(session_id.ok()) << session_id.status().ToString();

  ASSERT_TRUE(ExecuteOn(*admin, "KILL SESSION '" + *session_id + "'").ok());

  auto c3 = ConnectUserAndExecute(GetPort());
  ASSERT_TRUE(c3.ok()) << c3.status().ToString();
  CloseHeld(*admin);
  CloseHeld(*c2);
  CloseHeld(*c3);
}
