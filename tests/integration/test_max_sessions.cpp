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
// New sessions beyond the cap are rejected with Flight UNAVAILABLE (same
// pattern as graceful-drain rejects). Existing sessions keep working.

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/types.h"
#include "arrow/testing/gtest_util.h"
#include "duckdb_server.h"
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

arrow::Status ExecuteOn(HeldClient& held, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, held.sql_client->Execute(held.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          held.sql_client->DoGet(held.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return arrow::Status::OK();
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
  auto r = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(r.ok()) << r.status().ToString();
}

TEST_F(MaxSessionsFixture, RejectsBeyondCapWithUnavailable) {
  ASSERT_TRUE(IsServerReady());

  auto duckdb_server =
      std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_);
  ASSERT_NE(duckdb_server, nullptr);

  auto c1 = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(c1.ok()) << c1.status().ToString();
  auto c2 = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(c2.ok()) << c2.status().ToString();
  EXPECT_EQ(duckdb_server->GetActiveSessionCount(), 2u);

  auto overflow = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_FALSE(overflow.ok()) << "third session should be rejected at max-sessions=2";
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

  auto c3 = ConnectAndExecute(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(c3.ok()) << c3.status().ToString();
}
