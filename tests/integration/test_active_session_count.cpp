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

#include <gtest/gtest.h>

#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "duckdb_server.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Fixture for Active Session Count Tests
// ============================================================================

class ActiveSessionCountFixture
    : public gizmosql::testing::ServerTestFixture<ActiveSessionCountFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "active_session_count_test.db",
        .port = 31395,
        .health_port = 31396,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<ActiveSessionCountFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<ActiveSessionCountFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<ActiveSessionCountFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<ActiveSessionCountFixture>::config_{};

// ============================================================================
// Tests
// ============================================================================

TEST_F(ActiveSessionCountFixture, InitialCountIsZero) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto duckdb_server = std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_);
  ASSERT_NE(duckdb_server, nullptr);
  EXPECT_EQ(duckdb_server->GetActiveSessionCount(), 0u);
}

TEST_F(ActiveSessionCountFixture, CountIncreasesAfterClientConnect) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  auto duckdb_server = std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_);
  ASSERT_NE(duckdb_server, nullptr);

  // Connect a client and authenticate
  arrow::flight::FlightClientOptions flight_opts;
  auto loc_result = arrow::flight::Location::ForGrpcTcp("localhost", GetPort());
  ASSERT_TRUE(loc_result.ok()) << loc_result.status().ToString();
  auto connect_result = arrow::flight::FlightClient::Connect(*loc_result, flight_opts);
  ASSERT_TRUE(connect_result.ok()) << connect_result.status().ToString();
  auto flight_client = std::move(*connect_result);

  auto auth_result = flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  ASSERT_TRUE(auth_result.ok()) << auth_result.status().ToString();

  // Execute a query to establish a session
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(*auth_result);
  FlightSqlClient sql_client(std::move(flight_client));
  auto exec_result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_TRUE(exec_result.ok()) << exec_result.status().ToString();

  // After executing a query, session count should be at least 1
  EXPECT_GE(duckdb_server->GetActiveSessionCount(), 1u);
}
