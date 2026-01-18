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
#include <thread>
#include <chrono>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// Define the test fixture using the shared server infrastructure
class InstrumentationServerFixture
    : public gizmosql::testing::ServerTestFixture<InstrumentationServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "instr_tester.db",
        .port = 31339,
        .health_port = 31340,
        .username = "tester",
        .password = "tester",
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<InstrumentationServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<InstrumentationServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<InstrumentationServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<InstrumentationServerFixture>::config_{};

// Test that instrumentation records are created for sessions and statements
TEST_F(InstrumentationServerFixture, InstrumentationRecordsCreated) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  // Authenticate
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Execute a simple query
  ASSERT_ARROW_OK_AND_ASSIGN(auto info,
                             sql_client.Execute(call_options, "SELECT 1 AS test_col"));

  // Fetch results to complete the statement
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    ASSERT_EQ(table->num_rows(), 1);
  }

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Query the instrumentation database to verify records were created
  // Sessions should be visible
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto sessions_info,
      sql_client.Execute(call_options,
                         "SELECT * FROM _gizmosql_instr.sessions LIMIT 1"));

  bool found_session = false;
  for (const auto& endpoint : sessions_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      found_session = true;
    }
  }
  ASSERT_TRUE(found_session) << "Expected to find session instrumentation record";

  // Verify statements are tracked
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto statements_info,
      sql_client.Execute(call_options,
                         "SELECT * FROM _gizmosql_instr.sql_statements LIMIT 5"));

  bool found_statement = false;
  for (const auto& endpoint : statements_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      found_statement = true;
    }
  }
  ASSERT_TRUE(found_statement) << "Expected to find statement instrumentation records";
}

// Test that DETACH of instrumentation database is prevented
TEST_F(InstrumentationServerFixture, DetachInstrumentationPrevented) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Attempt to DETACH the instrumentation database - should fail
  auto result = sql_client.Execute(call_options, "DETACH _gizmosql_instr");
  ASSERT_FALSE(result.ok()) << "DETACH instrumentation should have been rejected";
  ASSERT_TRUE(result.status().ToString().find("Cannot DETACH") != std::string::npos)
      << "Expected error message about DETACH prevention";
}

// Test that modification of instrumentation database is prevented
TEST_F(InstrumentationServerFixture, ModifyInstrumentationPrevented) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Test DELETE - should fail
  auto delete_result =
      sql_client.Execute(call_options, "DELETE FROM _gizmosql_instr.sql_statements");
  ASSERT_FALSE(delete_result.ok()) << "DELETE from instrumentation should have been rejected";
  ASSERT_TRUE(delete_result.status().ToString().find("Cannot modify") != std::string::npos ||
              delete_result.status().ToString().find("read-only") != std::string::npos)
      << "Expected error message about modification prevention: "
      << delete_result.status().ToString();

  // Test INSERT - should fail
  auto insert_result = sql_client.Execute(
      call_options,
      "INSERT INTO _gizmosql_instr.sessions (session_id, instance_id, username, role, peer) "
      "VALUES ('00000000-0000-0000-0000-000000000000', "
      "'00000000-0000-0000-0000-000000000000', 'fake', 'fake', 'fake')");
  ASSERT_FALSE(insert_result.ok()) << "INSERT into instrumentation should have been rejected";
  ASSERT_TRUE(insert_result.status().ToString().find("Cannot modify") != std::string::npos ||
              insert_result.status().ToString().find("read-only") != std::string::npos)
      << "Expected error message about modification prevention: "
      << insert_result.status().ToString();

  // Test UPDATE - should fail
  auto update_result = sql_client.Execute(
      call_options, "UPDATE _gizmosql_instr.sessions SET username = 'hacked'");
  ASSERT_FALSE(update_result.ok()) << "UPDATE on instrumentation should have been rejected";
  ASSERT_TRUE(update_result.status().ToString().find("Cannot modify") != std::string::npos ||
              update_result.status().ToString().find("read-only") != std::string::npos)
      << "Expected error message about modification prevention: "
      << update_result.status().ToString();
}

// Test that active_sessions view works
TEST_F(InstrumentationServerFixture, ActiveSessionsView) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Query active sessions - our session should be visible
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(call_options, "SELECT * FROM _gizmosql_instr.active_sessions"));

  bool found_active = false;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      found_active = true;
    }
  }
  ASSERT_TRUE(found_active) << "Expected to find current session in active_sessions view";
}

// Test GIZMOSQL_CURRENT_SESSION() function returns current session ID
TEST_F(InstrumentationServerFixture, CurrentSessionFunction) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Use GIZMOSQL_CURRENT_SESSION() to get current session ID and verify it exists
  // in the sessions table
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(call_options,
                         "SELECT session_id FROM _gizmosql_instr.sessions "
                         "WHERE session_id = GIZMOSQL_CURRENT_SESSION()"));

  bool found_session = false;
  std::string session_id;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      found_session = true;
      // Extract the session ID to verify it's a valid UUID
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      session_id = array->GetString(0);
    }
  }
  ASSERT_TRUE(found_session)
      << "GIZMOSQL_CURRENT_SESSION() should return current session ID";
  ASSERT_FALSE(session_id.empty()) << "Session ID should not be empty";
  // Verify it looks like a UUID (36 chars with hyphens)
  ASSERT_EQ(session_id.length(), 36) << "Session ID should be a UUID";
}
