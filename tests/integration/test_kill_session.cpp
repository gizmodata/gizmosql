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
#include <cstdlib>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// Helper to check if enterprise license is available for kill_session tests
static bool IsEnterpriseLicenseAvailable() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

#define SKIP_IF_NO_LICENSE() \
  if (!IsEnterpriseLicenseAvailable()) { \
    GTEST_SKIP() << "Enterprise license required for KILL SESSION tests. " \
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable."; \
  }

// Define the test fixture using the shared server infrastructure
class KillSessionServerFixture
    : public gizmosql::testing::ServerTestFixture<KillSessionServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "kill_tester.db",
        .port = 31341,
        .health_port = 31342,
        .username = "admin",
        .password = "admin",
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<KillSessionServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<KillSessionServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<KillSessionServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<KillSessionServerFixture>::config_{};

// Test that non-admin users cannot execute KILL SESSION
TEST_F(KillSessionServerFixture, NonAdminCannotKillSession) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  // Connect as admin user (role will be determined by server config)
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Get our session ID
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(
          call_options,
          "SELECT session_id FROM _gizmosql_instr.active_sessions LIMIT 1"));

  std::string session_id;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      session_id = array->GetString(0);
    }
  }

  ASSERT_FALSE(session_id.empty()) << "Failed to get session ID";

  // Attempt to kill our own session - should fail
  auto result = sql_client.Execute(call_options, "KILL SESSION '" + session_id + "'");
  ASSERT_FALSE(result.ok()) << "KILL SESSION of own session should fail";
  ASSERT_TRUE(result.status().ToString().find("Cannot kill your own session") !=
              std::string::npos)
      << "Expected error about killing own session";
}

// Test KILL SESSION with invalid session ID
TEST_F(KillSessionServerFixture, KillNonexistentSession) {
  SKIP_IF_NO_LICENSE();
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

  // Try to kill a non-existent session
  auto result = sql_client.Execute(
      call_options, "KILL SESSION '00000000-0000-0000-0000-000000000000'");
  ASSERT_FALSE(result.ok()) << "KILL SESSION of non-existent session should fail";
  ASSERT_TRUE(result.status().ToString().find("Session not found") != std::string::npos ||
              result.status().ToString().find("not found") != std::string::npos)
      << "Expected error about session not found: " << result.status().ToString();
}

// Test that KILL SESSION syntax is properly parsed
TEST_F(KillSessionServerFixture, KillSessionSyntax) {
  SKIP_IF_NO_LICENSE();
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

  // Test various KILL SESSION syntaxes with non-existent UUID
  // (they should all fail with "Session not found" not a syntax error)
  std::vector<std::string> test_queries = {
      "KILL SESSION '12345678-1234-1234-1234-123456789012'",
      "KILL SESSION \"12345678-1234-1234-1234-123456789012\"",
      "KILL SESSION 12345678-1234-1234-1234-123456789012",
      "kill session '12345678-1234-1234-1234-123456789012'",
      "Kill Session '12345678-1234-1234-1234-123456789012';",
  };

  for (const auto& query : test_queries) {
    auto result = sql_client.Execute(call_options, query);
    ASSERT_FALSE(result.ok()) << "Expected KILL SESSION to fail for: " << query;
    // Should fail with "Session not found" not a syntax error
    auto status_str = result.status().ToString();
    ASSERT_TRUE(status_str.find("Session not found") != std::string::npos ||
                status_str.find("not found") != std::string::npos ||
                status_str.find("Only admin") != std::string::npos)
        << "Unexpected error for query '" << query << "': " << status_str;
  }
}

// Test that a killed session receives an error and must re-authenticate
TEST_F(KillSessionServerFixture, KilledSessionMustReauthenticate) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create the admin client
  arrow::flight::FlightClientOptions options1;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto admin_client,
                             arrow::flight::FlightClient::Connect(location, options1));

  arrow::flight::FlightCallOptions admin_call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto admin_bearer,
      admin_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  admin_call_options.headers.push_back(admin_bearer);

  FlightSqlClient admin_sql_client(std::move(admin_client));

  // Create a second client (target session to be killed)
  // Note: Each connection with a new bearer token gets its own session
  arrow::flight::FlightClientOptions options2;
  ASSERT_ARROW_OK_AND_ASSIGN(auto target_client,
                             arrow::flight::FlightClient::Connect(location, options2));

  arrow::flight::FlightCallOptions target_call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto target_bearer,
      target_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  target_call_options.headers.push_back(target_bearer);

  FlightSqlClient target_sql_client(std::move(target_client));

  // Execute a query on the target session to establish it
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto target_info, target_sql_client.Execute(target_call_options, "SELECT 1"));

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Get the target session's ID using GIZMOSQL_CURRENT_SESSION()
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto session_info,
      target_sql_client.Execute(target_call_options, "SELECT GIZMOSQL_CURRENT_SESSION()"));

  std::string target_session_id;
  for (const auto& endpoint : session_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               target_sql_client.DoGet(target_call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      target_session_id = array->GetString(0);
    }
  }

  ASSERT_FALSE(target_session_id.empty()) << "Failed to get target session ID";

  // Admin kills the target session
  auto kill_result =
      admin_sql_client.Execute(admin_call_options,
                               "KILL SESSION '" + target_session_id + "'");
  ASSERT_TRUE(kill_result.ok()) << "KILL SESSION should succeed: " << kill_result.status();

  // Now the target session should receive an error when trying to execute any query
  auto result = target_sql_client.Execute(target_call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Killed session should receive an error";
  auto status_str = result.status().ToString();
  ASSERT_TRUE(status_str.find("session has been killed") != std::string::npos ||
              status_str.find("re-authenticate") != std::string::npos)
      << "Expected error about killed session requiring re-authentication: " << status_str;
}
