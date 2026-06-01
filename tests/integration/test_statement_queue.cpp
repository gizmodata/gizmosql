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

// Enterprise integration tests for the statement-queue feature, exercised through
// a real Flight SQL client. The concurrency mechanism itself is unit-tested in
// test_admission_controller.cpp; these cover the end-to-end wiring: the
// SET gizmosql.bypass_queue handler (license + admin gating + value parsing) and
// that queries still execute when routed through the admission gate.
//
// Requires an enterprise license with the "statement_queue" feature (set
// GIZMOSQL_LICENSE_KEY_FILE); otherwise the tests skip.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

static bool IsEnterpriseLicenseAvailable() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

#define SKIP_IF_NO_LICENSE()                                                    \
  if (!IsEnterpriseLicenseAvailable()) {                                        \
    GTEST_SKIP() << "Enterprise license required for statement-queue tests. "   \
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable.";      \
  }

class StatementQueueServerFixture
    : public gizmosql::testing::ServerTestFixture<StatementQueueServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "statement_queue_tester.db",
        .port = 31360,
        .health_port = 31361,
        .username = "admin",
        .password = "admin",
        // Enable the queue so the admission gate engages for non-bypassed sessions.
        .max_concurrent_statements = 2,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::config_{};

namespace {

struct AdminClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

// Connect + Basic-authenticate as the configured (admin-role) user.
arrow::Result<AdminClient> ConnectAdmin(int port, const std::string& user,
                                        const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  arrow::flight::FlightClientOptions options;
  ARROW_ASSIGN_OR_RAISE(auto client,
                        arrow::flight::FlightClient::Connect(location, options));
  AdminClient ac;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, user, password));
  ac.call_options.headers.push_back(bearer);
  ac.sql_client = std::make_unique<FlightSqlClient>(std::move(client));
  return ac;
}

// Execute a statement and fully drain it (GetFlightInfo -> DoGet -> ToTable). For
// SET commands this forces HandleGizmoSQLSet() to run, so its success/error
// surfaces here rather than being deferred.
arrow::Status RunStatement(AdminClient& ac, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, ac.sql_client->Execute(ac.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, ac.sql_client->DoGet(ac.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable());
  }
  return arrow::Status::OK();
}

}  // namespace

// An admin session may toggle bypass_queue on and off.
TEST_F(StatementQueueServerFixture, AdminCanToggleBypassQueue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = true"));
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = false"));
  // Case/format tolerance handled by the parser.
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = 1"));
}

// A non-boolean value is rejected.
TEST_F(StatementQueueServerFixture, BypassQueueRejectsInvalidValue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  auto status = RunStatement(ac, "SET SESSION gizmosql.bypass_queue = 'maybe'");
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("bypass_queue"), std::string::npos)
      << "Expected an 'Invalid value for bypass_queue' error, got: " << status.ToString();
}

// With the queue enabled (limit=2) and the admin session opted IN to the queue
// (bypass disabled), queries still execute correctly through the admission gate.
TEST_F(StatementQueueServerFixture, QueriesExecuteThroughTheQueue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  // Opt this admin session into the queue so SELECTs traverse Acquire()/release().
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = false"));

  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(RunStatement(ac, "SELECT 42")) << "query " << i << " failed";
  }
}

// gizmosql_settings() is a composable, bind-parameterized table function: it can be
// filtered and ordered like any relation. (Not license-gated — it's a SQL rewrite.)
TEST_F(StatementQueueServerFixture, GizmoSqlSettingsIsComposable) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      ac.sql_client->Execute(
          ac.call_options,
          "SELECT name, scope, enterprise FROM gizmosql_settings() "
          "WHERE name LIKE 'gizmosql.max%' ORDER BY name"));
  std::shared_ptr<arrow::Table> table;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               ac.sql_client->DoGet(ac.call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
  }
  ASSERT_NE(table, nullptr);
  // The three gizmosql.max* settings: max_concurrent_statements, max_queue_wait,
  // max_queued_statements.
  EXPECT_EQ(table->num_rows(), 3);
  EXPECT_EQ(table->num_columns(), 3);
}
