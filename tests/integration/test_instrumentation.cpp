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
#include <filesystem>
#include <algorithm>
#include <cstdio>

#include <duckdb.hpp>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"
#include "instrumentation/instrumentation_manager.h"

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
              delete_result.status().ToString().find("read-only") != std::string::npos ||
              delete_result.status().ToString().find("Access denied") != std::string::npos)
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
              insert_result.status().ToString().find("read-only") != std::string::npos ||
              insert_result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected error message about modification prevention: "
      << insert_result.status().ToString();

  // Test UPDATE - should fail
  auto update_result = sql_client.Execute(
      call_options, "UPDATE _gizmosql_instr.sessions SET username = 'hacked'");
  ASSERT_FALSE(update_result.ok()) << "UPDATE on instrumentation should have been rejected";
  ASSERT_TRUE(update_result.status().ToString().find("Cannot modify") != std::string::npos ||
              update_result.status().ToString().find("read-only") != std::string::npos ||
              update_result.status().ToString().find("Access denied") != std::string::npos)
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

  // Run a simple query first to ensure the session is fully established
  // This triggers session creation in the instrumentation database
  ASSERT_ARROW_OK_AND_ASSIGN(auto warmup_info,
                             sql_client.Execute(call_options, "SELECT 1"));
  for (const auto& endpoint : warmup_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK(reader->ToTable().status());
  }

  // Allow async write queue to flush instrumentation records
  // Use a longer wait for CI environments which may be slower
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

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

// Test GIZMOSQL_CURRENT_INSTANCE() function returns current server instance ID
TEST_F(InstrumentationServerFixture, CurrentInstanceFunction) {
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

  // First, test that GIZMOSQL_CURRENT_INSTANCE() returns a valid UUID
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto direct_info,
      sql_client.Execute(call_options, "SELECT GIZMOSQL_CURRENT_INSTANCE()"));

  std::string direct_instance_id;
  for (const auto& endpoint : direct_info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      direct_instance_id = array->GetString(0);
    }
  }
  ASSERT_FALSE(direct_instance_id.empty())
      << "GIZMOSQL_CURRENT_INSTANCE() should return a non-empty value";
  // Verify it looks like a UUID (36 chars with hyphens)
  ASSERT_EQ(direct_instance_id.length(), 36)
      << "Instance ID should be a UUID, got: " << direct_instance_id;

  // Allow async write queue to flush instrumentation records
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Use GIZMOSQL_CURRENT_INSTANCE() to get current instance ID and verify it exists
  // in the instances table
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(call_options,
                         "SELECT instance_id FROM _gizmosql_instr.instances "
                         "WHERE instance_id = GIZMOSQL_CURRENT_INSTANCE()"));

  bool found_instance = false;
  std::string instance_id;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      found_instance = true;
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      instance_id = array->GetString(0);
    }
  }
  ASSERT_TRUE(found_instance)
      << "GIZMOSQL_CURRENT_INSTANCE() should return current instance ID that exists in instances table";
  ASSERT_FALSE(instance_id.empty()) << "Instance ID should not be empty";
  ASSERT_EQ(instance_id, direct_instance_id)
      << "Instance ID from instances table should match direct query result";
}

// Test GIZMOSQL_USER() function returns current username
TEST_F(InstrumentationServerFixture, UserFunction) {
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

  // Test that GIZMOSQL_USER() returns the expected username
  ASSERT_ARROW_OK_AND_ASSIGN(auto info,
                             sql_client.Execute(call_options, "SELECT GIZMOSQL_USER()"));

  std::string username;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      username = array->GetString(0);
    }
  }
  ASSERT_EQ(username, GetUsername())
      << "GIZMOSQL_USER() should return the authenticated username";
}

// Test GIZMOSQL_ROLE() function returns current user's role
TEST_F(InstrumentationServerFixture, RoleFunction) {
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

  // Test that GIZMOSQL_ROLE() returns the expected role (admin for system user)
  ASSERT_ARROW_OK_AND_ASSIGN(auto info,
                             sql_client.Execute(call_options, "SELECT GIZMOSQL_ROLE()"));

  std::string role;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    if (table->num_rows() > 0) {
      auto column = table->column(0);
      auto array = std::static_pointer_cast<arrow::StringArray>(column->chunk(0));
      role = array->GetString(0);
    }
  }
  // System user (username/password auth) should have admin role
  ASSERT_EQ(role, "admin") << "GIZMOSQL_ROLE() should return 'admin' for system user";
}

// Test that GetSqlInfo returns custom instrumentation metadata
TEST_F(InstrumentationServerFixture, InstrumentationSqlFunctions) {
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

  // Query the instrumentation SQL functions
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(
          call_options,
          "SELECT GIZMOSQL_INSTRUMENTATION_ENABLED() AS enabled,"
          " GIZMOSQL_INSTRUMENTATION_CATALOG() AS catalog,"
          " GIZMOSQL_INSTRUMENTATION_SCHEMA() AS schema"));

  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  std::shared_ptr<arrow::Table> table;
  ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());

  ASSERT_EQ(table->num_rows(), 1) << "Expected 1 row from instrumentation SQL functions";
  ASSERT_EQ(table->num_columns(), 3) << "Expected 3 columns";

  // Verify enabled column is true
  auto enabled_col = std::static_pointer_cast<arrow::BooleanArray>(
      table->column(0)->chunk(0));
  ASSERT_TRUE(enabled_col->Value(0))
      << "GIZMOSQL_INSTRUMENTATION_ENABLED() should be true";

  // Verify catalog is not empty
  auto catalog_col = std::static_pointer_cast<arrow::StringArray>(
      table->column(1)->chunk(0));
  std::string catalog_value = catalog_col->GetString(0);
  ASSERT_FALSE(catalog_value.empty())
      << "GIZMOSQL_INSTRUMENTATION_CATALOG() should not be empty when enabled";

  // Verify schema is not empty
  auto schema_col = std::static_pointer_cast<arrow::StringArray>(
      table->column(2)->chunk(0));
  std::string schema_value = schema_col->GetString(0);
  ASSERT_FALSE(schema_value.empty())
      << "GIZMOSQL_INSTRUMENTATION_SCHEMA() should not be empty when enabled";
}

// Test that stale 'running' instances from previous unclean shutdowns are cleaned up
// This is a unit test that directly tests InstrumentationManager::CleanupStaleRecords
TEST(InstrumentationManagerTest, StaleInstanceCleanup) {
  namespace fs = std::filesystem;

  // Use a temporary database for this test
  std::string test_db_path = "stale_cleanup_test_instr.db";

  // Clean up any existing test database
  fs::remove(test_db_path);

  // Create a DuckDB instance
  duckdb::DuckDB db(":memory:");
  auto shared_db = std::make_shared<duckdb::DuckDB>(std::move(db));

  // Create the first instrumentation manager to set up the schema
  auto result1 = gizmosql::ddb::InstrumentationManager::Create(shared_db, test_db_path);
  ASSERT_TRUE(result1.ok()) << "Failed to create first instrumentation manager: "
                            << result1.status().ToString();
  auto manager1 = result1.ValueOrDie();

  // Insert a fake "running" instance directly into the database
  // This simulates an instance that didn't shut down cleanly
  {
    duckdb::Connection conn(*shared_db);
    auto attach_result = conn.Query("ATTACH IF NOT EXISTS '" + test_db_path + "' AS _gizmosql_instr");
    ASSERT_FALSE(attach_result->HasError()) << attach_result->GetError();

    auto insert_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.instances
        (instance_id, gizmosql_version, gizmosql_edition, duckdb_version, arrow_version, hostname, port, database_path,
         tls_enabled, mtls_required, readonly, start_time, status)
      VALUES
        ('11111111-1111-1111-1111-111111111111', 'v1.0.0', 'Enterprise', 'v1.0.0', 'v20.0.0', 'test', 9999, '/test/db',
         false, false, false, now(), 'running')
    )SQL");
    ASSERT_FALSE(insert_result->HasError()) << insert_result->GetError();

    // Also insert a fake "active" session for the stale instance
    auto session_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sessions
        (session_id, instance_id, username, role, auth_method, peer, connection_protocol, start_time, status)
      VALUES
        ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111',
         'stale_user', 'user', 'Basic', '127.0.0.1', 'grpc', now(), 'active')
    )SQL");
    ASSERT_FALSE(session_result->HasError()) << session_result->GetError();

    // Insert a fake "executing" execution for the stale session
    auto stmt_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sql_statements
        (statement_id, session_id, sql_text, is_internal, prepare_success, created_time)
      VALUES
        ('33333333-3333-3333-3333-333333333333', '22222222-2222-2222-2222-222222222222',
         'SELECT * FROM stale_query', false, true, now())
    )SQL");
    ASSERT_FALSE(stmt_result->HasError()) << stmt_result->GetError();

    auto exec_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sql_executions
        (execution_id, statement_id, execution_start_time, status, rows_fetched)
      VALUES
        ('44444444-4444-4444-4444-444444444444', '33333333-3333-3333-3333-333333333333', now(), 'executing', 0)
    )SQL");
    ASSERT_FALSE(exec_result->HasError()) << exec_result->GetError();
  }

  // Verify the stale records exist and are in their "bad" states
  {
    duckdb::Connection conn(*shared_db);
    auto check_result = conn.Query(
        "SELECT status FROM _gizmosql_instr.instances "
        "WHERE instance_id = '11111111-1111-1111-1111-111111111111'");
    ASSERT_FALSE(check_result->HasError()) << check_result->GetError();
    ASSERT_EQ(check_result->RowCount(), 1);
    ASSERT_EQ(check_result->GetValue(0, 0).ToString(), "running");

    auto session_check = conn.Query(
        "SELECT status FROM _gizmosql_instr.sessions "
        "WHERE session_id = '22222222-2222-2222-2222-222222222222'");
    ASSERT_FALSE(session_check->HasError()) << session_check->GetError();
    ASSERT_EQ(session_check->RowCount(), 1);
    ASSERT_EQ(session_check->GetValue(0, 0).ToString(), "active");

    auto exec_check = conn.Query(
        "SELECT status FROM _gizmosql_instr.sql_executions "
        "WHERE execution_id = '44444444-4444-4444-4444-444444444444'");
    ASSERT_FALSE(exec_check->HasError()) << exec_check->GetError();
    ASSERT_EQ(exec_check->RowCount(), 1);
    ASSERT_EQ(exec_check->GetValue(0, 0).ToString(), "executing");
  }

  // Shut down the first manager
  manager1->Shutdown();
  manager1.reset();

  // Create a second instrumentation manager - this should clean up the stale records
  auto result2 = gizmosql::ddb::InstrumentationManager::Create(shared_db, test_db_path);
  ASSERT_TRUE(result2.ok()) << "Failed to create second instrumentation manager: "
                            << result2.status().ToString();
  auto manager2 = result2.ValueOrDie();

  // Verify the stale instance is now marked as 'stopped' with 'unclean_shutdown'
  {
    duckdb::Connection conn(*shared_db);
    auto check_result = conn.Query(
        "SELECT status, stop_reason FROM _gizmosql_instr.instances "
        "WHERE instance_id = '11111111-1111-1111-1111-111111111111'");
    ASSERT_FALSE(check_result->HasError()) << check_result->GetError();
    ASSERT_EQ(check_result->RowCount(), 1);
    ASSERT_EQ(check_result->GetValue(0, 0).ToString(), "stopped")
        << "Stale instance should be marked as stopped";
    ASSERT_EQ(check_result->GetValue(1, 0).ToString(), "unclean_shutdown")
        << "Stale instance should have unclean_shutdown reason";

    // Verify the stale session is now marked as 'closed'
    auto session_check = conn.Query(
        "SELECT status, stop_reason FROM _gizmosql_instr.sessions "
        "WHERE session_id = '22222222-2222-2222-2222-222222222222'");
    ASSERT_FALSE(session_check->HasError()) << session_check->GetError();
    ASSERT_EQ(session_check->RowCount(), 1);
    ASSERT_EQ(session_check->GetValue(0, 0).ToString(), "closed")
        << "Stale session should be marked as closed";
    ASSERT_EQ(session_check->GetValue(1, 0).ToString(), "unclean_shutdown")
        << "Stale session should have unclean_shutdown reason";

    // Verify the stale execution is now marked as 'error'
    auto exec_check = conn.Query(
        "SELECT status, error_message FROM _gizmosql_instr.sql_executions "
        "WHERE execution_id = '44444444-4444-4444-4444-444444444444'");
    ASSERT_FALSE(exec_check->HasError()) << exec_check->GetError();
    ASSERT_EQ(exec_check->RowCount(), 1);
    ASSERT_EQ(exec_check->GetValue(0, 0).ToString(), "error")
        << "Stale execution should be marked as error";
    ASSERT_EQ(exec_check->GetValue(1, 0).ToString(), "Server shutdown unexpectedly")
        << "Stale execution should have appropriate error message";
  }

  // Clean up
  manager2->Shutdown();
  manager2.reset();
  fs::remove(test_db_path);
}

// Test that SIGTERM gracefully closes instrumentation records
// This test spawns the server as a subprocess, creates session records,
// sends SIGTERM, and verifies records are properly closed
TEST(InstrumentationManagerTest, SIGTERMClosesRecords) {
  namespace fs = std::filesystem;

  // Use unique database files for this test
  std::string test_db = "sigterm_test.db";
  std::string instr_db = "sigterm_instr_test.db";
  int test_port = 31350;
  int health_port = 31351;

  // Clean up any existing test files
  fs::remove(test_db);
  fs::remove(test_db + ".wal");
  fs::remove(instr_db);
  fs::remove(instr_db + ".wal");

  // Get path to the server executable - try multiple possible locations
  fs::path server_exe;
  std::vector<fs::path> search_paths = {
      fs::current_path() / "build" / "gizmosql_server",      // CI: ./build/gizmosql_server
      fs::current_path() / "gizmosql_server",                 // ./gizmosql_server
      fs::current_path().parent_path() / "gizmosql_server",   // ../gizmosql_server (local dev)
  };

  for (const auto& path : search_paths) {
    if (fs::exists(path)) {
      server_exe = path;
      break;
    }
  }

  ASSERT_FALSE(server_exe.empty() || !fs::exists(server_exe))
      << "Server executable not found. Searched paths: "
      << search_paths[0] << ", " << search_paths[1] << ", " << search_paths[2];

  // Check for license key - instrumentation requires enterprise license
  const char* license_key_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_key_file || !fs::exists(license_key_file)) {
    GTEST_SKIP() << "License key file not found, skipping SIGTERM instrumentation test";
  }

  // Build command to start server with instrumentation
  std::string cmd = server_exe.string() +
                    " --database-filename " + test_db +
                    " --port " + std::to_string(test_port) +
                    " --health-port " + std::to_string(health_port) +
                    " --username tester --password tester" +
                    " --enable-instrumentation 1" +
                    " --instrumentation-db-path " + instr_db +
                    " 2>&1 &";

  // Start the server as a background process
  int ret = std::system(cmd.c_str());
  ASSERT_EQ(ret, 0) << "Failed to start server subprocess";

  // Wait for server to start and be ready
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Connect to the server and create a session
  {
    arrow::flight::FlightClientOptions options;
    auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
    ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();
    auto location = *location_result;

    auto client_result = arrow::flight::FlightClient::Connect(location, options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();
    auto client = std::move(*client_result);

    arrow::flight::FlightCallOptions call_options;
    auto auth_result = client->AuthenticateBasicToken({}, "tester", "tester");
    ASSERT_TRUE(auth_result.ok()) << auth_result.status().ToString();
    call_options.headers.push_back(*auth_result);

    arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

    // Execute a query to create instrumentation records
    auto exec_result = sql_client.Execute(call_options, "SELECT 1 AS test");
    ASSERT_TRUE(exec_result.ok()) << exec_result.status().ToString();

    // Fetch results to complete the execution
    for (const auto& endpoint : (*exec_result)->endpoints()) {
      auto reader_result = sql_client.DoGet(call_options, endpoint.ticket);
      ASSERT_TRUE(reader_result.ok()) << reader_result.status().ToString();
      auto table_result = (*reader_result)->ToTable();
      ASSERT_TRUE(table_result.ok()) << table_result.status().ToString();
    }

    // Allow async write queue to flush
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  // Find the server process and send SIGTERM
  std::string find_cmd = "pgrep -f 'gizmosql_server.*" + test_db + "'";
  FILE* pipe = popen(find_cmd.c_str(), "r");
  ASSERT_NE(pipe, nullptr) << "Failed to run pgrep";

  char pid_buf[64];
  std::string pid_str;
  if (fgets(pid_buf, sizeof(pid_buf), pipe) != nullptr) {
    pid_str = pid_buf;
    // Remove newline
    pid_str.erase(std::remove(pid_str.begin(), pid_str.end(), '\n'), pid_str.end());
  }
  pclose(pipe);

  ASSERT_FALSE(pid_str.empty()) << "Could not find server process";

  // Send SIGTERM to the server
  std::string kill_cmd = "kill -TERM " + pid_str;
  ret = std::system(kill_cmd.c_str());
  ASSERT_EQ(ret, 0) << "Failed to send SIGTERM to server";

  // Wait for server to shut down gracefully
  std::this_thread::sleep_for(std::chrono::seconds(3));

  // Verify the server is no longer running
  std::string check_cmd = "kill -0 " + pid_str + " 2>/dev/null";
  ret = std::system(check_cmd.c_str());
  ASSERT_NE(ret, 0) << "Server should have terminated after SIGTERM";

  // Now open the instrumentation database directly and verify records are closed
  {
    duckdb::DuckDB db(instr_db);
    duckdb::Connection conn(db);

    // Check that all instances are marked as 'stopped'
    auto instance_result = conn.Query(
        "SELECT status, stop_reason FROM instances WHERE status = 'running'");
    ASSERT_FALSE(instance_result->HasError()) << instance_result->GetError();
    ASSERT_EQ(instance_result->RowCount(), 0)
        << "All instances should be marked as 'stopped' after SIGTERM, but found "
        << instance_result->RowCount() << " still 'running'";

    // Check that there's at least one stopped instance with graceful shutdown
    auto stopped_result = conn.Query(
        "SELECT COUNT(*) FROM instances WHERE status = 'stopped' AND stop_reason = 'graceful'");
    ASSERT_FALSE(stopped_result->HasError()) << stopped_result->GetError();
    auto count = stopped_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(count, 1)
        << "Expected at least one instance with graceful stop_reason, got " << count;

    // Verify sessions are closed - ReleaseAllSessions() should mark them as 'closed'
    auto session_result = conn.Query(
        "SELECT session_id, status FROM sessions WHERE status = 'active'");
    ASSERT_FALSE(session_result->HasError()) << session_result->GetError();
    ASSERT_EQ(session_result->RowCount(), 0)
        << "All sessions should be marked as 'closed' after SIGTERM, but found "
        << session_result->RowCount() << " still 'active'";

    // Verify at least one session was created and closed
    auto closed_session_result = conn.Query(
        "SELECT COUNT(*) FROM sessions WHERE status = 'closed'");
    ASSERT_FALSE(closed_session_result->HasError()) << closed_session_result->GetError();
    auto closed_count = closed_session_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(closed_count, 1)
        << "Expected at least one closed session, got " << closed_count;

    // Check that all executions are completed (not 'executing')
    auto exec_result = conn.Query(
        "SELECT status FROM sql_executions WHERE status = 'executing'");
    ASSERT_FALSE(exec_result->HasError()) << exec_result->GetError();
    ASSERT_EQ(exec_result->RowCount(), 0)
        << "All executions should be completed after SIGTERM, but found "
        << exec_result->RowCount() << " still 'executing'";

    // Verify at least one execution completed successfully
    auto completed_exec_result = conn.Query(
        "SELECT COUNT(*) FROM sql_executions WHERE status = 'success'");
    ASSERT_FALSE(completed_exec_result->HasError()) << completed_exec_result->GetError();
    auto completed_count = completed_exec_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(completed_count, 1)
        << "Expected at least one completed execution, got " << completed_count;

    // Verify at least one statement was recorded
    auto stmt_result = conn.Query(
        "SELECT COUNT(*) FROM sql_statements");
    ASSERT_FALSE(stmt_result->HasError()) << stmt_result->GetError();
    auto stmt_count = stmt_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(stmt_count, 1)
        << "Expected at least one statement, got " << stmt_count;
  }

  // Clean up test files
  fs::remove(test_db);
  fs::remove(test_db + ".wal");
  fs::remove(instr_db);
  fs::remove(instr_db + ".wal");
}

// Test that GIZMOSQL_ENABLE_INSTRUMENTATION env var enables instrumentation
// without needing the --enable-instrumentation CLI argument
TEST(InstrumentationManagerTest, EnvVarEnablesInstrumentation) {
  namespace fs = std::filesystem;

  // Skip if no license available (instrumentation requires enterprise license)
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_file || std::string(license_file).empty()) {
    GTEST_SKIP() << "Skipping env var test - no enterprise license available. "
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable.";
  }

  // Use unique database files for this test
  std::string test_db = "envvar_test.db";
  std::string instr_db = "envvar_instr_test.db";
  int test_port = 31352;
  int health_port = 31353;

  // Clean up any existing test files
  fs::remove(test_db);
  fs::remove(test_db + ".wal");
  fs::remove(instr_db);
  fs::remove(instr_db + ".wal");

  // Get path to the server executable - try multiple possible locations
  fs::path server_exe;
  std::vector<fs::path> search_paths = {
      fs::current_path() / "build" / "gizmosql_server",      // CI: ./build/gizmosql_server
      fs::current_path() / "gizmosql_server",                 // ./gizmosql_server
      fs::current_path().parent_path() / "gizmosql_server",   // ../gizmosql_server (local dev)
  };

  for (const auto& path : search_paths) {
    if (fs::exists(path)) {
      server_exe = path;
      break;
    }
  }

  ASSERT_FALSE(server_exe.empty() || !fs::exists(server_exe))
      << "Server executable not found. Searched paths: "
      << search_paths[0] << ", " << search_paths[1] << ", " << search_paths[2];

  // Build command to start server WITH env var but WITHOUT --enable-instrumentation arg
  // The env var should enable instrumentation
  std::string cmd = "GIZMOSQL_ENABLE_INSTRUMENTATION=1 " +
                    server_exe.string() +
                    " --database-filename " + test_db +
                    " --port " + std::to_string(test_port) +
                    " --health-port " + std::to_string(health_port) +
                    " --username tester --password tester" +
                    " --instrumentation-db-path " + instr_db +
                    " --license-key-file " + std::string(license_file) +
                    " 2>&1 &";

  // Start the server as a background process
  int ret = std::system(cmd.c_str());
  ASSERT_EQ(ret, 0) << "Failed to start server subprocess";

  // Wait for server to start
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Connect and run a simple query to create session/statement records
  {
    arrow::flight::FlightClientOptions options;
    auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
    ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

    auto client_result = arrow::flight::FlightClient::Connect(*location_result, options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();
    auto client = std::move(*client_result);

    // Authenticate
    arrow::flight::FlightCallOptions call_options;
    auto auth_result = client->AuthenticateBasicToken({}, "tester", "tester");
    ASSERT_TRUE(auth_result.ok()) << auth_result.status().ToString();
    call_options.headers.push_back(*auth_result);

    // Run a simple query
    arrow::flight::sql::FlightSqlClient sql_client(std::move(client));
    auto info_result = sql_client.Execute(call_options, "SELECT 1 AS test");
    ASSERT_TRUE(info_result.ok()) << info_result.status().ToString();
  }

  // Find and kill the server process
  std::string find_cmd = "pgrep -f 'gizmosql_server.*" + std::to_string(test_port) + "'";
  FILE* pipe = popen(find_cmd.c_str(), "r");
  ASSERT_NE(pipe, nullptr);
  char buffer[128];
  std::string pid_str;
  if (fgets(buffer, sizeof(buffer), pipe) != nullptr) {
    pid_str = buffer;
  }
  pclose(pipe);

  if (!pid_str.empty()) {
    std::string kill_cmd = "kill -TERM " + pid_str;
    std::system(kill_cmd.c_str());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

  // Verify the instrumentation database was created and has records
  ASSERT_TRUE(fs::exists(instr_db)) << "Instrumentation DB should exist when enabled via env var";

  {
    duckdb::DuckDB db(instr_db);
    duckdb::Connection conn(db);

    // Verify at least one instance was recorded
    auto instance_result = conn.Query("SELECT COUNT(*) FROM instances");
    ASSERT_FALSE(instance_result->HasError()) << instance_result->GetError();
    auto instance_count = instance_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(instance_count, 1)
        << "Expected at least one instance record when instrumentation enabled via env var";

    // Verify at least one session was recorded
    auto session_result = conn.Query("SELECT COUNT(*) FROM sessions");
    ASSERT_FALSE(session_result->HasError()) << session_result->GetError();
    auto session_count = session_result->GetValue(0, 0).GetValue<int64_t>();
    ASSERT_GE(session_count, 1)
        << "Expected at least one session record when instrumentation enabled via env var";
  }

  // Clean up test files
  fs::remove(test_db);
  fs::remove(test_db + ".wal");
  fs::remove(instr_db);
  fs::remove(instr_db + ".wal");
}
