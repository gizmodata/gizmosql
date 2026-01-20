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

#include <duckdb.hpp>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"
#include "duckdb/instrumentation/instrumentation_manager.h"

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
        (instance_id, gizmosql_version, duckdb_version, arrow_version, hostname, port, database_path,
         tls_enabled, mtls_required, readonly, status)
      VALUES
        ('11111111-1111-1111-1111-111111111111', 'v1.0.0', 'v1.0.0', 'v20.0.0', 'test', 9999, '/test/db',
         false, false, false, 'running')
    )SQL");
    ASSERT_FALSE(insert_result->HasError()) << insert_result->GetError();

    // Also insert a fake "active" session for the stale instance
    auto session_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sessions
        (session_id, instance_id, username, role, auth_method, peer, status)
      VALUES
        ('22222222-2222-2222-2222-222222222222', '11111111-1111-1111-1111-111111111111',
         'stale_user', 'user', 'Basic', '127.0.0.1', 'active')
    )SQL");
    ASSERT_FALSE(session_result->HasError()) << session_result->GetError();

    // Insert a fake "executing" execution for the stale session
    auto stmt_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sql_statements
        (statement_id, session_id, sql_text)
      VALUES
        ('33333333-3333-3333-3333-333333333333', '22222222-2222-2222-2222-222222222222',
         'SELECT * FROM stale_query')
    )SQL");
    ASSERT_FALSE(stmt_result->HasError()) << stmt_result->GetError();

    auto exec_result = conn.Query(R"SQL(
      INSERT INTO _gizmosql_instr.sql_executions
        (execution_id, statement_id, status)
      VALUES
        ('44444444-4444-4444-4444-444444444444', '33333333-3333-3333-3333-333333333333', 'executing')
    )SQL");
    ASSERT_FALSE(exec_result->HasError()) << exec_result->GetError();
  }

  // Verify the stale records exist and are in their "bad" states
  {
    duckdb::Connection conn(*shared_db);
    auto check_result = conn.Query(
        "SELECT status_text FROM _gizmosql_instr.instances "
        "WHERE instance_id = '11111111-1111-1111-1111-111111111111'");
    ASSERT_FALSE(check_result->HasError()) << check_result->GetError();
    ASSERT_EQ(check_result->RowCount(), 1);
    ASSERT_EQ(check_result->GetValue(0, 0).ToString(), "running");

    auto session_check = conn.Query(
        "SELECT status_text FROM _gizmosql_instr.sessions "
        "WHERE session_id = '22222222-2222-2222-2222-222222222222'");
    ASSERT_FALSE(session_check->HasError()) << session_check->GetError();
    ASSERT_EQ(session_check->RowCount(), 1);
    ASSERT_EQ(session_check->GetValue(0, 0).ToString(), "active");

    auto exec_check = conn.Query(
        "SELECT status_text FROM _gizmosql_instr.sql_executions "
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
        "SELECT status_text, stop_reason FROM _gizmosql_instr.instances "
        "WHERE instance_id = '11111111-1111-1111-1111-111111111111'");
    ASSERT_FALSE(check_result->HasError()) << check_result->GetError();
    ASSERT_EQ(check_result->RowCount(), 1);
    ASSERT_EQ(check_result->GetValue(0, 0).ToString(), "stopped")
        << "Stale instance should be marked as stopped";
    ASSERT_EQ(check_result->GetValue(1, 0).ToString(), "unclean_shutdown")
        << "Stale instance should have unclean_shutdown reason";

    // Verify the stale session is now marked as 'closed'
    auto session_check = conn.Query(
        "SELECT status_text, stop_reason FROM _gizmosql_instr.sessions "
        "WHERE session_id = '22222222-2222-2222-2222-222222222222'");
    ASSERT_FALSE(session_check->HasError()) << session_check->GetError();
    ASSERT_EQ(session_check->RowCount(), 1);
    ASSERT_EQ(session_check->GetValue(0, 0).ToString(), "closed")
        << "Stale session should be marked as closed";
    ASSERT_EQ(session_check->GetValue(1, 0).ToString(), "unclean_shutdown")
        << "Stale session should have unclean_shutdown reason";

    // Verify the stale execution is now marked as 'error'
    auto exec_check = conn.Query(
        "SELECT status_text, error_message FROM _gizmosql_instr.sql_executions "
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
