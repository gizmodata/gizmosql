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

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Fixture for SQLite Backend Tests
// ============================================================================

class SQLiteServerFixture
    : public gizmosql::testing::ServerTestFixture<SQLiteServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "sqlite_test.db",
        .port = 31380,
        .health_port = 31381,
        .username = "sqlite_tester",
        .password = "sqlite_password",
        .backend = BackendType::sqlite,
        .enable_instrumentation = false,  // SQLite doesn't support instrumentation
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SQLiteServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<SQLiteServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SQLiteServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SQLiteServerFixture>::config_{};

// ============================================================================
// Helper function to execute a statement and return results
// ============================================================================

class SQLiteTestHelper {
 public:
  SQLiteTestHelper(FlightSqlClient* client, arrow::flight::FlightCallOptions* options)
      : client_(client), options_(options) {}

  arrow::Status ExecuteUpdate(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto update_result, client_->ExecuteUpdate(*options_, sql));
    return arrow::Status::OK();
  }

  arrow::Result<std::shared_ptr<arrow::Table>> ExecuteQuery(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(*options_, sql));
    if (info->endpoints().empty()) {
      return arrow::Status::Invalid("No endpoints returned");
    }
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          client_->DoGet(*options_, info->endpoints()[0].ticket));
    return reader->ToTable();
  }

 private:
  FlightSqlClient* client_;
  arrow::flight::FlightCallOptions* options_;
};

// ============================================================================
// Basic Connection Test
// ============================================================================

TEST_F(SQLiteServerFixture, CanConnect) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  ASSERT_FALSE(bearer.second.empty()) << "Should receive bearer token";
}

// ============================================================================
// Simple SELECT Test
// ============================================================================

TEST_F(SQLiteServerFixture, SimpleSelect) {
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

  // Simple SELECT without a table
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client.Execute(call_options, "SELECT 1 AS x"));

  ASSERT_FALSE(info->endpoints().empty()) << "Should have at least one endpoint";

  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));

  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_EQ(table->num_columns(), 1);
}

// ============================================================================
// CREATE TABLE Test
// ============================================================================

TEST_F(SQLiteServerFixture, CreateTable) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create a table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_create (id INTEGER PRIMARY KEY, name TEXT)"));

  // Verify table exists by selecting from it
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
                             helper.ExecuteQuery("SELECT * FROM test_create"));
  ASSERT_EQ(table->num_rows(), 0) << "New table should be empty";

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_create"));
}

// ============================================================================
// INSERT Test
// ============================================================================

TEST_F(SQLiteServerFixture, InsertData) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_insert (id INTEGER PRIMARY KEY, name TEXT, value "
      "REAL)"));

  // Insert data
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO test_insert (id, name, value) VALUES (1, "
                           "'Alice', 100.5)"));
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO test_insert (id, name, value) VALUES (2, 'Bob', "
                           "200.75)"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO test_insert (id, name, value) VALUES (3, 'Charlie', 300.25)"));

  // Verify data
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
                             helper.ExecuteQuery("SELECT * FROM test_insert ORDER BY id"));
  ASSERT_EQ(table->num_rows(), 3) << "Should have 3 rows";
  ASSERT_EQ(table->num_columns(), 3) << "Should have 3 columns";

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_insert"));
}

// ============================================================================
// UPDATE Test
// ============================================================================

TEST_F(SQLiteServerFixture, UpdateData) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create and populate table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_update (id INTEGER PRIMARY KEY, name TEXT, "
      "status TEXT)"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO test_update (id, name, status) VALUES (1, 'Item1', 'pending')"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO test_update (id, name, status) VALUES (2, 'Item2', 'pending')"));

  // Update data
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("UPDATE test_update SET status = 'completed' WHERE id = 1"));

  // Verify update
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table, helper.ExecuteQuery("SELECT status FROM test_update WHERE id = 1"));
  ASSERT_EQ(table->num_rows(), 1);

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_update"));
}

// ============================================================================
// DELETE Test
// ============================================================================

TEST_F(SQLiteServerFixture, DeleteData) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create and populate table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_delete (id INTEGER PRIMARY KEY, name TEXT)"));
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO test_delete (id, name) VALUES (1, 'Keep')"));
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO test_delete (id, name) VALUES (2, 'Delete')"));
  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO test_delete (id, name) VALUES (3, 'Keep')"));

  // Verify 3 rows
  ASSERT_ARROW_OK_AND_ASSIGN(auto before,
                             helper.ExecuteQuery("SELECT COUNT(*) FROM test_delete"));
  ASSERT_EQ(before->num_rows(), 1);

  // Delete one row
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DELETE FROM test_delete WHERE id = 2"));

  // Verify 2 rows remain
  ASSERT_ARROW_OK_AND_ASSIGN(auto after,
                             helper.ExecuteQuery("SELECT COUNT(*) FROM test_delete"));
  ASSERT_EQ(after->num_rows(), 1);

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_delete"));
}

// ============================================================================
// SELECT with WHERE clause Test
// ============================================================================

TEST_F(SQLiteServerFixture, SelectWithWhere) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create and populate table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_where (id INTEGER, category TEXT, amount "
      "INTEGER)"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO test_where VALUES (1, 'A', 100), (2, 'B', 200), (3, 'A', 150), (4, "
      "'B', 250)"));

  // Select with WHERE
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table, helper.ExecuteQuery("SELECT * FROM test_where WHERE category = 'A'"));
  ASSERT_EQ(table->num_rows(), 2) << "Should have 2 rows with category A";

  // Select with numeric comparison
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table2, helper.ExecuteQuery("SELECT * FROM test_where WHERE amount > 150"));
  ASSERT_EQ(table2->num_rows(), 2) << "Should have 2 rows with amount > 150";

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_where"));
}

// ============================================================================
// Aggregate Functions Test
// ============================================================================

TEST_F(SQLiteServerFixture, AggregateFunctions) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create and populate table
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS test_agg (id INTEGER, value INTEGER)"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO test_agg VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"));

  // Test COUNT
  ASSERT_ARROW_OK_AND_ASSIGN(auto count_result,
                             helper.ExecuteQuery("SELECT COUNT(*) FROM test_agg"));
  ASSERT_EQ(count_result->num_rows(), 1);

  // Test SUM
  ASSERT_ARROW_OK_AND_ASSIGN(auto sum_result,
                             helper.ExecuteQuery("SELECT SUM(value) FROM test_agg"));
  ASSERT_EQ(sum_result->num_rows(), 1);

  // Test AVG
  ASSERT_ARROW_OK_AND_ASSIGN(auto avg_result,
                             helper.ExecuteQuery("SELECT AVG(value) FROM test_agg"));
  ASSERT_EQ(avg_result->num_rows(), 1);

  // Test MIN/MAX
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto minmax_result,
      helper.ExecuteQuery("SELECT MIN(value), MAX(value) FROM test_agg"));
  ASSERT_EQ(minmax_result->num_rows(), 1);
  ASSERT_EQ(minmax_result->num_columns(), 2);

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE test_agg"));
}

// ============================================================================
// JOIN Test
// ============================================================================

TEST_F(SQLiteServerFixture, JoinTables) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Create and populate tables
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "CREATE TABLE IF NOT EXISTS orders (id INTEGER, user_id INTEGER, product TEXT)"));

  ASSERT_ARROW_OK(
      helper.ExecuteUpdate("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate(
      "INSERT INTO orders VALUES (1, 1, 'Widget'), (2, 1, 'Gadget'), (3, 2, 'Gizmo')"));

  // Test JOIN
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto join_result,
      helper.ExecuteQuery("SELECT users.name, orders.product FROM users JOIN orders ON "
                          "users.id = orders.user_id ORDER BY orders.id"));
  ASSERT_EQ(join_result->num_rows(), 3) << "Should have 3 order rows";

  // Cleanup
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE orders"));
  ASSERT_ARROW_OK(helper.ExecuteUpdate("DROP TABLE users"));
}

// ============================================================================
// Multiple Queries in Session Test
// ============================================================================

TEST_F(SQLiteServerFixture, MultipleQueriesInSession) {
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
  SQLiteTestHelper helper(&sql_client, &call_options);

  // Execute many queries in the same session
  for (int i = 0; i < 10; i++) {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto table, helper.ExecuteQuery("SELECT " + std::to_string(i) + " AS num"));
    ASSERT_EQ(table->num_rows(), 1) << "Query " << i << " should return 1 row";
  }
}
