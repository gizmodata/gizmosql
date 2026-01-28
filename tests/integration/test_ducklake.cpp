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

// DuckLake Integration Tests
//
// These tests verify GizmoSQL works correctly with DuckLake, using PostgreSQL
// as the metadata store. They also verify that instrumentation properly
// captures queries against DuckLake tables.
//
// Prerequisites:
//   docker-compose -f docker-compose.test.yml up -d
//
// The tests will be skipped if PostgreSQL is not available.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Configuration
// ============================================================================

// PostgreSQL connection settings (matching docker-compose.test.yml)
const char* POSTGRES_HOST = "localhost";
const int POSTGRES_PORT = 5432;
const char* POSTGRES_USER = "postgres";
const char* POSTGRES_PASSWORD = "testpassword";
const char* POSTGRES_DB = "ducklake_catalog";

// DuckLake data path (local directory for Parquet files)
const char* DUCKLAKE_DATA_PATH = "data/ducklake_test/";

// ============================================================================
// Helper Functions
// ============================================================================

struct QueryResult {
  bool success;
  int64_t row_count;
  std::string error_message;
  std::shared_ptr<arrow::Table> table;
};

// Run a query via GizmoSQL Flight SQL
QueryResult RunQuery(FlightSqlClient& client,
                     arrow::flight::FlightCallOptions& call_options,
                     const std::string& query) {
  QueryResult result{};

  auto flight_info_result = client.Execute(call_options, query);
  if (!flight_info_result.ok()) {
    result.success = false;
    result.error_message = flight_info_result.status().ToString();
    return result;
  }

  auto flight_info = std::move(*flight_info_result);
  if (flight_info->endpoints().empty()) {
    // Some queries (like CREATE, INSERT) may not return data
    result.success = true;
    result.row_count = 0;
    return result;
  }

  auto reader_result =
      client.DoGet(call_options, flight_info->endpoints()[0].ticket);
  if (!reader_result.ok()) {
    result.success = false;
    result.error_message = reader_result.status().ToString();
    return result;
  }

  auto reader = std::move(*reader_result);
  auto table_result = reader->ToTable();
  if (!table_result.ok()) {
    result.success = false;
    result.error_message = table_result.status().ToString();
    return result;
  }

  result.table = *table_result;
  result.row_count = result.table->num_rows();
  result.success = true;
  return result;
}

// Check if PostgreSQL is available by attempting a connection via DuckDB
bool IsPostgresAvailable() {
  // Try to connect to PostgreSQL using DuckDB's postgres extension
  // We'll do a quick TCP check instead
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return false;

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(POSTGRES_PORT);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  // Set a short timeout
  struct timeval timeout;
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

  int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
  close(sock);

  return result == 0;
}

// ============================================================================
// Test Fixture
// ============================================================================

class DuckLakeServerFixture
    : public gizmosql::testing::ServerTestFixture<DuckLakeServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "ducklake_test.db",
        .port = 31360,
        .health_port = 31361,
        .username = "ducklake_tester",
        .password = "ducklake_tester",
    };
  }
};

// Static member definitions
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<DuckLakeServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<DuckLakeServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<DuckLakeServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<DuckLakeServerFixture>::config_{};

// ============================================================================
// Tests
// ============================================================================

// Test DuckLake setup with PostgreSQL metadata store
TEST_F(DuckLakeServerFixture, DuckLakeSetupAndQuery) {
  // Skip if PostgreSQL is not available
  if (!IsPostgresAvailable()) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker-compose -f docker-compose.test.yml up -d";
  }

  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to GizmoSQL
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));

  std::cerr << "\n=== DuckLake Integration Test ===" << std::endl;

  // Step 1: Install required extensions
  std::cerr << "Installing DuckLake and PostgreSQL extensions..." << std::endl;
  auto result = RunQuery(sql_client, call_options, "INSTALL ducklake;");
  ASSERT_TRUE(result.success) << "Failed to install ducklake: " << result.error_message;

  result = RunQuery(sql_client, call_options, "INSTALL postgres;");
  ASSERT_TRUE(result.success) << "Failed to install postgres: " << result.error_message;

  result = RunQuery(sql_client, call_options, "LOAD ducklake;");
  ASSERT_TRUE(result.success) << "Failed to load ducklake: " << result.error_message;

  result = RunQuery(sql_client, call_options, "LOAD postgres;");
  ASSERT_TRUE(result.success) << "Failed to load postgres: " << result.error_message;

  // Step 2: Create PostgreSQL secret for DuckLake metadata
  std::cerr << "Creating PostgreSQL secret..." << std::endl;
  std::string create_pg_secret = R"(
    CREATE OR REPLACE SECRET postgres_secret (
      TYPE postgres,
      HOST 'localhost',
      PORT 5432,
      DATABASE 'ducklake_catalog',
      USER 'postgres',
      PASSWORD 'testpassword'
    );
  )";
  result = RunQuery(sql_client, call_options, create_pg_secret);
  ASSERT_TRUE(result.success) << "Failed to create postgres secret: " << result.error_message;

  // Step 3: Create DuckLake secret
  std::cerr << "Creating DuckLake secret..." << std::endl;
  std::string create_ducklake_secret = R"(
    CREATE OR REPLACE SECRET ducklake_secret (
      TYPE DUCKLAKE,
      METADATA_PATH '',
      DATA_PATH 'data/ducklake_test/',
      METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
    );
  )";
  result = RunQuery(sql_client, call_options, create_ducklake_secret);
  ASSERT_TRUE(result.success) << "Failed to create ducklake secret: " << result.error_message;

  // Step 4: Attach DuckLake
  std::cerr << "Attaching DuckLake..." << std::endl;
  result = RunQuery(sql_client, call_options,
                    "ATTACH 'ducklake:ducklake_secret' AS my_ducklake;");
  ASSERT_TRUE(result.success) << "Failed to attach ducklake: " << result.error_message;

  // Step 5: Use DuckLake schema
  result = RunQuery(sql_client, call_options, "USE my_ducklake;");
  ASSERT_TRUE(result.success) << "Failed to use ducklake: " << result.error_message;

  // Step 6: Create a test table
  std::cerr << "Creating test table in DuckLake..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    CREATE OR REPLACE TABLE test_cities (
      id INTEGER,
      name VARCHAR,
      country VARCHAR,
      population INTEGER
    );
  )");
  ASSERT_TRUE(result.success) << "Failed to create table: " << result.error_message;

  // Step 7: Insert test data
  std::cerr << "Inserting test data..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    INSERT INTO test_cities VALUES
      (1, 'Amsterdam', 'Netherlands', 872000),
      (2, 'Rotterdam', 'Netherlands', 651000),
      (3, 'The Hague', 'Netherlands', 545000),
      (4, 'Utrecht', 'Netherlands', 359000),
      (5, 'Eindhoven', 'Netherlands', 234000);
  )");
  ASSERT_TRUE(result.success) << "Failed to insert data: " << result.error_message;

  // Step 8: Query the data
  std::cerr << "Querying DuckLake table..." << std::endl;
  result = RunQuery(sql_client, call_options,
                    "SELECT * FROM test_cities ORDER BY population DESC;");
  ASSERT_TRUE(result.success) << "Failed to query: " << result.error_message;
  ASSERT_EQ(result.row_count, 5) << "Expected 5 rows";
  std::cerr << "  Retrieved " << result.row_count << " rows" << std::endl;

  // Step 9: Aggregation query
  result = RunQuery(sql_client, call_options, R"(
    SELECT country, COUNT(*) as city_count, SUM(population) as total_pop
    FROM test_cities
    GROUP BY country;
  )");
  ASSERT_TRUE(result.success) << "Failed aggregation query: " << result.error_message;
  ASSERT_EQ(result.row_count, 1) << "Expected 1 aggregation row";
  std::cerr << "  Aggregation returned " << result.row_count << " row" << std::endl;

  // Give instrumentation a moment to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Step 10: Verify instrumentation captured DuckLake queries
  std::cerr << "Verifying instrumentation captured DuckLake queries..." << std::endl;

  // Query instrumentation for our session's statements
  result = RunQuery(sql_client, call_options, R"(
    SELECT sql_text, prepare_success, is_internal
    FROM _gizmosql_instr.sql_statements
    WHERE session_id = GIZMOSQL_CURRENT_SESSION()
      AND sql_text LIKE '%test_cities%'
      AND is_internal = false
    ORDER BY created_time;
  )");
  ASSERT_TRUE(result.success) << "Failed to query instrumentation: " << result.error_message;
  std::cerr << "  Found " << result.row_count
            << " instrumented statements mentioning 'test_cities'" << std::endl;

  // We expect at least: CREATE TABLE, INSERT, SELECT, SELECT with GROUP BY
  ASSERT_GE(result.row_count, 4)
      << "Expected at least 4 instrumented DuckLake statements";

  // Verify executions were recorded
  result = RunQuery(sql_client, call_options, R"(
    SELECT e.execution_id, s.sql_text, e.status, e.rows_fetched, e.duration_ms
    FROM _gizmosql_instr.sql_executions e
    JOIN _gizmosql_instr.sql_statements s ON e.statement_id = s.statement_id
    WHERE s.session_id = GIZMOSQL_CURRENT_SESSION()
      AND s.sql_text LIKE '%test_cities%'
      AND s.is_internal = false
    ORDER BY e.execution_start_time;
  )");
  ASSERT_TRUE(result.success) << "Failed to query executions: " << result.error_message;
  std::cerr << "  Found " << result.row_count << " execution records" << std::endl;
  ASSERT_GE(result.row_count, 4) << "Expected at least 4 execution records";

  // Step 11: Clean up
  std::cerr << "Cleaning up..." << std::endl;
  result = RunQuery(sql_client, call_options, "DROP TABLE IF EXISTS test_cities;");
  // Note: cleanup may fail if table doesn't exist, that's OK

  result = RunQuery(sql_client, call_options, "DETACH my_ducklake;");
  // Detach may also have issues, OK to continue

  std::cerr << "\n=== DuckLake Integration Test PASSED ===" << std::endl;
}

// Test that DuckLake works with concurrent queries
TEST_F(DuckLakeServerFixture, DuckLakeConcurrentQueries) {
  // Skip if PostgreSQL is not available
  if (!IsPostgresAvailable()) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker-compose -f docker-compose.test.yml up -d";
  }

  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // This test uses a simpler approach - we just verify concurrent reads work
  // after the first test has set up DuckLake

  const int NUM_CLIENTS = 3;
  std::atomic<int> successful_clients{0};
  std::atomic<int> failed_clients{0};
  std::vector<std::thread> threads;
  std::mutex output_mutex;

  std::cerr << "\n=== DuckLake Concurrent Queries Test ===" << std::endl;

  // First, set up DuckLake (using client 0)
  {
    arrow::flight::FlightClientOptions options;
    auto location_result =
        arrow::flight::Location::ForGrpcTcp("localhost", GetPort());
    ASSERT_TRUE(location_result.ok());
    auto client_result =
        arrow::flight::FlightClient::Connect(*location_result, options);
    ASSERT_TRUE(client_result.ok());

    arrow::flight::FlightCallOptions call_options;
    auto bearer_result =
        (*client_result)->AuthenticateBasicToken({}, GetUsername(), GetPassword());
    ASSERT_TRUE(bearer_result.ok());
    call_options.headers.push_back(*bearer_result);

    FlightSqlClient sql_client(std::move(*client_result));

    // Setup DuckLake (extensions, secrets, attach)
    RunQuery(sql_client, call_options, "INSTALL ducklake; LOAD ducklake;");
    RunQuery(sql_client, call_options, "INSTALL postgres; LOAD postgres;");

    RunQuery(sql_client, call_options, R"(
      CREATE OR REPLACE SECRET postgres_secret (
        TYPE postgres, HOST 'localhost', PORT 5432,
        DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'testpassword'
      );
    )");

    RunQuery(sql_client, call_options, R"(
      CREATE OR REPLACE SECRET ducklake_secret (
        TYPE DUCKLAKE, METADATA_PATH '', DATA_PATH 'data/ducklake_test/',
        METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
      );
    )");

    RunQuery(sql_client, call_options,
             "ATTACH 'ducklake:ducklake_secret' AS my_ducklake;");
    RunQuery(sql_client, call_options, "USE my_ducklake;");

    // Create and populate test table
    RunQuery(sql_client, call_options, R"(
      CREATE OR REPLACE TABLE concurrent_test (id INTEGER, value VARCHAR);
    )");
    RunQuery(sql_client, call_options, R"(
      INSERT INTO concurrent_test VALUES (1, 'one'), (2, 'two'), (3, 'three');
    )");
  }

  // Now run concurrent queries from multiple clients
  for (int client_id = 0; client_id < NUM_CLIENTS; ++client_id) {
    threads.emplace_back([&, client_id]() {
      try {
        arrow::flight::FlightClientOptions options;
        auto location_result =
            arrow::flight::Location::ForGrpcTcp("localhost", GetPort());
        if (!location_result.ok()) {
          failed_clients++;
          return;
        }

        auto client_result =
            arrow::flight::FlightClient::Connect(*location_result, options);
        if (!client_result.ok()) {
          failed_clients++;
          return;
        }

        arrow::flight::FlightCallOptions call_options;
        auto bearer_result = (*client_result)
                                 ->AuthenticateBasicToken({}, GetUsername(), GetPassword());
        if (!bearer_result.ok()) {
          failed_clients++;
          return;
        }
        call_options.headers.push_back(*bearer_result);

        FlightSqlClient sql_client(std::move(*client_result));

        // Each client needs to set up its session to use DuckLake
        RunQuery(sql_client, call_options, "LOAD ducklake; LOAD postgres;");

        // Re-create secrets for this session
        RunQuery(sql_client, call_options, R"(
          CREATE OR REPLACE SECRET postgres_secret (
            TYPE postgres, HOST 'localhost', PORT 5432,
            DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'testpassword'
          );
        )");

        RunQuery(sql_client, call_options, R"(
          CREATE OR REPLACE SECRET ducklake_secret (
            TYPE DUCKLAKE, METADATA_PATH '', DATA_PATH 'data/ducklake_test/',
            METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
          );
        )");

        RunQuery(sql_client, call_options,
                 "ATTACH 'ducklake:ducklake_secret' AS my_ducklake;");
        RunQuery(sql_client, call_options, "USE my_ducklake;");

        // Run some queries
        for (int i = 0; i < 5; ++i) {
          auto result =
              RunQuery(sql_client, call_options, "SELECT * FROM concurrent_test;");
          if (!result.success || result.row_count != 3) {
            std::lock_guard<std::mutex> lock(output_mutex);
            std::cerr << "Client " << client_id << " query " << i
                      << " failed or wrong row count" << std::endl;
            failed_clients++;
            return;
          }
        }

        successful_clients++;
        {
          std::lock_guard<std::mutex> lock(output_mutex);
          std::cerr << "Client " << client_id << " completed 5 queries successfully"
                    << std::endl;
        }
      } catch (const std::exception& e) {
        std::lock_guard<std::mutex> lock(output_mutex);
        std::cerr << "Client " << client_id << " exception: " << e.what()
                  << std::endl;
        failed_clients++;
      }
    });
  }

  // Wait for all threads
  for (auto& t : threads) {
    t.join();
  }

  std::cerr << "Successful clients: " << successful_clients.load() << "/"
            << NUM_CLIENTS << std::endl;

  ASSERT_EQ(failed_clients.load(), 0) << "Some clients failed";
  ASSERT_EQ(successful_clients.load(), NUM_CLIENTS)
      << "Not all clients succeeded";

  std::cerr << "\n=== DuckLake Concurrent Queries Test PASSED ===" << std::endl;
}
