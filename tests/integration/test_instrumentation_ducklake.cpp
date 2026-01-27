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

// DuckLake Instrumentation Integration Tests
//
// These tests verify GizmoSQL instrumentation works correctly when using
// DuckLake as the instrumentation backend instead of a local DuckDB file.
//
// Prerequisites:
//   docker-compose -f docker-compose.test.yml up -d
//
// The tests will be skipped if PostgreSQL is not available.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <atomic>
#include <vector>
#include <mutex>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "gizmosql_library.h"
#include "test_server_fixture.h"

#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/enterprise_features.h"
#endif

using arrow::flight::sql::FlightSqlClient;
namespace fs = std::filesystem;

// ============================================================================
// Configuration
// ============================================================================

// PostgreSQL connection settings (matching docker-compose.test.yml)
const int POSTGRES_PORT = 5432;

// DuckLake data path (local directory for Parquet files)
const char* DUCKLAKE_INSTR_DATA_PATH = "data/ducklake_instrumentation_test/";

// ============================================================================
// Helper Functions
// ============================================================================

namespace {

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

// Check if PostgreSQL is available by attempting a TCP connection
bool IsPostgresAvailable() {
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

// SQL to setup DuckLake for instrumentation
std::string GetDuckLakeInitSQL(const std::string& catalog_name) {
  return R"SQL(
    INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
    CREATE OR REPLACE SECRET pg_instr_secret (
      TYPE postgres,
      HOST 'localhost',
      PORT 5432,
      DATABASE 'ducklake_catalog',
      USER 'postgres',
      PASSWORD 'testpassword'
    );
    CREATE OR REPLACE SECRET ducklake_instr_secret (
      TYPE DUCKLAKE,
      METADATA_PATH '',
      DATA_PATH 'data/ducklake_instrumentation_test/',
      METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_instr_secret'}
    );
    ATTACH 'ducklake:ducklake_instr_secret' AS )SQL" + catalog_name + ";";
}

}  // namespace

// ============================================================================
// Test: Single Instance with DuckLake Instrumentation
// ============================================================================

TEST(DuckLakeInstrumentation, SetupAndQuery) {
  // Skip if PostgreSQL is not available
  if (!IsPostgresAvailable()) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker-compose -f docker-compose.test.yml up -d";
  }

#ifdef GIZMOSQL_ENTERPRISE
  // Check for license
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_file || !fs::exists(license_file)) {
    GTEST_SKIP() << "License key file not found, skipping DuckLake instrumentation test";
  }

  auto& enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto license_status = enterprise.Initialize(license_file);
  if (!license_status.ok()) {
    GTEST_SKIP() << "Failed to initialize enterprise license: " << license_status.ToString();
  }
#else
  GTEST_SKIP() << "Enterprise features not available";
#endif

  std::cerr << "\n=== DuckLake Instrumentation Test ===" << std::endl;

  // Clean up test data directory
  std::error_code ec;
  fs::remove_all(DUCKLAKE_INSTR_DATA_PATH, ec);
  fs::create_directories(DUCKLAKE_INSTR_DATA_PATH, ec);

  // Configuration
  const int test_port = 31370;
  const int health_port = 31371;
  const std::string catalog_name = "instr_ducklake";
  const std::string schema_name = "main";

  fs::path db_path = "ducklake_instr_test.db";
  std::string init_sql = GetDuckLakeInitSQL(catalog_name);

  // Create server with DuckLake instrumentation
  auto result = gizmosql::CreateFlightSQLServer(
      BackendType::duckdb, db_path, "localhost", test_port,
      "tester", "tester",
      /*secret_key=*/"test_secret_key",
      /*tls_cert_path=*/fs::path(),
      /*tls_key_path=*/fs::path(),
      /*mtls_ca_cert_path=*/fs::path(),
      /*init_sql_commands=*/init_sql,
      /*init_sql_commands_file=*/fs::path(),
      /*print_queries=*/false,
      /*read_only=*/false,
      /*token_allowed_issuer=*/"",
      /*token_allowed_audience=*/"",
      /*token_signature_verify_cert_path=*/fs::path(),
      /*access_logging_enabled=*/false,
      /*query_timeout=*/0,
      /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*health_port=*/health_port,
      /*health_check_query=*/"",
      /*enable_instrumentation=*/true,
      /*instrumentation_db_path=*/"",
      /*instrumentation_catalog=*/catalog_name,
      /*instrumentation_schema=*/schema_name);

  ASSERT_TRUE(result.ok()) << "Failed to create server: " << result.status().ToString();
  auto server = *result;

  // Start server in background thread
  std::atomic<bool> server_ready{false};
  std::thread server_thread([&]() {
    server_ready = true;
    auto serve_status = server->Serve();
    if (!serve_status.ok()) {
      std::cerr << "Server serve ended: " << serve_status.ToString() << std::endl;
    }
  });

  // Wait for server to be ready
  auto start = std::chrono::steady_clock::now();
  while (!server_ready) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (elapsed > std::chrono::seconds(10)) {
      FAIL() << "Server failed to start within timeout";
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Connect to server
  arrow::flight::FlightClientOptions options;
  auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
  ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();
  auto location = *location_result;

  auto client_result = arrow::flight::FlightClient::Connect(location, options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();

  arrow::flight::FlightCallOptions call_options;
  auto bearer_result = (*client_result)->AuthenticateBasicToken({}, "tester", "tester");
  ASSERT_TRUE(bearer_result.ok()) << bearer_result.status().ToString();
  call_options.headers.push_back(*bearer_result);

  FlightSqlClient sql_client(std::move(*client_result));

  // Run some queries to generate instrumentation data
  std::cerr << "Running test queries..." << std::endl;
  auto query_result = RunQuery(sql_client, call_options, "SELECT 1 AS test_col");
  ASSERT_TRUE(query_result.success) << "Query failed: " << query_result.error_message;

  query_result = RunQuery(sql_client, call_options, "SELECT 2 AS another_test");
  ASSERT_TRUE(query_result.success) << "Query failed: " << query_result.error_message;

  // Allow instrumentation to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // Verify instrumentation data exists in DuckLake catalog
  std::cerr << "Verifying instrumentation in DuckLake..." << std::endl;

  // Query instances from DuckLake catalog
  query_result = RunQuery(sql_client, call_options,
      "SELECT instance_id, gizmosql_version, status_text FROM " + catalog_name + ".main.instances");
  ASSERT_TRUE(query_result.success)
      << "Failed to query instances: " << query_result.error_message;
  ASSERT_GE(query_result.row_count, 1) << "Expected at least 1 instance record";
  std::cerr << "  Found " << query_result.row_count << " instance(s)" << std::endl;

  // Query sessions from DuckLake catalog
  query_result = RunQuery(sql_client, call_options,
      "SELECT session_id, username, status_text FROM " + catalog_name + ".main.sessions");
  ASSERT_TRUE(query_result.success)
      << "Failed to query sessions: " << query_result.error_message;
  ASSERT_GE(query_result.row_count, 1) << "Expected at least 1 session record";
  std::cerr << "  Found " << query_result.row_count << " session(s)" << std::endl;

  // Query statements from DuckLake catalog
  query_result = RunQuery(sql_client, call_options,
      "SELECT statement_id, sql_text FROM " + catalog_name + ".main.sql_statements");
  ASSERT_TRUE(query_result.success)
      << "Failed to query statements: " << query_result.error_message;
  ASSERT_GE(query_result.row_count, 2) << "Expected at least 2 statement records";
  std::cerr << "  Found " << query_result.row_count << " statement(s)" << std::endl;

  // Shutdown server
  std::cerr << "Shutting down server..." << std::endl;
  (void)server->Shutdown();
  server_thread.join();
  server.reset();
  gizmosql::CleanupServerResources();

  // Cleanup
  fs::remove(db_path, ec);
  fs::remove(db_path.string() + ".wal", ec);

  std::cerr << "\n=== DuckLake Instrumentation Test PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Instances with Shared DuckLake Instrumentation
// ============================================================================

TEST(DuckLakeInstrumentation, MultipleInstancesConcurrent) {
  // Skip if PostgreSQL is not available
  if (!IsPostgresAvailable()) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker-compose -f docker-compose.test.yml up -d";
  }

#ifdef GIZMOSQL_ENTERPRISE
  // Check for license
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_file || !fs::exists(license_file)) {
    GTEST_SKIP() << "License key file not found, skipping multi-instance test";
  }

  auto& enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto license_status = enterprise.Initialize(license_file);
  if (!license_status.ok()) {
    GTEST_SKIP() << "Failed to initialize enterprise license: " << license_status.ToString();
  }
#else
  GTEST_SKIP() << "Enterprise features not available";
#endif

  std::cerr << "\n=== DuckLake Multi-Instance Instrumentation Test ===" << std::endl;

  // Clean up test data directory
  std::error_code ec;
  fs::remove_all(DUCKLAKE_INSTR_DATA_PATH, ec);
  fs::create_directories(DUCKLAKE_INSTR_DATA_PATH, ec);

  // Configuration for 3 GizmoSQL instances
  const int NUM_INSTANCES = 3;
  const int base_port = 31380;
  const std::string catalog_name = "shared_instr_ducklake";
  const std::string schema_name = "main";

  struct ServerInstance {
    std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server;
    std::thread thread;
    std::atomic<bool> ready{false};
    int port;
    fs::path db_path;
  };

  std::vector<ServerInstance> instances(NUM_INSTANCES);
  std::string init_sql = GetDuckLakeInitSQL(catalog_name);

  // Start all 3 instances
  std::cerr << "Starting " << NUM_INSTANCES << " GizmoSQL instances with shared DuckLake instrumentation..." << std::endl;

  for (int i = 0; i < NUM_INSTANCES; ++i) {
    instances[i].port = base_port + (i * 2);
    instances[i].db_path = "multi_instr_test_" + std::to_string(i) + ".db";

    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, instances[i].db_path, "localhost", instances[i].port,
        "tester", "tester",
        /*secret_key=*/"test_secret_key_" + std::to_string(i),
        /*tls_cert_path=*/fs::path(),
        /*tls_key_path=*/fs::path(),
        /*mtls_ca_cert_path=*/fs::path(),
        /*init_sql_commands=*/init_sql,
        /*init_sql_commands_file=*/fs::path(),
        /*print_queries=*/false,
        /*read_only=*/false,
        /*token_allowed_issuer=*/"",
        /*token_allowed_audience=*/"",
        /*token_signature_verify_cert_path=*/fs::path(),
        /*access_logging_enabled=*/false,
        /*query_timeout=*/0,
        /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*health_port=*/instances[i].port + 1,
        /*health_check_query=*/"",
        /*enable_instrumentation=*/true,
        /*instrumentation_db_path=*/"",
        /*instrumentation_catalog=*/catalog_name,
        /*instrumentation_schema=*/schema_name);

    ASSERT_TRUE(result.ok()) << "Failed to create server " << i << ": " << result.status().ToString();
    instances[i].server = *result;

    // Start server thread
    instances[i].thread = std::thread([&inst = instances[i]]() {
      inst.ready = true;
      auto serve_status = inst.server->Serve();
      if (!serve_status.ok()) {
        std::cerr << "Server on port " << inst.port << " ended: "
                  << serve_status.ToString() << std::endl;
      }
    });

    std::cerr << "  Started instance " << i << " on port " << instances[i].port << std::endl;
  }

  // Wait for all servers to be ready
  for (int i = 0; i < NUM_INSTANCES; ++i) {
    auto start = std::chrono::steady_clock::now();
    while (!instances[i].ready) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed > std::chrono::seconds(10)) {
        FAIL() << "Server " << i << " failed to start within timeout";
      }
    }
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Connect to each instance and run queries
  std::cerr << "Running queries on each instance..." << std::endl;
  std::vector<std::unique_ptr<FlightSqlClient>> clients(NUM_INSTANCES);
  std::vector<arrow::flight::FlightCallOptions> call_options_vec(NUM_INSTANCES);

  for (int i = 0; i < NUM_INSTANCES; ++i) {
    arrow::flight::FlightClientOptions options;
    auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", instances[i].port);
    ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

    auto client_result = arrow::flight::FlightClient::Connect(*location_result, options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();

    auto bearer_result = (*client_result)->AuthenticateBasicToken({}, "tester", "tester");
    ASSERT_TRUE(bearer_result.ok()) << bearer_result.status().ToString();
    call_options_vec[i].headers.push_back(*bearer_result);

    clients[i] = std::make_unique<FlightSqlClient>(std::move(*client_result));

    // Run queries on this instance
    for (int q = 0; q < 3; ++q) {
      std::string query = "SELECT " + std::to_string(i * 10 + q) +
                          " AS instance_" + std::to_string(i) + "_query_" + std::to_string(q);
      auto query_result = RunQuery(*clients[i], call_options_vec[i], query);
      ASSERT_TRUE(query_result.success)
          << "Query failed on instance " << i << ": " << query_result.error_message;
    }
    std::cerr << "  Ran 3 queries on instance " << i << std::endl;
  }

  // Allow instrumentation to flush
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Verify instrumentation data from all instances in the shared DuckLake catalog
  std::cerr << "Verifying shared instrumentation data..." << std::endl;

  // Query from first instance (all instances share the same catalog)
  auto& check_client = *clients[0];
  auto& check_options = call_options_vec[0];

  // Count instances - should be exactly 3
  auto query_result = RunQuery(check_client, check_options,
      "SELECT COUNT(DISTINCT instance_id) AS instance_count FROM " +
      catalog_name + ".main.instances WHERE status = 'running'");
  ASSERT_TRUE(query_result.success)
      << "Failed to count instances: " << query_result.error_message;
  ASSERT_EQ(query_result.row_count, 1);

  // Get the actual count from the result
  if (query_result.table) {
    auto count_column = query_result.table->column(0);
    auto count_array = std::static_pointer_cast<arrow::Int64Array>(count_column->chunk(0));
    int64_t instance_count = count_array->Value(0);
    std::cerr << "  Found " << instance_count << " running instance(s) in shared catalog" << std::endl;
    ASSERT_EQ(instance_count, NUM_INSTANCES)
        << "Expected " << NUM_INSTANCES << " running instances";
  }

  // Count unique sessions - should be at least 3 (one per instance)
  query_result = RunQuery(check_client, check_options,
      "SELECT COUNT(DISTINCT session_id) AS session_count FROM " +
      catalog_name + ".main.sessions");
  ASSERT_TRUE(query_result.success)
      << "Failed to count sessions: " << query_result.error_message;
  if (query_result.table) {
    auto count_column = query_result.table->column(0);
    auto count_array = std::static_pointer_cast<arrow::Int64Array>(count_column->chunk(0));
    int64_t session_count = count_array->Value(0);
    std::cerr << "  Found " << session_count << " session(s) across all instances" << std::endl;
    ASSERT_GE(session_count, NUM_INSTANCES)
        << "Expected at least " << NUM_INSTANCES << " sessions";
  }

  // Count statements - should be at least 9 (3 per instance)
  query_result = RunQuery(check_client, check_options,
      "SELECT COUNT(*) AS stmt_count FROM " + catalog_name +
      ".main.sql_statements WHERE is_internal = false");
  ASSERT_TRUE(query_result.success)
      << "Failed to count statements: " << query_result.error_message;
  if (query_result.table) {
    auto count_column = query_result.table->column(0);
    auto count_array = std::static_pointer_cast<arrow::Int64Array>(count_column->chunk(0));
    int64_t stmt_count = count_array->Value(0);
    std::cerr << "  Found " << stmt_count << " user statement(s) across all instances" << std::endl;
    ASSERT_GE(stmt_count, NUM_INSTANCES * 3)
        << "Expected at least " << (NUM_INSTANCES * 3) << " statements";
  }

  // Verify each instance has distinct instance_id
  query_result = RunQuery(check_client, check_options,
      "SELECT instance_id, hostname, port FROM " + catalog_name +
      ".main.instances WHERE status = 'running' ORDER BY port");
  ASSERT_TRUE(query_result.success)
      << "Failed to list instances: " << query_result.error_message;
  ASSERT_EQ(query_result.row_count, NUM_INSTANCES)
      << "Expected " << NUM_INSTANCES << " running instances";
  std::cerr << "  All " << NUM_INSTANCES << " instances have unique instance_ids" << std::endl;

  // Clear clients before shutting down servers
  clients.clear();

  // Shutdown all servers
  std::cerr << "Shutting down all instances..." << std::endl;
  for (int i = 0; i < NUM_INSTANCES; ++i) {
    (void)instances[i].server->Shutdown();
  }
  for (int i = 0; i < NUM_INSTANCES; ++i) {
    if (instances[i].thread.joinable()) {
      instances[i].thread.join();
    }
    instances[i].server.reset();
  }
  gizmosql::CleanupServerResources();

  // Cleanup database files
  for (int i = 0; i < NUM_INSTANCES; ++i) {
    fs::remove(instances[i].db_path, ec);
    fs::remove(instances[i].db_path.string() + ".wal", ec);
  }

  std::cerr << "\n=== DuckLake Multi-Instance Instrumentation Test PASSED ===" << std::endl;
}

// ============================================================================
// Test: Schema Validation with DuckLake
// ============================================================================

TEST(DuckLakeInstrumentation, SchemaValidation) {
  // Skip if PostgreSQL is not available
  if (!IsPostgresAvailable()) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker-compose -f docker-compose.test.yml up -d";
  }

#ifdef GIZMOSQL_ENTERPRISE
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_file || !fs::exists(license_file)) {
    GTEST_SKIP() << "License key file not found";
  }

  auto& enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto license_status = enterprise.Initialize(license_file);
  if (!license_status.ok()) {
    GTEST_SKIP() << "Failed to initialize enterprise license";
  }
#else
  GTEST_SKIP() << "Enterprise features not available";
#endif

  std::cerr << "\n=== DuckLake Schema Validation Test ===" << std::endl;

  // This test verifies that schema validation works correctly for DuckLake catalogs.
  // We start a server, let it create the schema, shut it down, then start again
  // to verify schema reuse works.

  std::error_code ec;
  fs::remove_all(DUCKLAKE_INSTR_DATA_PATH, ec);
  fs::create_directories(DUCKLAKE_INSTR_DATA_PATH, ec);

  const int test_port = 31390;
  const std::string catalog_name = "schema_val_ducklake";
  fs::path db_path = "schema_val_test.db";
  std::string init_sql = GetDuckLakeInitSQL(catalog_name);

  // First server start - creates schema
  std::cerr << "Starting first server instance (schema creation)..." << std::endl;
  {
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", test_port,
        "tester", "tester", "test_key",
        fs::path(), fs::path(), fs::path(),
        init_sql, fs::path(),
        false, false, "", "", fs::path(),
        false, 0,
        arrow::util::ArrowLogLevel::ARROW_INFO,
        arrow::util::ArrowLogLevel::ARROW_INFO,
        test_port + 1, "",
        true, "", catalog_name, "main");

    ASSERT_TRUE(result.ok()) << "Failed to create first server: " << result.status().ToString();
    auto server = *result;

    std::atomic<bool> ready{false};
    std::thread server_thread([&]() {
      ready = true;
      (void)server->Serve();
    });

    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Connect and verify schema exists
    arrow::flight::FlightClientOptions options;
    auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
    ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

    auto client_result = arrow::flight::FlightClient::Connect(*location_result, options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();

    arrow::flight::FlightCallOptions call_options;
    auto bearer_result = (*client_result)->AuthenticateBasicToken({}, "tester", "tester");
    ASSERT_TRUE(bearer_result.ok()) << bearer_result.status().ToString();
    call_options.headers.push_back(*bearer_result);

    FlightSqlClient sql_client(std::move(*client_result));

    auto query_result = RunQuery(sql_client, call_options,
        "SELECT COUNT(*) FROM " + catalog_name + ".main.instances");
    ASSERT_TRUE(query_result.success) << "Schema should exist: " << query_result.error_message;

    std::cerr << "  Schema created successfully" << std::endl;

    (void)server->Shutdown();
    server_thread.join();
    server.reset();
    gizmosql::CleanupServerResources();
  }

  // Second server start - should reuse existing schema
  std::cerr << "Starting second server instance (schema reuse)..." << std::endl;
  {
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", test_port,
        "tester", "tester", "test_key2",
        fs::path(), fs::path(), fs::path(),
        init_sql, fs::path(),
        false, false, "", "", fs::path(),
        false, 0,
        arrow::util::ArrowLogLevel::ARROW_INFO,
        arrow::util::ArrowLogLevel::ARROW_INFO,
        test_port + 1, "",
        true, "", catalog_name, "main");

    ASSERT_TRUE(result.ok()) << "Failed to create second server: " << result.status().ToString();
    auto server = *result;

    std::atomic<bool> ready{false};
    std::thread server_thread([&]() {
      ready = true;
      (void)server->Serve();
    });

    while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Connect and verify we now have 2 instances
    arrow::flight::FlightClientOptions options;
    auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
    ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

    auto client_result = arrow::flight::FlightClient::Connect(*location_result, options);
    ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();

    arrow::flight::FlightCallOptions call_options;
    auto bearer_result = (*client_result)->AuthenticateBasicToken({}, "tester", "tester");
    ASSERT_TRUE(bearer_result.ok()) << bearer_result.status().ToString();
    call_options.headers.push_back(*bearer_result);

    FlightSqlClient sql_client(std::move(*client_result));

    // Allow instrumentation to write
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    auto query_result = RunQuery(sql_client, call_options,
        "SELECT COUNT(*) FROM " + catalog_name + ".main.instances");
    ASSERT_TRUE(query_result.success) << "Schema should exist: " << query_result.error_message;
    ASSERT_EQ(query_result.row_count, 1);

    if (query_result.table) {
      auto count_column = query_result.table->column(0);
      auto count_array = std::static_pointer_cast<arrow::Int64Array>(count_column->chunk(0));
      int64_t count = count_array->Value(0);
      std::cerr << "  Found " << count << " total instance records (1 stopped + 1 running)" << std::endl;
      ASSERT_GE(count, 2) << "Expected at least 2 instance records";
    }

    (void)server->Shutdown();
    server_thread.join();
    server.reset();
    gizmosql::CleanupServerResources();
  }

  // Cleanup
  fs::remove(db_path, ec);
  fs::remove(db_path.string() + ".wal", ec);

  std::cerr << "\n=== DuckLake Schema Validation Test PASSED ===" << std::endl;
}
