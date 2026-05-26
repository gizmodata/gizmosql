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

#include <duckdb.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <thread>
#include <atomic>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif
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
#include "instrumentation/instrumentation_manager.h"
#endif

using arrow::flight::sql::FlightSqlClient;
namespace fs = std::filesystem;

// ============================================================================
// Configuration
// ============================================================================

// PostgreSQL connection settings (matching docker-compose.test.yml)
// Port 5432: General DuckLake tests (postgres container)
// Port 5433: Instrumentation tests only (postgres-instrumentation container)
const int POSTGRES_PORT = 5432;
const int POSTGRES_INSTR_PORT = 5433;

// MinIO (S3-compatible) connection settings (matching docker-compose.test.yml)
const int MINIO_PORT = 9000;
const char* MINIO_ACCESS_KEY = "minioadmin";
const char* MINIO_SECRET_KEY = "minioadmin";
const char* MINIO_BUCKET = "instrumentation";

// DuckLake data path (local directory for single-instance tests)
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

// Check if a TCP port is available by attempting a connection
bool IsPortAvailable(int port) {
#ifdef _WIN32
  WSADATA wsa;
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return false;
  SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_SOCKET) { WSACleanup(); return false; }
#else
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return false;
#endif

  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  // Set a short timeout
  struct timeval timeout;
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout), sizeof(timeout));

  int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
#ifdef _WIN32
  closesocket(sock);
  WSACleanup();
#else
  close(sock);
#endif

  return result == 0;
}

bool IsPostgresAvailable() {
  return IsPortAvailable(POSTGRES_PORT);
}

bool IsInstrumentationPostgresAvailable() {
  return IsPortAvailable(POSTGRES_INSTR_PORT);
}

bool IsMinioAvailable() {
  return IsPortAvailable(MINIO_PORT);
}

// SQL to setup DuckLake for instrumentation with S3/MinIO and PostgreSQL metadata
// Uses a dedicated PostgreSQL instance for instrumentation tests (port 5433)
// and MinIO for S3-compatible object storage
std::string GetDuckLakeInitSQLWithS3(const std::string& catalog_name,
                                      const std::string& s3_path_suffix = "") {
  // S3 path for this catalog's data
  std::string s3_data_path = "s3://" + std::string(MINIO_BUCKET) + "/" + s3_path_suffix;

  return R"SQL(
    INSTALL ducklake; INSTALL postgres; INSTALL httpfs;
    LOAD ducklake; LOAD postgres; LOAD httpfs;

    CREATE OR REPLACE SECRET s3_secret (
      TYPE s3,
      KEY_ID ')SQL" + std::string(MINIO_ACCESS_KEY) + R"SQL(',
      SECRET ')SQL" + std::string(MINIO_SECRET_KEY) + R"SQL(',
      ENDPOINT 'localhost:)SQL" + std::to_string(MINIO_PORT) + R"SQL(',
      USE_SSL false,
      URL_STYLE 'path'
    );

    CREATE OR REPLACE SECRET pg_instr_secret (
      TYPE postgres,
      HOST 'localhost',
      PORT )SQL" + std::to_string(POSTGRES_INSTR_PORT) + R"SQL(,
      DATABASE 'instrumentation_catalog',
      USER 'postgres',
      PASSWORD 'testpassword'
    );

    CREATE OR REPLACE SECRET ducklake_instr_secret (
      TYPE DUCKLAKE,
      METADATA_PATH '',
      DATA_PATH ')SQL" + s3_data_path + R"SQL(',
      METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_instr_secret'}
    );

    ATTACH 'ducklake:ducklake_instr_secret' AS )SQL" + catalog_name + ";";
}

}  // namespace

// ============================================================================
// Test: Single Instance with DuckLake Instrumentation
// ============================================================================

TEST(DuckLakeInstrumentation, SetupAndQuery) {
  // Skip if instrumentation PostgreSQL is not available (port 5433)
  if (!IsInstrumentationPostgresAvailable()) {
    GTEST_SKIP() << "Instrumentation PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }

  // Skip if MinIO is not available
  if (!IsMinioAvailable()) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
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

  // Configuration
  const int test_port = 31370;
  const int health_port = 31371;
  const std::string catalog_name = "instr_ducklake";
  const std::string schema_name = "main";

  fs::path db_path = "ducklake_instr_test.db";
  // Use S3/MinIO for data storage (matching real-world deployment)
  std::string init_sql = GetDuckLakeInitSQLWithS3(catalog_name, "instrumentation_data/");

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
      /*token_jwks_uri=*/"",
      /*token_default_role=*/"",
      /*token_authorized_emails=*/"",
      /*access_logging_enabled=*/false,
      /*query_timeout=*/0,
      /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*session_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
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
      "SELECT instance_id, gizmosql_version, status FROM " + catalog_name + ".main.instances");
  ASSERT_TRUE(query_result.success)
      << "Failed to query instances: " << query_result.error_message;
  ASSERT_GE(query_result.row_count, 1) << "Expected at least 1 instance record";
  std::cerr << "  Found " << query_result.row_count << " instance(s)" << std::endl;

  // Query sessions from DuckLake catalog
  query_result = RunQuery(sql_client, call_options,
      "SELECT session_id, username, status FROM " + catalog_name + ".main.sessions");
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
  std::error_code ec;
  fs::remove(db_path, ec);
  fs::remove(db_path.string() + ".wal", ec);

  std::cerr << "\n=== DuckLake Instrumentation Test PASSED ===" << std::endl;
}

// ============================================================================
// Test: Multiple Instances with Shared DuckLake Instrumentation (using MinIO/S3)
// ============================================================================

TEST(DuckLakeInstrumentation, MultipleInstancesConcurrent) {
  // Skip if instrumentation PostgreSQL is not available (port 5433)
  if (!IsInstrumentationPostgresAvailable()) {
    GTEST_SKIP() << "Instrumentation PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }

  // Skip if MinIO is not available (required for shared object storage)
  if (!IsMinioAvailable()) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
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

  std::cerr << "\n=== DuckLake Multi-Instance Instrumentation Test (S3/MinIO) ===" << std::endl;

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

  // Use S3/MinIO for shared object storage - all instances write to same bucket
  // Use a fixed path that matches the existing DuckLake catalog metadata
  std::string init_sql = GetDuckLakeInitSQLWithS3(catalog_name, "instrumentation_data/");

  // Clean up stale records from previous test runs by updating all running instances to stopped
  // This simulates what would happen if the previous test crashed without proper cleanup
  std::cerr << "Cleaning up stale records from previous test runs..." << std::endl;
  {
    // Connect via DuckDB directly to clean up stale records
    duckdb::DuckDB db(nullptr);
    duckdb::Connection conn(db);
    // Set up the DuckLake connection (using dedicated instrumentation postgres)
    conn.Query("INSTALL ducklake; INSTALL postgres; INSTALL httpfs; LOAD ducklake; LOAD postgres; LOAD httpfs");
    conn.Query("CREATE OR REPLACE SECRET s3_secret (TYPE s3, KEY_ID '" + std::string(MINIO_ACCESS_KEY) +
               "', SECRET '" + std::string(MINIO_SECRET_KEY) +
               "', ENDPOINT 'localhost:" + std::to_string(MINIO_PORT) + "', USE_SSL false, URL_STYLE 'path')");
    conn.Query("CREATE OR REPLACE SECRET pg_instr_secret (TYPE postgres, HOST 'localhost', PORT " +
               std::to_string(POSTGRES_INSTR_PORT) + ", DATABASE 'instrumentation_catalog', "
               "USER 'postgres', PASSWORD 'testpassword')");
    conn.Query("CREATE OR REPLACE SECRET ducklake_instr_secret (TYPE DUCKLAKE, METADATA_PATH '', "
               "DATA_PATH 's3://" + std::string(MINIO_BUCKET) + "/instrumentation_data/', "
               "METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_instr_secret'})");
    conn.Query("ATTACH 'ducklake:ducklake_instr_secret' AS " + catalog_name);

    // Mark all running instances as stopped (cleanup from previous test runs)
    auto cleanup_result = conn.Query("UPDATE " + catalog_name + "." + schema_name +
                                      ".instances SET status = 'stopped', stop_time = now(), "
                                      "stop_reason = 'test cleanup' WHERE status = 'running'");
    if (cleanup_result->HasError()) {
      std::cerr << "  Note: Could not clean up stale instances (table may not exist yet): "
                << cleanup_result->GetError() << std::endl;
    } else {
      auto changes = cleanup_result->GetValue(0, 0);
      std::cerr << "  Marked stale instances as stopped" << std::endl;
    }
  }

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
        /*token_jwks_uri=*/"",
        /*token_default_role=*/"",
        /*token_authorized_emails=*/"",
        /*access_logging_enabled=*/false,
        /*query_timeout=*/0,
        /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*session_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
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

  // Allow instrumentation to flush (DuckLake writes to Parquet files, needs more time)
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

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
  std::error_code ec;
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
  // Skip if instrumentation PostgreSQL is not available (port 5433)
  if (!IsInstrumentationPostgresAvailable()) {
    GTEST_SKIP() << "Instrumentation PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }

  // Skip if MinIO is not available
  if (!IsMinioAvailable()) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
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

  const int test_port = 31390;
  const std::string catalog_name = "schema_val_ducklake";
  fs::path db_path = "schema_val_test.db";
  // Use S3/MinIO for data storage (matching real-world deployment)
  std::string init_sql = GetDuckLakeInitSQLWithS3(catalog_name, "instrumentation_data/");

  // First server start - creates schema
  std::cerr << "Starting first server instance (schema creation)..." << std::endl;
  {
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", test_port,
        "tester", "tester", "test_key",
        fs::path(), fs::path(), fs::path(),
        init_sql, fs::path(),
        false, false, "", "", fs::path(),
        /*token_jwks_uri=*/"", /*token_default_role=*/"",
        /*token_authorized_emails=*/"",
        false, 0,
        arrow::util::ArrowLogLevel::ARROW_INFO,
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
        /*token_jwks_uri=*/"", /*token_default_role=*/"",
        /*token_authorized_emails=*/"",
        false, 0,
        arrow::util::ArrowLogLevel::ARROW_INFO,
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
  std::error_code ec;
  fs::remove(db_path, ec);
  fs::remove(db_path.string() + ".wal", ec);

  std::cerr << "\n=== DuckLake Schema Validation Test PASSED ===" << std::endl;
}

// ============================================================================
// Test: Instrumentation Catalog is Read-Only for Clients
// ============================================================================

TEST(DuckLakeInstrumentation, CatalogIsReadOnly) {
  // Skip if instrumentation PostgreSQL is not available (port 5433)
  if (!IsInstrumentationPostgresAvailable()) {
    GTEST_SKIP() << "Instrumentation PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }

  // Skip if MinIO is not available
  if (!IsMinioAvailable()) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
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

  std::cerr << "\n=== DuckLake Catalog Read-Only Protection Test ===" << std::endl;

  // This test verifies that clients cannot modify the instrumentation catalog
  // when using DuckLake as the backend. The catalog should be read-only for
  // admins and inaccessible for non-admins.

  const int test_port = 31395;
  const std::string catalog_name = "readonly_instr_ducklake";
  fs::path db_path = "readonly_test.db";
  std::string init_sql = GetDuckLakeInitSQLWithS3(catalog_name, "instrumentation_data/");

  auto result = gizmosql::CreateFlightSQLServer(
      BackendType::duckdb, db_path, "localhost", test_port,
      "admin_user", "admin_pass", "test_key",
      fs::path(), fs::path(), fs::path(),
      init_sql, fs::path(),
      false, false, "", "", fs::path(),
      /*token_jwks_uri=*/"", /*token_default_role=*/"",
      /*token_authorized_emails=*/"",
      false, 0,
      arrow::util::ArrowLogLevel::ARROW_INFO,
      arrow::util::ArrowLogLevel::ARROW_INFO,
      arrow::util::ArrowLogLevel::ARROW_INFO,
      test_port + 1, "",
      true, "", catalog_name, "main");

  ASSERT_TRUE(result.ok()) << "Failed to create server: " << result.status().ToString();
  auto server = *result;

  std::atomic<bool> ready{false};
  std::thread server_thread([&]() {
    ready = true;
    (void)server->Serve();
  });

  while (!ready) std::this_thread::sleep_for(std::chrono::milliseconds(10));
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Connect as admin
  arrow::flight::FlightClientOptions options;
  auto location_result = arrow::flight::Location::ForGrpcTcp("localhost", test_port);
  ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

  auto client_result = arrow::flight::FlightClient::Connect(*location_result, options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();

  arrow::flight::FlightCallOptions call_options;
  auto bearer_result = (*client_result)->AuthenticateBasicToken({}, "admin_user", "admin_pass");
  ASSERT_TRUE(bearer_result.ok()) << bearer_result.status().ToString();
  call_options.headers.push_back(*bearer_result);

  FlightSqlClient sql_client(std::move(*client_result));

  // Allow instrumentation to write initial records
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  // TEST 1: Admin can READ from instrumentation catalog
  std::cerr << "  Testing admin can READ instrumentation catalog..." << std::endl;
  auto read_result = RunQuery(sql_client, call_options,
      "SELECT COUNT(*) FROM " + catalog_name + ".main.instances");
  ASSERT_TRUE(read_result.success)
      << "Admin should be able to read instrumentation catalog: " << read_result.error_message;
  std::cerr << "    Read succeeded (as expected)" << std::endl;

  // TEST 2: Admin cannot WRITE to instrumentation catalog
  std::cerr << "  Testing admin cannot WRITE to instrumentation catalog..." << std::endl;
  auto write_result = RunQuery(sql_client, call_options,
      "INSERT INTO " + catalog_name + ".main.instances "
      "(instance_id, gizmosql_version, gizmosql_edition, duckdb_version, arrow_version, "
      "tls_enabled, mtls_required, readonly, start_time, status) VALUES "
      "('aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'test', 'test', 'test', 'test', "
      "false, false, false, now(), 'running')");
  ASSERT_FALSE(write_result.success)
      << "Admin should NOT be able to write to instrumentation catalog";
  ASSERT_TRUE(write_result.error_message.find("read-only") != std::string::npos ||
              write_result.error_message.find("Access denied") != std::string::npos)
      << "Error should mention read-only or access denied, got: " << write_result.error_message;
  std::cerr << "    Write blocked (as expected): " << write_result.error_message << std::endl;

  // TEST 3: Admin cannot UPDATE instrumentation catalog
  std::cerr << "  Testing admin cannot UPDATE instrumentation catalog..." << std::endl;
  auto update_result = RunQuery(sql_client, call_options,
      "UPDATE " + catalog_name + ".main.instances SET status = 'hacked' WHERE 1=1");
  ASSERT_FALSE(update_result.success)
      << "Admin should NOT be able to update instrumentation catalog";
  std::cerr << "    Update blocked (as expected): " << update_result.error_message << std::endl;

  // TEST 4: Admin cannot DELETE from instrumentation catalog
  std::cerr << "  Testing admin cannot DELETE from instrumentation catalog..." << std::endl;
  auto delete_result = RunQuery(sql_client, call_options,
      "DELETE FROM " + catalog_name + ".main.sessions WHERE 1=0");
  ASSERT_FALSE(delete_result.success)
      << "Admin should NOT be able to delete from instrumentation catalog";
  std::cerr << "    Delete blocked (as expected): " << delete_result.error_message << std::endl;

  // TEST 5: Admin cannot DROP tables in instrumentation catalog
  std::cerr << "  Testing admin cannot DROP tables in instrumentation catalog..." << std::endl;
  auto drop_result = RunQuery(sql_client, call_options,
      "DROP TABLE IF EXISTS " + catalog_name + ".main.sessions");
  ASSERT_FALSE(drop_result.success)
      << "Admin should NOT be able to drop tables in instrumentation catalog";
  std::cerr << "    Drop blocked (as expected): " << drop_result.error_message << std::endl;

  // TEST 6: Admin cannot DETACH the instrumentation catalog
  std::cerr << "  Testing admin cannot DETACH instrumentation catalog..." << std::endl;
  auto detach_result = RunQuery(sql_client, call_options,
      "DETACH " + catalog_name);
  ASSERT_FALSE(detach_result.success)
      << "Admin should NOT be able to detach instrumentation catalog";
  std::cerr << "    Detach blocked (as expected): " << detach_result.error_message << std::endl;

  // Cleanup
  (void)server->Shutdown();
  server_thread.join();
  server.reset();
  gizmosql::CleanupServerResources();

  std::error_code ec;
  fs::remove(db_path, ec);
  fs::remove(db_path.string() + ".wal", ec);

  std::cerr << "\n=== DuckLake Catalog Read-Only Protection Test PASSED ===" << std::endl;
}

// ============================================================================
// Test: Auto-migrate legacy naive-TIMESTAMP schema in a DuckLake catalog
// ============================================================================
//
// Simulates the customer scenario: a DuckLake catalog (Postgres metadata +
// MinIO data) was populated by an older GizmoSQL server that wrote naive
// TIMESTAMP values into instances/sessions/sql_statements/sql_executions.
// On startup, the new server must auto-migrate every timing column to
// TIMESTAMP WITH TIME ZONE in place, interpreting existing values as UTC,
// without losing rows or breaking the dependent views.
//
// The legacy rows are seeded by issuing the same DDL/DML the prior server
// version would have produced, directly against the live DuckLake catalog --
// the on-disk artifact (parquet files referenced from Postgres metadata) is
// identical to what an older binary would have written.
TEST(DuckLakeInstrumentation, AutoMigrateLegacyNaiveTimestampSchema) {
  if (!IsInstrumentationPostgresAvailable()) {
    GTEST_SKIP() << "Instrumentation PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }
  if (!IsMinioAvailable()) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }

#ifdef GIZMOSQL_ENTERPRISE
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (!license_file || !fs::exists(license_file)) {
    GTEST_SKIP() << "License key file not found";
  }
  auto& enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto license_status = enterprise.Initialize(license_file);
  if (!license_status.ok()) {
    GTEST_SKIP() << "Failed to initialize enterprise license: "
                 << license_status.ToString();
  }
#else
  GTEST_SKIP() << "Enterprise features not available";
#endif

  std::cerr << "\n=== DuckLake Auto-Migrate Legacy Schema Test ===" << std::endl;

  const std::string catalog_name = "tz_mig_ducklake";
  const std::string schema_name = "main";
  // Share the same DuckLake metadata DB + S3 data path as the other tests in
  // this file. DuckLake stores ONE data_path per metadata DB, so using a
  // different one here would break sibling tests when run in sequence.
  const std::string init_sql =
      GetDuckLakeInitSQLWithS3(catalog_name, "instrumentation_data/");

  // ---- Phase 1: seed the DuckLake catalog with the LEGACY schema. ----------
  // Use a private duckdb instance to lay down naive-TIMESTAMP tables + rows
  // that look exactly like what the prior server version would have written.
  std::cerr << "Phase 1: seeding legacy naive-TIMESTAMP schema in DuckLake..."
            << std::endl;
  {
    auto seed_db = std::make_shared<duckdb::DuckDB>(":memory:");
    duckdb::Connection seed_conn(*seed_db);
    auto attach_r = seed_conn.Query(init_sql);
    ASSERT_FALSE(attach_r->HasError())
        << "Failed to attach DuckLake catalog for seeding: " << attach_r->GetError();
    auto json_r = seed_conn.Query("INSTALL json; LOAD json;");
    ASSERT_FALSE(json_r->HasError()) << json_r->GetError();

    // Clean slate in case a prior run left tables behind in this DuckLake catalog.
    for (const char* view : {"execution_details", "session_stats",
                             "active_sessions", "session_activity"}) {
      seed_conn.Query("DROP VIEW IF EXISTS " + catalog_name + "." + schema_name +
                      "." + view);
    }
    for (const char* tbl : {"sql_executions", "sql_statements", "sessions",
                            "instances"}) {
      seed_conn.Query("DROP TABLE IF EXISTS " + catalog_name + "." + schema_name +
                      "." + tbl);
    }

    seed_conn.Query("USE " + catalog_name + "." + schema_name);

    // Create tables with the OLD naive-TIMESTAMP shape. Matches the
    // pre-migration schema in instrumentation_manager.cpp.
    auto create_r = seed_conn.Query(R"SQL(
      CREATE TABLE instances (
        instance_id UUID NOT NULL,
        gizmosql_version VARCHAR NOT NULL,
        gizmosql_edition VARCHAR NOT NULL,
        duckdb_version VARCHAR NOT NULL,
        arrow_version VARCHAR NOT NULL,
        hostname VARCHAR, hostname_arg VARCHAR, server_ip VARCHAR,
        port INTEGER, database_path VARCHAR,
        tls_enabled BOOLEAN NOT NULL, tls_cert_path VARCHAR, tls_key_path VARCHAR,
        mtls_required BOOLEAN NOT NULL, mtls_ca_cert_path VARCHAR,
        readonly BOOLEAN NOT NULL,
        os_platform VARCHAR, os_name VARCHAR, os_version VARCHAR,
        cpu_arch VARCHAR, cpu_model VARCHAR,
        cpu_count INTEGER, memory_total_bytes BIGINT,
        start_time TIMESTAMP NOT NULL,
        stop_time TIMESTAMP,
        status VARCHAR NOT NULL,
        stop_reason VARCHAR,
        instance_tag JSON);
      CREATE TABLE sessions (
        session_id UUID NOT NULL, instance_id UUID NOT NULL,
        username VARCHAR NOT NULL, role VARCHAR NOT NULL,
        auth_method VARCHAR NOT NULL, peer VARCHAR NOT NULL,
        peer_identity VARCHAR, user_agent VARCHAR,
        connection_protocol VARCHAR NOT NULL,
        start_time TIMESTAMP NOT NULL,
        stop_time TIMESTAMP,
        status VARCHAR NOT NULL,
        stop_reason VARCHAR,
        session_tag JSON);
      CREATE TABLE sql_statements (
        statement_id UUID NOT NULL, session_id UUID NOT NULL,
        sql_text VARCHAR NOT NULL, flight_method VARCHAR,
        is_internal BOOLEAN NOT NULL,
        prepare_success BOOLEAN NOT NULL, prepare_error VARCHAR,
        created_time TIMESTAMP NOT NULL,
        query_tag JSON);
      CREATE TABLE sql_executions (
        execution_id UUID NOT NULL, statement_id UUID NOT NULL,
        bind_parameters VARCHAR,
        execution_start_time TIMESTAMP NOT NULL,
        execution_end_time TIMESTAMP,
        rows_fetched BIGINT,
        status VARCHAR NOT NULL,
        error_message VARCHAR,
        duration_ms BIGINT);
    )SQL");
    ASSERT_FALSE(create_r->HasError())
        << "Failed to create legacy tables: " << create_r->GetError();

    // The legacy `now()` insert path stripped the tz to local wall-clock; the
    // migration assumes those naive values are UTC. We mimic that by writing
    // `(now() AT TIME ZONE 'UTC')::TIMESTAMP` so the assumed-UTC value is the
    // actual current UTC instant, which lets us verify rows survive AND that
    // the migrated tz-aware values compare correctly to `now()`.
    auto insert_r = seed_conn.Query(R"SQL(
      INSERT INTO instances VALUES
        ('11111111-1111-1111-1111-111111111111','legacy','enterprise','x','x',
         NULL,NULL,NULL,NULL,NULL,false,NULL,NULL,false,NULL,false,
         NULL,NULL,NULL,NULL,NULL,NULL,NULL,
         (now() AT TIME ZONE 'UTC')::TIMESTAMP, NULL, 'running', NULL, NULL);
      INSERT INTO sessions VALUES
        ('22222222-2222-2222-2222-222222222222',
         '11111111-1111-1111-1111-111111111111',
         'legacy_user','admin','Basic','127.0.0.1',NULL,NULL,'plaintext',
         (now() AT TIME ZONE 'UTC')::TIMESTAMP, NULL, 'active', NULL, NULL);
      INSERT INTO sql_statements VALUES
        ('33333333-3333-3333-3333-333333333333',
         '22222222-2222-2222-2222-222222222222',
         'SELECT 1', 'CreatePreparedStatement', false, true, NULL,
         (now() AT TIME ZONE 'UTC')::TIMESTAMP, NULL);
      INSERT INTO sql_executions VALUES
        ('44444444-4444-4444-4444-444444444444',
         '33333333-3333-3333-3333-333333333333',
         NULL, (now() AT TIME ZONE 'UTC')::TIMESTAMP, NULL,
         42, 'success', NULL, 5);
    )SQL");
    ASSERT_FALSE(insert_r->HasError())
        << "Failed to seed legacy rows: " << insert_r->GetError();

    std::cerr << "  Seeded 1 instance / 1 session / 1 statement / 1 execution"
              << std::endl;
  }

  // ---- Phase 2: open a fresh duckdb instance and run the migration. -------
  // This mirrors what the server does on startup: ATTACH the DuckLake catalog,
  // then hand the connection to InstrumentationManager::Create.
  std::cerr << "Phase 2: triggering InstrumentationManager auto-migration..."
            << std::endl;
  {
    auto db = std::make_shared<duckdb::DuckDB>(":memory:");
    duckdb::Connection setup_conn(*db);
    auto attach_r = setup_conn.Query(init_sql);
    ASSERT_FALSE(attach_r->HasError())
        << "Failed to attach DuckLake catalog for migration: "
        << attach_r->GetError();

    auto mgr_result = gizmosql::ddb::InstrumentationManager::Create(
        db, /*db_path=*/"", catalog_name, schema_name,
        /*use_external_catalog=*/true);
    ASSERT_TRUE(mgr_result.ok())
        << "Migration failed: " << mgr_result.status().ToString();
    auto mgr = *mgr_result;

    // ---- Phase 3: verify schema & rows on the migrated catalog. -----------
    duckdb::Connection verify_conn(*db);
    const std::vector<std::pair<std::string, std::string>> timing_columns = {
        {"instances", "start_time"},        {"instances", "stop_time"},
        {"sessions", "start_time"},         {"sessions", "stop_time"},
        {"sql_statements", "created_time"},
        {"sql_executions", "execution_start_time"},
        {"sql_executions", "execution_end_time"},
    };
    for (const auto& [tbl, col] : timing_columns) {
      auto q = verify_conn.Query(
          "SELECT data_type FROM information_schema.columns "
          "WHERE table_catalog = '" + catalog_name +
          "' AND table_name = '" + tbl +
          "' AND column_name = '" + col + "'");
      ASSERT_FALSE(q->HasError()) << q->GetError();
      ASSERT_EQ(q->RowCount(), 1u) << "Missing " << tbl << "." << col;
      auto type_str = q->GetValue(0, 0).ToString();
      EXPECT_NE(type_str.find("WITH TIME ZONE"), std::string::npos)
          << tbl << "." << col << " not migrated, got: " << type_str;
    }
    std::cerr << "  All 7 timing columns are TIMESTAMP WITH TIME ZONE" << std::endl;

    // Rows survived
    for (const auto& [tbl, expected_id] : std::vector<std::pair<std::string, std::string>>{
             {"instances", "11111111-1111-1111-1111-111111111111"},
             {"sessions", "22222222-2222-2222-2222-222222222222"},
             {"sql_statements", "33333333-3333-3333-3333-333333333333"},
             {"sql_executions", "44444444-4444-4444-4444-444444444444"}}) {
      std::string id_col = tbl == "instances"     ? "instance_id"
                           : tbl == "sessions"     ? "session_id"
                           : tbl == "sql_statements" ? "statement_id"
                                                     : "execution_id";
      auto q = verify_conn.Query("SELECT COUNT(*) FROM " + catalog_name + "." +
                                 schema_name + "." + tbl + " WHERE " + id_col +
                                 " = '" + expected_id + "'");
      ASSERT_FALSE(q->HasError()) << q->GetError();
      EXPECT_EQ(q->GetValue(0, 0).GetValue<int64_t>(), 1)
          << "Legacy row missing from " << tbl;
    }
    std::cerr << "  All 4 legacy rows preserved" << std::endl;

    // Customer's filter shape must work without a cast and find the seeded row.
    auto recent = verify_conn.Query(
        "SELECT COUNT(*) FROM " + catalog_name + "." + schema_name +
        ".sessions WHERE start_time > now() - INTERVAL '1 hour'");
    ASSERT_FALSE(recent->HasError()) << recent->GetError();
    EXPECT_GE(recent->GetValue(0, 0).GetValue<int64_t>(), 1)
        << "Customer-style filter should match the legacy session "
           "(timestamp interpreted as UTC, < 1 hour ago)";
    std::cerr << "  `WHERE start_time > now() - INTERVAL '1 hour'` works "
                 "without a cast"
              << std::endl;

    // Views must be present again (the migration drops then InitializeSchema
    // recreates them).
    for (const char* view : {"session_activity", "active_sessions",
                             "session_stats", "execution_details"}) {
      auto v = verify_conn.Query("SELECT COUNT(*) FROM " + catalog_name + "." +
                                 schema_name + "." + view);
      EXPECT_FALSE(v->HasError())
          << "View " << view << " missing after migration: " << v->GetError();
    }
    std::cerr << "  All 4 dependent views were recreated" << std::endl;

    mgr->Shutdown();
  }

  // ---- Phase 4: cleanup to leave the catalog usable by other tests --------
  {
    auto cleanup_db = std::make_shared<duckdb::DuckDB>(":memory:");
    duckdb::Connection cleanup_conn(*cleanup_db);
    cleanup_conn.Query(init_sql);
    for (const char* view : {"execution_details", "session_stats",
                             "active_sessions", "session_activity"}) {
      cleanup_conn.Query("DROP VIEW IF EXISTS " + catalog_name + "." +
                         schema_name + "." + view);
    }
    for (const char* tbl : {"sql_executions", "sql_statements", "sessions",
                            "instances"}) {
      cleanup_conn.Query("DROP TABLE IF EXISTS " + catalog_name + "." +
                         schema_name + "." + tbl);
    }
  }

  std::cerr << "\n=== DuckLake Auto-Migrate Legacy Schema Test PASSED ==="
            << std::endl;
}
