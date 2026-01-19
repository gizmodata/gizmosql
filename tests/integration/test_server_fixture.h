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

#pragma once

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "arrow/flight/sql/server.h"
#include "arrow/result.h"
#include "arrow/util/logging.h"
#include "gizmosql_library.h"

namespace fs = std::filesystem;

namespace gizmosql {

// Forward declaration of the internal CreateFlightSQLServer function
arrow::Result<std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>>
CreateFlightSQLServer(
    const BackendType backend, fs::path& database_filename, std::string hostname,
    int port, std::string username, std::string password, std::string secret_key,
    fs::path tls_cert_path, fs::path tls_key_path, fs::path mtls_ca_cert_path,
    std::string init_sql_commands, fs::path init_sql_commands_file,
    const bool& print_queries, const bool& read_only, std::string token_allowed_issuer,
    std::string token_allowed_audience, fs::path token_signature_verify_cert_path,
    const bool& access_logging_enabled, const int32_t& query_timeout,
    const arrow::util::ArrowLogLevel& query_log_level,
    const arrow::util::ArrowLogLevel& auth_log_level, const int& health_port,
    const bool& enable_instrumentation);

// Cleanup function to reset global state between test suites
void CleanupServerResources();

}  // namespace gizmosql

namespace gizmosql::testing {

struct TestServerConfig {
  std::string database_filename;
  int port;
  int health_port;
  std::string username;
  std::string password;
};

/// CRTP-based test fixture template for integration tests.
/// Each test suite should define its own fixture class that inherits from this
/// and provides a static GetConfig() method.
///
/// Example:
/// ```
/// class MyServerFixture
///     : public gizmosql::testing::ServerTestFixture<MyServerFixture> {
///  public:
///   static gizmosql::testing::TestServerConfig GetConfig() {
///     return {
///         .database_filename = "my_test.db",
///         .port = 31337,
///         .health_port = 31338,
///         .username = "tester",
///         .password = "tester",
///     };
///   }
/// };
///
/// // Static member definitions required by the template
/// template <>
/// std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
///     gizmosql::testing::ServerTestFixture<MyServerFixture>::server_{};
/// template <>
/// std::thread gizmosql::testing::ServerTestFixture<MyServerFixture>::server_thread_{};
/// template <>
/// std::atomic<bool>
///     gizmosql::testing::ServerTestFixture<MyServerFixture>::server_ready_{false};
/// template <>
/// gizmosql::testing::TestServerConfig
///     gizmosql::testing::ServerTestFixture<MyServerFixture>::config_{};
/// ```
template <typename Derived>
class ServerTestFixture : public ::testing::Test {
 protected:
  static std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server_;
  static std::thread server_thread_;
  static std::atomic<bool> server_ready_;
  static TestServerConfig config_;

  static void SetUpTestSuite() {
    config_ = Derived::GetConfig();

    // Remove any existing database files
    CleanupDatabaseFiles();

    fs::path db_path(config_.database_filename);
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", config_.port, config_.username,
        config_.password,
        /*secret_key=*/"test_secret_key_for_testing",
        /*tls_cert_path=*/fs::path(),
        /*tls_key_path=*/fs::path(),
        /*mtls_ca_cert_path=*/fs::path(),
        /*init_sql_commands=*/"",
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
        /*health_port=*/config_.health_port,
        /*enable_instrumentation=*/true);

    ASSERT_TRUE(result.ok()) << "Failed to create server: " << result.status().ToString();
    server_ = *result;

    // CreateFlightSQLServer already calls Init(), so we just need to call Serve()
    server_thread_ = std::thread([&]() {
      server_ready_ = true;
      auto serve_status = server_->Serve();
      if (!serve_status.ok()) {
        std::cerr << "Server serve ended: " << serve_status.ToString() << std::endl;
      }
    });

    // Wait for server to be ready (with timeout)
    auto start = std::chrono::steady_clock::now();
    while (!server_ready_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed > std::chrono::seconds(10)) {
        FAIL() << "Server failed to start within timeout";
      }
    }

    // Give the server a moment to fully initialize
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  static void TearDownTestSuite() {
    if (server_) {
      (void)server_->Shutdown();
    }

    if (server_thread_.joinable()) {
      server_thread_.join();
    }

    server_.reset();
    server_ready_ = false;

    // Clean up global resources
    gizmosql::CleanupServerResources();

    // Clean up database files
    CleanupDatabaseFiles();
  }

  static void CleanupDatabaseFiles() {
    // Remove main database file
    if (!config_.database_filename.empty()) {
      std::error_code ec;
      fs::remove(config_.database_filename, ec);
      // Also remove WAL file if it exists
      fs::remove(config_.database_filename + ".wal", ec);
    }

    // Remove instrumentation database file
    fs::path db_path(config_.database_filename);
    fs::path instr_path;
    if (db_path.has_parent_path()) {
      instr_path = db_path.parent_path() / "gizmosql_instrumentation.db";
    } else {
      instr_path = "gizmosql_instrumentation.db";
    }
    std::error_code ec;
    fs::remove(instr_path, ec);
    fs::remove(instr_path.string() + ".wal", ec);
  }

  bool IsServerReady() const { return server_ready_; }

  int GetPort() const { return config_.port; }

  const std::string& GetUsername() const { return config_.username; }

  const std::string& GetPassword() const { return config_.password; }
};

}  // namespace gizmosql::testing
