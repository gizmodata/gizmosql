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

#include <grpcpp/grpcpp.h>
#include "grpc/health/v1/health.grpc.pb.h"

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Fixture for Health Check Tests with Default Query
// ============================================================================

class HealthCheckDefaultQueryFixture
    : public gizmosql::testing::ServerTestFixture<HealthCheckDefaultQueryFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "health_default_test.db",
        .port = 31390,
        .health_port = 31391,
        .username = "health_tester",
        .password = "health_password",
        .backend = BackendType::duckdb,
        .enable_instrumentation = false,
        .health_check_query = "",  // Uses default "SELECT 1"
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<HealthCheckDefaultQueryFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<HealthCheckDefaultQueryFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<HealthCheckDefaultQueryFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<HealthCheckDefaultQueryFixture>::config_{};

// ============================================================================
// Test Fixture for Health Check Tests with Custom Query
// ============================================================================

class HealthCheckCustomQueryFixture
    : public gizmosql::testing::ServerTestFixture<HealthCheckCustomQueryFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "health_custom_test.db",
        .port = 31392,
        .health_port = 31393,
        .username = "health_tester",
        .password = "health_password",
        .backend = BackendType::duckdb,
        .enable_instrumentation = false,
        .health_check_query = "SELECT 42 AS answer",  // Custom health check query
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<HealthCheckCustomQueryFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<HealthCheckCustomQueryFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<HealthCheckCustomQueryFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<HealthCheckCustomQueryFixture>::config_{};

// ============================================================================
// Health Check Tests with Default Query
// ============================================================================

TEST_F(HealthCheckDefaultQueryFixture, ServerIsHealthyWithDefaultQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to the plaintext health port and check health status via gRPC Health service
  auto channel = grpc::CreateChannel(
      "localhost:" + std::to_string(GetHealthPort()),
      grpc::InsecureChannelCredentials());

  auto stub = grpc::health::v1::Health::NewStub(channel);

  grpc::ClientContext context;
  grpc::health::v1::HealthCheckRequest request;
  grpc::health::v1::HealthCheckResponse response;

  auto status = stub->Check(&context, request, &response);

  ASSERT_TRUE(status.ok()) << "Health check failed: " << status.error_message();
  ASSERT_EQ(response.status(), grpc::health::v1::HealthCheckResponse::SERVING)
      << "Server should be SERVING with default health check query 'SELECT 1'";
}

TEST_F(HealthCheckDefaultQueryFixture, CanExecuteQueriesWithDefaultHealthCheck) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect and authenticate
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto auth_result = client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  ASSERT_TRUE(auth_result.ok()) << "Authentication failed: " << auth_result.status().ToString();

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(auth_result.ValueOrDie());

  auto sql_client = std::make_unique<FlightSqlClient>(std::move(client));

  // Execute a simple query to verify server is functional
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client->Execute(call_options, "SELECT 1 AS test"));
  ASSERT_GT(info->endpoints().size(), 0);

  // Fetch the results
  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client->DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
}

// ============================================================================
// Health Check Tests with Custom Query
// ============================================================================

TEST_F(HealthCheckCustomQueryFixture, ServerIsHealthyWithCustomQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to the plaintext health port and check health status via gRPC Health service
  auto channel = grpc::CreateChannel(
      "localhost:" + std::to_string(GetHealthPort()),
      grpc::InsecureChannelCredentials());

  auto stub = grpc::health::v1::Health::NewStub(channel);

  grpc::ClientContext context;
  grpc::health::v1::HealthCheckRequest request;
  grpc::health::v1::HealthCheckResponse response;

  auto status = stub->Check(&context, request, &response);

  ASSERT_TRUE(status.ok()) << "Health check failed: " << status.error_message();
  ASSERT_EQ(response.status(), grpc::health::v1::HealthCheckResponse::SERVING)
      << "Server should be SERVING with custom health check query 'SELECT 42 AS answer'";
}

TEST_F(HealthCheckCustomQueryFixture, CanExecuteCustomHealthCheckQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect and authenticate
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto auth_result = client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  ASSERT_TRUE(auth_result.ok()) << "Authentication failed: " << auth_result.status().ToString();

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(auth_result.ValueOrDie());

  auto sql_client = std::make_unique<FlightSqlClient>(std::move(client));

  // Execute the same query as the custom health check to verify it works
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client->Execute(call_options, "SELECT 42 AS answer"));
  ASSERT_GT(info->endpoints().size(), 0);

  // Fetch the results
  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client->DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_EQ(table->num_rows(), 1);

  // Verify the result
  auto column = table->column(0);
  ASSERT_EQ(column->length(), 1);
  auto chunk = column->chunk(0);
  auto int_array = std::static_pointer_cast<arrow::Int32Array>(chunk);
  ASSERT_EQ(int_array->Value(0), 42);
}
