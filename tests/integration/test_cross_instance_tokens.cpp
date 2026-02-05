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

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"
#include "jwt-cpp/jwt.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Fixture for Cross-Instance Token Tests - STRICT MODE (default)
// ============================================================================

class CrossInstanceTokenStrictFixture
    : public gizmosql::testing::ServerTestFixture<CrossInstanceTokenStrictFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "cross_instance_strict.db",
        .port = 31355,
        .health_port = 31356,
        .username = "testuser",
        .password = "testpassword",
        .allow_cross_instance_tokens = false,  // Strict mode (default)
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenStrictFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<CrossInstanceTokenStrictFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenStrictFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenStrictFixture>::config_{};

// ============================================================================
// Test Fixture for Cross-Instance Token Tests - RELAXED MODE
// ============================================================================

class CrossInstanceTokenRelaxedFixture
    : public gizmosql::testing::ServerTestFixture<CrossInstanceTokenRelaxedFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "cross_instance_relaxed.db",
        .port = 31357,
        .health_port = 31358,
        .username = "testuser",
        .password = "testpassword",
        .allow_cross_instance_tokens = true,  // Relaxed mode
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenRelaxedFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<CrossInstanceTokenRelaxedFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenRelaxedFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CrossInstanceTokenRelaxedFixture>::config_{};

// ============================================================================
// Helper: Create a JWT token with a different instance_id
// ============================================================================

std::string CreateTokenWithDifferentInstanceId(const std::string& secret_key,
                                                const std::string& username,
                                                const std::string& role) {
  // Create a valid JWT but with a different instance_id than the server
  auto token = jwt::create()
                   .set_issuer("gizmosql")
                   .set_type("JWT")
                   .set_id("test-token-id")
                   .set_issued_at(std::chrono::system_clock::now())
                   .set_expires_at(std::chrono::system_clock::now() +
                                   std::chrono::seconds{3600})
                   .set_payload_claim("sub", jwt::claim(username))
                   .set_payload_claim("role", jwt::claim(role))
                   .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
                   .set_payload_claim("instance_id",
                                      jwt::claim(std::string("different-instance-12345")))
                   .set_payload_claim("session_id",
                                      jwt::claim(std::string("test-session-id")))
                   .sign(jwt::algorithm::hs256{secret_key});
  return token;
}

// ============================================================================
// STRICT MODE Tests - Cross-instance tokens should be REJECTED
// ============================================================================

TEST_F(CrossInstanceTokenStrictFixture, ConfigIsStrict) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_FALSE(GetAllowCrossInstanceTokens())
      << "This fixture should have strict mode enabled";
}

TEST_F(CrossInstanceTokenStrictFixture, ValidTokenFromSameInstanceWorks) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Get a valid token from the server
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Execute a query - should succeed
  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok()) << "Valid token should work: " << result.status().ToString();
}

TEST_F(CrossInstanceTokenStrictFixture, TokenFromDifferentInstanceRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a token with a different instance_id
  // Note: The test server uses "test_secret_key_for_testing" as the secret key
  std::string cross_instance_token =
      CreateTokenWithDifferentInstanceId("test_secret_key_for_testing", GetUsername(), "admin");

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer " + cross_instance_token});

  FlightSqlClient sql_client(std::move(client));

  // Execute a query - should FAIL because instance_id doesn't match
  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_FALSE(result.ok())
      << "Token from different instance should be rejected in strict mode";
  ASSERT_TRUE(result.status().ToString().find("Session not associated") !=
                  std::string::npos ||
              result.status().ToString().find("Unauthenticated") != std::string::npos ||
              result.status().ToString().find("instance") != std::string::npos)
      << "Expected instance mismatch error, got: " << result.status().ToString();
}

// ============================================================================
// RELAXED MODE Tests - Cross-instance tokens should be ACCEPTED
// ============================================================================

TEST_F(CrossInstanceTokenRelaxedFixture, ConfigIsRelaxed) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_TRUE(GetAllowCrossInstanceTokens())
      << "This fixture should have relaxed mode enabled";
}

TEST_F(CrossInstanceTokenRelaxedFixture, ValidTokenFromSameInstanceWorks) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Get a valid token from the server
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Execute a query - should succeed
  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok()) << "Valid token should work: " << result.status().ToString();
}

TEST_F(CrossInstanceTokenRelaxedFixture, TokenFromDifferentInstanceAccepted) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a token with a different instance_id
  // Note: The test server uses "test_secret_key_for_testing" as the secret key
  std::string cross_instance_token =
      CreateTokenWithDifferentInstanceId("test_secret_key_for_testing", GetUsername(), "admin");

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer " + cross_instance_token});

  FlightSqlClient sql_client(std::move(client));

  // Execute a query - should SUCCEED because relaxed mode allows cross-instance tokens
  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "Token from different instance should be accepted in relaxed mode: "
      << result.status().ToString();

  // Verify we can read the result
  for (const auto& endpoint : (*result)->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    ASSERT_EQ(table->num_rows(), 1) << "Query should return one row";
  }
}

TEST_F(CrossInstanceTokenRelaxedFixture, InvalidSecretKeyStillRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a token signed with a DIFFERENT secret key
  std::string bad_token =
      CreateTokenWithDifferentInstanceId("wrong_secret_key", GetUsername(), "admin");

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer " + bad_token});

  FlightSqlClient sql_client(std::move(client));

  // Execute a query - should FAIL because signature doesn't match
  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_FALSE(result.ok())
      << "Token with wrong secret key should still be rejected in relaxed mode";
}
