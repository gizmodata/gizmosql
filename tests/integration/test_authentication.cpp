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
#include "arrow/util/base64.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Fixture for Authentication Tests
// ============================================================================

class AuthenticationServerFixture
    : public gizmosql::testing::ServerTestFixture<AuthenticationServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "auth_tester.db",
        .port = 31343,
        .health_port = 31344,
        .username = "validuser",
        .password = "validpassword123",
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<AuthenticationServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<AuthenticationServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<AuthenticationServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<AuthenticationServerFixture>::config_{};

// ============================================================================
// Happy Path Tests - Valid Credentials
// ============================================================================

TEST_F(AuthenticationServerFixture, ValidCredentialsAuthenticate) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Valid credentials should authenticate successfully
  auto result = client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  ASSERT_TRUE(result.ok()) << "Valid credentials should authenticate: "
                           << result.status().ToString();

  // Verify we got a bearer token
  auto bearer = *result;
  ASSERT_FALSE(bearer.first.empty()) << "Should receive authorization header key";
  ASSERT_FALSE(bearer.second.empty()) << "Should receive bearer token value";
}

TEST_F(AuthenticationServerFixture, ValidCredentialsCanExecuteQuery) {
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

  // Execute a query
  ASSERT_ARROW_OK_AND_ASSIGN(auto info,
                             sql_client.Execute(call_options, "SELECT 1 AS result"));

  // Verify results
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
    ASSERT_EQ(table->num_rows(), 1) << "Query should return one row";
  }
}

// ============================================================================
// Invalid Password Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, WrongPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Wrong password should fail
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "wrongpassword");
  ASSERT_FALSE(result.ok()) << "Wrong password should not authenticate";
  ASSERT_TRUE(result.status().ToString().find("Unauthenticated") != std::string::npos ||
              result.status().ToString().find("Invalid") != std::string::npos ||
              result.status().ToString().find("username or password") != std::string::npos)
      << "Expected authentication failure message: " << result.status().ToString();
}

TEST_F(AuthenticationServerFixture, SimilarPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Similar password (off by one character) should fail
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "validpassword12");
  ASSERT_FALSE(result.ok()) << "Similar password should not authenticate";
}

TEST_F(AuthenticationServerFixture, CaseSensitivePassword) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Different case password should fail
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "VALIDPASSWORD123");
  ASSERT_FALSE(result.ok()) << "Case-different password should not authenticate";
}

// ============================================================================
// Invalid Username Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, WrongUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Wrong username should fail
  auto result = client->AuthenticateBasicToken({}, "wronguser", GetPassword());
  ASSERT_FALSE(result.ok()) << "Wrong username should not authenticate";
  ASSERT_TRUE(result.status().ToString().find("Unauthenticated") != std::string::npos ||
              result.status().ToString().find("Invalid") != std::string::npos ||
              result.status().ToString().find("username or password") != std::string::npos)
      << "Expected authentication failure message: " << result.status().ToString();
}

TEST_F(AuthenticationServerFixture, CaseSensitiveUsername) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Different case username should fail
  auto result = client->AuthenticateBasicToken({}, "VALIDUSER", GetPassword());
  ASSERT_FALSE(result.ok()) << "Case-different username should not authenticate";
}

TEST_F(AuthenticationServerFixture, SimilarUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Similar username (off by one character) should fail
  auto result = client->AuthenticateBasicToken({}, "validuse", GetPassword());
  ASSERT_FALSE(result.ok()) << "Similar username should not authenticate";
}

// ============================================================================
// Blank Credential Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, BlankUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Blank username should fail
  auto result = client->AuthenticateBasicToken({}, "", GetPassword());
  ASSERT_FALSE(result.ok()) << "Blank username should not authenticate";
}

TEST_F(AuthenticationServerFixture, BlankPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Blank password should fail
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "");
  ASSERT_FALSE(result.ok()) << "Blank password should not authenticate";
}

TEST_F(AuthenticationServerFixture, BothBlankRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Both blank should fail
  auto result = client->AuthenticateBasicToken({}, "", "");
  ASSERT_FALSE(result.ok()) << "Both blank credentials should not authenticate";
}

// ============================================================================
// Both Credentials Wrong Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, BothWrongRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Both wrong should fail
  auto result = client->AuthenticateBasicToken({}, "attacker", "hacked");
  ASSERT_FALSE(result.ok()) << "Both wrong credentials should not authenticate";
}

// ============================================================================
// Unauthenticated Access Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, UnauthenticatedQueryRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  FlightSqlClient sql_client(std::move(client));

  // Attempt to execute query without authentication
  arrow::flight::FlightCallOptions call_options;  // No bearer token
  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Unauthenticated query should be rejected";
  // Server returns "Invalid Authorization Header type!" when no auth is provided
  ASSERT_TRUE(result.status().ToString().find("Unauthenticated") != std::string::npos ||
              result.status().ToString().find("Authorization") != std::string::npos ||
              result.status().ToString().find("auth") != std::string::npos)
      << "Expected authentication required message: " << result.status().ToString();
}

// ============================================================================
// Forged Bearer Token Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, ForgedBearerTokenRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  FlightSqlClient sql_client(std::move(client));

  // Create a forged bearer token (not signed with the server's secret key)
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer forged.token.here"});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Forged bearer token should be rejected";
}

TEST_F(AuthenticationServerFixture, RandomJWTRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  FlightSqlClient sql_client(std::move(client));

  // Create a JWT-like forged token (three base64-encoded parts)
  // This is a fake JWT with invalid signature
  std::string fake_header = arrow::util::base64_encode(R"({"alg":"HS256","typ":"JWT"})");
  std::string fake_payload = arrow::util::base64_encode(
      R"({"sub":"attacker","name":"Hacker","iat":1516239022})");
  std::string fake_signature = arrow::util::base64_encode("fakesignature");
  std::string fake_jwt = fake_header + "." + fake_payload + "." + fake_signature;

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer " + fake_jwt});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Fake JWT should be rejected";
}

TEST_F(AuthenticationServerFixture, EmptyBearerTokenRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  FlightSqlClient sql_client(std::move(client));

  // Empty bearer token
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "Bearer "});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Empty bearer token should be rejected";
}

TEST_F(AuthenticationServerFixture, GarbageAuthorizationHeaderRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  FlightSqlClient sql_client(std::move(client));

  // Garbage authorization header
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({"authorization", "garbage!@#$%^&*()"});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Garbage authorization header should be rejected";
}

// ============================================================================
// Token Manipulation Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, ModifiedTokenRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // First, get a valid token
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  // Create a new client to test modified token
  ASSERT_ARROW_OK_AND_ASSIGN(auto client2,
                             arrow::flight::FlightClient::Connect(location, options));
  FlightSqlClient sql_client(std::move(client2));

  // Modify the token by appending characters
  std::string modified_token = bearer.second + "MODIFIED";
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({bearer.first, modified_token});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Modified token should be rejected";
}

TEST_F(AuthenticationServerFixture, TruncatedTokenRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // First, get a valid token
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  // Create a new client to test truncated token
  ASSERT_ARROW_OK_AND_ASSIGN(auto client2,
                             arrow::flight::FlightClient::Connect(location, options));
  FlightSqlClient sql_client(std::move(client2));

  // Truncate the token
  std::string truncated_token = bearer.second.substr(0, bearer.second.size() / 2);
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back({bearer.first, truncated_token});

  auto result = sql_client.Execute(call_options, "SELECT 1");
  ASSERT_FALSE(result.ok()) << "Truncated token should be rejected";
}

// ============================================================================
// Special Character Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, SQLInjectionInUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // SQL injection attempt in username
  auto result = client->AuthenticateBasicToken({}, "' OR '1'='1", GetPassword());
  ASSERT_FALSE(result.ok()) << "SQL injection in username should not authenticate";
}

TEST_F(AuthenticationServerFixture, SQLInjectionInPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // SQL injection attempt in password
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "' OR '1'='1");
  ASSERT_FALSE(result.ok()) << "SQL injection in password should not authenticate";
}

TEST_F(AuthenticationServerFixture, NullByteInUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Null byte in username
  std::string username_with_null = std::string(GetUsername()) + '\0' + "extra";
  auto result = client->AuthenticateBasicToken({}, username_with_null, GetPassword());
  ASSERT_FALSE(result.ok()) << "Null byte in username should not authenticate";
}

TEST_F(AuthenticationServerFixture, WhitespaceOnlyUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Whitespace-only username
  auto result = client->AuthenticateBasicToken({}, "   ", GetPassword());
  ASSERT_FALSE(result.ok()) << "Whitespace-only username should not authenticate";
}

TEST_F(AuthenticationServerFixture, WhitespaceOnlyPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Whitespace-only password
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "   ");
  ASSERT_FALSE(result.ok()) << "Whitespace-only password should not authenticate";
}

// ============================================================================
// Multiple Authentication Attempts Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, MultipleFailedAttemptsStillReject) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));

  // Try multiple wrong passwords - all should fail
  for (int i = 0; i < 5; i++) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(location, options));
    auto result = client->AuthenticateBasicToken({}, GetUsername(),
                                                  "wrongpassword" + std::to_string(i));
    ASSERT_FALSE(result.ok()) << "Attempt " << i << " with wrong password should fail";
  }

  // Valid credentials should still work after failed attempts
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  auto result = client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  ASSERT_TRUE(result.ok()) << "Valid credentials should still work after failed attempts: "
                           << result.status().ToString();
}

// ============================================================================
// Session Persistence Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, TokenRemainsValidForMultipleQueries) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  // Authenticate once
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Execute multiple queries with the same token
  for (int i = 0; i < 5; i++) {
    auto result = sql_client.Execute(call_options, "SELECT " + std::to_string(i));
    ASSERT_TRUE(result.ok()) << "Query " << i << " should succeed with valid token: "
                             << result.status().ToString();
  }
}

// ============================================================================
// Unicode/International Character Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, UnicodeUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Unicode username that doesn't match configured username
  auto result = client->AuthenticateBasicToken({}, "用户名", GetPassword());
  ASSERT_FALSE(result.ok()) << "Unicode username should not authenticate";
}

TEST_F(AuthenticationServerFixture, EmojiInCredentialsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Emoji in password
  auto result = client->AuthenticateBasicToken({}, GetUsername(), "password🔐");
  ASSERT_FALSE(result.ok()) << "Emoji in password should not authenticate";
}

// ============================================================================
// Very Long Credential Tests
// ============================================================================

TEST_F(AuthenticationServerFixture, VeryLongUsernameRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Very long username (10KB)
  std::string long_username(10240, 'a');
  auto result = client->AuthenticateBasicToken({}, long_username, GetPassword());
  ASSERT_FALSE(result.ok()) << "Very long username should not authenticate";
}

TEST_F(AuthenticationServerFixture, VeryLongPasswordRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Very long password (10KB)
  std::string long_password(10240, 'p');
  auto result = client->AuthenticateBasicToken({}, GetUsername(), long_password);
  ASSERT_FALSE(result.ok()) << "Very long password should not authenticate";
}
