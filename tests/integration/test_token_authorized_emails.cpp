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
#include <fstream>
#include <filesystem>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"
#include "jwt-cpp/jwt.h"

#include <openssl/rsa.h>
#include <openssl/pem.h>
#include <openssl/bio.h>
#include <openssl/evp.h>

using arrow::flight::sql::FlightSqlClient;

namespace fs = std::filesystem;

// ============================================================================
// RSA key pair generation for testing
// ============================================================================

struct TestKeyPair {
  std::string public_key_pem;
  std::string private_key_pem;
};

static TestKeyPair GenerateTestRSAKeyPair() {
  TestKeyPair kp;

  EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  EVP_PKEY_keygen_init(ctx);
  EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048);

  EVP_PKEY* pkey = nullptr;
  EVP_PKEY_keygen(ctx, &pkey);
  EVP_PKEY_CTX_free(ctx);

  // Extract public key PEM
  BIO* pub_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PUBKEY(pub_bio, pkey);
  char* pub_data = nullptr;
  long pub_len = BIO_get_mem_data(pub_bio, &pub_data);
  kp.public_key_pem = std::string(pub_data, pub_len);
  BIO_free(pub_bio);

  // Extract private key PEM
  BIO* priv_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(priv_bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
  char* priv_data = nullptr;
  long priv_len = BIO_get_mem_data(priv_bio, &priv_data);
  kp.private_key_pem = std::string(priv_data, priv_len);
  BIO_free(priv_bio);

  EVP_PKEY_free(pkey);
  return kp;
}

// Static key pair and cert file path shared by all fixtures
static TestKeyPair g_test_keys;
static std::string g_test_cert_path = "test_token_auth_emails_pub.pem";
static bool g_keys_initialized = false;

static void EnsureTestKeys() {
  if (!g_keys_initialized) {
    g_test_keys = GenerateTestRSAKeyPair();
    std::ofstream cert_file(g_test_cert_path);
    cert_file << g_test_keys.public_key_pem;
    cert_file.close();
    g_keys_initialized = true;
  }
}

// ============================================================================
// Helper: Create a signed JWT with email claim
// ============================================================================

static std::string CreateTestToken(const std::string& email,
                                   const std::string& role,
                                   const std::string& issuer = "test-email-issuer",
                                   const std::string& audience = "test-email-audience") {
  EnsureTestKeys();
  return jwt::create()
      .set_issuer(issuer)
      .set_type("JWT")
      .set_id("test-token-" + email)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_audience(audience)
      .set_subject(email)
      .set_payload_claim("email", jwt::claim(email))
      .set_payload_claim("role", jwt::claim(role))
      .sign(jwt::algorithm::rs256("", g_test_keys.private_key_pem, "", ""));
}

// ============================================================================
// Fixture: Domain wildcard pattern (*@allowed.com)
// ============================================================================

// Ensure keys exist before GetConfig is called
struct KeyInitializer {
  KeyInitializer() { EnsureTestKeys(); }
};
static KeyInitializer g_key_init;

class TokenAuthorizedEmailsDomainFixture
    : public gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsDomainFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "token_auth_emails_domain.db",
        .port = 31371,
        .health_port = 31372,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .token_authorized_emails = "*@allowed.com",
        .token_allowed_issuer = "test-email-issuer",
        .token_allowed_audience = "test-email-audience",
        .token_signature_verify_cert_path = g_test_cert_path,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsDomainFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsDomainFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsDomainFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsDomainFixture>::config_{};

// ============================================================================
// Fixture: Multiple patterns (exact + wildcard)
// ============================================================================

class TokenAuthorizedEmailsMultiFixture
    : public gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsMultiFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "token_auth_emails_multi.db",
        .port = 31373,
        .health_port = 31374,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .token_authorized_emails = "admin@external.com,*@allowed.com",
        .token_allowed_issuer = "test-email-issuer",
        .token_allowed_audience = "test-email-audience",
        .token_signature_verify_cert_path = g_test_cert_path,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsMultiFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsMultiFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsMultiFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TokenAuthorizedEmailsMultiFixture>::config_{};

// ============================================================================
// Tests: Domain wildcard pattern
// ============================================================================

TEST_F(TokenAuthorizedEmailsDomainFixture, MatchingDomainAccepted) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("user@allowed.com", "admin");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, "token", token));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "User with matching domain should be accepted: " << result.status().ToString();
}

TEST_F(TokenAuthorizedEmailsDomainFixture, NonMatchingDomainRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("user@notallowed.com", "admin");
  auto result = client->AuthenticateBasicToken({}, "token", token);

  ASSERT_FALSE(result.ok())
      << "User with non-matching domain should be rejected";
  ASSERT_TRUE(result.status().ToString().find("not authorized") != std::string::npos)
      << "Expected 'not authorized' error, got: " << result.status().ToString();
}

TEST_F(TokenAuthorizedEmailsDomainFixture, CaseInsensitiveMatch) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("User@ALLOWED.COM", "admin");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, "token", token));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "Case-insensitive match should work: " << result.status().ToString();
}

// ============================================================================
// Tests: Multiple patterns
// ============================================================================

TEST_F(TokenAuthorizedEmailsMultiFixture, ExactEmailMatchAccepted) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("admin@external.com", "admin");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, "token", token));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "Exact email match should be accepted: " << result.status().ToString();
}

TEST_F(TokenAuthorizedEmailsMultiFixture, WildcardPatternAccepted) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("anyone@allowed.com", "admin");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, "token", token));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "Wildcard pattern match should be accepted: " << result.status().ToString();
}

TEST_F(TokenAuthorizedEmailsMultiFixture, NonMatchingEmailRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifndef GIZMOSQL_ENTERPRISE
  GTEST_SKIP() << "Token authorized emails is an Enterprise feature";
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  auto token = CreateTestToken("random@gmail.com", "admin");
  auto result = client->AuthenticateBasicToken({}, "token", token);

  ASSERT_FALSE(result.ok())
      << "Non-matching email should be rejected";
  ASSERT_TRUE(result.status().ToString().find("not authorized") != std::string::npos)
      << "Expected 'not authorized' error, got: " << result.status().ToString();
}

TEST_F(TokenAuthorizedEmailsMultiFixture, BasicAuthStillWorks) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Basic auth (username/password) should not be affected by authorized emails
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto result = sql_client.Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(result.ok())
      << "Basic auth should not be affected by authorized emails: "
      << result.status().ToString();
}
