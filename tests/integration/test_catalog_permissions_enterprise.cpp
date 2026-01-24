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

// Tests for per-catalog permissions enterprise feature gating.
// These tests verify that bootstrap tokens with catalog_access claims
// are rejected when the enterprise feature is not licensed.

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <fstream>
#include <random>
#include <sstream>

#include <jwt-cpp/jwt.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logging.h"
#include "test_util.h"
#include "test_server_fixture.h"

#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/enterprise_features.h"
#endif

namespace fs = std::filesystem;

using arrow::flight::sql::FlightSqlClient;

// Constants for bootstrap token testing
const std::string kBootstrapTokenIssuer = "test-idp";
const std::string kBootstrapTokenAudience = "gizmosql-test";

// ============================================================================
// RSA Key Pair Generation for Testing
// ============================================================================

struct RSAKeyPair {
  std::string private_key_pem;
  std::string public_key_pem;
};

RSAKeyPair GenerateTestRSAKeyPair() {
  RSAKeyPair result;

  // Generate RSA key pair
  EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  EVP_PKEY_keygen_init(ctx);
  EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048);

  EVP_PKEY* pkey = nullptr;
  EVP_PKEY_keygen(ctx, &pkey);
  EVP_PKEY_CTX_free(ctx);

  // Extract private key PEM
  BIO* private_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(private_bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
  char* private_data = nullptr;
  long private_len = BIO_get_mem_data(private_bio, &private_data);
  result.private_key_pem = std::string(private_data, private_len);
  BIO_free(private_bio);

  // Extract public key PEM
  BIO* public_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PUBKEY(public_bio, pkey);
  char* public_data = nullptr;
  long public_len = BIO_get_mem_data(public_bio, &public_data);
  result.public_key_pem = std::string(public_data, public_len);
  BIO_free(public_bio);

  EVP_PKEY_free(pkey);

  return result;
}

// Simple UUID generation for tests (static to avoid conflicts with other test files)
static std::string GenerateTestUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 8; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 4; i++) ss << dis(gen);
  ss << "-4";
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  ss << dis2(gen);
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 12; i++) ss << dis(gen);

  return ss.str();
}

// Helper to create a bootstrap JWT token (signed with RSA, external issuer)
std::string CreateBootstrapToken(const RSAKeyPair& keys, const std::string& username,
                                  const std::string& role,
                                  const std::string& catalog_access_json = "") {
  auto builder = jwt::create()
                     .set_issuer(kBootstrapTokenIssuer)
                     .set_audience(kBootstrapTokenAudience)
                     .set_type("JWT")
                     .set_id("bootstrap-" + GenerateTestUUID())
                     .set_issued_at(std::chrono::system_clock::now())
                     .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{1})
                     .set_subject(username)
                     .set_payload_claim("role", jwt::claim(role));

  // Add catalog_access claim if provided
  if (!catalog_access_json.empty()) {
    picojson::value v;
    std::string err = picojson::parse(v, catalog_access_json);
    if (err.empty()) {
      builder = builder.set_payload_claim("catalog_access", jwt::claim(v));
    }
  }

  return builder.sign(jwt::algorithm::rs256("", keys.private_key_pem, "", ""));
}

// ============================================================================
// Test Fixture for Bootstrap Token Enterprise Feature Tests
// ============================================================================

class BootstrapTokenEnterpriseFixture : public ::testing::Test {
 protected:
  static std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server_;
  static std::thread server_thread_;
  static std::atomic<bool> server_ready_;
  static RSAKeyPair keys_;
  static fs::path public_key_path_;
  static int port_;
  static int health_port_;

  static void SetUpTestSuite() {
    port_ = 31355;
    health_port_ = 31356;

    // Generate RSA key pair for signing bootstrap tokens
    keys_ = GenerateTestRSAKeyPair();

    // Write public key to a temp file for the server
    public_key_path_ = fs::temp_directory_path() / "gizmosql_test_bootstrap_key.pem";
    std::ofstream key_file(public_key_path_);
    key_file << keys_.public_key_pem;
    key_file.close();

    // Remove any existing database files
    std::error_code ec;
    fs::remove("bootstrap_enterprise_test.db", ec);
    fs::remove("gizmosql_instrumentation.db", ec);

    fs::path db_path("bootstrap_enterprise_test.db");
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", port_,
        /*username=*/"admin",
        /*password=*/"admin",
        /*secret_key=*/"test_secret_key",
        /*tls_cert_path=*/fs::path(),
        /*tls_key_path=*/fs::path(),
        /*mtls_ca_cert_path=*/fs::path(),
        /*init_sql_commands=*/"",
        /*init_sql_commands_file=*/fs::path(),
        /*print_queries=*/false,
        /*read_only=*/false,
        /*token_allowed_issuer=*/kBootstrapTokenIssuer,
        /*token_allowed_audience=*/kBootstrapTokenAudience,
        /*token_signature_verify_cert_path=*/public_key_path_,
        /*access_logging_enabled=*/false,
        /*query_timeout=*/0,
        /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*health_port=*/health_port_,
        /*health_check_query=*/"",
        /*enable_instrumentation=*/false,
        /*instrumentation_db_path=*/"");

    ASSERT_TRUE(result.ok()) << "Failed to create server: " << result.status().ToString();
    server_ = *result;

    server_thread_ = std::thread([&]() {
      server_ready_ = true;
      auto serve_status = server_->Serve();
      if (!serve_status.ok()) {
        std::cerr << "Server serve ended: " << serve_status.ToString() << std::endl;
      }
    });

    // Wait for server to be ready
    auto start = std::chrono::steady_clock::now();
    while (!server_ready_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed > std::chrono::seconds(10)) {
        FAIL() << "Server failed to start within timeout";
      }
    }

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

    // Clean up
    gizmosql::CleanupServerResources();
    std::error_code ec;
    fs::remove("bootstrap_enterprise_test.db", ec);
    fs::remove("gizmosql_instrumentation.db", ec);
    fs::remove(public_key_path_, ec);
  }

  bool IsServerReady() const { return server_ready_; }

  int GetPort() const { return port_; }

  arrow::Result<std::unique_ptr<FlightSqlClient>> CreateClient() {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", port_));
    ARROW_ASSIGN_OR_RAISE(auto client,
                          arrow::flight::FlightClient::Connect(location, options));
    return std::make_unique<FlightSqlClient>(std::move(client));
  }

  // Authenticate with a bootstrap token (username "token", password is the token)
  arrow::Result<std::pair<std::string, std::string>> AuthenticateWithBootstrapToken(
      arrow::flight::FlightClient& client, const std::string& token) {
    return client.AuthenticateBasicToken({}, "token", token);
  }
};

// Static member definitions
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    BootstrapTokenEnterpriseFixture::server_{};
std::thread BootstrapTokenEnterpriseFixture::server_thread_{};
std::atomic<bool> BootstrapTokenEnterpriseFixture::server_ready_{false};
RSAKeyPair BootstrapTokenEnterpriseFixture::keys_{};
fs::path BootstrapTokenEnterpriseFixture::public_key_path_{};
int BootstrapTokenEnterpriseFixture::port_{0};
int BootstrapTokenEnterpriseFixture::health_port_{0};

// ============================================================================
// Bootstrap Token Tests - No Catalog Access (Should Always Work)
// ============================================================================

TEST_F(BootstrapTokenEnterpriseFixture, BootstrapTokenWithoutCatalogAccessSucceeds) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a bootstrap token WITHOUT catalog_access claim
  std::string token = CreateBootstrapToken(keys_, "test_user", "user");

  // Should authenticate successfully
  auto result = AuthenticateWithBootstrapToken(*client, token);
  ASSERT_TRUE(result.ok()) << "Bootstrap token without catalog_access should authenticate: "
                           << result.status().ToString();
}

// ============================================================================
// Bootstrap Token Tests - With Catalog Access (Requires Enterprise)
// ============================================================================

TEST_F(BootstrapTokenEnterpriseFixture, BootstrapTokenWithCatalogAccessRequiresEnterprise) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a bootstrap token WITH catalog_access claim
  std::string catalog_access = R"([{"catalog": "*", "access": "read"}])";
  std::string token = CreateBootstrapToken(keys_, "test_user", "user", catalog_access);

  auto result = AuthenticateWithBootstrapToken(*client, token);

#ifdef GIZMOSQL_ENTERPRISE
  // Check if enterprise features are available
  bool has_enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable();

  if (has_enterprise) {
    // With enterprise license, should succeed
    ASSERT_TRUE(result.ok())
        << "Bootstrap token with catalog_access should succeed with enterprise license: "
        << result.status().ToString();
  } else {
    // Without enterprise license, should fail
    ASSERT_FALSE(result.ok())
        << "Bootstrap token with catalog_access should fail without enterprise license";
    ASSERT_TRUE(result.status().ToString().find("Enterprise") != std::string::npos ||
                result.status().ToString().find("enterprise") != std::string::npos)
        << "Expected enterprise feature required message: " << result.status().ToString();
  }
#else
  // Core edition should reject catalog_access
  ASSERT_FALSE(result.ok())
      << "Bootstrap token with catalog_access should fail in Core edition";
#endif
}

TEST_F(BootstrapTokenEnterpriseFixture, BootstrapTokenWithWriteCatalogAccessRequiresEnterprise) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a bootstrap token WITH catalog_access claim (write access)
  std::string catalog_access = R"([{"catalog": "main", "access": "write"}])";
  std::string token = CreateBootstrapToken(keys_, "test_user", "admin", catalog_access);

  auto result = AuthenticateWithBootstrapToken(*client, token);

#ifdef GIZMOSQL_ENTERPRISE
  bool has_enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable();

  if (has_enterprise) {
    ASSERT_TRUE(result.ok())
        << "Bootstrap token with write catalog_access should succeed with enterprise license: "
        << result.status().ToString();
  } else {
    ASSERT_FALSE(result.ok())
        << "Bootstrap token with catalog_access should fail without enterprise license";
    ASSERT_TRUE(result.status().ToString().find("Enterprise") != std::string::npos ||
                result.status().ToString().find("enterprise") != std::string::npos)
        << "Expected enterprise feature required message: " << result.status().ToString();
  }
#else
  ASSERT_FALSE(result.ok())
      << "Bootstrap token with catalog_access should fail in Core edition";
#endif
}

TEST_F(BootstrapTokenEnterpriseFixture, BootstrapTokenWithMultipleCatalogRulesRequiresEnterprise) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  // Create a bootstrap token with multiple catalog_access rules
  std::string catalog_access = R"([
    {"catalog": "main", "access": "write"},
    {"catalog": "analytics", "access": "read"},
    {"catalog": "*", "access": "none"}
  ])";
  std::string token = CreateBootstrapToken(keys_, "data_analyst", "user", catalog_access);

  auto result = AuthenticateWithBootstrapToken(*client, token);

#ifdef GIZMOSQL_ENTERPRISE
  bool has_enterprise = gizmosql::enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable();

  if (has_enterprise) {
    ASSERT_TRUE(result.ok())
        << "Bootstrap token with multiple catalog rules should succeed with enterprise license: "
        << result.status().ToString();
  } else {
    ASSERT_FALSE(result.ok())
        << "Bootstrap token with catalog_access should fail without enterprise license";
  }
#else
  ASSERT_FALSE(result.ok())
      << "Bootstrap token with catalog_access should fail in Core edition";
#endif
}

// ============================================================================
// Error Message Tests
// ============================================================================

TEST_F(BootstrapTokenEnterpriseFixture, CatalogAccessRejectionHasHelpfulMessage) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

#ifdef GIZMOSQL_ENTERPRISE
  // Skip if enterprise features are available
  if (gizmosql::enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    GTEST_SKIP() << "Skipping error message test - enterprise features are available";
  }
#endif

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  std::string catalog_access = R"([{"catalog": "*", "access": "read"}])";
  std::string token = CreateBootstrapToken(keys_, "test_user", "user", catalog_access);

  auto result = AuthenticateWithBootstrapToken(*client, token);

  ASSERT_FALSE(result.ok()) << "Should fail without enterprise license";

  std::string error_msg = result.status().ToString();

  // Check that the error message is helpful
  EXPECT_TRUE(error_msg.find("catalog") != std::string::npos ||
              error_msg.find("Catalog") != std::string::npos)
      << "Error should mention catalog: " << error_msg;

  EXPECT_TRUE(error_msg.find("Enterprise") != std::string::npos ||
              error_msg.find("enterprise") != std::string::npos)
      << "Error should mention enterprise: " << error_msg;
}
