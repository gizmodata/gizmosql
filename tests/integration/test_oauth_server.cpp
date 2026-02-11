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

// Integration tests for the server-side OAuth code exchange feature.
// Uses a mock IdP HTTP server to simulate the full OAuth flow.

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <random>
#include <sstream>
#include <iomanip>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include <jwt-cpp/jwt.h>
#include <nlohmann/json.hpp>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/hmac.h>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/util/logging.h"
#include "test_server_fixture.h"

#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/enterprise_features.h"
#endif

namespace fs = std::filesystem;

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Test Constants
// ============================================================================
static const int kMockIdPPort = 31395;
static const int kOAuthTestPort = 31396;
static const int kFlightTestPort = 31397;
static const int kHealthTestPort = 31398;

static const std::string kTestIssuer = "http://localhost:" + std::to_string(kMockIdPPort);
static const std::string kTestAudience = "gizmosql-oauth-test";
static const std::string kTestClientId = "test-client-id";
static const std::string kTestClientSecret = "test-client-secret";
static const std::string kTestKid = "test-key-1";
static const std::string kSecretKey = "test_secret_key_for_oauth_testing";

// ============================================================================
// RSA Key Pair Generation for Mock IdP
// ============================================================================
struct RSAKeyPair {
  std::string private_key_pem;
  std::string public_key_pem;
};

static RSAKeyPair GenerateTestRSAKeyPair() {
  RSAKeyPair result;

  EVP_PKEY_CTX* ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
  EVP_PKEY_keygen_init(ctx);
  EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048);

  EVP_PKEY* pkey = nullptr;
  EVP_PKEY_keygen(ctx, &pkey);
  EVP_PKEY_CTX_free(ctx);

  BIO* private_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(private_bio, pkey, nullptr, nullptr, 0, nullptr, nullptr);
  char* private_data = nullptr;
  long private_len = BIO_get_mem_data(private_bio, &private_data);
  result.private_key_pem = std::string(private_data, private_len);
  BIO_free(private_bio);

  BIO* public_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PUBKEY(public_bio, pkey);
  char* public_data = nullptr;
  long public_len = BIO_get_mem_data(public_bio, &public_data);
  result.public_key_pem = std::string(public_data, public_len);
  BIO_free(public_bio);

  EVP_PKEY_free(pkey);
  return result;
}

// ============================================================================
// HMAC-SHA256 helper (same as SecurityUtilities::HMAC_SHA256)
// ============================================================================
static std::string HMAC_SHA256_Hex(const std::string& key, const std::string& data) {
  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int hash_len = 0;

  HMAC(EVP_sha256(), key.c_str(), static_cast<int>(key.length()),
       reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), hash,
       &hash_len);

  std::stringstream ss;
  for (unsigned int i = 0; i < hash_len; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
  }
  return ss.str();
}

// ============================================================================
// UUID generation for tests
// ============================================================================
static std::string GenerateUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  const char* hex = "0123456789abcdef";
  std::string uuid(36, '-');
  for (int i = 0; i < 36; i++) {
    if (i == 8 || i == 13 || i == 18 || i == 23) continue;
    if (i == 14) {
      uuid[i] = '4';
    } else if (i == 19) {
      uuid[i] = hex[dis2(gen)];
    } else {
      uuid[i] = hex[dis(gen)];
    }
  }
  return uuid;
}

// ============================================================================
// Extract RSA public key components (n, e) from PEM for JWKS
// ============================================================================
static std::pair<std::string, std::string> ExtractRSAComponents(const std::string& pub_pem) {
  BIO* bio = BIO_new_mem_buf(pub_pem.c_str(), -1);
  EVP_PKEY* pkey = PEM_read_bio_PUBKEY(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);

  BIGNUM* n_bn = nullptr;
  BIGNUM* e_bn = nullptr;
  EVP_PKEY_get_bn_param(pkey, "n", &n_bn);
  EVP_PKEY_get_bn_param(pkey, "e", &e_bn);

  // Convert to base64url
  auto bn_to_base64url = [](BIGNUM* bn) -> std::string {
    int len = BN_num_bytes(bn);
    std::vector<unsigned char> buf(len);
    BN_bn2bin(bn, buf.data());

    // Base64url encode
    static const char table[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    std::string result;
    int val = 0, valb = -6;
    for (unsigned char c : buf) {
      val = (val << 8) + c;
      valb += 8;
      while (valb >= 0) {
        result.push_back(table[(val >> valb) & 0x3F]);
        valb -= 6;
      }
    }
    if (valb > -6) result.push_back(table[((val << 8) >> (valb + 8)) & 0x3F]);
    return result;
  };

  auto n_str = bn_to_base64url(n_bn);
  auto e_str = bn_to_base64url(e_bn);

  BN_free(n_bn);
  BN_free(e_bn);
  EVP_PKEY_free(pkey);

  return {n_str, e_str};
}

// ============================================================================
// Mock IdP HTTP Server
// ============================================================================
class MockIdPServer {
 public:
  MockIdPServer(const RSAKeyPair& keys, int port)
      : keys_(keys), port_(port), server_() {}

  void Start() {
    auto [n, e] = ExtractRSAComponents(keys_.public_key_pem);

    // OIDC Discovery
    server_.Get("/.well-known/openid-configuration",
                [this](const httplib::Request&, httplib::Response& res) {
                  nlohmann::json discovery;
                  discovery["issuer"] = kTestIssuer;
                  discovery["authorization_endpoint"] =
                      kTestIssuer + "/authorize";
                  discovery["token_endpoint"] = kTestIssuer + "/token";
                  discovery["jwks_uri"] = kTestIssuer + "/jwks";
                  res.set_content(discovery.dump(), "application/json");
                });

    // JWKS endpoint
    server_.Get("/jwks", [this, n, e](const httplib::Request&, httplib::Response& res) {
      nlohmann::json jwks;
      jwks["keys"] = nlohmann::json::array();
      nlohmann::json key;
      key["kty"] = "RSA";
      key["kid"] = kTestKid;
      key["alg"] = "RS256";
      key["use"] = "sig";
      key["n"] = n;
      key["e"] = e;
      jwks["keys"].push_back(key);
      res.set_content(jwks.dump(), "application/json");
    });

    // Authorization endpoint (simulates redirect — not used in tests directly)
    server_.Get("/authorize", [](const httplib::Request&, httplib::Response& res) {
      res.status = 200;
      res.set_content("Mock authorization page", "text/html");
    });

    // Token endpoint
    server_.Post("/token",
                 [this](const httplib::Request& req, httplib::Response& res) {
                   HandleTokenExchange(req, res);
                 });

    server_thread_ = std::thread([this]() { server_.listen("0.0.0.0", port_); });
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void Stop() {
    server_.stop();
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  /// Set the authorization code that will be accepted
  void SetValidCode(const std::string& code) { valid_code_ = code; }

  /// Set the email to include in the ID token
  void SetEmail(const std::string& email) { email_ = email; }

 private:
  void HandleTokenExchange(const httplib::Request& req, httplib::Response& res) {
    // Parse form-encoded body
    auto code_it = req.params.find("code");
    auto client_id_it = req.params.find("client_id");
    auto client_secret_it = req.params.find("client_secret");

    if (code_it == req.params.end() || client_id_it == req.params.end() ||
        client_secret_it == req.params.end()) {
      nlohmann::json error;
      error["error"] = "invalid_request";
      error["error_description"] = "Missing required parameters";
      res.status = 400;
      res.set_content(error.dump(), "application/json");
      return;
    }

    // Verify client credentials
    if (client_id_it->second != kTestClientId ||
        client_secret_it->second != kTestClientSecret) {
      nlohmann::json error;
      error["error"] = "invalid_client";
      res.status = 401;
      res.set_content(error.dump(), "application/json");
      return;
    }

    // Verify authorization code
    if (code_it->second != valid_code_) {
      nlohmann::json error;
      error["error"] = "invalid_grant";
      error["error_description"] = "Invalid authorization code";
      res.status = 400;
      res.set_content(error.dump(), "application/json");
      return;
    }

    // Generate ID token
    auto id_token = jwt::create()
                        .set_issuer(kTestIssuer)
                        .set_type("JWT")
                        .set_key_id(kTestKid)
                        .set_audience(kTestAudience)
                        .set_subject("test-user-123")
                        .set_payload_claim("email", jwt::claim(email_))
                        .set_payload_claim("role", jwt::claim(std::string("admin")))
                        .set_issued_at(std::chrono::system_clock::now())
                        .set_expires_at(std::chrono::system_clock::now() +
                                        std::chrono::hours(1))
                        .sign(jwt::algorithm::rs256("", keys_.private_key_pem, "", ""));

    nlohmann::json token_response;
    token_response["access_token"] = "mock-access-token";
    token_response["token_type"] = "Bearer";
    token_response["expires_in"] = 3600;
    token_response["id_token"] = id_token;

    res.set_content(token_response.dump(), "application/json");
  }

  RSAKeyPair keys_;
  int port_;
  httplib::Server server_;
  std::thread server_thread_;
  std::string valid_code_ = "test-auth-code";
  std::string email_ = "testuser@gizmodata.com";
};

// ============================================================================
// Test Fixture
// ============================================================================

#ifdef GIZMOSQL_ENTERPRISE

class OAuthServerTest : public ::testing::Test {
 protected:
  static RSAKeyPair keys_;
  static std::unique_ptr<MockIdPServer> mock_idp_;
  static std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server_;
  static std::thread server_thread_;
  static std::atomic<bool> server_ready_;

  static void SetUpTestSuite() {
    // Initialize enterprise features
    const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
    std::string license_path = license_file ? license_file : "";
    auto license_status =
        gizmosql::enterprise::EnterpriseFeatures::Instance().Initialize(license_path);

    if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsExternalAuthAvailable()) {
      GTEST_SKIP() << "External auth enterprise feature not available";
      return;
    }

    // Generate test RSA keys
    keys_ = GenerateTestRSAKeyPair();

    // Start mock IdP
    mock_idp_ = std::make_unique<MockIdPServer>(keys_, kMockIdPPort);
    mock_idp_->Start();

    // Clean up any existing database
    std::error_code ec;
    fs::remove("oauth_test.db", ec);
    fs::remove("oauth_test.db.wal", ec);

    // Create server with OAuth enabled
    fs::path db_path("oauth_test.db");
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", kFlightTestPort,
        /*username=*/"admin",
        /*password=*/"admin",
        /*secret_key=*/kSecretKey,
        /*tls_cert_path=*/fs::path(),
        /*tls_key_path=*/fs::path(),
        /*mtls_ca_cert_path=*/fs::path(),
        /*init_sql_commands=*/"",
        /*init_sql_commands_file=*/fs::path(),
        /*print_queries=*/false,
        /*read_only=*/false,
        /*token_allowed_issuer=*/kTestIssuer,
        /*token_allowed_audience=*/kTestAudience,
        /*token_signature_verify_cert_path=*/fs::path(),
        /*token_jwks_uri=*/"",
        /*token_default_role=*/"admin",
        /*token_authorized_emails=*/"*@gizmodata.com",
        /*access_logging_enabled=*/false,
        /*query_timeout=*/0,
        /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*health_port=*/kHealthTestPort,
        /*health_check_query=*/"",
        /*enable_instrumentation=*/false,
        /*instrumentation_db_path=*/"",
        /*instrumentation_catalog=*/"",
        /*instrumentation_schema=*/"",
        /*allow_cross_instance_tokens=*/false,
        /*oauth_client_id=*/kTestClientId,
        /*oauth_client_secret=*/kTestClientSecret,
        /*oauth_scopes=*/"openid profile email",
        /*oauth_port=*/kOAuthTestPort,
        /*oauth_redirect_uri=*/"",
        /*oauth_disable_tls=*/false);

    ASSERT_TRUE(result.ok()) << "Failed to create server: " << result.status().ToString();
    server_ = *result;

    server_thread_ = std::thread([&]() {
      server_ready_ = true;
      auto serve_status = server_->Serve();
      if (!serve_status.ok()) {
        std::cerr << "Server serve ended: " << serve_status.ToString() << std::endl;
      }
    });

    auto start = std::chrono::steady_clock::now();
    while (!server_ready_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      auto elapsed = std::chrono::steady_clock::now() - start;
      if (elapsed > std::chrono::seconds(10)) {
        FAIL() << "Server failed to start within timeout";
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
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

    if (mock_idp_) {
      mock_idp_->Stop();
      mock_idp_.reset();
    }

    gizmosql::CleanupServerResources();

    std::error_code ec;
    fs::remove("oauth_test.db", ec);
    fs::remove("oauth_test.db.wal", ec);
  }
};

// Static member definitions
RSAKeyPair OAuthServerTest::keys_;
std::unique_ptr<MockIdPServer> OAuthServerTest::mock_idp_;
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> OAuthServerTest::server_;
std::thread OAuthServerTest::server_thread_;
std::atomic<bool> OAuthServerTest::server_ready_{false};

// ============================================================================
// Test Cases
// ============================================================================

TEST_F(OAuthServerTest, SuccessfulOAuthFlow) {
  // 1. Call /oauth/initiate to get UUID and auth URL
  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  oauth_client.set_follow_location(false);

  auto initiate_res = oauth_client.Get("/oauth/initiate");
  ASSERT_TRUE(initiate_res) << "Failed to connect to OAuth server";
  ASSERT_EQ(initiate_res->status, 200) << "Initiate failed: " << initiate_res->body;

  auto initiate_json = nlohmann::json::parse(initiate_res->body);
  ASSERT_TRUE(initiate_json.contains("session_uuid"));
  ASSERT_TRUE(initiate_json.contains("auth_url"));

  std::string uuid = initiate_json["session_uuid"];
  std::string auth_url = initiate_json["auth_url"];
  ASSERT_FALSE(uuid.empty());
  ASSERT_FALSE(auth_url.empty());

  // Verify auth URL contains expected parameters
  ASSERT_TRUE(auth_url.find("/authorize") != std::string::npos)
      << "Auth URL should point to IdP authorization endpoint";
  ASSERT_TRUE(auth_url.find("client_id=" + kTestClientId) != std::string::npos)
      << "Auth URL should include client_id";

  // 2. Extract state (session hash) from the auth URL
  auto state_pos = auth_url.find("state=");
  ASSERT_TRUE(state_pos != std::string::npos) << "Auth URL should include state parameter";
  std::string session_hash = auth_url.substr(state_pos + 6);
  // Trim at next & if present
  auto amp_pos = session_hash.find('&');
  if (amp_pos != std::string::npos) {
    session_hash = session_hash.substr(0, amp_pos);
  }

  // 3. Simulate IdP callback with authorization code
  mock_idp_->SetValidCode("test-auth-code-123");
  mock_idp_->SetEmail("testuser@gizmodata.com");

  auto callback_res = oauth_client.Get(
      "/oauth/callback?code=test-auth-code-123&state=" + session_hash);
  ASSERT_TRUE(callback_res) << "Failed to call callback";
  ASSERT_EQ(callback_res->status, 200) << "Callback failed: " << callback_res->body;
  ASSERT_TRUE(callback_res->body.find("Authentication Successful") != std::string::npos)
      << "Expected success page, got: " << callback_res->body;

  // 4. Poll for token using the UUID from /oauth/initiate
  auto token_res = oauth_client.Get("/oauth/token/" + uuid);
  ASSERT_TRUE(token_res) << "Failed to poll for token";
  ASSERT_EQ(token_res->status, 200);

  auto token_json = nlohmann::json::parse(token_res->body);
  ASSERT_EQ(token_json["status"], "complete");
  ASSERT_TRUE(token_json.contains("token"));

  std::string id_token = token_json["token"];
  ASSERT_FALSE(id_token.empty());

  // 5. Verify the token is the raw IdP ID token (not a GizmoSQL JWT)
  auto decoded = jwt::decode(id_token);
  ASSERT_EQ(decoded.get_issuer(), kTestIssuer);
  ASSERT_TRUE(decoded.has_payload_claim("email"));
  ASSERT_EQ(decoded.get_payload_claim("email").as_string(), "testuser@gizmodata.com");

  // 6. Use the ID token via Basic Auth (username="token") on the Flight port.
  // VerifyAndDecodeBootstrapToken validates the IdP token and issues a session JWT.
  arrow::flight::FlightClientOptions options;
  auto location_result =
      arrow::flight::Location::ForGrpcTcp("localhost", kFlightTestPort);
  ASSERT_TRUE(location_result.ok()) << location_result.status().ToString();

  auto client_result =
      arrow::flight::FlightClient::Connect(*location_result, options);
  ASSERT_TRUE(client_result.ok()) << client_result.status().ToString();
  auto& client = *client_result;

  auto bearer_result = client->AuthenticateBasicToken({}, "token", id_token);
  ASSERT_TRUE(bearer_result.ok()) << "Basic auth with IdP ID token failed: "
                                  << bearer_result.status().ToString();

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(*bearer_result);

  auto flight_client = std::make_unique<FlightSqlClient>(std::move(client));
  auto info_result = flight_client->Execute(call_options, "SELECT 1 AS result");
  ASSERT_TRUE(info_result.ok()) << "Query after OAuth token auth failed: "
                                << info_result.status().ToString();
}

TEST_F(OAuthServerTest, InitiateEndpointReturnsValidJson) {
  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));

  auto res = oauth_client.Get("/oauth/initiate");
  ASSERT_TRUE(res);
  ASSERT_EQ(res->status, 200);

  auto json = nlohmann::json::parse(res->body);
  ASSERT_TRUE(json.contains("session_uuid"));
  ASSERT_TRUE(json.contains("auth_url"));

  // UUID should be in standard format (36 chars with dashes)
  std::string uuid = json["session_uuid"];
  ASSERT_EQ(uuid.size(), 36u);
  ASSERT_EQ(uuid[8], '-');
  ASSERT_EQ(uuid[13], '-');
  ASSERT_EQ(uuid[18], '-');
  ASSERT_EQ(uuid[23], '-');

  // Auth URL should contain expected components
  std::string auth_url = json["auth_url"];
  ASSERT_TRUE(auth_url.find("response_type=code") != std::string::npos);
  ASSERT_TRUE(auth_url.find("client_id=") != std::string::npos);
  ASSERT_TRUE(auth_url.find("redirect_uri=") != std::string::npos);
  ASSERT_TRUE(auth_url.find("state=") != std::string::npos);

  // Should be able to poll for the session (should be pending)
  auto token_res = oauth_client.Get("/oauth/token/" + uuid);
  ASSERT_TRUE(token_res);
  ASSERT_EQ(token_res->status, 200);

  auto token_json = nlohmann::json::parse(token_res->body);
  ASSERT_EQ(token_json["status"], "pending");
}

TEST_F(OAuthServerTest, InvalidAuthorizationCode) {
  std::string uuid = GenerateUUID();
  std::string session_hash = HMAC_SHA256_Hex(kSecretKey, uuid);

  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  oauth_client.set_follow_location(false);

  // Start the flow
  auto start_res = oauth_client.Get("/oauth/start?session=" + session_hash);
  ASSERT_EQ(start_res->status, 302);

  // Callback with invalid code
  auto callback_res = oauth_client.Get(
      "/oauth/callback?code=bad-code&state=" + session_hash);
  ASSERT_TRUE(callback_res);
  // Should show error page
  ASSERT_TRUE(callback_res->body.find("Authentication Failed") != std::string::npos ||
              callback_res->body.find("error") != std::string::npos);

  // Poll should show error
  auto token_res = oauth_client.Get("/oauth/token/" + uuid);
  ASSERT_TRUE(token_res);
  auto token_json = nlohmann::json::parse(token_res->body);
  ASSERT_EQ(token_json["status"], "error");
}

TEST_F(OAuthServerTest, UnauthorizedEmail) {
  std::string uuid = GenerateUUID();
  std::string session_hash = HMAC_SHA256_Hex(kSecretKey, uuid);

  // Set mock IdP to return unauthorized email
  mock_idp_->SetEmail("unauthorized@evil.com");
  mock_idp_->SetValidCode("unauth-code-456");

  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  oauth_client.set_follow_location(false);

  auto start_res = oauth_client.Get("/oauth/start?session=" + session_hash);
  ASSERT_EQ(start_res->status, 302);

  auto callback_res = oauth_client.Get(
      "/oauth/callback?code=unauth-code-456&state=" + session_hash);
  ASSERT_TRUE(callback_res);
  // Should show error about unauthorized email
  ASSERT_TRUE(callback_res->body.find("Authentication Failed") != std::string::npos);

  // Poll should show error
  auto token_res = oauth_client.Get("/oauth/token/" + uuid);
  ASSERT_TRUE(token_res);
  auto token_json = nlohmann::json::parse(token_res->body);
  ASSERT_EQ(token_json["status"], "error");

  // Restore valid email for other tests
  mock_idp_->SetEmail("testuser@gizmodata.com");
}

TEST_F(OAuthServerTest, TokenPollNotFound) {
  // Poll with a UUID that was never registered
  std::string random_uuid = GenerateUUID();

  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  auto res = oauth_client.Get("/oauth/token/" + random_uuid);
  ASSERT_TRUE(res);
  ASSERT_EQ(res->status, 404);

  auto json = nlohmann::json::parse(res->body);
  ASSERT_EQ(json["status"], "not_found");
}

TEST_F(OAuthServerTest, PendingSessionPoll) {
  std::string uuid = GenerateUUID();
  std::string session_hash = HMAC_SHA256_Hex(kSecretKey, uuid);

  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  oauth_client.set_follow_location(false);

  // Start the flow (creates pending session)
  auto start_res = oauth_client.Get("/oauth/start?session=" + session_hash);
  ASSERT_EQ(start_res->status, 302);

  // Poll before callback (should be pending)
  auto token_res = oauth_client.Get("/oauth/token/" + uuid);
  ASSERT_TRUE(token_res);
  ASSERT_EQ(token_res->status, 200);

  auto json = nlohmann::json::parse(token_res->body);
  ASSERT_EQ(json["status"], "pending");
}

TEST_F(OAuthServerTest, DuplicateSessionRejected) {
  std::string uuid = GenerateUUID();
  std::string session_hash = HMAC_SHA256_Hex(kSecretKey, uuid);

  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));
  oauth_client.set_follow_location(false);

  // Start the flow
  auto start_res = oauth_client.Get("/oauth/start?session=" + session_hash);
  ASSERT_EQ(start_res->status, 302);

  // Try to start again with the same session hash
  auto start_res2 = oauth_client.Get("/oauth/start?session=" + session_hash);
  ASSERT_EQ(start_res2->status, 409);
}

TEST_F(OAuthServerTest, MissingSessionParameter) {
  httplib::Client oauth_client("http://localhost:" + std::to_string(kOAuthTestPort));

  auto res = oauth_client.Get("/oauth/start");
  ASSERT_TRUE(res);
  ASSERT_EQ(res->status, 400);
}

#else
// Non-enterprise: skip OAuth tests
TEST(OAuthServerSkipped, SkippedNonEnterprise) {
  GTEST_SKIP() << "OAuth server tests require GIZMOSQL_ENTERPRISE build";
}
#endif
