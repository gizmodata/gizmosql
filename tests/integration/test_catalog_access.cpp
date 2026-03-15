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
#include <algorithm>
#include <set>
#include <thread>
#include <chrono>
#include <random>
#include <sstream>

#include <jwt-cpp/jwt.h>

#include "arrow/flight/types.h"
#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// Helper to check if enterprise license is available for catalog_permissions tests
bool HasEnterpriseLicense() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

// Macro to skip tests that require enterprise catalog_permissions feature
#define SKIP_WITHOUT_LICENSE() \
  if (!HasEnterpriseLicense()) { \
    GTEST_SKIP() << "Catalog permissions is an enterprise feature. " \
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable."; \
  }

// Test secret key - must match the one used by the test server fixture
const std::string kTestSecretKey = "test_secret_key_for_testing";
const std::string kServerJWTIssuer = "gizmosql";
// The catalog name comes from the database filename without .db extension
const std::string kDefaultCatalog = "catalog_access_test";

// Helper to execute a statement and consume the result (required for DDL/DML to actually run)
arrow::Status ExecuteAndConsume(FlightSqlClient& client,
                                 const arrow::flight::FlightCallOptions& call_options,
                                 const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(call_options, query));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return arrow::Status::OK();
}

// Simple UUID generation for tests (avoids Boost dependency issues)
std::string GenerateTestUUID() {
  static std::random_device rd;
  static std::mt19937 gen(rd());
  static std::uniform_int_distribution<> dis(0, 15);
  static std::uniform_int_distribution<> dis2(8, 11);

  std::stringstream ss;
  ss << std::hex;
  for (int i = 0; i < 8; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 4; i++) ss << dis(gen);
  ss << "-4";  // Version 4
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  ss << dis2(gen);  // Variant
  for (int i = 0; i < 3; i++) ss << dis(gen);
  ss << "-";
  for (int i = 0; i < 12; i++) ss << dis(gen);

  return ss.str();
}

// Helper to create a JWT token with catalog_access claims
std::string CreateTestJWT(const std::string& username, const std::string& role,
                          const std::string& catalog_access_json = "") {
  auto builder = jwt::create()
                     .set_issuer(kServerJWTIssuer)
                     .set_type("JWT")
                     .set_id("test-" + GenerateTestUUID())
                     .set_issued_at(std::chrono::system_clock::now())
                     .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{24})
                     .set_payload_claim("sub", jwt::claim(username))
                     .set_payload_claim("role", jwt::claim(role))
                     .set_payload_claim("auth_method", jwt::claim(std::string("TestToken")))
                     .set_payload_claim("session_id", jwt::claim(GenerateTestUUID()));

  // Add catalog_access claim if provided
  if (!catalog_access_json.empty()) {
    // Parse the JSON and add as claim
    picojson::value v;
    std::string err = picojson::parse(v, catalog_access_json);
    if (err.empty()) {
      builder = builder.set_payload_claim("catalog_access", jwt::claim(v));
    }
  }

  return builder.sign(jwt::algorithm::hs256{kTestSecretKey});
}

// ============================================================================
// Test Fixture for Catalog Access Control Tests
// ============================================================================

class CatalogAccessServerFixture
    : public gizmosql::testing::ServerTestFixture<CatalogAccessServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "catalog_access_test.db",
        .port = 31345,
        .health_port = 31346,
        .username = "tester",
        .password = "tester",
    };
  }

 protected:
  // Helper to create a FlightSqlClient with a custom JWT token
  arrow::Result<std::unique_ptr<FlightSqlClient>> CreateClientWithToken(
      const std::string& token) {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client,
                          arrow::flight::FlightClient::Connect(location, options));
    return std::make_unique<FlightSqlClient>(std::move(client));
  }

  // Helper to get call options with a Bearer token
  arrow::flight::FlightCallOptions GetCallOptionsWithToken(const std::string& token) {
    arrow::flight::FlightCallOptions call_options;
    call_options.headers.push_back({"authorization", "Bearer " + token});
    return call_options;
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CatalogAccessServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<CatalogAccessServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CatalogAccessServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CatalogAccessServerFixture>::config_{};

// ============================================================================
// System User Full Access Tests (Backward Compatibility)
// Users authenticated with system user/password have full access
// ============================================================================

TEST_F(CatalogAccessServerFixture, SystemUserHasFullAccess) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // System user should be able to create and write to any catalog
  ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "CREATE TABLE sys_test_table (id INTEGER)"));
  // System user should be able to read
  ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "SELECT * FROM sys_test_table"));
  // Clean up
  ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "DROP TABLE sys_test_table"));
}

TEST_F(CatalogAccessServerFixture, SystemUserCanReadInstrumentation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // System user (admin) should be able to read instrumentation
  ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "SELECT * FROM _gizmosql_instr.sessions LIMIT 1"));
}

TEST_F(CatalogAccessServerFixture, SystemUserCannotWriteInstrumentation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));

  // Even system user (admin) should not be able to write to instrumentation
  auto result = sql_client.Execute(call_options, "DELETE FROM _gizmosql_instr.sessions WHERE 1=0");
  ASSERT_FALSE(result.ok()) << "Admin should not be able to modify instrumentation";
}

// ============================================================================
// Token with No catalog_access Claim (Backward Compatibility)
// ============================================================================

TEST_F(CatalogAccessServerFixture, TokenWithoutCatalogAccessHasFullAccess) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string token = CreateTestJWT("test_user", "user");
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // User should have full access when no catalog_access rules are defined
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "CREATE TABLE test_no_rules (id INTEGER)"));
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT * FROM test_no_rules"));
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "DROP TABLE test_no_rules"));
}

// ============================================================================
// Token with catalog_access Rules - Basic Enforcement
// ============================================================================

TEST_F(CatalogAccessServerFixture, TokenWithReadOnlyAccessCannotWrite) {
  SKIP_WITHOUT_LICENSE();  // Token-based catalog permissions require enterprise license
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // First, create a table using system credentials
  {
    arrow::flight::FlightClientOptions options;
    ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                               arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(location, options));
    arrow::flight::FlightCallOptions call_options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options.headers.push_back(bearer);
    FlightSqlClient sql_client(std::move(client));
    ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "CREATE TABLE IF NOT EXISTS readonly_test (id INTEGER)"));
  }

  // Create a token with read-only access to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}])";
  std::string token = CreateTestJWT("readonly_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should be able to read
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT * FROM readonly_test"));

  // Should NOT be able to write
  auto write_result = client->Execute(call_options, "INSERT INTO readonly_test VALUES (1)");
  ASSERT_FALSE(write_result.ok()) << "Read-only user should not be able to write";
  ASSERT_TRUE(write_result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << write_result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, TokenWithNoAccessCannotRead) {
  SKIP_WITHOUT_LICENSE();  // Token-based catalog permissions require enterprise license
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // First, create a table using system credentials
  {
    arrow::flight::FlightClientOptions options;
    ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                               arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(location, options));
    arrow::flight::FlightCallOptions call_options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options.headers.push_back(bearer);
    FlightSqlClient sql_client(std::move(client));
    ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options, "CREATE TABLE IF NOT EXISTS noaccess_test (id INTEGER)"));
  }

  // Create a token with no access to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "none"}])";
  std::string token = CreateTestJWT("noaccess_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should NOT be able to read
  auto read_result = client->Execute(call_options, "SELECT * FROM noaccess_test");
  ASSERT_FALSE(read_result.ok()) << "No-access user should not be able to read";
  ASSERT_TRUE(read_result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << read_result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, TokenWithWriteAccessCanReadAndWrite) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with write access to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "write"}])";
  std::string token = CreateTestJWT("write_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should be able to write
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "CREATE TABLE write_access_test (id INTEGER)"));
  // Should be able to read
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT * FROM write_access_test"));
  // Clean up
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "DROP TABLE write_access_test"));
}

// ============================================================================
// Wildcard Rules Tests
// ============================================================================

TEST_F(CatalogAccessServerFixture, WildcardReadOnlyAccessDeniesWrite) {
  SKIP_WITHOUT_LICENSE();  // Token-based catalog permissions require enterprise license
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with wildcard read-only access
  std::string catalog_access = R"([{"catalog": "*", "access": "read"}])";
  std::string token = CreateTestJWT("wildcard_readonly", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should NOT be able to write anywhere
  auto result = client->Execute(call_options, "CREATE TABLE wildcard_test (id INTEGER)");
  ASSERT_FALSE(result.ok()) << "Wildcard read-only user should not be able to create tables";
  ASSERT_TRUE(result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, WildcardWriteAccessAllowsAll) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with wildcard write access
  std::string catalog_access = R"([{"catalog": "*", "access": "write"}])";
  std::string token = CreateTestJWT("wildcard_write", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should be able to write
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "CREATE TABLE wildcard_write_test (id INTEGER)"));
  // Should be able to read
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT * FROM wildcard_write_test"));
  // Clean up
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "DROP TABLE wildcard_write_test"));
}

// ============================================================================
// First-Match-Wins Semantics Tests
// ============================================================================

TEST_F(CatalogAccessServerFixture, FirstMatchWinsSpecificBeforeWildcard) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with specific catalog override before wildcard
  std::string catalog_access = R"([
    {"catalog": ")" + kDefaultCatalog + R"(", "access": "write"},
    {"catalog": "*", "access": "read"}
  ])";
  std::string token = CreateTestJWT("first_match_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should be able to write to default catalog (first match is default_catalog=write)
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "CREATE TABLE first_match_test (id INTEGER)"));
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "DROP TABLE first_match_test"));
}

TEST_F(CatalogAccessServerFixture, FirstMatchWinsWildcardDeniesSpecificAfter) {
  SKIP_WITHOUT_LICENSE();  // Token-based catalog permissions require enterprise license
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with wildcard before specific (wildcard wins)
  std::string catalog_access = R"([
    {"catalog": "*", "access": "read"},
    {"catalog": ")" + kDefaultCatalog + R"(", "access": "write"}
  ])";
  std::string token = CreateTestJWT("wildcard_first_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should NOT be able to write (wildcard read matches first)
  auto result = client->Execute(call_options, "CREATE TABLE wildcard_first_test (id INTEGER)");
  ASSERT_FALSE(result.ok()) << "Wildcard should match first and deny write";
  ASSERT_TRUE(result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << result.status().ToString();
}

// ============================================================================
// Instrumentation Database Protection Tests
// ============================================================================

TEST_F(CatalogAccessServerFixture, NonAdminCannotReadInstrumentation) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with user role (non-admin) and full wildcard access
  std::string catalog_access = R"([{"catalog": "*", "access": "write"}])";
  std::string token = CreateTestJWT("non_admin_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Non-admin should NOT be able to read instrumentation even with wildcard
  auto result = client->Execute(call_options, "SELECT * FROM _gizmosql_instr.sessions LIMIT 1");
  ASSERT_FALSE(result.ok()) << "Non-admin should not be able to read instrumentation";
  ASSERT_TRUE(result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, AdminCanReadInstrumentationWithToken) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with admin role
  std::string catalog_access = R"([{"catalog": "*", "access": "write"}])";
  std::string token = CreateTestJWT("admin_user", "admin", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Admin should be able to read instrumentation
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT * FROM _gizmosql_instr.sessions LIMIT 1"));
}

TEST_F(CatalogAccessServerFixture, AdminCannotWriteInstrumentationWithToken) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with admin role and full wildcard write access
  std::string catalog_access = R"([{"catalog": "*", "access": "write"}])";
  std::string token = CreateTestJWT("admin_writer", "admin", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Even admin should NOT be able to write to instrumentation
  auto result = client->Execute(call_options, "DELETE FROM _gizmosql_instr.sessions WHERE 1=0");
  ASSERT_FALSE(result.ok()) << "Admin should not be able to modify instrumentation";
  ASSERT_TRUE(result.status().ToString().find("Access denied") != std::string::npos)
      << "Expected access denied error: " << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, InstrumentationProtectionIgnoresTokenRules) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with explicit write access to instrumentation catalog
  std::string catalog_access = R"([{"catalog": "_gizmosql_instr", "access": "write"}])";
  std::string token = CreateTestJWT("instr_hacker", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Should NOT be able to read (non-admin)
  auto read_result = client->Execute(call_options, "SELECT * FROM _gizmosql_instr.sessions LIMIT 1");
  ASSERT_FALSE(read_result.ok())
      << "Non-admin should not be able to read instrumentation even with explicit token rule";

  // Should NOT be able to write
  auto write_result = client->Execute(call_options, "DELETE FROM _gizmosql_instr.sessions WHERE 1=0");
  ASSERT_FALSE(write_result.ok())
      << "Should not be able to modify instrumentation even with explicit token rule";
}

// ============================================================================
// Catalog Visibility Filtering Tests (Metadata Row Filtering)
// These tests verify that metadata queries only show authorized catalogs.
// ============================================================================

// Helper to execute a query and return the result as a Table
arrow::Result<std::shared_ptr<arrow::Table>> ExecuteToTable(
    FlightSqlClient& client,
    const arrow::flight::FlightCallOptions& call_options,
    const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(call_options, query));
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    for (int i = 0; i < table->num_columns(); ++i) {
      // Collect all batches
    }
    // Return the first endpoint's table (typically there's only one)
    return table;
  }
  return arrow::Status::Invalid("No endpoints in result");
}

// Helper to get column values as strings from a table
std::vector<std::string> GetColumnValues(const std::shared_ptr<arrow::Table>& table,
                                          int column_index) {
  std::vector<std::string> values;
  auto chunked = table->column(column_index);
  for (int chunk = 0; chunk < chunked->num_chunks(); ++chunk) {
    auto array = std::static_pointer_cast<arrow::StringArray>(chunked->chunk(chunk));
    for (int64_t i = 0; i < array->length(); ++i) {
      if (!array->IsNull(i)) {
        values.push_back(array->GetString(i));
      }
    }
  }
  return values;
}

TEST_F(CatalogAccessServerFixture, ShowDatabasesFiltersUnauthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("visibility_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // SHOW DATABASES should only return allowed catalogs + system/temp
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
  auto db_names = GetColumnValues(table, 0);

  // Should contain the default catalog
  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), kDefaultCatalog) != db_names.end())
      << "Should see the authorized catalog";

  // Should contain system and temp
  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), "system") != db_names.end())
      << "Should always see 'system' catalog";
  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), "temp") != db_names.end())
      << "Should always see 'temp' catalog";
}

TEST_F(CatalogAccessServerFixture, ShowSchemasFiltersUnauthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("show_schemas_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // SHOW SCHEMAS should only return schemas from allowed catalogs + system/temp
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW SCHEMAS"));
  // Column 0 is database_name
  auto db_names = GetColumnValues(table, 0);

  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& db : db_names) {
    ASSERT_TRUE(allowed_set.count(db) > 0)
        << "Unexpected catalog '" << db << "' in SHOW SCHEMAS result";
  }
}

TEST_F(CatalogAccessServerFixture, UseUnauthorizedCatalogIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("use_denied_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  auto result = client->Execute(call_options, "USE _gizmosql_instr");
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().ToString().find("Access denied") != std::string::npos)
      << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, UseUnauthorizedRegularCatalogIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("use_regular_denied_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  auto result = client->Execute(call_options, "USE " + kDefaultCatalog);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().ToString().find("read access to catalog") != std::string::npos)
      << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, UseQuotedUnauthorizedCatalogIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("use_quoted_denied_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  auto result = client->Execute(call_options, "USE \"" + kDefaultCatalog + "\"");
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().ToString().find("read access to catalog") != std::string::npos)
      << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, SetSessionOptionsUnauthorizedCatalogIsDenied) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("set_catalog_denied_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));
  ASSERT_ARROW_OK(ExecuteAndConsume(*client, call_options, "SELECT 1"));

  arrow::flight::SetSessionOptionsRequest request(
      {{"catalog", std::string("\"" + kDefaultCatalog + "\"")}});
  auto result = client->SetSessionOptions(call_options, request);
  ASSERT_FALSE(result.ok());
  ASSERT_TRUE(result.status().ToString().find("read access to catalog") != std::string::npos)
      << result.status().ToString();
}

TEST_F(CatalogAccessServerFixture, InformationSchemaTablesFiltersUnauthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // First, create a table using system credentials so there's something to see
  {
    arrow::flight::FlightClientOptions options;
    ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                               arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(location, options));
    arrow::flight::FlightCallOptions sys_call_options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    sys_call_options.headers.push_back(bearer);
    FlightSqlClient sql_client(std::move(client));
    ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, sys_call_options, "CREATE TABLE IF NOT EXISTS visibility_test (id INTEGER)"));
  }

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("visibility_user2", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Query information_schema.tables — should only show tables from authorized catalogs
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
      ExecuteToTable(*client, call_options, "SELECT table_catalog, table_name FROM information_schema.tables"));

  // All returned rows should have table_catalog in the allowed set
  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat << "' in information_schema.tables result";
  }
}

TEST_F(CatalogAccessServerFixture, CatalogQualifiedInformationSchemaFilters) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("catalog_qualified_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Query with catalog-qualified reference: system.information_schema.tables
  // This should still be filtered, not break the SQL
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT table_catalog, table_name FROM system.information_schema.tables"));

  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat << "' in system.information_schema.tables result";
  }
}

TEST_F(CatalogAccessServerFixture, QuotedInformationSchemaFiltersUnauthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("quoted_visibility_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT table_catalog, table_name FROM "
                     "\"information_schema\".\"tables\""));

  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat
        << "' in quoted information_schema.tables result";
  }
}

TEST_F(CatalogAccessServerFixture,
       QuotedInformationSchemaOrderByFiltersUnauthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog +
                               R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token =
      CreateTestJWT("quoted_visibility_order_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT DISTINCT table_catalog FROM "
                     "\"information_schema\".\"tables\" ORDER BY 1"));

  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat
        << "' in quoted information_schema.tables ORDER BY result";
  }
}

TEST_F(CatalogAccessServerFixture, QuotedCatalogQualifiedInformationSchemaFilters) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("quoted_catalog_visibility_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT table_catalog, table_name FROM "
                     "\"system\".\"information_schema\".\"tables\""));

  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat
        << "' in quoted system.information_schema.tables result";
  }
}

TEST_F(CatalogAccessServerFixture,
       QuotedCatalogQualifiedInformationSchemaOrderByFilters) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog +
                               R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("quoted_catalog_visibility_order_user", "user",
                                    catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table,
      ExecuteToTable(*client, call_options,
                     "SELECT DISTINCT table_catalog FROM "
                     "\"system\".\"information_schema\".\"tables\" ORDER BY 1"));

  auto catalogs = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& cat : catalogs) {
    ASSERT_TRUE(allowed_set.count(cat) > 0)
        << "Unexpected catalog '" << cat
        << "' in quoted system.information_schema.tables ORDER BY result";
  }
}

TEST_F(CatalogAccessServerFixture, DuckdbTablesFiltersByAuthorizedCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("visibility_user3", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // Query duckdb_tables() — should only show tables from authorized catalogs
  ASSERT_ARROW_OK_AND_ASSIGN(auto table,
      ExecuteToTable(*client, call_options, "SELECT database_name, table_name FROM duckdb_tables()"));

  auto db_names = GetColumnValues(table, 0);
  std::set<std::string> allowed_set = {kDefaultCatalog, "system", "temp"};
  for (const auto& db : db_names) {
    ASSERT_TRUE(allowed_set.count(db) > 0)
        << "Unexpected database '" << db << "' in duckdb_tables() result";
  }
}

TEST_F(CatalogAccessServerFixture, WildcardNoneOnlyShowsSystemAndTemp) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with wildcard none access — only system/temp should be visible
  std::string catalog_access = R"([{"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("none_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // SHOW DATABASES should only return system and temp
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
  auto db_names = GetColumnValues(table, 0);

  std::set<std::string> allowed_set = {"system", "temp"};
  for (const auto& db : db_names) {
    ASSERT_TRUE(allowed_set.count(db) > 0)
        << "Unexpected catalog '" << db << "' visible with wildcard none access";
  }
}

TEST_F(CatalogAccessServerFixture, WildcardReadShowsAllCatalogs) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with wildcard read access — all catalogs should be visible
  std::string catalog_access = R"([{"catalog": "*", "access": "read"}])";
  std::string token = CreateTestJWT("read_all_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // SHOW DATABASES should show the default catalog (at minimum)
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
  auto db_names = GetColumnValues(table, 0);

  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), kDefaultCatalog) != db_names.end())
      << "Wildcard read should show the default catalog";
  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), "system") != db_names.end())
      << "Wildcard read should show system catalog";
}

TEST_F(CatalogAccessServerFixture, NoCatalogAccessMeansNoFiltering) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token WITHOUT catalog_access — backward compat, no filtering
  std::string token = CreateTestJWT("no_rules_user", "user");
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // SHOW DATABASES should show the default catalog (no filtering applied)
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
  auto db_names = GetColumnValues(table, 0);

  ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), kDefaultCatalog) != db_names.end())
      << "Without catalog_access rules, all catalogs should be visible";
}

TEST_F(CatalogAccessServerFixture, CatalogAccessRulesIgnoredWithoutEnterpriseLicense) {
  // NOTE: No SKIP_WITHOUT_LICENSE() — this test is important for Core edition
  // and Enterprise builds without a license. Even if a token has catalog_access
  // rules, they should be ignored when the enterprise feature is not licensed,
  // so all catalogs remain visible (backward compatible behavior).
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token WITH catalog_access rules that would deny access to the default catalog
  std::string catalog_access = R"([{"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("core_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  if (!HasEnterpriseLicense()) {
    // Without enterprise license: catalog_access rules should be ignored entirely,
    // so the default catalog should still be visible in SHOW DATABASES
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
    auto db_names = GetColumnValues(table, 0);
    ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), kDefaultCatalog) != db_names.end())
        << "Without enterprise license, catalog_access rules should be ignored";
  } else {
    // With enterprise license: catalog_access rules ARE enforced, so wildcard none
    // means only system/temp are visible — the default catalog should NOT appear
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, ExecuteToTable(*client, call_options, "SHOW DATABASES"));
    auto db_names = GetColumnValues(table, 0);
    ASSERT_TRUE(std::find(db_names.begin(), db_names.end(), kDefaultCatalog) == db_names.end())
        << "With enterprise license, wildcard none should hide the default catalog";
  }
}

TEST_F(CatalogAccessServerFixture, GetCatalogsRPCRespectsFiltering) {
  SKIP_WITHOUT_LICENSE();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Create a token with access only to the default catalog
  std::string catalog_access = R"([{"catalog": ")" + kDefaultCatalog + R"(", "access": "read"}, {"catalog": "*", "access": "none"}])";
  std::string token = CreateTestJWT("rpc_filter_user", "user", catalog_access);
  auto call_options = GetCallOptionsWithToken(token);

  ASSERT_ARROW_OK_AND_ASSIGN(auto client, CreateClientWithToken(token));

  // GetCatalogs RPC goes through DuckDBStatement::Create, so filtering applies
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, client->GetCatalogs(call_options));
  std::vector<std::string> catalog_names;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader, client->DoGet(call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
    auto names = GetColumnValues(table, 0);
    catalog_names.insert(catalog_names.end(), names.begin(), names.end());
  }

  // Should contain the authorized catalog
  ASSERT_TRUE(std::find(catalog_names.begin(), catalog_names.end(), kDefaultCatalog) != catalog_names.end())
      << "GetCatalogs should include authorized catalog";

  // Should contain system and temp
  ASSERT_TRUE(std::find(catalog_names.begin(), catalog_names.end(), "system") != catalog_names.end())
      << "GetCatalogs should always include 'system'";
  ASSERT_TRUE(std::find(catalog_names.begin(), catalog_names.end(), "temp") != catalog_names.end())
      << "GetCatalogs should always include 'temp'";
}
