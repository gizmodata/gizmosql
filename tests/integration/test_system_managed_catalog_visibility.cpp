// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
//
// System-managed catalogs (instrumentation, catalog logging) are admin-only.
// Reads were always denied to non-admins, but the catalogs still showed up in
// metadata listings for sessions without catalog_access rules, because the
// visibility filter only ran when rules were present. These tests pin the
// hardened behaviour: a non-admin never sees the instrumentation catalog in
// GetCatalogs / GetDbSchemas / GetTables, duckdb_*(), information_schema or
// SHOW output, while an admin still does.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

const std::string kInstrCatalog = "_gizmosql_instr";
// Must match the server fixture's JWT secret and issuer.
const std::string kTestSecretKey = "test_secret_key_for_testing";
const std::string kServerJWTIssuer = "gizmosql";

bool HasEnterpriseLicense() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

std::string RandomHex(size_t n) {
  static std::mt19937 gen{std::random_device{}()};
  static std::uniform_int_distribution<> dis(0, 15);
  std::stringstream ss;
  ss << std::hex;
  for (size_t i = 0; i < n; i++) ss << dis(gen);
  return ss.str();
}

// A server-issued-style JWT for a non-admin user with no catalog_access rules:
// the shape a read-only service account has.
std::string CreateNonAdminToken(const std::string& username) {
  return jwt::create()
      .set_issuer(kServerJWTIssuer)
      .set_type("JWT")
      .set_id("test-" + RandomHex(16))
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{1})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(std::string("user")))
      .set_payload_claim("auth_method", jwt::claim(std::string("TestToken")))
      .set_payload_claim("session_id", jwt::claim(RandomHex(32)))
      .sign(jwt::algorithm::hs256{kTestSecretKey});
}

}  // namespace

class SystemManagedCatalogVisibilityFixture
    : public gizmosql::testing::ServerTestFixture<SystemManagedCatalogVisibilityFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "sys_managed_visibility.db",
        .port = 31397,
        .health_port = 31398,
        .username = "tester",
        .password = "tester",
        .enable_instrumentation = true,
    };
  }

 protected:
  struct Session {
    std::unique_ptr<FlightSqlClient> client;
    arrow::flight::FlightCallOptions options;
  };

  arrow::Result<Session> Connect(const std::string& bearer_token) {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location, options));
    Session s;
    s.client = std::make_unique<FlightSqlClient>(std::move(client));
    s.options.headers.push_back({"authorization", "Bearer " + bearer_token});
    return s;
  }

  arrow::Result<Session> ConnectAsAdmin() {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location, options));
    ARROW_ASSIGN_OR_RAISE(auto bearer,
                          client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    Session s;
    s.client = std::make_unique<FlightSqlClient>(std::move(client));
    s.options.headers.push_back(bearer);
    return s;
  }

  // Every string value of `column` across all endpoints of a FlightInfo.
  static arrow::Result<std::vector<std::string>> Column(
      Session& s, std::unique_ptr<arrow::flight::FlightInfo>& info, const std::string& column) {
    std::vector<std::string> out;
    for (const auto& endpoint : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, s.client->DoGet(s.options, endpoint.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      int idx = table->schema()->GetFieldIndex(column);
      if (idx < 0) return arrow::Status::Invalid("no column " + column);
      for (const auto& chunk : table->column(idx)->chunks()) {
        auto arr = std::static_pointer_cast<arrow::StringArray>(chunk);
        for (int64_t i = 0; i < arr->length(); i++) {
          if (!arr->IsNull(i)) out.push_back(arr->GetString(i));
        }
      }
    }
    return out;
  }

  static arrow::Result<std::vector<std::string>> Query(Session& s, const std::string& sql,
                                                       const std::string& column) {
    ARROW_ASSIGN_OR_RAISE(auto info, s.client->Execute(s.options, sql));
    return Column(s, info, column);
  }

  static bool Contains(const std::vector<std::string>& v, const std::string& x) {
    return std::find(v.begin(), v.end(), x) != v.end();
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SystemManagedCatalogVisibilityFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<SystemManagedCatalogVisibilityFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SystemManagedCatalogVisibilityFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SystemManagedCatalogVisibilityFixture>::config_{};

TEST_F(SystemManagedCatalogVisibilityFixture, AdminSeesInstrumentationCatalog) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Instrumentation needs GIZMOSQL_LICENSE_KEY_FILE";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto admin, ConnectAsAdmin());

  ASSERT_ARROW_OK_AND_ASSIGN(auto info, admin.client->GetCatalogs(admin.options));
  ASSERT_ARROW_OK_AND_ASSIGN(auto catalogs, Column(admin, info, "catalog_name"));
  EXPECT_TRUE(Contains(catalogs, kInstrCatalog)) << "admin GetCatalogs lost the instrumentation catalog";

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto dbs, Query(admin, "SELECT database_name FROM duckdb_databases()", "database_name"));
  EXPECT_TRUE(Contains(dbs, kInstrCatalog)) << "admin duckdb_databases() lost the instrumentation catalog";

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto tables, admin.client->GetTables(admin.options, &kInstrCatalog, nullptr, nullptr, false, nullptr));
  ASSERT_ARROW_OK_AND_ASSIGN(auto names, Column(admin, tables, "table_name"));
  EXPECT_TRUE(Contains(names, "sessions")) << "admin GetTables should list the instrumentation tables";

  // And gizmosql_settings() tells the admin where instrumentation lives.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto instr_catalog,
      Query(admin, "SELECT value FROM gizmosql_settings() WHERE name = 'gizmosql.instrumentation_catalog'", "value"));
  ASSERT_EQ(instr_catalog.size(), 1u);
  EXPECT_EQ(instr_catalog[0], kInstrCatalog);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto instr_on,
      Query(admin, "SELECT value FROM gizmosql_settings() WHERE name = 'gizmosql.enable_instrumentation'", "value"));
  ASSERT_EQ(instr_on.size(), 1u);
  EXPECT_EQ(instr_on[0], "true");
}

TEST_F(SystemManagedCatalogVisibilityFixture, NonAdminNeverSeesInstrumentationCatalog) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Instrumentation needs GIZMOSQL_LICENSE_KEY_FILE";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto user, Connect(CreateNonAdminToken("readonly_svc")));

  // Flight SQL metadata RPCs (what ADBC drivers and the MCP server use).
  {
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, user.client->GetCatalogs(user.options));
    ASSERT_ARROW_OK_AND_ASSIGN(auto catalogs, Column(user, info, "catalog_name"));
    EXPECT_FALSE(Contains(catalogs, kInstrCatalog)) << "GetCatalogs listed the instrumentation catalog";
    EXPECT_FALSE(catalogs.empty()) << "GetCatalogs should still list the user's catalogs";
  }
  {
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, user.client->GetDbSchemas(user.options, nullptr, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto catalogs, Column(user, info, "catalog_name"));
    EXPECT_FALSE(Contains(catalogs, kInstrCatalog)) << "GetDbSchemas listed the instrumentation catalog";
    ASSERT_ARROW_OK_AND_ASSIGN(auto scoped, user.client->GetDbSchemas(user.options, &kInstrCatalog, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto scoped_rows, Column(user, scoped, "catalog_name"));
    EXPECT_TRUE(scoped_rows.empty()) << "GetDbSchemas(instrumentation catalog) returned rows";
  }
  {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto info, user.client->GetTables(user.options, nullptr, nullptr, nullptr, false, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto catalogs, Column(user, info, "catalog_name"));
    EXPECT_FALSE(Contains(catalogs, kInstrCatalog)) << "GetTables listed the instrumentation catalog";
    ASSERT_ARROW_OK_AND_ASSIGN(auto names, Column(user, info, "table_name"));
    EXPECT_FALSE(Contains(names, "sessions")) << "GetTables listed an instrumentation table";
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto scoped, user.client->GetTables(user.options, &kInstrCatalog, nullptr, nullptr, false, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto scoped_rows, Column(user, scoped, "table_name"));
    EXPECT_TRUE(scoped_rows.empty()) << "GetTables(instrumentation catalog) returned rows";
  }

  // SQL metadata: duckdb_*(), information_schema, SHOW.
  const std::vector<std::pair<std::string, std::string>> queries = {
      {"SELECT database_name FROM duckdb_databases()", "database_name"},
      {"SELECT DISTINCT database_name FROM duckdb_schemas()", "database_name"},
      {"SELECT DISTINCT database_name FROM duckdb_tables()", "database_name"},
      {"SELECT DISTINCT catalog_name FROM information_schema.schemata", "catalog_name"},
      {"SELECT DISTINCT table_catalog FROM information_schema.tables", "table_catalog"},
      {"SHOW DATABASES", "database_name"},
      {"SELECT DISTINCT database FROM (SHOW ALL TABLES)", "database"},
  };
  for (const auto& [sql, column] : queries) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto values, Query(user, sql, column));
    EXPECT_FALSE(Contains(values, kInstrCatalog)) << sql << " listed the instrumentation catalog";
  }

  // gizmosql_settings() does not name the hidden catalog to a non-admin either.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto setting_names,
      Query(user, "SELECT name FROM gizmosql_settings() WHERE name LIKE 'gizmosql.%instrumentation%' OR name LIKE 'gizmosql.log_%'", "name"));
  EXPECT_TRUE(setting_names.empty()) << "admin-only settings rows visible to a non-admin";
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto setting_values,
      Query(user, "SELECT value FROM gizmosql_settings() WHERE value = '" + kInstrCatalog + "'", "value"));
  EXPECT_TRUE(setting_values.empty()) << "instrumentation catalog name leaked through gizmosql_settings()";

  // Reading it stays denied, as before.
  auto denied = user.client->Execute(user.options, "SELECT * FROM " + kInstrCatalog + ".main.sessions LIMIT 1");
  ASSERT_FALSE(denied.ok());
  EXPECT_NE(denied.status().ToString().find("Only administrators"), std::string::npos)
      << denied.status().ToString();
}
