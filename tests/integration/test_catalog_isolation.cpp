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

// Catalog isolation between tenants on one server (Enterprise catalog_access).
//
// Reproduces the setup of the "GizmoSQL catalog isolation" findings
// (agent.cloud / Summation, Oct 2026): one DuckLake catalog per app on a
// shared server, and a non-admin token per app scoped to its own catalog:
//   catalog_access: [{catalog: <own>, access: write}, {catalog: "*", access: none}]
// Here the catalogs keep their metadata in local DuckDB files and their data
// in local directories, so no Postgres server or bucket is needed; the
// external-database and remote-URL statements of findings 1, 2 and 4 are
// refused before they reach either.
//
// A pass means every cross-catalog statement is refused while app A keeps full
// read-write on its own catalog.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <random>
#include <sstream>
#include <string>
#include <thread>

#include <jwt-cpp/jwt.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"
#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/enterprise_features.h"
#endif

using arrow::flight::sql::FlightSqlClient;
namespace fs = std::filesystem;

#ifndef _WIN32

namespace {

const std::string kIsolationSecretKey = "test_secret_key_for_testing";
const std::string kLakeA = "iso_lake_a";
const std::string kLakeB = "iso_lake_b";
const fs::path kWorkDir = "catalog_isolation_test";

bool CatalogPermissionsLicensed() {
#ifdef GIZMOSQL_ENTERPRISE
  return gizmosql::enterprise::EnterpriseFeatures::Instance()
      .IsCatalogPermissionsAvailable();
#else
  return false;
#endif
}

std::string RandomId() {
  static std::mt19937_64 gen{std::random_device{}()};
  std::stringstream ss;
  ss << std::hex << gen() << gen();
  return ss.str();
}

// A server-issued (HS256) token. Each token carries its own session id, so
// statements sent with the same token share a session.
std::string IsolationToken(const std::string& username, const std::string& role,
                           const std::string& catalog_access_json) {
  auto builder = jwt::create()
                     .set_issuer("gizmosql")
                     .set_type("JWT")
                     .set_id("iso-" + RandomId())
                     .set_issued_at(std::chrono::system_clock::now())
                     .set_expires_at(std::chrono::system_clock::now() + std::chrono::hours{1})
                     .set_payload_claim("sub", jwt::claim(username))
                     .set_payload_claim("role", jwt::claim(role))
                     .set_payload_claim("auth_method", jwt::claim(std::string("TestToken")))
                     .set_payload_claim("session_id", jwt::claim(RandomId()));
  picojson::value rules;
  const std::string err = picojson::parse(rules, catalog_access_json);
  EXPECT_TRUE(err.empty()) << err;
  builder = builder.set_payload_claim("catalog_access", jwt::claim(rules));
  return builder.sign(jwt::algorithm::hs256{kIsolationSecretKey});
}

}  // namespace

class CatalogIsolationFixture
    : public gizmosql::testing::ServerTestFixture<CatalogIsolationFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "catalog_isolation_test.db",
        .port = 31690,
        .health_port = 31691,
        .username = "iso_admin",
        .password = "iso_admin_pass",
    };
  }

 protected:
  void SetUp() override {
    if (!CatalogPermissionsLicensed()) {
      GTEST_SKIP() << "Catalog isolation needs the Enterprise catalog_permissions feature. "
                   << "Set GIZMOSQL_LICENSE_KEY_FILE.";
    }
    ASSERT_TRUE(IsServerReady());
    ASSERT_ARROW_OK_AND_ASSIGN(auto bearer, AdminBearer());
    admin_ = bearer;

    // DuckLake is a runtime extension; without network access it cannot be
    // installed, which is an environment problem rather than a failure here.
    if (!Exec(admin_, "INSTALL ducklake").ok() || !Exec(admin_, "LOAD ducklake").ok()) {
      GTEST_SKIP() << "The ducklake extension is not available in this environment";
    }

    std::error_code ec;
    fs::create_directories(kWorkDir / "a", ec);
    fs::create_directories(kWorkDir / "b", ec);
    for (const auto& [name, dir] :
         {std::pair{kLakeA, std::string("a")}, std::pair{kLakeB, std::string("b")}}) {
      const auto meta = (kWorkDir / (dir + ".ducklake")).string();
      const auto data = (kWorkDir / dir).string() + "/";
      // Idempotent across tests in this suite (the server outlives each test).
      (void)Exec(admin_, "DETACH DATABASE IF EXISTS " + name);
      ASSERT_ARROW_OK(Exec(admin_, "ATTACH 'ducklake:" + meta + "' AS " + name +
                                       " (DATA_PATH '" + data + "')"));
    }
    ASSERT_ARROW_OK(Exec(admin_, "CREATE OR REPLACE TABLE " + kLakeB +
                                     ".main.secret_b AS SELECT 42 AS secret"));

    // App A's token, scoped exactly as in the report (plus its temp catalog,
    // for the macro test).
    app_a_ = Bearer(IsolationToken(
        "app_a", "user",
        R"([{"catalog": ")" + kLakeA + R"(", "access": "write"},)"
        R"( {"catalog": "temp", "access": "write"},)"
        R"( {"catalog": "*", "access": "none"}])"));
    app_a_readonly_ = Bearer(IsolationToken(
        "app_a_reader", "user",
        R"([{"catalog": ")" + kLakeA + R"(", "access": "read"},)"
        R"( {"catalog": "*", "access": "none"}])"));
  }

  static void TearDownTestSuite() {
    std::error_code ec;
    fs::remove_all(kWorkDir, ec);
    gizmosql::testing::ServerTestFixture<CatalogIsolationFixture>::TearDownTestSuite();
  }

  using Header = std::pair<std::string, std::string>;

  static Header Bearer(const std::string& token) {
    return {"authorization", "Bearer " + token};
  }

  arrow::Result<Header> AdminBearer() {
    ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
    return client->AuthenticateBasicToken({}, GetUsername(), GetPassword());
  }

  arrow::Status Exec(const Header& auth, const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(auth);
    FlightSqlClient sql_client(std::move(client));
    ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(co, sql));
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(co, ep.ticket));
      ARROW_RETURN_NOT_OK(reader->ToTable().status());
    }
    return arrow::Status::OK();
  }

  void ExpectRefused(const Header& auth, const std::string& sql, const std::string& why) {
    auto st = Exec(auth, sql);
    EXPECT_FALSE(st.ok()) << "should be refused: " << sql;
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << "\n  expected the error to mention: " << why << "\n  got: " << st.ToString();
  }

  Header admin_, app_a_, app_a_readonly_;
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CatalogIsolationFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<CatalogIsolationFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CatalogIsolationFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CatalogIsolationFixture>::config_{};

// What held in the report must still hold.
TEST_F(CatalogIsolationFixture, OwnCatalogIsFullyUsableAndOtherCatalogIsNot) {
  const auto t = kLakeA + ".main.own_t";
  ASSERT_ARROW_OK(Exec(app_a_, "CREATE OR REPLACE TABLE " + t + " (i INTEGER)"));
  ASSERT_ARROW_OK(Exec(app_a_, "INSERT INTO " + t + " VALUES (1), (2)"));
  ASSERT_ARROW_OK(Exec(app_a_, "UPDATE " + t + " SET i = 3 WHERE i = 2"));
  ASSERT_ARROW_OK(Exec(app_a_, "DELETE FROM " + t + " WHERE i = 1"));
  ASSERT_ARROW_OK(Exec(app_a_, "SELECT * FROM " + t));

  EXPECT_FALSE(Exec(app_a_, "SELECT * FROM " + kLakeB + ".main.secret_b").ok());
  EXPECT_FALSE(Exec(app_a_, "INSERT INTO " + kLakeB + ".main.secret_b VALUES (1)").ok());
}

// Finding 1: postgres_query / postgres_execute into another catalog's metadata.
TEST_F(CatalogIsolationFixture, Finding1ExternalDatabaseFunctionsAreAdminOnly) {
  const std::string meta = "'__ducklake_metadata_" + kLakeB + "'";
  ExpectRefused(app_a_,
                "SELECT * FROM postgres_query(" + meta +
                    ", 'select table_id, table_name, path from ducklake_table')",
                "postgres_query() (external database access)");
  ExpectRefused(app_a_, "CALL postgres_execute(" + meta + ", 'select 1')",
                "postgres_execute() (external database access)");
  // The metadata catalog itself stays out of reach by name, as before.
  EXPECT_FALSE(
      Exec(app_a_, "SELECT * FROM __ducklake_metadata_" + kLakeB + ".ducklake_table").ok());
}

// Finding 2: COPY ... TO another catalog's object-storage prefix.
TEST_F(CatalogIsolationFixture, Finding2RemoteCopyIsAdminOnly) {
  ExpectRefused(app_a_,
                "COPY (SELECT 1 AS i) TO 'gs://staging-bucket/org/catalog-b/main/secret_b/"
                "injected-by-a.parquet' (FORMAT parquet)",
                "COPY TO (remote URL)");
  // Nor into B's local data directory, nor via DuckLake's file registration.
  ExpectRefused(app_a_,
                "COPY (SELECT 1 AS i) TO '" + (kWorkDir / "b" / "injected.parquet").string() +
                    "' (FORMAT parquet)",
                "COPY TO (local filesystem)");
  ExpectRefused(app_a_,
                "CALL ducklake_add_data_files('" + kLakeA + "', 'own_t', '" +
                    (kWorkDir / "b").string() + "/main/secret_b/x.parquet')",
                "ducklake_add_data_files()");
}

// Finding 3: DuckLake table functions read another catalog's metadata.
TEST_F(CatalogIsolationFixture, Finding3DuckLakeFunctionsHonorCatalogAccess) {
  ExpectRefused(app_a_, "SELECT * FROM ducklake_list_files('" + kLakeB + "', 'secret_b')",
                "no read access to that catalog");
  ExpectRefused(app_a_, "SELECT * FROM ducklake_snapshots('" + kLakeB + "')",
                "no read access to that catalog");
  ExpectRefused(app_a_, "SELECT * FROM ducklake_table_info('" + kLakeB + "')",
                "no read access to that catalog");
  ExpectRefused(app_a_, "CALL ducklake_expire_snapshots('" + kLakeB + "', dry_run => true)",
                "no write access to that catalog");

  // Its own catalog: allowed, and maintenance needs write.
  ARROW_EXPECT_OK(Exec(app_a_, "SELECT * FROM ducklake_snapshots('" + kLakeA + "')"));
  ARROW_EXPECT_OK(Exec(app_a_, "SELECT * FROM ducklake_table_info('" + kLakeA + "')"));
  ARROW_EXPECT_OK(
      Exec(app_a_, "CALL ducklake_expire_snapshots('" + kLakeA + "', dry_run => true)"));
  ARROW_EXPECT_OK(Exec(app_a_readonly_, "SELECT * FROM ducklake_snapshots('" + kLakeA + "')"));
  ExpectRefused(app_a_readonly_,
                "CALL ducklake_expire_snapshots('" + kLakeA + "', dry_run => true)",
                "no write access to that catalog");
}

// Finding 3, indirectly: the catalog name supplied by a macro argument is only
// known once the statement is bound.
TEST_F(CatalogIsolationFixture, Finding3CannotBeReachedThroughAMacro) {
  ASSERT_ARROW_OK(Exec(app_a_,
                       "CREATE OR REPLACE TEMP MACRO snaps(c) AS TABLE "
                       "SELECT * FROM ducklake_snapshots(c)"));
  ARROW_EXPECT_OK(Exec(app_a_, "SELECT * FROM snaps('" + kLakeA + "')"));
  ExpectRefused(app_a_, "SELECT * FROM snaps('" + kLakeB + "')",
                "no read access to that catalog");
}

// Finding 4: outbound HTTP.
TEST_F(CatalogIsolationFixture, Finding4OutboundHttpIsAdminOnly) {
  ExpectRefused(app_a_, "SELECT * FROM read_csv('https://example.com/')",
                "read_csv() (remote URL)");
  ExpectRefused(app_a_, "SELECT * FROM 'https://example.com/data.parquet'",
                "remote URL");
}

// Admins are unaffected by the non-admin gate.
TEST_F(CatalogIsolationFixture, AdminMayInspectEveryCatalog) {
  ARROW_EXPECT_OK(Exec(admin_, "SELECT * FROM ducklake_snapshots('" + kLakeB + "')"));
  ARROW_EXPECT_OK(Exec(admin_, "SELECT * FROM ducklake_list_files('" + kLakeB + "', 'secret_b')"));
}

#endif  // _WIN32
