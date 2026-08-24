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

// DuckLake catalog fan-out regression tests
//
// With N DuckLake catalogs attached (metadata in PostgreSQL), a `USE <catalog>`
// that names exactly one of them must NOT open a PostgreSQL connection to every
// attached catalog. GizmoSQL used to probe catalog existence with
// `information_schema.schemata`, which enumerates the schemas of every attached
// catalog and therefore opened one metadata-store connection per catalog. The
// probe now uses duckdb_databases(), which is answered in-process.
//
// The fan-out is measured with PostgreSQL's cumulative pg_stat_database.sessions
// counter (PG 14+), sampled before and after the statement — deterministic, no
// timing-based sampling. Connection pools are drained first so the count is
// attributable to the statement under test.
//
// Two variants: DuckLake data on a local path and on S3 (MinIO). The metadata
// fan-out is independent of where the data lives; both are covered so the test
// mirrors real deployments.
//
// Prerequisites:
//   docker compose -f docker-compose.test.yml up -d
//
// Skipped when PostgreSQL (or MinIO, for the S3 variant) is unreachable.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// PostgreSQL / MinIO settings (matching docker-compose.test.yml and ci.yml)
// PostgreSQL port can be overridden with GIZMOSQL_TEST_PG_PORT for local runs
// where 5432 is already taken.
int PostgresPort() {
  if (const char* env = std::getenv("GIZMOSQL_TEST_PG_PORT"); env && *env) {
    return std::atoi(env);
  }
  return 5432;
}
constexpr int kMinioPort = 9000;
std::string PostgresConninfo() {
  return "host=localhost port=" + std::to_string(PostgresPort()) +
         " user=postgres password=testpassword dbname=ducklake_catalog";
}
const char* kMinioBucket = "instrumentation";

// Number of DuckLake catalogs to attach. The unfixed probe opened one PG
// connection per attached catalog, so N must be comfortably above the
// tolerance asserted below.
constexpr int kCatalogCount = 10;
// Sessions a single-catalog `USE` may legitimately open: the named catalog's
// own metadata connection plus the monitoring catalog's pooled connection
// being reaped/reopened between the two counter reads.
constexpr int64_t kMaxSessionsPerUse = 3;

bool IsTcpPortOpen(int port) {
#ifdef _WIN32
  WSADATA wsa;
  if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) return false;
  SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock == INVALID_SOCKET) {
    WSACleanup();
    return false;
  }
#else
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) return false;
#endif
  struct sockaddr_in addr;
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  struct timeval timeout;
  timeout.tv_sec = 2;
  timeout.tv_usec = 0;
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout),
             sizeof(timeout));
  int result = connect(sock, (struct sockaddr*)&addr, sizeof(addr));
#ifdef _WIN32
  closesocket(sock);
  WSACleanup();
#else
  close(sock);
#endif
  return result == 0;
}

struct QueryResult {
  bool success = false;
  std::string error_message;
  std::shared_ptr<arrow::Table> table;
};

QueryResult RunQuery(FlightSqlClient& client, arrow::flight::FlightCallOptions& opts,
                     const std::string& query) {
  QueryResult r;
  auto info = client.Execute(opts, query);
  if (!info.ok()) {
    r.error_message = info.status().ToString();
    return r;
  }
  if ((*info)->endpoints().empty()) {
    r.success = true;
    return r;
  }
  auto reader = client.DoGet(opts, (*info)->endpoints()[0].ticket);
  if (!reader.ok()) {
    r.error_message = reader.status().ToString();
    return r;
  }
  auto table = (*reader)->ToTable();
  if (!table.ok()) {
    r.error_message = table.status().ToString();
    return r;
  }
  r.table = *table;
  r.success = true;
  return r;
}

int64_t ScalarInt64(const QueryResult& r) {
  EXPECT_TRUE(r.success) << r.error_message;
  if (!r.success || !r.table || r.table->num_rows() == 0) return -1;
  auto scalar = r.table->column(0)->GetScalar(0);
  EXPECT_TRUE(scalar.ok());
  if (!scalar.ok()) return -1;
  auto casted = (*scalar)->CastTo(arrow::int64());
  EXPECT_TRUE(casted.ok());
  if (!casted.ok()) return -1;
  return std::static_pointer_cast<arrow::Int64Scalar>(*casted)->value;
}

// Cumulative count of sessions ever established against the metadata DB.
int64_t PgSessionsEstablished(FlightSqlClient& client,
                              arrow::flight::FlightCallOptions& opts) {
  return ScalarInt64(RunQuery(
      client, opts,
      "SELECT sessions FROM postgres_query('pgmon', "
      "'SELECT sessions FROM pg_stat_database WHERE datname = current_database()')"));
}

// Live backends against the metadata DB, excluding the monitor's own.
int64_t PgLiveBackends(FlightSqlClient& client, arrow::flight::FlightCallOptions& opts) {
  return ScalarInt64(RunQuery(
      client, opts,
      "SELECT n FROM postgres_query('pgmon', "
      "'SELECT count(*) - 1 AS n FROM pg_stat_activity "
      "WHERE datname = current_database() AND pid <> pg_backend_pid()')"));
}

// Wait for every DuckLake catalog's connection pool to drain so that a later
// sessions-counter delta is attributable to the statement under test.
void DrainPools(FlightSqlClient& client, arrow::flight::FlightCallOptions& opts) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
  while (std::chrono::steady_clock::now() < deadline) {
    if (PgLiveBackends(client, opts) <= 0) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
  }
  FAIL() << "PostgreSQL connection pools did not drain in time";
}

// Drop a DuckLake metadata schema natively in PostgreSQL (postgres_execute
// bypasses postgres_scanner's cached view of the remote catalog).
std::string DropMetadataSchema(const std::string& schema) {
  return "CALL postgres_execute('pgmon', 'DROP SCHEMA IF EXISTS " + schema + " CASCADE')";
}

std::string CatalogName(int i) { return "fanout_" + std::to_string(i); }

// Shared test body. `data_path_root` is where DuckLake writes Parquet: a local
// directory or an s3:// prefix on MinIO.
void RunFanoutTest(FlightSqlClient& client, arrow::flight::FlightCallOptions& opts,
                   const std::string& data_path_root) {
  QueryResult r;

  // Remember the session's default catalog so cleanup can restore it: DuckDB
  // refuses to DETACH the current catalog.
  r = RunQuery(client, opts, "SELECT current_catalog()");
  ASSERT_TRUE(r.success) << r.error_message;
  const std::string home_catalog =
      r.table->column(0)->GetScalar(0).ValueOrDie()->ToString();

  // Short-lived pools so DrainPools() converges quickly. Session-scoped.
  for (const char* sql : {"SET pg_pool_idle_timeout_millis = 500",
                          "SET pg_pool_max_lifetime_millis = 500",
                          "SET pg_pool_enable_reaper_thread = true"}) {
    r = RunQuery(client, opts, sql);
    ASSERT_TRUE(r.success) << sql << ": " << r.error_message;
  }

  // Monitoring catalog: a plain postgres_scanner attachment to the same DB so
  // the test can read pg_stat_* through the server.
  r = RunQuery(client, opts,
               "ATTACH '" + PostgresConninfo() + "' AS pgmon (TYPE postgres)");
  ASSERT_TRUE(r.success) << r.error_message;

  // Reset metadata schemas from a previous run, then attach one DuckLake
  // catalog per PostgreSQL schema (METADATA_SCHEMA) — each attachment owns its
  // own connection pool, which is what makes the fan-out observable.
  for (int i = 0; i < kCatalogCount; ++i) {
    const std::string name = CatalogName(i);
    r = RunQuery(client, opts, DropMetadataSchema(name));
    ASSERT_TRUE(r.success) << "reset " << name << ": " << r.error_message;
    r = RunQuery(client, opts,
                 "ATTACH 'ducklake:postgres:" + PostgresConninfo() + "' AS " +
                     name + " (DATA_PATH '" + data_path_root + name +
                     "/', METADATA_SCHEMA '" + name + "')");
    ASSERT_TRUE(r.success) << "ATTACH " << name << ": " << r.error_message;
    r = RunQuery(client, opts,
                 "CREATE TABLE " + name + ".main.t0 AS SELECT " + std::to_string(i) +
                     " AS x");
    ASSERT_TRUE(r.success) << "CREATE TABLE in " << name << ": " << r.error_message;
  }

  const std::string target = CatalogName(kCatalogCount / 2);

  // --- USE <catalog>: must not fan out to every attached catalog ------------
  ASSERT_NO_FATAL_FAILURE(DrainPools(client, opts));
  const int64_t before_use = PgSessionsEstablished(client, opts);
  ASSERT_GE(before_use, 0);
  r = RunQuery(client, opts, "USE " + target);
  ASSERT_TRUE(r.success) << r.error_message;
  const int64_t after_use = PgSessionsEstablished(client, opts);
  const int64_t use_delta = after_use - before_use;
  std::cerr << "USE " << target << " opened " << use_delta
            << " PostgreSQL session(s) with " << kCatalogCount << " catalogs attached"
            << std::endl;
  EXPECT_LE(use_delta, kMaxSessionsPerUse)
      << "USE fanned out across attached DuckLake catalogs";
  EXPECT_LT(use_delta, kCatalogCount);

  // USE actually took effect and the catalog is real (data readable).
  r = RunQuery(client, opts, "SELECT current_catalog()");
  ASSERT_TRUE(r.success) << r.error_message;
  ASSERT_EQ(r.table->column(0)->GetScalar(0).ValueOrDie()->ToString(), target);
  ASSERT_EQ(ScalarInt64(RunQuery(client, opts, "SELECT x FROM t0")), kCatalogCount / 2);

  // --- Flight SQL GetCatalogs: lists names only, must not fan out ------------
  ASSERT_NO_FATAL_FAILURE(DrainPools(client, opts));
  const int64_t before_cat = PgSessionsEstablished(client, opts);
  {
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, client.GetCatalogs(opts));
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               client.DoGet(opts, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
    EXPECT_GE(table->num_rows(), kCatalogCount);
  }
  const int64_t cat_delta = PgSessionsEstablished(client, opts) - before_cat;
  std::cerr << "GetCatalogs opened " << cat_delta << " PostgreSQL session(s)" << std::endl;
  EXPECT_LE(cat_delta, kMaxSessionsPerUse)
      << "GetCatalogs fanned out across attached DuckLake catalogs";

  // --- Flight SQL GetDbSchemas(catalog): scans only the named catalog --------
  ASSERT_NO_FATAL_FAILURE(DrainPools(client, opts));
  const int64_t before_sch = PgSessionsEstablished(client, opts);
  {
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, client.GetDbSchemas(opts, &target, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               client.DoGet(opts, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
    ASSERT_GE(table->num_rows(), 1);
    EXPECT_EQ(table->GetColumnByName("catalog_name")->GetScalar(0).ValueOrDie()->ToString(),
              target);
  }
  const int64_t sch_delta = PgSessionsEstablished(client, opts) - before_sch;
  std::cerr << "GetDbSchemas(" << target << ") opened " << sch_delta
            << " PostgreSQL session(s)" << std::endl;
  EXPECT_LE(sch_delta, kMaxSessionsPerUse)
      << "GetDbSchemas fanned out across attached DuckLake catalogs";

  // --- Flight SQL GetTables(catalog): scans only the named catalog -----------
  ASSERT_NO_FATAL_FAILURE(DrainPools(client, opts));
  const int64_t before_tab = PgSessionsEstablished(client, opts);
  {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto info, client.GetTables(opts, &target, nullptr, nullptr, false, nullptr));
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               client.DoGet(opts, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
    ASSERT_EQ(table->num_rows(), 1);
    EXPECT_EQ(table->GetColumnByName("table_name")->GetScalar(0).ValueOrDie()->ToString(),
              "t0");
    EXPECT_EQ(table->GetColumnByName("table_type")->GetScalar(0).ValueOrDie()->ToString(),
              "BASE TABLE");
  }
  const int64_t tab_delta = PgSessionsEstablished(client, opts) - before_tab;
  std::cerr << "GetTables(" << target << ") opened " << tab_delta
            << " PostgreSQL session(s)" << std::endl;
  EXPECT_LE(tab_delta, kMaxSessionsPerUse)
      << "GetTables fanned out across attached DuckLake catalogs";

  // --- Qualified read of one catalog: same bound -----------------------------
  ASSERT_NO_FATAL_FAILURE(DrainPools(client, opts));
  const int64_t before_sel = PgSessionsEstablished(client, opts);
  ASSERT_EQ(ScalarInt64(RunQuery(client, opts,
                                 "SELECT x FROM " + CatalogName(1) + ".main.t0")),
            1);
  const int64_t sel_delta = PgSessionsEstablished(client, opts) - before_sel;
  std::cerr << "qualified SELECT opened " << sel_delta << " PostgreSQL session(s)"
            << std::endl;
  EXPECT_LE(sel_delta, kMaxSessionsPerUse);

  // Cleanup
  r = RunQuery(client, opts, "USE " + home_catalog);
  EXPECT_TRUE(r.success) << r.error_message;
  for (int i = 0; i < kCatalogCount; ++i) {
    r = RunQuery(client, opts, "DETACH " + CatalogName(i));
    EXPECT_TRUE(r.success) << "DETACH " << CatalogName(i) << ": " << r.error_message;
    RunQuery(client, opts, DropMetadataSchema(CatalogName(i)));
  }
  r = RunQuery(client, opts, "DETACH pgmon");
  EXPECT_TRUE(r.success) << r.error_message;
}

}  // namespace

// ============================================================================
// Fixture
// ============================================================================

class DuckLakeFanoutFixture
    : public gizmosql::testing::ServerTestFixture<DuckLakeFanoutFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "ducklake_fanout_test.db",
        .port = 31620,
        .health_port = 31621,
        .username = "fanout_tester",
        .password = "fanout_tester",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<DuckLakeFanoutFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<DuckLakeFanoutFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<DuckLakeFanoutFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<DuckLakeFanoutFixture>::config_{};

// ============================================================================
// Tests
// ============================================================================

TEST_F(DuckLakeFanoutFixture, UseDoesNotFanOutLocalData) {
  if (!IsTcpPortOpen(PostgresPort())) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient client(std::move(flight_client));
  for (const char* sql : {"INSTALL ducklake", "INSTALL postgres", "LOAD ducklake",
                          "LOAD postgres"}) {
    auto lr = RunQuery(client, call_options, sql);
    ASSERT_TRUE(lr.success) << sql << ": " << lr.error_message;
  }
  RunFanoutTest(client, call_options, "data/ducklake_fanout_test/");
}

TEST_F(DuckLakeFanoutFixture, UseDoesNotFanOutS3Data) {
  if (!IsTcpPortOpen(PostgresPort())) {
    GTEST_SKIP() << "PostgreSQL not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }
  if (!IsTcpPortOpen(kMinioPort)) {
    GTEST_SKIP() << "MinIO not available. Start it with: "
                 << "docker compose -f docker-compose.test.yml up -d";
  }
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient client(std::move(flight_client));
  for (const char* sql : {"INSTALL ducklake", "INSTALL postgres", "LOAD ducklake",
                          "LOAD postgres"}) {
    auto lr = RunQuery(client, call_options, sql);
    ASSERT_TRUE(lr.success) << sql << ": " << lr.error_message;
  }
  for (const char* sql : {"INSTALL httpfs", "LOAD httpfs"}) {
    auto lr = RunQuery(client, call_options, sql);
    ASSERT_TRUE(lr.success) << sql << ": " << lr.error_message;
  }
  auto r = RunQuery(client, call_options, R"SQL(
    CREATE OR REPLACE SECRET s3_fanout (
      TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
      ENDPOINT 'localhost:9000', USE_SSL false, URL_STYLE 'path', REGION 'us-east-1'
    ))SQL");
  ASSERT_TRUE(r.success) << r.error_message;
  RunFanoutTest(client, call_options,
                "s3://" + std::string(kMinioBucket) + "/ducklake_fanout_test/");
}
