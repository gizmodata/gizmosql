// Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>
#include <cmath>
#include <thread>
#include "enterprise/metrics/metrics_registry.h"
#include "enterprise/enterprise_features.h"
using namespace gizmosql::enterprise;

TEST(MetricsRegistryTest, FloatingPointUpdatesAreAtomicUnderContention) {
  AtomicMetricValue value;
  std::vector<std::thread> workers;
  for (int worker = 0; worker < 8; ++worker) {
    workers.emplace_back([&] {
      for (int i = 0; i < 10000; ++i) {
        value.fetch_add(.5);
        value.fetch_sub(.25);
      }
    });
  }
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(value.load(), 20000);
  value.store(NAN);
  EXPECT_TRUE(std::isnan(value.fetch_add(1)));
  EXPECT_TRUE(std::isnan(value.load()));
}

TEST(MetricsRegistryTest, HistogramsAreCumulativeAndCountEveryOutcome) {
  MetricsRegistry registry;
  registry.StatementFinished(StatementKind::Read, "", .005);
  registry.StatementFinished(StatementKind::Read, "", .006);
  registry.StatementFinished(StatementKind::Write, "Out of Memory Error", 301);
  registry.StatementFinished(StatementKind::Other, "INTERRUPT Error", 1);
  auto snapshot = registry.Snapshot();
  double last = 0;
  for (const auto& row : snapshot) {
    if (row.name == "gizmosql_statement_duration_seconds_bucket" &&
        row.labels.at("kind") == "read") {
      EXPECT_GE(row.value, last);
      last = row.value;
      if (row.labels.at("le") == "0.005") EXPECT_EQ(row.value, 1);
      if (row.labels.at("le") == "+Inf") EXPECT_EQ(row.value, 2);
    }
    if (row.name == "gizmosql_statement_duration_seconds_count" &&
        row.labels.at("kind") == "read")
      EXPECT_EQ(row.value, last);
  }
  EXPECT_EQ(registry.At("gizmosql_statements_total", {{"status", "ok"}}).value.load(), 2);
  EXPECT_EQ(registry.At("gizmosql_statements_total", {{"status", "error"}}).value.load(),
            1);
  EXPECT_EQ(
      registry.At("gizmosql_statements_total", {{"status", "cancelled"}}).value.load(),
      1);
  EXPECT_EQ(registry.At("gizmosql_statement_errors_total", {{"class", "out_of_memory"}})
                .value.load(),
            1);
}
TEST(MetricsRegistryTest, SerializationEscapesLabelsAndHasOneTypePerFamily) {
  MetricsRegistry registry;
  registry.Add("gizmosql_test_info", "gauge", "a\\b\nc",
               {{"version", "quote\"slash\\newline\n"}}, 1);
  auto text = MetricsRegistry::Serialize(registry.Snapshot());
  EXPECT_NE(text.find("version=\"quote\\\"slash\\\\newline\\n\""), text.npos);
  EXPECT_NE(text.find("# HELP gizmosql_test_info a\\\\b\\nc\n"), text.npos);
  const std::string type = "# TYPE gizmosql_statement_duration_seconds histogram\n";
  auto first = text.find(type);
  ASSERT_NE(first, text.npos);
  EXPECT_EQ(text.find(type, first + 1), text.npos);
  EXPECT_EQ(text.back(), '\n');
}
TEST(MetricsRegistryTest, ParsedStatementClassification) {
  using T = duckdb::StatementType;
  EXPECT_EQ(ClassifyStatement(T::SELECT_STATEMENT), StatementKind::Read);
  EXPECT_EQ(ClassifyStatement(T::MERGE_INTO_STATEMENT), StatementKind::Write);
  EXPECT_EQ(ClassifyStatement(T::ATTACH_STATEMENT), StatementKind::Ddl);
  EXPECT_EQ(ClassifyStatement(T::CALL_STATEMENT), StatementKind::Other);
}
TEST(MetricsRegistryTest, ErrorClassification) {
  EXPECT_EQ(ClassifyStatementError("Out of Memory Error: allocation failed"),
            StatementError::OutOfMemory);
  EXPECT_EQ(ClassifyStatementError("IO Error: disk full"), StatementError::Io);
  EXPECT_EQ(ClassifyStatementError("Catalog Error: missing table"),
            StatementError::Catalog);
  EXPECT_EQ(ClassifyStatementError("Parser Error: syntax error"), StatementError::Syntax);
  EXPECT_EQ(ClassifyStatementError("Permission denied"), StatementError::Permission);
  EXPECT_EQ(ClassifyStatementError("INTERRUPT Error: Interrupted!"),
            StatementError::Cancelled);
  EXPECT_EQ(ClassifyStatementError("unknown"), StatementError::Other);
}
TEST(MetricsLicenseTest, FeatureIsIndependentOfOtherEnterpriseFeatures) {
  LicenseInfo license;
  license.features = {kFeatureInstrumentation, kFeatureStatementQueue};
  EXPECT_FALSE(license.HasFeature(kFeatureMetrics));
  license.features.insert(kFeatureMetrics);
  EXPECT_TRUE(license.HasFeature(kFeatureMetrics));
}

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <chrono>
#include <filesystem>
#include "duckdb_server.h"
#include "enterprise/metrics/metrics_service.h"
#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/extension/uuid.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

class MetricsEndpointFixture
    : public gizmosql::testing::ServerTestFixture<MetricsEndpointFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "metrics_endpoint_tester.db",
        .port = 31600,
        .health_port = 31601,
        .username = "tester",
        .password = "tester",
        .enable_instrumentation = false,
        .enable_metrics = true,
        .metrics_port = 31602,
    };
  }

  static void SetUpTestSuite() {
    const auto* license = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
    const auto* inline_license = std::getenv("GIZMOSQL_LICENSE_KEY");
    auto status = EnterpriseFeatures::Instance().Initialize(
        license ? license : "", inline_license ? inline_license : "");
    if (!status.ok() || !EnterpriseFeatures::Instance().IsMetricsAvailable()) {
      GTEST_SKIP() << "A local license with the metrics entitlement is required";
    }
    ServerTestFixture::SetUpTestSuite();
  }

 protected:
  std::unique_ptr<FlightSqlClient> sql_client_;
  arrow::flight::FlightCallOptions call_options_;

  void SetUp() override {
    ASSERT_TRUE(IsServerReady()) << "Server not ready";
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(
                                   location, arrow::flight::FlightClientOptions{}));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options_.headers.push_back(bearer);
    sql_client_ = std::make_unique<FlightSqlClient>(std::move(client));
  }

  void Exec(const std::string& sql) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto n, sql_client_->ExecuteUpdate(call_options_, sql));
    (void)n;
  }

  arrow::Result<std::shared_ptr<arrow::Table>> Query(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, sql_client_->Execute(call_options_, sql));
    ARROW_ASSIGN_OR_RAISE(auto stream,
                          sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
    return stream->ToTable();
  }

  // Returns the first column of the first row of a query, as a string.
  std::string Scalar(const std::string& sql) {
    auto table = Query(sql);
    EXPECT_TRUE(table.ok()) << table.status().ToString();
    if (!table.ok()) return "<error>";
    EXPECT_EQ((*table)->num_rows(), 1);
    auto scalar = (*table)->column(0)->GetScalar(0);
    EXPECT_TRUE(scalar.ok());
    return (*scalar)->ToString();
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MetricsEndpointFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<MetricsEndpointFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MetricsEndpointFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MetricsEndpointFixture>::config_{};

// The unclean-exit marker sits beside the database file, so only the process
// holding DuckDB's single write lock may own it; a read-only server (several may
// share one file) must neither write it nor report another instance's runs.
TEST(MetricsExitMarkerTest, ReadOnlyServersNeverTouchTheMarker) {
  const auto* license = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  const auto* inline_license = std::getenv("GIZMOSQL_LICENSE_KEY");
  auto status = EnterpriseFeatures::Instance().Initialize(
      license ? license : "", inline_license ? inline_license : "");
  if (!status.ok() || !EnterpriseFeatures::Instance().IsMetricsAvailable()) {
    GTEST_SKIP() << "A local license with the metrics entitlement is required";
  }
  const auto database =
      std::filesystem::path(::testing::TempDir()) /
      ("metrics_marker_" +
       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
       ".db");
  const auto marker = database.string() + ".gizmosql-metrics-state";
  std::filesystem::remove(marker);
  auto db = std::make_shared<duckdb::DuckDB>(nullptr);
  auto sampler = [](gizmosql::enterprise::MetricsRegistry&) {};
  {
    gizmosql::enterprise::MetricsService read_only(db, database, "", 0, sampler,
                                                   /*read_only=*/true);
    ASSERT_ARROW_OK(read_only.Start(0, "127.0.0.1"));
    EXPECT_FALSE(std::filesystem::exists(marker));
    EXPECT_TRUE(
        std::isnan(read_only.Registry()->At("gizmosql_last_exit_unclean").value.load()));
    read_only.Stop();
    EXPECT_FALSE(std::filesystem::exists(marker));
  }
  {
    gizmosql::enterprise::MetricsService writer(db, database, "", 0, sampler,
                                                /*read_only=*/false);
    ASSERT_ARROW_OK(writer.Start(0, "127.0.0.1"));
    EXPECT_TRUE(std::filesystem::exists(marker));
    EXPECT_EQ(writer.Registry()->At("gizmosql_last_exit_unclean").value.load(), 0);
    writer.Stop();
  }
  std::filesystem::remove(marker);
}

TEST_F(MetricsEndpointFixture, HttpAndSqlExposeTheSameRegistry) {
  httplib::Client http("127.0.0.1", 31602);
  auto response = http.Get("/metrics");
  ASSERT_TRUE(response);
  ASSERT_EQ(response->status, 200);
  EXPECT_NE(response->get_header_value("Content-Type").find("text/plain"),
            std::string::npos);
  EXPECT_NE(response->body.find("gizmosql_build_info{"), std::string::npos);
  EXPECT_EQ(
      Scalar(
          "SELECT value FROM gizmosql_settings() WHERE name = 'gizmosql.enable_metrics'"),
      "true");
  auto registry = MetricsRegistry::Current();
  ASSERT_TRUE(registry);
  const auto before =
      registry->At("gizmosql_statements_total", {{"status", "ok"}}).value.load();
  Exec("CREATE OR REPLACE TABLE metrics_ledger(n INTEGER)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client_->Execute(call_options_, "INSERT INTO metrics_ledger VALUES (1)"));
  const auto after =
      registry->At("gizmosql_statements_total", {{"status", "ok"}}).value.load();
  EXPECT_EQ(after - before, 2);
  for (int i = 0; i < 3; ++i) {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
  }
  EXPECT_EQ(registry->At("gizmosql_statements_total", {{"status", "ok"}}).value.load(),
            after);
  EXPECT_EQ(Scalar("SELECT value::BIGINT FROM gizmosql_metrics() WHERE name = "
                   "'gizmosql_session_limit'"),
            "0");
  EXPECT_EQ(Scalar("SELECT labels['edition'] FROM gizmosql_metrics() WHERE name = "
                   "'gizmosql_build_info'"),
            "Enterprise");
  response = http.Get("/metrics");
  ASSERT_TRUE(response);
  EXPECT_NE(response->body.find("gizmosql_session_limit 0\n"), std::string::npos);
  auto missing = http.Get("/not-metrics");
  ASSERT_TRUE(missing);
  EXPECT_EQ(missing->status, 404);
}

TEST_F(MetricsEndpointFixture, CollectorRefreshesWithoutScrapes) {
  auto registry = MetricsRegistry::Current();
  ASSERT_TRUE(registry);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(8);
  while (std::chrono::steady_clock::now() < deadline &&
         std::isnan(registry->At("gizmosql_metrics_collection_success").value.load())) {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  EXPECT_EQ(registry->At("gizmosql_metrics_collection_success").value.load(), 1);
  EXPECT_GT(registry->At("gizmosql_metrics_last_collection_success_seconds").value.load(),
            0);
  EXPECT_GT(registry->At("process_resident_memory_bytes").value.load(), 0);
  EXPECT_GT(registry->At("gizmosql_duckdb_memory_limit_bytes").value.load(), 0);
}

TEST_F(MetricsEndpointFixture, ParserAndCatalogFailuresAreCounted) {
  auto registry = MetricsRegistry::Current();
  ASSERT_TRUE(registry);
  auto& errors = registry->At("gizmosql_statements_total", {{"status", "error"}}).value;
  const auto before = errors.load();
  EXPECT_FALSE(sql_client_->Execute(call_options_, "SELECT FROM").ok());
  EXPECT_FALSE(
      sql_client_->Execute(call_options_, "SELECT * FROM metrics_missing_table").ok());
  EXPECT_EQ(errors.load() - before, 2);
  EXPECT_GE(
      registry->At("gizmosql_statement_errors_total", {{"class", "syntax"}}).value.load(),
      1);
  EXPECT_GE(registry->At("gizmosql_statement_errors_total", {{"class", "catalog"}})
                .value.load(),
            1);
}

TEST_F(MetricsEndpointFixture, TransactionStatesFollowTheRetainedSessionConnection) {
  auto registry = MetricsRegistry::Current();
  auto server = std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_);
  ASSERT_TRUE(registry);
  ASSERT_TRUE(server);
  Exec("CREATE OR REPLACE TABLE metrics_transactions(n INTEGER PRIMARY KEY)");
  Exec("INSERT INTO metrics_transactions VALUES (1)");
  Exec("BEGIN TRANSACTION");
  EXPECT_EQ(Scalar("SELECT 42"), "42");
  server->SampleMetrics(*registry);
  EXPECT_EQ(
      registry->At("gizmosql_sessions", {{"state", "idle_in_transaction"}}).value.load(),
      1);
  EXPECT_FALSE(
      sql_client_
          ->ExecuteUpdate(call_options_, "INSERT INTO metrics_transactions VALUES (1)")
          .ok());
  server->SampleMetrics(*registry);
  EXPECT_EQ(registry->At("gizmosql_sessions", {{"state", "idle_in_transaction_aborted"}})
                .value.load(),
            1);
  Exec("ROLLBACK");
  server->SampleMetrics(*registry);
  EXPECT_EQ(
      registry->At("gizmosql_sessions", {{"state", "idle_in_transaction"}}).value.load(),
      0);
  EXPECT_EQ(registry->At("gizmosql_sessions", {{"state", "idle_in_transaction_aborted"}})
                .value.load(),
            0);
}
