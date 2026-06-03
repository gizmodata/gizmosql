// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
//
// Integration tests for catalog logging (forking server logs to an attached
// catalog's `logs` table) and the cluster_id / GIZMOSQL_CURRENT_CLUSTER()
// feature. Exercises the file-based default backend (_gizmosql_logs); the
// PostgreSQL and DuckLake backends are covered by the instrumentation-backend
// suites that share the same CatalogLogSink schema/writer code paths.

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "detail/gizmosql_logging.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kClusterId = "abcdef01-2345-6789-abcd-ef0123456789";

// Connect + authenticate as the basic-auth admin user, returning a ready
// FlightSqlClient and the call options carrying the bearer token.
struct AuthedClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

// Materialize a single query into an Arrow table.
arrow::Result<std::shared_ptr<arrow::Table>> RunQuery(
    FlightSqlClient& sql_client, const arrow::flight::FlightCallOptions& call_options,
    const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, sql));
  std::shared_ptr<arrow::Table> out;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(out, reader->ToTable());
  }
  return out;
}

}  // namespace

class CatalogLoggingFixture
    : public gizmosql::testing::ServerTestFixture<CatalogLoggingFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "catalog_log_tester.db",
        .port = 31450,
        .health_port = 31451,
        .username = "tester",
        .password = "tester",
        .cluster_id = kClusterId,
        .enable_catalog_logging = true,
        .log_catalog_db_path = "catalog_log_test_logs.db",
    };
  }

  // The integration harness builds the server via CreateFlightSQLServer(),
  // which (unlike RunFlightSQLServer) does not call InitLogging — so the default
  // logger is Arrow's, not the GizmoSQLLogger whose Log() forks records to
  // registered sinks. Install the GizmoSQLLogger here so emitted records route
  // through it and reach the catalog log sink, exactly as in production. The
  // sink itself was registered during server creation; InitLogging does not
  // touch the registered-sinks list, so this only swaps the primary logger.
  void SetUp() override {
    gizmosql::LogConfig cfg;
    cfg.level = arrow::util::ArrowLogLevel::ARROW_INFO;
    gizmosql::InitLogging(cfg);
  }

  // Connect + authenticate as the (admin) basic-auth user.
  AuthedClient Connect() {
    arrow::flight::FlightClientOptions options;
    auto location = arrow::flight::Location::ForGrpcTcp("localhost", GetPort()).ValueOrDie();
    auto client = arrow::flight::FlightClient::Connect(location, options).ValueOrDie();
    arrow::flight::FlightCallOptions call_options;
    auto bearer =
        client->AuthenticateBasicToken({}, GetUsername(), GetPassword()).ValueOrDie();
    call_options.headers.push_back(bearer);
    AuthedClient ac;
    ac.sql_client = std::make_unique<FlightSqlClient>(std::move(client));
    ac.call_options = std::move(call_options);
    return ac;
  }

  // Run a few client queries so the sink has something to fork, then wait for
  // the writer thread to flush.
  void GenerateAndFlushLogs(AuthedClient& ac) {
    for (int i = 0; i < 3; ++i) {
      ASSERT_ARROW_OK(
          RunQuery(*ac.sql_client, ac.call_options, "SELECT 42 AS catalog_log_probe").status());
    }
    // The sink writer thread batches asynchronously; give it time to drain.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }

  int64_t ScalarCount(AuthedClient& ac, const std::string& sql) {
    auto table = RunQuery(*ac.sql_client, ac.call_options, sql).ValueOrDie();
    EXPECT_EQ(table->num_rows(), 1);
    auto arr = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
    return arr->Value(0);
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<CatalogLoggingFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<CatalogLoggingFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<CatalogLoggingFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<CatalogLoggingFixture>::config_{};

// Logs are forked into the catalog's `logs` table, with the promoted columns
// populated (log_time/level are NOT NULL by schema, so any row proves it).
TEST_F(CatalogLoggingFixture, LogsForkedToCatalog) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto ac = Connect();
  GenerateAndFlushLogs(ac);

  int64_t total = ScalarCount(ac, "SELECT count(*) FROM _gizmosql_logs.logs");
  ASSERT_GT(total, 0) << "Expected forked log rows in _gizmosql_logs.logs";

  // Every row has the NOT NULL columns and a recognizable level.
  int64_t well_formed = ScalarCount(
      ac,
      "SELECT count(*) FROM _gizmosql_logs.logs "
      "WHERE log_time IS NOT NULL AND level IN ('DEBUG','INFO','WARN','ERROR','FATAL')");
  ASSERT_EQ(well_formed, total);

  // The instance_id column matches this server's instance (every record carries it).
  int64_t with_instance =
      ScalarCount(ac, "SELECT count(*) FROM _gizmosql_logs.logs WHERE instance_id IS NOT NULL");
  ASSERT_GT(with_instance, 0) << "Expected logs to carry the server instance_id";
}

// The cluster_id is injected into every record and queryable as a column, and
// the GIZMOSQL_CURRENT_CLUSTER() pseudo-function returns the configured UUID.
TEST_F(CatalogLoggingFixture, ClusterIdInLogsAndFunction) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto ac = Connect();

  // GIZMOSQL_CURRENT_CLUSTER() returns the configured cluster UUID.
  auto table =
      RunQuery(*ac.sql_client, ac.call_options, "SELECT GIZMOSQL_CURRENT_CLUSTER() AS c")
          .ValueOrDie();
  ASSERT_EQ(table->num_rows(), 1);
  auto cluster_arr = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  ASSERT_FALSE(cluster_arr->IsNull(0));
  ASSERT_EQ(cluster_arr->GetString(0), kClusterId);

  GenerateAndFlushLogs(ac);

  // All forked rows carry that cluster_id (the server injects it on every record).
  int64_t total = ScalarCount(ac, "SELECT count(*) FROM _gizmosql_logs.logs");
  int64_t with_cluster = ScalarCount(
      ac, std::string("SELECT count(*) FROM _gizmosql_logs.logs "
                      "WHERE CAST(cluster_id AS VARCHAR) = '") +
              kClusterId + "'");
  ASSERT_GT(total, 0);
  ASSERT_EQ(with_cluster, total) << "Every forked log row should carry the cluster_id";
}

// The log catalog is system-managed: admins can read it, but nobody can write to
// it via SQL, and it cannot be DETACHed. (Non-admin read denial shares the exact
// code path proven for the instrumentation catalog in test_catalog_access.)
TEST_F(CatalogLoggingFixture, LogCatalogIsReadOnlyToClients) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto ac = Connect();

  // Admin can read.
  ASSERT_ARROW_OK(
      RunQuery(*ac.sql_client, ac.call_options, "SELECT * FROM _gizmosql_logs.logs LIMIT 1")
          .status());

  // Writes are rejected (system-managed / read-only) — even for the admin.
  auto del = ac.sql_client->Execute(ac.call_options, "DELETE FROM _gizmosql_logs.logs");
  ASSERT_FALSE(del.ok()) << "Clients must not be able to write to the log catalog";
  ASSERT_NE(del.status().ToString().find("read-only"), std::string::npos)
      << del.status().ToString();

  // DETACH of the log catalog is rejected.
  auto detach = ac.sql_client->Execute(ac.call_options, "DETACH _gizmosql_logs");
  ASSERT_FALSE(detach.ok()) << "Clients must not be able to DETACH the log catalog";
  ASSERT_NE(detach.status().ToString().find("Cannot DETACH"), std::string::npos)
      << detach.status().ToString();
}
