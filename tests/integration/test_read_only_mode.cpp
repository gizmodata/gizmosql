// Regression test: server must start successfully when opened in read-only
// mode. Previously, startup failed with
//   "ATTACH ':memory:' AS _gizmosql_system; Error: Invalid: Catalog Error:
//    Cannot launch in-memory database in read-only mode!"
// because the system-catalog ATTACH inherited the parent's READ_ONLY access
// mode. The fix marks the system-catalog ATTACH (and the instrumentation
// ATTACH) as (READ_WRITE) explicitly.
//
// This test pre-creates a DuckDB file (DuckDB cannot open a non-existent
// file in read-only mode), starts the server with read_only=true, then
// verifies the system-catalog views are queryable end-to-end.
//
// We bypass ServerTestFixture intentionally: that fixture auto-deletes any
// existing database file before server startup, which is incompatible with
// read-only mode (the file must already exist).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <thread>

#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "duckdb.hpp"
#include "test_server_fixture.h"  // pulls in CreateFlightSQLServer fwd-decl
#include "test_util.h"

namespace fs = std::filesystem;
using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kDbFile = "read_only_test.db";
constexpr int kPort = 31380;
constexpr int kHealthPort = 31381;
constexpr const char* kUser = "rouser";
constexpr const char* kPassword = "ropassword123";

}  // namespace

class ReadOnlyServerTest : public ::testing::Test {
 protected:
  static std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> server_;
  static std::thread server_thread_;
  static std::atomic<bool> server_ready_;

  static void SetUpTestSuite() {
    std::error_code ec;
    fs::remove(kDbFile, ec);
    fs::remove(std::string(kDbFile) + ".wal", ec);

    {
      duckdb::DuckDB db(kDbFile);
      duckdb::Connection conn(db);
      auto r = conn.Query(
          "CREATE TABLE sentinel (n INTEGER); INSERT INTO sentinel VALUES (1);");
      ASSERT_FALSE(r->HasError()) << r->GetError();
    }

    fs::path db_path(kDbFile);
    auto result = gizmosql::CreateFlightSQLServer(
        BackendType::duckdb, db_path, "localhost", kPort, kUser, kPassword,
        /*secret_key=*/"test_secret_key_for_testing",
        /*tls_cert_path=*/fs::path(),
        /*tls_key_path=*/fs::path(),
        /*mtls_ca_cert_path=*/fs::path(),
        /*init_sql_commands=*/"",
        /*init_sql_commands_file=*/fs::path(),
        /*print_queries=*/false,
        /*read_only=*/true,
        /*token_allowed_issuer=*/"",
        /*token_allowed_audience=*/"",
        /*token_signature_verify_cert_path=*/fs::path(),
        /*token_jwks_uri=*/"",
        /*token_default_role=*/"",
        /*token_authorized_emails=*/"",
        /*access_logging_enabled=*/false,
        /*query_timeout=*/0,
        /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*session_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
        /*health_port=*/kHealthPort,
        /*health_check_query=*/"",
        /*enable_instrumentation=*/false);

    ASSERT_TRUE(result.ok())
        << "Server failed to start in read-only mode: " << result.status().ToString();
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
      if (std::chrono::steady_clock::now() - start > std::chrono::seconds(10)) {
        FAIL() << "Server failed to start within timeout";
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  static void TearDownTestSuite() {
    if (server_) (void)server_->Shutdown();
    if (server_thread_.joinable()) server_thread_.join();
    server_.reset();
    server_ready_ = false;
    gizmosql::CleanupServerResources();

    std::error_code ec;
    fs::remove(kDbFile, ec);
    fs::remove(std::string(kDbFile) + ".wal", ec);
  }

  static FlightSqlClient ConnectAndAuth(arrow::flight::FlightCallOptions& opts) {
    arrow::flight::FlightClientOptions client_opts;
    auto loc = arrow::flight::Location::ForGrpcTcp("localhost", kPort);
    EXPECT_TRUE(loc.ok()) << loc.status();
    auto client_res = arrow::flight::FlightClient::Connect(*loc, client_opts);
    EXPECT_TRUE(client_res.ok()) << client_res.status();
    auto client = std::move(*client_res);
    auto bearer = client->AuthenticateBasicToken({}, kUser, kPassword);
    EXPECT_TRUE(bearer.ok()) << bearer.status();
    opts.headers.push_back(*bearer);
    return FlightSqlClient(std::move(client));
  }

  static arrow::Status ExecuteAndConsume(
      FlightSqlClient& client, const arrow::flight::FlightCallOptions& opts,
      const std::string& query) {
    ARROW_ASSIGN_OR_RAISE(auto info, client.Execute(opts, query));
    for (const auto& endpoint : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client.DoGet(opts, endpoint.ticket));
      ARROW_RETURN_NOT_OK(reader->ToTable().status());
    }
    return arrow::Status::OK();
  }
};

std::shared_ptr<arrow::flight::sql::FlightSqlServerBase> ReadOnlyServerTest::server_{};
std::thread ReadOnlyServerTest::server_thread_{};
std::atomic<bool> ReadOnlyServerTest::server_ready_{false};

TEST_F(ReadOnlyServerTest, ServerStartsAndSystemCatalogIsQueryable) {
  ASSERT_TRUE(server_ready_) << "Server not ready";
  arrow::flight::FlightCallOptions call_options;
  auto sql_client = ConnectAndAuth(call_options);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Sentinel data from the pre-created file is readable.
  ASSERT_ARROW_OK(ExecuteAndConsume(sql_client, call_options,
                                    "SELECT n FROM sentinel"));

  // The system-catalog views attached at startup are queryable — this is
  // the actual regression: previously the ATTACH ':memory:' for the system
  // catalog failed under read-only mode and the server never finished init.
  ASSERT_ARROW_OK(ExecuteAndConsume(
      sql_client, call_options,
      "SELECT * FROM _gizmosql_system.main.gizmosql_index_info LIMIT 1"));
  ASSERT_ARROW_OK(ExecuteAndConsume(
      sql_client, call_options,
      "SELECT * FROM _gizmosql_system.main.gizmosql_view_definition LIMIT 1"));

  // Writes to the main (read-only) database must still be denied.
  auto write_result = sql_client.ExecuteUpdate(
      call_options, "INSERT INTO sentinel VALUES (2)");
  ASSERT_FALSE(write_result.ok())
      << "Expected INSERT against read-only main DB to be denied";
}
