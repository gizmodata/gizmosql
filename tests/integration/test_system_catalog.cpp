// Tests for the GizmoSQL system catalog (_gizmosql_system).
//
// The catalog is created at server startup as an in-memory DuckDB attach
// and hosts metadata helper views (gizmosql_index_info,
// gizmosql_view_definition). It must be:
//   * readable by every authenticated user (regardless of role/license),
//   * write-protected for everyone — DDL or DML targeting the catalog
//     must fail with a clear "read-only" error.
//
// This guards against regressions in src/duckdb/duckdb_statement.cpp's
// system-catalog write-deny gate and in
// src/enterprise/catalog_permissions/catalog_permissions_handler.cpp's
// short-circuit that grants kRead for the system catalog regardless of
// catalog_access rules.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

class SystemCatalogServerFixture
    : public gizmosql::testing::ServerTestFixture<SystemCatalogServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "system_catalog_test.db",
        .port = 31360,
        .health_port = 31361,
        .username = "sysuser",
        .password = "syspassword123",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SystemCatalogServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<SystemCatalogServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SystemCatalogServerFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SystemCatalogServerFixture>::config_{};

namespace {

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

FlightSqlClient ConnectAndAuth(int port, const std::string& user,
                               const std::string& password,
                               arrow::flight::FlightCallOptions& call_options) {
  arrow::flight::FlightClientOptions options;
  auto location_res = arrow::flight::Location::ForGrpcTcp("localhost", port);
  EXPECT_TRUE(location_res.ok()) << location_res.status();
  auto client_res = arrow::flight::FlightClient::Connect(*location_res, options);
  EXPECT_TRUE(client_res.ok()) << client_res.status();
  auto client = std::move(*client_res);
  auto bearer_res = client->AuthenticateBasicToken({}, user, password);
  EXPECT_TRUE(bearer_res.ok()) << bearer_res.status();
  call_options.headers.push_back(*bearer_res);
  return FlightSqlClient(std::move(client));
}

}  // namespace

TEST_F(SystemCatalogServerFixture, ReadsAreAllowed) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightCallOptions call_options;
  auto sql_client =
      ConnectAndAuth(GetPort(), GetUsername(), GetPassword(), call_options);
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Both metadata helper views must be queryable.
  ASSERT_ARROW_OK(ExecuteAndConsume(
      sql_client, call_options,
      "SELECT * FROM _gizmosql_system.main.gizmosql_index_info LIMIT 1"));
  ASSERT_ARROW_OK(ExecuteAndConsume(
      sql_client, call_options,
      "SELECT * FROM _gizmosql_system.main.gizmosql_view_definition LIMIT 1"));
}

TEST_F(SystemCatalogServerFixture, CreateInSystemCatalogIsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightCallOptions call_options;
  auto sql_client =
      ConnectAndAuth(GetPort(), GetUsername(), GetPassword(), call_options);

  auto result = sql_client.Execute(
      call_options,
      "CREATE TABLE _gizmosql_system.main.t_should_fail (id INTEGER)");
  ASSERT_FALSE(result.ok()) << "Expected CREATE in _gizmosql_system to be denied";
  EXPECT_NE(result.status().message().find("read-only"), std::string::npos)
      << "Error should mention read-only: " << result.status().message();
}

TEST_F(SystemCatalogServerFixture, DropViewInSystemCatalogIsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightCallOptions call_options;
  auto sql_client =
      ConnectAndAuth(GetPort(), GetUsername(), GetPassword(), call_options);

  auto result = sql_client.Execute(
      call_options, "DROP VIEW _gizmosql_system.main.gizmosql_index_info");
  ASSERT_FALSE(result.ok()) << "Expected DROP VIEW in _gizmosql_system to be denied";
  EXPECT_NE(result.status().message().find("read-only"), std::string::npos)
      << "Error should mention read-only: " << result.status().message();
}

TEST_F(SystemCatalogServerFixture, InsertIntoSystemCatalogIsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightCallOptions call_options;
  auto sql_client =
      ConnectAndAuth(GetPort(), GetUsername(), GetPassword(), call_options);

  // The helper views are not insertable, but the server must reject the
  // statement at the catalog gate before DuckDB sees it. Verify a generic
  // write into the system catalog fails with the read-only message.
  auto result = sql_client.Execute(
      call_options,
      "CREATE OR REPLACE VIEW _gizmosql_system.main.evil AS SELECT 1");
  ASSERT_FALSE(result.ok()) << "Expected write to _gizmosql_system to be denied";
  EXPECT_NE(result.status().message().find("read-only"), std::string::npos)
      << "Error should mention read-only: " << result.status().message();
}
