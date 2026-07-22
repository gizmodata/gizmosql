// =============================================================================
// Tests: --allow-unsigned-extensions / GIZMOSQL_ALLOW_UNSIGNED_EXTENSIONS
// DuckDB's allow_unsigned_extensions setting is GLOBAL_ONLY, so it can only be
// applied via DBConfig before the database is opened — these tests verify the
// server plumbs the option through (visible via current_setting()) and that
// the secure default remains off.
// =============================================================================

#include <gtest/gtest.h>

#include <arrow/array.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>
#include <arrow/table.h>
#include <memory>
#include <string>

#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

// =============================================================================
// Fixture A: default configuration — unsigned extensions must stay disabled
// =============================================================================
class UnsignedExtensionsDefaultFixture
    : public gizmosql::testing::ServerTestFixture<UnsignedExtensionsDefaultFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "unsigned_ext_default_test.db",
        .port = 31610,
        .health_port = 31611,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        // allow_unsigned_extensions deliberately left at its default (false)
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsDefaultFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsDefaultFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsDefaultFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsDefaultFixture>::config_{};

// =============================================================================
// Fixture B: allow_unsigned_extensions = true
// =============================================================================
class UnsignedExtensionsEnabledFixture
    : public gizmosql::testing::ServerTestFixture<UnsignedExtensionsEnabledFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "unsigned_ext_enabled_test.db",
        .port = 31612,
        .health_port = 31613,
        .username = "testuser",
        .password = "testpassword",
        .enable_instrumentation = false,
        .allow_unsigned_extensions = true,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsEnabledFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsEnabledFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsEnabledFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<UnsignedExtensionsEnabledFixture>::config_{};

// =============================================================================
// Helper: query current_setting('allow_unsigned_extensions') as a string
// =============================================================================
static arrow::Result<std::string> QueryUnsignedExtensionsSetting(
    int port, const std::string& username, const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(
                            location, arrow::flight::FlightClientOptions()));

  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        flight_client->AuthenticateBasicToken({}, username, password));
  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));
  ARROW_ASSIGN_OR_RAISE(
      auto info,
      sql_client.Execute(call_options,
                         "SELECT current_setting('allow_unsigned_extensions')::VARCHAR "
                         "AS setting_value"));

  std::shared_ptr<arrow::Table> table;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
  }
  if (!table || table->num_rows() != 1) {
    return arrow::Status::Invalid("Expected exactly one row from current_setting()");
  }
  auto column = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  return column->GetString(0);
}

TEST_F(UnsignedExtensionsDefaultFixture, DisabledByDefault) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto setting = QueryUnsignedExtensionsSetting(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(setting.ok()) << setting.status().ToString();
  EXPECT_EQ(*setting, "false")
      << "allow_unsigned_extensions must remain off by default (secure default)";
}

TEST_F(UnsignedExtensionsEnabledFixture, EnabledViaConfig) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto setting = QueryUnsignedExtensionsSetting(GetPort(), GetUsername(), GetPassword());
  ASSERT_TRUE(setting.ok()) << setting.status().ToString();
  EXPECT_EQ(*setting, "true")
      << "allow_unsigned_extensions=true must reach DuckDB's DBConfig at open";
}
