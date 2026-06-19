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

// Integration tests for the *live adjustment* of the graceful-shutdown settings.
//
// Graceful shutdown originally exposed only the boot-time --graceful-shutdown /
// --shutdown-grace-period-seconds flags. They are now also surfaced as
// live-adjustable, server-global (admin-only) settings:
//
//   * gizmosql.graceful_shutdown               (BOOLEAN)
//   * gizmosql.shutdown_grace_period_seconds   (INTEGER, seconds; 0 = unlimited)
//
// These tests cover the settings plumbing end-to-end via a Flight SQL client:
// SET GLOBAL applies and is reflected in the gizmosql_settings() view, and the
// value parsing / validation is enforced. The actual drain-on-signal behavior is
// covered by test_graceful_shutdown.cpp. (Not enterprise-gated — graceful
// shutdown is a Core feature.)
//
// Note: the underlying state is process-global, so every test SETs the value it
// asserts on rather than relying on a default, to stay robust against any other
// test in the binary having toggled it.

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

class GracefulShutdownSettingsFixture
    : public gizmosql::testing::ServerTestFixture<GracefulShutdownSettingsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "graceful_shutdown_settings_tester.db",
        .port = 31414,
        .health_port = 31415,
        .username = "admin",
        .password = "admin",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<GracefulShutdownSettingsFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<GracefulShutdownSettingsFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<GracefulShutdownSettingsFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<GracefulShutdownSettingsFixture>::config_{};

namespace {

struct AdminClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

// Connect + Basic-authenticate as the configured (admin-role) user.
arrow::Result<AdminClient> ConnectAdmin(int port, const std::string& user,
                                        const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  arrow::flight::FlightClientOptions options;
  ARROW_ASSIGN_OR_RAISE(auto client,
                        arrow::flight::FlightClient::Connect(location, options));
  AdminClient ac;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, user, password));
  ac.call_options.headers.push_back(bearer);
  ac.sql_client = std::make_unique<FlightSqlClient>(std::move(client));
  return ac;
}

// Execute a statement and fully drain it (GetFlightInfo -> DoGet -> ToTable),
// returning the result table. For SET commands this forces HandleGizmoSQLSet() to
// run so its success/error surfaces here.
arrow::Result<std::shared_ptr<arrow::Table>> RunQuery(AdminClient& ac,
                                                      const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, ac.sql_client->Execute(ac.call_options, sql));
  std::shared_ptr<arrow::Table> table;
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          ac.sql_client->DoGet(ac.call_options, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
  }
  return table;
}

arrow::Status RunStatement(AdminClient& ac, const std::string& sql) {
  return RunQuery(ac, sql).status();
}

// Read a single column of a single-setting gizmosql_settings() row as a string.
arrow::Result<std::string> GetSettingColumn(AdminClient& ac, const std::string& name,
                                            const std::string& column) {
  ARROW_ASSIGN_OR_RAISE(
      auto table,
      RunQuery(ac, "SELECT CAST(" + column +
                       " AS VARCHAR) FROM gizmosql_settings() WHERE name = '" + name +
                       "'"));
  if (!table || table->num_rows() != 1) {
    return arrow::Status::Invalid("expected exactly one row for setting ", name);
  }
  auto chunk = std::static_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  if (chunk->IsNull(0)) return std::string{};
  return chunk->GetString(0);
}

}  // namespace

// SET GLOBAL gizmosql.graceful_shutdown toggles the flag, reflected in the view.
TEST_F(GracefulShutdownSettingsFixture, GracefulShutdownIsLiveAdjustable) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_OK(RunStatement(ac, "SET GLOBAL gizmosql.graceful_shutdown = true"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto enabled,
                             GetSettingColumn(ac, "gizmosql.graceful_shutdown", "value"));
  EXPECT_EQ(enabled, "true");

  ASSERT_OK(RunStatement(ac, "SET GLOBAL gizmosql.graceful_shutdown = false"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto disabled,
      GetSettingColumn(ac, "gizmosql.graceful_shutdown", "global_value"));
  EXPECT_EQ(disabled, "false");

  // The descriptor metadata is stable regardless of the live value.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto scope, GetSettingColumn(ac, "gizmosql.graceful_shutdown", "scope"));
  EXPECT_EQ(scope, "GLOBAL");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto ent, GetSettingColumn(ac, "gizmosql.graceful_shutdown", "enterprise"));
  EXPECT_EQ(ent, "false");
}

// SET GLOBAL gizmosql.shutdown_grace_period_seconds applies and is reflected.
TEST_F(GracefulShutdownSettingsFixture, GracePeriodIsLiveAdjustable) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_OK(
      RunStatement(ac, "SET GLOBAL gizmosql.shutdown_grace_period_seconds = 600"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto v, GetSettingColumn(ac, "gizmosql.shutdown_grace_period_seconds", "value"));
  EXPECT_EQ(v, "600");

  // 0 = wait indefinitely is allowed.
  ASSERT_OK(RunStatement(ac, "SET GLOBAL gizmosql.shutdown_grace_period_seconds = 0"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto zero,
      GetSettingColumn(ac, "gizmosql.shutdown_grace_period_seconds", "global_value"));
  EXPECT_EQ(zero, "0");

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto def,
      GetSettingColumn(ac, "gizmosql.shutdown_grace_period_seconds", "default_value"));
  EXPECT_EQ(def, "300");
}

// A negative grace period is rejected.
TEST_F(GracefulShutdownSettingsFixture, GracePeriodRejectsNegative) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  auto status =
      RunStatement(ac, "SET GLOBAL gizmosql.shutdown_grace_period_seconds = -5");
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("shutdown_grace_period_seconds"), std::string::npos)
      << "expected a validation error mentioning the setting; got: "
      << status.ToString();
}

// A non-boolean value for the enable flag is rejected.
TEST_F(GracefulShutdownSettingsFixture, GracefulShutdownRejectsInvalidValue) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  auto status = RunStatement(ac, "SET GLOBAL gizmosql.graceful_shutdown = 'maybe'");
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("graceful_shutdown"), std::string::npos)
      << "expected an 'Invalid value for graceful_shutdown' error; got: "
      << status.ToString();
}
