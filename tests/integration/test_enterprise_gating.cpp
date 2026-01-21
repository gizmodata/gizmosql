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

// Tests that enterprise features are properly gated when no license is present.
// These tests should pass in both Core and Enterprise builds when no license is provided.

#include <gtest/gtest.h>
#include <cstdlib>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"

#include "test_util.h"
#include "test_server_fixture.h"

// Helper to check if enterprise license is available - these tests
// should only run when NO license is present
static bool IsEnterpriseLicenseAvailable() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

#define SKIP_IF_LICENSE_PRESENT() \
  if (IsEnterpriseLicenseAvailable()) { \
    GTEST_SKIP() << "License present - these tests verify behavior WITHOUT a license. " \
                 << "Unset GIZMOSQL_LICENSE_KEY_FILE to run these tests."; \
  }

namespace flight = arrow::flight;
namespace flightsql = arrow::flight::sql;

// ============================================================================
// Test Fixture for Enterprise Gating Tests
// ============================================================================

class EnterpriseGatingTestFixture
    : public gizmosql::testing::ServerTestFixture<EnterpriseGatingTestFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31350,
        .health_port = 0,  // disabled
        .username = "test_user",
        .password = "test_password_enterprise_gating",
        .enable_instrumentation = false,  // No instrumentation (no license)
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<flightsql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<EnterpriseGatingTestFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<EnterpriseGatingTestFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<EnterpriseGatingTestFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<EnterpriseGatingTestFixture>::config_{};

// ============================================================================
// Enterprise Feature Gating Tests
// ============================================================================

// Test that KILL SESSION is rejected without enterprise license
TEST_F(EnterpriseGatingTestFixture, KillSessionRejectedWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             flight::FlightClient::Connect(location, options));

  // Authenticate
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  flight::FlightCallOptions call_options;
  call_options.headers.emplace_back(bearer.first, bearer.second);

  flightsql::FlightSqlClient sql_client(std::move(client));

  // Try to execute KILL SESSION command (using UUID format)
  auto result = sql_client.Execute(call_options, "KILL SESSION '00000000-0000-0000-0000-000000000000'");

  // Should fail with an error about enterprise feature
  ASSERT_FALSE(result.ok());
  std::string error_msg = result.status().ToString();

  // Check that the error message mentions it's an enterprise feature
  EXPECT_TRUE(error_msg.find("enterprise") != std::string::npos ||
              error_msg.find("Enterprise") != std::string::npos ||
              error_msg.find("license") != std::string::npos)
      << "Expected enterprise/license error, got: " << error_msg;
}

// Test that basic queries still work (not an enterprise feature)
TEST_F(EnterpriseGatingTestFixture, BasicQueriesWorkWithoutLicense) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             flight::FlightClient::Connect(location, options));

  // Authenticate
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  flight::FlightCallOptions call_options;
  call_options.headers.emplace_back(bearer.first, bearer.second);

  flightsql::FlightSqlClient sql_client(std::move(client));

  // Execute a basic query
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client.Execute(call_options, "SELECT 1 AS value"));

  // Get results
  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));

  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 1);
}

// Test that GIZMOSQL_EDITION() returns 'Core' without license
TEST_F(EnterpriseGatingTestFixture, EditionReturnsCoreWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             flight::FlightClient::Connect(location, options));

  // Authenticate
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  flight::FlightCallOptions call_options;
  call_options.headers.emplace_back(bearer.first, bearer.second);

  flightsql::FlightSqlClient sql_client(std::move(client));

  // Execute GIZMOSQL_EDITION() query
  ASSERT_ARROW_OK_AND_ASSIGN(auto info,
                             sql_client.Execute(call_options, "SELECT GIZMOSQL_EDITION()"));

  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));

  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 1);

  // Edition should be 'Core' without a valid enterprise license
  auto edition_array = std::dynamic_pointer_cast<arrow::StringArray>(table->column(0)->chunk(0));
  ASSERT_NE(edition_array, nullptr);
  EXPECT_EQ(edition_array->GetString(0), "Core");
}
