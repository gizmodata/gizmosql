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
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <optional>

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

// Test that SET gizmosql.session_tag is rejected without enterprise license
TEST_F(EnterpriseGatingTestFixture, SetSessionTagRejectedWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             flight::FlightClient::Connect(location, options));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  flight::FlightCallOptions call_options;
  call_options.headers.emplace_back(bearer.first, bearer.second);

  flightsql::FlightSqlClient sql_client(std::move(client));

  // Execute returns FlightInfo OK; error surfaces during DoGet
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.session_tag = '{"test":"gating"}')"));

  bool got_error = false;
  std::string error_msg;
  for (const auto& endpoint : info->endpoints()) {
    auto reader_result = sql_client.DoGet(call_options, endpoint.ticket);
    if (!reader_result.ok()) {
      got_error = true;
      error_msg = reader_result.status().ToString();
      break;
    }
    auto table_result = reader_result.ValueOrDie()->ToTable();
    if (!table_result.ok()) {
      got_error = true;
      error_msg = table_result.status().ToString();
      break;
    }
  }

  ASSERT_TRUE(got_error) << "SET gizmosql.session_tag should be rejected without license";
  EXPECT_TRUE(error_msg.find("enterprise") != std::string::npos ||
              error_msg.find("Enterprise") != std::string::npos ||
              error_msg.find("license") != std::string::npos)
      << "Expected enterprise/license error, got: " << error_msg;
}

// Test that SET gizmosql.query_tag is rejected without enterprise license
TEST_F(EnterpriseGatingTestFixture, SetQueryTagRejectedWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             flight::FlightClient::Connect(location, options));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  flight::FlightCallOptions call_options;
  call_options.headers.emplace_back(bearer.first, bearer.second);

  flightsql::FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client.Execute(call_options,
                         R"(SET gizmosql.query_tag = '{"test":"gating"}')"));

  bool got_error = false;
  std::string error_msg;
  for (const auto& endpoint : info->endpoints()) {
    auto reader_result = sql_client.DoGet(call_options, endpoint.ticket);
    if (!reader_result.ok()) {
      got_error = true;
      error_msg = reader_result.status().ToString();
      break;
    }
    auto table_result = reader_result.ValueOrDie()->ToTable();
    if (!table_result.ok()) {
      got_error = true;
      error_msg = table_result.status().ToString();
      break;
    }
  }

  ASSERT_TRUE(got_error) << "SET gizmosql.query_tag should be rejected without license";
  EXPECT_TRUE(error_msg.find("enterprise") != std::string::npos ||
              error_msg.find("Enterprise") != std::string::npos ||
              error_msg.find("license") != std::string::npos)
      << "Expected enterprise/license error, got: " << error_msg;
}

// ============================================================================
// Statement-queue startup gate
//
// Statement queuing (--max-concurrent-statements and its tuning knobs) requires
// the 'statement_queue' Enterprise feature. Configuring it without a license
// must HARD-ERROR at startup (RunFlightSQLServer returns non-zero before the
// server begins serving), rather than silently running with the concurrency cap
// unenforced. These call RunFlightSQLServer directly (like test_max_metadata_size)
// because the startup gate lives there, not in CreateFlightSQLServer used by the
// fixture. They only run without a license (the gate is a no-op when licensed).
// ============================================================================

namespace {

// Invoke RunFlightSQLServer with the given statement-queue knobs and NO license,
// returning its exit code. The startup gate returns before serving, so this
// completes promptly; the 20s timeout guards against a regression where the gate
// fails to fire and the server actually starts (reported as the -999 sentinel).
int RunServerStatementQueueGate(int32_t max_concurrent, int32_t max_queued,
                                int32_t max_queue_wait, int port) {
  auto fut = std::async(std::launch::async, [=]() {
    return RunFlightSQLServer(
        BackendType::duckdb, std::filesystem::path(":memory:"),
        /*hostname=*/"localhost", port, /*username=*/"gating_user",
        /*password=*/"gating_pass", /*secret_key=*/"test_secret_key_for_testing",
        /*tls_cert_path=*/std::filesystem::path(),
        /*tls_key_path=*/std::filesystem::path(),
        /*mtls_ca_cert_path=*/std::filesystem::path(),
        /*init_sql_commands=*/"", /*init_sql_commands_file=*/std::filesystem::path(),
        /*print_queries=*/false, /*read_only=*/false,
        /*token_allowed_issuer=*/"", /*token_allowed_audience=*/"",
        /*token_signature_verify_cert_path=*/std::filesystem::path(),
        /*token_jwks_uri=*/"", /*token_default_role=*/"",
        /*token_authorized_emails=*/"", /*log_level=*/"error", /*log_format=*/"text",
        /*access_log=*/"off", /*log_file=*/"", /*query_timeout=*/0,
        /*query_log_level=*/"error", /*auth_log_level=*/"error",
        /*session_log_level=*/"error", /*health_port=*/0, /*health_check_query=*/"",
        /*enable_instrumentation=*/std::optional<bool>(false),
        /*instrumentation_db_path=*/"", /*instrumentation_catalog=*/"",
        /*instrumentation_schema=*/"", /*instance_tag=*/"",
        /*license_key_file=*/"", /*license_key=*/"",
        /*allow_cross_instance_tokens=*/std::optional<bool>(false),
        /*oauth_client_id=*/"", /*oauth_client_secret=*/"", /*oauth_scopes=*/"",
        /*oauth_port=*/0, /*oauth_base_url=*/"", /*oauth_redirect_uri=*/"",
        /*oauth_instance_id=*/"", /*oauth_disable_tls=*/std::optional<bool>(false),
        /*otel_enabled=*/std::optional<bool>(false), /*otel_exporter=*/"",
        /*otel_endpoint=*/"", /*otel_service_name=*/"", /*otel_headers=*/"",
        /*max_metadata_size=*/0, /*storage_version=*/"",
        /*max_concurrent_statements=*/max_concurrent,
        /*max_queued_statements=*/max_queued,
        /*max_queue_wait_seconds=*/max_queue_wait);
  });
  if (fut.wait_for(std::chrono::seconds(20)) != std::future_status::ready) {
    ShutdownFlightServer();  // gate didn't fire and the server is serving; unblock
    fut.wait();
    return -999;
  }
  return fut.get();
}

}  // namespace

TEST(StatementQueueLicenseGating, StartupRejectsMaxConcurrentWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  int rc = RunServerStatementQueueGate(/*max_concurrent=*/8, /*max_queued=*/-1,
                                       /*max_queue_wait=*/-1, /*port=*/31352);
  EXPECT_NE(rc, -999) << "Server must fail fast at startup, not start serving";
  EXPECT_NE(rc, 0)
      << "Unlicensed --max-concurrent-statements must hard-error at startup";
}

TEST(StatementQueueLicenseGating, StartupRejectsQueueTuningKnobWithoutLicense) {
  SKIP_IF_LICENSE_PRESENT();
  // Even with concurrency disabled (0), explicitly setting a queue tuning knob
  // (here --max-queue-wait) signals intent to use the licensed feature and must
  // hard-error rather than be silently ignored.
  int rc = RunServerStatementQueueGate(/*max_concurrent=*/0, /*max_queued=*/-1,
                                       /*max_queue_wait=*/60, /*port=*/31353);
  EXPECT_NE(rc, -999) << "Server must fail fast at startup, not start serving";
  EXPECT_NE(rc, 0) << "Unlicensed --max-queue-wait must hard-error at startup";
}
