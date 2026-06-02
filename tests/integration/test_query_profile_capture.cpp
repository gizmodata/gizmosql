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

// Integration tests for the Enterprise query-profile-capture feature
// (`SET gizmosql.capture_query_profile = off|standard|detailed`). Profiles are
// DuckDB's native profiling JSON, stored verbatim in
// _gizmosql_instr.sql_executions.query_profile. These tests require an enterprise
// license (for instrumentation) and are skipped otherwise.

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// Returns true if a valid enterprise license key file is available.
bool HasEnterpriseLicense() {
  const char* lf = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return lf && lf[0] != '\0';
}

// Execute a query and return the result as a table.
arrow::Result<std::shared_ptr<arrow::Table>> ExecuteAndFetch(
    FlightSqlClient& sql_client, arrow::flight::FlightCallOptions& call_options,
    const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, query));
  if (info->endpoints().empty()) {
    return arrow::Status::Invalid("No endpoints returned");
  }
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  return reader->ToTable();
}

// Execute a statement and drain its result (for SET / probe queries we don't read).
arrow::Status ExecuteAndDrain(FlightSqlClient& sql_client,
                              arrow::flight::FlightCallOptions& call_options,
                              const std::string& query) {
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, query));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable().status());
  }
  return arrow::Status::OK();
}

}  // namespace

class QueryProfileServerFixture
    : public gizmosql::testing::ServerTestFixture<QueryProfileServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "query_profile_test.db",
        .port = 31460,
        .health_port = 31461,
        .username = "tester",
        .password = "tester",
        // Server default stays off; tests opt in per-session / globally.
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<QueryProfileServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<QueryProfileServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<QueryProfileServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<QueryProfileServerFixture>::config_{};

namespace {

// Connect + authenticate, returning a ready FlightSqlClient with bearer header set.
struct Connected {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

// Fetch (has_profile, profile_len) for the most recent execution of a statement
// whose SQL text matches the given probe marker. profile_len is the character
// length of the stored JSON (0 when NULL).
arrow::Result<std::pair<bool, int64_t>> FetchProfileFor(
    FlightSqlClient& sql_client, arrow::flight::FlightCallOptions& call_options,
    const std::string& probe_marker) {
  const std::string query =
      "SELECT e.query_profile IS NOT NULL AS has_profile, "
      "       COALESCE(length(CAST(e.query_profile AS VARCHAR)), 0) AS len "
      "FROM _gizmosql_instr.sql_executions e "
      "JOIN _gizmosql_instr.sql_statements st ON e.statement_id = st.statement_id "
      "WHERE st.sql_text LIKE '%" + probe_marker + "%' "
      "  AND st.sql_text NOT LIKE '%sql_executions%' "
      "ORDER BY e.execution_start_time DESC LIMIT 1";
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, query));
  if (info->endpoints().empty()) return arrow::Status::Invalid("No endpoints");
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
  if (table->num_rows() == 0) return arrow::Status::Invalid("No matching execution row");
  auto has =
      std::static_pointer_cast<arrow::BooleanArray>(table->column(0)->chunk(0))->Value(0);
  auto len =
      std::static_pointer_cast<arrow::Int64Array>(table->column(1)->chunk(0))->Value(0);
  return std::make_pair(has, len);
}

}  // namespace

// Default server mode is off: executions record a NULL query_profile.
TEST_F(QueryProfileServerFixture, CaptureOffByDefaultYieldsNullProfile) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK(
      ExecuteAndDrain(sql_client, call_options, "SELECT 1 AS profile_off_probe"));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  ASSERT_ARROW_OK_AND_ASSIGN(auto result,
                             FetchProfileFor(sql_client, call_options, "profile_off_probe"));
  EXPECT_FALSE(result.first) << "query_profile should be NULL when capture is off";
}

// SET SESSION standard => subsequent executions store a non-empty JSON profile.
TEST_F(QueryProfileServerFixture, SessionStandardCapturesProfileJson) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SET gizmosql.capture_query_profile = 'standard'"));
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SELECT 42 AS profile_standard_probe"));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto result, FetchProfileFor(sql_client, call_options, "profile_standard_probe"));
  EXPECT_TRUE(result.first) << "query_profile should be populated under 'standard'";
  EXPECT_GT(result.second, 0) << "query_profile JSON should be non-empty";

  // Verify it is valid JSON via DuckDB's json_valid.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto valid_table,
      ExecuteAndFetch(
          sql_client, call_options,
          "SELECT bool_and(json_valid(CAST(e.query_profile AS VARCHAR))) "
          "FROM _gizmosql_instr.sql_executions e "
          "JOIN _gizmosql_instr.sql_statements st ON e.statement_id = st.statement_id "
          "WHERE st.sql_text LIKE '%profile_standard_probe%' "
          "  AND e.query_profile IS NOT NULL"));
  ASSERT_GT(valid_table->num_rows(), 0);
  auto all_valid =
      std::static_pointer_cast<arrow::BooleanArray>(valid_table->column(0)->chunk(0));
  EXPECT_TRUE(all_valid->Value(0)) << "Stored query_profile must be valid JSON";
}

// SET SESSION detailed => a populated, valid JSON profile (richer than standard,
// but we assert version-tolerantly: non-empty + valid).
TEST_F(QueryProfileServerFixture, SessionDetailedCapturesProfileJson) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SET gizmosql.capture_query_profile = 'detailed'"));
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SELECT 7 AS profile_detailed_probe"));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto result, FetchProfileFor(sql_client, call_options, "profile_detailed_probe"));
  EXPECT_TRUE(result.first) << "query_profile should be populated under 'detailed'";
  EXPECT_GT(result.second, 0) << "detailed query_profile JSON should be non-empty";
}

// SET GLOBAL applies to a brand-new session (admin only). The system user
// (username/password) has the admin role.
TEST_F(QueryProfileServerFixture, GlobalScopeAppliesToNewSession) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Session A: set the global default to standard.
  {
    arrow::flight::FlightClientOptions options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(location, options));
    arrow::flight::FlightCallOptions call_options;
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options.headers.push_back(bearer);
    FlightSqlClient sql_client(std::move(client));
    ASSERT_ARROW_OK(ExecuteAndDrain(
        sql_client, call_options,
        "SET GLOBAL gizmosql.capture_query_profile = 'standard'"));
  }

  // Session B: a fresh connection inherits the server default without any SET.
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SELECT 5 AS profile_global_probe"));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto result, FetchProfileFor(sql_client, call_options, "profile_global_probe"));
  EXPECT_TRUE(result.first)
      << "query_profile should be populated when the global default is 'standard'";

  // Reset the global default so it does not leak into other tests in this suite.
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SET GLOBAL gizmosql.capture_query_profile = 'off'"));
}

// Toggling back to off mid-session stops capture for subsequent executions.
TEST_F(QueryProfileServerFixture, ToggleOffStopsCapture) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SET gizmosql.capture_query_profile = 'standard'"));
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SELECT 11 AS profile_toggle_on_probe"));
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SET gizmosql.capture_query_profile = 'off'"));
  ASSERT_ARROW_OK(ExecuteAndDrain(sql_client, call_options,
                                  "SELECT 12 AS profile_toggle_off_probe"));
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto on_res, FetchProfileFor(sql_client, call_options, "profile_toggle_on_probe"));
  EXPECT_TRUE(on_res.first) << "Profile captured while standard was active";

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto off_res, FetchProfileFor(sql_client, call_options, "profile_toggle_off_probe"));
  EXPECT_FALSE(off_res.first) << "Profile must be NULL after toggling capture off";
}

// An invalid mode value is rejected with a clear error.
TEST_F(QueryProfileServerFixture, InvalidValueRejected) {
  if (!HasEnterpriseLicense()) GTEST_SKIP() << "Requires enterprise license";
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  auto status = ExecuteAndDrain(sql_client, call_options,
                                "SET gizmosql.capture_query_profile = 'bogus'");
  ASSERT_FALSE(status.ok()) << "An invalid capture mode should be rejected";
  EXPECT_NE(status.ToString().find("capture_query_profile"), std::string::npos)
      << "Error should mention the offending parameter: " << status.ToString();
}
