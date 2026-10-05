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

// Basic-auth credential parsing (RFC 7617)
//
// The client sends base64("username:password"). The user-id cannot contain
// ':', so the FIRST ':' separates it from the password, and the password may
// contain ':'. GizmoSQL used to cut the password at its own next ':' — so a
// password like "pa:ss" never authenticated, while "<password>:<anything>"
// did. A ':' in the server's configured username is rejected at startup.

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "gizmosql_library.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// Authenticate with Basic credentials; on success also run a query, so a
// "successful" handshake is proven usable.
arrow::Status LoginAndQuery(int port, const std::string& username,
                            const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location));
  arrow::flight::FlightCallOptions call_options;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, username, password));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, "SELECT 1"));
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  return reader->ToTable().status();
}

}  // namespace

// ----------------------------------------------------------------------------
// A server whose password contains ':'
// ----------------------------------------------------------------------------
class ColonPasswordFixture
    : public gizmosql::testing::ServerTestFixture<ColonPasswordFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31664,
        .health_port = 31665,
        .username = "colon_user",
        .password = "pa:ss:word",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<ColonPasswordFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<ColonPasswordFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<ColonPasswordFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<ColonPasswordFixture>::config_{};

TEST_F(ColonPasswordFixture, PasswordWithColonsAuthenticates) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK(LoginAndQuery(GetPort(), GetUsername(), "pa:ss:word"));
}

TEST_F(ColonPasswordFixture, PrefixOrExtendedPasswordIsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  for (const std::string wrong : {"pa", "pa:ss", "pa:ss:word:", "pa:ss:word:extra"}) {
    auto status = LoginAndQuery(GetPort(), GetUsername(), wrong);
    ASSERT_FALSE(status.ok()) << "authenticated with wrong password '" << wrong << "'";
    EXPECT_NE(status.ToString().find("Invalid credentials"), std::string::npos)
        << wrong << " -> " << status.ToString();
  }
}

// ----------------------------------------------------------------------------
// A server whose password has no ':' — appending ":<anything>" must not work
// ----------------------------------------------------------------------------
class PlainPasswordFixture
    : public gizmosql::testing::ServerTestFixture<PlainPasswordFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31666,
        .health_port = 31667,
        .username = "plain_user",
        .password = "secret",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<PlainPasswordFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<PlainPasswordFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<PlainPasswordFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<PlainPasswordFixture>::config_{};

TEST_F(PlainPasswordFixture, CorrectPasswordAuthenticates) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK(LoginAndQuery(GetPort(), GetUsername(), "secret"));
}

TEST_F(PlainPasswordFixture, PasswordWithAppendedColonSuffixIsRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  auto status = LoginAndQuery(GetPort(), GetUsername(), "secret:anything");
  ASSERT_FALSE(status.ok()) << "'secret:anything' authenticated as 'secret'";
  EXPECT_NE(status.ToString().find("Invalid credentials"), std::string::npos)
      << status.ToString();
}

// ----------------------------------------------------------------------------
// A ':' in the configured username is refused at startup
// ----------------------------------------------------------------------------
TEST(CredentialValidation, UsernameWithColonIsRejectedAtStartup) {
  std::filesystem::path database_filename(":memory:");
  auto result = gizmosql::CreateFlightSQLServer(
      BackendType::duckdb, database_filename, /*hostname=*/"localhost",
      /*port=*/31668,
      /*username=*/"ad:min",
      /*password=*/"password",
      /*secret_key=*/"test_secret_key",
      /*tls_cert_path=*/std::filesystem::path(),
      /*tls_key_path=*/std::filesystem::path(),
      /*mtls_ca_cert_path=*/std::filesystem::path(),
      /*init_sql_commands=*/"",
      /*init_sql_commands_file=*/std::filesystem::path(),
      /*print_queries=*/false,
      /*read_only=*/false,
      /*token_allowed_issuer=*/"",
      /*token_allowed_audience=*/"",
      /*token_signature_verify_cert_path=*/std::filesystem::path(),
      /*token_jwks_uri=*/"",
      /*token_default_role=*/"",
      /*token_authorized_emails=*/"",
      /*access_logging_enabled=*/false,
      /*query_timeout=*/0,
      /*query_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*auth_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*session_log_level=*/arrow::util::ArrowLogLevel::ARROW_INFO,
      /*health_port=*/31669,
      /*health_check_query=*/"",
      /*enable_instrumentation=*/false,
      /*instrumentation_db_path=*/"");
  ASSERT_FALSE(result.ok()) << "server started with a ':' in its username";
  EXPECT_NE(result.status().ToString().find("username must not contain a ':'"),
            std::string::npos)
      << result.status().ToString();
}
