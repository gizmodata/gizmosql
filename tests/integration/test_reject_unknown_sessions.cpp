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

// --reject-unknown-sessions
//
// A bearer token names its session. When that session is gone from the
// instance (closed, idle-evicted) or was created by another instance, GizmoSQL
// has historically created a new, empty session for the token without a word —
// losing the client's open transaction, temp tables, USE and SET. With
// --reject-unknown-sessions the request fails with Unauthenticated instead.
// The default (off) keeps the historical behavior.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kTestSecretKey = "test_secret_key_for_testing";

struct Session {
  std::unique_ptr<arrow::flight::FlightClient> flight;
  arrow::flight::FlightCallOptions call_options;
};

arrow::Result<Session> Login(int port, const std::string& username,
                             const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  Session session;
  ARROW_ASSIGN_OR_RAISE(session.flight, arrow::flight::FlightClient::Connect(location));
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        session.flight->AuthenticateBasicToken({}, username, password));
  session.call_options.headers.push_back(bearer);
  return session;
}

// A session with a bearer token minted outside this server, for another instance.
arrow::Result<Session> WithForeignInstanceToken(int port) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  Session session;
  ARROW_ASSIGN_OR_RAISE(session.flight, arrow::flight::FlightClient::Connect(location));
  const auto token = jwt::create()
                         .set_issuer("gizmosql")
                         .set_type("JWT")
                         .set_id("test-foreign-instance-token")
                         .set_issued_at(std::chrono::system_clock::now())
                         .set_expires_at(std::chrono::system_clock::now() +
                                         std::chrono::hours{1})
                         .set_payload_claim("sub", jwt::claim(std::string("tester")))
                         .set_payload_claim("role", jwt::claim(std::string("admin")))
                         .set_payload_claim("auth_method",
                                            jwt::claim(std::string("BasicAuth")))
                         .set_payload_claim("instance_id",
                                            jwt::claim(std::string("some-other-instance")))
                         .set_payload_claim("session_id",
                                            jwt::claim(std::string("foreign-session-1")))
                         .sign(jwt::algorithm::hs256{kTestSecretKey});
  session.call_options.headers.emplace_back("authorization", "Bearer " + token);
  return session;
}

// Run one query on the session's token (a fresh FlightSqlClient each time, so
// only the bearer token carries the session).
arrow::Status RunQuery(Session& session, int port) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight, arrow::flight::FlightClient::Connect(location));
  FlightSqlClient sql_client(std::move(flight));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(session.call_options, "SELECT 1"));
  ARROW_ASSIGN_OR_RAISE(auto reader,
                        sql_client.DoGet(session.call_options, info->endpoints()[0].ticket));
  return reader->ToTable().status();
}

arrow::Status CloseSession(Session& session) {
  return session.flight
      ->CloseSession(session.call_options, arrow::flight::CloseSessionRequest{})
      .status();
}

}  // namespace

// ----------------------------------------------------------------------------
// --reject-unknown-sessions on
// ----------------------------------------------------------------------------
class RejectUnknownSessionsFixture
    : public gizmosql::testing::ServerTestFixture<RejectUnknownSessionsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31670,
        .health_port = 31671,
        .username = "tester",
        .password = "tester_password",
        // So a token minted for another instance gets past authentication and
        // reaches the session lookup (multi-replica setups run like this).
        .allow_cross_instance_tokens = true,
        .session_idle_timeout_seconds = 2,
        .reject_unknown_sessions = true,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RejectUnknownSessionsFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<RejectUnknownSessionsFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RejectUnknownSessionsFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RejectUnknownSessionsFixture>::config_{};

TEST_F(RejectUnknownSessionsFixture, NewSessionStartsOnFirstUse) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Login(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
}

TEST_F(RejectUnknownSessionsFixture, ClosedSessionIsRefused) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Login(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
  ASSERT_ARROW_OK(CloseSession(session));

  auto status = RunQuery(session, GetPort());
  ASSERT_FALSE(status.ok()) << "a closed session's token silently got a new session";
  EXPECT_NE(status.ToString().find("has ended on this GizmoSQL instance"), std::string::npos)
      << status.ToString();

  // Re-authenticating starts a new session normally.
  ASSERT_ARROW_OK_AND_ASSIGN(auto fresh, Login(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK(RunQuery(fresh, GetPort()));
}

TEST_F(RejectUnknownSessionsFixture, IdleEvictedSessionIsRefused) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Login(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
  // Idle timeout 2s, swept every second.
  std::this_thread::sleep_for(std::chrono::seconds(4));

  auto status = RunQuery(session, GetPort());
  ASSERT_FALSE(status.ok()) << "an idle-evicted session's token silently got a new session";
  EXPECT_NE(status.ToString().find("has ended on this GizmoSQL instance"), std::string::npos)
      << status.ToString();
}

TEST_F(RejectUnknownSessionsFixture, OtherInstancesSessionIsRefused) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, WithForeignInstanceToken(GetPort()));
  auto status = RunQuery(session, GetPort());
  ASSERT_FALSE(status.ok()) << "another instance's session silently got a new session here";
  EXPECT_NE(status.ToString().find("belongs to GizmoSQL instance some-other-instance"),
            std::string::npos)
      << status.ToString();
}

// ----------------------------------------------------------------------------
// Default (off): the historical behavior — the session is recreated
// ----------------------------------------------------------------------------
class UnknownSessionsDefaultFixture
    : public gizmosql::testing::ServerTestFixture<UnknownSessionsDefaultFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31672,
        .health_port = 31673,
        .username = "tester",
        .password = "tester_password",
        .allow_cross_instance_tokens = true,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<UnknownSessionsDefaultFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<UnknownSessionsDefaultFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<UnknownSessionsDefaultFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<UnknownSessionsDefaultFixture>::config_{};

TEST_F(UnknownSessionsDefaultFixture, ClosedSessionIsRecreatedByDefault) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Login(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
  ASSERT_ARROW_OK(CloseSession(session));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
}

TEST_F(UnknownSessionsDefaultFixture, OtherInstancesSessionIsCreatedByDefault) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, WithForeignInstanceToken(GetPort()));
  ASSERT_ARROW_OK(RunQuery(session, GetPort()));
}
