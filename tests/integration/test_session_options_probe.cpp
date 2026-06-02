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

// Regression tests for the GetSessionOptions liveness-probe behaviour.
//
// GizmoSQL sessions are created lazily: whichever Flight SQL RPC arrives first
// for a given JWT session_id creates the session. The catalog is only applied
// when the client sends SetSessionOptions (USE "<catalog>"), which the JDBC
// driver does exactly once, right after the Handshake. If a pooled connection's
// server-side session disappears (idle eviction, or the connection re-routing to
// a replica that never saw the session), the next data RPC would lazily create a
// *new* session in the default ("memory") catalog and every query would fail
// with a catalog error.
//
// To let clients detect this before issuing a query, GetSessionOptions performs
// a NON-creating lookup: an evicted session returns a Flight error instead of
// silently materialising a fresh session. A client's Connection.isValid() can
// call GetSessionOptions and recycle the connection on error, forcing a fresh
// Handshake + SetSessionOptions that lands the replacement session in the right
// catalog.

#include <gtest/gtest.h>

#include "arrow/flight/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// Run a statement and fully consume its result (so it actually executes and, as
// a side effect, lazily creates the server-side session via GetClientSession).
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

}  // namespace

class SessionOptionsProbeFixture
    : public gizmosql::testing::ServerTestFixture<SessionOptionsProbeFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "session_options_probe_test.db",
        .port = 31389,
        .health_port = 31390,
        .username = "tester",
        .password = "tester",
    };
  }

 protected:
  // Connect a raw FlightClient and authenticate, returning the wrapped
  // FlightSqlClient plus call options carrying the issued Bearer token. The
  // Bearer token embeds a fresh session_id, so each call establishes a distinct
  // server-side session identity.
  struct AuthedClient {
    std::unique_ptr<FlightSqlClient> sql_client;
    arrow::flight::FlightCallOptions call_options;
  };

  arrow::Result<AuthedClient> ConnectAndAuthenticate() {
    arrow::flight::FlightClientOptions options;
    ARROW_ASSIGN_OR_RAISE(auto location,
                          arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ARROW_ASSIGN_OR_RAISE(auto client,
                          arrow::flight::FlightClient::Connect(location, options));

    arrow::flight::FlightCallOptions call_options;
    ARROW_ASSIGN_OR_RAISE(auto bearer,
                          client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options.headers.push_back(bearer);

    AuthedClient result;
    result.sql_client = std::make_unique<FlightSqlClient>(std::move(client));
    result.call_options = std::move(call_options);
    return result;
  }
};

// Static member definitions required by the CRTP fixture template.
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SessionOptionsProbeFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<SessionOptionsProbeFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SessionOptionsProbeFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SessionOptionsProbeFixture>::config_{};

// A live session answers GetSessionOptions with its current catalog/schema.
TEST_F(SessionOptionsProbeFixture, GetSessionOptionsSucceedsForLiveSession) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  ASSERT_ARROW_OK_AND_ASSIGN(auto authed, ConnectAndAuthenticate());

  // A data RPC lazily creates the server-side session.
  ASSERT_ARROW_OK(ExecuteAndConsume(*authed.sql_client, authed.call_options, "SELECT 1"));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto opts, authed.sql_client->GetSessionOptions(authed.call_options,
                                                      arrow::flight::GetSessionOptionsRequest{}));
  EXPECT_TRUE(opts.session_options.count("catalog") > 0)
      << "GetSessionOptions should report the live session's catalog";
}

// The core regression: once a session has been evicted, GetSessionOptions must
// surface an error rather than silently recreating the session (which would
// previously have started in the wrong, default "memory" catalog).
TEST_F(SessionOptionsProbeFixture, GetSessionOptionsFailsForEvictedSession) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  ASSERT_ARROW_OK_AND_ASSIGN(auto authed, ConnectAndAuthenticate());

  // Establish the session, then confirm the probe sees it.
  ASSERT_ARROW_OK(ExecuteAndConsume(*authed.sql_client, authed.call_options, "SELECT 1"));
  ASSERT_ARROW_OK(authed.sql_client
                      ->GetSessionOptions(authed.call_options,
                                          arrow::flight::GetSessionOptionsRequest{})
                      .status());

  // Evict the session server-side (CloseSession removes it from the session map
  // without marking the token as killed, mirroring an idle/eviction scenario
  // where the same Bearer token is later reused).
  ASSERT_ARROW_OK(authed.sql_client
                      ->CloseSession(authed.call_options, arrow::flight::CloseSessionRequest{})
                      .status());

  // Re-probing with the same (now-stale) Bearer token must NOT lazily recreate a
  // session — it must report the session as gone.
  auto reprobe = authed.sql_client->GetSessionOptions(
      authed.call_options, arrow::flight::GetSessionOptionsRequest{});
  ASSERT_FALSE(reprobe.ok())
      << "GetSessionOptions must fail for an evicted session instead of lazily "
         "recreating it in the default catalog";
}

// After eviction, a brand-new connection (fresh Handshake => fresh session_id)
// probes healthy again — demonstrating the recycle path a connection pool would
// take when isValid() returns false.
TEST_F(SessionOptionsProbeFixture, FreshConnectionProbesHealthyAfterEviction) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  ASSERT_ARROW_OK_AND_ASSIGN(auto stale, ConnectAndAuthenticate());
  ASSERT_ARROW_OK(ExecuteAndConsume(*stale.sql_client, stale.call_options, "SELECT 1"));
  ASSERT_ARROW_OK(stale.sql_client
                      ->CloseSession(stale.call_options, arrow::flight::CloseSessionRequest{})
                      .status());
  ASSERT_FALSE(stale.sql_client
                   ->GetSessionOptions(stale.call_options,
                                       arrow::flight::GetSessionOptionsRequest{})
                   .ok());

  // A fresh authenticated connection establishes a new session and probes OK.
  ASSERT_ARROW_OK_AND_ASSIGN(auto fresh, ConnectAndAuthenticate());
  ASSERT_ARROW_OK(ExecuteAndConsume(*fresh.sql_client, fresh.call_options, "SELECT 1"));
  ASSERT_ARROW_OK(fresh.sql_client
                      ->GetSessionOptions(fresh.call_options,
                                          arrow::flight::GetSessionOptionsRequest{})
                      .status());
}
