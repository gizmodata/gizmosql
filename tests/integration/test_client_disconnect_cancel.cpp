// =============================================================================
// Tests: a client that goes away mid-query has its statement interrupted.
//
// The execute-wait loop in DuckDBStatement::Execute() polls the Flight call's
// is_cancelled() and interrupts DuckDB when gRPC reports the peer gone. These
// tests drive the two ways a client can vanish that can be reproduced in
// process — cancelling the DoGet stream, and letting a client deadline expire
// — and use the *session* as the oracle: a session's DuckDB connection runs
// one statement at a time, so a follow-up `SELECT 1` on the same session only
// returns promptly if the long query was actually interrupted.
// =============================================================================

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <thread>

#include <arrow/api.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/client.h>

#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::FlightCallOptions;
using arrow::flight::sql::FlightSqlClient;

namespace {

// CPU-bound for far longer than any test timeout: the only way the session
// frees up promptly is a server-side interrupt.
constexpr const char* kLongQuery =
    "SELECT sum(a.range * b.range) FROM range(100000000) a, range(100000) b";

// How long a follow-up statement on the same session may take before we
// conclude the long query is still running.
constexpr auto kSessionFreeWithin = std::chrono::seconds(30);

struct SqlSession {
  std::unique_ptr<FlightSqlClient> client;
  FlightCallOptions call_options;  // carries the session's bearer token
};

arrow::Result<SqlSession> Connect(int port, const std::string& username,
                                  const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(
                            location, arrow::flight::FlightClientOptions()));
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        flight_client->AuthenticateBasicToken({}, username, password));
  SqlSession session;
  session.call_options.headers.push_back(bearer);
  session.client = std::make_unique<FlightSqlClient>(std::move(flight_client));
  return session;
}

// Same session (same bearer token), fresh gRPC connection: what a client
// that reconnects after dropping a stream looks like to the server.
arrow::Result<SqlSession> Reconnect(int port, const SqlSession& existing) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                        arrow::flight::FlightClient::Connect(
                            location, arrow::flight::FlightClientOptions()));
  SqlSession session;
  session.call_options = existing.call_options;
  session.client = std::make_unique<FlightSqlClient>(std::move(flight_client));
  return session;
}

// Runs `SELECT 1` on the session from a helper thread and reports whether it
// completed within `budget`. A statement still executing on the session's
// DuckDB connection blocks this for hours, which is exactly the regression.
bool SessionFreesWithin(SqlSession& session, std::chrono::seconds budget) {
  auto fut = std::async(std::launch::async, [&]() -> arrow::Status {
    ARROW_ASSIGN_OR_RAISE(auto info, session.client->Execute(session.call_options, "SELECT 1"));
    for (const auto& endpoint : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader,
                            session.client->DoGet(session.call_options, endpoint.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      if (table->num_rows() != 1) return arrow::Status::Invalid("expected one row");
    }
    return arrow::Status::OK();
  });
  if (fut.wait_for(budget) != std::future_status::ready) {
    return false;  // the future is leaked deliberately; the test is failing anyway
  }
  auto st = fut.get();
  EXPECT_TRUE(st.ok()) << st.ToString();
  return st.ok();
}

}  // namespace

class ClientDisconnectFixture
    : public gizmosql::testing::ServerTestFixture<ClientDisconnectFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "client_disconnect_test.db",
        .port = 31630,
        .health_port = 31631,
        .username = "testuser",
        .password = "testpassword",
        .print_queries = true,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<ClientDisconnectFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<ClientDisconnectFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<ClientDisconnectFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<ClientDisconnectFixture>::config_{};

// The client cancels its DoGet stream mid-execution (what a driver does when
// its process is interrupted, or what gRPC does when the socket drops).
TEST_F(ClientDisconnectFixture, CancelledDoGetInterruptsRunningQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Connect(GetPort(), GetUsername(), GetPassword()));

  ASSERT_ARROW_OK_AND_ASSIGN(auto info, session.client->Execute(session.call_options, kLongQuery));
  ASSERT_EQ(info->endpoints().size(), 1u);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto reader, session.client->DoGet(session.call_options, info->endpoints()[0].ticket));

  // Next() blocks while the server executes; it returns once the call is cancelled.
  std::thread consumer([&reader]() { (void)reader->Next(); });
  std::this_thread::sleep_for(std::chrono::seconds(1));
  reader->Cancel();
  consumer.join();

  ASSERT_ARROW_OK_AND_ASSIGN(auto again, Reconnect(GetPort(), session));
  EXPECT_TRUE(SessionFreesWithin(again, kSessionFreeWithin))
      << "the session's long query was not interrupted after the client cancelled its stream";
}

// The client's own deadline expires: gRPC cancels the call on the server side
// without any explicit action from the client.
TEST_F(ClientDisconnectFixture, ClientDeadlineInterruptsRunningQuery) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Connect(GetPort(), GetUsername(), GetPassword()));

  ASSERT_ARROW_OK_AND_ASSIGN(auto info, session.client->Execute(session.call_options, kLongQuery));
  ASSERT_EQ(info->endpoints().size(), 1u);

  FlightCallOptions short_deadline = session.call_options;
  short_deadline.timeout = arrow::flight::TimeoutDuration(1.0);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto reader, session.client->DoGet(short_deadline, info->endpoints()[0].ticket));
  auto next = reader->Next();
  EXPECT_FALSE(next.ok()) << "expected the 1s client deadline to fail the DoGet";

  ASSERT_ARROW_OK_AND_ASSIGN(auto again, Reconnect(GetPort(), session));
  EXPECT_TRUE(SessionFreesWithin(again, kSessionFreeWithin))
      << "the session's long query was not interrupted after the client's deadline expired";
}

// Control: a query the client actually consumes is not affected by the polling.
TEST_F(ClientDisconnectFixture, ConsumedQueryCompletesNormally) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto session, Connect(GetPort(), GetUsername(), GetPassword()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, session.client->Execute(session.call_options,
                                         "SELECT count(*) AS n FROM range(1000000)"));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto reader, session.client->DoGet(session.call_options, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
  auto n = std::static_pointer_cast<arrow::Int64Array>(table->column(0)->chunk(0));
  EXPECT_EQ(n->Value(0), 1000000);
}
