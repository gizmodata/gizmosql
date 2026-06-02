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

// Enterprise integration tests for the statement-queue feature, exercised through
// a real Flight SQL client. The concurrency mechanism itself is unit-tested in
// test_admission_controller.cpp; these cover the end-to-end wiring: the
// SET gizmosql.bypass_queue handler (license + admin gating + value parsing) and
// that queries still execute when routed through the admission gate.
//
// Requires an enterprise license with the "statement_queue" feature (set
// GIZMOSQL_LICENSE_KEY_FILE); otherwise the tests skip.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;

static bool IsEnterpriseLicenseAvailable() {
  const char* license_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  return license_file != nullptr && license_file[0] != '\0';
}

#define SKIP_IF_NO_LICENSE()                                                    \
  if (!IsEnterpriseLicenseAvailable()) {                                        \
    GTEST_SKIP() << "Enterprise license required for statement-queue tests. "   \
                 << "Set GIZMOSQL_LICENSE_KEY_FILE environment variable.";      \
  }

class StatementQueueServerFixture
    : public gizmosql::testing::ServerTestFixture<StatementQueueServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "statement_queue_tester.db",
        .port = 31360,
        .health_port = 31361,
        .username = "admin",
        .password = "admin",
        // Enable the queue so the admission gate engages for non-bypassed sessions.
        .max_concurrent_statements = 2,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<StatementQueueServerFixture>::config_{};

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

// Execute a statement and fully drain it (GetFlightInfo -> DoGet -> ToTable). For
// SET commands this forces HandleGizmoSQLSet() to run, so its success/error
// surfaces here rather than being deferred.
arrow::Status RunStatement(AdminClient& ac, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, ac.sql_client->Execute(ac.call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, ac.sql_client->DoGet(ac.call_options, endpoint.ticket));
    ARROW_RETURN_NOT_OK(reader->ToTable());
  }
  return arrow::Status::OK();
}

}  // namespace

// An admin session may toggle bypass_queue on and off.
TEST_F(StatementQueueServerFixture, AdminCanToggleBypassQueue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = true"));
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = false"));
  // Case/format tolerance handled by the parser.
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = 1"));
}

// A non-boolean value is rejected.
TEST_F(StatementQueueServerFixture, BypassQueueRejectsInvalidValue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  auto status = RunStatement(ac, "SET SESSION gizmosql.bypass_queue = 'maybe'");
  ASSERT_FALSE(status.ok());
  EXPECT_NE(status.ToString().find("bypass_queue"), std::string::npos)
      << "Expected an 'Invalid value for bypass_queue' error, got: " << status.ToString();
}

// With the queue enabled (limit=2) and the admin session opted IN to the queue
// (bypass disabled), queries still execute correctly through the admission gate.
TEST_F(StatementQueueServerFixture, QueriesExecuteThroughTheQueue) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  // Opt this admin session into the queue so SELECTs traverse Acquire()/release().
  ASSERT_OK(RunStatement(ac, "SET SESSION gizmosql.bypass_queue = false"));

  for (int i = 0; i < 5; ++i) {
    ASSERT_OK(RunStatement(ac, "SELECT 42")) << "query " << i << " failed";
  }
}

// gizmosql_settings() is a composable, bind-parameterized table function: it can be
// filtered and ordered like any relation. (Not license-gated — it's a SQL rewrite.)
TEST_F(StatementQueueServerFixture, GizmoSqlSettingsIsComposable) {
  ASSERT_TRUE(IsServerReady());
  ASSERT_ARROW_OK_AND_ASSIGN(auto ac, ConnectAdmin(GetPort(), GetUsername(), GetPassword()));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      ac.sql_client->Execute(
          ac.call_options,
          "SELECT name, scope, enterprise FROM gizmosql_settings() "
          "WHERE name LIKE 'gizmosql.max%' ORDER BY name"));
  std::shared_ptr<arrow::Table> table;
  for (const auto& endpoint : info->endpoints()) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               ac.sql_client->DoGet(ac.call_options, endpoint.ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(table, reader->ToTable());
  }
  ASSERT_NE(table, nullptr);
  // The three gizmosql.max* settings: max_concurrent_statements, max_queue_wait,
  // max_queued_statements.
  EXPECT_EQ(table->num_rows(), 3);
  EXPECT_EQ(table->num_columns(), 3);
}

namespace {
// Occupy an execution slot for `ms` milliseconds (admin session opted INTO the
// queue) using DuckDB's sleep_ms(). Best-effort: errors are ignored.
void HoldSlot(int port, std::string user, std::string password, int ms) {
  auto ac = ConnectAdmin(port, user, password);
  if (!ac.ok()) return;
  if (!RunStatement(*ac, "SET SESSION gizmosql.bypass_queue = false").ok()) return;
  RunStatement(*ac, "SELECT sleep_ms(" + std::to_string(ms) + ")");
}

// True if DuckDB's sleep_ms() is available AND actually blocks on this build.
// sleep_ms() was added to core_functions in DuckDB v1.5.0 (commit 8e6261070,
// 2025-11-11) and is absent from the entire v1.4.x LTS line, so HoldSlot-based
// contention can't be set up there. Contention tests skip when this is false; the
// AdmissionController unit tests cover the mechanism deterministically everywhere.
bool SleepMsUsable(int port, const std::string& user, const std::string& password) {
  auto ac = ConnectAdmin(port, user, password);
  if (!ac.ok()) return false;
  const auto t0 = std::chrono::steady_clock::now();
  const auto st = RunStatement(*ac, "SELECT sleep_ms(200)");
  const auto slept = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - t0)
                         .count();
  return st.ok() && slept >= 150;
}

// Poll `predicate` until true or the timeout elapses; returns the final value.
template <typename Predicate>
bool WaitFor(Predicate predicate,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  return predicate();
}
}  // namespace

// With both execution slots (limit = 2) held by sleep_ms() statements, a third
// statement must wait for a slot — proving the cap actually serializes execution.
TEST_F(StatementQueueServerFixture, QueueSerializesConcurrentStatements) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());

  // sleep_ms() (used to deterministically hold slots) isn't available on every
  // DuckDB channel — notably the v1.4.x LTS line. Skip where it can't hold a slot;
  // the AdmissionController unit tests cover serialization on every build.
  if (!SleepMsUsable(GetPort(), GetUsername(), GetPassword())) {
    GTEST_SKIP() << "sleep_ms() unavailable/non-blocking on this DuckDB build "
                    "(e.g. the LTS channel); AdmissionController unit tests cover "
                    "serialization.";
  }

  std::thread h1(HoldSlot, GetPort(), GetUsername(), GetPassword(), 2000);
  std::thread h2(HoldSlot, GetPort(), GetUsername(), GetPassword(), 2000);
  // Let the holders occupy both slots before the probe is submitted.
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_ARROW_OK_AND_ASSIGN(auto probe,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));
  ASSERT_OK(RunStatement(probe, "SET SESSION gizmosql.bypass_queue = false"));
  const auto start = std::chrono::steady_clock::now();
  ASSERT_OK(RunStatement(probe, "SELECT 7"));
  const auto waited = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start)
                          .count();

  h1.join();
  h2.join();

  // The probe couldn't start until a holder finished (~2s in), so submitted at
  // ~0.5s it must have waited ~1.5s. A generous floor avoids CI flakiness.
  EXPECT_GE(waited, 800) << "probe should have queued for a slot; waited " << waited << "ms";
}

// A statement forced to queue records its queued phase in instrumentation: it
// reaches the terminal 'success' state with a *real* (non-trivial) queue_wait_ms,
// proving the queued->executing->success lifecycle and the measured wait — the
// backbone of the SQL-monitor.
TEST_F(StatementQueueServerFixture, QueuedExecutionRecordsEnqueueTime) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  if (!SleepMsUsable(GetPort(), GetUsername(), GetPassword())) {
    GTEST_SKIP() << "sleep_ms() unavailable/non-blocking on this DuckDB build "
                    "(e.g. the LTS channel); real contention is needed to measure "
                    "the queue wait.";
  }

  // Hold both slots so the probe must wait ~1.5s for one to free.
  std::thread h1(HoldSlot, GetPort(), GetUsername(), GetPassword(), 2000);
  std::thread h2(HoldSlot, GetPort(), GetUsername(), GetPassword(), 2000);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  ASSERT_ARROW_OK_AND_ASSIGN(auto probe,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));
  ASSERT_OK(RunStatement(probe, "SET SESSION gizmosql.bypass_queue = false"));
  ASSERT_OK(RunStatement(probe, "SELECT 4242 AS qprobe"));
  h1.join();
  h2.join();

  // Poll instrumentation (async writer) for the probe's *completed* execution row,
  // then assert the recorded queue wait reflects the real ~1.5s it spent queued
  // (not the ~0ms a non-contended statement would show). The status='success'
  // filter confirms it advanced through the full queued->executing->success cycle.
  int64_t queue_wait_ms = -1;
  for (int attempt = 0; attempt < 25 && queue_wait_ms < 0; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto info = probe.sql_client->Execute(
        probe.call_options,
        "SELECT queue_wait_ms FROM _gizmosql_instr.execution_details "
        "WHERE sql_text LIKE '%qprobe%' AND status = 'success' "
        "AND queue_wait_ms IS NOT NULL ORDER BY enqueue_time DESC LIMIT 1");
    if (!info.ok()) continue;
    for (const auto& endpoint : (*info)->endpoints()) {
      auto reader = probe.sql_client->DoGet(probe.call_options, endpoint.ticket);
      if (!reader.ok()) continue;
      auto t = (*reader)->ToTable();
      if (!t.ok() || (*t)->num_rows() == 0) continue;
      queue_wait_ms = std::static_pointer_cast<arrow::Int64Array>(
                          (*t)->column(0)->chunk(0))
                          ->Value(0);
    }
  }

  ASSERT_GE(queue_wait_ms, 0)
      << "queued probe's completed execution row was never recorded";
  EXPECT_GE(queue_wait_ms, 500) << "queued probe should record a real queue wait; got "
                                << queue_wait_ms << "ms";
}

// A statement killed while it is queued is cancelled promptly — it abandons the
// queue without waiting for a slot, and is recorded with status='cancelled' (never
// runs to a misleading 'success'). Exercises queue + KILL SESSION + instrumentation
// together.
TEST_F(StatementQueueServerFixture, KilledWhileQueuedRecordsCancelled) {
  SKIP_IF_NO_LICENSE();
  ASSERT_TRUE(IsServerReady());
  if (!SleepMsUsable(GetPort(), GetUsername(), GetPassword())) {
    GTEST_SKIP() << "sleep_ms() unavailable/non-blocking on this DuckDB build "
                    "(e.g. the LTS channel); can't hold slots to force queuing.";
  }

  // Hold both slots for 5s so the victim is forced to queue with plenty of margin.
  std::thread h1(HoldSlot, GetPort(), GetUsername(), GetPassword(), 5000);
  std::thread h2(HoldSlot, GetPort(), GetUsername(), GetPassword(), 5000);
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Victim: grab its session id (admin bypasses the queue), opt into the queue,
  // then submit a statement that must wait behind the holders.
  std::atomic<bool> victim_ready{false};
  std::atomic<bool> victim_done{false};
  std::atomic<bool> victim_failed{false};
  std::string victim_session_id;
  std::thread victim([&] {
    auto ac = ConnectAdmin(GetPort(), GetUsername(), GetPassword());
    if (!ac.ok()) {
      victim_done.store(true);
      return;
    }
    auto info =
        ac->sql_client->Execute(ac->call_options, "SELECT GIZMOSQL_CURRENT_SESSION()");
    if (info.ok()) {
      for (const auto& ep : (*info)->endpoints()) {
        auto reader = ac->sql_client->DoGet(ac->call_options, ep.ticket);
        if (!reader.ok()) continue;
        auto t = (*reader)->ToTable();
        if (t.ok() && (*t)->num_rows() > 0) {
          victim_session_id = std::static_pointer_cast<arrow::StringArray>(
                                  (*t)->column(0)->chunk(0))
                                  ->GetString(0);
        }
      }
    }
    if (!RunStatement(*ac, "SET SESSION gizmosql.bypass_queue = false").ok()) {
      victim_done.store(true);
      return;
    }
    victim_ready.store(true);
    auto st = RunStatement(*ac, "SELECT 1 AS killvictim");  // must queue, then be killed
    victim_failed.store(!st.ok());
    victim_done.store(true);
  });

  ASSERT_TRUE(WaitFor([&] { return victim_ready.load(); }));
  ASSERT_FALSE(victim_session_id.empty()) << "could not obtain victim session id";
  std::this_thread::sleep_for(std::chrono::milliseconds(500));  // let it enter the queue

  // Admin kills the victim from a separate session.
  ASSERT_ARROW_OK_AND_ASSIGN(auto killer,
                             ConnectAdmin(GetPort(), GetUsername(), GetPassword()));
  ASSERT_OK(RunStatement(killer, "KILL SESSION '" + victim_session_id + "'"));

  // It must abort promptly — well within the 5s the holders keep their slots. If
  // the kill didn't interrupt the wait, the victim would only return ~5s later when
  // a holder frees (and would run to 'success'); a 2.5s bound proves promptness.
  EXPECT_TRUE(WaitFor([&] { return victim_done.load(); }, std::chrono::milliseconds(2500)))
      << "a killed queued statement should abort promptly, not wait for a slot";
  EXPECT_TRUE(victim_failed.load()) << "killed queued statement should return an error";

  h1.join();
  h2.join();
  victim.join();

  // Instrumentation should record the killed queued statement as 'cancelled'.
  bool cancelled_recorded = false;
  for (int attempt = 0; attempt < 25 && !cancelled_recorded; ++attempt) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    auto info = killer.sql_client->Execute(
        killer.call_options,
        "SELECT count(*) FROM _gizmosql_instr.execution_details "
        "WHERE sql_text LIKE '%killvictim%' AND status = 'cancelled'");
    if (!info.ok()) continue;
    for (const auto& ep : (*info)->endpoints()) {
      auto reader = killer.sql_client->DoGet(killer.call_options, ep.ticket);
      if (!reader.ok()) continue;
      auto t = (*reader)->ToTable();
      if (!t.ok() || (*t)->num_rows() == 0) continue;
      if (std::static_pointer_cast<arrow::Int64Array>((*t)->column(0)->chunk(0))
              ->Value(0) >= 1)
        cancelled_recorded = true;
    }
  }
  EXPECT_TRUE(cancelled_recorded)
      << "a statement killed while queued should be recorded as 'cancelled'";
}
