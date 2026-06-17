// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Tests for --graceful-shutdown / GIZMOSQL_GRACEFUL_SHUTDOWN.
//
// When graceful shutdown is enabled, the first SIGINT/SIGTERM (or a programmatic
// RequestGracefulShutdown()) puts the server into a draining state instead of
// stopping immediately:
//
//   * already-running queries and their result fetches finish (or hit the
//     per-query timeout / the grace-period cap), and
//   * new sessions and new statements are rejected with an UNAVAILABLE error.
//
// These tests spin up a real server via RunFlightSQLServer() on a background
// thread (the same path container deployments use) and drive it with a Flight
// SQL client. Drain is triggered with RequestGracefulShutdown(), which is the
// in-process equivalent of delivering a SIGINT/SIGTERM.
//
// The "in-flight query completes" assertions need a query that actually blocks
// on the server for a while. We use DuckDB's sleep_ms(), which was added to
// core_functions in DuckDB v1.5.0 and is absent from the v1.4.x LTS line, so
// those assertions are skipped when sleep_ms() is unavailable (the rejection /
// teardown behavior is still exercised everywhere).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <optional>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "gizmosql_library.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr char kUser[] = "testuser";
constexpr char kPass[] = "testpassword";

// Run a SELECT end-to-end (auth + Execute + DoGet + ToTable) on a fresh
// connection. Returns the call status — OK if the server accepted and answered,
// non-OK if it rejected the work (e.g. UNAVAILABLE while draining).
arrow::Status RunSelect(int port, const std::string& sql) {
  arrow::flight::FlightClientOptions options;
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client,
                        arrow::flight::FlightClient::Connect(location, options));
  ARROW_ASSIGN_OR_RAISE(auto bearer, client->AuthenticateBasicToken({}, kUser, kPass));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(call_options, sql));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
  }
  return arrow::Status::OK();
}

// Launch RunFlightSQLServer() on a background thread with graceful shutdown
// enabled and the given grace period. All non-graceful parameters are left at
// their defaults.
std::thread LaunchGracefulServer(int port, int health_port, const std::string& db,
                                 int grace_period_seconds) {
  return std::thread([=]() {
    RunFlightSQLServer(
        BackendType::duckdb, std::filesystem::path(db), /*hostname=*/"localhost", port,
        kUser, kPass, /*secret_key=*/"test_secret_key_for_testing",
        /*tls_cert_path=*/std::filesystem::path(),
        /*tls_key_path=*/std::filesystem::path(),
        /*mtls_ca_cert_path=*/std::filesystem::path(),
        /*init_sql_commands=*/"", /*init_sql_commands_file=*/std::filesystem::path(),
        /*print_queries=*/false, /*read_only=*/false, /*token_allowed_issuer=*/"",
        /*token_allowed_audience=*/"",
        /*token_signature_verify_cert_path=*/std::filesystem::path(),
        /*token_jwks_uri=*/"", /*token_default_role=*/"", /*token_authorized_emails=*/"",
        /*log_level=*/"warn", /*log_format=*/"text", /*access_log=*/"off",
        /*log_file=*/"", /*query_timeout=*/0, /*query_log_level=*/"warn",
        /*auth_log_level=*/"warn", /*session_log_level=*/"warn",
        /*health_port=*/health_port, /*health_check_query=*/"",
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
        /*max_concurrent_statements=*/0, /*max_queued_statements=*/-1,
        /*max_queue_wait_seconds=*/-1, /*admin_bypass_queue_default=*/std::nullopt,
        /*memory_limit=*/"", /*capture_query_profile=*/"", /*cluster_id=*/"",
        /*enable_catalog_logging=*/std::optional<bool>(false), /*log_catalog=*/"",
        /*log_schema=*/"", /*log_catalog_db_path=*/"",
        /*graceful_shutdown=*/std::optional<bool>(true),
        /*shutdown_grace_period_seconds=*/grace_period_seconds);
  });
}

// Probe the server until it accepts an authenticated connection, or time out.
bool WaitReachable(int port, std::chrono::seconds timeout = std::chrono::seconds(30)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    auto loc = arrow::flight::Location::ForGrpcTcp("localhost", port);
    if (loc.ok()) {
      arrow::flight::FlightClientOptions opts;
      auto cl = arrow::flight::FlightClient::Connect(*loc, opts);
      if (cl.ok() && (*cl)->AuthenticateBasicToken({}, kUser, kPass).ok()) return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  return false;
}

// True if DuckDB's sleep_ms() is available and actually blocks on this build.
bool SleepMsUsable(int port) {
  const auto t0 = std::chrono::steady_clock::now();
  const auto st = RunSelect(port, "SELECT sleep_ms(200)");
  const auto slept = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - t0)
                         .count();
  return st.ok() && slept >= 150;
}

void RemoveDb(const std::string& db) {
  std::error_code ec;
  std::filesystem::remove(db, ec);
  std::filesystem::remove(db + ".wal", ec);
}

}  // namespace

#ifndef _WIN32

// Join the server thread and clean up. Safe to call on every exit path (the
// background RunFlightSQLServer thread must be joined before its std::thread is
// destroyed, or the process std::terminate()s).
void StopServer(std::thread& server, const std::string& db) {
  if (server.joinable()) {
    RequestGracefulShutdown();  // ensure the server stops if it hasn't already
    server.join();
  }
  gizmosql::CleanupServerResources();
  RemoveDb(db);
}

// In-flight query finishes while new work is rejected, and the server then stops
// on its own once the query drains.
TEST(GracefulShutdown, DrainsInFlightAndRejectsNewWork) {
  const int kPort = 31410;
  const int kHealthPort = 31411;
  const std::string kDb = "graceful_shutdown_drain.db";
  RemoveDb(kDb);

  std::thread server = LaunchGracefulServer(kPort, kHealthPort, kDb, /*grace=*/30);
  const bool reachable = WaitReachable(kPort);
  if (!reachable) {
    StopServer(server, kDb);
    FAIL() << "graceful-shutdown server failed to come up";
  }

  const bool sleep_ms_usable = SleepMsUsable(kPort);
  if (!sleep_ms_usable) {
    // Can't construct a reliably long-running query on this DuckDB build.
    StopServer(server, kDb);
    GTEST_SKIP() << "DuckDB sleep_ms() unavailable (v1.4.x LTS) — skipping drain timing";
  }

  // Start a long-running query in the background. It holds an in-flight slot for
  // ~5s; the drain must let it finish.
  std::atomic<bool> slow_done{false};
  arrow::Status slow_status;
  std::thread slow_query([&]() {
    slow_status = RunSelect(kPort, "SELECT sleep_ms(5000) AS slept");
    slow_done.store(true);
  });

  // Give the slow query time to reach the server and start executing.
  std::this_thread::sleep_for(std::chrono::milliseconds(800));
  const bool slow_running_at_drain = !slow_done.load();

  // Begin the graceful drain (equivalent to one SIGINT/SIGTERM).
  RequestGracefulShutdown();

  // The drain takes effect asynchronously (the watcher flips the flag within
  // ~100ms). Poll a fresh statement until it is rejected, while the in-flight
  // query is still running.
  arrow::Status rejected = arrow::Status::OK();
  const auto reject_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < reject_deadline) {
    rejected = RunSelect(kPort, "SELECT 1 AS x");
    if (!rejected.ok()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }

  // The in-flight query must complete successfully (not be interrupted).
  slow_query.join();

  // Server stops on its own once the query drained.
  ASSERT_TRUE(server.joinable());
  server.join();
  gizmosql::CleanupServerResources();
  RemoveDb(kDb);

  // Assertions after teardown so a failure never leaves a joinable thread.
  EXPECT_TRUE(slow_running_at_drain) << "slow query finished before drain began";
  EXPECT_FALSE(rejected.ok())
      << "a new statement during drain should be rejected, but it succeeded";
  EXPECT_NE(rejected.ToString().find("shutting down"), std::string::npos)
      << "rejection should mention the instance is shutting down; got: "
      << rejected.ToString();
  EXPECT_TRUE(slow_status.ok())
      << "in-flight query should finish during drain; got: " << slow_status.ToString();
}

// With no in-flight work, a graceful drain stops the server promptly.
TEST(GracefulShutdown, StopsPromptlyWhenIdle) {
  const int kPort = 31412;
  const int kHealthPort = 31413;
  const std::string kDb = "graceful_shutdown_idle.db";
  RemoveDb(kDb);

  std::thread server = LaunchGracefulServer(kPort, kHealthPort, kDb, /*grace=*/30);
  const bool reachable = WaitReachable(kPort);
  if (!reachable) {
    StopServer(server, kDb);
    FAIL() << "graceful-shutdown server failed to come up";
  }

  // A normal query works before the drain.
  const bool pre_drain_ok = RunSelect(kPort, "SELECT 1 AS x").ok();

  const auto t0 = std::chrono::steady_clock::now();
  RequestGracefulShutdown();
  ASSERT_TRUE(server.joinable());
  server.join();
  const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::steady_clock::now() - t0)
                           .count();
  gizmosql::CleanupServerResources();
  RemoveDb(kDb);

  EXPECT_TRUE(pre_drain_ok) << "a normal query should succeed before the drain";
  EXPECT_LT(elapsed, 10) << "idle drain should stop well within the grace period";
}

#endif  // !_WIN32
