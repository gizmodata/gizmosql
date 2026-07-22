// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Tests for --max-metadata-size / GIZMOSQL_MAX_METADATA_SIZE.
//
// gRPC C++'s default GRPC_ARG_MAX_METADATA_SIZE is ~8 KB. Clients that send
// large per-call metadata (e.g. extra Apache Flight SQL JDBC URL parameters
// forwarded as gRPC headers, large bearer tokens, accumulated cookies, or
// proxy-injected trace headers) hit
//
//   Http2Exception$HeaderListSizeException: Header size exceeded max allowed size (8192)
//
// These tests verify both the CLI/library `max_metadata_size` argument AND
// its env var fallback (`GIZMOSQL_MAX_METADATA_SIZE`) actually raise the
// server's HTTP/2 SETTINGS_MAX_HEADER_LIST_SIZE.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

// Build a custom ASCII header value of the requested size. We avoid the
// `authorization` header (the server rewrites that with a Bearer token) and
// instead use a benign custom key. gRPC counts the *full* header list, so
// the per-header value just needs to push the list over the limit.
std::string MakeBigHeaderValue(size_t bytes) {
  return std::string(bytes, 'A');
}

// Send a SELECT 1 with `payload` set on a custom header `x-test-payload`.
// Returns the call status (OK if the server accepted the headers and replied
// successfully; non-OK if either the local gRPC stack or the server rejected
// the oversized header list).
arrow::Status TrySelectWithBigHeader(int port, const std::string& username,
                                     const std::string& password,
                                     const std::string& payload) {
  arrow::flight::FlightClientOptions options;
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client,
                        arrow::flight::FlightClient::Connect(location, options));

  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, username, password));

  arrow::flight::FlightCallOptions call_options;
  call_options.headers.push_back(bearer);
  call_options.headers.push_back({"x-test-payload", payload});

  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info,
                        sql_client.Execute(call_options, "SELECT 1 AS result"));
  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          sql_client.DoGet(call_options, endpoint.ticket));
    std::shared_ptr<arrow::Table> table;
    ARROW_ASSIGN_OR_RAISE(table, reader->ToTable());
    if (table->num_rows() != 1) {
      return arrow::Status::Invalid("expected one row, got ", table->num_rows());
    }
  }
  return arrow::Status::OK();
}

}  // namespace

// =============================================================================
// Fixture A: server with the gRPC default limit (~8 KB).
// =============================================================================

class MaxMetadataDefaultFixture
    : public gizmosql::testing::ServerTestFixture<MaxMetadataDefaultFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "max_metadata_default.db",
        .port = 31385,
        .health_port = 31386,
        .username = "testuser",
        .password = "testpassword",
        // max_metadata_size left at default 0 (= use gRPC default ~8 KB)
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MaxMetadataDefaultFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<MaxMetadataDefaultFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MaxMetadataDefaultFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MaxMetadataDefaultFixture>::config_{};

TEST_F(MaxMetadataDefaultFixture, SmallHeaderSucceeds) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  // ~1 KB header — well under the gRPC default of ~8 KB.
  auto status = TrySelectWithBigHeader(GetPort(), GetUsername(), GetPassword(),
                                       MakeBigHeaderValue(1024));
  ASSERT_TRUE(status.ok()) << "small header should be accepted: "
                            << status.ToString();
}

TEST_F(MaxMetadataDefaultFixture, OversizedHeaderRejected) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  // 40 KB header value — newer gRPC rejects sizes between the soft (~8 KB)
  // and hard (~16 KB) limits only *probabilistically* (grpc/grpc#31643), so
  // the header must exceed the hard limit to be deterministically rejected
  // somewhere in the gRPC HTTP/2 stack (client- or server-side).
  auto status = TrySelectWithBigHeader(GetPort(), GetUsername(), GetPassword(),
                                       MakeBigHeaderValue(40 * 1024));
  ASSERT_FALSE(status.ok())
      << "oversized header should be rejected at the gRPC default limit";
}

// =============================================================================
// Fixture B: server with explicit max_metadata_size = 64 KB (CLI/library arg).
// =============================================================================

class MaxMetadataRaisedFixture
    : public gizmosql::testing::ServerTestFixture<MaxMetadataRaisedFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "max_metadata_raised.db",
        .port = 31387,
        .health_port = 31388,
        .username = "testuser",
        .password = "testpassword",
        .max_metadata_size = 64 * 1024,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<MaxMetadataRaisedFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<MaxMetadataRaisedFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<MaxMetadataRaisedFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<MaxMetadataRaisedFixture>::config_{};

TEST_F(MaxMetadataRaisedFixture, OversizedHeaderAcceptedWhenLimitRaised) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  // 12 KB header — would be rejected at the default ~8 KB limit, but must
  // succeed when the server is configured with max_metadata_size = 64 KB.
  auto status = TrySelectWithBigHeader(GetPort(), GetUsername(), GetPassword(),
                                       MakeBigHeaderValue(12 * 1024));
  ASSERT_TRUE(status.ok())
      << "12 KB header should be accepted with max_metadata_size = 64 KB: "
      << status.ToString();
}

// =============================================================================
// Env var path: spin up a server in-process via RunFlightSQLServer with the
// arg left at 0 and GIZMOSQL_MAX_METADATA_SIZE set in the environment, then
// verify the same oversized header is accepted. This exercises the library
// env var fallback (the same code path container deployments rely on).
// =============================================================================

#ifndef _WIN32
namespace {

class EnvVarGuard {
 public:
  EnvVarGuard(const char* name, const char* value) : name_(name) {
    const char* prev = std::getenv(name);
    had_prev_ = prev != nullptr;
    if (had_prev_) prev_ = prev;
    setenv(name, value, /*overwrite=*/1);
  }
  ~EnvVarGuard() {
    if (had_prev_) {
      setenv(name_.c_str(), prev_.c_str(), /*overwrite=*/1);
    } else {
      unsetenv(name_.c_str());
    }
  }
  EnvVarGuard(const EnvVarGuard&) = delete;
  EnvVarGuard& operator=(const EnvVarGuard&) = delete;

 private:
  std::string name_;
  bool had_prev_;
  std::string prev_;
};

}  // namespace

TEST(MaxMetadataEnvVar, EnvVarRaisesLimit) {
  // Set the env var BEFORE creating the server so RunFlightSQLServer's
  // resolver picks it up.
  EnvVarGuard env_guard("GIZMOSQL_MAX_METADATA_SIZE", "65536");

  const int kPort = 31389;
  const int kHealthPort = 31390;
  const std::string kUser = "testuser";
  const std::string kPass = "testpassword";
  const std::string kDb = "max_metadata_envvar.db";
  std::error_code ec;
  std::filesystem::remove(kDb, ec);

  // Run RunFlightSQLServer on a background thread. We pass max_metadata_size = 0
  // so the env var fallback is the only thing that can raise the limit.
  std::thread server_thread([&]() {
    RunFlightSQLServer(
        BackendType::duckdb, std::filesystem::path(kDb),
        /*hostname=*/"localhost", kPort, kUser, kPass,
        /*secret_key=*/"test_secret_key_for_testing",
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
        /*log_level=*/"warn",
        /*log_format=*/"text",
        /*access_log=*/"off",
        /*log_file=*/"",
        /*query_timeout=*/0,
        /*query_log_level=*/"warn",
        /*auth_log_level=*/"warn",
        /*session_log_level=*/"warn",
        /*health_port=*/kHealthPort,
        /*health_check_query=*/"",
        /*enable_instrumentation=*/std::optional<bool>(false),
        /*instrumentation_db_path=*/"",
        /*instrumentation_catalog=*/"",
        /*instrumentation_schema=*/"",
        /*instance_tag=*/"",
        /*license_key_file=*/"", /*license_key=*/"",
        /*allow_cross_instance_tokens=*/std::optional<bool>(false),
        /*oauth_client_id=*/"",
        /*oauth_client_secret=*/"",
        /*oauth_scopes=*/"",
        /*oauth_port=*/0,
        /*oauth_base_url=*/"",
        /*oauth_redirect_uri=*/"",
        /*oauth_instance_id=*/"",
        /*oauth_disable_tls=*/std::optional<bool>(false),
        /*otel_enabled=*/std::optional<bool>(false),
        /*otel_exporter=*/"",
        /*otel_endpoint=*/"",
        /*otel_service_name=*/"",
        /*otel_headers=*/"",
        /*max_metadata_size=*/0);
  });

  // Wait for the server to come up. The fixture pattern uses a 100ms grace
  // after the `Serve()` call returns ready; we don't have that signal here,
  // so probe via repeated connect attempts up to a timeout.
  bool reachable = false;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    arrow::flight::FlightClientOptions opts;
    auto loc = arrow::flight::Location::ForGrpcTcp("localhost", kPort);
    if (loc.ok()) {
      auto cl = arrow::flight::FlightClient::Connect(*loc, opts);
      if (cl.ok()) {
        auto auth = (*cl)->AuthenticateBasicToken({}, kUser, kPass);
        if (auth.ok()) {
          reachable = true;
          break;
        }
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  if (!reachable) {
    // Tear down BEFORE asserting: ASSERT_TRUE returns from the test body,
    // and destroying the still-joinable server_thread would call
    // std::terminate and abort the entire test binary ("terminate called
    // without an active exception").
    ShutdownFlightServer();
    if (server_thread.joinable()) server_thread.join();
    gizmosql::CleanupServerResources();
  }
  ASSERT_TRUE(reachable) << "env-var server failed to come up";

  auto status = TrySelectWithBigHeader(kPort, kUser, kPass,
                                       MakeBigHeaderValue(12 * 1024));
  EXPECT_TRUE(status.ok())
      << "GIZMOSQL_MAX_METADATA_SIZE=65536 should permit 12 KB headers: "
      << status.ToString();

  // Tear down.
  ShutdownFlightServer();
  if (server_thread.joinable()) server_thread.join();
  gizmosql::CleanupServerResources();
  std::filesystem::remove(kDb, ec);
  std::filesystem::remove(kDb + ".wal", ec);
}
#endif  // !_WIN32
