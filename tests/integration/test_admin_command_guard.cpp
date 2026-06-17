// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unit tests for the rudimentary admin-command gate (Core) — the parser-based
// classifier that gates dangerous filesystem/engine commands for non-admin
// sessions ahead of the full RBAC model (GizmoSQL 2.0).
//
// These exercise gizmosql::ddb::ClassifyGatedCommand() directly (no server), so
// they cover the detection logic exhaustively: every gated category, admin-style
// negatives, remote-path allowances, replacement scans, deep nesting, and
// case/comment/whitespace evasion.

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <string>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

#include "admin_command_guard.h"

using arrow::flight::sql::FlightSqlClient;
using gizmosql::ddb::CheckNonAdminCommandAllowed;
using gizmosql::ddb::ClassifyGatedCommand;

namespace {

// True if `sql` is gated (would be rejected for a non-admin).
bool Gated(const std::string& sql) {
  return ClassifyGatedCommand(sql).has_value();
}

std::string Category(const std::string& sql) {
  return ClassifyGatedCommand(sql).value_or("");
}

}  // namespace

// =============================================================================
// ATTACH / DETACH — always gated, exact (statement-type based).
// =============================================================================

TEST(AdminCommandGuard, AttachIsGated) {
  EXPECT_TRUE(Gated("ATTACH 'lake.db' AS lake"));
  EXPECT_TRUE(Gated("ATTACH DATABASE 'lake.db' AS lake"));
  EXPECT_TRUE(Gated("ATTACH 'ducklake:metadata.db' AS my_lake (DATA_PATH 'data/')"));
  EXPECT_TRUE(Gated("ATTACH 'postgres://user@host/db' AS pg (TYPE postgres)"));
  EXPECT_EQ(Category("ATTACH 'x.db' AS x"), "ATTACH");
}

TEST(AdminCommandGuard, DetachIsGated) {
  EXPECT_TRUE(Gated("DETACH lake"));
  EXPECT_TRUE(Gated("DETACH DATABASE lake"));
  // The exact thing the customer worried about: detaching a user DuckLake catalog.
  EXPECT_TRUE(Gated("detach my_ducklake_catalog"));
  EXPECT_EQ(Category("DETACH lake"), "DETACH");
}

TEST(AdminCommandGuard, AttachDetachEvasionStillCaught) {
  EXPECT_TRUE(Gated("  \n\t ATTACH 'x.db' AS x"));
  EXPECT_TRUE(Gated("aTtAcH 'x.db' AS x"));
  EXPECT_TRUE(Gated("/* sneaky */ DETACH lake"));
  EXPECT_TRUE(Gated("-- comment\nDETACH lake"));
  // Multi-statement: one gated statement gates the batch.
  EXPECT_TRUE(Gated("SELECT 1; ATTACH 'x.db' AS x"));
}

// =============================================================================
// SET GLOBAL / RESET GLOBAL, and dangerous bare SET.
// =============================================================================

TEST(AdminCommandGuard, SetGlobalIsGated) {
  EXPECT_TRUE(Gated("SET GLOBAL memory_limit = '10GB'"));
  EXPECT_TRUE(Gated("SET GLOBAL search_path = 'x'"));  // any setting under explicit GLOBAL
  EXPECT_TRUE(Gated("RESET GLOBAL memory_limit"));
  EXPECT_TRUE(Gated("SET GLOBAL gizmosql.query_log_level = 'DEBUG'"));
}

TEST(AdminCommandGuard, BareSetOfDangerousGlobalSettingIsGated) {
  EXPECT_TRUE(Gated("SET memory_limit = '100GB'"));
  EXPECT_TRUE(Gated("SET threads = 1"));
  EXPECT_TRUE(Gated("SET enable_external_access = false"));
  EXPECT_TRUE(Gated("SET allow_unsigned_extensions = true"));
  EXPECT_TRUE(Gated("SET temp_directory = '/tmp/evil'"));
  EXPECT_TRUE(Gated("SET disabled_filesystems = ''"));
  EXPECT_TRUE(Gated("RESET memory_limit"));
}

TEST(AdminCommandGuard, HarmlessSessionSetIsAllowed) {
  EXPECT_FALSE(Gated("SET SESSION memory_limit = '1GB'"));  // explicit session
  EXPECT_FALSE(Gated("SET search_path = 'main'"));          // bare, harmless setting
  EXPECT_FALSE(Gated("SET TimeZone = 'UTC'"));
  EXPECT_FALSE(Gated("SET timezone = 'UTC'"));
  EXPECT_FALSE(Gated("SET gizmosql.query_log_level = 'DEBUG'"));  // bare session-scoped
}

// =============================================================================
// INSTALL / LOAD extensions.
// =============================================================================

TEST(AdminCommandGuard, InstallLoadIsGated) {
  EXPECT_TRUE(Gated("INSTALL httpfs"));
  EXPECT_TRUE(Gated("FORCE INSTALL httpfs"));
  EXPECT_TRUE(Gated("LOAD httpfs"));
  EXPECT_TRUE(Gated("install spatial"));
  EXPECT_EQ(Category("INSTALL httpfs"), "INSTALL extension");
  EXPECT_EQ(Category("LOAD httpfs"), "LOAD extension");
}

// =============================================================================
// CHECKPOINT.
// =============================================================================

TEST(AdminCommandGuard, CheckpointIsGated) {
  EXPECT_TRUE(Gated("CHECKPOINT"));
  EXPECT_TRUE(Gated("FORCE CHECKPOINT"));
  EXPECT_TRUE(Gated("checkpoint"));
  EXPECT_EQ(Category("CHECKPOINT"), "CHECKPOINT");
}

TEST(AdminCommandGuard, HarmlessPragmaIsAllowed) {
  EXPECT_FALSE(Gated("PRAGMA table_info('my_table')"));
  EXPECT_FALSE(Gated("PRAGMA database_list"));
  EXPECT_FALSE(Gated("PRAGMA version"));
}

// =============================================================================
// COPY — local gated, remote allowed; nested reads still gated.
// =============================================================================

TEST(AdminCommandGuard, CopyLocalIsGated) {
  EXPECT_TRUE(Gated("COPY my_table TO '/tmp/out.csv'"));
  EXPECT_TRUE(Gated("COPY my_table TO 'out.parquet' (FORMAT parquet)"));
  EXPECT_TRUE(Gated("COPY my_table FROM '/etc/passwd'"));
  EXPECT_TRUE(Gated("COPY (SELECT 1) TO '/tmp/x.csv'"));
  EXPECT_EQ(Category("COPY my_table TO '/tmp/out.csv'"), "COPY TO (local filesystem)");
  EXPECT_EQ(Category("COPY t FROM '/x.csv'"), "COPY FROM (local filesystem)");
}

TEST(AdminCommandGuard, CopyRemoteIsAllowed) {
  EXPECT_FALSE(Gated("COPY my_table TO 's3://bucket/out.parquet' (FORMAT parquet)"));
  EXPECT_FALSE(Gated("COPY my_table TO 'gs://bucket/out.parquet'"));
  EXPECT_FALSE(Gated("COPY my_table FROM 'https://example.com/data.csv'"));
}

TEST(AdminCommandGuard, CopyRemoteButNestedLocalReadIsGated) {
  // COPY target is remote (allowed) but it reads a local file in the subquery.
  EXPECT_TRUE(Gated(
      "COPY (SELECT * FROM read_csv('/etc/passwd')) TO 's3://bucket/out.parquet'"));
}

// =============================================================================
// EXPORT DATABASE.
// =============================================================================

TEST(AdminCommandGuard, ExportImportDatabaseAlwaysGated) {
  // Full-database egress (EXPORT) / ingress (IMPORT) is gated regardless of
  // local vs remote — it is not bounded by object grants.
  EXPECT_TRUE(Gated("EXPORT DATABASE '/tmp/dump'"));
  EXPECT_TRUE(Gated("EXPORT DATABASE 'dump_dir' (FORMAT parquet)"));
  EXPECT_TRUE(Gated("EXPORT DATABASE 's3://bucket/dump'"));
  EXPECT_EQ(Category("EXPORT DATABASE '/tmp/dump'"), "EXPORT DATABASE");
  EXPECT_EQ(Category("EXPORT DATABASE 's3://bucket/dump'"), "EXPORT DATABASE");

  EXPECT_TRUE(Gated("IMPORT DATABASE '/tmp/dump'"));
  EXPECT_TRUE(Gated("IMPORT DATABASE 's3://bucket/dump'"));
  EXPECT_EQ(Category("IMPORT DATABASE '/tmp/dump'"), "IMPORT DATABASE");
}

// =============================================================================
// read_* table functions — local gated, remote allowed, any nesting.
// =============================================================================

TEST(AdminCommandGuard, ReadFunctionsLocalAreGated) {
  EXPECT_TRUE(Gated("SELECT * FROM read_csv('/etc/passwd')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_csv_auto('data.csv')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_parquet('/data/x.parquet')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_json('cfg.json')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_text('/etc/hosts')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_blob('/etc/shadow')"));
  EXPECT_TRUE(Gated("SELECT * FROM glob('/home/*')"));
  EXPECT_TRUE(Gated("SELECT * FROM parquet_scan('local.parquet')"));
}

TEST(AdminCommandGuard, ReadFunctionsRemoteAreAllowed) {
  EXPECT_FALSE(Gated("SELECT * FROM read_parquet('s3://bucket/x.parquet')"));
  EXPECT_FALSE(Gated("SELECT * FROM read_csv('https://example.com/data.csv')"));
  EXPECT_FALSE(Gated("SELECT * FROM read_json('gs://bucket/data.json')"));
  EXPECT_FALSE(Gated("SELECT * FROM glob('s3://bucket/*')"));
}

TEST(AdminCommandGuard, ReadFunctionsNonLiteralPathIsGated) {
  // Cannot prove the path is remote -> fail closed (gate it).
  EXPECT_TRUE(Gated("SELECT * FROM read_parquet(some_path_column)"));
  EXPECT_TRUE(Gated("SELECT * FROM read_csv(concat('/tmp/', 'x.csv'))"));
}

TEST(AdminCommandGuard, ReadFunctionsNestedAreGated) {
  EXPECT_TRUE(Gated(
      "SELECT a FROM (SELECT * FROM read_csv('/etc/passwd')) sub"));
  EXPECT_TRUE(Gated(
      "WITH t AS (SELECT * FROM read_parquet('/x.parquet')) SELECT * FROM t"));
  EXPECT_TRUE(Gated(
      "SELECT * FROM my_table JOIN read_csv('/etc/passwd') USING (id)"));
  EXPECT_TRUE(Gated(
      "SELECT * FROM my_table WHERE id IN (SELECT id FROM read_csv('/x.csv'))"));
  EXPECT_TRUE(Gated(
      "CREATE TABLE stolen AS SELECT * FROM read_csv('/etc/passwd')"));
  EXPECT_TRUE(Gated(
      "INSERT INTO t SELECT * FROM read_parquet('/x.parquet')"));
  EXPECT_TRUE(Gated(
      "SELECT * FROM read_csv('a.csv') UNION ALL SELECT * FROM my_table"));
}

TEST(AdminCommandGuard, PrepareIndirectionIsGated) {
  // PREPARE staging a gated read is caught at prepare time, closing the
  // PREPARE/EXECUTE indirection (a non-admin can't then EXECUTE it).
  EXPECT_TRUE(Gated("PREPARE p AS SELECT * FROM read_csv('/etc/passwd')"));
  EXPECT_FALSE(Gated("PREPARE p AS SELECT * FROM my_table"));
}

// =============================================================================
// duckdb_secrets() — always gated.
// =============================================================================

TEST(AdminCommandGuard, DuckDBSecretsIsAlwaysGated) {
  EXPECT_TRUE(Gated("SELECT * FROM duckdb_secrets()"));
  EXPECT_TRUE(Gated("SELECT name FROM duckdb_secrets() WHERE name = 'x'"));
  EXPECT_TRUE(Gated("SELECT * FROM (SELECT * FROM duckdb_secrets()) s"));
  EXPECT_EQ(Category("SELECT * FROM duckdb_secrets()"), "duckdb_secrets()");
}

// =============================================================================
// Replacement scans (SELECT * FROM '<path>').
// =============================================================================

TEST(AdminCommandGuard, LocalReplacementScanIsGated) {
  EXPECT_TRUE(Gated("SELECT * FROM '/etc/passwd'"));
  EXPECT_TRUE(Gated("SELECT * FROM 'data.parquet'"));
  EXPECT_TRUE(Gated("SELECT * FROM './local/data.csv'"));
  EXPECT_TRUE(Gated("SELECT * FROM 'subdir/file.json'"));
}

TEST(AdminCommandGuard, RemoteReplacementScanIsAllowed) {
  EXPECT_FALSE(Gated("SELECT * FROM 's3://bucket/data.parquet'"));
  EXPECT_FALSE(Gated("SELECT * FROM 'https://example.com/data.csv'"));
}

// =============================================================================
// Allowed (negative) cases — ordinary queries non-admins should run.
// =============================================================================

TEST(AdminCommandGuard, OrdinaryQueriesAreAllowed) {
  EXPECT_FALSE(Gated("SELECT 1"));
  EXPECT_FALSE(Gated("SELECT * FROM my_table WHERE id > 10"));
  EXPECT_FALSE(Gated("SELECT a, b FROM schema1.my_table JOIN other USING (id)"));
  EXPECT_FALSE(Gated("INSERT INTO my_table VALUES (1, 'a')"));
  EXPECT_FALSE(Gated("UPDATE my_table SET a = 1 WHERE id = 2"));
  EXPECT_FALSE(Gated("DELETE FROM my_table WHERE id = 2"));
  EXPECT_FALSE(Gated("CREATE TABLE t (id INTEGER, name VARCHAR)"));
  EXPECT_FALSE(Gated("CREATE TABLE t AS SELECT * FROM other_table"));
  EXPECT_FALSE(Gated("SHOW TABLES"));
  EXPECT_FALSE(Gated("DESCRIBE my_table"));
  EXPECT_FALSE(Gated("BEGIN TRANSACTION"));
  EXPECT_FALSE(Gated("COMMIT"));
  EXPECT_FALSE(Gated("SELECT * FROM range(100)"));  // non-filesystem table function
  EXPECT_FALSE(Gated("SELECT * FROM duckdb_settings()"));  // not duckdb_secrets
}

TEST(AdminCommandGuard, UnparseableSqlIsAllowedThroughToDuckDB) {
  // We don't gate what we can't parse; DuckDB surfaces the real error later.
  EXPECT_FALSE(Gated("this is not valid sql @#$%"));
  EXPECT_FALSE(Gated(""));
}

// =============================================================================
// The Flight-error wrapper.
// =============================================================================

TEST(AdminCommandGuard, CheckWrapperReturnsErrorForGated) {
  auto st = CheckNonAdminCommandAllowed("ATTACH 'x.db' AS x");
  EXPECT_FALSE(st.ok());
  EXPECT_NE(st.message().find("admin"), std::string::npos);
  EXPECT_NE(st.message().find("ATTACH"), std::string::npos);
}

TEST(AdminCommandGuard, CheckWrapperOkForAllowed) {
  EXPECT_TRUE(CheckNonAdminCommandAllowed("SELECT 1").ok());
  EXPECT_TRUE(CheckNonAdminCommandAllowed("SELECT * FROM my_table").ok());
}

// =============================================================================
// End-to-end integration: prove the gate is wired into the query path and keys
// off the session role. A non-admin token is rejected for gated commands and
// allowed for ordinary queries; an admin (basic-auth) session is unaffected.
// =============================================================================

#ifndef _WIN32

class AdminGateServerFixture
    : public gizmosql::testing::ServerTestFixture<AdminGateServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "admin_gate_test.db",
        .port = 31420,
        .health_port = 31421,
        .username = "admin_user",
        .password = "admin_pass",
        // Relaxed mode so a self-minted non-admin token (with an arbitrary
        // instance_id) is accepted; the server uses "test_secret_key_for_testing".
        .allow_cross_instance_tokens = true,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<AdminGateServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<AdminGateServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<AdminGateServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<AdminGateServerFixture>::config_{};

namespace {

// Mint a GizmoSQL-format bearer token with the given role, signed with the test
// secret. instance_id is arbitrary (accepted under allow_cross_instance_tokens).
std::string MintToken(const std::string& username, const std::string& role) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("admin-gate-test-token")
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("admin-gate-instance")))
      .set_payload_claim("session_id", jwt::claim(std::string("admin-gate-session")))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

// Execute `sql` with the given authorization header, draining any result. Returns
// the call status (non-OK if the gate — or anything else — rejected it).
arrow::Status ExecAs(int port, const std::pair<std::string, std::string>& auth_header,
                     const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto loc,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client,
                        arrow::flight::FlightClient::Connect(loc, {}));
  arrow::flight::FlightCallOptions co;
  co.headers.push_back(auth_header);
  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(co, sql));
  for (const auto& ep : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(co, ep.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    (void)table;
  }
  return arrow::Status::OK();
}

}  // namespace

TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForGatedCommands) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("analyst_user", "analyst")};

  for (const std::string& sql : {
           std::string("ATTACH 'side.db' AS side"),
           std::string("DETACH some_catalog"),
           std::string("SET GLOBAL memory_limit = '512MB'"),
           std::string("INSTALL httpfs"),
           std::string("SELECT * FROM read_csv('/etc/passwd')"),
           std::string("SELECT * FROM duckdb_secrets()"),
           std::string("CHECKPOINT"),
       }) {
    auto st = ExecAs(GetPort(), analyst, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    EXPECT_NE(st.ToString().find("admin"), std::string::npos)
        << "rejection should mention the admin requirement for: " << sql
        << " (got: " << st.ToString() << ")";
  }
}

TEST_F(AdminGateServerFixture, NonAdminTokenAllowedForOrdinaryQueries) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("analyst_user", "analyst")};

  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT 1 AS x").ok());
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT * FROM range(10)").ok());
  // A harmless session SET is allowed for non-admins.
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SET search_path = 'main'").ok());
}

TEST_F(AdminGateServerFixture, AdminBasicAuthUnaffected) {
  ASSERT_TRUE(IsServerReady());
  // Basic-auth users get the "admin" role, so the gate must not block them.
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto loc, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  // SET GLOBAL is gated for non-admins but must succeed for an admin.
  EXPECT_TRUE(ExecAs(GetPort(), bearer, "SET GLOBAL memory_limit = '512MB'").ok());
  EXPECT_TRUE(ExecAs(GetPort(), bearer, "CHECKPOINT").ok());
}

#endif  // !_WIN32
