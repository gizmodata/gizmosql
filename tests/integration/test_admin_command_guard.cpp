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
#include <vector>

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
using gizmosql::ddb::ClassifyUnredactedSecretsSet;

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

// EXPLAIN ANALYZE executes the wrapped statement, so wrapping a gated statement
// in EXPLAIN [ANALYZE] must not get it past the gate.
TEST(AdminCommandGuard, ExplainWrappedStatementsAreGated) {
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE COPY (SELECT 42) TO '/tmp/x.csv'"));
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE SET GLOBAL threads = 3"));
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE CHECKPOINT"));
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE SELECT * FROM read_text('/etc/passwd')"));
  EXPECT_TRUE(
      Gated("EXPLAIN ANALYZE COPY (SELECT * FROM read_text('/etc/passwd')) "
            "TO 's3://bucket/x.csv'"));
  EXPECT_TRUE(Gated("EXPLAIN SELECT * FROM duckdb_secrets()"));
  EXPECT_FALSE(Gated("EXPLAIN ANALYZE SELECT 42"));
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

TEST(UnredactedSecretsSet, TrueIsMatched) {
  EXPECT_TRUE(
      ClassifyUnredactedSecretsSet("SET allow_unredacted_secrets = true").has_value());
  EXPECT_TRUE(
      ClassifyUnredactedSecretsSet("SET allow_unredacted_secrets=true").has_value());
  EXPECT_TRUE(ClassifyUnredactedSecretsSet("/* c */ SET ALLOW_UNREDACTED_SECRETS = TRUE")
                  .has_value());
}

// DuckDB casts the SET value to BOOLEAN, so every one of these enables the
// setting and must be matched, not just the literal `true`.
TEST(UnredactedSecretsSet, TrueEquivalentsAreMatched) {
  for (const char* sql : {
           "SET allow_unredacted_secrets = 1",
           "SET allow_unredacted_secrets = 'true'",
           "SET allow_unredacted_secrets = 't'",
           "SET allow_unredacted_secrets = 'yes'",
           "SET allow_unredacted_secrets = true::BOOLEAN",
           "SET allow_unredacted_secrets = (true)",
           "SET allow_unredacted_secrets = NOT false",
           "SET allow_unredacted_secrets = (SELECT true)",
           "SET allow_unredacted_secrets TO true",
           "SET GLOBAL allow_unredacted_secrets = true",
           "SET \"allow_unredacted_secrets\" = true",
           "PRAGMA allow_unredacted_secrets = true",
           "SELECT 1; SET allow_unredacted_secrets = true",
       }) {
    EXPECT_TRUE(ClassifyUnredactedSecretsSet(sql).has_value()) << sql;
  }
}

TEST(UnredactedSecretsSet, ExplainWrappedSetIsMatched) {
  EXPECT_TRUE(
      ClassifyUnredactedSecretsSet("EXPLAIN ANALYZE SET allow_unredacted_secrets = true")
          .has_value());
  EXPECT_TRUE(ClassifyUnredactedSecretsSet("EXPLAIN SET allow_unredacted_secrets = true")
                  .has_value());
}

TEST(UnredactedSecretsSet, FalseAndOtherSqlAreNotMatched) {
  for (const char* sql : {
           "SET allow_unredacted_secrets = false",
           "SET allow_unredacted_secrets = 0",
           "SET allow_unredacted_secrets = 'false'",
           "PRAGMA allow_unredacted_secrets = false",
           "RESET allow_unredacted_secrets",
           "SELECT current_setting('allow_unredacted_secrets')",
           "SET threads = 4",
           "SELECT 1",
       }) {
    EXPECT_FALSE(ClassifyUnredactedSecretsSet(sql).has_value()) << sql;
  }
}

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

// Object-storage COPY is not decided by the text gate: for a session scoped by
// catalog_access rules the file system limits it to its catalogs' data paths
// (RemotePathScope). A network URL and a local path stay gated for every non-admin.
TEST(AdminCommandGuard, CopyToObjectStorageIsLeftToTheFileSystem) {
  EXPECT_FALSE(Gated("COPY my_table TO 's3://bucket/out.parquet' (FORMAT parquet)"));
  EXPECT_FALSE(
      Gated("COPY (SELECT 1 AS i) TO 'gs://bucket/org/main/t/x.parquet' "
            "(FORMAT parquet)"));
  EXPECT_EQ(Category("COPY my_table TO 'https://example.com/x'"),
            "COPY TO (network URL)");
  EXPECT_EQ(Category("COPY my_table TO '/tmp/x.csv'"), "COPY TO (local filesystem)");
}

TEST(AdminCommandGuard, CopyFromObjectStorageIsAllowedButNetworkUrlIsNot) {
  EXPECT_FALSE(Gated("COPY my_table FROM 's3://bucket/data.csv'"));
  EXPECT_FALSE(Gated("COPY my_table FROM 'gs://bucket/data.parquet' (FORMAT parquet)"));
  EXPECT_EQ(Category("COPY my_table FROM 'https://example.com/data.csv'"),
            "COPY FROM (network URL)");
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

// Object-storage reads are not decided by the text gate either (see above): a
// path can be built from a macro, query() or a computed argument, which text
// inspection cannot follow. Network URLs are, in any function and any position.
TEST(AdminCommandGuard, ObjectStorageReadFunctionsAreLeftToTheFileSystem) {
  EXPECT_FALSE(Gated("SELECT * FROM read_parquet('s3://bucket/x.parquet')"));
  EXPECT_FALSE(Gated("SELECT * FROM read_json('gs://bucket/data.json')"));
  EXPECT_FALSE(Gated("SELECT * FROM glob('s3://bucket/*')"));
  EXPECT_FALSE(Gated("SELECT * FROM delta_scan('s3://bucket/table')"));
}

TEST(AdminCommandGuard, NetworkUrlInAnyTableFunctionIsGated) {
  EXPECT_EQ(Category("SELECT * FROM delta_scan('https://example.com/t')"),
            "delta_scan() (network URL)");
  EXPECT_TRUE(Gated("SELECT * FROM iceberg_scan('http://169.254.169.254/x')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_xlsx('hf://datasets/x/a.xlsx')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_parquet(['https://a/x.parquet', 's3://b/y'])"));
  EXPECT_TRUE(Gated(
      "SELECT * FROM read_parquet('https://a/x.parquet', hive_partitioning = true)"));
  EXPECT_TRUE(Gated("SELECT * FROM (SELECT * FROM delta_scan('https://a/t')) s"));
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

TEST(AdminCommandGuard, CreateDropSecretIsGated) {
  // Secrets hold credentials — every CREATE/DROP SECRET variant is admin-only.
  EXPECT_TRUE(Gated("CREATE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"));
  EXPECT_TRUE(Gated("CREATE OR REPLACE SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"));
  EXPECT_TRUE(Gated("CREATE PERSISTENT SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"));
  EXPECT_TRUE(Gated(
      "CREATE OR REPLACE PERSISTENT SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"));
  EXPECT_TRUE(Gated("CREATE TEMPORARY SECRET s (TYPE s3, KEY_ID 'k', SECRET 'v')"));
  EXPECT_EQ(Category("CREATE SECRET s (TYPE s3)"), "CREATE SECRET");

  EXPECT_TRUE(Gated("DROP SECRET s"));
  EXPECT_TRUE(Gated("DROP SECRET IF EXISTS s"));
  EXPECT_TRUE(Gated("DROP PERSISTENT SECRET s"));
  EXPECT_TRUE(Gated("DROP TEMPORARY SECRET s"));
  EXPECT_EQ(Category("DROP SECRET s"), "DROP SECRET");

  // Ordinary CREATE/DROP are not secret operations and stay allowed.
  EXPECT_FALSE(Gated("CREATE TABLE t (id INTEGER)"));
  EXPECT_FALSE(Gated("DROP TABLE t"));
  EXPECT_FALSE(Gated("DROP VIEW v"));
}

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

TEST(AdminCommandGuard, RemoteReplacementScanIsGatedOnlyForNetworkUrls) {
  EXPECT_FALSE(Gated("SELECT * FROM 's3://bucket/data.parquet'"));
  EXPECT_FALSE(Gated("SELECT * FROM 'gs://bucket/other_catalog/data.parquet'"));
  EXPECT_TRUE(Gated("SELECT * FROM 'https://example.com/data.csv'"));
}

// =============================================================================
// Allowed (negative) cases — ordinary queries non-admins should run.
// =============================================================================

TEST(AdminCommandGuard, QuackFunctionsAreGated) {
  // DuckDB's Quack remote protocol: quack_serve() would open a network server
  // inside GizmoSQL. CALL, table-function and scalar forms are all gated.
  EXPECT_EQ(Category("CALL quack_serve('quack:localhost:9494')"), "quack_serve()");
  EXPECT_EQ(Category("SELECT * FROM quack_query('quack:host', 'SELECT 1')"),
            "quack_query()");
  EXPECT_EQ(Category("SELECT quack_cancel('lake', 42)"), "quack_cancel()");
  EXPECT_FALSE(Gated("SELECT 'quack_serve' AS s"));
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
TEST(AdminCommandGuard, CopyInsideCteIsGated) {
  // DuckDB 2.0 allows COPY TO as a CTE body, so a SELECT can write a file.
  EXPECT_EQ(Category("WITH c AS (COPY (SELECT 42) TO '/tmp/out.csv') SELECT 1"),
            "COPY TO (local filesystem)");
  EXPECT_EQ(Category("SELECT * FROM (WITH c AS (COPY (SELECT 42) TO 'x.csv') SELECT 1) t"),
            "COPY TO (local filesystem)");
  EXPECT_EQ(Category("SELECT 1 UNION ALL (WITH c AS (COPY (SELECT 1) TO 'x.csv') SELECT 2)"),
            "COPY TO (local filesystem)");
  EXPECT_TRUE(
      Gated("WITH c AS (COPY (SELECT 42) TO 'https://example.com/out.csv') SELECT 1"));
  // Object storage is left to the file system (see CopyToObjectStorageIsLeftToTheFileSystem).
  EXPECT_FALSE(Gated("WITH c AS (COPY (SELECT 42) TO 's3://bucket/out.csv') SELECT 1"));
}

TEST(AdminCommandGuard, ConnectAndExternalResourceAreGated) {
  // CONNECT attaches its target and then forwards raw SQL to it.
  EXPECT_EQ(Category("CONNECT 'other.db'"), "CONNECT");
  EXPECT_EQ(Category("DISCONNECT"), "DISCONNECT");
}

TEST(AdminCommandGuard, NewSingleFileReadersAreGated) {
  EXPECT_EQ(Category("SELECT * FROM read_single_csv_file('/etc/passwd')"),
            "read_single_csv_file() (local filesystem)");
  EXPECT_EQ(Category("SELECT * FROM read_single_json_file('/etc/x.json')"),
            "read_single_json_file() (local filesystem)");
}

TEST(AdminCommandGuard, DmlSourcesAreWalked) {
  EXPECT_EQ(Category("UPDATE t SET a = x.a FROM read_csv('/etc/passwd') x"),
            "read_csv() (local filesystem)");
  EXPECT_EQ(Category("DELETE FROM t USING read_csv('/etc/passwd') x WHERE t.a = x.a"),
            "read_csv() (local filesystem)");
  EXPECT_FALSE(Gated("UPDATE t SET a = 1 WHERE b = 2"));
}
#endif

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

// The cross-catalog reach reported against a shared multi-tenant server: a
// non-admin token must not be able to run SQL in another catalog's metadata
// database, make outbound HTTP calls, or register arbitrary files in a DuckLake
// catalog, including through query(). All are refused before execution, so no
// Postgres or bucket is needed. (Object storage is limited per session by the
// file system instead; see RemotePathScopeFixture.)
TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForCrossCatalogReach) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> user{"authorization",
                                                 "Bearer " + MintToken("app_a", "user")};

  for (const std::string& sql : {
           std::string("SELECT * FROM postgres_query('__ducklake_metadata_b', "
                       "'select table_id, table_name, path from ducklake_table')"),
           std::string("SELECT * FROM postgres_query('__ducklake_metadata_b', "
                       "'select datname from pg_database')"),
           std::string("CALL postgres_execute('__ducklake_metadata_b', 'select 1')"),
           std::string(
               "SELECT 1; CALL postgres_execute('__ducklake_metadata_b', 'select 1')"),
           std::string("CREATE VIEW v AS SELECT * FROM postgres_query('b', 'select 1')"),
           std::string("SELECT * FROM query('SELECT * FROM postgres_query(''b'', "
                       "''select 1'')')"),
           std::string("SELECT * FROM query('SELECT * FROM read_csv(''/etc/passwd'')')"),
           std::string("COPY (SELECT 1 AS i) TO 'https://example.com/x.parquet'"),
           std::string("SELECT * FROM read_csv('https://example.com/')"),
           std::string("SELECT * FROM 'https://example.com/x.csv'"),
           std::string(
               "CALL ducklake_add_data_files('lake', 't', 'gs://other/x.parquet')"),
       }) {
    auto st = ExecAs(GetPort(), user, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    EXPECT_NE(st.ToString().find("requires the 'admin' role"), std::string::npos)
        << sql << " -> " << st.ToString();
  }
}

TEST_F(AdminGateServerFixture, AdminIsNotBlockedFromCrossCatalogFunctions) {
  ASSERT_TRUE(IsServerReady());
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  // The gate does not fire for an admin; the call then fails on its own (no such
  // catalog / extension), which is a different error.
  auto st =
      ExecAs(GetPort(), bearer, "SELECT * FROM postgres_query('no_such_db', 'select 1')");
  EXPECT_FALSE(st.ok());
  EXPECT_EQ(st.ToString().find("requires the 'admin' role"), std::string::npos)
      << st.ToString();
}

// The same functions reached indirectly: through a table macro that passes its
// argument on to query() or read_text(), through CALL, and through statement forms
// that carry a query (SUMMARIZE, SET VARIABLE, UPDATE). The macros are created by
// an admin, so the refusal has to come from the statement that uses them.
TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForIndirectRoutes) {
  ASSERT_TRUE(IsServerReady());
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto admin, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  // An admin may create such macros, and they work for the admin.
  ASSERT_ARROW_OK(
      ExecAs(GetPort(), admin,
             "CREATE OR REPLACE MACRO route_sql(s) AS TABLE SELECT * FROM query(s)"));
  ASSERT_ARROW_OK(ExecAs(
      GetPort(), admin,
      "CREATE OR REPLACE MACRO route_file(p) AS TABLE SELECT * FROM read_text(p)"));
  ASSERT_ARROW_OK(
      ExecAs(GetPort(), admin,
             "SELECT * FROM route_sql('SELECT count(*) FROM duckdb_secrets()')"));
  ASSERT_ARROW_OK(ExecAs(GetPort(), admin, "CALL duckdb_secrets()"));

  const std::pair<std::string, std::string> user{"authorization",
                                                 "Bearer " + MintToken("app_a", "user")};
  for (const std::string& sql : {
           std::string(
               "SELECT * FROM route_sql('SELECT count(*) FROM duckdb_secrets()')"),
           std::string("SELECT * FROM route_sql('SELECT * FROM postgres_query(''b'', "
                       "''select 1'')')"),
           std::string(
               "SELECT * FROM route_sql('SELECT * FROM read_text(''/etc/hosts'')')"),
           std::string("SELECT * FROM route_file('/etc/hosts')"),
           std::string(
               "CREATE OR REPLACE TEMP MACRO mine(s) AS TABLE SELECT * FROM query(s)"),
           std::string("CREATE OR REPLACE TEMP MACRO mine(p) AS TABLE "
                       "SELECT * FROM read_text(p)"),
           std::string("CREATE OR REPLACE TEMP MACRO mine() AS TABLE SELECT * FROM "
                       "query('SELECT count(*) FROM duckdb_' || 'secrets()')"),
           std::string("CREATE OR REPLACE TEMP MACRO mine(s) AS "
                       "(SELECT count(*) FROM query(s))"),
           std::string("CALL duckdb_secrets()"),
           std::string("CALL read_text('/etc/hosts')"),
           std::string("CALL glob('/etc/*')"),
           std::string("CALL query('SELECT count(*) FROM duckdb_secrets()')"),
           std::string("CALL postgres_query('b', 'select 1')"),
           std::string("SELECT * FROM query_table('/etc/hosts.csv')"),
           std::string("SUMMARIZE SELECT * FROM read_text('/etc/hosts')"),
           std::string("DESCRIBE SELECT * FROM postgres_query('b', 'select 1')"),
           std::string(
               "SET VARIABLE leaked = (SELECT content FROM read_text('/etc/hosts'))"),
           std::string("CREATE TEMP TABLE route_t AS SELECT 'a' AS x; UPDATE route_t SET "
                       "x = (SELECT content FROM read_text('/etc/hosts'))"),
       }) {
    auto st = ExecAs(GetPort(), user, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    EXPECT_NE(st.ToString().find("requires the 'admin' role"), std::string::npos)
        << sql << " -> " << st.ToString();
  }

  // The same forms without a gated function still work for the non-admin.
  for (const std::string& sql : {
           std::string("CREATE OR REPLACE TEMP MACRO fine(x) AS x + 1"),
           std::string("CREATE OR REPLACE TEMP MACRO fine_t(n) AS TABLE "
                       "SELECT * FROM range(n)"),
           std::string("SELECT * FROM fine_t(3)"),
           std::string("SELECT * FROM route_sql('SELECT 42 AS answer')"),
           std::string("DESCRIBE SELECT 1 AS x"),
           std::string("SUMMARIZE SELECT 1 AS x"),
           std::string("SET VARIABLE harmless = (SELECT 42)"),
           std::string(
               "SELECT 'CALL duckdb_secrets() and read_text(''/etc/hosts'')' AS s"),
       }) {
    auto st = ExecAs(GetPort(), user, sql);
    EXPECT_TRUE(st.ok()) << sql << " -> " << st.ToString();
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

TEST_F(AdminGateServerFixture, AdminSetUnredactedSecretsTrueIsRejected) {
  ASSERT_TRUE(IsServerReady());
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  auto rejected = ExecAs(GetPort(), bearer, "SET allow_unredacted_secrets = true");
  EXPECT_FALSE(rejected.ok());
  EXPECT_NE(rejected.ToString().find("disabled on this server"), std::string::npos)
      << rejected.ToString();

  EXPECT_TRUE(ExecAs(GetPort(), bearer, "SET allow_unredacted_secrets = false").ok());
}

TEST_F(AdminGateServerFixture, SqlWithNulByteIsRejected) {
  ASSERT_TRUE(IsServerReady());
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  static constexpr char kSql[] = "SELECT 1\0; SET allow_unredacted_secrets = true";
  const std::string sql(kSql, sizeof(kSql) - 1);  // keep the embedded NUL
  auto st = ExecAs(GetPort(), bearer, sql);
  EXPECT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("NUL byte"), std::string::npos) << st.ToString();
}

class UnredactedSecretsAllowedFixture
    : public gizmosql::testing::ServerTestFixture<UnredactedSecretsAllowedFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "unredacted_secrets_allowed_test.db",
        .port = 31422,
        .health_port = 31423,
        .username = "admin_user",
        .password = "admin_pass",
        .block_unredacted_secrets = false,
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<UnredactedSecretsAllowedFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<
    UnredactedSecretsAllowedFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<UnredactedSecretsAllowedFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<UnredactedSecretsAllowedFixture>::config_{};

TEST_F(UnredactedSecretsAllowedFixture, SetTrueReachesDuckDB) {
  ASSERT_TRUE(IsServerReady());
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(loc, options));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));

  auto st = ExecAs(GetPort(), bearer, "SET allow_unredacted_secrets = true");
  EXPECT_FALSE(st.ok());
  EXPECT_EQ(st.ToString().find("disabled on this server"), std::string::npos)
      << st.ToString();
  EXPECT_NE(st.ToString().find("Cannot change allow_unredacted_secrets"),
            std::string::npos)
      << st.ToString();
}

#endif  // !_WIN32

// =============================================================================
// Cross-catalog reach: scanner functions, network URLs, DuckLake functions.
// =============================================================================

TEST(AdminCommandGuard, ExternalDatabaseFunctionsAreGated) {
  EXPECT_EQ(Category("SELECT * FROM postgres_query('__ducklake_metadata_b', "
                     "'select 1')"),
            "postgres_query()");
  EXPECT_EQ(Category("CALL postgres_execute('__ducklake_metadata_b', 'select 1')"),
            "postgres_execute()");
  EXPECT_TRUE(Gated("SELECT * FROM postgres_scan('dbname=x', 'public', 't')"));
  EXPECT_TRUE(Gated("CALL postgres_clear_cache()"));
  EXPECT_TRUE(Gated("SELECT * FROM mysql_query('db', 'select 1')"));
  EXPECT_TRUE(Gated("SELECT * FROM sqlite_scan('x.db', 't')"));
  EXPECT_TRUE(Gated("SELECT * FROM (SELECT * FROM POSTGRES_QUERY('b', 'select 1')) t"));
  EXPECT_TRUE(Gated("CREATE TABLE t AS SELECT * FROM postgres_query('b', 'select 1')"));
  EXPECT_TRUE(Gated("CREATE VIEW v AS SELECT * FROM postgres_query('b', 'select 1')"));
  EXPECT_TRUE(
      Gated("CREATE MACRO m() AS TABLE SELECT * FROM postgres_query('b', 'select 1')"));
  EXPECT_FALSE(Gated("CREATE MACRO add1(x) AS x + 1"));
  EXPECT_FALSE(Gated("SELECT pg_typeof(1)"));
}

TEST(AdminCommandGuard, NetworkUrlReadsAreGated) {
  EXPECT_EQ(Category("SELECT * FROM read_csv('https://example.com/')"),
            "read_csv() (network URL)");
  EXPECT_TRUE(Gated("SELECT * FROM read_parquet('http://169.254.169.254/x')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_json('hf://datasets/x/y.json')"));
  EXPECT_FALSE(Gated("SELECT * FROM read_parquet('s3://bucket/x.parquet')"));
}

// query('<sql>') runs its argument as SQL. The checks used to see only the string.
TEST(AdminCommandGuard, QueryFunctionArgumentIsChecked) {
  EXPECT_TRUE(
      Gated("SELECT * FROM query('SELECT * FROM postgres_query(''m'', ''select 1'')')"));
  EXPECT_TRUE(Gated("SELECT * FROM query('SELECT * FROM read_csv(''/etc/passwd'')')"));
  EXPECT_TRUE(
      Gated("SELECT * FROM query('SELECT * FROM read_csv(''https://x/y.csv'')')"));
  EXPECT_TRUE(Gated("SELECT * FROM query('SELECT * FROM duckdb_secrets()')"));
  EXPECT_TRUE(Gated("SELECT * FROM query('COPY (SELECT 1) TO ''/tmp/x.csv''')"));
  EXPECT_TRUE(
      Gated("SELECT * FROM (SELECT * FROM query('SELECT * FROM glob(''/*'')')) s"));
  EXPECT_FALSE(Gated("SELECT * FROM query('SELECT 1 AS x')"));
  EXPECT_FALSE(
      Gated("SELECT * FROM query('SELECT * FROM read_parquet(''s3://b/x.parquet'')')"));
}

TEST(AdminCommandGuard, QueryFunctionNestingAndNonLiteralAreGated) {
  // SQL that is not a literal cannot be checked.
  EXPECT_EQ(Category("SELECT * FROM query(some_column)"),
            "query() (SQL that is not a string literal)");
  EXPECT_TRUE(Gated("SELECT * FROM query(concat('SELECT ', '1'))"));
  // A query() inside a query() string is followed.
  EXPECT_TRUE(
      Gated("SELECT * FROM query('SELECT * FROM query(''SELECT * FROM "
            "read_csv(''''/etc/passwd'''')'')')"));
}

TEST(AdminCommandGuard, SerializedSqlExecutionIsGated) {
  EXPECT_TRUE(
      Gated("SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT 1'))"));
}

TEST(AdminCommandGuard, DuckLakeCallsInsideQueryAreReported) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM query('SELECT * FROM ducklake_snapshots(''other'')')", &refs));
  ASSERT_EQ(refs.size(), 1u);
  EXPECT_EQ(refs[0].catalog, std::optional<std::string>("other"));
  EXPECT_FALSE(refs[0].qualified);
}

// A view runs with the access of whoever selects from it, so the caller refuses a
// view that wraps a DuckLake call; plain views are untouched.
TEST(AdminCommandGuard, DuckLakeCallInViewIsFlagged) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "CREATE VIEW v AS SELECT * FROM ducklake_snapshots('other')", &refs));
  ASSERT_EQ(refs.size(), 1u);
  EXPECT_TRUE(refs[0].in_view);
  refs.clear();
  EXPECT_FALSE(ClassifyGatedCommand("CREATE VIEW v AS SELECT 1 AS x", &refs));
  EXPECT_TRUE(refs.empty());
}

TEST(AdminCommandGuard, QualifiedCallsAreMarkedSoTheCallerCanResolveTheQualifier) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM someschema.settings(); SELECT * FROM ducklake_settings('lake')",
      &refs));
  ASSERT_EQ(refs.size(), 2u);
  EXPECT_TRUE(refs[0].qualified);
  EXPECT_FALSE(refs[1].qualified);
}

TEST(AdminCommandGuard, DuckLakeFunctionsReportTheirCatalog) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(
      ClassifyGatedCommand("SELECT * FROM ducklake_list_files('lake_b', 'secret_b'); "
                           "SELECT * FROM lake_b.snapshots(); "
                           "CALL ducklake_expire_snapshots('lake_c', '2020-01-01'); "
                           "CALL lake_d.set_option('x', 'y')",
                           &refs));
  ASSERT_EQ(refs.size(), 4u);
  EXPECT_EQ(refs[0].catalog, std::optional<std::string>("lake_b"));
  EXPECT_FALSE(refs[0].writes);
  EXPECT_EQ(refs[1].catalog, std::optional<std::string>("lake_b"));
  EXPECT_FALSE(refs[1].writes);
  EXPECT_EQ(refs[2].catalog, std::optional<std::string>("lake_c"));
  EXPECT_TRUE(refs[2].writes);
  EXPECT_EQ(refs[3].catalog, std::optional<std::string>("lake_d"));
  EXPECT_TRUE(refs[3].writes);
}

// Custom DuckLake builds add branch functions that take the catalog first. The
// read-only ones need read access; every other ducklake_* call needs write.
TEST(AdminCommandGuard, DuckLakeBranchFunctionsAreCheckedByAccessKind) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM ducklake_branches('a'); SELECT * FROM ducklake_current_branch('b'); "
      "SELECT * FROM ducklake_branch_table('c', 't'); "
      "CALL ducklake_create_branch('d', 'x'); CALL ducklake_set_branch('e', 'x'); "
      "CALL ducklake_merge_branch('f', 'x'); CALL ducklake_drop_branch('g', 'x'); "
      "CALL ducklake_close_stale_branches('h')",
      &refs));
  ASSERT_EQ(refs.size(), 8u);
  for (size_t i = 0; i < refs.size(); ++i) {
    EXPECT_EQ(refs[i].writes, i >= 3) << refs[i].function;
  }
  refs.clear();
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM ducklake_settings('x'); SELECT * FROM ducklake_options('y')",
      &refs));
  ASSERT_EQ(refs.size(), 2u);
  EXPECT_EQ(refs[0].catalog, std::optional<std::string>("x"));
  EXPECT_FALSE(refs[0].writes);
  EXPECT_FALSE(refs[1].writes);
}

// delta/iceberg use their own object-store client, which the file system scope
// cannot see: the guard reports them so a catalog-scoped session can be refused.
TEST(AdminCommandGuard, OwnStorageClientFunctionsAreReported) {
  for (const char* sql :
       {"SELECT * FROM delta_scan('s3://b/t')", "SELECT * FROM iceberg_scan('s3://b/t')",
        "SELECT * FROM query('SELECT * FROM delta_scan(''s3://b/t'')')",
        "SELECT count(*) FROM (SELECT * FROM delta_scan('s3://b/t'))",
        "CREATE MACRO m() AS TABLE SELECT * FROM delta_scan('s3://b/t')"}) {
    std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
    ClassifyGatedCommand(sql, &refs);
    ASSERT_FALSE(refs.empty()) << sql;
    EXPECT_TRUE(refs[0].own_storage_client) << sql;
  }
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand("SELECT * FROM read_parquet('s3://b/x')", &refs));
  EXPECT_TRUE(refs.empty());
}

TEST(AdminCommandGuard, DuckLakeNonLiteralCatalogCannotBeVerified) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM ducklake_snapshots(current_database())", &refs));
  ASSERT_EQ(refs.size(), 1u);
  EXPECT_FALSE(refs[0].catalog.has_value());
}

TEST(AdminCommandGuard, DuckLakeAddDataFilesIsGated) {
  EXPECT_TRUE(Gated("CALL ducklake_add_data_files('lake', 't', 'gs://other/x.parquet')"));
  EXPECT_TRUE(Gated("CALL lake.add_data_files('t', 'gs://other/x.parquet')"));
}

// =============================================================================
// Indirect routes: macros, CALL, query_table, and statement forms the walker
// does not reach.
// =============================================================================

// A macro runs its body for whoever calls it, so the body is held to the same
// rules as a statement. A body that passes a parameter on as a path or as SQL is
// refused: the parameter is not a string literal.
TEST(AdminCommandGuard, MacroBodiesAreChecked) {
  EXPECT_TRUE(Gated("CREATE MACRO m(s) AS TABLE SELECT * FROM query(s)"));
  EXPECT_TRUE(
      Gated("CREATE OR REPLACE TEMP MACRO m(p) AS TABLE SELECT * FROM read_csv(p)"));
  EXPECT_TRUE(
      Gated("CREATE MACRO m() AS TABLE SELECT * FROM "
            "query('SELECT * FROM duckdb_' || 'secrets()')"));
  EXPECT_TRUE(Gated("CREATE MACRO m(s := 'SELECT 1') AS TABLE SELECT * FROM query(s)"));
  EXPECT_TRUE(Gated("CREATE MACRO m(s) AS (SELECT count(*) FROM query(s))"));
  EXPECT_TRUE(
      Gated("CREATE MACRO m() AS (SELECT count(*) FROM read_csv('/etc/passwd'))"));
  EXPECT_TRUE(Gated("CREATE MACRO m() AS TABLE SELECT * FROM glob('/etc/*')"));
  EXPECT_TRUE(Gated("CREATE MACRO m(t) AS TABLE SELECT * FROM query_table(t)"));
  EXPECT_TRUE(
      Gated("CREATE MACRO m() AS TABLE SELECT * FROM read_csv('https://a/b.csv')"));
  // Ordinary macros are untouched.
  EXPECT_FALSE(Gated("CREATE MACRO add1(x) AS x + 1"));
  EXPECT_FALSE(Gated("CREATE MACRO recent(n) AS TABLE SELECT * FROM orders LIMIT n"));
  EXPECT_FALSE(Gated("CREATE MACRO firsts(n) AS TABLE SELECT * FROM range(n)"));
  EXPECT_FALSE(Gated("CREATE MACRO total() AS (SELECT sum(amount) FROM orders)"));
  EXPECT_FALSE(Gated(
      "CREATE MACRO lake() AS TABLE SELECT * FROM read_parquet('s3://b/x.parquet')"));
  EXPECT_FALSE(Gated("CREATE MACRO one() AS TABLE SELECT * FROM query('SELECT 1 AS x')"));
}

// CALL f(...) returns f's rows, like SELECT * FROM f(...).
TEST(AdminCommandGuard, CallIsHeldToTheTableFunctionRules) {
  EXPECT_EQ(Category("CALL duckdb_secrets()"), "duckdb_secrets()");
  EXPECT_EQ(Category("CALL read_csv('/etc/passwd')"), "read_csv() (local filesystem)");
  EXPECT_TRUE(Gated("CALL glob('/etc/*')"));
  EXPECT_TRUE(Gated("CALL read_text(getvariable('p'))"));
  EXPECT_TRUE(Gated("CALL read_csv('https://example.com/x.csv')"));
  EXPECT_TRUE(Gated("CALL query('SELECT * FROM duckdb_secrets()')"));
  EXPECT_TRUE(Gated("CALL query(getvariable('s'))"));
  EXPECT_TRUE(Gated("CALL json_execute_serialized_sql(json_serialize_sql('SELECT 1'))"));
  EXPECT_TRUE(Gated("PRAGMA json_execute_serialized_sql('{}')"));
  EXPECT_TRUE(Gated("CALL some_function((SELECT count(*) FROM duckdb_secrets()))"));
  EXPECT_FALSE(Gated("CALL pragma_table_info('t')"));
  EXPECT_FALSE(Gated("CALL read_parquet('s3://bucket/x.parquet')"));
  EXPECT_FALSE(Gated("CALL query('SELECT 1 AS x')"));
  EXPECT_FALSE(Gated("PRAGMA table_info('t')"));
}

// query_table('<name>') resolves a name like FROM does, so a path is a file read.
TEST(AdminCommandGuard, QueryTableNamesAreChecked) {
  EXPECT_EQ(Category("SELECT * FROM query_table('/etc/passwd')"),
            "query_table() (local filesystem)");
  EXPECT_TRUE(Gated("SELECT * FROM query_table('data.csv')"));
  EXPECT_TRUE(Gated("SELECT * FROM query_table(['t', '/tmp/x.parquet'], true)"));
  EXPECT_TRUE(Gated("SELECT * FROM query_table('https://example.com/x.csv')"));
  EXPECT_TRUE(Gated("SELECT * FROM query_table(getvariable('t'))"));
  EXPECT_FALSE(Gated("SELECT * FROM query_table('orders')"));
  EXPECT_FALSE(Gated("SELECT * FROM query_table(['orders', 'returns'], true)"));
}

// A table function these checks do not know (an extension's reader, a table macro)
// may hand its string argument to read_csv() or query().
TEST(AdminCommandGuard, StringArgumentsOfAnyTableFunctionAreChecked) {
  EXPECT_TRUE(Gated("SELECT * FROM m('SELECT count(*) FROM duckdb_secrets()')"));
  EXPECT_TRUE(
      Gated("SELECT * FROM m('SELECT * FROM postgres_query(''b'', ''select 1'')')"));
  EXPECT_TRUE(Gated("SELECT * FROM m(s := 'SELECT * FROM read_csv(''/etc/passwd'')')"));
  EXPECT_TRUE(Gated("SELECT * FROM m('COPY (SELECT 1) TO ''/tmp/x.csv''')"));
  EXPECT_TRUE(Gated("SELECT * FROM r('/etc/passwd')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_xlsx('/data/book.xlsx')"));
  EXPECT_TRUE(Gated("SELECT * FROM read_json_objects_auto('events.json')"));
  EXPECT_TRUE(Gated("SELECT * FROM st_read('file:///data/shapes.shp')"));
  EXPECT_TRUE(Gated("SELECT * FROM r(['~/notes.txt'])"));
  EXPECT_TRUE(Gated("CALL r('C:\\Users\\x\\data.csv')"));
  // Ordinary arguments are untouched.
  EXPECT_FALSE(Gated("SELECT * FROM m('SELECT 42 AS answer')"));
  EXPECT_FALSE(Gated("SELECT * FROM in_zone('America/New_York')"));
  EXPECT_FALSE(Gated("SELECT * FROM greet('hello world')"));
  EXPECT_FALSE(Gated("SELECT * FROM since('2026-01-01 00:00:00')"));
  EXPECT_FALSE(Gated("SELECT * FROM range(10)"));
  EXPECT_FALSE(Gated("SELECT * FROM delta_scan('s3://bucket/table')"));
  EXPECT_FALSE(Gated("SELECT * FROM generate_series(1, 5)"));

  // DuckLake calls inside such a string are reported like any other.
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(ClassifyGatedCommand(
      "SELECT * FROM m('SELECT * FROM ducklake_snapshots(''other'')')", &refs));
  ASSERT_EQ(refs.size(), 1u);
  EXPECT_EQ(refs[0].catalog, std::optional<std::string>("other"));
}

// Statement forms the walker does not reach fail closed: a call it did not inspect
// is refused, whatever statement carries it.
TEST(AdminCommandGuard, CallsInUninspectedStatementFormsAreRefused) {
  for (const char* sql : {
           "DESCRIBE SELECT * FROM read_csv('/etc/passwd')",
           "SUMMARIZE SELECT * FROM read_csv('/etc/passwd')",
           "SHOW (SELECT * FROM read_csv('/etc/passwd'))",
           "SELECT * FROM (DESCRIBE SELECT * FROM read_csv('/etc/passwd'))",
           "SET VARIABLE v = (SELECT column0 FROM read_csv('/etc/passwd'))",
           "CREATE TYPE e AS ENUM (SELECT column0 FROM read_csv('/etc/passwd'))",
           "PIVOT (SELECT * FROM read_csv('/etc/passwd')) ON column0 USING count(*)",
           "UPDATE t SET x = (SELECT count(*) FROM duckdb_secrets())",
           "UPDATE t SET x = r.a FROM read_csv('/etc/passwd') r",
           "DELETE FROM t WHERE x IN (SELECT a FROM postgres_query('b', 'select 1'))",
           "DELETE FROM t USING read_csv('/etc/passwd') r WHERE t.x = r.a",
           "MERGE INTO t USING read_csv('/etc/passwd') r ON t.x = r.a "
           "WHEN NOT MATCHED THEN INSERT VALUES (r.a)",
           "DESCRIBE SELECT * FROM query('SELECT * FROM duckdb_secrets()')",
           "SUMMARIZE SELECT * FROM read_parquet('https://example.com/x.parquet')",
           "DESCRIBE SELECT * FROM read_parquet(getvariable('p'))",
           "DESCRIBE SELECT * FROM read_parquet('s3://b/x' || '.parquet')",
           // A replacement scan names no function at all.
           "SUMMARIZE '/etc/passwd'",
           "DESCRIBE 'data.csv'",
           "DESCRIBE SELECT * FROM '/etc/passwd'",
           "SUMMARIZE SELECT * FROM \"/etc/passwd\"",
           "SUMMARIZE SELECT * FROM 'https://example.com/x.csv'",
           "UPDATE t SET x = s.a FROM '/etc/passwd' s",
           "UPDATE t SET x = s.a FROM u JOIN v ON true, '/etc/passwd' s",
           "DELETE FROM t USING '~/x.csv' s WHERE t.x = s.a",
           "MERGE INTO t USING 'data.parquet' s ON t.x = s.a "
           "WHEN NOT MATCHED THEN INSERT VALUES (s.a)",
           "PIVOT '/data/x.csv' ON a USING count(*)",
           "SET VARIABLE v = (SELECT a FROM t, '/etc/passwd')",
           // However the call is spelled.
           "DESCRIBE SELECT * FROM \"read_csv\"('/etc/passwd')",
           "DESCRIBE SELECT * FROM READ_CSV /* c */ ('/etc/passwd')",
           "DESCRIBE SELECT * FROM main.read_csv('/etc/passwd')",
           "DESCRIBE SELECT 'it''s' AS q, * FROM read_csv('/etc/passwd')",
           "DESCRIBE SELECT E'\\'' AS q, * FROM read_csv('/etc/passwd')",
           "DESCRIBE SELECT $$x$$ AS q, * FROM read_csv('/etc/passwd')",
           "DESCRIBE SELECT 1 -- c\n FROM read_csv('/etc/passwd')",
       }) {
    EXPECT_TRUE(Gated(sql)) << sql;
  }
}

// The same forms stay usable when they name no gated call, when the name only
// appears in a string or a comment, and for literal object-storage reads (which
// is all the walker would have asked of them).
TEST(AdminCommandGuard, UninspectedStatementFormsWithoutGatedCallsAreAllowed) {
  for (const char* sql : {
           "DESCRIBE SELECT 1 AS x",
           "SUMMARIZE SELECT * FROM orders",
           "SET VARIABLE v = (SELECT max(id) FROM orders)",
           "CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')",
           "PIVOT orders ON region USING sum(amount)",
           "UPDATE t SET x = (SELECT max(id) FROM orders)",
           "DELETE FROM t WHERE x IN (SELECT id FROM returns)",
           "MERGE INTO t USING staged s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
           "DESCRIBE SELECT * FROM read_parquet('s3://bucket/x.parquet')",
           "SUMMARIZE SELECT * FROM read_parquet('gs://bucket/dir/*.parquet', "
           "hive_partitioning = true)",
           "MERGE INTO t USING read_parquet('s3://bucket/staged/*.parquet') s "
           "ON t.id = s.id WHEN NOT MATCHED THEN INSERT *",
           "UPDATE t SET note = 'see read_csv(''/etc/passwd'') and duckdb_secrets()'",
           "DESCRIBE SELECT 'CALL query(1)' AS s",
           "DESCRIBE SELECT $$ read_csv('/etc/passwd') $$ AS s",
           "DESCRIBE SELECT E'read_csv(\\'/etc/passwd\\')' AS s",
           "DESCRIBE SELECT 1 AS x -- read_csv('/etc/passwd')",
           "DESCRIBE SELECT 1 /* duckdb_secrets() */ AS x",
           "DESCRIBE SELECT 1 AS \"read_csv\"",
           "DESCRIBE SELECT * FROM 's3://bucket/x.parquet'",
           "UPDATE t SET path = '/data/x.csv', note = 'a, /b.csv' WHERE id = 1",
           "UPDATE t SET x = 1 FROM u WHERE u.path IN ('/a.csv', '/b.csv')",
           "DELETE FROM t WHERE path = '/data/x.csv'",
           "SUMMARIZE SELECT '/etc/passwd' AS p, 'x.csv' AS q FROM orders",
           "SELECT * FROM unnest(['report.csv', '/tmp/x.txt'])",
           "SELECT * FROM orders, returns WHERE orders.file = '/x.csv'",
           "SELECT query, glob FROM audit_log WHERE query IN ('a', 'b')",
           "INSERT INTO t SELECT * FROM read_parquet('s3://bucket/x.parquet')",
           "WITH c AS (SELECT * FROM read_parquet('s3://b/x.parquet')) SELECT * FROM c "
           "WHERE id IN (SELECT id FROM read_parquet('s3://b/y.parquet'))",
       }) {
    EXPECT_FALSE(Gated(sql)) << sql << " -> " << Category(sql);
  }
}

// delta/iceberg calls in an uninspected form are still reported, so the caller
// can refuse them for catalog-scoped sessions.
TEST(AdminCommandGuard, OwnStorageClientCallsInUninspectedFormsAreReported) {
  std::vector<gizmosql::ddb::DuckLakeFunctionRef> refs;
  EXPECT_FALSE(
      ClassifyGatedCommand("DESCRIBE SELECT * FROM delta_scan('s3://b/t')", &refs));
  ASSERT_FALSE(refs.empty());
  EXPECT_TRUE(refs[0].own_storage_client);
  refs.clear();
  // A DuckLake call must either be reported for the token check (DuckDB 2.x, whose
  // DESCRIBE the walker reaches) or be refused outright (DuckDB 1.x).
  auto described =
      ClassifyGatedCommand("DESCRIBE SELECT * FROM ducklake_snapshots('other')", &refs);
  EXPECT_TRUE(
      described.has_value() ||
      (refs.size() == 1 && refs[0].catalog == std::optional<std::string>("other")));
  refs.clear();
  auto summarized =
      ClassifyGatedCommand("SUMMARIZE SELECT * FROM other_lake.snapshots()", &refs);
  EXPECT_TRUE(summarized.has_value() ||
              (refs.size() == 1 && refs[0].qualified &&
               refs[0].catalog == std::optional<std::string>("other_lake")));
}
