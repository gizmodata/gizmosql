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
#include <filesystem>
#include <fstream>
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

TEST(AdminCommandGuard, SettingsThatWriteFilesAreGatedInEveryScope) {
  for (const char* sql : {"SET log_query_path = '/tmp/q.log'",
                          "SET SESSION profiling_output = '/tmp/p.txt'",
                          "SET http_logging_output = '/tmp/h.log'"}) {
    EXPECT_TRUE(Gated(sql)) << sql;
  }
  EXPECT_FALSE(Gated("RESET profiling_output"));        // back to no file
  EXPECT_FALSE(Gated("SET SESSION TimeZone = 'UTC'"));  // positive twin
}

TEST(AdminCommandGuard, BindTimeReadsAreGated) {
  // DESCRIBE / SUMMARIZE and CREATE VIEW bind their query, reading what it
  // names, before any later check.
  for (const char* sql :
       {"DESCRIBE SELECT * FROM read_csv('/etc/passwd')",
        "SUMMARIZE SELECT * FROM read_csv('/etc/passwd')", "DESCRIBE '/etc/passwd'",
        "CREATE VIEW v AS SELECT * FROM read_csv('/etc/passwd')",
        "CREATE OR REPLACE TEMP VIEW v AS SELECT * FROM '/etc/passwd'"}) {
    EXPECT_TRUE(Gated(sql)) << sql;
  }
  EXPECT_FALSE(Gated("DESCRIBE SELECT 1"));  // positive twins
  EXPECT_FALSE(Gated("CREATE VIEW v AS SELECT 1"));
}

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

TEST(AdminCommandGuard, RemoteReplacementScanIsAllowed) {
  EXPECT_FALSE(Gated("SELECT * FROM 's3://bucket/data.parquet'"));
  EXPECT_FALSE(Gated("SELECT * FROM 'https://example.com/data.csv'"));
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

// DuckDB 2.0's external-resource table functions run callbacks named by string
// on an internal connection, past every check on the client's own statement:
// destroy_external_resource('query', '<sql>') executes <sql> and returns its
// error text, so it reads any table or file the engine can reach.
TEST(AdminCommandGuard, ExternalResourceFunctionsAreGated) {
  EXPECT_EQ(Category("SELECT * FROM destroy_external_resource('query', 'SELECT 1')"),
            "destroy_external_resource()");
  EXPECT_EQ(Category("CALL destroy_external_resource('query', 'SELECT 1')"),
            "destroy_external_resource()");
  EXPECT_EQ(Category("SELECT * FROM create_external_resource('t', 'n')"),
            "create_external_resource()");
  EXPECT_EQ(
      Category(
          "CALL register_external_resource_type('t', create_function := 'read_csv')"),
      "register_external_resource_type()");
  EXPECT_EQ(Category("SELECT * FROM register_external_resource('t', 'n', {})"),
            "register_external_resource()");
  EXPECT_EQ(Category("CALL deregister_external_resource('t', 'n')"),
            "deregister_external_resource()");
  // Nesting, case and PREPARE do not get it past the gate.
  EXPECT_TRUE(Gated(
      "SELECT * FROM (SELECT * FROM DESTROY_EXTERNAL_RESOURCE('query', 'SELECT 1')) x"));
  EXPECT_TRUE(
      Gated("WITH c AS (SELECT * FROM destroy_external_resource('query', 'SELECT 1')) "
            "SELECT * FROM c"));
  EXPECT_TRUE(
      Gated("SELECT 1 WHERE EXISTS "
            "(SELECT * FROM destroy_external_resource('query', 'SELECT 1'))"));
  EXPECT_TRUE(
      Gated("PREPARE p AS SELECT * FROM destroy_external_resource('query', 'SELECT 1')"));
  EXPECT_FALSE(Gated("SELECT 'destroy_external_resource' AS s"));
}

TEST(AdminCommandGuard, ExternalResourceMetadataIsGated) {
  // Instance-wide: handles, URIs and callback names of every resource.
  EXPECT_EQ(Category("SELECT * FROM duckdb_external_resources()"),
            "duckdb_external_resources()");
  EXPECT_EQ(Category("SELECT * FROM duckdb_external_resource_types()"),
            "duckdb_external_resource_types()");
  EXPECT_EQ(Category("SELECT * FROM duckdb_extension_repositories()"),
            "duckdb_extension_repositories()");
}

// A repository's public keys become trusted extension-signing keys, so adding
// one lets its owner load their own native code. These parse as LOAD
// statements; the category names what the user actually typed.
TEST(AdminCommandGuard, ExtensionRepositoryStatementsAreGated) {
  EXPECT_EQ(
      Category("CREATE EXTENSION REPOSITORY r WITH PREFIX 'https://example.invalid/ext' "
               "USING PUBLIC KEY 'k'"),
      "CREATE EXTENSION REPOSITORY");
  EXPECT_EQ(
      Category(
          "CREATE OR REPLACE EXTENSION REPOSITORY r PREFIX 'https://example.invalid/x'"),
      "CREATE EXTENSION REPOSITORY");
  EXPECT_EQ(Category("DROP EXTENSION REPOSITORY IF EXISTS r"),
            "DROP EXTENSION REPOSITORY");
  EXPECT_EQ(Category("INSTALL AND LOAD httpfs"), "INSTALL extension");
}

// The gate parses with DuckDB's built-in grammar and lets through what it
// cannot parse (DuckDB reports the error). A session that switches its own
// grammar could send SQL the gate cannot parse but the engine can, so these
// settings are gated in every scope, not only as bare SETs.
TEST(AdminCommandGuard, GrammarSwitchingSettingsAreGatedInAnyScope) {
  EXPECT_EQ(Category("SET current_dialect = 'other'"), "SET current_dialect");
  EXPECT_EQ(Category("SET SESSION current_dialect = 'other'"), "SET current_dialect");
  EXPECT_EQ(Category("SET active_grammar_extensions = ['x']"),
            "SET active_grammar_extensions");
  EXPECT_EQ(Category("SET SESSION active_grammar_extensions = ['x']"),
            "SET active_grammar_extensions");
  EXPECT_EQ(Category("SET SESSION enable_legacy_parser = true"),
            "SET enable_legacy_parser");
  // EXPLAIN ANALYZE runs the wrapped SET (DuckDB does not PREPARE a SET).
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE SET SESSION current_dialect = 'other'"));
  // Going back to the built-in grammar is harmless.
  EXPECT_FALSE(Gated("RESET current_dialect"));
  EXPECT_FALSE(Gated("RESET active_grammar_extensions"));
}

TEST(AdminCommandGuard, NewExtensionAndLoggingSettingsAreGated) {
  EXPECT_EQ(Category("SET allow_extension_repositories = 'allowed'"),
            "SET GLOBAL allow_extension_repositories");
  EXPECT_EQ(Category("SET extension_repository_directory = '/tmp/repos'"),
            "SET GLOBAL extension_repository_directory");
  EXPECT_EQ(Category("SET extension_directories = ['/tmp/ext']"),
            "SET GLOBAL extension_directories");
  // false would put other sessions' HTTP headers into duckdb_logs.
  EXPECT_EQ(Category("SET redact_http_logs = false"), "SET GLOBAL redact_http_logs");
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

// SQL inside query() / query_table() is checked like the statement around it:
// it is only a string literal to the parser, but DuckDB runs it. The file is an
// ordinary local file (not one the sensitive-path guard knows), so only the
// admin gate can refuse it.
TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForNestedSql) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("analyst_user", "analyst")};
  const std::string file =
      (std::filesystem::temp_directory_path() /
       ("gizmosql_nested_sql_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
        ".csv"))
          .string();
  {
    std::ofstream out(file);
    out << "a\n1\n";
  }
  const std::string q1 = "'" + file + "'";    // a literal at depth 1
  const std::string q2 = "''" + file + "''";  // inside query('...')
  const std::string q3 = std::string(4, '\'') + file + std::string(4, '\'');  // two deep
  for (const std::string& sql : {
           "SELECT * FROM query('SELECT * FROM read_csv(" + q2 + ")')",
           "SELECT * FROM query('SELECT * FROM " + q2 + "')",
           std::string("SELECT * FROM query('SELECT name FROM duckdb_secrets()')"),
           "SELECT * FROM query('SELECT * FROM query(''SELECT * FROM read_csv(" + q3 +
               ")'')')",
           "SELECT * FROM query('SELECT ' || '* FROM read_csv(" + q2 + ")')",
           "SELECT * FROM query_table(" + q1 + ")",
           "SELECT * FROM query_table(['t', " + q1 + "])",
       }) {
    auto st = ExecAs(GetPort(), analyst, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    EXPECT_NE(st.ToString().find("requires the 'admin' role"), std::string::npos)
        << "rejection should mention the admin requirement for: " << sql
        << " (got: " << st.ToString() << ")";
  }
  // Positive twins: nested SQL that reads nothing gated still runs, and the
  // admin may read the file this way.
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT * FROM query('SELECT 42 AS x')").ok());
  EXPECT_TRUE(
      ExecAs(GetPort(), analyst, "SELECT * FROM query('SELECT * FROM range(3)')").ok());
  // The admin logs in with Basic auth (every minted token here shares a session).
  ASSERT_ARROW_OK_AND_ASSIGN(auto loc,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto admin, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  auto st = ExecAs(GetPort(), admin,
                   "SELECT * FROM query('SELECT * FROM read_csv(" + q2 + ")')");
  EXPECT_TRUE(st.ok()) << st.ToString();
  std::filesystem::remove(file);
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

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
// DuckDB 2.0 surfaces, end to end. The marker in the callback SQL proves the
// gate refused the statement before the engine ran it: without the gate,
// destroy_external_resource executes the string and returns its error text.
TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForDuckDB2Commands) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("analyst_user", "analyst")};
  const std::string kMarker = "reached-the-engine";

  for (const std::string& sql : {
           "SELECT * FROM destroy_external_resource('query', 'SELECT error(''" + kMarker +
               "'')')",
           "CALL destroy_external_resource('query', 'SELECT error(''" + kMarker + "'')')",
           std::string("SELECT * FROM duckdb_external_resources()"),
           std::string("SET SESSION current_dialect = 'other'"),
           std::string("SET allow_extension_repositories = 'allowed'"),
           std::string(
               "CREATE EXTENSION REPOSITORY r WITH PREFIX 'https://example.invalid/x'"),
       }) {
    auto st = ExecAs(GetPort(), analyst, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    EXPECT_NE(st.ToString().find("requires the 'admin' role"), std::string::npos)
        << "rejection should come from the admin gate for: " << sql
        << " (got: " << st.ToString() << ")";
    EXPECT_EQ(st.ToString().find(kMarker), std::string::npos)
        << "the engine ran the callback SQL for: " << sql << " (got: " << st.ToString()
        << ")";
  }
}
#endif

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

// A macro's body runs when it is called, and FROM m() names no file, so the
// body is checked when the macro is created. json_execute_serialized_sql() runs
// a statement the parser only sees as a string.
TEST_F(AdminGateServerFixture, NonAdminTokenRejectedForMacroAndSerializedSql) {
  ASSERT_TRUE(IsServerReady());
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("analyst_user", "analyst")};
  const std::string file =
      (std::filesystem::temp_directory_path() /
       ("gizmosql_macro_" +
        std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
        ".csv"))
          .string();
  {
    std::ofstream out(file);
    out << "a\n1\n";
  }
  for (const std::string& sql : {
           "CREATE TEMP MACRO pm() AS TABLE SELECT * FROM read_csv('" + file + "')",
           std::string(
               "CREATE TEMP MACRO ps() AS TABLE SELECT name FROM duckdb_secrets()"),
           std::string("CREATE MACRO pq(x) AS TABLE SELECT * FROM query_table(x)"),
           std::string("CREATE MACRO pr(p) AS TABLE SELECT * FROM read_csv(p)"),
           "CREATE MACRO pc() AS (SELECT count(*) FROM read_csv('" + file + "'))",
           "CREATE MACRO pd(p := (SELECT count(*) FROM read_csv('" + file + "'))) AS p",
           "SELECT * FROM json_execute_serialized_sql(json_serialize_sql('SELECT * FROM "
           "read_csv(''" +
               file + "'')'))",
       }) {
    auto st = ExecAs(GetPort(), analyst, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be rejected for: " << sql;
    // DuckDB 1.4 (LTS) refuses a default value that is not a constant itself.
    const bool lts_default =
        sql.rfind("CREATE MACRO pd", 0) == 0 &&
        st.ToString().find("Invalid default value for parameter") != std::string::npos;
    EXPECT_TRUE(lts_default ||
                st.ToString().find("requires the 'admin' role") != std::string::npos)
        << "rejection should mention the admin requirement for: " << sql
        << " (got: " << st.ToString() << ")";
  }
  // Positive twins: macros that read nothing gated are created and run.
  EXPECT_TRUE(ExecAs(GetPort(), analyst,
                     "CREATE TEMP MACRO ok_t(n) AS TABLE SELECT * FROM range(n)")
                  .ok());
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT * FROM ok_t(3)").ok());
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "CREATE TEMP MACRO ok_s(a) AS a + 1").ok());
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT ok_s(1)").ok());
  std::filesystem::remove(file);
}
