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

#include <duckdb.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>

#include "admin_command_guard.h"
#include "duckdb_compat.h"
#include "plan_access_guard.h"

using arrow::flight::sql::FlightSqlClient;
using gizmosql::ddb::CheckNonAdminCommandAllowed;
using gizmosql::ddb::ClassifyGatedCommand;
using gizmosql::ddb::ClassifyUnredactedSecretsSet;
using gizmosql::ddb::GateOptions;

namespace {

// True if `sql` is gated (would be rejected for a non-admin) under the default
// server policy (remote URLs admin-only).
bool Gated(const std::string& sql) {
  return ClassifyGatedCommand(sql).has_value();
}

std::string Category(const std::string& sql) {
  return ClassifyGatedCommand(sql).value_or("");
}

// The same, on a server started with --allow-non-admin-remote-access.
const GateOptions kAllowRemote{.allow_remote_paths = true};

bool GatedAllowingRemote(const std::string& sql) {
  return ClassifyGatedCommand(sql, kAllowRemote).has_value();
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
// COPY — local always gated; remote gated unless the server allows it; nested
// reads still gated.
// =============================================================================

TEST(AdminCommandGuard, CopyLocalIsGated) {
  EXPECT_TRUE(Gated("COPY my_table TO '/tmp/out.csv'"));
  EXPECT_TRUE(Gated("COPY my_table TO 'out.parquet' (FORMAT parquet)"));
  EXPECT_TRUE(Gated("COPY my_table FROM '/etc/passwd'"));
  EXPECT_TRUE(Gated("COPY (SELECT 1) TO '/tmp/x.csv'"));
  EXPECT_EQ(Category("COPY my_table TO '/tmp/out.csv'"), "COPY TO (local filesystem)");
  EXPECT_EQ(Category("COPY t FROM '/x.csv'"), "COPY FROM (local filesystem)");
}

TEST(AdminCommandGuard, CopyRemoteIsGatedByDefault) {
  // A non-admin's remote COPY runs with the server's credentials, so on a
  // shared bucket it can write into another catalog's data prefix.
  EXPECT_EQ(Category("COPY my_table TO 's3://bucket/out.parquet' (FORMAT parquet)"),
            "COPY TO (remote URL)");
  EXPECT_EQ(Category("COPY my_table TO 'gs://bucket/out.parquet'"), "COPY TO (remote URL)");
  EXPECT_EQ(Category("COPY my_table FROM 'https://example.com/data.csv'"),
            "COPY FROM (remote URL)");
  EXPECT_EQ(Category("COPY my_table TO 'GS://Bucket/Out.parquet'"), "COPY TO (remote URL)");
}

TEST(AdminCommandGuard, CopyRemoteIsAllowedWhenServerAllowsIt) {
  EXPECT_FALSE(GatedAllowingRemote("COPY my_table TO 's3://bucket/out.parquet'"));
  EXPECT_FALSE(GatedAllowingRemote("COPY my_table TO 'gs://bucket/out.parquet'"));
  EXPECT_FALSE(GatedAllowingRemote("COPY my_table FROM 'https://example.com/data.csv'"));
  // Local stays gated whatever the policy.
  EXPECT_TRUE(GatedAllowingRemote("COPY my_table TO '/tmp/out.csv'"));
}

TEST(AdminCommandGuard, CopyRemoteButNestedLocalReadIsGated) {
  // COPY target is remote (allowed by policy) but it reads a local file.
  EXPECT_TRUE(GatedAllowingRemote(
      "COPY (SELECT * FROM read_csv('/etc/passwd')) TO 's3://bucket/out.parquet'"));
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
// read_* table functions — local always gated, remote gated unless allowed,
// any nesting.
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

TEST(AdminCommandGuard, ReadFunctionsRemoteAreGatedByDefault) {
  // Finding 4 of the Summation report: outbound HTTP from the cluster.
  EXPECT_EQ(Category("SELECT * FROM read_csv('https://example.com/')"),
            "read_csv() (remote URL)");
  EXPECT_EQ(Category("SELECT * FROM read_parquet('s3://bucket/x.parquet')"),
            "read_parquet() (remote URL)");
  EXPECT_TRUE(Gated("SELECT * FROM read_json('gs://bucket/data.json')"));
  EXPECT_TRUE(Gated("SELECT * FROM glob('s3://bucket/*')"));
}

TEST(AdminCommandGuard, ReadFunctionsRemoteAreAllowedWhenServerAllowsIt) {
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM read_parquet('s3://bucket/x.parquet')"));
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM read_csv('https://example.com/data.csv')"));
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM read_json('gs://bucket/data.json')"));
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM glob('s3://bucket/*')"));
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

TEST(AdminCommandGuard, RemoteReplacementScanIsGatedByDefault) {
  EXPECT_EQ(Category("SELECT * FROM 's3://bucket/data.parquet'"),
            "reading a remote URL via FROM 's3://bucket/data.parquet'");
  EXPECT_TRUE(Gated("SELECT * FROM 'https://example.com/data.csv'"));
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM 's3://bucket/data.parquet'"));
  EXPECT_FALSE(GatedAllowingRemote("SELECT * FROM 'https://example.com/data.csv'"));
}

// =============================================================================
// External-database functions — postgres_*/mysql_*/sqlite_* are admin-only.
// Finding 1 of the Summation report: postgres_query/postgres_execute reached
// another catalog's DuckLake metadata Postgres as that catalog's own role.
// =============================================================================

TEST(AdminCommandGuard, ReportFinding1StatementsAreGated) {
  const std::string meta = "'__ducklake_metadata_agc_staging_a9cf0fd1e2d43e1e_canary'";
  EXPECT_EQ(Category("SELECT * FROM postgres_query(" + meta +
                     ", 'select table_id, table_name, path from ducklake_table')"),
            "postgres_query() (external database access)");
  EXPECT_TRUE(Gated("SELECT * FROM postgres_query(" + meta +
                    ", 'select current_user, current_database()')"));
  EXPECT_TRUE(Gated("SELECT * FROM postgres_query(" + meta +
                    ", 'select datname from pg_database')"));
  EXPECT_EQ(Category("CALL postgres_execute(" + meta + ", 'select 1')"),
            "postgres_execute() (external database access)");
}

TEST(AdminCommandGuard, ExternalDatabaseFunctionsAreGatedEverywhere) {
  EXPECT_TRUE(Gated("SELECT * FROM postgres_scan('dbname=x', 'public', 't')"));
  EXPECT_TRUE(Gated("SELECT * FROM POSTGRES_QUERY('pg', 'select 1')"));  // case
  EXPECT_TRUE(Gated("SELECT * FROM mysql_query('my', 'select 1')"));
  EXPECT_TRUE(Gated("SELECT * FROM sqlite_scan('/tmp/x.db', 't')"));
  EXPECT_TRUE(Gated("CALL postgres_attach('dbname=x')"));
  // Nesting, EXPLAIN ANALYZE (executes) and PREPARE.
  EXPECT_TRUE(Gated("SELECT * FROM t WHERE id IN (SELECT 1 FROM postgres_query('pg', 'x'))"));
  EXPECT_TRUE(Gated("WITH m AS (SELECT * FROM postgres_query('pg', 'x')) SELECT * FROM m"));
  EXPECT_TRUE(Gated("EXPLAIN ANALYZE SELECT * FROM postgres_query('pg', 'x')"));
  EXPECT_TRUE(Gated("PREPARE p AS SELECT * FROM postgres_query('pg', 'x')"));
  EXPECT_TRUE(Gated("INSERT INTO t SELECT * FROM postgres_query('pg', 'x')"));
  // Policy-independent.
  EXPECT_TRUE(GatedAllowingRemote("SELECT * FROM postgres_query('pg', 'select 1')"));
  // Selecting from an attached catalog's tables is not a function call.
  EXPECT_FALSE(Gated("SELECT * FROM my_pg.public.orders"));
  EXPECT_FALSE(Gated("SELECT 'postgres_query' AS s"));
}

// =============================================================================
// DuckLake functions. Admin-only ones are refused at parse time; the
// catalog-checked ones (finding 3) pass here and are checked at bind time
// against the token's catalog access (see the plan-guard tests below).
// =============================================================================

TEST(AdminCommandGuard, DuckLakeAdminOnlyFunctionsAreGated) {
  EXPECT_EQ(Category("CALL ducklake_add_data_files('lake', 't', 'gs://b/other/x.parquet')"),
            "ducklake_add_data_files()");
  EXPECT_TRUE(Gated("SELECT * FROM ducklake_add_data_files('lake', 't', 's3://b/x.parquet')"));
  EXPECT_TRUE(Gated("SELECT * FROM ducklake_commit('meta', 1)"));
  EXPECT_EQ(Category("SELECT * FROM ducklake_scan('gs://b/other/x.parquet')"),
            "ducklake_scan()");
}

TEST(AdminCommandGuard, DuckLakeCatalogFunctionsAreLeftToTheBindTimeCheck) {
  EXPECT_FALSE(Gated("SELECT * FROM ducklake_snapshots('lake_a')"));
  EXPECT_FALSE(Gated("SELECT * FROM ducklake_list_files('lake_a', 't')"));
  EXPECT_FALSE(Gated("CALL ducklake_expire_snapshots('lake_a', dry_run => true)"));
}

// =============================================================================
// Bind-time classification of individual table function calls.
// =============================================================================

namespace {

using gizmosql::ddb::ClassifyTableFunctionCall;
using gizmosql::ddb::PlanTableFunctionCall;

PlanTableFunctionCall Call(const std::string& name, std::vector<duckdb::Value> params = {}) {
  PlanTableFunctionCall call;
  call.name_lower = name;
  for (auto& p : params) call.parameters.push_back(std::move(p));
  return call;
}

// Token rules of the report: write on its own catalog, nothing else.
bool OwnCatalogOnly(const std::string& catalog, bool /*need_write*/) {
  return catalog == "lake_a";
}
bool ReadOnlyOwnCatalog(const std::string& catalog, bool need_write) {
  return catalog == "lake_a" && !need_write;
}

std::string Bind(const PlanTableFunctionCall& call, const GateOptions& options = {},
                 gizmosql::ddb::CatalogAccessCheck check = OwnCatalogOnly) {
  return ClassifyTableFunctionCall(call, options, check).value_or("");
}

}  // namespace

TEST(PlanAccessGuard, ExternalDatabaseFunctionsCalledWithArgumentsAreRefused) {
  EXPECT_EQ(Bind(Call("postgres_query", {duckdb::Value("__ducklake_metadata_lake_b"),
                                         duckdb::Value("select 1")})),
            "postgres_query() (external database access)");
  EXPECT_FALSE(Bind(Call("postgres_execute", {duckdb::Value("pg"), duckdb::Value("x")})).empty());
  // DuckDB scans an attached Postgres/SQLite catalog's tables with these same
  // functions, but with no arguments; those scans are left to catalog grants.
  EXPECT_EQ(Bind(Call("postgres_scan")), "");
  EXPECT_EQ(Bind(Call("sqlite_scan")), "");
}

TEST(PlanAccessGuard, DuckLakeFunctionsAreCheckedAgainstCatalogAccess) {
  // Finding 3 of the Summation report.
  EXPECT_EQ(Bind(Call("ducklake_list_files", {duckdb::Value("lake_b"), duckdb::Value("secret_b")})),
            "ducklake_list_files() on catalog 'lake_b' (no read access to that catalog)");
  EXPECT_FALSE(Bind(Call("ducklake_snapshots", {duckdb::Value("lake_b")})).empty());
  EXPECT_FALSE(Bind(Call("ducklake_table_info", {duckdb::Value("lake_b")})).empty());
  EXPECT_EQ(Bind(Call("ducklake_snapshots", {duckdb::Value("lake_a")})), "");
  // Maintenance needs write.
  EXPECT_EQ(Bind(Call("ducklake_expire_snapshots", {duckdb::Value("lake_a")})), "");
  EXPECT_EQ(Bind(Call("ducklake_expire_snapshots", {duckdb::Value("lake_a")}), {},
                 ReadOnlyOwnCatalog),
            "ducklake_expire_snapshots() on catalog 'lake_a' (no write access to that "
            "catalog)");
  EXPECT_EQ(Bind(Call("ducklake_snapshots", {duckdb::Value("lake_a")}), {}, ReadOnlyOwnCatalog),
            "");
  // Unknown future ducklake_* functions need write (fail closed).
  EXPECT_FALSE(Bind(Call("ducklake_some_new_function", {duckdb::Value("lake_a")}), {},
                    ReadOnlyOwnCatalog)
                   .empty());
  // No constant catalog: fail closed.
  EXPECT_FALSE(Bind(Call("ducklake_snapshots")).empty());
  EXPECT_FALSE(Bind(Call("ducklake_snapshots", {duckdb::Value(42)})).empty());
  EXPECT_FALSE(Bind(Call("ducklake_snapshots", {duckdb::Value()})).empty());
}

TEST(PlanAccessGuard, DuckLakeScanAndAdminFunctions) {
  EXPECT_EQ(Bind(Call("ducklake_scan")), "");  // scan of a DuckLake table
  EXPECT_EQ(Bind(Call("ducklake_scan", {duckdb::Value("gs://b/x.parquet")})), "ducklake_scan()");
  EXPECT_EQ(Bind(Call("ducklake_add_data_files", {duckdb::Value("lake_a")})),
            "ducklake_add_data_files()");
  EXPECT_EQ(Bind(Call("ducklake_commit", {duckdb::Value("m")})), "ducklake_commit()");
}

TEST(PlanAccessGuard, FileReadsAndRemoteUrls) {
  EXPECT_EQ(Bind(Call("read_csv", {duckdb::Value("/etc/passwd")})),
            "read_csv() (local filesystem)");
  EXPECT_EQ(Bind(Call("glob", {duckdb::Value("/etc/*")})), "glob() (local filesystem)");
  EXPECT_EQ(Bind(Call("read_csv", {duckdb::Value("https://example.com/")})),
            "read_csv() (remote URL)");
  EXPECT_EQ(Bind(Call("read_csv", {duckdb::Value("https://example.com/")}), kAllowRemote), "");
  // A list mixing remote and local paths is refused for its local entry.
  auto mixed = duckdb::Value::LIST(
      {duckdb::Value("s3://b/1.parquet"), duckdb::Value("/data/2.parquet")});
  EXPECT_EQ(Bind(Call("read_parquet", {mixed}), kAllowRemote),
            "read_parquet() (local filesystem)");
  // No path we can read: fail closed.
  EXPECT_FALSE(Bind(Call("read_parquet")).empty());
  // Any other table function handed a URL.
  EXPECT_EQ(Bind(Call("iceberg_scan", {duckdb::Value("s3://b/table")})),
            "iceberg_scan() (remote URL)");
  EXPECT_EQ(Bind(Call("iceberg_scan", {duckdb::Value("s3://b/table")}), kAllowRemote), "");
  // Ordinary table functions.
  EXPECT_EQ(Bind(Call("range", {duckdb::Value::BIGINT(10)})), "");
  EXPECT_EQ(Bind(Call("seq_scan")), "");
  EXPECT_EQ(Bind(Call("duckdb_tables")), "");
}

// =============================================================================
// Bind-time guard on a real DuckDB connection: the plan walk sees through
// macros, views and `?` parameters on every DuckDB channel.
// =============================================================================

namespace {

// Applies the non-admin plan classification to every statement bound on the
// connection, the way the server's per-session guard does.
class TestPlanGuard : public duckdb::ClientContextState {
 public:
  bool CanRequestRebind() override { return true; }
  duckdb::RebindQueryInfo OnFinalizePrepare(duckdb::ClientContext&,
                                            duckdb::PreparedStatementData& prepared,
                                            duckdb::PreparedStatementMode) override {
    if (auto v = gizmosql::ddb::ClassifyPreparedPlan(prepared, GateOptions{},
                                                     OwnCatalogOnly)) {
      throw duckdb::PermissionException("blocked: " + *v);
    }
    return duckdb::RebindQueryInfo::DO_NOT_REBIND;
  }
};

struct GuardedConnection {
  duckdb::DuckDB db{nullptr};
  duckdb::Connection admin{db};  // sets up objects without the guard
  duckdb::Connection user{db};

  GuardedConnection() {
    user.context->registered_state->Insert("test_plan_guard",
                                           duckdb::make_shared_ptr<TestPlanGuard>());
  }
  std::string Run(const std::string& sql) {
    auto result = user.Query(sql);
    return result->HasError() ? result->GetError() : "";
  }
};

}  // namespace

TEST(PlanAccessGuard, OrdinaryStatementsBindNormally) {
  GuardedConnection c;
  ASSERT_EQ(c.Run("CREATE TABLE t AS SELECT * FROM range(3) r(i)"), "");
  EXPECT_EQ(c.Run("SELECT count(*) FROM t"), "");
  EXPECT_EQ(c.Run("SELECT * FROM range(5) JOIN t ON range = t.i"), "");
  EXPECT_EQ(c.Run("WITH x AS (SELECT * FROM t) SELECT * FROM x"), "");
}

TEST(PlanAccessGuard, MacroWrappingAGatedFunctionIsCaught) {
  GuardedConnection c;
  ASSERT_FALSE(c.admin.Query("CREATE MACRO lister(p) AS TABLE SELECT * FROM glob(p)")
                   ->HasError());
  const auto error = c.Run("SELECT * FROM lister('/gizmosql_no_such_dir/*')");
  EXPECT_NE(error.find("glob() (local filesystem)"), std::string::npos) << error;
}

TEST(PlanAccessGuard, ViewWrappingAGatedFunctionIsCaught) {
  GuardedConnection c;
  ASSERT_FALSE(c.admin.Query("CREATE VIEW v AS SELECT * FROM glob('/gizmosql_no_such_dir/*')")
                   ->HasError());
  const auto error = c.Run("SELECT * FROM v");
  EXPECT_NE(error.find("glob() (local filesystem)"), std::string::npos) << error;
  // Nested inside a subquery and a CTE too.
  EXPECT_NE(c.Run("SELECT * FROM range(1) WHERE EXISTS (SELECT 1 FROM v)"), "");
  EXPECT_NE(c.Run("WITH w AS (SELECT * FROM v) SELECT * FROM w"), "");
}

// Token rules match catalog names case-sensitively, DuckDB resolves them
// case-insensitively: with a deny-list rule ({lake_b: none}, {*: write}),
// ducklake_snapshots('LAKE_B') must still be checked as lake_b.
TEST(PlanAccessGuard, CatalogNamesAreResolvedAsDuckDBSpellsThem) {
  struct Resolver : duckdb::ClientContextState {
    std::string upper, mixed, missing;
    bool CanRequestRebind() override { return true; }
    duckdb::RebindQueryInfo OnFinalizePrepare(duckdb::ClientContext& context,
                                              duckdb::PreparedStatementData&,
                                              duckdb::PreparedStatementMode) override {
      upper = gizmosql::ddb::CanonicalCatalogName(context, "LAKE_B");
      mixed = gizmosql::ddb::CanonicalCatalogName(context, "Lake_B");
      missing = gizmosql::ddb::CanonicalCatalogName(context, "no_such_catalog");
      return duckdb::RebindQueryInfo::DO_NOT_REBIND;
    }
  };
  duckdb::DuckDB db(nullptr);
  duckdb::Connection con(db);
  ASSERT_FALSE(con.Query("ATTACH ':memory:' AS lake_b")->HasError());
  auto resolver = duckdb::make_shared_ptr<Resolver>();
  con.context->registered_state->Insert("resolver", resolver);
  ASSERT_FALSE(con.Query("SELECT 1")->HasError());
  EXPECT_EQ(resolver->upper, "lake_b");
  EXPECT_EQ(resolver->mixed, "lake_b");
  EXPECT_EQ(resolver->missing, "no_such_catalog");
}

TEST(PlanAccessGuard, PreparedStatementParameterIsCaught) {
  GuardedConnection c;
  auto stmt = c.user.Prepare("SELECT * FROM glob(?)");
  if (stmt->HasError()) {
    GTEST_SKIP() << "this DuckDB cannot prepare glob(?), so a parameter cannot supply "
                    "its path: "
                 << stmt->GetError();
  }
  duckdb::vector<duckdb::Value> values{duckdb::Value("/gizmosql_no_such_dir/*")};
  auto result = gizmosql::ddb::compat::ExecuteMaterialized(*stmt, values);
  ASSERT_TRUE(result->HasError()) << "a `?` must not smuggle a path past the guard";
  EXPECT_NE(result->GetError().find("glob()"), std::string::npos) << result->GetError();
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
  // A remote target in a CTE follows the same policy as a top-level COPY.
  EXPECT_EQ(Category("WITH c AS (COPY (SELECT 42) TO 's3://bucket/out.csv') SELECT 1"),
            "COPY TO (remote URL)");
  EXPECT_FALSE(
      GatedAllowingRemote("WITH c AS (COPY (SELECT 42) TO 's3://bucket/out.csv') SELECT 1"));
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
           // Summation report findings 1, 2 and 4 (refused before binding, so no
           // Postgres server or bucket is needed).
           std::string("SELECT * FROM postgres_query('__ducklake_metadata_other', "
                       "'select datname from pg_database')"),
           std::string("CALL postgres_execute('__ducklake_metadata_other', 'select 1')"),
           std::string("COPY (SELECT 1 AS i) TO "
                       "'gs://bucket/org/other-catalog/main/t/injected.parquet' "
                       "(FORMAT parquet)"),
           std::string("SELECT * FROM read_csv('https://example.com/')"),
           std::string("SELECT * FROM 'https://example.com/data.csv'"),
           std::string("CALL ducklake_add_data_files('lake', 't', 'gs://b/x.parquet')"),
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

TEST_F(AdminGateServerFixture, ViewOrMacroWrappingAGatedFunctionIsRefusedAtBindTime) {
  ASSERT_TRUE(IsServerReady());
  // One token = one session, so the temp view and macro persist across calls.
  const std::pair<std::string, std::string> analyst{
      "authorization", "Bearer " + MintToken("wrapper_user", "analyst")};

  // Defining them binds nothing the parse-time gate looks at...
  ASSERT_TRUE(ExecAs(GetPort(), analyst,
                     "CREATE OR REPLACE TEMP VIEW etc_files AS SELECT * FROM glob('/etc/*')")
                  .ok());
  ASSERT_TRUE(ExecAs(GetPort(), analyst,
                     "CREATE OR REPLACE TEMP MACRO lister(p) AS TABLE SELECT * FROM glob(p)")
                  .ok());

  // ...but using them binds glob('/etc/*'), which the bind-time guard refuses.
  for (const std::string& sql : {std::string("SELECT * FROM etc_files"),
                                 std::string("SELECT * FROM lister('/etc/*')"),
                                 std::string("SELECT count(*) FROM range(3), etc_files")}) {
    auto st = ExecAs(GetPort(), analyst, sql);
    EXPECT_FALSE(st.ok()) << "non-admin should be refused for: " << sql;
    EXPECT_NE(st.ToString().find("glob() (local filesystem)"), std::string::npos)
        << sql << " -> " << st.ToString();
  }
  // The session keeps working for ordinary statements afterwards.
  EXPECT_TRUE(ExecAs(GetPort(), analyst, "SELECT * FROM range(3)").ok());
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
