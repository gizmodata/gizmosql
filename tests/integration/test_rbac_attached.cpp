// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Access control on attached catalogs: grants and column grants apply to
// DuckLake tables (and PostgreSQL tables when GIZMOSQL_TEST_RBAC_PG_URI is set)
// exactly as to native ones, and a refusal names the table rather than an
// internal scan function. Needs a license that includes RBAC and the ducklake
// extension.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <string>
#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#endif

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "enterprise/enterprise_features.h"
#include "jwt-cpp/jwt.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-attached-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-attached-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

arrow::Status RunAs(int port, const std::string& user, const std::string& role,
                    const std::string& session, const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  arrow::flight::FlightCallOptions co;
  co.headers.push_back({"authorization", "Bearer " + MintToken(user, role, session)});
  FlightSqlClient flight_sql(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, flight_sql.Execute(co, sql));
  for (const auto& ep : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, flight_sql.DoGet(co, ep.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    (void)table;
  }
  return arrow::Status::OK();
}

// The first column of the result, as strings.
arrow::Result<std::vector<std::string>> ValuesAs(int port, const std::string& user,
                                                 const std::string& role,
                                                 const std::string& session,
                                                 const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  arrow::flight::FlightCallOptions co;
  co.headers.push_back({"authorization", "Bearer " + MintToken(user, role, session)});
  FlightSqlClient flight_sql(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, flight_sql.Execute(co, sql));
  std::vector<std::string> values;
  for (const auto& ep : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, flight_sql.DoGet(co, ep.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    for (int64_t r = 0; r < table->num_rows(); ++r) {
      ARROW_ASSIGN_OR_RAISE(auto scalar, table->column(0)->GetScalar(r));
      values.push_back(scalar->ToString());
    }
  }
  return values;
}

class RbacAttachedFixture
    : public gizmosql::testing::ServerTestFixture<RbacAttachedFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_attached_test.db",
        .port = 31720,
        .health_port = 31721,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  const std::string kAdminSession = "00000000-0000-4000-a000-000000000001";
  const std::string kUserSession = "00000000-0000-4000-a000-000000000002";

  arrow::Status Admin(const std::string& sql) {
    return RunAs(GetPort(), "admin_user", "admin", kAdminSession, sql);
  }
  arrow::Status User(const std::string& sql) {
    return RunAs(GetPort(), "lake_user", "user", kUserSession, sql);
  }
  void ExpectBlocked(const std::string& sql, const std::string& why) {
    auto st = User(sql);
    ASSERT_FALSE(st.ok()) << "NOT BLOCKED: " << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos) << st.ToString();
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << " -> expected '" << why << "' in: " << st.ToString();
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacAttachedFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacAttachedFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RbacAttachedFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacAttachedFixture>::config_{};

TEST_F(RbacAttachedFixture, GrantsApplyToDuckLakeTables) {
  if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
    GTEST_SKIP() << "license does not include RBAC";
  }
  ASSERT_TRUE(IsServerReady());
  const auto dir = std::filesystem::temp_directory_path() /
                   ("gizmosql_rbac_ducklake_" + std::to_string(getpid()));
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir / "data");

  (void)Admin("INSTALL ducklake");  // already installed is fine
  ASSERT_OK(Admin("LOAD ducklake"));
  ASSERT_OK(Admin("ATTACH 'ducklake:" + (dir / "meta.ducklake").string() +
                  "' AS lake (DATA_PATH '" + (dir / "data").string() + "/')"));
  ASSERT_OK(Admin("CREATE TABLE lake.main.events AS SELECT 1 AS id, 'payload' AS body"));
  ASSERT_OK(Admin("CREATE TABLE lake.main.hidden AS SELECT 1 AS id"));
  ASSERT_OK(Admin("CREATE ROLE lake_reader"));
  ASSERT_OK(Admin("CREATE USER lake_user"));
  ASSERT_OK(Admin("GRANT lake_reader TO lake_user"));
  ASSERT_OK(Admin("GRANT SELECT (id) ON lake.main.events TO lake_reader"));

  ASSERT_OK(User("SELECT id FROM lake.main.events"));
  ASSERT_OK(User("SELECT count(*) FROM lake.events"));
  ExpectBlocked("SELECT body FROM lake.main.events", "no SELECT on column(s) body");
  ExpectBlocked("SELECT * FROM lake.main.hidden",
                "no SELECT privilege on table lake.main.hidden");
  ExpectBlocked("INSERT INTO lake.main.events VALUES (2, 'x')", "no INSERT privilege");
  // Time travel reads the same table: same check.
  ExpectBlocked("SELECT body FROM lake.main.events AT (VERSION => 1)", "body");
#if GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
  // A DuckLake view the administrator made reads with the administrator's
  // rights (DuckLake stores the view's SQL and parses it back: same definition).
  ASSERT_OK(Admin("CREATE VIEW lake.main.hidden_v AS SELECT id FROM lake.main.hidden"));
  ExpectBlocked("SELECT * FROM lake.main.hidden_v", "no SELECT privilege on view");
  ASSERT_OK(Admin("GRANT SELECT ON lake.main.hidden_v TO lake_reader"));
  ASSERT_OK(User("SELECT * FROM lake.main.hidden_v"));
  ExpectBlocked("SELECT * FROM lake.main.hidden",
                "no SELECT privilege on table lake.main.hidden");
#endif

  // Row and masking policies apply to DuckLake tables as well.
  ASSERT_OK(
      Admin("CREATE TABLE lake.main.governed AS SELECT * FROM (VALUES (1, 'EU', 'a@x'), "
            "(2, 'US', 'b@x')) t(id, region, email)"));
  ASSERT_OK(Admin("GRANT SELECT ON lake.main.governed TO lake_reader"));
  ASSERT_OK(
      Admin("CREATE ROW POLICY eu_only ON lake.main.governed USING (region = 'EU')"));
  ASSERT_OK(Admin(
      "CREATE MASKING POLICY lake_mask AS (val VARCHAR) RETURNS VARCHAR -> 'masked'"));
  ASSERT_OK(Admin(
      "ALTER TABLE lake.main.governed MODIFY COLUMN email SET MASKING POLICY lake_mask"));
  auto user_values = [&](const std::string& sql) {
    return ValuesAs(GetPort(), "lake_user", "user", kUserSession, sql);
  };
  ASSERT_OK_AND_ASSIGN(auto ids, user_values("SELECT id FROM lake.main.governed"));
  EXPECT_EQ(ids, std::vector<std::string>{"1"});
  ASSERT_OK_AND_ASSIGN(auto emails, user_values("SELECT email FROM lake.main.governed"));
  EXPECT_EQ(emails, std::vector<std::string>{"masked"});
  ASSERT_OK_AND_ASSIGN(
      auto oracle,
      user_values("SELECT count(*) FROM lake.main.governed WHERE email = 'a@x'"));
  EXPECT_EQ(oracle, std::vector<std::string>{"0"});
  ASSERT_OK(
      Admin("ALTER TABLE lake.main.governed MODIFY COLUMN email UNSET MASKING POLICY"));
  ASSERT_OK(Admin("DROP ROW POLICY eu_only ON lake.main.governed"));
  std::filesystem::remove_all(dir);
}

TEST_F(RbacAttachedFixture, GrantsApplyToPostgresTables) {
  const char* pg = std::getenv("GIZMOSQL_TEST_RBAC_PG_URI");
  if (!pg) GTEST_SKIP() << "GIZMOSQL_TEST_RBAC_PG_URI is not set";
  if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
    GTEST_SKIP() << "license does not include RBAC";
  }
  ASSERT_TRUE(IsServerReady());
  ASSERT_OK(Admin("LOAD postgres"));
  ASSERT_OK(Admin(std::string("ATTACH '") + pg + "' AS pgc (TYPE postgres)"));
  ASSERT_OK(
      Admin("CALL postgres_execute('pgc', 'DROP TABLE IF EXISTS public.rbac_pg_t')"));
  ASSERT_OK(
      Admin("CREATE TABLE pgc.public.rbac_pg_t AS SELECT 1 AS id, 'pg-secret' AS s"));
  ASSERT_OK(Admin("CREATE ROLE IF NOT EXISTS lake_reader"));
  ASSERT_OK(Admin("CREATE USER IF NOT EXISTS lake_user"));
  ASSERT_OK(Admin("GRANT lake_reader TO lake_user"));
  ASSERT_OK(Admin("GRANT SELECT (id) ON pgc.public.rbac_pg_t TO lake_reader"));

  ASSERT_OK(User("SELECT id FROM pgc.public.rbac_pg_t"));
  ExpectBlocked("SELECT s FROM pgc.public.rbac_pg_t", "no SELECT on column(s) s");
  // The pass-through function runs SQL in PostgreSQL directly: refused.
  auto st = User("SELECT * FROM postgres_query('pgc', 'SELECT s FROM public.rbac_pg_t')");
  EXPECT_FALSE(st.ok()) << "postgres_query was allowed";
  ASSERT_OK(
      Admin("CALL postgres_execute('pgc', 'DROP TABLE IF EXISTS public.rbac_pg_t')"));
  ASSERT_OK(Admin("DETACH pgc"));
}
