// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Views under access control read with their owner's rights, attacked over
// Flight SQL. The caller needs SELECT on the view; what the view reads is
// checked against the view's owner, and anything the statement names itself is
// still checked against the caller.
//
// Every negative test asserts WHY the statement failed, and has a positive twin.
// Needs a license that includes RBAC, and DuckDB 1.5 or later (on DuckDB 1.4 a
// view's body is checked with the caller's rights, and these tests skip).

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
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

bool RbacLicensed() {
  return gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable();
}

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-views-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-views-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

// One Flight SQL connection per user; `n` picks the server session.
class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-8000-%012d", n);
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  arrow::flight::FlightCallOptions Options() const {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
    return co;
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(co, sql));
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      tables.push_back(table);
    }
    if (tables.empty()) {
      ARROW_ASSIGN_OR_RAISE(auto schema, info->GetSchema(nullptr));
      return arrow::Table::MakeEmpty(schema);
    }
    return arrow::ConcatenateTables(tables);
  }
  arrow::Result<std::shared_ptr<arrow::flight::sql::PreparedStatement>> Prepare(
      const std::string& sql) {
    return client_->Prepare(Options(), sql);
  }
  arrow::Result<int64_t> ExecutePrepared(arrow::flight::sql::PreparedStatement& stmt) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto info, stmt.Execute(co));
    int64_t rows = 0;
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      rows += table->num_rows();
    }
    return rows;
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

std::string ParquetPath() {
  return (std::filesystem::temp_directory_path() /
          ("gizmosql_rbac_views_" + std::to_string(getpid()) + ".parquet"))
      .string();
}

class RbacViewsFixture : public gizmosql::testing::ServerTestFixture<RbacViewsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_views_test.db",
        .port = 31730,
        .health_port = 31731,
        .username = "admin_user",
        .password = "admin_pass",
        // Runs as GizmoSQL itself, so this view gets no owner record: it stands
        // for a view made before access control was turned on.
        .init_sql_commands =
            "CREATE SCHEMA IF NOT EXISTS legacy; "
            "CREATE OR REPLACE TABLE legacy.base AS SELECT 1 AS id, 'legacy-secret' AS "
            "s; "
            "CREATE OR REPLACE VIEW legacy.v AS SELECT id, s FROM legacy.base;",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  void SetUp() override {
    if (!RbacLicensed()) GTEST_SKIP() << "license does not include RBAC";
#if !GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
    GTEST_SKIP()
        << "this DuckDB has no view-binding hook; views read with the caller's rights";
#endif
    ASSERT_TRUE(IsServerReady()) << "RBAC test server did not start";
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      const std::string parquet = ParquetPath();
      for (const std::string& sql : {
               std::string("CREATE TABLE raw_orders (id INT, amount INT, customer "
                           "VARCHAR, ssn VARCHAR)"),
               std::string("INSERT INTO raw_orders VALUES (1, 10, 'ann', '111'), (2, 20, "
                           "'bob', '222'), "
                           "(3, 30, 'cara', '333')"),
               std::string("CREATE TABLE secrets (id INT, value VARCHAR)"),
               std::string("INSERT INTO secrets VALUES (1, 'top-secret-value')"),
               std::string("CREATE TABLE allowed (tenant VARCHAR)"),
               std::string("INSERT INTO allowed VALUES ('ann')"),
               "COPY (SELECT 42 AS answer) TO '" + parquet + "' (FORMAT parquet)",
               std::string("CREATE SCHEMA curated"),
               std::string("CREATE VIEW curated.orders_v AS SELECT id, amount, customer "
                           "FROM main.raw_orders"),
               std::string(
                   "CREATE VIEW curated.ssn_v AS SELECT id, ssn FROM main.raw_orders"),
               std::string("CREATE VIEW curated.nested_v AS SELECT id, amount FROM "
                           "curated.orders_v"),
               // `allowed` is unqualified on purpose: a session's temporary
               // table of that name would be found first.
               std::string("CREATE VIEW curated.tenant_v AS SELECT id, customer FROM "
                           "main.raw_orders "
                           "WHERE customer IN (SELECT tenant FROM allowed)"),
               "CREATE VIEW curated.file_v AS SELECT * FROM read_parquet('" + parquet +
                   "')",
               std::string("CREATE ROLE analyst"),
               std::string("CREATE ROLE builder"),
               std::string("CREATE ROLE publisher"),
               std::string("GRANT SELECT ON curated.orders_v TO analyst"),
               std::string("GRANT SELECT ON curated.nested_v TO analyst"),
               std::string("GRANT SELECT ON curated.tenant_v TO analyst"),
               std::string("GRANT SELECT ON curated.file_v TO analyst"),
               std::string("GRANT SELECT (id) ON curated.ssn_v TO analyst"),
               std::string("GRANT CREATE VIEW ON SCHEMA main TO builder"),
               std::string("GRANT SELECT (id, amount) ON main.raw_orders TO builder"),
               std::string("CREATE USER ann"),
               std::string("CREATE USER bob"),
               std::string("CREATE USER dan"),
               std::string("GRANT analyst TO ann"),
               std::string("GRANT builder TO bob"),
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    ann_ = std::make_unique<Client>(GetPort(), "ann", "user", 2);
    bob_ = std::make_unique<Client>(GetPort(), "bob", "user", 3);
    dan_ = std::make_unique<Client>(GetPort(), "dan", "user", 4);
  }

  // The statement must be refused by the access-control check, mentioning `why`.
  static std::string ExpectBlocked(Client& who, const std::string& sql,
                                   const std::string& why) {
    auto st = who.Run(sql).status();
    EXPECT_FALSE(st.ok()) << "NOT BLOCKED: " << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos)
        << sql << " failed, but not by access control: " << st.ToString();
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << " -> expected '" << why << "' in: " << st.ToString();
    return st.ToString();
  }
  static std::shared_ptr<arrow::Table> ExpectAllowed(Client& who,
                                                     const std::string& sql) {
    auto result = who.Run(sql);
    EXPECT_TRUE(result.ok()) << "WRONGLY BLOCKED: " << sql << " -> "
                             << result.status().ToString();
    return result.ok() ? *result : nullptr;
  }
  static int64_t Rows(const std::shared_ptr<arrow::Table>& t) {
    return t ? t->num_rows() : -1;
  }

  std::unique_ptr<Client> admin_, ann_, bob_, dan_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacViewsFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacViewsFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacViewsFixture>::server_ready_{
    false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacViewsFixture>::config_{};

TEST_F(RbacViewsFixture, AnAdministratorsViewPublishesWhatItSelects) {
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM curated.orders_v")), 3);
  EXPECT_EQ(Rows(ExpectAllowed(
                *ann_, "SELECT customer, sum(amount) FROM curated.orders_v GROUP BY 1")),
            3);
  // BI tools wrap everything in a subquery.
  EXPECT_EQ(Rows(ExpectAllowed(
                *ann_, "SELECT * FROM (SELECT * FROM curated.orders_v) t LIMIT 100")),
            3);
  ExpectBlocked(*ann_, "SELECT * FROM raw_orders", "no SELECT privilege on table");
}

TEST_F(RbacViewsFixture, TheCallerStillNeedsSelectOnTheView) {
  ExpectBlocked(*dan_, "SELECT * FROM curated.orders_v", "no SELECT privilege on view");
  ExpectBlocked(*dan_, "SELECT * FROM (SELECT id FROM curated.orders_v) t",
                "no SELECT privilege on view");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT id FROM curated.orders_v")), 3);
}

TEST_F(RbacViewsFixture, NestedViewsCheckEachHopAgainstItsOwner) {
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM curated.nested_v")), 3);
  // nested_v is readable without SELECT on orders_v, which it reads...
  ExpectAllowed(*admin_, "REVOKE SELECT ON curated.orders_v FROM analyst");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM curated.nested_v")), 3);
  // ...but naming orders_v directly is the caller's own reference.
  ExpectBlocked(*ann_, "SELECT * FROM curated.orders_v", "no SELECT privilege on view");
  ExpectAllowed(*admin_, "GRANT SELECT ON curated.orders_v TO analyst");
  ExpectBlocked(*bob_, "SELECT * FROM curated.nested_v", "no SELECT privilege on view");
}

TEST_F(RbacViewsFixture, ColumnGrantsOnAViewCoverEveryUseOfAColumn) {
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT id FROM curated.ssn_v")), 3);
  ExpectAllowed(*ann_, "SELECT count(*) FROM curated.ssn_v");
  const char* kSsn = "no SELECT on column(s) ssn";
  ExpectBlocked(*ann_, "SELECT ssn FROM curated.ssn_v", kSsn);
  ExpectBlocked(*ann_, "SELECT * FROM curated.ssn_v", kSsn);
  ExpectBlocked(*ann_, "SELECT id FROM curated.ssn_v WHERE ssn = '111'", kSsn);
  ExpectBlocked(*ann_, "SELECT id FROM curated.ssn_v ORDER BY ssn", kSsn);
  ExpectBlocked(*ann_, "SELECT id, length(ssn) FROM curated.ssn_v", kSsn);
  ExpectBlocked(*ann_, "SELECT * FROM (SELECT ssn FROM curated.ssn_v) t", kSsn);
  ExpectBlocked(
      *ann_, "SELECT id FROM curated.ssn_v UNION ALL SELECT ssn::INT FROM curated.ssn_v",
      kSsn);
  ExpectBlocked(*ann_,
                "SELECT a.id FROM curated.ssn_v a JOIN curated.ssn_v b ON a.ssn = b.ssn",
                kSsn);
  ExpectBlocked(*ann_, "SELECT max(ssn) OVER () FROM curated.ssn_v", kSsn);
  // Renaming the view's columns in FROM does not rename the grant: "id" here
  // is the view's ssn column.
  ExpectBlocked(*ann_, "SELECT id FROM curated.ssn_v AS x(other, id)", kSsn);
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT renamed FROM curated.ssn_v AS x(renamed)")),
            3);
}

TEST_F(RbacViewsFixture, ADirectReferenceBesideAViewIsCheckedAgainstTheCaller) {
  const char* kRaw = "no SELECT privilege on table";
  ExpectBlocked(*ann_, "SELECT * FROM curated.orders_v v JOIN raw_orders r USING (id)",
                kRaw);
  ExpectBlocked(
      *ann_, "SELECT (SELECT max(ssn) FROM raw_orders), * FROM curated.orders_v", kRaw);
  ExpectBlocked(
      *ann_, "SELECT * FROM curated.orders_v WHERE id IN (SELECT id FROM secrets)", kRaw);
  ExpectBlocked(*ann_,
                "WITH r AS (SELECT * FROM raw_orders) SELECT * FROM curated.orders_v, r",
                kRaw);
  ExpectBlocked(
      *ann_,
      "SELECT * FROM curated.orders_v UNION ALL SELECT id, amount, ssn FROM raw_orders",
      kRaw);
  EXPECT_EQ(
      Rows(ExpectAllowed(
          *ann_, "SELECT * FROM curated.orders_v v JOIN curated.nested_v n USING (id)")),
      3);
}

TEST_F(RbacViewsFixture, AViewCannotReachWhatItsOwnerCannot) {
  // bob may create views in main and read two columns of raw_orders.
  ExpectAllowed(*bob_, "CREATE VIEW main.bob_ok AS SELECT id, amount FROM raw_orders");
  ExpectAllowed(*bob_, "CREATE VIEW main.bob_ssn AS SELECT id, ssn FROM raw_orders");
  ExpectAllowed(*bob_, "CREATE VIEW main.bob_secret AS SELECT * FROM secrets");
  EXPECT_EQ(Rows(ExpectAllowed(*bob_, "SELECT * FROM main.bob_ok")), 3);  // he owns it
  const char* kOwner = "its owner lacks a privilege on an object the view reads";
  ExpectBlocked(*bob_, "SELECT * FROM main.bob_ssn", kOwner);
  auto message = ExpectBlocked(*bob_, "SELECT * FROM main.bob_secret", kOwner);
  EXPECT_EQ(message.find("secrets"), std::string::npos)
      << "the refusal named what the view reads: " << message;

  // Granting the view does not lend the reader more than the owner has.
  ExpectAllowed(*admin_, "GRANT SELECT ON main.bob_ok TO analyst");
  ExpectAllowed(*admin_, "GRANT SELECT ON main.bob_secret TO analyst");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM main.bob_ok")), 3);
  ExpectBlocked(*ann_, "SELECT * FROM main.bob_secret", kOwner);
  ExpectBlocked(*ann_, "SELECT * FROM raw_orders", "no SELECT privilege on table");
}

TEST_F(RbacViewsFixture, TheOwnersCurrentRightsApply) {
  (void)bob_->Run("CREATE VIEW main.bob_now AS SELECT id, amount FROM raw_orders");
  ExpectAllowed(*admin_, "GRANT SELECT ON main.bob_now TO analyst");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM main.bob_now")), 3);
  auto prepared = ann_->Prepare("SELECT * FROM main.bob_now");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_OK_AND_ASSIGN(auto rows, ann_->ExecutePrepared(**prepared));
  EXPECT_EQ(rows, 3);

  ExpectAllowed(*admin_, "REVOKE SELECT (amount) ON main.raw_orders FROM builder");
  ExpectBlocked(*ann_, "SELECT * FROM main.bob_now", "its owner lacks a privilege");
  auto st = ann_->ExecutePrepared(**prepared).status();
  ASSERT_FALSE(st.ok()) << "a statement prepared before the REVOKE still ran";
  EXPECT_NE(st.ToString().find("its owner lacks a privilege"), std::string::npos)
      << st.ToString();
  ExpectAllowed(*admin_, "GRANT SELECT (amount) ON main.raw_orders TO builder");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM main.bob_now")), 3);
}

TEST_F(RbacViewsFixture, ADisabledOwnerClosesTheView) {
  (void)bob_->Run("CREATE VIEW main.bob_disabled AS SELECT id FROM raw_orders");
  ExpectAllowed(*admin_, "GRANT SELECT ON main.bob_disabled TO analyst");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM main.bob_disabled")), 3);
  ExpectAllowed(*admin_, "ALTER USER bob DISABLE");
  ExpectBlocked(*ann_, "SELECT * FROM main.bob_disabled",
                "its owner is disabled or was dropped");
  ExpectAllowed(*admin_, "ALTER USER bob ENABLE");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM main.bob_disabled")), 3);
}

TEST_F(RbacViewsFixture, AViewWithoutAnOwnerReadsWithTheCallersRights) {
  ExpectAllowed(*admin_, "GRANT SELECT ON legacy.v TO analyst");
  ExpectBlocked(*ann_, "SELECT * FROM legacy.v",
                "which has no owner, so it reads with your rights");
  // The fix for a view made before access control: give it an owner.
  ExpectAllowed(*admin_, "GRANT SELECT ON legacy.base TO publisher");
  ExpectAllowed(*admin_, "ALTER VIEW legacy.v OWNER TO publisher");
  auto t = ExpectAllowed(*ann_, "SELECT * FROM legacy.v");
  EXPECT_EQ(Rows(t), 1);
  ExpectBlocked(*ann_, "SELECT * FROM legacy.base", "no SELECT privilege on table");
  // A role owner's rights are the role's, checked at query time.
  ExpectAllowed(*admin_, "REVOKE SELECT ON legacy.base FROM publisher");
  ExpectBlocked(*ann_, "SELECT * FROM legacy.v", "its owner lacks a privilege");
  // The administrator can take it back, under its own name.
  ExpectAllowed(*admin_, "ALTER VIEW legacy.v OWNER TO admin_user");
  EXPECT_EQ(Rows(ExpectAllowed(*ann_, "SELECT * FROM legacy.v")), 1);
}

TEST_F(RbacViewsFixture, ASessionsTemporaryObjectsCannotStandInForWhatAViewReads) {
  Client ann_t(GetPort(), "ann", "user", 20);
  auto t = ExpectAllowed(ann_t, "SELECT customer FROM curated.tenant_v");
  EXPECT_EQ(Rows(t), 1);  // the view's own list allows ann only
  ExpectAllowed(
      ann_t,
      "CREATE TEMP TABLE allowed AS SELECT 'bob' AS tenant UNION ALL SELECT 'cara'");
  EXPECT_EQ(Rows(ExpectAllowed(ann_t, "SELECT * FROM allowed")),
            2);  // her own table is hers to read
  ExpectBlocked(ann_t, "SELECT customer FROM curated.tenant_v",
                "cannot read a temporary table of this session");
  ExpectAllowed(ann_t, "DROP TABLE temp.main.allowed");
  EXPECT_EQ(Rows(ExpectAllowed(ann_t, "SELECT customer FROM curated.tenant_v")), 1);

  Client ann_v(GetPort(), "ann", "user", 21);
  ExpectAllowed(ann_v, "CREATE TEMP VIEW allowed AS SELECT 'bob' AS tenant");
  ExpectBlocked(ann_v, "SELECT customer FROM curated.tenant_v",
                "cannot read a temporary view of this session");
}

TEST_F(RbacViewsFixture, SessionMacrosAndVariablesAreRefused) {
  // A session's macro is found before the catalog's functions, also inside a
  // view: it could make an administrator's view compute something else.
  ExpectBlocked(*ann_, "CREATE TEMP MACRO lower(x) AS (SELECT max(value) FROM secrets)",
                "creating macros needs an administrator");
  ExpectBlocked(*ann_, "CREATE TEMP TYPE mood AS ENUM ('a', 'b')",
                "creating types needs an administrator");
  // SET VARIABLE runs a query of its own, and getvariable() hands the value to
  // any view that asks.
  ExpectBlocked(*ann_, "SET VARIABLE leak = (SELECT max(value) FROM secrets)",
                "SET VARIABLE is not available");
  ExpectBlocked(*ann_, "SET VARIABLE x = 1", "SET VARIABLE is not available");
  auto t = ExpectAllowed(*ann_, "SELECT lower('A') AS l, getvariable('leak') AS v");
  ASSERT_TRUE(t);
  EXPECT_EQ(t->GetColumnByName("l")->GetScalar(0).ValueOrDie()->ToString(), "a");
  EXPECT_FALSE(t->GetColumnByName("v")->GetScalar(0).ValueOrDie()->is_valid);
}

TEST_F(RbacViewsFixture, AnAdministratorsViewMayReadAFile) {
  auto t = ExpectAllowed(*ann_, "SELECT answer FROM curated.file_v");
  EXPECT_EQ(Rows(t), 1);
  auto st = ann_->Run("SELECT * FROM read_parquet('" + ParquetPath() + "')").status();
  ASSERT_FALSE(st.ok()) << "a user read a file directly";
  EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos) << st.ToString();
}

TEST_F(RbacViewsFixture, ARefusalIsAlwaysRecorded) {
  // The server logs a structured record of every access-control refusal, also
  // when queries are not logged (this server does not log them).
  ::testing::internal::CaptureStderr();
  ExpectBlocked(*dan_, "SELECT * FROM curated.orders_v", "no SELECT privilege on view");
  const std::string log = ::testing::internal::GetCapturedStderr();
  // (The log format, JSON or text, is whatever earlier tests left set.)
  const auto record = log.find("Access control refused a statement");
  ASSERT_NE(record, std::string::npos) << log;
  const std::string line = log.substr(log.rfind('\n', record) + 1,
                                      log.find('\n', record) - log.rfind('\n', record));
  EXPECT_NE(line.find("denied"), std::string::npos) << line;
  EXPECT_NE(line.find("dan"), std::string::npos) << line;

  // So is what an allowed statement read, including through a view's owner.
  ::testing::internal::CaptureStderr();
  ExpectAllowed(*ann_, "SELECT id FROM curated.nested_v");
  const std::string allowed = ::testing::internal::GetCapturedStderr();
  EXPECT_NE(allowed.find("Access control allowed a statement"), std::string::npos)
      << allowed;
  EXPECT_NE(allowed.find("SELECT on view rbac_views_test.curated.nested_v (id)"),
            std::string::npos)
      << allowed;
  EXPECT_NE(allowed.find("as the owner of view rbac_views_test.curated.nested_v"),
            std::string::npos)
      << allowed;
}
