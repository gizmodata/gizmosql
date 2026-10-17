// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Access-control enforcement on query plans, attacked over Flight SQL.
//
// Every negative test asserts WHY the statement failed (GizmoSQL's refusal and
// the privilege it names), and has a positive twin showing the same user can do
// the legitimate version. Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <map>
#include <string>

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
      .set_id("rbac-enforce-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-enforce-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

// One Flight SQL connection per user, each its own server session.
class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-9000-%012d", n);
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
  arrow::Status ExecutePrepared(arrow::flight::sql::PreparedStatement& stmt) {
    return ExecutePreparedRows(stmt).status();
  }
  arrow::Result<int64_t> ExecutePreparedRows(
      arrow::flight::sql::PreparedStatement& stmt) {
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
  // A Flight metadata call's rows.
  arrow::Result<int64_t> MetadataRows(
      arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> info) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto flight_info, std::move(info));
    int64_t rows = 0;
    for (const auto& ep : flight_info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      rows += table->num_rows();
    }
    return rows;
  }
  FlightSqlClient& flight() { return *client_; }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

class RbacEnforcementFixture
    : public gizmosql::testing::ServerTestFixture<RbacEnforcementFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_enforce_test.db",
        .port = 31710,
        .health_port = 31711,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  void SetUp() override {
    if (!RbacLicensed()) GTEST_SKIP() << "license does not include RBAC";
    ASSERT_TRUE(IsServerReady()) << "RBAC test server did not start";
    static bool seeded = false;
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", "admin", 1);
    if (!seeded) {
      for (const char* sql : {
               "CREATE TABLE orders (id INT, amount INT, customer VARCHAR)",
               "INSERT INTO orders VALUES (1, 10, 'ann'), (2, 20, 'bob')",
               "CREATE TABLE customers (id INT, email VARCHAR, ssn VARCHAR)",
               "INSERT INTO customers VALUES (1, 'ann@x', '111'), (2, 'bob@x', '222')",
               "CREATE TABLE secrets (id INT, value VARCHAR)",
               "INSERT INTO secrets VALUES (1, 'top-secret-value')",
               "CREATE VIEW orders_over_secrets AS SELECT o.id, s.value FROM orders o "
               "JOIN secrets s USING (id)",
               "CREATE ROLE reader",
               "CREATE ROLE colreader",
               "CREATE ROLE writer",
               "CREATE ROLE builder",
               "GRANT SELECT ON orders TO reader",
               "GRANT SELECT (id) ON customers TO colreader",
               "GRANT INSERT, UPDATE, DELETE ON orders TO writer",
               "GRANT CREATE TABLE, CREATE VIEW ON SCHEMA main TO builder",
               "CREATE USER joe",
               "CREATE USER eve",
               "CREATE USER wanda",
               "CREATE USER bob",
               "GRANT reader, colreader TO joe",
               "GRANT writer TO wanda",
               "GRANT builder TO bob",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    joe_ = std::make_unique<Client>(GetPort(), "joe", "user", 2);
    eve_ = std::make_unique<Client>(GetPort(), "eve", "user", 3);
    wanda_ = std::make_unique<Client>(GetPort(), "wanda", "user", 4);
    bob_ = std::make_unique<Client>(GetPort(), "bob", "user", 5);
  }

  // The statement must be refused by the access-control check, mentioning `why`.
  static void ExpectBlocked(Client& who, const std::string& sql,
                            const std::string& why = "") {
    auto st = who.Run(sql).status();
    ASSERT_FALSE(st.ok()) << "NOT BLOCKED: " << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos)
        << sql << " failed, but not by access control: " << st.ToString();
    if (!why.empty()) {
      EXPECT_NE(st.ToString().find(why), std::string::npos)
          << sql << " -> expected '" << why << "' in: " << st.ToString();
    }
  }
  static std::shared_ptr<arrow::Table> ExpectAllowed(Client& who,
                                                     const std::string& sql) {
    auto result = who.Run(sql);
    EXPECT_TRUE(result.ok()) << "WRONGLY BLOCKED: " << sql << " -> "
                             << result.status().ToString();
    return result.ok() ? *result : nullptr;
  }

  std::unique_ptr<Client> admin_, joe_, eve_, wanda_, bob_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacEnforcementFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<RbacEnforcementFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RbacEnforcementFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacEnforcementFixture>::config_{};

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

TEST_F(RbacEnforcementFixture, GrantedReadsWork) {
  auto t = ExpectAllowed(*joe_, "SELECT * FROM orders ORDER BY id");
  ASSERT_TRUE(t);
  EXPECT_EQ(t->num_rows(), 2);
  ExpectAllowed(*joe_, "SELECT id FROM customers");
  ExpectAllowed(*joe_,
                "SELECT count(*) FROM customers");  // any column grant allows count(*)
  ExpectAllowed(*joe_, "SELECT o.id, c.id FROM orders o JOIN customers c ON o.id = c.id");
  ExpectAllowed(*joe_, "SELECT * FROM range(3)");
  ExpectAllowed(*joe_, "SELECT table_name FROM information_schema.tables");
  ExpectAllowed(*admin_, "SELECT * FROM secrets");  // the administrator is unaffected
}

TEST_F(RbacEnforcementFixture, UngrantedTablesAreDenied) {
  ExpectBlocked(*eve_, "SELECT * FROM orders", "no SELECT privilege on table");
  ExpectBlocked(*joe_, "SELECT * FROM secrets", "no SELECT privilege on table");
  ExpectBlocked(*joe_, "SELECT * FROM \"SECRETS\"", "no SELECT");
  ExpectBlocked(*joe_, "SELECT * FROM main.secrets", "no SELECT");
  ExpectBlocked(*joe_, "SELECT count(*) FROM secrets", "no SELECT");
}

TEST_F(RbacEnforcementFixture, ColumnGrantsCoverEveryUseOfAColumn) {
  ExpectBlocked(*joe_, "SELECT email FROM customers", "no SELECT on column(s) email");
  ExpectBlocked(*joe_, "SELECT * FROM customers", "no SELECT on column(s)");
  // Columns used only to filter, sort, group or aggregate are reads too.
  ExpectBlocked(*joe_, "SELECT id FROM customers WHERE ssn = '111'", "ssn");
  ExpectBlocked(*joe_, "SELECT id FROM customers ORDER BY email", "email");
  ExpectBlocked(*joe_, "SELECT max(ssn) FROM customers", "ssn");
  ExpectBlocked(*joe_, "SELECT id, count(*) FROM customers GROUP BY id, email", "email");
  ExpectBlocked(*joe_, "SELECT id FROM customers WHERE length(email) > 100", "email");
}

TEST_F(RbacEnforcementFixture, IndirectReadsAreChecked) {
  const char* kSecret = "no SELECT privilege on table";
  ExpectBlocked(*joe_, "SELECT o.id FROM orders o JOIN secrets s ON o.id = s.id",
                kSecret);
  ExpectBlocked(*joe_, "SELECT * FROM orders WHERE id IN (SELECT id FROM secrets)",
                kSecret);
  ExpectBlocked(*joe_, "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM secrets)",
                kSecret);
  ExpectBlocked(*joe_, "SELECT (SELECT value FROM secrets LIMIT 1) AS v", kSecret);
  ExpectBlocked(*joe_, "WITH x AS (SELECT * FROM secrets) SELECT * FROM x", kSecret);
  ExpectBlocked(*joe_, "WITH x AS MATERIALIZED (SELECT * FROM secrets) SELECT * FROM x",
                kSecret);
  ExpectBlocked(*joe_, "SELECT id FROM orders UNION ALL SELECT id FROM secrets", kSecret);
  ExpectBlocked(*joe_, "SELECT id FROM orders EXCEPT SELECT id FROM secrets", kSecret);
  ExpectBlocked(
      *joe_,
      "SELECT * FROM orders o, LATERAL (SELECT ssn FROM customers c WHERE c.id = o.id)",
      "ssn");
  ExpectBlocked(*joe_,
                "WITH RECURSIVE r(n) AS (SELECT id FROM secrets UNION ALL SELECT n + 1 "
                "FROM r WHERE n < 3) SELECT * FROM r",
                kSecret);
  // SQL from a string is bound inside DuckDB before any check: refused up front.
  ExpectBlocked(*joe_, "SELECT * FROM query('SELECT * FROM secrets')", "query()");
  ExpectBlocked(*joe_, "SELECT * FROM query_table('secrets')", "query_table()");
  ExpectBlocked(*joe_, "SELECT * FROM query('SELECT 1')", "query()");
  ExpectBlocked(*joe_, "SUMMARIZE secrets");
  ExpectBlocked(*joe_, "FROM secrets SELECT value", kSecret);  // FROM-first
}

TEST_F(RbacEnforcementFixture, WrappingAStatementDoesNotHideIt) {
  const char* kSecret = "no SELECT privilege on table";
  ExpectBlocked(*joe_, "EXPLAIN SELECT * FROM secrets", kSecret);
  ExpectBlocked(*joe_, "EXPLAIN ANALYZE SELECT * FROM secrets", kSecret);
  // A batch: the denied statement stops it (DuckDB runs these one by one).
  ExpectBlocked(*joe_, "SELECT 1; SELECT * FROM secrets", kSecret);
  // Client-side prepared statements are checked when prepared, with parameters
  // too (DuckDB 1.x plans those again when they run).
  auto prepared = joe_->Prepare("SELECT * FROM secrets WHERE id = ?");
  ASSERT_FALSE(prepared.ok());
  EXPECT_NE(prepared.status().ToString().find(kSecret), std::string::npos)
      << prepared.status().ToString();
  // Session-local wrappers inline the protected table: still checked.
  ExpectAllowed(*joe_, "CREATE TEMP VIEW tv AS SELECT * FROM secrets");
  ExpectBlocked(*joe_, "SELECT * FROM tv", kSecret);
  // A session's macro would be found before the catalog's functions, inside a
  // view too, so users may not create even temporary ones.
  ExpectBlocked(*joe_, "CREATE TEMP MACRO tm() AS TABLE SELECT * FROM secrets",
                "creating macros needs an administrator");
}

// One INT32 parameter row for a prepared statement.
static std::shared_ptr<arrow::RecordBatch> IntParameter(int32_t value) {
  arrow::Int32Builder builder;
  (void)builder.Append(value);
  auto array = builder.Finish().ValueOrDie();
  return arrow::RecordBatch::Make(arrow::schema({arrow::field("p", arrow::int32())}), 1,
                                  {array});
}

TEST_F(RbacEnforcementFixture,
       PreparedStatementsWithParametersAreCheckedWhenPreparedAndRun) {
  const char* kSecret = "no SELECT privilege on table";
  // Positive twin: a granted table, read with a bound parameter.
  auto ok = joe_->Prepare("SELECT * FROM orders WHERE id = ?");
  ASSERT_TRUE(ok.ok()) << "WRONGLY BLOCKED: " << ok.status().ToString();
  ASSERT_TRUE((*ok)->SetParameters(IntParameter(/*value=*/2)).ok());
  auto rows = joe_->ExecutePreparedRows(**ok);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  EXPECT_EQ(*rows, 1);
  // The same statement on a table joe may not read fails when prepared, before
  // its columns are described.
  auto denied = joe_->Prepare("SELECT value FROM secrets WHERE id = ?");
  ASSERT_FALSE(denied.ok()) << "NOT BLOCKED";
  EXPECT_NE(denied.status().ToString().find(kSecret), std::string::npos)
      << denied.status().ToString();
  // A parameter whose type DuckDB can only learn from the value: no plan when
  // prepared, so the run's plan is the one checked.
  auto open_type = joe_->Prepare("SELECT value FROM secrets WHERE ? IS NOT NULL");
  if (open_type.ok()) {
    ASSERT_TRUE((*open_type)->SetParameters(IntParameter(/*value=*/1)).ok());
    auto st = joe_->ExecutePrepared(**open_type);
    ASSERT_FALSE(st.ok()) << "NOT BLOCKED when run";
    EXPECT_NE(st.ToString().find(kSecret), std::string::npos) << st.ToString();
  } else {
    EXPECT_NE(open_type.status().ToString().find(kSecret), std::string::npos)
        << open_type.status().ToString();
  }
  // Positive twin of that shape.
  auto open_ok = joe_->Prepare("SELECT id FROM orders WHERE ? IS NOT NULL");
  ASSERT_TRUE(open_ok.ok()) << "WRONGLY BLOCKED: " << open_ok.status().ToString();
  ASSERT_TRUE((*open_ok)->SetParameters(IntParameter(/*value=*/1)).ok());
  auto open_rows = joe_->ExecutePreparedRows(**open_ok);
  ASSERT_TRUE(open_rows.ok()) << open_rows.status().ToString();
  EXPECT_EQ(*open_rows, 2);
}

TEST_F(RbacEnforcementFixture, StoreUsersCanBrowseMetadata) {
  auto co = joe_->Options();
  auto catalogs = joe_->MetadataRows(joe_->flight().GetCatalogs(co));
  ASSERT_TRUE(catalogs.ok()) << catalogs.status().ToString();
  EXPECT_GE(*catalogs, 1);
  auto schemas = joe_->MetadataRows(joe_->flight().GetDbSchemas(co, nullptr, nullptr));
  ASSERT_TRUE(schemas.ok()) << schemas.status().ToString();
  EXPECT_GE(*schemas, 1);
  auto tables = joe_->MetadataRows(joe_->flight().GetTables(
      co, nullptr, nullptr, nullptr, /*include_schema=*/false, nullptr));
  ASSERT_TRUE(tables.ok()) << tables.status().ToString();
  EXPECT_GE(*tables, 1);
  auto with_schema = joe_->MetadataRows(joe_->flight().GetTables(
      co, nullptr, nullptr, nullptr, /*include_schema=*/true, nullptr));
  ASSERT_TRUE(with_schema.ok()) << with_schema.status().ToString();
  const std::string orders = "orders";
  auto filtered = joe_->MetadataRows(joe_->flight().GetTables(
      co, nullptr, nullptr, &orders, /*include_schema=*/true, nullptr));
  ASSERT_TRUE(filtered.ok()) << filtered.status().ToString();
  EXPECT_EQ(*filtered, 1);
}

TEST_F(RbacEnforcementFixture, GizmoSqlsMetadataViewsNeedNoGrant) {
  // The views JDBC metadata calls use need no grant, like duckdb_views().
  ExpectAllowed(*joe_, "SELECT * FROM _gizmosql_system.main.gizmosql_index_info");
  ExpectAllowed(*joe_, "SELECT * FROM _gizmosql_system.main.gizmosql_view_definition");
  // Nothing else can be put there to ride on that: the catalog is read-only.
  auto st = admin_->Run("CREATE VIEW _gizmosql_system.main.leak AS SELECT 1").status();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("is read-only"), std::string::npos) << st.ToString();
  // The views list only what joe may list (here: all catalogs, no tenant).
  ExpectBlocked(*joe_, "SELECT * FROM secrets", "no SELECT privilege on table");
}

TEST_F(RbacEnforcementFixture, TheCheckCannotBeSwitchedOff) {
  ExpectBlocked(*joe_, "SET enable_optimizer = false", "changing enable_optimizer");
  ExpectBlocked(*joe_, "SET disabled_optimizers = 'extension'", "disabled_optimizers");
  ExpectBlocked(*joe_, "PRAGMA disable_optimizer");
  // Positive twin: ordinary session settings still work.
  ExpectAllowed(*joe_, "SET search_path = 'main'");
  ExpectAllowed(*joe_, "SET TimeZone = 'UTC'");
  ExpectAllowed(*joe_, "BEGIN TRANSACTION");
  ExpectAllowed(*joe_, "SELECT * FROM orders");
  ExpectAllowed(*joe_, "COMMIT");
}

TEST_F(RbacEnforcementFixture, TableFunctionsAreAllowListed) {
  // Remote files are refused before DuckDB binds them (binding already reaches
  // the network), so no request leaves the server on the user's behalf.
  ExpectBlocked(*joe_, "SELECT * FROM read_parquet('s3://bucket/x.parquet')",
                "files and URLs");
  ExpectBlocked(*joe_, "SELECT * FROM read_csv('https://example.invalid/x.csv')",
                "files and URLs");
  ExpectBlocked(*joe_, "SELECT * FROM 'https://example.invalid/x.parquet'");
  ExpectBlocked(*joe_, "COPY (SELECT * FROM orders) TO 's3://bucket/out.parquet'",
                "files and URLs");
  ExpectAllowed(*joe_, "SELECT * FROM generate_series(1, 3)");
  ExpectAllowed(*joe_, "SELECT * FROM duckdb_tables()");
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

TEST_F(RbacEnforcementFixture, WritesNeedTheirOwnPrivilege) {
  ExpectBlocked(*joe_, "INSERT INTO orders VALUES (9, 9, 'x')", "no INSERT privilege");
  ExpectBlocked(*joe_, "UPDATE orders SET amount = 0", "no UPDATE privilege");
  ExpectBlocked(*joe_, "DELETE FROM orders", "no DELETE privilege");
  // wanda may write but not read.
  ExpectAllowed(*wanda_, "INSERT INTO orders VALUES (3, 30, 'cat')");
  // SET amount = amount reads amount, so it needs SELECT on it (as in Postgres).
  ExpectBlocked(*wanda_, "UPDATE orders SET amount = amount", "no SELECT");
  ExpectBlocked(*wanda_, "SELECT * FROM orders", "no SELECT");
}

TEST_F(RbacEnforcementFixture, WritesThatReadNeedSelect) {
  // Filtering rows by a column reads it (as in Postgres).
  ExpectBlocked(*wanda_, "DELETE FROM orders WHERE customer = 'ann'", "no SELECT");
  ExpectBlocked(*wanda_, "UPDATE orders SET amount = 1 RETURNING *", "no SELECT");
  ExpectBlocked(*wanda_, "INSERT INTO orders VALUES (5, 5, 'e') RETURNING id",
                "no SELECT");
  ExpectBlocked(*wanda_, "INSERT INTO orders SELECT id, 0, value FROM secrets",
                "no SELECT");
  ExpectBlocked(*joe_,
                "MERGE INTO orders USING (SELECT 1 AS id) s ON orders.id = s.id "
                "WHEN MATCHED THEN DELETE");
  // Positive twin: an unfiltered bulk write needs no SELECT.
  ExpectAllowed(*wanda_, "UPDATE orders SET amount = 0");
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
TEST_F(RbacEnforcementFixture, DmlInsideACteIsAWrite) {
  ExpectBlocked(*joe_, "WITH d AS (DELETE FROM orders RETURNING *) SELECT * FROM d",
                "no DELETE privilege");
  ExpectBlocked(*eve_,
                "WITH i AS (INSERT INTO orders VALUES (7, 7, 'g') RETURNING *) SELECT 1",
                "no INSERT privilege");
}
#endif

// ---------------------------------------------------------------------------
// DDL and ownership
// ---------------------------------------------------------------------------

TEST_F(RbacEnforcementFixture, CreatingNeedsThePrivilegeAndMakesTheCreatorOwner) {
  ExpectBlocked(*eve_, "CREATE TABLE eve_table (x INT)",
                "no CREATE TABLE privilege on schema");
  ExpectBlocked(*eve_, "CREATE VIEW eve_view AS SELECT 1", "no CREATE VIEW privilege");
  ExpectAllowed(*bob_, "CREATE TABLE bob_table (x INT)");
  ExpectAllowed(*bob_, "INSERT INTO bob_table VALUES (1)");
  ExpectAllowed(*bob_, "SELECT * FROM bob_table");  // the owner holds every privilege
  ExpectBlocked(*eve_, "SELECT * FROM bob_table", "no SELECT");
  ExpectBlocked(*eve_, "DROP TABLE bob_table", "may not drop table");
  ExpectBlocked(*eve_, "ALTER TABLE bob_table RENAME TO stolen", "may not alter table");
  ExpectAllowed(*bob_, "DROP TABLE bob_table");
}

TEST_F(RbacEnforcementFixture, RevokingStopsAStatementThatIsRunning) {
  ExpectAllowed(*admin_, "CREATE ROLE runner");
  ExpectAllowed(*admin_, "CREATE USER rob");
  ExpectAllowed(*admin_, "GRANT runner TO rob");
  ExpectAllowed(*admin_, "GRANT SELECT ON orders TO runner");
  Client rob(GetPort(), "rob", "user", 30);
  Client rob_runner(GetPort(), "rob", "user", 31);
  auto running = std::async(std::launch::async, [&] {
    return rob_runner
        .Run("SELECT sum(o.id * r.range) FROM orders o, range(1000000000000) r")
        .status();
  });
  // If the test fails while the statement still runs, end it so the test can.
  struct EndIfStillRunning {
    Client& admin;
    std::future<arrow::Status>& f;
    ~EndIfStillRunning() {
      if (f.valid() && f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        (void)admin.Run("KILL SESSION '00000000-0000-4000-9000-000000000031'");
        f.wait_for(std::chrono::seconds(60));
      }
    }
  } end_if_still_running{*admin_, running};
  std::this_thread::sleep_for(std::chrono::seconds(1));
  // Gaining a right leaves it running...
  ExpectAllowed(*admin_, "GRANT SELECT (id) ON customers TO runner");
  ASSERT_EQ(running.wait_for(std::chrono::seconds(2)), std::future_status::timeout)
      << "the statement stopped on a GRANT: "
      << (running.valid() ? running.get().ToString() : "");
  // ...losing one stops it.
  ExpectAllowed(*admin_, "REVOKE SELECT ON orders FROM runner");
  ASSERT_EQ(running.wait_for(std::chrono::seconds(30)), std::future_status::ready)
      << "a running statement kept going after its user lost access";
  const auto st = running.get();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("nterrupt"), std::string::npos) << st.ToString();
  // Both sessions live on; their next statements are checked against the new grants.
  ExpectAllowed(rob, "SELECT 1");
  ExpectAllowed(rob_runner, "SELECT 1");
  ExpectBlocked(rob_runner, "SELECT * FROM orders", "no SELECT privilege");
  ExpectAllowed(rob, "SELECT id FROM customers");
}

TEST_F(RbacEnforcementFixture, AnOwnerSharesWhatItCreated) {
  ExpectAllowed(*bob_, "CREATE TABLE bob_shared AS SELECT 1 AS id, 'b' AS note");
  ExpectBlocked(*joe_, "SELECT * FROM bob_shared", "no SELECT privilege on table");
  ExpectAllowed(*bob_, "GRANT SELECT (id) ON bob_shared TO reader");
  ExpectAllowed(*joe_, "SELECT id FROM bob_shared");
  ExpectBlocked(*joe_, "SELECT note FROM bob_shared", "no SELECT on column(s) note");
  // Not on what bob does not own.
  auto st = bob_->Run("GRANT SELECT ON secrets TO builder").status();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("may not grant SELECT"), std::string::npos)
      << st.ToString();
  ExpectAllowed(*bob_, "REVOKE SELECT (id) ON bob_shared FROM reader");
  ExpectBlocked(*joe_, "SELECT id FROM bob_shared", "no SELECT");
}

TEST_F(RbacEnforcementFixture, ASchemasCreatorOwnsIt) {
  ExpectAllowed(*admin_, "CREATE ROLE IF NOT EXISTS schema_maker");
  ExpectAllowed(*admin_,
                "GRANT CREATE SCHEMA ON CATALOG rbac_enforce_test TO schema_maker");
  ExpectAllowed(*admin_, "GRANT schema_maker TO bob");
  ExpectBlocked(*joe_, "CREATE SCHEMA joes", "no CREATE SCHEMA privilege");
  ExpectAllowed(*bob_, "CREATE SCHEMA bobs");
  // The owner of a schema may create in it and drop it; nobody else may.
  ExpectAllowed(*bob_, "CREATE TABLE bobs.t AS SELECT 1 AS x");
  ExpectAllowed(*bob_, "SELECT * FROM bobs.t");
  // However the schema is named, another user may not drop it.
  for (const char* sql :
       {"DROP SCHEMA bobs CASCADE", "DROP SCHEMA IF EXISTS bobs CASCADE",
        "DROP SCHEMA BOBS CASCADE", "DROP SCHEMA rbac_enforce_test.bobs CASCADE",
        "DROP SCHEMA IF EXISTS rbac_enforce_test.bobs CASCADE"}) {
    ExpectBlocked(*joe_, sql, "may not drop schema");
  }
  ExpectAllowed(*bob_, "SELECT * FROM bobs.t");  // still there
  ExpectAllowed(*bob_, "DROP TABLE bobs.t");
  ExpectAllowed(*bob_, "DROP SCHEMA bobs");
  ExpectAllowed(*admin_, "REVOKE schema_maker FROM bob");
}

TEST_F(RbacEnforcementFixture, DropIfExistsOfAnotherOwnersObjectIsRefused) {
  for (const char* sql :
       {"DROP TABLE IF EXISTS orders", "DROP TABLE IF EXISTS main.ORDERS",
        "DROP TABLE IF EXISTS rbac_enforce_test.main.orders",
        "DROP VIEW IF EXISTS orders_over_secrets",
        "DROP SCHEMA IF EXISTS main CASCADE"}) {
    ExpectBlocked(*joe_, sql, "may not drop");
  }
  ExpectAllowed(*joe_, "SELECT count(*) FROM orders");         // still there
  ExpectAllowed(*joe_, "DROP TABLE IF EXISTS no_such_table");  // nothing to drop
}

TEST_F(RbacEnforcementFixture, ReplacingOrIgnoringAnotherOwnersObject) {
  // bob may create tables but does not own orders.
  ExpectBlocked(*bob_, "CREATE OR REPLACE TABLE orders AS SELECT 1 AS id",
                "may not drop table");
  // IF NOT EXISTS on an existing table changes nothing and grants nothing.
  ExpectAllowed(*bob_, "CREATE TABLE IF NOT EXISTS orders (id INT)");
  ExpectBlocked(*bob_, "SELECT * FROM orders", "no SELECT");
  ExpectBlocked(*bob_, "DROP TABLE orders", "may not drop table");
}

TEST_F(RbacEnforcementFixture, AdministrativeStatementsStayDenied) {
  ExpectBlocked(*joe_, "CREATE SEQUENCE seq1", "needs an administrator");
  ExpectBlocked(*joe_, "CREATE MACRO m1(a) AS a + 1", "needs an administrator");
  // SQL-level PREPARE is checked on the statement it prepares.
  ExpectBlocked(*joe_, "PREPARE p_secret AS SELECT * FROM secrets", "no SELECT");
  // Temporary objects are the session's own.
  ExpectAllowed(*joe_, "CREATE TEMP TABLE scratch (x INT)");
  ExpectAllowed(*joe_, "INSERT INTO scratch VALUES (1)");
  ExpectAllowed(*joe_, "SELECT * FROM scratch");
}

// ---------------------------------------------------------------------------
// Views (this release: a view reads with the caller's rights)
// ---------------------------------------------------------------------------

// Views read with their owner's rights (test_rbac_views.cpp has the rest): the
// administrator's view publishes what it selects once the view is granted,
// while the table behind it stays closed.
TEST_F(RbacEnforcementFixture, ViewsReadWithTheirOwnersRights) {
  ExpectBlocked(*joe_, "SELECT * FROM orders_over_secrets",
                "no SELECT privilege on view");
  ExpectAllowed(*admin_, "GRANT SELECT ON orders_over_secrets TO reader");
  auto t = ExpectAllowed(*joe_, "SELECT * FROM orders_over_secrets");
  ASSERT_TRUE(t);
  EXPECT_EQ(t->num_rows(), 1);
  ExpectBlocked(*joe_, "SELECT * FROM secrets", "no SELECT privilege on table");
  ExpectAllowed(*admin_, "REVOKE SELECT ON orders_over_secrets FROM reader");
}

// ---------------------------------------------------------------------------
// Revocation
// ---------------------------------------------------------------------------

TEST_F(RbacEnforcementFixture, RevokingStopsTheNextStatementAndPreparedOnes) {
  ExpectAllowed(*admin_, "CREATE ROLE temp_reader");
  ExpectAllowed(*admin_, "CREATE USER IF NOT EXISTS rita");
  ExpectAllowed(*admin_, "GRANT SELECT ON orders TO temp_reader");
  ExpectAllowed(*admin_, "GRANT temp_reader TO rita");
  Client rita(GetPort(), "rita", "user", 6);
  ExpectAllowed(rita, "SELECT * FROM orders");
  auto prepared = rita.Prepare("SELECT * FROM orders");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_OK(rita.ExecutePrepared(**prepared));

  ExpectAllowed(*admin_, "REVOKE SELECT ON orders FROM temp_reader");
  ExpectBlocked(rita, "SELECT * FROM orders", "no SELECT");
  auto st = rita.ExecutePrepared(**prepared);
  ASSERT_FALSE(st.ok()) << "a statement prepared before the REVOKE still ran";
  EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos) << st.ToString();
}
