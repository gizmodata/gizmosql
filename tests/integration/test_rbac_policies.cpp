// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Row policies and column masking (RBAC 1.2), attacked over Flight SQL.
// Rows a policy hides stay hidden however the statement is spelled; a masked
// column is masked wherever it is used (filters, joins, grouping, sorting,
// copies), so it cannot be used as an oracle for the real value. Needs a
// license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <future>
#include <thread>
#include <set>
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

std::string MintToken(const std::string& username, const std::string& role,
                      const std::string& session_id) {
  return jwt::create()
      .set_issuer("gizmosql")
      .set_type("JWT")
      .set_id("rbac-policies-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-policies-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-5000-%012d", n);
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
  arrow::Result<std::shared_ptr<arrow::Table>> ExecutePrepared(
      arrow::flight::sql::PreparedStatement& stmt) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto info, stmt.Execute(co));
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto& ep : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(co, ep.ticket));
      ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
      tables.push_back(table);
    }
    return arrow::ConcatenateTables(tables);
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

class RbacPoliciesFixture
    : public gizmosql::testing::ServerTestFixture<RbacPoliciesFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_policies_test.db",
        .port = 31760,
        .health_port = 31761,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }

 protected:
  void SetUp() override {
    if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsRbacAvailable()) {
      GTEST_SKIP() << "license does not include RBAC";
    }
    ASSERT_TRUE(IsServerReady());
    admin_ = std::make_unique<Client>(GetPort(), "admin_user", "admin", 1);
    static bool seeded = false;
    if (!seeded) {
      for (const char* sql : {
               "CREATE TABLE emp (id INT, name VARCHAR, region VARCHAR, salary INT, "
               "email VARCHAR, "
               "ssn VARCHAR)",
               "INSERT INTO emp VALUES (1, 'ann', 'EU', 100, 'ann@x.com', '111'), "
               "(2, 'bob', 'US', 200, 'bob@x.com', '222'), (3, 'cat', 'EU', 300, "
               "'cat@x.com', '333'), "
               "(4, 'dan', 'APAC', 400, 'dan@x.com', '444')",
               "CREATE TABLE notes (owner VARCHAR, note VARCHAR)",
               "INSERT INTO notes VALUES ('ann', 'ann-note'), ('bob', 'bob-note'), "
               "('duckdb', 'nobody')",
               "CREATE VIEW emp_v AS SELECT * FROM emp",
               "CREATE ROLE staff",
               "CREATE ROLE hr",
               "CREATE ROLE auditors",
               "GRANT SELECT ON emp TO staff",
               "GRANT SELECT ON emp TO auditors",
               "GRANT SELECT ON emp_v TO staff",
               "GRANT SELECT ON notes TO staff",
               "GRANT DELETE ON emp TO staff",
               "GRANT INSERT, UPDATE ON emp TO staff",
               "GRANT CREATE TABLE ON SCHEMA main TO staff",
               "CREATE USER ann",
               "CREATE USER bob",
               "CREATE USER hal",
               "CREATE USER dan",
               "CREATE USER aud",
               "GRANT staff TO ann",
               "GRANT staff TO bob",
               "GRANT staff TO hal",
               "GRANT hr TO hal",
               "GRANT staff TO dan",
               "GRANT auditors TO aud",
               "ALTER USER ann SET ATTRIBUTE region = 'EU'",
               "ALTER USER bob SET ATTRIBUTE region = 'US'",
               "ALTER USER hal SET ATTRIBUTE region = 'US'",
               "CREATE ROW POLICY region_rows ON emp TO staff USING (region = "
               "session_attribute('region'))",
               "CREATE ROW POLICY hr_all ON emp TO hr USING (true)",
               "CREATE ROW POLICY not_apac ON emp AS RESTRICTIVE USING (region <> "
               "'APAC')",
               "CREATE ROW POLICY own_notes ON notes USING (owner = current_user)",
               "CREATE MASKING POLICY email_mask AS (val VARCHAR) RETURNS VARCHAR -> "
               "CASE WHEN is_role_active('hr') THEN val ELSE regexp_replace(val, '.+@', "
               "'****@') END",
               "ALTER TABLE emp MODIFY COLUMN email SET MASKING POLICY email_mask",
               "CREATE MASKING POLICY hide AS (val VARCHAR) RETURNS VARCHAR -> 'XXX'",
               "ALTER TABLE emp MODIFY COLUMN ssn SET MASKING POLICY hide EXEMPT ROLE hr",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    ann_ = std::make_unique<Client>(GetPort(), "ann", "user", 2);
    bob_ = std::make_unique<Client>(GetPort(), "bob", "user", 3);
    hal_ = std::make_unique<Client>(GetPort(), "hal", "user", 4);
    dan_ = std::make_unique<Client>(GetPort(), "dan", "user", 5);
    aud_ = std::make_unique<Client>(GetPort(), "aud", "user", 6);
  }

  // The first column of the result, as strings, sorted.
  static std::multiset<std::string> Values(Client& who, const std::string& sql) {
    auto r = who.Run(sql);
    EXPECT_TRUE(r.ok()) << sql << " -> " << r.status().ToString();
    std::multiset<std::string> out;
    if (!r.ok()) return out;
    for (int64_t i = 0; i < (*r)->num_rows(); ++i) {
      out.insert((*r)->column(0)->GetScalar(i).ValueOrDie()->ToString());
    }
    return out;
  }
  static std::string One(Client& who, const std::string& sql) {
    auto v = Values(who, sql);
    EXPECT_EQ(v.size(), 1u) << sql;
    return v.empty() ? "" : *v.begin();
  }
  static void ExpectBlocked(Client& who, const std::string& sql, const std::string& why) {
    auto st = who.Run(sql).status();
    ASSERT_FALSE(st.ok()) << "NOT BLOCKED: " << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos) << st.ToString();
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << " -> " << st.ToString();
  }
  std::string AdminError(const std::string& sql) {
    auto st = admin_->Run(sql).status();
    return st.ok() ? "" : st.ToString();
  }

  using S = std::multiset<std::string>;
  std::unique_ptr<Client> admin_, ann_, bob_, hal_, dan_, aud_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacPoliciesFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacPoliciesFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RbacPoliciesFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacPoliciesFixture>::config_{};

TEST_F(RbacPoliciesFixture, EachUserSeesTheRowsTheirPoliciesAllow) {
  EXPECT_EQ(Values(*ann_, "SELECT id FROM emp"), (S{"1", "3"}));
  EXPECT_EQ(Values(*bob_, "SELECT id FROM emp"), (S{"2"}));
  // hr's permissive policy OR-ed in; the restrictive one still removes APAC.
  EXPECT_EQ(Values(*hal_, "SELECT id FROM emp"), (S{"1", "2", "3"}));
  // No attribute: the predicate is NULL, so nothing.
  EXPECT_EQ(Values(*dan_, "SELECT id FROM emp"), (S{}));
  // A role with SELECT but no permissive policy that applies sees nothing.
  EXPECT_EQ(Values(*aud_, "SELECT id FROM emp"), (S{}));
  // The administrator is not subject to policies.
  EXPECT_EQ(Values(*admin_, "SELECT id FROM emp"), (S{"1", "2", "3", "4"}));
}

TEST_F(RbacPoliciesFixture, NoSpellingOfTheQueryGetsAroundTheFilter) {
  for (const auto& [sql, expected] : std::vector<std::pair<std::string, S>>{
           {"SELECT count(*) FROM emp", {"2"}},
           {"SELECT id FROM emp WHERE id = 2", {}},
           {"SELECT max(salary) FROM emp", {"300"}},
           {"SELECT count(*) FROM (SELECT * FROM emp) t", {"2"}},
           {"WITH e AS (SELECT * FROM emp) SELECT count(*) FROM e", {"2"}},
           {"WITH e AS MATERIALIZED (SELECT * FROM emp) SELECT count(*) FROM e", {"2"}},
           {"SELECT count(*) FROM emp a JOIN emp b ON a.id = b.id", {"2"}},
           {"SELECT count(*) FROM emp a, emp b", {"4"}},
           {"SELECT id FROM emp UNION SELECT id FROM emp", {"1", "3"}},
           {"SELECT count(*) FROM emp WHERE id IN (SELECT id FROM emp WHERE region = "
            "'US')",
            {"0"}},
           {"SELECT EXISTS (SELECT 1 FROM emp WHERE name = 'bob')", {"false"}},
           {"SELECT (SELECT max(salary) FROM emp)", {"300"}},
           {"SELECT count(*) FROM emp_v", {"2"}},
           {"SELECT count(*) FROM emp_v WHERE region = 'APAC'", {"0"}},
           {"SELECT count(*) FROM emp e, LATERAL (SELECT e.id AS x) l", {"2"}},
           {"SELECT region, count(*) FROM emp GROUP BY region", {"EU"}},
       }) {
    EXPECT_EQ(Values(*ann_, sql), expected) << sql;
  }
  // Prepared statements are filtered the same way.
  auto prepared = ann_->Prepare("SELECT count(*) FROM emp");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_OK_AND_ASSIGN(auto t, ann_->ExecutePrepared(**prepared));
  EXPECT_EQ(t->column(0)->GetScalar(0).ValueOrDie()->ToString(), "2");
}

TEST_F(RbacPoliciesFixture, PreparedStatementsWithParametersAreFilteredAndMasked) {
  // DuckDB plans a statement with parameters again for each run: the policies
  // apply to that plan.
  auto param = [](const std::shared_ptr<arrow::DataType>& type,
                  const std::string& value) {
    auto scalar = arrow::Scalar::Parse(type, value).ValueOrDie();
    auto array = arrow::MakeArrayFromScalar(*scalar, 1).ValueOrDie();
    return arrow::RecordBatch::Make(arrow::schema({arrow::field("p", type)}), 1, {array});
  };
  auto by_id = ann_->Prepare("SELECT id, ssn FROM emp WHERE id >= ? ORDER BY id");
  ASSERT_TRUE(by_id.ok()) << by_id.status().ToString();
  ASSERT_TRUE((*by_id)->SetParameters(param(arrow::int32(), "0")).ok());
  ASSERT_OK_AND_ASSIGN(auto rows, ann_->ExecutePrepared(**by_id));
  ASSERT_EQ(rows->num_rows(), 2);
  EXPECT_EQ(rows->column(0)->GetScalar(0).ValueOrDie()->ToString(), "1");
  EXPECT_EQ(rows->column(0)->GetScalar(1).ValueOrDie()->ToString(), "3");
  EXPECT_EQ(rows->column(1)->GetScalar(0).ValueOrDie()->ToString(), "XXX");
  // Running it again with another value: still filtered.
  ASSERT_TRUE((*by_id)->SetParameters(param(arrow::int32(), "2")).ok());
  ASSERT_OK_AND_ASSIGN(auto again, ann_->ExecutePrepared(**by_id));
  ASSERT_EQ(again->num_rows(), 1);
  EXPECT_EQ(again->column(0)->GetScalar(0).ValueOrDie()->ToString(), "3");
  // A parameter is no oracle for the masked value.
  auto by_ssn = ann_->Prepare("SELECT count(*) FROM emp WHERE ssn = ?");
  ASSERT_TRUE(by_ssn.ok()) << by_ssn.status().ToString();
  ASSERT_TRUE((*by_ssn)->SetParameters(param(arrow::utf8(), "111")).ok());
  ASSERT_OK_AND_ASSIGN(auto count, ann_->ExecutePrepared(**by_ssn));
  EXPECT_EQ(count->column(0)->GetScalar(0).ValueOrDie()->ToString(), "0");
  // Positive twin: the exempt user finds it.
  auto hal_ssn = hal_->Prepare("SELECT count(*) FROM emp WHERE ssn = ?");
  ASSERT_TRUE(hal_ssn.ok()) << hal_ssn.status().ToString();
  ASSERT_TRUE((*hal_ssn)->SetParameters(param(arrow::utf8(), "111")).ok());
  ASSERT_OK_AND_ASSIGN(auto hal_count, hal_->ExecutePrepared(**hal_ssn));
  EXPECT_EQ(hal_count->column(0)->GetScalar(0).ValueOrDie()->ToString(), "1");
}

TEST_F(RbacPoliciesFixture, AnOwnersViewDoesNotLiftTheCallersPolicies) {
  // emp_v is the administrator's, but policies follow the caller.
  EXPECT_EQ(Values(*ann_, "SELECT id FROM emp_v"), (S{"1", "3"}));
  EXPECT_EQ(Values(*bob_, "SELECT id FROM emp_v"), (S{"2"}));
}

TEST_F(RbacPoliciesFixture, ContextFunctionsAreTheSessionsNotDuckDBs) {
  // DuckDB's own current_user is the constant 'duckdb'; the policy must not see it.
  EXPECT_EQ(Values(*ann_, "SELECT note FROM notes"), (S{"ann-note"}));
  EXPECT_EQ(Values(*bob_, "SELECT note FROM notes"), (S{"bob-note"}));
}

TEST_F(RbacPoliciesFixture, AMaskedColumnIsMaskedWhereverItIsUsed) {
  EXPECT_EQ(One(*ann_, "SELECT email FROM emp WHERE id = 1"), "****@x.com");
  EXPECT_EQ(One(*ann_, "SELECT ssn FROM emp WHERE id = 1"), "XXX");
  // Not an oracle for the real value: filters, grouping and joins see the mask.
  EXPECT_EQ(One(*ann_, "SELECT count(*) FROM emp WHERE email = 'ann@x.com'"), "0");
  EXPECT_EQ(One(*ann_, "SELECT count(*) FROM emp WHERE ssn = '111'"), "0");
  EXPECT_EQ(One(*ann_, "SELECT count(*) FROM emp WHERE ssn LIKE '1%'"), "0");
  EXPECT_EQ(One(*ann_, "SELECT count(DISTINCT ssn) FROM emp"), "1");
  EXPECT_EQ(One(*ann_, "SELECT count(*) FROM emp a JOIN emp b ON a.ssn = b.ssn"), "4");
  EXPECT_EQ(Values(*ann_, "SELECT max(email) FROM emp"), (S{"****@x.com"}));
  EXPECT_EQ(Values(*ann_, "SELECT upper(ssn) FROM emp"), (S{"XXX", "XXX"}));
  EXPECT_EQ(Values(*ann_, "SELECT ssn FROM (SELECT * FROM emp) t"), (S{"XXX", "XXX"}));
  EXPECT_EQ(Values(*ann_, "WITH c AS (SELECT ssn FROM emp) SELECT * FROM c"),
            (S{"XXX", "XXX"}));
  EXPECT_EQ(Values(*ann_, "SELECT ssn FROM emp_v"), (S{"XXX", "XXX"}));
  EXPECT_EQ(Values(*ann_, "SELECT ssn FROM emp UNION ALL SELECT ssn FROM emp"),
            (S{"XXX", "XXX", "XXX", "XXX"}));
  EXPECT_EQ(Values(*ann_, "SELECT id FROM emp ORDER BY ssn, id LIMIT 1"), (S{"1"}));
  // A copy carries the mask, not the value.
  ASSERT_TRUE(ann_->Run("CREATE TABLE ann_copy AS SELECT * FROM emp").ok());
  EXPECT_EQ(Values(*ann_, "SELECT ssn FROM ann_copy"), (S{"XXX", "XXX"}));
  // Exempt and unmasking roles see the real values.
  EXPECT_EQ(One(*hal_, "SELECT ssn FROM emp WHERE id = 2"), "222");
  EXPECT_EQ(One(*hal_, "SELECT email FROM emp WHERE id = 2"), "bob@x.com");
}

TEST_F(RbacPoliciesFixture, WritesToAGovernedTableAreRefused) {
  const char* kWhy = "which has row or masking policies";
  ExpectBlocked(*ann_, "INSERT INTO emp VALUES (9, 'eve', 'US', 1, 'e@x', '999')", kWhy);
  ExpectBlocked(*ann_, "UPDATE emp SET salary = 0 WHERE id = 1", kWhy);
  ExpectBlocked(*ann_, "DELETE FROM emp WHERE id = 1 RETURNING *", "masked columns");
  // DELETE removes only rows the user can see.
  ASSERT_OK_AND_ASSIGN(auto deleted, ann_->Run("DELETE FROM emp WHERE id = 2"));
  EXPECT_EQ(Values(*admin_, "SELECT count(*) FROM emp"), (S{"4"}))
      << "a hidden row was deleted";
  // The administrator is not restricted.
  ASSERT_TRUE(admin_->Run("UPDATE emp SET salary = salary WHERE id = 1").ok());
}

TEST_F(RbacPoliciesFixture, PolicyChangesReachTheNextStatementAndPreparedOnes) {
  auto prepared = bob_->Prepare("SELECT count(*) FROM emp");
  ASSERT_TRUE(prepared.ok());
  ASSERT_OK_AND_ASSIGN(auto before, bob_->ExecutePrepared(**prepared));
  EXPECT_EQ(before->column(0)->GetScalar(0).ValueOrDie()->ToString(), "1");
  ASSERT_EQ(
      AdminError("CREATE OR REPLACE ROW POLICY region_rows ON emp TO staff USING (true)"),
      "");
  ASSERT_OK_AND_ASSIGN(auto after, bob_->ExecutePrepared(**prepared));
  EXPECT_EQ(after->column(0)->GetScalar(0).ValueOrDie()->ToString(), "3");
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY region_rows ON emp TO staff USING "
                       "(region = session_attribute('region'))"),
            "");
  EXPECT_EQ(One(*bob_, "SELECT count(*) FROM emp"), "1");
}

TEST_F(RbacPoliciesFixture, PolicySqlIsCheckedWhenItIsCreated) {
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"CREATE ROW POLICY p1 ON emp USING (id IN (SELECT id FROM notes))",
            "subquery"},
           {"CREATE ROW POLICY p2 ON emp USING (region = getvariable('r'))",
            "getvariable"},
           {"CREATE ROW POLICY p3 ON emp USING (region = current_setting('threads'))",
            "current_setting"},
           {"CREATE ROW POLICY p4 ON emp USING (no_such_column = 1)", "does not fit"},
           {"CREATE ROW POLICY p5 ON emp USING (session_attribute(region))",
            "string constant"},
           {"CREATE ROW POLICY p6 ON emp_v USING (true)", "is a view"},
           {"CREATE ROW POLICY region_rows ON emp USING (true)", "already exists"},
           {"CREATE MASKING POLICY m1 AS (val VARCHAR) RETURNS VARCHAR -> name",
            "only its parameter"},
           {"CREATE MASKING POLICY m2 AS (val VARCHAR) RETURNS VARCHAR -> (SELECT 'x')",
            "subquery"},
           {"DROP MASKING POLICY hide", "unset it there first"},
       }) {
    const auto error = AdminError(sql);
    EXPECT_FALSE(error.empty()) << sql << " was accepted";
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  // Only the administrator manages policies.
  ExpectBlocked(*ann_, "CREATE ROW POLICY mine ON emp USING (true)",
                "only administrators manage row and masking policies");
  ExpectBlocked(*ann_, "ALTER TABLE emp MODIFY COLUMN ssn UNSET MASKING POLICY",
                "only administrators manage row and masking policies");
  ExpectBlocked(*ann_, "SHOW ROW POLICIES", "only administrators");
  auto shown = admin_->Run("SHOW ROW POLICIES ON emp");
  ASSERT_TRUE(shown.ok()) << shown.status().ToString();
  EXPECT_EQ((*shown)->num_rows(), 3);
}

TEST_F(RbacPoliciesFixture, AnErrorInTheUsersOwnPredicateDoesNotRevealHiddenRows) {
  // Rows 2 (bob, US) and 4 (dan, APAC) are hidden from ann. Each predicate
  // fails only on a hidden row, with the row's name in the error, so it leaks
  // if DuckDB evaluates it before the policy's filter. emp2's policy cannot be
  // pushed into the scan, so the two filters really are separate operators.
  ASSERT_EQ(AdminError("CREATE TABLE IF NOT EXISTS emp2 AS SELECT * FROM emp"), "");
  ASSERT_EQ(AdminError("GRANT SELECT ON emp2 TO staff"), "");
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY emp2_rows ON emp2 TO staff USING "
                       "(lower(region) || '' = lower(session_attribute('region')))"),
            "");
  for (const std::string table : {"emp", "emp2"}) {
    for (const std::string where : {
             "CASE WHEN id = 2 THEN CAST(name AS INTEGER) = 1 ELSE false END",
             "CASE WHEN region <> 'EU' THEN error('row ' || name) IS NULL ELSE true END",
             "id IN (2, 4) AND CAST(name AS INTEGER) = 1",
             "CAST(CASE WHEN region = 'EU' THEN '1' ELSE name END AS INTEGER) > 0",
         }) {
      const std::string sql = "SELECT id FROM " + table + " WHERE " + where;
      // The control: with the row visible, the predicate does reach it.
      auto control = admin_->Run(sql).status();
      ASSERT_FALSE(control.ok()) << sql;
      ASSERT_TRUE(control.ToString().find("bob") != std::string::npos ||
                  control.ToString().find("dan") != std::string::npos)
          << sql << ": " << control.ToString();
      auto st = ann_->Run(sql).status();
      EXPECT_EQ(st.ToString().find("bob"), std::string::npos)
          << sql << " revealed a hidden row: " << st.ToString();
      EXPECT_EQ(st.ToString().find("dan"), std::string::npos)
          << sql << " revealed a hidden row: " << st.ToString();
    }
    const std::string having =
        "SELECT region FROM " + table +
        " GROUP BY region HAVING max(CASE WHEN region <> 'EU' THEN "
        "CAST(name AS INTEGER) END) > 0";
    auto st = ann_->Run(having).status();
    EXPECT_EQ(st.ToString().find("bob"), std::string::npos)
        << having << ": " << st.ToString();
  }
}

TEST_F(RbacPoliciesFixture, APolicyChangeStopsAStatementThatIsRunning) {
  Client hal_runner(GetPort(), "hal", "user", 40);
  auto running = std::async(std::launch::async, [&] {
    return hal_runner
        .Run("SELECT sum(e.salary * r.range) FROM emp e, range(1000000000000) r")
        .status();
  });
  struct EndIfStillRunning {
    Client& admin;
    std::future<arrow::Status>& f;
    ~EndIfStillRunning() {
      if (f.valid() && f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        (void)admin.Run("KILL SESSION '00000000-0000-4000-5000-000000000040'");
        f.wait_for(std::chrono::seconds(60));
      }
    }
  } end_if_still_running{*admin_, running};
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EQ(running.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
  // A policy that does not concern hal leaves it running...
  ASSERT_EQ(
      AdminError(
          "CREATE OR REPLACE ROW POLICY unrelated ON notes TO auditors USING (true)"),
      "");
  ASSERT_EQ(running.wait_for(std::chrono::seconds(2)), std::future_status::timeout);
  // ...one that narrows what he sees stops it.
  ASSERT_EQ(AdminError(
                "CREATE OR REPLACE ROW POLICY hr_all ON emp TO hr USING (region = 'EU')"),
            "");
  ASSERT_EQ(running.wait_for(std::chrono::seconds(30)), std::future_status::ready)
      << "a statement kept running after its user's row policy changed";
  EXPECT_FALSE(running.get().ok());
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY hr_all ON emp TO hr USING (true)"),
            "");
  ASSERT_EQ(AdminError("DROP ROW POLICY unrelated ON notes"), "");
  EXPECT_EQ(Values(*hal_, "SELECT id FROM emp"), (S{"1", "2", "3"}));
}

TEST_F(RbacPoliciesFixture, ContextFunctionsInEveryForm) {
  // current_user() and session_user() as function calls (DuckDB's would return 'duckdb').
  ASSERT_EQ(AdminError("CREATE TABLE IF NOT EXISTS notes2 AS SELECT * FROM notes"), "");
  ASSERT_EQ(AdminError("GRANT SELECT ON notes2 TO staff"), "");
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY own2 ON notes2 USING "
                       "(owner = current_user() OR owner = session_user())"),
            "");
  EXPECT_EQ(Values(*ann_, "SELECT note FROM notes2"), (S{"ann-note"}));
  EXPECT_EQ(Values(*bob_, "SELECT note FROM notes2"), (S{"bob-note"}));
}

TEST_F(RbacPoliciesFixture, ARowPolicyDecidesOnTheRealValueOfAMaskedColumn) {
  // ssn is masked for ann ('XXX'), but the policy hiding ssn 333 must see 333.
  ASSERT_EQ(AdminError("CREATE TABLE IF NOT EXISTS emp3 AS SELECT * FROM emp"), "");
  ASSERT_EQ(AdminError("GRANT SELECT ON emp3 TO staff"), "");
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY all3 ON emp3 USING (true)"), "");
  ASSERT_EQ(AdminError("CREATE OR REPLACE ROW POLICY not333 ON emp3 AS RESTRICTIVE USING "
                       "(ssn <> '333')"),
            "");
  ASSERT_EQ(AdminError("ALTER TABLE emp3 MODIFY COLUMN ssn SET MASKING POLICY hide"), "");
  EXPECT_EQ(Values(*ann_, "SELECT id FROM emp3"), (S{"1", "2", "4"}));
  EXPECT_EQ(Values(*ann_, "SELECT ssn FROM emp3"), (S{"XXX", "XXX", "XXX"}));
}
