// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Role-based access control end to end, over Flight SQL: the administrator runs
// access-control statements, store users connect with tokens, unknown users are
// refused, disabling a user ends their live session, and statements cannot be
// smuggled past the access-control parser. Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <future>
#include <thread>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "enterprise/enterprise_features.h"
#include "enterprise/rbac/rbac_engine.h"
#include "gizmosql_logging.h"
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
      .set_id("rbac-test-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-test-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

using Header = std::pair<std::string, std::string>;
Header Bearer(const std::string& user, const std::string& role,
              const std::string& session) {
  return {"authorization", "Bearer " + MintToken(user, role, session)};
}

// Run `sql` and return the result table (or the error).
arrow::Result<std::shared_ptr<arrow::Table>> Query(int port, const Header& auth,
                                                   const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  arrow::flight::FlightCallOptions co;
  co.headers.push_back(auth);
  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(co, sql));
  std::vector<std::shared_ptr<arrow::Table>> tables;
  for (const auto& ep : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(co, ep.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    tables.push_back(table);
  }
  if (tables.empty()) {
    ARROW_ASSIGN_OR_RAISE(auto schema, info->GetSchema(nullptr));
    return arrow::Table::MakeEmpty(schema);
  }
  return arrow::ConcatenateTables(tables);
}

// Log in with a user name and password (Basic); the server answers with the
// Bearer header the client uses from then on.
arrow::Result<Header> PasswordLogin(int port, const std::string& user,
                                    const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("127.0.0.1", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  ARROW_ASSIGN_OR_RAISE(auto bearer, client->AuthenticateBasicToken({}, user, password));
  return Header{bearer.first, bearer.second};
}

std::string Cell(const std::shared_ptr<arrow::Table>& t, int col, int row) {
  auto chunked = t->column(col);
  auto arr = std::static_pointer_cast<arrow::StringArray>(chunked->chunk(0));
  return arr->GetString(row);
}

class RbacServerFixture : public gizmosql::testing::ServerTestFixture<RbacServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_server_test.db",
        .port = 31700,
        .health_port = 31701,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = "memory",
    };
  }
};

// A server on a shared PostgreSQL store (GIZMOSQL_TEST_RBAC_PG_URI), so a
// second engine on the same store can act as another node of the cluster.
const char* ClusterStoreUri() { return std::getenv("GIZMOSQL_TEST_RBAC_PG_URI"); }

class RbacClusterFixture
    : public gizmosql::testing::ServerTestFixture<RbacClusterFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_cluster_test.db",
        .port = 31704,
        .health_port = 31705,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
        .rbac_store_uri = ClusterStoreUri() ? ClusterStoreUri() : "memory",
    };
  }
};

class NoRbacServerFixture
    : public gizmosql::testing::ServerTestFixture<NoRbacServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_off_server_test.db",
        .port = 31702,
        .health_port = 31703,
        .username = "admin_user",
        .password = "admin_pass",
        .allow_cross_instance_tokens = true,
    };
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacServerFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacServerFixture>::server_ready_{
    false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacServerFixture>::config_{};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacClusterFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacClusterFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacClusterFixture>::server_ready_{
    false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacClusterFixture>::config_{};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<NoRbacServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<NoRbacServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<NoRbacServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<NoRbacServerFixture>::config_{};

#define REQUIRE_RBAC_SERVER()                                             \
  do {                                                                    \
    if (!RbacLicensed()) GTEST_SKIP() << "license does not include RBAC"; \
    ASSERT_TRUE(IsServerReady()) << "RBAC test server did not start";     \
  } while (0)

TEST_F(RbacServerFixture, AdministratorManagesAccessControl) {
  REQUIRE_RBAC_SERVER();
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000001");
  ASSERT_ARROW_OK_AND_ASSIGN(auto created,
                             Query(GetPort(), admin, "CREATE ROLE analyst"));
  EXPECT_EQ(Cell(created, 0, 0), "Role analyst created");
  ASSERT_OK(Query(GetPort(), admin, "CREATE TABLE orders (id INT, amount INT)").status());
  ASSERT_OK(Query(GetPort(), admin, "CREATE USER joe").status());
  ASSERT_OK(Query(GetPort(), admin, "GRANT analyst TO joe").status());
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto granted, Query(GetPort(), admin, "GRANT SELECT (id) ON orders TO analyst"));
  EXPECT_NE(Cell(granted, 0, 0).find("Granted SELECT (id) on"), std::string::npos)
      << Cell(granted, 0, 0);

  ASSERT_ARROW_OK_AND_ASSIGN(auto grants, Query(GetPort(), admin, "SHOW GRANTS FOR joe"));
  ASSERT_EQ(grants->num_columns(), 8);
  EXPECT_EQ(grants->schema()->field(0)->name(), "privilege");
  bool saw = false;
  for (int r = 0; r < grants->num_rows(); ++r) {
    if (Cell(grants, 0, r) == "SELECT" && Cell(grants, 3, r) == "id") saw = true;
  }
  EXPECT_TRUE(saw) << grants->ToString();
}

TEST_F(RbacServerFixture, StoreUsersConnectWithTokensAndCannotGrant) {
  REQUIRE_RBAC_SERVER();
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000002");
  ASSERT_OK(Query(GetPort(), admin, "CREATE USER IF NOT EXISTS mia").status());
  const auto mia = Bearer("mia", "user", "00000000-0000-4000-8000-000000000007");
  // A store user may look at their own grants ...
  ASSERT_OK(Query(GetPort(), mia, "SHOW GRANTS").status());
  // ... but not change anything, and the refusal says why.
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"CREATE ROLE sneaky", "only administrators manage users and roles"},
           {"CREATE USER helper", "only administrators manage users and roles"},
           {"ALTER USER mia DISABLE", "only administrators manage users and roles"},
           {"GRANT SELECT ON orders TO PUBLIC", "may not grant SELECT"},
           {"GRANT PUBLIC TO mia", "cannot be granted"}}) {
    auto st = Query(GetPort(), mia, sql).status();
    EXPECT_FALSE(st.ok()) << sql;
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << " -> " << st.ToString();
  }
  ASSERT_ARROW_OK_AND_ASSIGN(auto roles, Query(GetPort(), admin, "SHOW GRANTS FOR mia"));
  EXPECT_EQ(roles->num_rows(), 0);  // nothing was granted by those attempts
}

TEST_F(RbacServerFixture, UnknownUsersAreRefusedAtConnect) {
  REQUIRE_RBAC_SERVER();
  auto st = Query(GetPort(),
                  Bearer("nobody_here", "user", "00000000-0000-4000-8000-000000000008"),
                  "SELECT 1")
                .status();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("is not defined on this server"), std::string::npos)
      << st.ToString();
  // An old-style token role does not help: only "admin" is special.
  st = Query(GetPort(),
             Bearer("nobody_here", "readonly", "00000000-0000-4000-8000-000000000009"),
             "SELECT 1")
           .status();
  EXPECT_FALSE(st.ok());
}

TEST_F(RbacServerFixture, DisablingAUserEndsTheirLiveSession) {
  REQUIRE_RBAC_SERVER();
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000003");
  ASSERT_OK(Query(GetPort(), admin, "CREATE USER IF NOT EXISTS leo").status());
  ASSERT_OK(Query(GetPort(), admin, "ALTER USER leo ENABLE").status());
  const auto leo = Bearer("leo", "user", "00000000-0000-4000-8000-000000000005");
  ASSERT_OK(Query(GetPort(), leo, "SELECT 1").status());  // session is live
  ASSERT_OK(Query(GetPort(), admin, "ALTER USER leo DISABLE").status());
  auto st = Query(GetPort(), leo, "SELECT 1").status();
  EXPECT_FALSE(st.ok()) << "the disabled user's open session kept working";
  // A brand-new session is refused too.
  st = Query(GetPort(), Bearer("leo", "user", "00000000-0000-4000-8000-000000000006"),
             "SELECT 1")
           .status();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("is disabled"), std::string::npos) << st.ToString();
}

TEST_F(RbacServerFixture, StoreUsersLogInWithTheirPassword) {
  REQUIRE_RBAC_SERVER();
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000010");
  ASSERT_OK(Query(GetPort(), admin,
                  "CREATE USER IF NOT EXISTS pat WITH PASSWORD 'correct horse 1'")
                .status());
  ASSERT_OK(Query(GetPort(), admin, "CREATE USER IF NOT EXISTS nopass").status());

  ASSERT_ARROW_OK_AND_ASSIGN(auto pat,
                             PasswordLogin(GetPort(), "pat", "correct horse 1"));
  ASSERT_OK(Query(GetPort(), pat, "SELECT 1").status());
  // The session is pat's, not the administrator's.
  auto st = Query(GetPort(), pat, "CREATE ROLE pats_role").status();
  ASSERT_FALSE(st.ok()) << "a password login got administrator rights";
  EXPECT_NE(st.ToString().find("only administrators manage users and roles"),
            std::string::npos)
      << st.ToString();

  // Every failure looks the same: wrong password, no password, unknown name.
  for (const auto& [user, password] :
       std::vector<std::pair<std::string, std::string>>{{"pat", "wrong"},
                                                        {"pat", "correct horse"},
                                                        {"nopass", "anything"},
                                                        {"nobody_at_all", "x"}}) {
    auto login = PasswordLogin(GetPort(), user, password);
    ASSERT_FALSE(login.ok()) << user << " logged in with a bad password";
    EXPECT_NE(login.status().ToString().find("Invalid credentials"), std::string::npos)
        << user << " -> " << login.status().ToString();
  }

  ASSERT_OK(Query(GetPort(), admin, "ALTER USER pat DISABLE").status());
  EXPECT_FALSE(PasswordLogin(GetPort(), "pat", "correct horse 1").ok())
      << "a disabled user logged in";
  ASSERT_OK(Query(GetPort(), admin, "ALTER USER pat ENABLE").status());
  ASSERT_OK(Query(GetPort(), admin, "ALTER USER pat WITH PASSWORD 'battery staple 2'")
                .status());
  EXPECT_FALSE(PasswordLogin(GetPort(), "pat", "correct horse 1").ok())
      << "the old password still works";
  ASSERT_OK(PasswordLogin(GetPort(), "pat", "battery staple 2").status());

  // The administrator's own login is unchanged, and its name is not available.
  ASSERT_ARROW_OK_AND_ASSIGN(auto admin_login,
                             PasswordLogin(GetPort(), "admin_user", "admin_pass"));
  ASSERT_OK(
      Query(GetPort(), admin_login, "CREATE ROLE IF NOT EXISTS via_password").status());
  for (const char* sql :
       {"CREATE USER admin_user WITH PASSWORD 'x'", "CREATE USER ADMIN_USER"}) {
    st = Query(GetPort(), admin, sql).status();
    ASSERT_FALSE(st.ok()) << sql;
    EXPECT_NE(st.ToString().find("is the server administrator's name"), std::string::npos)
        << sql << " -> " << st.ToString();
  }
}

TEST_F(RbacServerFixture, NothingRidesAlongWithAnAccessControlStatement) {
  REQUIRE_RBAC_SERVER();
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000004");
  ASSERT_OK(
      Query(GetPort(), admin, "CREATE TABLE IF NOT EXISTS keepme (x INT)").status());
  for (const char* sql : {"CREATE ROLE r1; DROP TABLE keepme",
                          "GRANT SELECT ON keepme TO PUBLIC; DROP TABLE keepme",
                          "/* x */ CREATE ROLE r2 -- y\n; DROP TABLE keepme"}) {
    auto st = Query(GetPort(), admin, sql).status();
    EXPECT_FALSE(st.ok()) << sql;
    EXPECT_NE(st.ToString().find("one at a time"), std::string::npos) << st.ToString();
  }
  ASSERT_OK(Query(GetPort(), admin, "SELECT * FROM keepme").status());  // still there
  ASSERT_ARROW_OK_AND_ASSIGN(auto grants,
                             Query(GetPort(), admin, "SHOW GRANTS FOR ROLE PUBLIC"));
  EXPECT_EQ(grants->num_rows(), 0);  // and no grant slipped in
}

TEST(RbacLogging, UserDdlIsRedactedWhereverTheKeywordsSit) {
  for (const char* sql :
       {"CREATE USER joe PASSWORD 'hunter2'", "alter user joe with password 'hunter2'",
        "/* /* nested */ */ CREATE USER joe PASSWORD 'hunter2'",
        "-- comment\nCREATE\n USER joe PASSWORD 'hunter2'"}) {
    const std::string logged = gizmosql::redact_sql_for_logs(sql);
    EXPECT_EQ(logged.find("hunter2"), std::string::npos) << sql << " -> " << logged;
  }
  EXPECT_EQ(gizmosql::redact_sql_for_logs("SELECT 1"), "SELECT 1");
}

TEST_F(NoRbacServerFixture, AccessControlStatementsNeedTheStore) {
  ASSERT_TRUE(IsServerReady());
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-000000000010");
  auto st = Query(GetPort(), admin, "CREATE ROLE analyst").status();
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("access control is not enabled"), std::string::npos)
      << st.ToString();
  // SHOW GRANTS is not ours here: DuckDB treats it as DESCRIBE of a table.
  ASSERT_OK(Query(GetPort(), admin, "CREATE TABLE grants (g INT)").status());
  ASSERT_OK(Query(GetPort(), admin, "SHOW GRANTS").status());
}

// Another node (a second engine on the same store) disables a user: the user's
// live session on this node ends once the change arrives by the version poll.
TEST_F(RbacClusterFixture, DisablingAUserOnAnotherNodeEndsSessionsHere) {
  if (!ClusterStoreUri()) GTEST_SKIP() << "GIZMOSQL_TEST_RBAC_PG_URI is not set";
  REQUIRE_RBAC_SERVER();
  namespace rbac = gizmosql::enterprise::rbac;
  rbac::RbacEngine::Options options;
  options.store_uri = ClusterStoreUri();
  options.poll_interval = std::chrono::milliseconds(0);
  ASSERT_ARROW_OK_AND_ASSIGN(auto other_node, rbac::RbacEngine::Create(options));
  struct NoResolver : rbac::ObjectResolver {
    arrow::Result<rbac::ResolvedObject> ResolveObject(
        const rbac::QualifiedName&) override {
      return arrow::Status::Invalid("unused");
    }
    arrow::Result<rbac::ObjectPath> ResolveSchema(const rbac::QualifiedName&) override {
      return arrow::Status::Invalid("unused");
    }
    arrow::Result<std::string> ResolveCatalog(const std::string&) override {
      return arrow::Status::Invalid("unused");
    }
  } resolver;
  auto run_elsewhere = [&](const std::string& sql) {
    auto cmd = rbac::ParseRbacStatement(sql);
    ASSERT_TRUE(cmd.ok()) << cmd.status().ToString();
    auto result =
        other_node->Execute(*cmd, rbac::Principal::Superuser("admin"), resolver);
    ASSERT_TRUE(result.ok()) << sql << " -> " << result.status().ToString();
  };
  const std::string user =
      "nina_" +
      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() %
                     1000000);
  run_elsewhere("CREATE USER " + user);

  // This node learns of the new user by polling; then the user can connect.
  const auto session = Bearer(user, "user", "00000000-0000-4000-8000-0000000000aa");
  arrow::Status st;
  for (int i = 0; i < 50; ++i) {
    st = Query(GetPort(), session, "SELECT 1").status();
    if (st.ok()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_OK(st);

  run_elsewhere("ALTER USER " + user + " DISABLE");
  bool ended = false;
  for (int i = 0; i < 50 && !ended; ++i) {
    ended = !Query(GetPort(), session, "SELECT 1").status().ok();
    if (!ended) std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  EXPECT_TRUE(ended) << "the session survived a DISABLE made on another node";
}

TEST_F(RbacClusterFixture, ARevokeOnAnotherNodeStopsAStatementRunningHere) {
  if (!ClusterStoreUri()) GTEST_SKIP() << "GIZMOSQL_TEST_RBAC_PG_URI is not set";
  REQUIRE_RBAC_SERVER();
  namespace rbac = gizmosql::enterprise::rbac;
  rbac::RbacEngine::Options options;
  options.store_uri = ClusterStoreUri();
  options.poll_interval = std::chrono::milliseconds(0);
  ASSERT_ARROW_OK_AND_ASSIGN(auto other_node, rbac::RbacEngine::Create(options));
  // The other node resolves names as this server's catalog does.
  struct TableResolver : rbac::ObjectResolver {
    arrow::Result<rbac::ResolvedObject> ResolveObject(
        const rbac::QualifiedName& n) override {
      return rbac::ResolvedObject{{"rbac_cluster_test", {"main"}, n.parts.back()},
                                  rbac::ObjectType::kTable,
                                  {"id"}};
    }
    arrow::Result<rbac::ObjectPath> ResolveSchema(const rbac::QualifiedName&) override {
      return arrow::Status::Invalid("unused");
    }
    arrow::Result<std::string> ResolveCatalog(const std::string&) override {
      return arrow::Status::Invalid("unused");
    }
  } resolver;
  auto run_elsewhere = [&](const std::string& sql) {
    auto cmd = rbac::ParseRbacStatement(sql);
    ASSERT_TRUE(cmd.ok()) << cmd.status().ToString();
    auto result =
        other_node->Execute(*cmd, rbac::Principal::Superuser("admin"), resolver);
    ASSERT_TRUE(result.ok()) << sql << " -> " << result.status().ToString();
  };
  const std::string tag = std::to_string(
      std::chrono::steady_clock::now().time_since_epoch().count() % 1000000);
  const std::string user = "otto_" + tag, role = "otto_role_" + tag,
                    table = "otto_t_" + tag;
  const auto admin =
      Bearer("admin_user", "admin", "00000000-0000-4000-8000-0000000000b0");
  ASSERT_OK(
      Query(GetPort(), admin, "CREATE TABLE " + table + " AS SELECT 1 AS id").status());
  run_elsewhere("CREATE ROLE " + role);
  run_elsewhere("CREATE USER " + user);
  run_elsewhere("GRANT " + role + " TO " + user);
  run_elsewhere("GRANT SELECT ON " + table + " TO " + role);

  const auto session = Bearer(user, "user", "00000000-0000-4000-8000-0000000000b1");
  arrow::Status st;
  for (int i = 0; i < 50; ++i) {  // this node learns of the grant by polling
    st = Query(GetPort(), session, "SELECT * FROM " + table).status();
    if (st.ok()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  ASSERT_OK(st);
  auto running = std::async(std::launch::async, [&] {
    return Query(
               GetPort(), session,
               "SELECT sum(t.id * r.range) FROM " + table + " t, range(1000000000000) r")
        .status();
  });
  struct EndIfStillRunning {
    int port;
    std::future<arrow::Status>& f;
    ~EndIfStillRunning() {
      if (f.valid() && f.wait_for(std::chrono::seconds(0)) != std::future_status::ready) {
        (void)Query(port,
                    Bearer("admin_user", "admin", "00000000-0000-4000-8000-0000000000b2"),
                    "KILL SESSION '00000000-0000-4000-8000-0000000000b1'");
        f.wait_for(std::chrono::seconds(60));
      }
    }
  } end_if_still_running{GetPort(), running};
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_EQ(running.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

  run_elsewhere("REVOKE SELECT ON " + table + " FROM " + role);
  ASSERT_EQ(running.wait_for(std::chrono::seconds(30)), std::future_status::ready)
      << "a REVOKE on another node left a statement running here";
  EXPECT_FALSE(running.get().ok());
}
