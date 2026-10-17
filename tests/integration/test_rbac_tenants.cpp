// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Tenants on one shared server (RBAC 1.3), attacked over Flight SQL. A
// tenant's users see only the tenant's catalogs: another tenant's catalog is
// indistinguishable from one that does not exist, in errors and in every
// metadata listing. Grants, roles and ownership never cross a tenant, and a
// tenant admin manages only its own tenant. Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
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
      .set_id("rbac-tenants-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-tenants-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-4000-%012d", n);
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
  arrow::Result<std::shared_ptr<arrow::Table>> Fetch(
      arrow::Result<std::unique_ptr<arrow::flight::FlightInfo>> info_result) {
    auto co = Options();
    ARROW_ASSIGN_OR_RAISE(auto info, std::move(info_result));
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
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
    return Fetch(client_->Execute(Options(), sql));
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Catalogs() {
    return Fetch(client_->GetCatalogs(Options()));
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Tables() {
    return Fetch(
        client_->GetTables(Options(), nullptr, nullptr, nullptr, false, nullptr));
  }

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

// Every value in the table, as text.
std::string AllText(const std::shared_ptr<arrow::Table>& t) {
  return t ? t->ToString() : "";
}

std::set<std::string> Column(const std::shared_ptr<arrow::Table>& t, int col = 0) {
  std::set<std::string> out;
  if (!t) return out;
  for (int64_t r = 0; r < t->num_rows(); ++r) {
    out.insert(t->column(col)->GetScalar(r).ValueOrDie()->ToString());
  }
  return out;
}

class RbacTenantsFixture
    : public gizmosql::testing::ServerTestFixture<RbacTenantsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_tenants_test.db",
        .port = 31770,
        .health_port = 31771,
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
               "CREATE TABLE shared_default AS SELECT 'default-secret' AS s",
               "ATTACH ':memory:' AS acme_db",
               "ATTACH ':memory:' AS beta_db",
               "CREATE TABLE acme_db.main.orders AS SELECT 1 AS id, 'acme-order' AS note",
               "CREATE TABLE beta_db.main.secrets AS SELECT 1 AS id, 'beta-secret' AS "
               "hush",
               "CREATE VIEW beta_db.main.secrets_view AS SELECT hush FROM "
               "beta_db.main.secrets",
               "CREATE TABLE beta_db.main.secrets_keyed (id INT PRIMARY KEY)",
               "CREATE INDEX secrets_idx ON beta_db.main.secrets (hush)",
               "CREATE TENANT acme",
               "CREATE TENANT beta",
               "ALTER CATALOG acme_db SET TENANT acme",
               "ALTER CATALOG beta_db SET TENANT beta",
               "ALTER TENANT acme SET DEFAULT CATALOG acme_db",
               "CREATE USER acme/boss",
               "GRANT acme/tenant_admin TO acme/boss",
               "CREATE USER beta/bea",
               "GRANT beta/tenant_admin TO beta/bea",
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      Client boss(GetPort(), "acme/boss", "user", 2);
      for (const char* sql :
           {"CREATE USER acme/joe", "CREATE ROLE acme/readers",
            "GRANT SELECT ON acme_db.main.orders TO acme/readers",
            "GRANT acme/readers TO acme/joe", "CREATE ROLE acme/builders",
            "GRANT CREATE VIEW ON SCHEMA acme_db.main TO acme/builders",
            "GRANT acme/builders TO acme/joe"}) {
        auto st = boss.Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
    joe_ = std::make_unique<Client>(GetPort(), "acme/joe", "user", 3);
    boss_ = std::make_unique<Client>(GetPort(), "acme/boss", "user", 4);
    bea_ = std::make_unique<Client>(GetPort(), "beta/bea", "user", 5);
  }

  static std::string Error(Client& who, const std::string& sql) {
    auto st = who.Run(sql).status();
    return st.ok() ? "" : st.ToString();
  }
  std::string AdminError(const std::string& sql) { return Error(*admin_, sql); }

  std::unique_ptr<Client> admin_, joe_, boss_, bea_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacTenantsFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacTenantsFixture>::server_thread_{};
template <>
std::atomic<bool> gizmosql::testing::ServerTestFixture<RbacTenantsFixture>::server_ready_{
    false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacTenantsFixture>::config_{};

TEST_F(RbacTenantsFixture, ATenantsUsersWorkInTheirOwnCatalogs) {
  // The default catalog is the tenant's, so unqualified names work.
  auto t = joe_->Run("SELECT note FROM orders");
  ASSERT_TRUE(t.ok()) << t.status().ToString();
  EXPECT_EQ(Column(*t), (std::set<std::string>{"acme-order"}));
  ASSERT_TRUE(joe_->Run("SELECT note FROM acme_db.main.orders").ok());
  // The tenant admin may create in the tenant's catalog and share it.
  ASSERT_EQ(Error(*boss_, "CREATE TABLE acme_db.main.made_by_boss AS SELECT 7 AS x"), "");
  ASSERT_EQ(Error(*boss_, "GRANT SELECT ON acme_db.main.made_by_boss TO acme/readers"),
            "");
  EXPECT_EQ(Error(*joe_, "SELECT x FROM made_by_boss"), "");
}

TEST_F(RbacTenantsFixture, AnotherTenantsCatalogLooksExactlyLikeAMissingOne) {
  // The reference: a catalog that does not exist at all.
  const std::string missing = Error(*joe_, "SELECT * FROM no_such_db.main.t");
  ASSERT_NE(missing, "");
  ASSERT_NE(missing.find("GizmoSQL blocked"), std::string::npos) << missing;
  for (const char* sql :
       {"SELECT * FROM beta_db.main.secrets", "SELECT * FROM beta_db.secrets",
        "SELECT hush FROM beta_db.main.secrets",
        "SELECT no_such_column FROM beta_db.main.secrets",
        "SELECT * FROM beta_db.main.no_such_table",
        "SELECT * FROM rbac_tenants_test.main.shared_default",
        "SELECT * FROM shared_default", "SELECT * FROM secrets", "USE beta_db",
        "CREATE TABLE beta_db.main.planted AS SELECT 1",
        "DROP TABLE beta_db.main.secrets",
        "INSERT INTO beta_db.main.secrets VALUES (2, 'x')",
        // Describing or building on it must not tell either.
        "DESCRIBE beta_db.main.secrets", "DESCRIBE SELECT * FROM beta_db.main.secrets",
        "SUMMARIZE beta_db.main.secrets", "SHOW beta_db.main.secrets",
        "CREATE VIEW acme_db.main.peek_a AS SELECT * FROM beta_db.main.secrets",
        "CREATE VIEW acme_db.main.peek_b AS SELECT * FROM beta_db.main.no_such_table",
        "CREATE TABLE acme_db.main.copy_a AS SELECT * FROM beta_db.main.secrets",
        "EXPLAIN SELECT * FROM beta_db.main.secrets"}) {
    EXPECT_EQ(Error(*joe_, sql), missing)
        << sql << " told joe more than a missing catalog would";
  }
  // Nothing in an error ever names the other tenant's objects.
  for (const char* sql : {"SELECT * FROM secretz", "SELECT * FROM beta_db.main.secretz",
                          "SELECT * FROM orderz"}) {
    const auto error = Error(*joe_, sql);
    EXPECT_EQ(error.find("beta_db"), std::string::npos) << sql << ": " << error;
    EXPECT_EQ(error.find("secrets"), std::string::npos) << sql << ": " << error;
  }
}

TEST_F(RbacTenantsFixture, MetadataShowsOnlyTheTenantsCatalogs) {
  for (const char* sql :
       {"SELECT database_name FROM duckdb_databases()", "SHOW DATABASES",
        "SELECT * FROM duckdb_tables()", "SELECT * FROM information_schema.tables",
        "SELECT * FROM information_schema.schemata", "SELECT * FROM duckdb_columns()",
        "SHOW ALL TABLES"}) {
    auto t = joe_->Run(sql);
    ASSERT_TRUE(t.ok()) << sql << ": " << t.status().ToString();
    const std::string text = AllText(*t);
    EXPECT_EQ(text.find("beta_db"), std::string::npos)
        << sql << " listed another tenant's catalog";
    EXPECT_EQ(text.find("secrets"), std::string::npos)
        << sql << " listed another tenant's table";
    EXPECT_EQ(text.find("shared_default"), std::string::npos)
        << sql << " listed the shared default catalog";
  }
  auto databases =
      joe_->Run("SELECT database_name FROM duckdb_databases() WHERE NOT internal");
  ASSERT_TRUE(databases.ok());
  // GizmoSQL's helper catalog (views for JDBC metadata calls) is listed too.
  EXPECT_EQ(Column(*databases), (std::set<std::string>{"_gizmosql_system", "acme_db"}));
  // The Flight SQL catalog calls too.
  auto catalogs = joe_->Catalogs();
  ASSERT_TRUE(catalogs.ok()) << catalogs.status().ToString();
  EXPECT_EQ(AllText(*catalogs).find("beta_db"), std::string::npos) << AllText(*catalogs);
  auto tables = joe_->Tables();
  ASSERT_TRUE(tables.ok()) << tables.status().ToString();
  EXPECT_EQ(AllText(*tables).find("secrets"), std::string::npos);
  EXPECT_NE(AllText(*tables).find("orders"), std::string::npos)
      << "its own tables are listed";
}

TEST_F(RbacTenantsFixture, MetadataCannotBeReachedAnotherWay) {
  // Joe's own views over the metadata functions, and GizmoSQL's helper views,
  // list only what joe could list directly.
  for (const char* sql : {
           "CREATE OR REPLACE VIEW acme_db.main.all_views AS SELECT * FROM "
           "duckdb_views()",
           "CREATE OR REPLACE VIEW acme_db.main.all_tables AS SELECT * FROM "
           "duckdb_tables()",
           "CREATE OR REPLACE VIEW acme_db.main.all_info AS SELECT * FROM "
           "information_schema.tables",
           "CREATE OR REPLACE VIEW acme_db.main.all_defs AS "
           "SELECT * FROM _gizmosql_system.main.gizmosql_view_definition",
       }) {
    ASSERT_EQ(Error(*joe_, sql), "") << sql;
  }
  for (const char* sql : {
           "SELECT * FROM acme_db.main.all_views",
           "SELECT * FROM acme_db.main.all_tables",
           "SELECT * FROM acme_db.main.all_info",
           "SELECT * FROM acme_db.main.all_defs",
           "SELECT * FROM _gizmosql_system.main.gizmosql_view_definition",
           "SELECT * FROM _gizmosql_system.main.gizmosql_index_info",
           "SELECT * FROM _gizmosql_system.gizmosql_view_definition",
           "WITH x AS (SELECT * FROM duckdb_tables()) SELECT * FROM x",
           "SELECT * FROM duckdb_tables ()",
           "SELECT * FROM \"duckdb_views\"()",
           "SELECT * FROM duckdb_indexes()",
           "SELECT * FROM duckdb_constraints()",
           "SELECT * FROM duckdb_schemas()",
           "SELECT * FROM pragma_database_size()",
           "SELECT database_name, count(*) FROM duckdb_columns() GROUP BY ALL",
           "FROM duckdb_views() SELECT sql",
           "SHOW TABLES",
           "SHOW ALL TABLES",
           "SELECT * FROM information_schema.columns",
           "SELECT * FROM information_schema.views",
           "SELECT * FROM information_schema.table_constraints",
           "SELECT * FROM pg_catalog.pg_class",
           "SELECT * FROM pg_catalog.pg_namespace",
       }) {
    auto t = joe_->Run(sql);
    if (!t.ok()) {
      // Refusing is fine; it must not say why in a way that names beta's objects.
      const auto error = t.status().ToString();
      EXPECT_EQ(error.find("beta"), std::string::npos) << sql << ": " << error;
      continue;
    }
    const std::string text = AllText(*t);
    EXPECT_EQ(text.find("beta_db"), std::string::npos)
        << sql << " listed another tenant's catalog";
    EXPECT_EQ(text.find("secrets"), std::string::npos)
        << sql << " listed another tenant's objects";
    EXPECT_EQ(text.find("hush"), std::string::npos)
        << sql << " listed another tenant's columns";
  }
  // Positive twins: joe's own objects are there.
  auto own = joe_->Run("SELECT table_name FROM acme_db.main.all_tables");
  ASSERT_TRUE(own.ok()) << own.status().ToString();
  EXPECT_TRUE(Column(*own).count("orders"));
  auto own_defs = joe_->Run("SELECT \"TABLE_NAME\" FROM acme_db.main.all_defs");
  ASSERT_TRUE(own_defs.ok()) << own_defs.status().ToString();
  EXPECT_TRUE(Column(*own_defs).count("all_views"));
  // The administrator still sees everything through the same views.
  auto all = admin_->Run(
      "SELECT \"TABLE_NAME\" FROM _gizmosql_system.main.gizmosql_view_definition");
  ASSERT_TRUE(all.ok());
  EXPECT_TRUE(Column(*all).count("secrets_view"));

  // Naming the other tenant's objects inside a metadata call or a setting.
  const std::string missing = Error(*joe_, "SELECT * FROM no_such_db.main.t");
  for (const char* sql :
       {"SELECT * FROM pragma_table_info('beta_db.main.secrets')",
        "PRAGMA table_info('beta_db.main.secrets')",
        "SELECT * FROM pragma_show('beta_db.main.secrets')",
        "DESCRIBE beta_db.main.secrets", "SUMMARIZE beta_db.main.secrets",
        "SET search_path = 'beta_db.main'", "SET schema = 'beta_db.main'",
        "SET search_path = 'acme_db.main,\"beta_db\".main'", "USE beta_db",
        "USE beta_db.main"}) {
    EXPECT_EQ(Error(*joe_, sql), missing)
        << sql << " told joe more than a missing catalog would";
  }
  // Positive twins.
  EXPECT_EQ(Error(*joe_, "SELECT * FROM pragma_table_info('acme_db.main.orders')"), "");
  EXPECT_EQ(Error(*joe_, "DESCRIBE acme_db.main.orders"), "");
  EXPECT_EQ(Error(*joe_, "SET search_path = 'acme_db.main'"), "");
  EXPECT_EQ(Error(*joe_, "USE acme_db"), "");
}

TEST_F(RbacTenantsFixture, NothingCrossesATenant) {
  for (const auto& [who, sql, why] :
       std::vector<std::tuple<Client*, std::string, std::string>>{
           // a tenant admin manages only its own tenant's principals
           {boss_.get(), "CREATE USER beta/sneak",
            "only administrators manage users and roles"},
           {boss_.get(), "CREATE USER plain_name",
            "only administrators manage users and roles"},
           {boss_.get(), "CREATE ROLE beta/r",
            "only administrators manage users and roles"},
           {boss_.get(), "DROP USER beta/bea",
            "only administrators manage users and roles"},
           {boss_.get(), "ALTER USER beta/bea DISABLE",
            "only administrators manage users and roles"},
           // its roles hold privileges only on its catalogs
           {boss_.get(), "GRANT SELECT ON beta_db.main.secrets TO acme/readers",
            "only on that tenant's catalogs"},
           {admin_.get(), "GRANT SELECT ON beta_db.main.secrets TO acme/readers",
            "only on that tenant's catalogs"},
           {admin_.get(), "GRANT SELECT ON SCHEMA rbac_tenants_test.main TO acme/readers",
            "only on that tenant's catalogs"},
           // roles never cross
           {boss_.get(), "GRANT beta/tenant_admin TO acme/joe",
            "may not grant or revoke role"},
           {admin_.get(), "GRANT beta/tenant_admin TO acme/joe",
            "roles never cross tenants"},
           {admin_.get(), "GRANT acme/readers TO beta/bea", "roles never cross tenants"},
           // no system privileges for a tenant
           {admin_.get(), "GRANT READ FILESYSTEM TO acme/readers", "cannot hold them"},
           // tenants and catalogs are the administrator's
           {boss_.get(), "ALTER CATALOG beta_db SET TENANT acme",
            "only administrators manage tenants"},
           {boss_.get(), "CREATE TENANT gamma", "only administrators manage tenants"},
           {admin_.get(), "ALTER CATALOG beta_db SET TENANT acme", "UNSET TENANT first"},
           {admin_.get(), "CREATE USER nosuch/x", "tenant nosuch does not exist"},
           {admin_.get(), "DROP TENANT acme", "still has users"},
       }) {
    const auto error = Error(*who, sql);
    EXPECT_NE(error, "") << sql << " was allowed";
    EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  // A view in acme's catalog over beta's, even the administrator's, does not
  // let acme read beta.
  ASSERT_EQ(AdminError("CREATE OR REPLACE VIEW acme_db.main.peek AS SELECT * FROM "
                       "beta_db.main.secrets"),
            "");
  ASSERT_EQ(AdminError("GRANT SELECT ON acme_db.main.peek TO acme/readers"), "");
  EXPECT_EQ(Error(*joe_, "SELECT * FROM acme_db.main.peek"),
            Error(*joe_, "SELECT * FROM no_such_db.main.t"));
  ASSERT_EQ(AdminError("DROP VIEW acme_db.main.peek"), "");
  // bea, beta's admin, sees none of acme either.
  EXPECT_EQ(Error(*bea_, "SELECT * FROM acme_db.main.orders"),
            Error(*bea_, "SELECT * FROM no_such_db.main.t"));
}

TEST_F(RbacTenantsFixture, ShowTenants) {
  // The administrator sees every tenant.
  auto all = admin_->Run("SHOW TENANTS");
  ASSERT_TRUE(all.ok()) << all.status().ToString();
  EXPECT_EQ(Column(*all), (std::set<std::string>{"acme", "beta"}));
  const std::string text = AllText(*all);
  EXPECT_NE(text.find("acme_db"), std::string::npos) << text;
  // A tenant's users see only their own.
  auto own = joe_->Run("SHOW TENANTS");
  ASSERT_TRUE(own.ok()) << own.status().ToString();
  EXPECT_EQ(Column(*own), (std::set<std::string>{"acme"}));
  EXPECT_EQ(AllText(*own).find("beta"), std::string::npos) << AllText(*own);
  // DROP TENANT names every user still in it.
  const auto error = AdminError("DROP TENANT acme");
  EXPECT_NE(error.find("acme/boss, acme/joe"), std::string::npos) << error;
}

TEST_F(RbacTenantsFixture, ChangingATenantsCatalogsEndsItsSessions) {
  ASSERT_EQ(AdminError("ATTACH ':memory:' AS acme_extra"), "");
  ASSERT_EQ(AdminError("CREATE TABLE acme_extra.main.extra AS SELECT 1 AS x"), "");
  Client joe(GetPort(), "acme/joe", "user", 30);
  ASSERT_TRUE(joe.Run("SELECT 1").ok());
  ASSERT_EQ(AdminError("ALTER CATALOG acme_extra SET TENANT acme"), "");
  EXPECT_FALSE(joe.Run("SELECT 1").ok())
      << "a session kept the old view of its tenant's catalogs";
  Client fresh(GetPort(), "acme/joe", "user", 31);
  auto t = fresh.Run("SELECT database_name FROM duckdb_databases() WHERE NOT internal");
  ASSERT_TRUE(t.ok()) << t.status().ToString();
  EXPECT_EQ(Column(*t),
            (std::set<std::string>{"_gizmosql_system", "acme_db", "acme_extra"}));
  ASSERT_EQ(AdminError("ALTER CATALOG acme_extra UNSET TENANT"), "");
}

TEST_F(RbacTenantsFixture, ATenantsUserBuildsViewsOnItsOwnTables) {
  // Positive twin of the cross-tenant views refused above (a session of its
  // own: an earlier test ends joe's sessions on purpose).
  Client joe(GetPort(), "acme/joe", "user", 60);
  EXPECT_EQ(Error(joe,
                  "CREATE OR REPLACE VIEW acme_db.main.own_v AS SELECT * FROM "
                  "acme_db.main.orders"),
            "");
  EXPECT_EQ(
      Error(joe,
            "CREATE OR REPLACE TEMP VIEW own_tv AS SELECT * FROM acme_db.main.orders"),
      "");
  // Temporary tables are the session's own too.
  EXPECT_EQ(Error(joe, "CREATE TEMP TABLE own_tt AS SELECT * FROM acme_db.main.orders"),
            "");
  EXPECT_EQ(Error(joe, "SELECT * FROM own_tt"), "");
}
