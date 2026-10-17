// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// System privileges (RBAC 1.1) over Flight SQL: each one lets its holders past
// the admin command gate for one family of statements, and the plan check
// requires it again. A statement passes only if everything gated in it is
// covered. Every refusal is asserted with its reason, next to a holder who may.
// Needs a license that includes RBAC.

#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <unistd.h>

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
      .set_id("rbac-sys-" + session_id)
      .set_issued_at(std::chrono::system_clock::now())
      .set_expires_at(std::chrono::system_clock::now() + std::chrono::seconds{3600})
      .set_payload_claim("sub", jwt::claim(username))
      .set_payload_claim("role", jwt::claim(role))
      .set_payload_claim("auth_method", jwt::claim(std::string("Basic")))
      .set_payload_claim("instance_id", jwt::claim(std::string("rbac-sys-instance")))
      .set_payload_claim("session_id", jwt::claim(session_id))
      .sign(jwt::algorithm::hs256{"test_secret_key_for_testing"});
}

class Client {
 public:
  Client(int port, const std::string& user, const std::string& role, int n) {
    char sid[64];
    std::snprintf(sid, sizeof(sid), "00000000-0000-4000-7000-%012d", n);
    header_ = {"authorization", "Bearer " + MintToken(user, role, sid)};
    auto loc = arrow::flight::Location::ForGrpcTcp("127.0.0.1", port).ValueOrDie();
    client_ = std::make_unique<FlightSqlClient>(
        arrow::flight::FlightClient::Connect(loc, {}).ValueOrDie());
  }
  arrow::Result<std::shared_ptr<arrow::Table>> Run(const std::string& sql) {
    arrow::flight::FlightCallOptions co;
    co.headers.push_back(header_);
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

 private:
  std::pair<std::string, std::string> header_;
  std::unique_ptr<FlightSqlClient> client_;
};

std::string Dir() {
  static const std::string dir = [] {
    auto d = std::filesystem::temp_directory_path() /
             ("gizmosql_rbac_sys_" + std::to_string(getpid()));
    std::filesystem::remove_all(d);
    std::filesystem::create_directories(d);
    return d.string();
  }();
  return dir;
}

class RbacSystemPrivileges
    : public gizmosql::testing::ServerTestFixture<RbacSystemPrivileges> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "rbac_sys_test.db",
        .port = 31740,
        .health_port = 31741,
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
      for (const std::string& sql : {
               std::string("CREATE TABLE t AS SELECT 1 AS id"),
               std::string("CREATE TABLE hidden AS SELECT 'secret' AS s"),
               "COPY (SELECT 42 AS n) TO '" + Dir() + "/in.parquet' (FORMAT parquet)",
               std::string("CREATE ROLE fs_reader"),
               std::string("CREATE ROLE fs_writer"),
               std::string("CREATE ROLE catalogs"),
               std::string("CREATE ROLE config"),
               std::string("CREATE ROLE checkpointer"),
               std::string("CREATE ROLE secrets"),
               std::string("CREATE ROLE extensions"),
               std::string("CREATE ROLE readers"),
               std::string("GRANT READ FILESYSTEM TO fs_reader"),
               std::string("GRANT CREATE VIEW ON SCHEMA main TO fs_reader"),
               std::string("GRANT WRITE FILESYSTEM TO fs_writer"),
               std::string("GRANT SELECT ON t TO fs_writer"),
               std::string("GRANT MANAGE CATALOGS TO catalogs"),
               std::string("GRANT MANAGE CONFIG TO config"),
               std::string("GRANT CHECKPOINT TO checkpointer"),
               std::string("GRANT MANAGE SECRETS TO secrets"),
               std::string("GRANT MANAGE EXTENSIONS TO extensions"),
               std::string("CREATE USER fay"),
               std::string("CREATE USER wes"),
               std::string("CREATE USER cam"),
               std::string("CREATE USER con"),
               std::string("CREATE USER kit"),
               std::string("CREATE USER sam"),
               std::string("CREATE USER eli"),
               std::string("CREATE USER nob"),
               std::string("GRANT fs_reader TO fay"),
               std::string("GRANT fs_writer TO wes"),
               std::string("GRANT catalogs TO cam"),
               std::string("GRANT config TO con"),
               std::string("GRANT checkpointer TO kit"),
               std::string("GRANT secrets TO sam"),
               std::string("GRANT extensions TO eli"),
               std::string("GRANT readers TO nob"),
           }) {
        auto st = admin_->Run(sql).status();
        ASSERT_TRUE(st.ok()) << sql << " -> " << st.ToString();
      }
      seeded = true;
    }
  }

  Client As(const std::string& user, int n) { return Client(GetPort(), user, "user", n); }

  static void ExpectBlocked(Client& who, const std::string& sql, const std::string& why) {
    auto st = who.Run(sql).status();
    ASSERT_FALSE(st.ok()) << "NOT BLOCKED: " << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked"), std::string::npos)
        << sql << " failed, but not by access control: " << st.ToString();
    EXPECT_NE(st.ToString().find(why), std::string::npos)
        << sql << " -> expected '" << why << "' in: " << st.ToString();
  }
  static std::shared_ptr<arrow::Table> ExpectAllowed(Client& who,
                                                     const std::string& sql) {
    auto r = who.Run(sql);
    EXPECT_TRUE(r.ok()) << "WRONGLY BLOCKED: " << sql << " -> " << r.status().ToString();
    return r.ok() ? *r : nullptr;
  }

  std::unique_ptr<Client> admin_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<RbacSystemPrivileges>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<RbacSystemPrivileges>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<RbacSystemPrivileges>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<RbacSystemPrivileges>::config_{};

TEST_F(RbacSystemPrivileges, ReadFilesystem) {
  auto fay = As("fay", 10);
  auto nob = As("nob", 11);
  const std::string file = Dir() + "/in.parquet";
  auto t = ExpectAllowed(fay, "SELECT n FROM read_parquet('" + file + "')");
  ASSERT_TRUE(t);
  EXPECT_EQ(t->num_rows(), 1);
  ExpectAllowed(fay, "SELECT * FROM '" + file + "'");
  ExpectBlocked(nob, "SELECT n FROM read_parquet('" + file + "')", "read_parquet()");
  ExpectBlocked(nob, "SELECT * FROM '" + file + "'", "reading a local file");
  // Reading files is not writing them, nor reading tables without a grant.
  ExpectBlocked(fay, "COPY (SELECT 1) TO '" + Dir() + "/fay.csv'", "COPY TO");
  ExpectBlocked(fay, "SELECT n, s FROM read_parquet('" + file + "'), hidden",
                "no SELECT privilege on table");
  // Every gated part of a statement must be covered: the file read is, the
  // ATTACH beside it is not.
  ExpectBlocked(fay,
                "SELECT * FROM read_parquet('" + file + "'); ATTACH ':memory:' AS sneaky",
                "ATTACH");
  // query() is only for the administrator, and only the gate can see it (DuckDB
  // binds its SQL away): a covered file read next to it, in the same statement
  // or the next one, must not carry it through.
  ExpectBlocked(fay,
                "SELECT * FROM read_parquet('" + file + "'), query('SELECT 42 AS x')",
                "query()");
  ExpectBlocked(
      fay, "SELECT * FROM read_parquet('" + file + "'); SELECT * FROM query('SELECT 42')",
      "query()");
}

TEST_F(RbacSystemPrivileges, AFileViewReadsWithItsOwnersFilesystemPrivilege) {
  auto fay = As("fay", 12);
  auto nob = As("nob", 13);
  const std::string file = Dir() + "/in.parquet";
  ExpectAllowed(fay,
                "CREATE VIEW fay_file AS SELECT n FROM read_parquet('" + file + "')");
  ExpectAllowed(*admin_, "GRANT SELECT ON fay_file TO readers");
  ExpectAllowed(nob, "SELECT * FROM fay_file");  // nob has no READ FILESYSTEM himself
  ExpectAllowed(*admin_, "REVOKE READ FILESYSTEM FROM fs_reader");
  ExpectBlocked(nob, "SELECT * FROM fay_file", "its owner lacks a privilege");
  ExpectAllowed(*admin_, "GRANT READ FILESYSTEM TO fs_reader");
}

TEST_F(RbacSystemPrivileges, WriteFilesystem) {
  auto wes = As("wes", 20);
  auto nob = As("nob", 21);
  ExpectAllowed(wes, "COPY (SELECT * FROM t) TO '" + Dir() + "/wes.csv'");
  EXPECT_TRUE(std::filesystem::exists(Dir() + "/wes.csv"));
  ExpectBlocked(wes, "COPY (SELECT * FROM hidden) TO '" + Dir() + "/wes2.csv'",
                "no SELECT privilege on table");
  ExpectBlocked(nob, "COPY (SELECT 1) TO '" + Dir() + "/nob.csv'", "COPY TO");
  // Writing is not reading: a COPY that reads a file needs READ FILESYSTEM too.
  ExpectBlocked(wes,
                "COPY (SELECT * FROM read_parquet('" + Dir() + "/in.parquet')) TO '" +
                    Dir() + "/wes3.csv'",
                "read_parquet()");
}

TEST_F(RbacSystemPrivileges, ManageCatalogs) {
  auto cam = As("cam", 30);
  auto nob = As("nob", 31);
  ExpectAllowed(cam, "ATTACH ':memory:' AS cam_scratch");
  ExpectAllowed(cam, "DETACH cam_scratch");
  ExpectBlocked(nob, "ATTACH ':memory:' AS nob_scratch", "ATTACH");
}

TEST_F(RbacSystemPrivileges, ManageConfig) {
  auto con = As("con", 40);
  auto nob = As("nob", 41);
  auto threads = ExpectAllowed(*admin_, "SELECT current_setting('threads') AS t");
  ASSERT_TRUE(threads);
  const std::string value = threads->column(0)->GetScalar(0).ValueOrDie()->ToString();
  ExpectAllowed(con, "SET GLOBAL threads = " + value);
  ExpectBlocked(nob, "SET GLOBAL threads = " + value, "SET GLOBAL threads");
  // Not the switches the access check itself depends on.
  ExpectBlocked(con, "SET GLOBAL disabled_optimizers = 'filter_pushdown'",
                "disabled_optimizers");
}

TEST_F(RbacSystemPrivileges, Checkpoint) {
  auto kit = As("kit", 50);
  auto nob = As("nob", 51);
  ExpectAllowed(kit, "CHECKPOINT");
  ExpectBlocked(nob, "CHECKPOINT", "CHECKPOINT");
}

TEST_F(RbacSystemPrivileges, ManageSecrets) {
  auto sam = As("sam", 60);
  auto nob = As("nob", 61);
  ExpectAllowed(sam,
                "CREATE TEMPORARY SECRET sam_secret (TYPE s3, KEY_ID 'k', SECRET 'v')");
  auto listed =
      ExpectAllowed(sam, "SELECT name FROM duckdb_secrets() WHERE name = 'sam_secret'");
  ASSERT_TRUE(listed);
  EXPECT_EQ(listed->num_rows(), 1);
  ExpectBlocked(nob, "SELECT name FROM duckdb_secrets()", "duckdb_secrets()");
  ExpectBlocked(nob, "DROP SECRET sam_secret", "DROP SECRET");
  ExpectAllowed(sam, "DROP SECRET sam_secret");
}

TEST_F(RbacSystemPrivileges, ManageExtensions) {
  auto eli = As("eli", 70);
  auto nob = As("nob", 71);
  ExpectAllowed(eli, "LOAD parquet");
  ExpectBlocked(nob, "LOAD parquet", "LOAD extension");
}

TEST_F(RbacSystemPrivileges, ShowGrantsListsThem) {
  auto rows = ExpectAllowed(*admin_, "SHOW GRANTS FOR fay");
  ASSERT_TRUE(rows);
  bool saw = false;
  for (int64_t r = 0; r < rows->num_rows(); ++r) {
    if (rows->column(0)->GetScalar(r).ValueOrDie()->ToString() == "READ FILESYSTEM" &&
        rows->column(1)->GetScalar(r).ValueOrDie()->ToString() == "SYSTEM") {
      saw = true;
    }
  }
  EXPECT_TRUE(saw) << rows->ToString();
}

TEST_F(RbacSystemPrivileges, ARefusalNamesThePrivilegeThatWouldAllowIt) {
  auto nob = As("nob", 80);
  ExpectBlocked(nob, "SELECT * FROM read_parquet('" + Dir() + "/in.parquet')",
                "needs the READ FILESYSTEM system privilege");
  ExpectBlocked(nob, "ATTACH ':memory:' AS x",
                "needs the MANAGE CATALOGS system privilege");
  // What no privilege covers stays the administrator's, and says so.
  ExpectBlocked(nob, "SELECT * FROM query('SELECT 1')", "requires the 'admin' role");
}
