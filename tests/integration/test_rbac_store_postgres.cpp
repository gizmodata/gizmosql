// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// The access-control store on a real PostgreSQL: replicas see each other's
// changes, concurrent writers serialize without losing or duplicating
// anything, data survives a restart, and connection errors never echo the
// connection string.
//
// Runs when GIZMOSQL_TEST_RBAC_PG_URI names a PostgreSQL database the tests
// may write to (they drop and recreate the gizmosql_rbac schema), e.g.
//   docker run -d -e POSTGRES_PASSWORD=rbac_test_pw -p 127.0.0.1:55432:5432 postgres:16-alpine
//   export GIZMOSQL_TEST_RBAC_PG_URI=postgresql://postgres:rbac_test_pw@127.0.0.1:55432/postgres
// When the variable is set, an unreachable database is a FAILURE, never a skip.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <thread>

#include <duckdb.hpp>

#include "enterprise/rbac/rbac_engine.h"

using namespace gizmosql::enterprise::rbac;

namespace {

const char* PgUri() { return std::getenv("GIZMOSQL_TEST_RBAC_PG_URI"); }

class FixedResolver : public ObjectResolver {
 public:
  arrow::Result<ResolvedObject> ResolveObject(const QualifiedName& n) override {
    return ResolvedObject{{"lake", {"main"}, n.parts.back()}, ObjectType::kTable, {"id"}};
  }
  arrow::Result<ObjectPath> ResolveSchema(const QualifiedName& n) override {
    return ObjectPath{"lake", {n.parts.back()}, ""};
  }
  arrow::Result<std::string> ResolveCatalog(const std::string& name) override { return name; }
};

class RbacPostgresStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!PgUri()) {
      GTEST_SKIP() << "GIZMOSQL_TEST_RBAC_PG_URI is not set; PostgreSQL store tests not run";
    }
    // Start from an empty store.
    duckdb::DuckDB db(nullptr);
    duckdb::Connection conn(db);
    auto load = conn.Query("LOAD postgres");
    if (load->HasError()) {
      ASSERT_FALSE(conn.Query("INSTALL postgres")->HasError());
      ASSERT_FALSE(conn.Query("LOAD postgres")->HasError());
    }
    std::string uri = PgUri();
    auto attach = conn.Query("ATTACH '" + uri + "' AS pg (TYPE postgres)");
    ASSERT_FALSE(attach->HasError())
        << "GIZMOSQL_TEST_RBAC_PG_URI is set but PostgreSQL is unreachable: "
        << attach->GetError();
    auto drop = conn.Query("CALL postgres_execute('pg', 'DROP SCHEMA IF EXISTS gizmosql_rbac CASCADE')");
    ASSERT_FALSE(drop->HasError()) << drop->GetError();
  }

  std::shared_ptr<RbacEngine> NewEngine(std::chrono::milliseconds poll = std::chrono::milliseconds(0)) {
    RbacEngine::Options options;
    options.store_uri = PgUri();
    options.password_iterations = 1000;
    options.poll_interval = poll;
    auto engine = RbacEngine::Create(options);
    EXPECT_TRUE(engine.ok()) << engine.status().ToString();
    return engine.ok() ? *engine : nullptr;
  }

  static arrow::Status Run(RbacEngine& engine, const std::string& sql) {
    FixedResolver resolver;
    ARROW_ASSIGN_OR_RAISE(auto cmd, ParseRbacStatement(sql));
    return engine.Execute(cmd, Principal::Superuser("admin"), resolver).status();
  }
};

}  // namespace

TEST_F(RbacPostgresStoreTest, ReplicasSeeEachOthersChanges) {
  auto a = NewEngine();
  auto b = NewEngine(std::chrono::milliseconds(100));  // b polls
  ASSERT_TRUE(a && b);
  ASSERT_TRUE(Run(*a, "CREATE USER joe").ok());
  ASSERT_TRUE(Run(*a, "CREATE ROLE reader").ok());
  ASSERT_TRUE(Run(*a, "GRANT reader TO joe").ok());
  ASSERT_TRUE(Run(*a, "GRANT SELECT ON orders TO reader").ok());

  // b picks the change up by polling, within a few intervals.
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline && !b->snapshot()->FindUserByName("joe")) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  ASSERT_NE(b->snapshot()->FindUserByName("joe"), nullptr) << "replica never saw the change";
  auto joe = b->PrincipalForUser("joe");
  ASSERT_TRUE(joe.ok());
  AccessRequest req{{"lake", {"main"}, "orders"}, ObjectType::kTable, Privilege::kSelect, {}};
  EXPECT_TRUE(b->snapshot()->Check(*joe, req).allowed);

  // A revoke on b reaches a (explicit refresh this time).
  ASSERT_TRUE(Run(*b, "REVOKE SELECT ON orders FROM reader").ok());
  ASSERT_TRUE(a->Refresh().ok());
  EXPECT_FALSE(a->snapshot()->Check(*joe, req).allowed);
}

TEST_F(RbacPostgresStoreTest, ConcurrentWritersSerialize) {
  auto a = NewEngine();
  auto b = NewEngine();
  ASSERT_TRUE(a && b);
  std::atomic<int> failures{0};
  auto writer = [&](RbacEngine& engine, const std::string& prefix) {
    for (int i = 0; i < 15; ++i) {
      if (!Run(engine, "CREATE ROLE " + prefix + std::to_string(i)).ok()) ++failures;
    }
  };
  std::thread ta(writer, std::ref(*a), "a_role_");
  std::thread tb(writer, std::ref(*b), "b_role_");
  ta.join();
  tb.join();
  EXPECT_EQ(failures.load(), 0);
  ASSERT_TRUE(a->Refresh().ok());
  EXPECT_EQ(a->snapshot()->roles().size(), 31u);  // PUBLIC + 30
}

TEST_F(RbacPostgresStoreTest, TheSameNameRacedFromTwoReplicasIsCreatedOnce) {
  auto a = NewEngine();
  auto b = NewEngine();
  ASSERT_TRUE(a && b);
  for (int round = 0; round < 10; ++round) {
    const std::string name = "raced_" + std::to_string(round);
    std::atomic<int> ok{0};
    std::thread ta([&] { ok += Run(*a, "CREATE ROLE " + name).ok(); });
    std::thread tb([&] { ok += Run(*b, "CREATE ROLE " + name).ok(); });
    ta.join();
    tb.join();
    EXPECT_EQ(ok.load(), 1) << name;
  }
  ASSERT_TRUE(a->Refresh().ok());
  EXPECT_EQ(a->snapshot()->roles().size(), 11u);  // PUBLIC + 10, no duplicates
}

TEST_F(RbacPostgresStoreTest, DataSurvivesARestart) {
  {
    auto engine = NewEngine();
    ASSERT_TRUE(engine);
    ASSERT_TRUE(Run(*engine, "CREATE USER joe WITH PASSWORD 'pw'").ok());
    ASSERT_TRUE(Run(*engine, "CREATE ROLE \"Data Team\"").ok());
    ASSERT_TRUE(Run(*engine, "GRANT SELECT (id) ON orders TO \"Data Team\"").ok());
  }
  auto restarted = NewEngine();
  ASSERT_TRUE(restarted);
  EXPECT_TRUE(restarted->AuthenticatePassword("joe", "pw").ok());
  const auto* role = restarted->snapshot()->FindRoleByName("data team");
  ASSERT_NE(role, nullptr);
  EXPECT_EQ(role->name, "Data Team");
  ASSERT_EQ(restarted->snapshot()->grants().size(), 1u);
  EXPECT_EQ(restarted->snapshot()->grants()[0].columns, std::vector<std::string>{"id"});
}

TEST_F(RbacPostgresStoreTest, ConnectionErrorsNeverEchoTheConnectionString) {
  std::string bad = PgUri();
  // Same server, wrong password.
  const std::string secret = "not-the-password-8c1f";
  auto at = bad.find(':', bad.find("://") + 3);
  auto amp = bad.find('@');
  ASSERT_NE(at, std::string::npos);
  ASSERT_NE(amp, std::string::npos);
  bad = bad.substr(0, at + 1) + secret + bad.substr(amp);
  RbacEngine::Options options;
  options.store_uri = bad;
  options.poll_interval = std::chrono::milliseconds(0);
  auto engine = RbacEngine::Create(options);
  ASSERT_FALSE(engine.ok());
  const std::string message = engine.status().message();
  EXPECT_EQ(message.find(secret), std::string::npos) << message;
  EXPECT_NE(message.find("could not connect to the PostgreSQL store"), std::string::npos);
  // The reason survives, so an operator can still tell what went wrong.
  EXPECT_NE(message.find("password authentication failed"), std::string::npos) << message;

  // The same with a libpq key=value connection string.
  options.store_uri = "host=127.0.0.1 port=55432 user=postgres dbname=postgres password=" + secret;
  auto keyvalue = RbacEngine::Create(options);
  ASSERT_FALSE(keyvalue.ok());
  EXPECT_EQ(keyvalue.status().message().find(secret), std::string::npos)
      << keyvalue.status().message();
}
