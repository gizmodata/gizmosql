// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// The access-control backstop: a statement whose plan never reached the check
// (here: a database without the plan-check hook, standing in for any way the
// optimizer might be skipped) is refused for a store user. No server.

#include <gtest/gtest.h>

#include <duckdb.hpp>

#include "enterprise/rbac/rbac_enforcement.h"

using namespace gizmosql::enterprise::rbac;

namespace {

std::shared_ptr<RbacEngine> MemoryEngine() {
  RbacEngine::Options options;
  options.store_uri = "memory";
  options.password_iterations = 1000;
  options.poll_interval = std::chrono::milliseconds(0);
  return RbacEngine::Create(options).ValueOrDie();
}

}  // namespace

TEST(RbacBackstop, UncheckedPlansAreRefusedForStoreUsers) {
  auto engine = MemoryEngine();
  duckdb::DuckDB db(nullptr);  // no plan-check hook registered
  duckdb::Connection con(db);
  ASSERT_FALSE(con.Query("CREATE TABLE t AS SELECT 1 AS x")->HasError());
  AttachRbacSession(*con.context, engine, Principal{false, "some-user-id", "joe"});

  auto result = con.Query("SELECT * FROM t");
  ASSERT_TRUE(result->HasError()) << "an unchecked plan ran";
  EXPECT_NE(result->GetError().find("could not be checked"), std::string::npos)
      << result->GetError();

  // A client-side prepared statement may be prepared (DuckDB 2.x re-plans it
  // before running), but it can never RUN unchecked.
  auto prepared = con.Prepare("SELECT * FROM t");
  if (!prepared->HasError()) {
    duckdb::vector<duckdb::Value> no_params;
    auto run = prepared->Execute(no_params);
    ASSERT_TRUE(run->HasError()) << "a prepared statement ran unchecked";
    EXPECT_NE(run->GetError().find("could not be checked"), std::string::npos)
        << run->GetError();
  }
}

TEST(RbacBackstop, SuperusersAndInternalStatementsAreNotRefused) {
  auto engine = MemoryEngine();
  duckdb::DuckDB db(nullptr);
  duckdb::Connection admin(db);
  AttachRbacSession(*admin.context, engine, Principal::Superuser("admin"));
  EXPECT_FALSE(admin.Query("SELECT 42")->HasError());

  duckdb::Connection user(db);
  AttachRbacSession(*user.context, engine, Principal{false, "some-user-id", "joe"});
  RbacSessionState::BypassScope internal(*user.context);
  EXPECT_FALSE(user.Query("SELECT 42")->HasError());
}
