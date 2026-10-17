// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// The plan check requires system privileges on its own, without the admin
// command gate in front of it (an embedded database, no server): whatever
// slips past the gate's text checks still meets this.

#include <gtest/gtest.h>

#include <cstdlib>

#include <filesystem>
#include <unistd.h>

#include <duckdb.hpp>

#include "enterprise/rbac/rbac_ddl.h"
#include "enterprise/rbac/rbac_duckdb.h"
#include "enterprise/rbac/rbac_enforcement.h"

using namespace gizmosql::enterprise::rbac;

namespace {

class RbacSystemUnit : public ::testing::Test {
 protected:
  void SetUp() override {
    RbacEngine::Options options;
    options.store_uri = "memory";
    options.password_iterations = 1000;
    options.poll_interval = std::chrono::milliseconds(0);
    engine_ = RbacEngine::Create(options).ValueOrDie();
    RegisterRbacEnforcement(config_);
    db_ = std::make_unique<duckdb::DuckDB>(nullptr, &config_);
    admin_ = std::make_unique<duckdb::Connection>(*db_);
    dir_ = (std::filesystem::temp_directory_path() /
            ("gizmosql_rbac_sysunit_" + std::to_string(getpid())))
               .string();
    std::filesystem::create_directories(dir_);
    ASSERT_FALSE(admin_->Query("CREATE TABLE t AS SELECT 1 AS id")->HasError());
    // The s3 secret type comes with httpfs, which DuckDB loads (installing it
    // if needed) when a statement uses it, as on a GizmoSQL server.
    for (const char* sql : {"SET autoinstall_known_extensions = true",
                            "SET autoload_known_extensions = true"}) {
      auto set = admin_->Query(sql);
      ASSERT_FALSE(set->HasError()) << sql << ": " << set->GetError();
    }
    if (const char* dir = std::getenv("GIZMOSQL_TEST_EXTENSION_DIRECTORY")) {
      // Lets a run start from no installed extensions, as on a CI runner.
      auto set = admin_->Query(std::string("SET extension_directory = '") + dir + "'");
      ASSERT_FALSE(set->HasError()) << set->GetError();
    }
    ASSERT_FALSE(
        admin_->Query("COPY (SELECT 7 AS n) TO '" + dir_ + "/f.parquet' (FORMAT parquet)")
            ->HasError());
    for (const char* sql : {"CREATE USER pat", "CREATE ROLE ops", "GRANT ops TO pat",
                            "GRANT SELECT ON main.t TO ops"}) {
      Admin(sql);
    }
    pat_ = std::make_unique<duckdb::Connection>(*db_);
    AttachRbacSession(*pat_->context, engine_,
                      engine_->PrincipalForUser("pat").ValueOrDie());
  }
  void TearDown() override { std::filesystem::remove_all(dir_); }

  void Admin(const std::string& sql) {
    auto parsed = ParseRbacStatement(sql);
    ASSERT_TRUE(parsed.ok()) << sql;
    SessionObjectResolver resolver(*admin_);
    auto r = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver);
    ASSERT_TRUE(r.ok()) << sql << " -> " << r.status().ToString();
  }
  // "" when it ran, else the error.
  std::string Pat(const std::string& sql) {
    auto r = pat_->Query(sql);
    return r->HasError() ? r->GetError() : "";
  }
  // Refused without `privilege`, allowed once ops holds it.
  void Check(const std::string& privilege, const std::string& sql) {
    const auto refused = Pat(sql);
    EXPECT_NE(refused.find("needs the " + privilege + " privilege"), std::string::npos)
        << sql << " -> " << refused;
    Admin("GRANT " + privilege + " TO ops");
    EXPECT_EQ(Pat(sql), "") << sql;
    Admin("REVOKE " + privilege + " FROM ops");
  }

  duckdb::DBConfig config_;
  std::shared_ptr<RbacEngine> engine_;
  std::unique_ptr<duckdb::DuckDB> db_;
  std::unique_ptr<duckdb::Connection> admin_, pat_;
  std::string dir_;
};

}  // namespace

TEST_F(RbacSystemUnit, EachStatementFamilyNeedsItsPrivilege) {
  Check("READ FILESYSTEM", "SELECT * FROM read_parquet('" + dir_ + "/f.parquet')");
  Check("READ FILESYSTEM", "SELECT * FROM '" + dir_ + "/f.parquet'");
  Check("WRITE FILESYSTEM", "COPY (SELECT * FROM t) TO '" + dir_ + "/out.csv'");
  Check("MANAGE CATALOGS", "ATTACH ':memory:' AS pat_scratch");
  Check("MANAGE CATALOGS", "DETACH pat_scratch");
  Check("CHECKPOINT", "CHECKPOINT");
  Check("MANAGE SECRETS",
        "CREATE TEMPORARY SECRET pat_s (TYPE s3, KEY_ID 'k', SECRET 'v')");
  Check("MANAGE SECRETS", "SELECT * FROM duckdb_secrets()");
  Check("MANAGE SECRETS", "DROP SECRET pat_s");
  Check("MANAGE EXTENSIONS", "LOAD parquet");
  Check("MANAGE CONFIG", "SET GLOBAL threads = 2");
}

TEST_F(RbacSystemUnit, OnePrivilegeDoesNotCoverAnother) {
  Admin("GRANT WRITE FILESYSTEM TO ops");
  const auto refused = Pat("COPY (SELECT * FROM read_parquet('" + dir_ +
                           "/f.parquet')) TO '" + dir_ + "/o2.csv'");
  EXPECT_NE(refused.find("needs the READ FILESYSTEM privilege"), std::string::npos)
      << refused;
}
