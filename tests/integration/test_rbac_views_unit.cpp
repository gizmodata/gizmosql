// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Owner's rights for views, on an embedded database (no server): an owner
// record applies only to the view whose definition it was recorded for, and a
// view's marker never reaches execution unchecked.

#include <gtest/gtest.h>

#include <duckdb.hpp>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>

#include "duckdb_compat.h"
#include "enterprise/rbac/rbac_ddl.h"
#include "enterprise/rbac/rbac_duckdb.h"
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

class RbacViewsUnit : public ::testing::Test {
 protected:
  void SetUp() override {
#if !GIZMOSQL_DUCKDB_HAS_VIEW_HOOK
    GTEST_SKIP() << "this DuckDB has no view-binding hook";
#endif
    engine_ = MemoryEngine();
    RegisterRbacEnforcement(config_);
    db_ = std::make_unique<duckdb::DuckDB>(nullptr, &config_);
    ASSERT_TRUE(FinishRbacEnforcement(*db_).ok());
    admin_ = std::make_unique<duckdb::Connection>(*db_);  // GizmoSQL's own: unchecked
    for (const char* sql : {"CREATE TABLE secret AS SELECT 'TOP' AS s",
                            "CREATE VIEW v AS SELECT s FROM secret"}) {
      ASSERT_FALSE(admin_->Query(sql)->HasError()) << sql;
    }
    for (const char* sql : {"CREATE USER joe", "CREATE ROLE r", "GRANT r TO joe",
                            "GRANT SELECT ON main.v TO r"}) {
      auto parsed = ParseRbacStatement(sql);
      ASSERT_TRUE(parsed.ok()) << sql;
      SessionObjectResolver resolver(*admin_);
      auto done = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver);
      ASSERT_TRUE(done.ok()) << sql << " -> " << done.status().ToString();
    }
    joe_ = std::make_unique<duckdb::Connection>(*db_);
    AttachRbacSession(*joe_->context, engine_,
                      engine_->PrincipalForUser("joe").ValueOrDie());
  }

  ObjectPath ViewPath() { return {"memory", {"main"}, "v"}; }

  std::string CurrentHash() {
    std::string hash;
    admin_->context->RunFunctionInTransaction([&] {
      auto& view = duckdb::Catalog::GetEntry<duckdb::ViewCatalogEntry>(
          *admin_->context, "memory", "main", "v");
      hash = ViewDefinitionHash(view.GetQuery());
    });
    return hash;
  }

  duckdb::DBConfig config_;
  std::shared_ptr<RbacEngine> engine_;
  std::unique_ptr<duckdb::DuckDB> db_;
  std::unique_ptr<duckdb::Connection> admin_, joe_;
};

}  // namespace

TEST_F(RbacViewsUnit, AnOwnerRecordAppliesOnlyToTheDefinitionItWasRecordedFor) {
  // No owner: the view reads with joe's rights, and joe cannot read `secret`.
  auto r = joe_->Query("SELECT * FROM v");
  ASSERT_TRUE(r->HasError()) << "an unowned view lent its reader access";
  EXPECT_NE(r->GetError().find("which has no owner"), std::string::npos) << r->GetError();

  // A record left by some earlier view of this name does not apply.
  ASSERT_TRUE(engine_
                  ->RecordOwner(
                      ViewPath(), ObjectType::kView, Principal::Superuser("admin"),
                      "0000000000000000000000000000000000000000000000000000000000000000")
                  .ok());
  r = joe_->Query("SELECT * FROM v");
  ASSERT_TRUE(r->HasError()) << "a stale owner record applied to a different view";
  EXPECT_NE(r->GetError().find("which has no owner"), std::string::npos) << r->GetError();

  // The record for this definition does.
  const std::string hash = CurrentHash();
  ASSERT_EQ(hash.size(), 64u);
  ASSERT_TRUE(engine_
                  ->RecordOwner(ViewPath(), ObjectType::kView,
                                Principal::Superuser("admin"), hash)
                  .ok());
  r = joe_->Query("SELECT * FROM v");
  ASSERT_FALSE(r->HasError()) << r->GetError();
  EXPECT_EQ(gizmosql::ddb::compat::ResultValue(*r, /*column=*/0, /*row=*/0).ToString(),
            "TOP");

  // Replacing the view without recording its owner (as a statement whose
  // owner record failed to save would) takes the owner's rights away again.
  ASSERT_FALSE(
      admin_->Query("CREATE OR REPLACE VIEW v AS SELECT s || '!' AS s FROM secret")
          ->HasError());
  EXPECT_NE(CurrentHash(), hash);
  r = joe_->Query("SELECT * FROM v");
  ASSERT_TRUE(r->HasError()) << "the old owner record applied to the replaced view";
  EXPECT_NE(r->GetError().find("which has no owner"), std::string::npos) << r->GetError();
}

TEST_F(RbacViewsUnit, AViewNeverRunsUnchecked) {
  ASSERT_TRUE(engine_
                  ->RecordOwner(ViewPath(), ObjectType::kView,
                                Principal::Superuser("admin"), CurrentHash())
                  .ok());
  ASSERT_FALSE(joe_->Query("SELECT * FROM v")->HasError());

  // With the optimizer off, the check (and the removal of the view's marker)
  // never runs: the statement must not run either. (Users may not turn it off;
  // GizmoSQL's own statements may, which stands in for any way it is skipped.)
  {
    RbacSessionState::BypassScope internal(*joe_->context);
    auto set = joe_->Query("PRAGMA disable_optimizer");
    ASSERT_FALSE(set->HasError()) << set->GetError();
  }
  auto r = joe_->Query("SELECT * FROM v");
  ASSERT_TRUE(r->HasError()) << "a view ran without the access check";
  EXPECT_NE(r->GetError().find("GizmoSQL blocked"), std::string::npos) << r->GetError();
}
