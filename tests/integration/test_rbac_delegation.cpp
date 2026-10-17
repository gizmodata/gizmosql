// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Delegated grants (RBAC 1.1): owners grant on what they own, WITH GRANT
// OPTION and WITH ADMIN OPTION pass the right to grant on, and a REVOKE takes
// away what was granted through the revoked right (CASCADE), or is refused
// (RESTRICT). Engine only: an in-memory store, no server.

#include <gtest/gtest.h>

#include <string>

#include "enterprise/rbac/rbac_engine.h"

using namespace gizmosql::enterprise::rbac;

namespace {

class Resolver : public ObjectResolver {
 public:
  arrow::Result<ResolvedObject> ResolveObject(const QualifiedName& n) override {
    const std::string name = n.parts.back();
    for (const char* t : {"orders", "other"}) {
      if (IdentifiersEqual(name, t)) {
        return ResolvedObject{{"lake", {"main"}, t}, ObjectType::kTable, {"id", "amount", "card"}};
      }
    }
    return arrow::Status::Invalid("table " + n.ToString() + " does not exist");
  }
  arrow::Result<ObjectPath> ResolveSchema(const QualifiedName& n) override {
    return ObjectPath{"lake", {n.parts.back()}, ""};
  }
  arrow::Result<std::string> ResolveCatalog(const std::string& name) override { return name; }
};

class RbacDelegation : public ::testing::Test {
 protected:
  void SetUp() override {
    RbacEngine::Options options;
    options.store_uri = "memory";
    options.password_iterations = 1000;
    options.poll_interval = std::chrono::milliseconds(0);
    options.administrator_name = "admin";
    engine_ = RbacEngine::Create(options).ValueOrDie();
    for (const char* sql : {"CREATE USER ann", "CREATE USER bob", "CREATE USER cat",
                            "CREATE ROLE leads", "CREATE ROLE team", "CREATE ROLE others",
                            "GRANT leads TO ann", "GRANT team TO bob", "GRANT others TO cat"}) {
      Admin(sql);
    }
  }

  void Admin(const std::string& sql) {
    auto parsed = ParseRbacStatement(sql);
    ASSERT_TRUE(parsed.ok()) << sql << " -> " << parsed.status().ToString();
    auto r = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver_);
    ASSERT_TRUE(r.ok()) << sql << " -> " << r.status().ToString();
  }
  // "" when it worked, else the error.
  std::string As(const std::string& user, const std::string& sql) {
    auto parsed = ParseRbacStatement(sql);
    if (!parsed.ok()) return parsed.status().message();
    auto r = engine_->Execute(*parsed, engine_->PrincipalForUser(user).ValueOrDie(), resolver_);
    return r.ok() ? "" : r.status().message();
  }
  bool Can(const std::string& user, Privilege p, std::vector<std::string> columns = {},
           const std::string& table = "orders") {
    AccessRequest req{{"lake", {"main"}, table}, ObjectType::kTable, p, std::move(columns)};
    return engine_->snapshot()->Check(engine_->PrincipalForUser(user).ValueOrDie(), req).allowed;
  }
  void Own(const std::string& user, const std::string& table = "orders") {
    ASSERT_TRUE(engine_
                    ->RecordOwner({"lake", {"main"}, table}, ObjectType::kTable,
                                  engine_->PrincipalForUser(user).ValueOrDie())
                    .ok());
  }

  std::shared_ptr<RbacEngine> engine_;
  Resolver resolver_;
};

}  // namespace

TEST_F(RbacDelegation, AnOwnerGrantsOnWhatItOwnsOnly) {
  Own("ann");
  EXPECT_EQ(As("ann", "GRANT SELECT ON orders TO team"), "");
  EXPECT_TRUE(Can("bob", Privilege::kSelect));
  EXPECT_NE(As("ann", "GRANT SELECT ON other TO team").find("may not grant SELECT on lake.main.other"),
            std::string::npos);
  EXPECT_FALSE(Can("bob", Privilege::kSelect, {}, "other"));
  // Not a schema or catalog it does not own, either.
  EXPECT_NE(As("ann", "GRANT SELECT ON SCHEMA main TO team").find("may not grant"), std::string::npos);
  EXPECT_NE(As("ann", "GRANT SELECT ON CATALOG lake TO team").find("may not grant"), std::string::npos);
}

TEST_F(RbacDelegation, GrantOptionPassesOnExactlyWhatItCovers) {
  Admin("GRANT SELECT (id, amount) ON orders TO leads WITH GRANT OPTION");
  Admin("GRANT INSERT ON orders TO leads");  // no grant option
  EXPECT_EQ(As("ann", "GRANT SELECT (id) ON orders TO team"), "");
  EXPECT_TRUE(Can("bob", Privilege::kSelect, {"id"}));
  EXPECT_FALSE(Can("bob", Privilege::kSelect, {"amount"}));
  // Not a column outside the grant, not the whole table, not another privilege.
  EXPECT_NE(As("ann", "GRANT SELECT (card) ON orders TO team").find("may not grant"), std::string::npos);
  EXPECT_NE(As("ann", "GRANT SELECT ON orders TO team").find("may not grant"), std::string::npos);
  EXPECT_NE(As("ann", "GRANT INSERT ON orders TO team").find("may not grant INSERT"), std::string::npos);
  EXPECT_FALSE(Can("bob", Privilege::kSelect, {"card"}));
  EXPECT_FALSE(Can("bob", Privilege::kInsert));
  // Someone without the option cannot pass on what they were given.
  EXPECT_NE(As("bob", "GRANT SELECT (id) ON orders TO others").find("may not grant"), std::string::npos);
  EXPECT_FALSE(Can("cat", Privilege::kSelect, {"id"}));
}

TEST_F(RbacDelegation, RevokeCascadesThroughWhatWasGrantedWithIt) {
  Admin("GRANT SELECT ON orders TO leads WITH GRANT OPTION");
  ASSERT_EQ(As("ann", "GRANT SELECT ON orders TO team WITH GRANT OPTION"), "");
  ASSERT_EQ(As("bob", "GRANT SELECT ON orders TO others"), "");
  ASSERT_TRUE(Can("cat", Privilege::kSelect));

  // RESTRICT refuses while others depend on it, and changes nothing.
  auto parsed = ParseRbacStatement("REVOKE SELECT ON orders FROM leads RESTRICT");
  ASSERT_TRUE(parsed.ok());
  auto refused = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver_);
  ASSERT_FALSE(refused.ok());
  EXPECT_NE(refused.status().message().find("other grants depend on this one"), std::string::npos);
  EXPECT_TRUE(Can("cat", Privilege::kSelect));

  Admin("REVOKE SELECT ON orders FROM leads");
  EXPECT_FALSE(Can("ann", Privilege::kSelect));
  EXPECT_FALSE(Can("bob", Privilege::kSelect)) << "a grant made through a revoked option survived";
  EXPECT_FALSE(Can("cat", Privilege::kSelect)) << "a second-hand grant survived";
}

TEST_F(RbacDelegation, RevokingOnlyTheGrantOptionKeepsThePrivilege) {
  Admin("GRANT SELECT ON orders TO leads WITH GRANT OPTION");
  ASSERT_EQ(As("ann", "GRANT SELECT ON orders TO team"), "");
  Admin("REVOKE GRANT OPTION FOR SELECT ON orders FROM leads");
  EXPECT_TRUE(Can("ann", Privilege::kSelect));
  EXPECT_FALSE(Can("bob", Privilege::kSelect));
  EXPECT_NE(As("ann", "GRANT SELECT ON orders TO team").find("may not grant"), std::string::npos);
}

TEST_F(RbacDelegation, GrantsMadeThroughEachOtherDoNotKeepEachOtherAlive) {
  // dan owns the table and gives leads the option. ann (leads) passes it to
  // team, and bob (team) passes it back to leads: two grants that each justify
  // the other. Once dan's grant goes, neither has a root and both must fall.
  Admin("CREATE USER dan");
  Own("dan");
  ASSERT_EQ(As("dan", "GRANT SELECT ON orders TO leads WITH GRANT OPTION"), "");
  ASSERT_EQ(As("ann", "GRANT SELECT ON orders TO team WITH GRANT OPTION"), "");
  ASSERT_EQ(As("bob", "GRANT SELECT ON orders TO leads WITH GRANT OPTION"), "");
  ASSERT_EQ(As("dan", "REVOKE SELECT ON orders FROM leads"), "");  // only dan's own record
  EXPECT_FALSE(Can("ann", Privilege::kSelect)) << "a cycle of grants kept itself alive";
  EXPECT_FALSE(Can("bob", Privilege::kSelect)) << "a cycle of grants kept itself alive";
}

TEST_F(RbacDelegation, OnlyTheGrantorOrAnAdministratorRevokes) {
  Admin("GRANT SELECT ON orders TO leads WITH GRANT OPTION");
  Admin("GRANT SELECT ON orders TO team");
  // ann cannot take away the administrator's grant to team...
  EXPECT_NE(As("ann", "REVOKE SELECT ON orders FROM team").find("from another grantor"),
            std::string::npos);
  EXPECT_TRUE(Can("bob", Privilege::kSelect));
  // ...but can revoke her own, which leaves the administrator's in place.
  ASSERT_EQ(As("ann", "GRANT SELECT ON orders TO others"), "");
  EXPECT_EQ(As("ann", "REVOKE SELECT ON orders FROM others"), "");
  EXPECT_FALSE(Can("cat", Privilege::kSelect));
}

TEST_F(RbacDelegation, AdminOptionDelegatesRoleMembership) {
  EXPECT_NE(As("ann", "GRANT team TO cat").find("may not grant or revoke role team"),
            std::string::npos);
  Admin("GRANT team TO ann WITH ADMIN OPTION");
  Admin("GRANT SELECT ON orders TO team");
  EXPECT_EQ(As("ann", "GRANT team TO cat"), "");
  EXPECT_TRUE(Can("cat", Privilege::kSelect));
  // Not a role she cannot administer.
  EXPECT_NE(As("ann", "GRANT others TO bob").find("may not grant or revoke role others"),
            std::string::npos);
  // Taking her admin option away takes away what she granted with it.
  Admin("REVOKE ADMIN OPTION FOR team FROM ann");
  EXPECT_TRUE(Can("ann", Privilege::kSelect));
  EXPECT_FALSE(Can("cat", Privilege::kSelect)) << "a membership granted through a revoked admin option survived";
}

TEST_F(RbacDelegation, AnOwnerHandsAnObjectOnlyToItselfOrItsRoles) {
  Own("ann");
  EXPECT_NE(As("ann", "ALTER TABLE orders OWNER TO bob").find("may not change the owner"),
            std::string::npos);
  EXPECT_NE(As("ann", "ALTER TABLE orders OWNER TO team").find("may not change the owner"),
            std::string::npos);
  EXPECT_NE(As("ann", "ALTER TABLE orders OWNER TO admin").find("may not change the owner"),
            std::string::npos);
  EXPECT_NE(As("bob", "ALTER TABLE orders OWNER TO team").find("may not change the owner"),
            std::string::npos);  // not the owner at all
  EXPECT_EQ(As("ann", "ALTER TABLE orders OWNER TO leads"), "");
  EXPECT_TRUE(Can("ann", Privilege::kDelete));  // through leads, the owner now
}

TEST_F(RbacDelegation, AnOwnersGrantsSurviveAHandOver) {
  Own("ann");
  ASSERT_EQ(As("ann", "GRANT SELECT ON orders TO team"), "");
  Admin("ALTER TABLE orders OWNER TO cat");
  EXPECT_TRUE(Can("bob", Privilege::kSelect)) << "the previous owner's grant fell with the ownership";
  EXPECT_FALSE(Can("ann", Privilege::kSelect));
}

TEST_F(RbacDelegation, UsersAndRolesStayWithTheAdministrator) {
  Admin("GRANT team TO ann WITH ADMIN OPTION");
  for (const char* sql : {"CREATE USER x", "CREATE ROLE x", "DROP ROLE team", "ALTER USER bob DISABLE"}) {
    EXPECT_NE(As("ann", sql).find("only administrators manage users and roles"), std::string::npos)
        << sql;
  }
}

TEST_F(RbacDelegation, SystemPrivilegesAndTheirAdminOption) {
  auto has = [&](const std::string& user, SystemPrivilege p) {
    return engine_->snapshot()->HasSystemPrivilege(engine_->PrincipalForUser(user).ValueOrDie(), p);
  };
  EXPECT_NE(As("ann", "GRANT READ FILESYSTEM TO team").find("may not grant READ FILESYSTEM"),
            std::string::npos);
  Admin("GRANT READ FILESYSTEM, CHECKPOINT TO leads WITH ADMIN OPTION");
  EXPECT_TRUE(has("ann", SystemPrivilege::kReadFilesystem));
  EXPECT_FALSE(has("bob", SystemPrivilege::kReadFilesystem));
  EXPECT_EQ(As("ann", "GRANT READ FILESYSTEM TO team"), "");
  EXPECT_TRUE(has("bob", SystemPrivilege::kReadFilesystem));
  // Only what she holds with the option.
  EXPECT_NE(As("ann", "GRANT MANAGE CATALOGS TO team").find("may not grant MANAGE CATALOGS"),
            std::string::npos);
  EXPECT_NE(As("bob", "GRANT READ FILESYSTEM TO others").find("may not grant"), std::string::npos);
  // Revoking the administrator's grant takes ann's with it.
  Admin("REVOKE READ FILESYSTEM FROM leads");
  EXPECT_FALSE(has("ann", SystemPrivilege::kReadFilesystem));
  EXPECT_FALSE(has("bob", SystemPrivilege::kReadFilesystem)) << "a delegated system privilege survived";
  EXPECT_TRUE(has("ann", SystemPrivilege::kCheckpoint));
}

TEST_F(RbacDelegation, SystemPrivilegeGrammar) {
  for (const auto& [sql, why] : std::vector<std::pair<std::string, std::string>>{
           {"GRANT READ FILESYSTEM ON orders TO team", "no ON clause"},
           {"GRANT MANAGE CATALOGS TO ann", "granted to roles, not users"},
           {"REVOKE GRANT OPTION FOR CHECKPOINT FROM team", ""}}) {
    auto parsed = ParseRbacStatement(sql);
    std::string error = parsed.ok() ? "" : parsed.status().message();
    if (parsed.ok()) {
      auto r = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver_);
      error = r.ok() ? "" : r.status().message();
    }
    EXPECT_FALSE(error.empty()) << sql << " was accepted";
    if (!why.empty()) EXPECT_NE(error.find(why), std::string::npos) << sql << " -> " << error;
  }
  EXPECT_EQ(As("ann", "SHOW GRANTS"), "");
}
