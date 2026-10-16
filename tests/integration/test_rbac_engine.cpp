// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unit tests for the access-control engine (Enterprise RBAC) over an in-memory
// store: users, roles, memberships, grants, ownership, authentication and the
// authorization decision itself. No server.

#include <gtest/gtest.h>

#include <map>
#include <string>

#include "enterprise/rbac/rbac_engine.h"

using namespace gizmosql::enterprise::rbac;

namespace {

// Resolves names against a fixed set of objects in catalog "lake", schema "main".
class FakeResolver : public ObjectResolver {
 public:
  FakeResolver() {
    Add("lake", {"main"}, "orders", ObjectType::kTable, {"id", "amount", "customer"});
    Add("lake", {"main"}, "users", ObjectType::kTable, {"id", "email", "Signup TS"});
    Add("lake", {"main"}, "daily", ObjectType::kView, {"day", "total"});
    Add("lake", {"finance", "reports"}, "q3", ObjectType::kTable, {"x"});
  }
  void Add(const std::string& cat, std::vector<std::string> schema, const std::string& name,
           ObjectType type, std::vector<std::string> cols) {
    objects_.push_back({{cat, std::move(schema), name}, type, std::move(cols)});
  }
  arrow::Result<ResolvedObject> ResolveObject(const QualifiedName& n) override {
    const std::string name = n.parts.back();
    for (const auto& o : objects_) {
      if (IdentifiersEqual(o.path.object_name, name)) return o;
    }
    return arrow::Status::Invalid("table or view " + n.ToString() + " does not exist");
  }
  arrow::Result<ObjectPath> ResolveSchema(const QualifiedName& n) override {
    ObjectPath p;
    p.catalog = n.parts.size() > 1 ? n.parts[0] : "lake";
    p.schema_path.assign(n.parts.begin() + (n.parts.size() > 1 ? 1 : 0), n.parts.end());
    return p;
  }
  arrow::Result<std::string> ResolveCatalog(const std::string& name) override { return name; }

 private:
  std::vector<ResolvedObject> objects_;
};

class RbacEngineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    RbacEngine::Options options;
    options.store_uri = "memory";
    options.password_iterations = 1000;  // fast tests
    options.poll_interval = std::chrono::milliseconds(0);
    auto created = RbacEngine::Create(options);
    ASSERT_TRUE(created.ok()) << created.status().ToString();
    engine_ = *created;
  }

  // Run SQL as the administrator; fail the test on error.
  CommandResult Admin(const std::string& sql) {
    auto parsed = ParseRbacStatement(sql);
    EXPECT_TRUE(parsed.ok()) << sql << " -> " << parsed.status().ToString();
    auto result = engine_->Execute(*parsed, Principal::Superuser("admin"), resolver_);
    EXPECT_TRUE(result.ok()) << sql << " -> " << result.status().ToString();
    return result.ok() ? *result : CommandResult{};
  }
  // Run SQL and return the error message ("" if it succeeded).
  std::string Error(const std::string& sql, const Principal& who = Principal::Superuser("admin")) {
    auto parsed = ParseRbacStatement(sql);
    if (!parsed.ok()) return parsed.status().message();
    auto result = engine_->Execute(*parsed, who, resolver_);
    return result.ok() ? "" : result.status().message();
  }
  Principal User(const std::string& name) {
    auto p = engine_->PrincipalForUser(name);
    EXPECT_TRUE(p.ok()) << p.status().ToString();
    return p.ok() ? *p : Principal{};
  }
  bool Can(const Principal& who, Privilege priv, const std::string& table,
           std::vector<std::string> columns = {}) {
    ObjectPath path{"lake", {"main"}, table};
    if (table == "q3") path = {"lake", {"finance", "reports"}, "q3"};
    AccessRequest req{path, ObjectType::kTable, priv, std::move(columns)};
    return engine_->snapshot()->Check(who, req).allowed;
  }

  std::shared_ptr<RbacEngine> engine_;
  FakeResolver resolver_;
};

}  // namespace

TEST_F(RbacEngineTest, UsersAndRolesShareOneNamespace) {
  Admin("CREATE USER joe WITH PASSWORD 'pw1'");
  Admin("CREATE ROLE analyst");
  EXPECT_NE(Error("CREATE ROLE JOE").find("a user named JOE already exists"), std::string::npos);
  EXPECT_NE(Error("CREATE USER Analyst").find("a role named Analyst already exists"),
            std::string::npos);
  EXPECT_NE(Error("CREATE ROLE public").find("reserved"), std::string::npos);
  EXPECT_NE(Error("DROP ROLE PUBLIC").find("built-in"), std::string::npos);
  // IF NOT EXISTS is a no-op, not an error.
  EXPECT_EQ(Admin("CREATE USER IF NOT EXISTS joe").rows[0][0], "User joe already exists");
  EXPECT_EQ(Error("DROP USER nobody"), "GizmoSQL access control: user nobody does not exist");
  EXPECT_EQ(Error("DROP USER IF EXISTS nobody"), "");
}

TEST_F(RbacEngineTest, PrivilegesFlowThroughRoles) {
  Admin("CREATE USER joe");
  Admin("CREATE ROLE reader");
  Admin("CREATE ROLE analyst");
  Admin("GRANT SELECT ON orders TO reader");
  auto joe = User("joe");
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));  // default deny

  Admin("GRANT reader TO analyst");  // analyst inherits reader
  Admin("GRANT analyst TO joe");
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "orders"));
  EXPECT_FALSE(Can(joe, Privilege::kInsert, "orders"));
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "users"));

  Admin("REVOKE reader FROM analyst");
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));
}

TEST_F(RbacEngineTest, RoleCyclesAndSelfGrantsAreRefused) {
  Admin("CREATE ROLE a");
  Admin("CREATE ROLE b");
  Admin("CREATE ROLE c");
  Admin("GRANT a TO b");
  Admin("GRANT b TO c");
  EXPECT_NE(Error("GRANT c TO a").find("cycle"), std::string::npos);
  EXPECT_NE(Error("GRANT a TO a").find("itself"), std::string::npos);
  EXPECT_NE(Error("GRANT PUBLIC TO a").find("PUBLIC"), std::string::npos);
}

TEST_F(RbacEngineTest, PrivilegesGoToRolesNotUsers) {
  Admin("CREATE USER joe");
  EXPECT_NE(Error("GRANT SELECT ON orders TO joe").find("granted to roles, not users"),
            std::string::npos);
  EXPECT_NE(Error("GRANT SELECT ON orders TO nobody").find("does not exist"), std::string::npos);
  EXPECT_NE(Error("GRANT SELECT ON missing_table TO PUBLIC").find("does not exist"),
            std::string::npos);
}

TEST_F(RbacEngineTest, PublicReachesEveryUser) {
  Admin("CREATE USER joe");
  Admin("GRANT SELECT ON orders TO PUBLIC");
  EXPECT_TRUE(Can(User("joe"), Privilege::kSelect, "orders"));
}

TEST_F(RbacEngineTest, SchemaAndCatalogGrantsCoverEverythingInside) {
  Admin("CREATE USER joe");
  Admin("CREATE ROLE r");
  Admin("GRANT r TO joe");
  auto joe = User("joe");
  Admin("GRANT SELECT ON SCHEMA lake.finance TO r");
  // A schema grant covers nested schemas (DuckDB 2.x).
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "q3"));
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));
  Admin("GRANT INSERT ON CATALOG lake TO r");
  EXPECT_TRUE(Can(joe, Privilege::kInsert, "orders"));
  EXPECT_TRUE(Can(joe, Privilege::kInsert, "q3"));
  EXPECT_FALSE(Can(joe, Privilege::kDelete, "orders"));
  Admin("GRANT ALL ON lake.main.users TO r");
  EXPECT_TRUE(Can(joe, Privilege::kDelete, "users"));
  EXPECT_TRUE(Can(joe, Privilege::kUpdate, "users"));
  Admin("REVOKE ALL ON lake.main.users FROM r");
  EXPECT_FALSE(Can(joe, Privilege::kDelete, "users"));
}

TEST_F(RbacEngineTest, ColumnGrants) {
  Admin("CREATE USER joe");
  Admin("CREATE ROLE r");
  Admin("GRANT r TO joe");
  auto joe = User("joe");
  Admin("GRANT SELECT (email) ON users TO r");
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "users", {"email"}));
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "users", {"EMAIL"}));  // case-insensitive
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "users", {"email", "id"}));
  // Column grants add up.
  Admin("GRANT SELECT (id) ON users TO r");
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "users", {"email", "id"}));
  // A column that does not exist is refused at GRANT time.
  EXPECT_NE(Error("GRANT SELECT (password) ON users TO r").find("column password does not exist"),
            std::string::npos);
  // Revoking one column from a whole-table grant keeps the others.
  Admin("GRANT SELECT ON orders TO r");
  Admin("REVOKE SELECT (customer) ON orders FROM r");
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "orders", {"id", "amount"}));
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders", {"customer"}));
}

TEST_F(RbacEngineTest, OwnersHoldEveryPrivilegeOnTheirObject) {
  Admin("CREATE USER joe");
  Admin("CREATE ROLE eng");
  Admin("CREATE USER ann");
  Admin("GRANT eng TO ann");
  auto joe = User("joe");
  auto ann = User("ann");
  ASSERT_TRUE(engine_->RecordOwner({"lake", {"main"}, "orders"}, ObjectType::kTable, joe).ok());
  EXPECT_TRUE(Can(joe, Privilege::kDelete, "orders"));
  EXPECT_FALSE(Can(ann, Privilege::kSelect, "orders"));
  // Ownership by a role reaches its members; the old owner loses it.
  Admin("ALTER TABLE orders OWNER TO ROLE eng");
  EXPECT_TRUE(Can(ann, Privilege::kSelect, "orders"));
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));
  EXPECT_NE(Error("DROP ROLE eng").find("owns"), std::string::npos);
  EXPECT_NE(Error("ALTER VIEW orders OWNER TO joe").find("is a table"), std::string::npos);
}

TEST_F(RbacEngineTest, DisabledAndDroppedUsersLoseEverything) {
  Admin("CREATE USER joe WITH PASSWORD 'pw'");
  Admin("GRANT SELECT ON orders TO PUBLIC");
  auto joe = User("joe");
  EXPECT_TRUE(Can(joe, Privilege::kSelect, "orders"));
  auto disable = Admin("ALTER USER joe DISABLE");
  ASSERT_EQ(disable.terminate_user_ids.size(), 1u);  // live sessions must end
  EXPECT_EQ(disable.terminate_user_ids[0], joe.user_id);
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));
  EXPECT_FALSE(engine_->IsActive(joe));
  EXPECT_FALSE(engine_->AuthenticatePassword("joe", "pw").ok());
  EXPECT_FALSE(engine_->PrincipalForUser("joe").ok());
  Admin("ALTER USER joe ENABLE");
  EXPECT_TRUE(engine_->AuthenticatePassword("joe", "pw").ok());
  Admin("DROP USER joe");
  EXPECT_FALSE(Can(joe, Privilege::kSelect, "orders"));
}

TEST_F(RbacEngineTest, PasswordLoginFailuresLookAlike) {
  Admin("CREATE USER joe WITH PASSWORD 'correct horse'");
  Admin("CREATE USER token_only");
  auto ok = engine_->AuthenticatePassword("JOE", "correct horse");  // names are case-insensitive
  ASSERT_TRUE(ok.ok());
  EXPECT_EQ(ok->user_name, "joe");
  const std::string wrong = engine_->AuthenticatePassword("joe", "Correct horse").status().message();
  const std::string unknown = engine_->AuthenticatePassword("nobody", "x").status().message();
  const std::string no_password = engine_->AuthenticatePassword("token_only", "").status().message();
  EXPECT_EQ(wrong, "GizmoSQL access control: invalid user name or password");
  EXPECT_EQ(unknown, wrong);
  EXPECT_EQ(no_password, wrong);
  // Passwords are hashed, never stored as written.
  const auto* stored = engine_->snapshot()->FindUserByName("joe");
  ASSERT_NE(stored, nullptr);
  ASSERT_TRUE(stored->password_hash.has_value());
  EXPECT_EQ(stored->password_hash->find("correct horse"), std::string::npos);
  EXPECT_EQ(stored->password_hash->rfind("pbkdf2-sha256$1000$", 0), 0u);
  // A password change takes effect at once.
  Admin("ALTER USER joe WITH PASSWORD 'battery staple'");
  EXPECT_FALSE(engine_->AuthenticatePassword("joe", "correct horse").ok());
  EXPECT_TRUE(engine_->AuthenticatePassword("joe", "battery staple").ok());
}

TEST_F(RbacEngineTest, OnlyAdministratorsChangeAccessControl) {
  Admin("CREATE USER joe");
  Admin("CREATE ROLE analyst");
  Admin("GRANT analyst TO joe");
  auto joe = User("joe");
  for (const char* sql : {"CREATE ROLE sneaky", "GRANT analyst TO joe",
                          "GRANT SELECT ON orders TO analyst", "DROP USER joe",
                          "ALTER USER joe WITH PASSWORD 'x'", "ALTER TABLE orders OWNER TO joe",
                          "REVOKE analyst FROM joe"}) {
    EXPECT_NE(Error(sql, joe).find("only administrators manage"), std::string::npos) << sql;
  }
  // ... and none of them changed anything.
  EXPECT_FALSE(engine_->snapshot()->FindRoleByName("sneaky"));
}

TEST_F(RbacEngineTest, ShowGrants) {
  Admin("CREATE USER joe");
  Admin("CREATE USER ann");
  Admin("CREATE ROLE analyst");
  Admin("GRANT analyst TO joe");
  Admin("GRANT SELECT (email) ON users TO analyst");
  auto joe = User("joe");

  auto parsed = ParseRbacStatement("SHOW GRANTS");
  auto mine = engine_->Execute(*parsed, joe, resolver_);
  ASSERT_TRUE(mine.ok());
  bool saw_column_grant = false;
  for (const auto& row : mine->rows) {
    if (row[0] == "SELECT" && row[2] == "lake.main.users" && row[3] == "email") saw_column_grant = true;
  }
  EXPECT_TRUE(saw_column_grant);
  // A user may look at their own grants and roles they hold, nobody else's.
  EXPECT_EQ(Error("SHOW GRANTS FOR ROLE analyst", joe), "");
  EXPECT_NE(Error("SHOW GRANTS FOR USER ann", joe).find("blocked SHOW GRANTS for another user"),
            std::string::npos);
  Admin("CREATE ROLE secret_role");
  EXPECT_NE(Error("SHOW GRANTS FOR secret_role", joe).find("which you do not hold"),
            std::string::npos);
  // The administrator sees anyone's.
  auto ann_grants = Admin("SHOW GRANTS FOR ann");
  EXPECT_TRUE(ann_grants.rows.empty());
}

TEST_F(RbacEngineTest, NamesAreCaseInsensitiveButKeepTheirSpelling) {
  Admin("CREATE ROLE \"Data Team\"");
  Admin("GRANT SELECT ON ORDERS TO \"data team\"");
  const auto* role = engine_->snapshot()->FindRoleByName("DATA TEAM");
  ASSERT_NE(role, nullptr);
  EXPECT_EQ(role->name, "Data Team");
  EXPECT_EQ(engine_->snapshot()->grants().size(), 1u);
  // Granting the same thing twice does not duplicate it.
  Admin("GRANT SELECT ON orders TO \"Data Team\"");
  EXPECT_EQ(engine_->snapshot()->grants().size(), 1u);
}

TEST_F(RbacEngineTest, AStoreVersionBumpsOnEveryChange) {
  const auto v0 = engine_->snapshot()->version();
  Admin("CREATE ROLE a");
  const auto v1 = engine_->snapshot()->version();
  EXPECT_GT(v1, v0);
  // A failed statement changes nothing.
  (void)Error("CREATE ROLE a");
  EXPECT_FALSE(Error("CREATE ROLE a").empty());
  EXPECT_EQ(engine_->snapshot()->roles().size(), 2u);  // PUBLIC + a
}
