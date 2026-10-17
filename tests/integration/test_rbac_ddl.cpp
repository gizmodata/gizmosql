// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unit tests for the access-control statement parser (Enterprise RBAC). No
// server: these drive ParseRbacStatement() / LooksLikeRbacStatement() directly,
// including hostile input — smuggled second statements, comment and quoting
// tricks, privilege words used as names.

#include <gtest/gtest.h>

#include <string>
#include <variant>

#include "enterprise/rbac/rbac_ddl.h"

using namespace gizmosql::enterprise::rbac;

namespace {

template <typename T>
T ParseAs(const std::string& sql) {
  auto result = ParseRbacStatement(sql);
  EXPECT_TRUE(result.ok()) << sql << " -> " << result.status().ToString();
  if (!result.ok()) return T{};
  EXPECT_TRUE(std::holds_alternative<T>(*result)) << sql;
  if (!std::holds_alternative<T>(*result)) return T{};
  return std::get<T>(*result);
}

std::string ParseError(const std::string& sql) {
  auto result = ParseRbacStatement(sql);
  EXPECT_FALSE(result.ok()) << "expected a parse error for: " << sql;
  return result.ok() ? "" : result.status().message();
}

}  // namespace

// ---------------------------------------------------------------------------
// Which statements GizmoSQL takes (and which it must leave for DuckDB).
// ---------------------------------------------------------------------------

TEST(RbacDdl, RecognizesAccessControlStatements) {
  for (const char* sql :
       {"GRANT SELECT ON t TO r", "grant r to u", "REVOKE r FROM u", "CREATE USER joe",
        "create role analyst", "DROP USER joe", "DROP ROLE IF EXISTS r",
        "ALTER USER joe DISABLE", "ALTER VIEW v OWNER TO r",
        "ALTER TABLE a.b.c OWNER TO r", "SHOW GRANTS", "show grants for joe",
        "/* hi */ -- there\n  GRANT r TO u",
        "/* nested /* comment */ still */ GRANT r TO u"}) {
    EXPECT_TRUE(LooksLikeRbacStatement(sql)) << sql;
  }
}

TEST(RbacDdl, LeavesEngineStatementsToDuckDB) {
  for (const char* sql :
       {"SELECT 1", "CREATE TABLE users (id INT)", "DROP TABLE roles",
        "CREATE VIEW grants AS SELECT 1", "ALTER TABLE t ADD COLUMN x INT",
        "ALTER TABLE owner RENAME TO x", "ALTER VIEW v RENAME TO w", "SHOW TABLES",
        "SHOW users", "DESCRIBE grants", "", "-- GRANT r TO u\nSELECT 1",
        "/* GRANT r TO u */ SELECT 1", "SELECT 'GRANT r TO u'",
        "CREATE USERS_TABLE AS SELECT 1"}) {
    EXPECT_FALSE(LooksLikeRbacStatement(sql)) << sql;
  }
}

// ---------------------------------------------------------------------------
// Users and roles.
// ---------------------------------------------------------------------------

TEST(RbacDdl, CreateUser) {
  auto cmd = ParseAs<CreateUserCommand>("CREATE USER joe WITH PASSWORD 'it''s secret'");
  EXPECT_EQ(cmd.name, "joe");
  ASSERT_TRUE(cmd.password.has_value());
  EXPECT_EQ(*cmd.password, "it's secret");
  EXPECT_FALSE(cmd.if_not_exists);

  auto token_only =
      ParseAs<CreateUserCommand>("CREATE USER IF NOT EXISTS \"Joe Smith\";");
  EXPECT_EQ(token_only.name, "Joe Smith");
  EXPECT_FALSE(token_only.password.has_value());
  EXPECT_TRUE(token_only.if_not_exists);

  auto no_with = ParseAs<CreateUserCommand>("create user joe password 'x'");
  EXPECT_EQ(*no_with.password, "x");
}

TEST(RbacDdl, AlterUser) {
  EXPECT_EQ(ParseAs<AlterUserCommand>("ALTER USER joe DISABLE").action,
            AlterUserCommand::Action::kDisable);
  EXPECT_EQ(ParseAs<AlterUserCommand>("ALTER USER joe ENABLE").action,
            AlterUserCommand::Action::kEnable);
  auto pw = ParseAs<AlterUserCommand>("ALTER USER joe WITH PASSWORD 'n3w'");
  EXPECT_EQ(pw.action, AlterUserCommand::Action::kSetPassword);
  EXPECT_EQ(pw.password, "n3w");
  EXPECT_EQ(ParseAs<AlterUserCommand>("ALTER USER joe PASSWORD NULL").action,
            AlterUserCommand::Action::kClearPassword);
}

TEST(RbacDdl, DropAndRoleStatements) {
  EXPECT_TRUE(ParseAs<DropUserCommand>("DROP USER IF EXISTS joe").if_exists);
  EXPECT_EQ(ParseAs<DropRoleCommand>("DROP ROLE analyst").name, "analyst");
  EXPECT_TRUE(
      ParseAs<CreateRoleCommand>("CREATE ROLE IF NOT EXISTS analyst").if_not_exists);
}

TEST(RbacDdl, RoleMembership) {
  auto cmd =
      ParseAs<RoleMembershipCommand>("GRANT analyst, reader TO USER joe, ROLE lead");
  EXPECT_TRUE(cmd.grant);
  ASSERT_EQ(cmd.roles.size(), 2u);
  EXPECT_EQ(cmd.roles[1], "reader");
  ASSERT_EQ(cmd.grantees.size(), 2u);
  EXPECT_EQ(cmd.grantees[0].kind, GranteeKind::kUser);
  EXPECT_EQ(cmd.grantees[1].kind, GranteeKind::kRole);
  EXPECT_EQ(cmd.grantees[1].name, "lead");

  auto revoke = ParseAs<RoleMembershipCommand>("REVOKE analyst FROM joe");
  EXPECT_FALSE(revoke.grant);
  EXPECT_EQ(revoke.grantees[0].kind, GranteeKind::kUnspecified);

  EXPECT_TRUE(
      ParseAs<RoleMembershipCommand>("GRANT a TO b WITH ADMIN OPTION").with_admin_option);
  EXPECT_TRUE(ParseAs<RoleMembershipCommand>("REVOKE ADMIN OPTION FOR a FROM b")
                  .with_admin_option);
}

TEST(RbacDdl, PrincipalsNamedLikeKeywords) {
  // USER / ROLE are kind keywords only when another name follows.
  auto cmd = ParseAs<RoleMembershipCommand>("GRANT analyst TO role");
  EXPECT_EQ(cmd.grantees[0].kind, GranteeKind::kUnspecified);
  EXPECT_EQ(cmd.grantees[0].name, "role");
  // A role named like a privilege must be quoted, as in Postgres.
  auto quoted = ParseAs<RoleMembershipCommand>("GRANT \"select\" TO joe");
  EXPECT_EQ(quoted.roles[0], "select");
  // ... but a role whose name merely starts like a privilege phrase is fine.
  EXPECT_EQ(ParseAs<RoleMembershipCommand>("GRANT manage TO bob").roles[0], "manage");
  EXPECT_EQ(ParseAs<RoleMembershipCommand>("GRANT create TO bob").roles[0], "create");
}

// ---------------------------------------------------------------------------
// Object privileges.
// ---------------------------------------------------------------------------

TEST(RbacDdl, ObjectPrivileges) {
  auto cmd = ParseAs<ObjectPrivilegeCommand>(
      "GRANT SELECT (email, \"Signup TS\"), INSERT ON TABLE crm.public.users TO analyst");
  EXPECT_TRUE(cmd.grant);
  ASSERT_EQ(cmd.privileges.size(), 2u);
  EXPECT_EQ(cmd.privileges[0].privilege, Privilege::kSelect);
  ASSERT_EQ(cmd.privileges[0].columns.size(), 2u);
  EXPECT_EQ(cmd.privileges[0].columns[1], "Signup TS");
  EXPECT_EQ(cmd.privileges[1].privilege, Privilege::kInsert);
  EXPECT_EQ(cmd.scope, GrantScope::kObject);
  ASSERT_EQ(cmd.target.parts.size(), 3u);
  EXPECT_EQ(cmd.target.parts[2], "users");
  EXPECT_EQ(cmd.grantees[0].name, "analyst");
}

TEST(RbacDdl, ObjectNamedTable) {
  // "GRANT SELECT ON table TO r": a table literally named table.
  auto cmd = ParseAs<ObjectPrivilegeCommand>("GRANT SELECT ON table TO r");
  ASSERT_EQ(cmd.target.parts.size(), 1u);
  EXPECT_EQ(cmd.target.parts[0], "table");
  auto view = ParseAs<ObjectPrivilegeCommand>("GRANT SELECT ON VIEW v TO r");
  EXPECT_EQ(view.target.parts[0], "v");
}

TEST(RbacDdl, SchemaAndCatalogScopes) {
  auto schema = ParseAs<ObjectPrivilegeCommand>(
      "GRANT ALL PRIVILEGES ON SCHEMA lake.staging TO loader");
  EXPECT_EQ(schema.scope, GrantScope::kSchema);
  EXPECT_EQ(schema.privileges[0].privilege, Privilege::kAll);
  ASSERT_EQ(schema.target.parts.size(), 2u);

  auto catalog =
      ParseAs<ObjectPrivilegeCommand>("GRANT CREATE SCHEMA, SELECT ON CATALOG lake TO r");
  EXPECT_EQ(catalog.scope, GrantScope::kCatalog);
  EXPECT_EQ(catalog.privileges[0].privilege, Privilege::kCreateSchema);

  auto ddl =
      ParseAs<ObjectPrivilegeCommand>("GRANT CREATE TABLE, DROP VIEW ON SCHEMA s TO r");
  EXPECT_EQ(ddl.privileges[0].privilege, Privilege::kCreateTable);
  EXPECT_EQ(ddl.privileges[1].privilege, Privilege::kDropView);
}

TEST(RbacDdl, RevokeForms) {
  auto revoke =
      ParseAs<ObjectPrivilegeCommand>("REVOKE SELECT (email) ON users FROM r RESTRICT");
  EXPECT_FALSE(revoke.grant);
  EXPECT_FALSE(revoke.cascade);
  auto option =
      ParseAs<ObjectPrivilegeCommand>("REVOKE GRANT OPTION FOR SELECT ON t FROM r");
  EXPECT_TRUE(option.with_grant_option);
  EXPECT_TRUE(ParseAs<ObjectPrivilegeCommand>("GRANT SELECT ON t TO r WITH GRANT OPTION")
                  .with_grant_option);
}

TEST(RbacDdl, AlterOwnerAndShowGrants) {
  auto owner =
      ParseAs<AlterOwnerCommand>("ALTER VIEW published.daily OWNER TO ROLE data_eng");
  EXPECT_TRUE(owner.is_view);
  EXPECT_EQ(owner.new_owner.kind, GranteeKind::kRole);
  EXPECT_FALSE(ParseAs<ShowGrantsCommand>("SHOW GRANTS").principal.has_value());
  auto show = ParseAs<ShowGrantsCommand>("SHOW GRANTS FOR USER joe");
  ASSERT_TRUE(show.principal.has_value());
  EXPECT_EQ(show.principal->kind, GranteeKind::kUser);
}

// ---------------------------------------------------------------------------
// Hostile input.
// ---------------------------------------------------------------------------

TEST(RbacDdl, SecondStatementIsRejected) {
  for (const char* sql :
       {"GRANT r TO u; DROP TABLE t", "GRANT r TO u;;", "CREATE ROLE a; CREATE ROLE b",
        "GRANT SELECT ON t TO r; SELECT 1", "SHOW GRANTS; ATTACH 'x.db'",
        "GRANT r TO u /* ok */ ; -- c\n x"}) {
    const std::string error = ParseError(sql);
    EXPECT_NE(error.find("one at a time"), std::string::npos) << sql << " -> " << error;
  }
  // A single trailing semicolon (and trailing comments) is fine.
  ParseAs<RoleMembershipCommand>("GRANT r TO u; -- done");
}

TEST(RbacDdl, MalformedInputIsAClearError) {
  EXPECT_NE(ParseError("GRANT SELECT ON t TO").find("expected a user or role name"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT SELECT t TO r").find("expected ON"), std::string::npos);
  EXPECT_NE(ParseError("CREATE USER joe PASSWORD unquoted").find("quoted string"),
            std::string::npos);
  EXPECT_NE(ParseError("CREATE USER 'joe'").find("expected a user name"),
            std::string::npos);
  EXPECT_NE(ParseError("CREATE USER joe PASSWORD 'x").find("unterminated string"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT r TO \"u").find("unterminated quoted identifier"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT r TO u /* never closed").find("unterminated comment"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT r TO \"\"").find("empty quoted identifier"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT INSERT (a) ON t TO r").find("only supported for SELECT"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT CREATE TABLE ON t TO r").find("ON SCHEMA or ON CATALOG"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT CREATE SCHEMA ON SCHEMA s TO r").find("ON CATALOG"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT analyst ON t TO r").find("is not a privilege"),
            std::string::npos);
  EXPECT_NE(ParseError("ALTER TABLE t RENAME TO x").find("OWNER TO"), std::string::npos);
  // Characters outside the grammar (e.g. $-quoting, backslashes) are refused,
  // never skipped.
  EXPECT_NE(ParseError("CREATE USER joe PASSWORD $$x$$").find("unexpected character"),
            std::string::npos);
  EXPECT_NE(ParseError("GRANT r TO u\\").find("unexpected character"), std::string::npos);
  // Every error is branded.
  EXPECT_EQ(ParseError("GRANT").rfind("GizmoSQL access control: ", 0), 0u);
}

TEST(RbacDdl, QuotingPreservesNamesExactly) {
  auto cmd = ParseAs<ObjectPrivilegeCommand>(
      "GRANT SELECT ON \"my.catalog\".\"we\"\"ird\".\"T 1\" TO \"Data Team\"");
  ASSERT_EQ(cmd.target.parts.size(), 3u);
  EXPECT_EQ(cmd.target.parts[0], "my.catalog");
  EXPECT_EQ(cmd.target.parts[1], "we\"ird");
  EXPECT_EQ(cmd.target.parts[2], "T 1");
  EXPECT_EQ(cmd.grantees[0].name, "Data Team");
  EXPECT_EQ(cmd.target.ToString(), "\"my.catalog\".\"we\"\"ird\".\"T 1\"");
}

TEST(RbacDdl, KeywordsAreCaseInsensitiveButQuotedKeywordsAreNames) {
  ParseAs<ObjectPrivilegeCommand>("gRaNt SeLeCt On T tO r");
  // A quoted "ON" is an identifier, so this is a role grant of role "x" ...
  // no: `GRANT x "ON" t TO r` is not valid — the quoted word is not the keyword.
  EXPECT_FALSE(ParseRbacStatement("GRANT SELECT \"ON\" t TO r").ok());
}

TEST(RbacDdl, CommandKinds) {
  EXPECT_EQ(CommandKind(*ParseRbacStatement("GRANT SELECT ON t TO r")), "GRANT");
  EXPECT_EQ(CommandKind(*ParseRbacStatement("REVOKE r FROM u")), "REVOKE");
  EXPECT_EQ(CommandKind(*ParseRbacStatement("ALTER VIEW v OWNER TO r")),
            "ALTER VIEW OWNER");
}
