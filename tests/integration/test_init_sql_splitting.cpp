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

// Splitting init SQL (--init-sql-commands / -file) into statements.
//
// The old splitter only understood single-quoted strings, so a ';' inside a
// comment, a "double;quoted" identifier or a $$dollar;quoted$$ string cut one
// statement in two and the server failed to start (it wedged a production
// cluster's boot).

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "detail/sql_splitter.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;
using gizmosql::SplitSqlStatements;
using Statements = std::vector<std::string>;

// ----------------------------------------------------------------------------
// SplitSqlStatements
// ----------------------------------------------------------------------------

TEST(SqlSplitter, SplitsOnTopLevelSemicolons) {
  EXPECT_EQ(SplitSqlStatements("SELECT 1; SELECT 2;"), (Statements{"SELECT 1", "SELECT 2"}));
  EXPECT_EQ(SplitSqlStatements("  SELECT 1  \n;\n SELECT 2"),
            (Statements{"SELECT 1", "SELECT 2"}));
  EXPECT_EQ(SplitSqlStatements(";;  ;"), Statements{});
  EXPECT_EQ(SplitSqlStatements(""), Statements{});
}

TEST(SqlSplitter, SemicolonInStringLiteralDoesNotSplit) {
  EXPECT_EQ(SplitSqlStatements("SELECT 'a;b'; SELECT 'it''s;x'"),
            (Statements{"SELECT 'a;b'", "SELECT 'it''s;x'"}));
  // E'...' strings also escape with a backslash.
  EXPECT_EQ(SplitSqlStatements(R"(SELECT E'a\';b'; SELECT 2)"),
            (Statements{R"(SELECT E'a\';b')", "SELECT 2"}));
}

TEST(SqlSplitter, SemicolonInQuotedIdentifierDoesNotSplit) {
  EXPECT_EQ(SplitSqlStatements(R"(CREATE TABLE "a;b" (x INT); SELECT 1)"),
            (Statements{R"(CREATE TABLE "a;b" (x INT))", "SELECT 1"}));
  EXPECT_EQ(SplitSqlStatements(R"(SELECT 1 AS "q""; x")"),
            (Statements{R"(SELECT 1 AS "q""; x")"}));
}

TEST(SqlSplitter, SemicolonInCommentsDoesNotSplit) {
  EXPECT_EQ(SplitSqlStatements("SELECT 1; -- setup; done\nSELECT 2"),
            (Statements{"SELECT 1", "-- setup; done\nSELECT 2"}));
  EXPECT_EQ(SplitSqlStatements("SELECT /* a; b */ 1; SELECT 2"),
            (Statements{"SELECT /* a; b */ 1", "SELECT 2"}));
  // Block comments nest.
  EXPECT_EQ(SplitSqlStatements("SELECT /* x /* y; */ z; */ 1"),
            (Statements{"SELECT /* x /* y; */ z; */ 1"}));
  // A fragment that is only comments is not a statement.
  EXPECT_EQ(SplitSqlStatements("SELECT 1; -- the end;"), (Statements{"SELECT 1"}));
  EXPECT_EQ(SplitSqlStatements("/* only; a comment */"), Statements{});
}

TEST(SqlSplitter, SemicolonInDollarQuotedStringDoesNotSplit) {
  EXPECT_EQ(SplitSqlStatements("SELECT $$a;b$$; SELECT 2"),
            (Statements{"SELECT $$a;b$$", "SELECT 2"}));
  EXPECT_EQ(SplitSqlStatements("SELECT $fn$ x; $$ y; $fn$; SELECT 2"),
            (Statements{"SELECT $fn$ x; $$ y; $fn$", "SELECT 2"}));
  // $1 / $name parameters are not dollar quotes.
  EXPECT_EQ(SplitSqlStatements("SELECT $1; SELECT $name; SELECT 3"),
            (Statements{"SELECT $1", "SELECT $name", "SELECT 3"}));
}

TEST(SqlSplitter, UnterminatedQuoteRunsToTheEnd) {
  EXPECT_EQ(SplitSqlStatements("SELECT 'oops; SELECT 2"),
            (Statements{"SELECT 'oops; SELECT 2"}));
}

// ----------------------------------------------------------------------------
// A server whose init SQL has ';' inside comments, identifiers and $$ strings
// ----------------------------------------------------------------------------
class InitSqlSplittingFixture
    : public gizmosql::testing::ServerTestFixture<InitSqlSplittingFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31674,
        .health_port = 31675,
        .username = "tester",
        .password = "tester_password",
        .init_sql_commands =
            "-- create the objects; the cluster needs them\n"
            "CREATE TABLE \"semi;colon\" (note VARCHAR);\n"
            "/* seed; one row */ INSERT INTO \"semi;colon\" VALUES ('a;b');\n"
            "CREATE MACRO semi_text() AS $$x;y$$;\n"
            "-- done;\n",
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<InitSqlSplittingFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<InitSqlSplittingFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<InitSqlSplittingFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<InitSqlSplittingFixture>::config_{};

TEST_F(InitSqlSplittingFixture, InitSqlWithSemicolonsInsideRunsIntact) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto bearer,
                             client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options,
                                    "SELECT note, semi_text() AS m FROM \"semi;colon\""));
  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, reader->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
  EXPECT_EQ(table->column(0)->GetScalar(0).ValueOrDie()->ToString(), "a;b");
  EXPECT_EQ(table->column(1)->GetScalar(0).ValueOrDie()->ToString(), "x;y");
}
