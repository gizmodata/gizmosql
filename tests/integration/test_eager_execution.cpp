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

// Eager execution contract: writes finish before GetFlightInfo returns,
// prepared handles remain reusable, and completed tickets never repeat writes.

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/extension/uuid.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

class EagerExecutionFixture
    : public gizmosql::testing::ServerTestFixture<EagerExecutionFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "eager_execution_tester.db",
        .port = 31580,
        .health_port = 31581,
        .username = "tester",
        .password = "tester",
    };
  }

 protected:
  std::unique_ptr<FlightSqlClient> sql_client_;
  arrow::flight::FlightCallOptions call_options_;

  void SetUp() override {
    ASSERT_TRUE(IsServerReady()) << "Server not ready";
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                               arrow::flight::FlightClient::Connect(
                                   location, arrow::flight::FlightClientOptions{}));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options_.headers.push_back(bearer);
    sql_client_ = std::make_unique<FlightSqlClient>(std::move(client));
  }

  void Exec(const std::string& sql) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto n, sql_client_->ExecuteUpdate(call_options_, sql));
    (void)n;
  }

  arrow::Result<std::shared_ptr<arrow::Table>> Query(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, sql_client_->Execute(call_options_, sql));
    ARROW_ASSIGN_OR_RAISE(auto stream,
                          sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
    return stream->ToTable();
  }

  // Returns the first column of the first row of a query, as a string.
  std::string Scalar(const std::string& sql) {
    auto table = Query(sql);
    EXPECT_TRUE(table.ok()) << table.status().ToString();
    if (!table.ok()) return "<error>";
    EXPECT_EQ((*table)->num_rows(), 1);
    auto scalar = (*table)->column(0)->GetScalar(0);
    EXPECT_TRUE(scalar.ok());
    return (*scalar)->ToString();
  }
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<EagerExecutionFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<EagerExecutionFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<EagerExecutionFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<EagerExecutionFixture>::config_{};

// These are the release contract, deliberately run against the lazy baseline
// before implementation. Ledger changes must occur once when Execute returns;
// consuming an execution ticket any number of times must never repeat a write.
TEST_F(EagerExecutionFixture, EagerWritesHonorFlightTransactions) {
  Exec("CREATE OR REPLACE TABLE eager_transactions(n INTEGER)");
  ASSERT_ARROW_OK_AND_ASSIGN(auto tx, sql_client_->BeginTransaction(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto rolled_back,
      sql_client_->Execute(call_options_, "INSERT INTO eager_transactions VALUES (1)",
                           tx));
  ASSERT_ARROW_OK(sql_client_->Rollback(call_options_, tx));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto replay, sql_client_->DoGet(call_options_, rolled_back->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto result, replay->ToTable());
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_transactions"), "0");
  ASSERT_ARROW_OK_AND_ASSIGN(auto committed_tx,
                             sql_client_->BeginTransaction(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto committed,
      sql_client_->Execute(call_options_, "INSERT INTO eager_transactions VALUES (2)",
                           committed_tx));
  ASSERT_ARROW_OK(sql_client_->Commit(call_options_, committed_tx));
  EXPECT_EQ(Scalar("SELECT sum(n) FROM eager_transactions"), "2");
}

TEST_F(EagerExecutionFixture, CompletedTicketsCannotBeFetchedByAnotherSession) {
  Exec("CREATE OR REPLACE TABLE eager_owned(n INTEGER)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client_->Execute(call_options_, "INSERT INTO eager_owned VALUES (1)"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto other, arrow::flight::FlightClient::Connect(location));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, other->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  arrow::flight::FlightCallOptions options;
  options.headers.push_back(bearer);
  auto stolen = other->DoGet(options, info->endpoints()[0].ticket);
  if (stolen.ok()) EXPECT_FALSE((*stolen)->ToTable().ok());
  EXPECT_EQ(Scalar("SELECT sum(n) FROM eager_owned"), "1");
}

TEST_F(EagerExecutionFixture, CompletedTicketsAreNotCancellable) {
  Exec("CREATE OR REPLACE TABLE eager_cancel(n INTEGER)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client_->Execute(call_options_, "INSERT INTO eager_cancel VALUES (42)"));
  const auto ticket = info->endpoints()[0].ticket;
  arrow::flight::CancelFlightInfoRequest request(std::move(info));
  ASSERT_ARROW_OK_AND_ASSIGN(auto cancelled,
                             sql_client_->CancelFlightInfo(call_options_, request));
  EXPECT_EQ(cancelled.status, arrow::flight::CancelStatus::kNotCancellable);
  ASSERT_ARROW_OK_AND_ASSIGN(auto replay, sql_client_->DoGet(call_options_, ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto result, replay->ToTable());
  EXPECT_EQ(result->num_rows(), 1);
  EXPECT_EQ(Scalar("SELECT sum(n) FROM eager_cancel"), "42");
}

TEST_F(EagerExecutionFixture, EvictedTicketsFailWithoutRepeatingWrites) {
  Exec("CREATE OR REPLACE TABLE eager_eviction(n INTEGER)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto oldest,
      sql_client_->Execute(call_options_, "INSERT INTO eager_eviction VALUES (1)"));
  // The completed-result cache is bounded to 1024 entries per session.
  for (int i = 0; i < 1024; ++i) {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto execution,
        sql_client_->Execute(call_options_, "INSERT INTO eager_eviction VALUES (1)"));
  }
  auto evicted = sql_client_->DoGet(call_options_, oldest->endpoints()[0].ticket);
  if (evicted.ok()) EXPECT_FALSE((*evicted)->ToTable().ok());
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_eviction"), "1025");
}

TEST_F(EagerExecutionFixture, CopyAndAttachSideEffectsFinishWithoutFetching) {
  const auto path =
      std::filesystem::temp_directory_path() / "gizmosql_eager_copy_test.csv";
  std::filesystem::remove(path);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto copied, sql_client_->Execute(call_options_, "COPY (SELECT 42 AS n) TO '" +
                                                           path.string() + "' (HEADER)"));
  EXPECT_TRUE(std::filesystem::exists(path));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto attached,
      sql_client_->Execute(call_options_, "ATTACH ':memory:' AS eager_attached"));
  EXPECT_EQ(Scalar("SELECT count(*) FROM duckdb_databases() WHERE database_name = "
                   "'eager_attached'"),
            "1");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto detached, sql_client_->Execute(call_options_, "DETACH eager_attached"));
  EXPECT_EQ(Scalar("SELECT count(*) FROM duckdb_databases() WHERE database_name = "
                   "'eager_attached'"),
            "0");
  std::filesystem::remove(path);
}

TEST_F(EagerExecutionFixture, UnpreparedWriteCommitsWithoutDoGet) {
  Exec("CREATE OR REPLACE TABLE eager_ledger (token VARCHAR, n BIGINT)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client_->Execute(call_options_,
                                      "INSERT INTO eager_ledger VALUES ('no_fetch', 1)"));
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_ledger WHERE token = 'no_fetch'"), "1");
}

TEST_F(EagerExecutionFixture, UnpreparedTicketReplayDoesNotRepeatWrite) {
  Exec("CREATE OR REPLACE TABLE eager_replay (token VARCHAR, n BIGINT)");
  Exec("INSERT INTO eager_replay VALUES ('replay', 0)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info,
      sql_client_->Execute(call_options_,
                           "UPDATE eager_replay SET n = n + 1 WHERE token = 'replay'"));
  std::shared_ptr<arrow::Table> first;
  for (int i = 0; i < 2; ++i) {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
    if (first) EXPECT_TRUE(first->Equals(*table));
    first = table;
  }
  EXPECT_EQ(Scalar("SELECT n FROM eager_replay WHERE token = 'replay'"), "1");
}

TEST_F(EagerExecutionFixture, PreparedTicketReplayAndReuseAreDistinctExecutions) {
  Exec("CREATE OR REPLACE TABLE eager_prepared (token VARCHAR, n BIGINT)");
  Exec("INSERT INTO eager_prepared VALUES ('prepared', 0)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(
          call_options_, "UPDATE eager_prepared SET n = n + 1 WHERE token = 'prepared'"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto first, prepared->Execute(call_options_));
  EXPECT_EQ(Scalar("SELECT n FROM eager_prepared"), "1");
  ASSERT_ARROW_OK_AND_ASSIGN(auto second, prepared->Execute(call_options_));
  EXPECT_EQ(Scalar("SELECT n FROM eager_prepared"), "2");
  for (const auto* info : {first.get(), second.get(), first.get()}) {
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
    EXPECT_EQ(table->num_rows(), 1);
  }
  EXPECT_EQ(Scalar("SELECT n FROM eager_prepared"), "2");
  ASSERT_ARROW_OK(prepared->Close(call_options_));
}

TEST_F(EagerExecutionFixture, OnePreparedInsertCanBindAndExecuteManyTimes) {
  Exec("CREATE OR REPLACE TABLE eager_bound (token BIGINT PRIMARY KEY, n BIGINT)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "INSERT INTO eager_bound VALUES (?, ?)"));
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_bound"), "0");
  std::vector<std::unique_ptr<arrow::flight::FlightInfo>> executions;
  for (int64_t token = 1; token <= 20; ++token) {
    arrow::Int64Builder tokens, values;
    ASSERT_ARROW_OK(tokens.Append(token));
    ASSERT_ARROW_OK(values.Append(token * 7));
    ASSERT_ARROW_OK_AND_ASSIGN(auto token_array, tokens.Finish());
    ASSERT_ARROW_OK_AND_ASSIGN(auto value_array, values.Finish());
    auto batch =
        arrow::RecordBatch::Make(arrow::schema({arrow::field("token", arrow::int64()),
                                                arrow::field("n", arrow::int64())}),
                                 1, {token_array, value_array});
    ASSERT_ARROW_OK(prepared->SetParameters(batch));
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, prepared->Execute(call_options_));
    executions.push_back(std::move(info));
    EXPECT_EQ(Scalar("SELECT count(*) FROM eager_bound"), std::to_string(token));
  }
  // Rebinding cannot change old execution tickets. Closing the prepared
  // statement cannot discard a completed write or make a fetch run it again.
  ASSERT_ARROW_OK(prepared->Close(call_options_));
  for (int pass = 0; pass < 2; ++pass) {
    for (auto it = executions.rbegin(); it != executions.rend(); ++it) {
      ASSERT_ARROW_OK_AND_ASSIGN(
          auto stream, sql_client_->DoGet(call_options_, (*it)->endpoints()[0].ticket));
      ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
      EXPECT_EQ(table->num_rows(), 1);
    }
  }
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_bound"), "20");
  EXPECT_EQ(Scalar("SELECT sum(n) FROM eager_bound"), "1470");
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_bound WHERE n <> token * 7"), "0");
}

TEST_F(EagerExecutionFixture, MetricsEnablementIsDiscoverableAndStartupOnly) {
  EXPECT_EQ(
      Scalar(
          "SELECT value FROM gizmosql_settings() WHERE name = 'gizmosql.enable_metrics'"),
      "false");
  EXPECT_EQ(Scalar("SELECT settable FROM gizmosql_settings() WHERE name = "
                   "'gizmosql.enable_metrics'"),
            "false");
  EXPECT_EQ(Scalar("SELECT cli_flag FROM gizmosql_settings() WHERE name = "
                   "'gizmosql.enable_metrics'"),
            "--enable-metrics");
  auto result =
      sql_client_->ExecuteUpdate(call_options_, "SET gizmosql.enable_metrics = true");
  ASSERT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("--enable-metrics"), std::string::npos);
}

TEST_F(EagerExecutionFixture, EagerDdlAndMergeCompleteWithoutFetching) {
  const std::vector<std::string> sqls = {
      "CREATE TABLE eager_ddl (token INTEGER PRIMARY KEY, n INTEGER)",
      "ALTER TABLE eager_ddl ADD COLUMN extra INTEGER DEFAULT 0",
      "INSERT INTO eager_ddl(token, n) VALUES (1, 0), (2, 0)",
      "MERGE INTO eager_ddl t USING (VALUES (1, 3), (3, 5)) s(token,n) ON "
      "t.token=s.token "
      "WHEN MATCHED THEN UPDATE SET n=t.n+1 WHEN NOT MATCHED THEN INSERT(token,n) "
      "VALUES(s.token,s.n)",
      "DELETE FROM eager_ddl WHERE token=2"};
  for (const auto& sql : sqls) {
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, sql_client_->Execute(call_options_, sql));
  }
  EXPECT_EQ(Scalar("SELECT count(*) FROM eager_ddl"), "2");
  EXPECT_EQ(Scalar("SELECT n FROM eager_ddl WHERE token=1"), "1");
  EXPECT_EQ(Scalar("SELECT sum(n) FROM eager_ddl"), "6");
  ASSERT_ARROW_OK_AND_ASSIGN(auto drop,
                             sql_client_->Execute(call_options_, "DROP TABLE eager_ddl"));
  EXPECT_EQ(Scalar("SELECT count(*) FROM duckdb_tables() WHERE table_name='eager_ddl'"),
            "0");
}
