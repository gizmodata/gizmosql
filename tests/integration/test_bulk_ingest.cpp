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

#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <iostream>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::flight::sql::FlightSqlClient;
using arrow::flight::sql::TableDefinitionOptions;
using arrow::flight::sql::TableDefinitionOptionsTableExistsOption;
using arrow::flight::sql::TableDefinitionOptionsTableNotExistOption;

//--------------------------------------------------
// Helpers
//--------------------------------------------------

std::shared_ptr<arrow::RecordBatchReader> MakeTestBatches() {
  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  ARROW_EXPECT_OK(id_builder.AppendValues({1, 2, 3}));
  ARROW_EXPECT_OK(name_builder.AppendValues({"alice", "bob", "carol"}));

  std::shared_ptr<arrow::Array> ids, names;
  ARROW_EXPECT_OK(id_builder.Finish(&ids));
  ARROW_EXPECT_OK(name_builder.Finish(&names));

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("name", arrow::utf8())});

  auto batch = arrow::RecordBatch::Make(schema, ids->length(), {ids, names});

  auto maybe_reader = arrow::RecordBatchReader::Make({batch});
  ARROW_EXPECT_OK(maybe_reader.status());
  return *maybe_reader;
}

//--------------------------------------------------
// Server fixture
//--------------------------------------------------

class BulkIngestServerFixture
    : public gizmosql::testing::ServerTestFixture<BulkIngestServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "bulk_ingest_tester.db",
        .port = DEFAULT_FLIGHT_PORT,
        .health_port = DEFAULT_HEALTH_PORT,
        .username = "tester",
        .password = "tester",
    };
  }
};

// Static member definitions required by the template
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<BulkIngestServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<BulkIngestServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<BulkIngestServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<BulkIngestServerFixture>::config_{};

//--------------------------------------------------
// Integration test
//--------------------------------------------------
TEST_F(BulkIngestServerFixture, ExecuteIngestEndToEnd) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;

  // Authenticate and attach bearer header
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  auto record_batch_reader = MakeTestBatches();

  TableDefinitionOptions table_opts;
  table_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  table_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;

  std::unordered_map<std::string, std::string> ingest_options = {{"key1", "val1"},
                                                                 {"key2", "val2"}};

  auto maybe_rows = sql_client.ExecuteIngest(
      call_options, record_batch_reader, table_opts, "test_table_with_default",
      std::nullopt, std::nullopt, false /* temporary */,
      arrow::flight::sql::no_transaction(), ingest_options);

  if (!maybe_rows.ok()) {
    std::cerr << "\nExecuteIngest failed:\n"
              << maybe_rows.status().ToString() << std::endl;
    FAIL() << "ExecuteIngest failed";
  }

  auto updated_rows = *maybe_rows;
  std::cerr << "ExecuteIngest succeeded: " << updated_rows << " rows" << std::endl;

  ASSERT_EQ(updated_rows, 3) << "Expected 3 ingested rows";
}

// Regression test for https://github.com/gizmodata/gizmosql/issues/155
// Ingest must succeed when the client already has an open transaction
// (e.g. ADBC clients with autocommit=False). Previously, the server
// unconditionally opened a nested transaction which DuckDB rejects.
TEST_F(BulkIngestServerFixture, ExecuteIngestInsideOpenTransaction) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  // Open a client-side transaction — mimics autocommit=False.
  ASSERT_ARROW_OK_AND_ASSIGN(auto transaction, sql_client.BeginTransaction(call_options));

  TableDefinitionOptions table_opts;
  table_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  table_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;

  // Case 1: client sends the transaction_id on the ingest message.
  {
    auto reader = MakeTestBatches();
    auto maybe_rows = sql_client.ExecuteIngest(
        call_options, reader, table_opts, "ingest_in_txn_with_id", std::nullopt,
        std::nullopt, false /* temporary */, transaction, {});
    ASSERT_TRUE(maybe_rows.ok())
        << "ExecuteIngest with transaction_id failed: " << maybe_rows.status().ToString();
    ASSERT_EQ(*maybe_rows, 3);
  }

  // Case 2: client omits transaction_id on the ingest (Go ADBC driver
  // behavior). Server must still detect the outer transaction and not
  // attempt to open a nested one.
  {
    auto reader = MakeTestBatches();
    auto maybe_rows = sql_client.ExecuteIngest(
        call_options, reader, table_opts, "ingest_in_txn_no_id", std::nullopt,
        std::nullopt, false /* temporary */, arrow::flight::sql::no_transaction(), {});
    ASSERT_TRUE(maybe_rows.ok())
        << "ExecuteIngest without transaction_id inside open txn failed: "
        << maybe_rows.status().ToString();
    ASSERT_EQ(*maybe_rows, 3);
  }

  // Commit the outer transaction — rows should now be visible.
  ASSERT_ARROW_OK(sql_client.Commit(call_options, transaction));
}

// Regression test for https://github.com/gizmodata/gizmosql/issues/158
// Repeated ingests with temporary=true must not fail with "already exists".
// Previously, TableExists() only consulted CURRENT_DATABASE(), missing tables
// in DuckDB's implicit `temp` catalog, so the server treated the temp table
// as non-existent on subsequent calls and tried to CREATE it again.
TEST_F(BulkIngestServerFixture, ExecuteIngestTemporaryRepeatable) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  // Case 1: create_append twice (kCreate + kAppend)
  {
    TableDefinitionOptions opts;
    opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
    opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;

    for (int i = 0; i < 2; ++i) {
      auto reader = MakeTestBatches();
      auto maybe_rows = sql_client.ExecuteIngest(
          call_options, reader, opts, "temp_create_append", std::nullopt,
          std::nullopt, true /* temporary */, arrow::flight::sql::no_transaction(), {});
      ASSERT_TRUE(maybe_rows.ok())
          << "temp create_append iter " << i << ": " << maybe_rows.status().ToString();
      ASSERT_EQ(*maybe_rows, 3);
    }
  }

  // Case 2: replace twice (kCreate + kReplace)
  {
    TableDefinitionOptions opts;
    opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
    opts.if_exists = TableDefinitionOptionsTableExistsOption::kReplace;

    for (int i = 0; i < 2; ++i) {
      auto reader = MakeTestBatches();
      auto maybe_rows = sql_client.ExecuteIngest(
          call_options, reader, opts, "temp_replace", std::nullopt, std::nullopt,
          true /* temporary */, arrow::flight::sql::no_transaction(), {});
      ASSERT_TRUE(maybe_rows.ok())
          << "temp replace iter " << i << ": " << maybe_rows.status().ToString();
      ASSERT_EQ(*maybe_rows, 3);
    }
  }

  // Case 3: create then append (kCreate+kFail, then kFail+kAppend)
  {
    {
      TableDefinitionOptions create_opts;
      create_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
      create_opts.if_exists = TableDefinitionOptionsTableExistsOption::kFail;
      auto reader = MakeTestBatches();
      auto maybe_rows = sql_client.ExecuteIngest(
          call_options, reader, create_opts, "temp_then_append", std::nullopt,
          std::nullopt, true /* temporary */, arrow::flight::sql::no_transaction(), {});
      ASSERT_TRUE(maybe_rows.ok())
          << "temp create: " << maybe_rows.status().ToString();
      ASSERT_EQ(*maybe_rows, 3);
    }
    {
      TableDefinitionOptions append_opts;
      append_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kFail;
      append_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;
      auto reader = MakeTestBatches();
      auto maybe_rows = sql_client.ExecuteIngest(
          call_options, reader, append_opts, "temp_then_append", std::nullopt,
          std::nullopt, true /* temporary */, arrow::flight::sql::no_transaction(), {});
      ASSERT_TRUE(maybe_rows.ok())
          << "temp append to existing: " << maybe_rows.status().ToString();
      ASSERT_EQ(*maybe_rows, 3);
    }
  }
}
