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
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/util/logger.h"
#include "test_util.h"
#include "test_server_fixture.h"

using arrow::util::ArrowLogLevel;
using arrow::util::LogDetails;
using arrow::util::Logger;
using arrow::util::LoggerRegistry;

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
        .print_queries = true,
    };
  }
};

namespace {

// Captures every log record so tests can assert on the structured fields.
class CapturingLogger final : public Logger {
 public:
  struct Entry {
    ArrowLogLevel severity;
    std::string message;
  };

  explicit CapturingLogger(ArrowLogLevel threshold) : threshold_(threshold) {}

  bool is_enabled() const override { return true; }
  ArrowLogLevel severity_threshold() const override { return threshold_; }

  void Log(const LogDetails& d) override {
    std::lock_guard<std::mutex> lk(mu_);
    entries_.push_back({d.severity, std::string(d.message)});
  }

  std::vector<Entry> TakeEntries() {
    std::lock_guard<std::mutex> lk(mu_);
    auto result = std::move(entries_);
    entries_.clear();
    return result;
  }

 private:
  ArrowLogLevel threshold_;
  mutable std::mutex mu_;
  std::vector<Entry> entries_;
};

}  // namespace

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

// Regression: a typeless (Arrow `null`) column — what pandas produces for an
// object column whose values are all None in the ingested chunk — rendered as
// a "NULL" column type in the generated CREATE TABLE. Plain DuckDB silently
// resolved that to INTEGER; DuckLake catalogs rejected it outright
// ("Failed to convert DuckDB type to DuckLake - unsupported type NULL").
// It must become VARCHAR, and a later append with real strings must work.
TEST_F(BulkIngestServerFixture, NullTypedColumnIngestsAsVarchar) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(
                                              location, arrow::flight::FlightClientOptions{}));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  auto schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("sparse", arrow::null())});

  // First chunk: every value of "sparse" is null -> Arrow null type.
  arrow::Int32Builder id_builder;
  ARROW_EXPECT_OK(id_builder.AppendValues({1, 2}));
  std::shared_ptr<arrow::Array> ids;
  ARROW_EXPECT_OK(id_builder.Finish(&ids));
  auto nulls = std::make_shared<arrow::NullArray>(2);
  auto first = arrow::RecordBatch::Make(schema, 2, {ids, nulls});
  ASSERT_ARROW_OK_AND_ASSIGN(auto first_reader, arrow::RecordBatchReader::Make({first}));

  TableDefinitionOptions create_opts;
  create_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  create_opts.if_exists = TableDefinitionOptionsTableExistsOption::kReplace;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto created_rows,
      sql_client.ExecuteIngest(call_options, first_reader, create_opts, "null_col_ingest",
                               std::nullopt, std::nullopt, false,
                               arrow::flight::sql::no_transaction(), {}));
  EXPECT_EQ(created_rows, 2);

  // Second chunk: the same column now carries strings -> must append cleanly.
  auto typed_schema = arrow::schema(
      {arrow::field("id", arrow::int32()), arrow::field("sparse", arrow::utf8())});
  arrow::Int32Builder id2_builder;
  arrow::StringBuilder str_builder;
  ARROW_EXPECT_OK(id2_builder.AppendValues({3}));
  ARROW_EXPECT_OK(str_builder.Append("value"));
  std::shared_ptr<arrow::Array> ids2, strs;
  ARROW_EXPECT_OK(id2_builder.Finish(&ids2));
  ARROW_EXPECT_OK(str_builder.Finish(&strs));
  auto second = arrow::RecordBatch::Make(typed_schema, 1, {ids2, strs});
  ASSERT_ARROW_OK_AND_ASSIGN(auto second_reader, arrow::RecordBatchReader::Make({second}));

  TableDefinitionOptions append_opts;
  append_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kFail;
  append_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto appended_rows,
      sql_client.ExecuteIngest(call_options, second_reader, append_opts, "null_col_ingest",
                               std::nullopt, std::nullopt, false,
                               arrow::flight::sql::no_transaction(), {}));
  EXPECT_EQ(appended_rows, 1);

  // Column type is VARCHAR and the data round-trips.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto info, sql_client.Execute(call_options,
                                    "SELECT typeof(sparse), COUNT(*), COUNT(sparse) "
                                    "FROM null_col_ingest GROUP BY 1"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto stream,
                             sql_client.DoGet(call_options, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_ARROW_OK_AND_ASSIGN(auto type_name, table->column(0)->GetScalar(0));
  ASSERT_ARROW_OK_AND_ASSIGN(auto total, table->column(1)->GetScalar(0));
  ASSERT_ARROW_OK_AND_ASSIGN(auto non_null, table->column(2)->GetScalar(0));
  EXPECT_EQ(type_name->ToString(), "VARCHAR");
  EXPECT_EQ(total->ToString(), "3");
  EXPECT_EQ(non_null->ToString(), "1");
}

// Bulk ingest bypasses DuckDBStatement, so it must emit its own query-style
// logs: caller, fully qualified target table, and ingested row count at INFO,
// independent of the global --log-level (query logs use their own threshold).
TEST_F(BulkIngestServerFixture, ExecuteIngestLogsCallerTargetAndRowCount) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  struct RestoreLogger {
    std::shared_ptr<Logger> previous;
    ~RestoreLogger() { LoggerRegistry::SetDefaultLogger(previous); }
  } restore{LoggerRegistry::GetDefaultLogger()};
  auto capture = std::make_shared<CapturingLogger>(ArrowLogLevel::ARROW_WARNING);
  LoggerRegistry::SetDefaultLogger(capture);

  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto client, arrow::flight::FlightClient::Connect(location));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  arrow::flight::sql::FlightSqlClient sql_client(std::move(client));

  TableDefinitionOptions table_opts;
  table_opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  table_opts.if_exists = TableDefinitionOptionsTableExistsOption::kAppend;

  // Omit catalog and schema: the log must still show the effective ones.
  auto reader = MakeTestBatches();
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto rows,
      sql_client.ExecuteIngest(call_options, reader, table_opts, "ingest_logging_table",
                               std::nullopt, std::nullopt, false /* temporary */,
                               arrow::flight::sql::no_transaction(), {}));
  ASSERT_EQ(rows, 3);

  const CapturingLogger::Entry* attempt = nullptr;
  const CapturingLogger::Entry* success = nullptr;
  auto entries = capture->TakeEntries();
  for (const auto& entry : entries) {
    if (entry.message.find("\"kind\":\"ingest\"") == std::string::npos) continue;
    if (entry.message.find("\"status\":\"attempt\"") != std::string::npos)
      attempt = &entry;
    if (entry.message.find("\"status\":\"success\"") != std::string::npos)
      success = &entry;
  }
  ASSERT_NE(attempt, nullptr) << "no ingest attempt log record";
  ASSERT_NE(success, nullptr) << "no ingest success log record";

  for (const auto* entry : {attempt, success}) {
    EXPECT_EQ(entry->severity, ArrowLogLevel::ARROW_INFO) << entry->message;
    EXPECT_NE(entry->message.find("\"user\":\"tester\""), std::string::npos)
        << entry->message;
    EXPECT_NE(entry->message.find("\"session_id\":\""), std::string::npos)
        << entry->message;
    EXPECT_NE(entry->message.find("\"catalog\":\"bulk_ingest_tester\""),
              std::string::npos)
        << entry->message;
    EXPECT_NE(entry->message.find("\"schema\":\"main\""), std::string::npos)
        << entry->message;
    EXPECT_NE(entry->message.find("\"table\":\"ingest_logging_table\""),
              std::string::npos)
        << entry->message;
    EXPECT_NE(entry->message.find("\"flight_method\":\"DoPutCommandStatementIngest\""),
              std::string::npos)
        << entry->message;
  }
  EXPECT_NE(success->message.find("\"rows_ingested\":\"3\""), std::string::npos)
      << success->message;
  EXPECT_NE(success->message.find("\"duration_ms\":\""), std::string::npos)
      << success->message;
}
