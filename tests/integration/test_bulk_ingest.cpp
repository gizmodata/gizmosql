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
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "arrow/flight/sql/types.h"
#include "arrow/flight/sql/client.h"
#include "arrow/api.h"
#include "arrow/extension/bool8.h"
#include "arrow/extension/uuid.h"
#include "arrow/json/from_string.h"
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

namespace {

std::shared_ptr<arrow::Array> Json(const std::shared_ptr<arrow::DataType>& type,
                                   const std::string& json) {
  return arrow::json::ArrayFromJSONString(type, json).ValueOrDie();
}

std::shared_ptr<arrow::Array> DictJson(const std::shared_ptr<arrow::DataType>& type,
                                       const std::string& indices,
                                       const std::string& dictionary) {
  return arrow::json::DictArrayFromJSONString(type, indices, dictionary).ValueOrDie();
}

struct IngestClient {
  std::unique_ptr<FlightSqlClient> sql_client;
  arrow::flight::FlightCallOptions call_options;
};

arrow::Result<IngestClient> ConnectIngestClient(int port, const std::string& username,
                                                const std::string& password) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(location));
  IngestClient result;
  ARROW_ASSIGN_OR_RAISE(auto bearer,
                        client->AuthenticateBasicToken({}, username, password));
  result.call_options.headers.push_back(bearer);
  result.sql_client = std::make_unique<FlightSqlClient>(std::move(client));
  return result;
}

arrow::Result<int64_t> IngestReplace(IngestClient& c, const std::string& table_name,
                                     const std::shared_ptr<arrow::RecordBatch>& batch) {
  ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
  TableDefinitionOptions opts;
  opts.if_not_exist = TableDefinitionOptionsTableNotExistOption::kCreate;
  opts.if_exists = TableDefinitionOptionsTableExistsOption::kReplace;
  return c.sql_client->ExecuteIngest(c.call_options, reader, opts, table_name,
                                     std::nullopt, std::nullopt, false /* temporary */,
                                     arrow::flight::sql::no_transaction(), {});
}

arrow::Result<std::shared_ptr<arrow::Table>> QueryTable(IngestClient& c,
                                                        const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, c.sql_client->Execute(c.call_options, sql));
  ARROW_ASSIGN_OR_RAISE(auto stream,
                        c.sql_client->DoGet(c.call_options, info->endpoints()[0].ticket));
  return stream->ToTable();
}

// column_name -> data_type of an ingested table, as information_schema reports it.
std::map<std::string, std::string> ColumnTypes(IngestClient& c,
                                               const std::string& table_name) {
  std::map<std::string, std::string> types;
  auto table = QueryTable(c,
                          "SELECT column_name, data_type FROM information_schema.columns "
                          "WHERE table_name = '" +
                              table_name + "'");
  EXPECT_TRUE(table.ok()) << table.status().ToString();
  if (!table.ok()) return types;
  for (int64_t i = 0; i < (*table)->num_rows(); ++i) {
    types[(*table)->column(0)->GetScalar(i).ValueOrDie()->ToString()] =
        (*table)->column(1)->GetScalar(i).ValueOrDie()->ToString();
  }
  return types;
}

// Row 0 of a query, every column rendered as text.
std::vector<std::string> FirstRow(IngestClient& c, const std::string& sql) {
  std::vector<std::string> row;
  auto table = QueryTable(c, sql);
  EXPECT_TRUE(table.ok()) << table.status().ToString();
  if (!table.ok() || (*table)->num_rows() == 0) return row;
  for (int i = 0; i < (*table)->num_columns(); ++i) {
    row.push_back((*table)->column(i)->GetScalar(0).ValueOrDie()->ToString());
  }
  return row;
}

}  // namespace

// Regression test for https://github.com/gizmodata/gizmosql/issues/196:
// a month_day_nano interval column (what ADBC drivers return for SQL INTERVAL)
// was created as VARCHAR, so interval math on it failed. It must become
// INTERVAL like a duration column does, months included.
TEST_F(BulkIngestServerFixture, MonthDayNanoIntervalIngestsAsInterval) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto c, ConnectIngestClient(GetPort(), GetUsername(), GetPassword()));

  auto schema =
      arrow::schema({arrow::field("mdn", arrow::month_day_nano_interval()),
                     arrow::field("ym", arrow::month_day_nano_interval()),
                     arrow::field("dur", arrow::duration(arrow::TimeUnit::MICRO))});
  auto batch = arrow::RecordBatch::Make(
      schema, 1,
      {Json(arrow::month_day_nano_interval(), "[[0, 0, 7200000000000]]"),
       Json(arrow::month_day_nano_interval(), "[[14, 3, 0]]"),
       Json(arrow::duration(arrow::TimeUnit::MICRO), "[7200000000]")});
  ASSERT_ARROW_OK_AND_ASSIGN(auto rows, IngestReplace(c, "interval_ingest", batch));
  EXPECT_EQ(rows, 1);

  const auto types = ColumnTypes(c, "interval_ingest");
  EXPECT_EQ(types.at("mdn"), "INTERVAL");
  EXPECT_EQ(types.at("ym"), "INTERVAL");
  EXPECT_EQ(types.at("dur"), "INTERVAL");

  EXPECT_EQ(FirstRow(c,
                     "SELECT epoch(mdn), epoch(dur), ym::VARCHAR, "
                     "ym = INTERVAL '1 year 2 months 3 days' FROM interval_ingest"),
            (std::vector<std::string>{"7200", "7200", "1 year 2 months 3 days", "true"}));
}

// Every Arrow type DuckDB's arrow_scan can read gets the column type DuckDB's
// own ingest gives it (checked against DuckDB 1.5.6 via pyarrow), with its
// value intact. Before #196 these all fell through to VARCHAR.
TEST_F(BulkIngestServerFixture, ArrowTypesIngestAsTheirDuckDBTypes) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto c, ConnectIngestClient(GetPort(), GetUsername(), GetPassword()));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto list_view, arrow::ListViewArray::FromArrays(*Json(arrow::int32(), "[0]"),
                                                       *Json(arrow::int32(), "[2]"),
                                                       *Json(arrow::int32(), "[1, 2]")));
  ASSERT_ARROW_OK_AND_ASSIGN(auto large_list_view, arrow::LargeListViewArray::FromArrays(
                                                       *Json(arrow::int64(), "[0]"),
                                                       *Json(arrow::int64(), "[1]"),
                                                       *Json(arrow::int32(), "[7]")));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto run_end_encoded,
      arrow::RunEndEncodedArray::Make(1, Json(arrow::int32(), "[1]"),
                                      Json(arrow::utf8(), R"(["ree"])")));
  auto uuid = arrow::ExtensionType::WrapArray(
      arrow::extension::uuid(),
      Json(arrow::fixed_size_binary(16), R"(["0123456789abcdef"])"));
  auto bool8 = arrow::ExtensionType::WrapArray(arrow::extension::bool8(),
                                               Json(arrow::int8(), "[1]"));
  auto sparse_union_type = arrow::sparse_union(
      {arrow::field("i", arrow::int64()), arrow::field("s", arrow::utf8())}, {0, 1});

  const std::vector<std::shared_ptr<arrow::Array>> arrays = {
      Json(arrow::utf8_view(), R"(["view"])"),
      Json(arrow::binary_view(), R"(["bv"])"),
      Json(arrow::fixed_size_binary(4), R"(["abcd"])"),
      DictJson(arrow::dictionary(arrow::int32(), arrow::utf8()), "[0]", R"(["dict"])"),
      Json(arrow::decimal32(7, 2), R"(["12.34"])"),
      Json(arrow::decimal64(15, 3), R"(["123.456"])"),
      list_view,
      large_list_view,
      Json(arrow::month_interval(), "[14]"),
      Json(arrow::day_time_interval(), "[[1, 3600000]]"),
      run_end_encoded,
      uuid,
      bool8,
      Json(sparse_union_type, R"([[1, "u"]])"),
  };
  const std::vector<std::string> names = {
      "sview",  "bview",  "fsb",      "dict", "dec32", "dec64", "lview",
      "llview", "months", "day_time", "ree",  "uuid",  "b8",    "sunion"};
  arrow::FieldVector fields;
  for (size_t i = 0; i < arrays.size(); ++i) {
    fields.push_back(arrow::field(names[i], arrays[i]->type()));
  }
  auto batch = arrow::RecordBatch::Make(arrow::schema(fields), 1, arrays);
  ASSERT_ARROW_OK_AND_ASSIGN(auto rows, IngestReplace(c, "arrow_types_ingest", batch));
  EXPECT_EQ(rows, 1);

  const std::map<std::string, std::string> expected_types = {
      {"sview", "VARCHAR"},      {"bview", "BLOB"},
      {"fsb", "BLOB"},           {"dict", "VARCHAR"},
      {"dec32", "DECIMAL(7,2)"}, {"dec64", "DECIMAL(15,3)"},
      {"lview", "INTEGER[]"},    {"llview", "INTEGER[]"},
      {"months", "INTERVAL"},    {"day_time", "INTERVAL"},
      {"ree", "VARCHAR"},        {"uuid", "UUID"},
      {"b8", "BOOLEAN"},         {"sunion", "UNION(i BIGINT, s VARCHAR)"},
  };
  EXPECT_EQ(ColumnTypes(c, "arrow_types_ingest"), expected_types);

  EXPECT_EQ(FirstRow(c,
                     "SELECT sview, bview::VARCHAR, fsb::VARCHAR, dict, dec32::VARCHAR, "
                     "dec64::VARCHAR, lview::VARCHAR, llview::VARCHAR, months::VARCHAR, "
                     "day_time::VARCHAR, ree, uuid::VARCHAR, b8::VARCHAR, "
                     "sunion::VARCHAR FROM arrow_types_ingest"),
            (std::vector<std::string>{
                "view", "bv", "abcd", "dict", "12.34", "123.456", "[1, 2]", "[7]",
                "1 year 2 months", "1 day 01:00:00", "ree",
                "30313233-3435-3637-3839-616263646566", "true", "u"}));
}

// An Arrow type DuckDB cannot store is refused with the reason, instead of
// silently becoming a VARCHAR column (#196). Its twin, a float32 column, ingests.
TEST_F(BulkIngestServerFixture, UnsupportedArrowTypeIsRefusedNotStoredAsVarchar) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto c, ConnectIngestClient(GetPort(), GetUsername(), GetPassword()));

  auto half =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("h", arrow::float16())}), 1,
                               {Json(arrow::float16(), "[1.5]")});
  auto refused = IngestReplace(c, "halffloat_ingest", half);
  ASSERT_FALSE(refused.ok()) << "halffloat ingest unexpectedly succeeded";
  EXPECT_NE(refused.status().message().find(
                "Arrow type halffloat has no DuckDB equivalent for bulk ingest"),
            std::string::npos)
      << refused.status().ToString();
  EXPECT_TRUE(ColumnTypes(c, "halffloat_ingest").empty());

  // A day_time interval DuckDB would misread: top-level columns are converted
  // (see ArrowTypesIngestAsTheirDuckDBTypes); nested ones are refused.
  auto nested_day_time = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("l", arrow::list(arrow::day_time_interval()))}), 1,
      {Json(arrow::list(arrow::day_time_interval()), "[[[1, 3600000]]]")});
  auto nested_refused = IngestReplace(c, "nested_day_time_ingest", nested_day_time);
  ASSERT_FALSE(nested_refused.ok()) << "nested day_time ingest unexpectedly succeeded";
  EXPECT_NE(nested_refused.status().message().find(
                "a day_time interval nested in another type cannot be ingested"),
            std::string::npos)
      << nested_refused.status().ToString();

  auto single =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("h", arrow::float32())}), 1,
                               {Json(arrow::float32(), "[1.5]")});
  ASSERT_ARROW_OK_AND_ASSIGN(auto rows, IngestReplace(c, "float_ingest", single));
  EXPECT_EQ(rows, 1);
  EXPECT_EQ(ColumnTypes(c, "float_ingest").at("h"), "FLOAT");
}
