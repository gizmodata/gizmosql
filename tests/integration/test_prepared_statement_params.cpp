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

// Prepared-statement bind parameter handling: typed values, NULLs,
// dictionary-encoded strings, multi-row batches (one execution per row),
// and untyped placeholders (`SELECT ? AS x`) whose types DuckDB only
// resolves at execution.

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

namespace {

class PreparedParamsFixture
    : public gizmosql::testing::ServerTestFixture<PreparedParamsFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "prepared_params_tester.db",
        .port = DEFAULT_FLIGHT_PORT,
        .health_port = DEFAULT_HEALTH_PORT,
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
    gizmosql::testing::ServerTestFixture<PreparedParamsFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<PreparedParamsFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<PreparedParamsFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<PreparedParamsFixture>::config_{};

TEST_F(PreparedParamsFixture, NullParameterIsStoredAsSqlNull) {
  Exec("CREATE OR REPLACE TABLE pp_null (id INT, name VARCHAR)");

  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  ARROW_EXPECT_OK(id_builder.Append(1));
  ARROW_EXPECT_OK(name_builder.AppendNull());
  std::shared_ptr<arrow::Array> id_arr, name_arr;
  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  ARROW_EXPECT_OK(name_builder.Finish(&name_arr));
  auto batch =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int32()),
                                              arrow::field("name", arrow::utf8())}),
                               1, {id_arr, name_arr});

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "INSERT INTO pp_null VALUES (?, ?)"));
  ASSERT_ARROW_OK(prepared->SetParameters(batch));
  ASSERT_ARROW_OK_AND_ASSIGN(auto affected, prepared->ExecuteUpdate(call_options_));
  EXPECT_EQ(affected, 1);

  // Previously stored as the literal string "null".
  EXPECT_EQ(Scalar("SELECT name IS NULL FROM pp_null WHERE id = 1"), "true");
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM pp_null WHERE name = 'null'"), "0");
}

TEST_F(PreparedParamsFixture, DictionaryEncodedStringParameterIsDecoded) {
  Exec("CREATE OR REPLACE TABLE pp_dict (id INT, name VARCHAR)");

  arrow::Int32Builder id_builder;
  ARROW_EXPECT_OK(id_builder.Append(1));
  std::shared_ptr<arrow::Array> id_arr;
  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));

  // What Arrow JS `tableFromArrays` produces for a string column.
  arrow::StringBuilder dict_builder;
  ARROW_EXPECT_OK(dict_builder.AppendValues({"alpha", "beta"}));
  std::shared_ptr<arrow::Array> dict;
  ARROW_EXPECT_OK(dict_builder.Finish(&dict));
  arrow::Int32Builder idx_builder;
  ARROW_EXPECT_OK(idx_builder.Append(1));  // -> "beta"
  std::shared_ptr<arrow::Array> indices;
  ARROW_EXPECT_OK(idx_builder.Finish(&indices));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto name_arr,
      arrow::DictionaryArray::FromArrays(arrow::dictionary(arrow::int32(), arrow::utf8()),
                                         indices, dict));

  auto batch = arrow::RecordBatch::Make(
      arrow::schema(
          {arrow::field("id", arrow::int32()),
           arrow::field("name", arrow::dictionary(arrow::int32(), arrow::utf8()))}),
      1, {id_arr, name_arr});

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "INSERT INTO pp_dict VALUES (?, ?)"));
  ASSERT_ARROW_OK(prepared->SetParameters(batch));
  ASSERT_ARROW_OK_AND_ASSIGN(auto affected, prepared->ExecuteUpdate(call_options_));
  EXPECT_EQ(affected, 1);

  // Previously stored as a dump of the whole dictionary.
  EXPECT_EQ(Scalar("SELECT name FROM pp_dict WHERE id = 1"), "beta");
}

TEST_F(PreparedParamsFixture, MultiRowBatchExecutesUpdateOncePerRow) {
  Exec("CREATE OR REPLACE TABLE pp_multi (id INT, name VARCHAR)");

  arrow::Int32Builder id_builder;
  arrow::StringBuilder name_builder;
  ARROW_EXPECT_OK(id_builder.AppendValues({1, 2, 3}));
  ARROW_EXPECT_OK(name_builder.AppendValues({"one", "two", "three"}));
  std::shared_ptr<arrow::Array> id_arr, name_arr;
  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  ARROW_EXPECT_OK(name_builder.Finish(&name_arr));
  auto batch =
      arrow::RecordBatch::Make(arrow::schema({arrow::field("id", arrow::int32()),
                                              arrow::field("name", arrow::utf8())}),
                               3, {id_arr, name_arr});

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "INSERT INTO pp_multi VALUES (?, ?)"));
  ASSERT_ARROW_OK(prepared->SetParameters(batch));
  // Previously: "Parameter argument/count mismatch, identifiers of the excess
  // parameters: 3, 4" — every value of every row was bound positionally.
  ASSERT_ARROW_OK_AND_ASSIGN(auto affected, prepared->ExecuteUpdate(call_options_));
  EXPECT_EQ(affected, 3);

  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM pp_multi"), "3");
  EXPECT_EQ(Scalar("SELECT string_agg(name, ',' ORDER BY id) FROM pp_multi"),
            "one,two,three");
}

TEST_F(PreparedParamsFixture, MultiRowBatchIsRejectedForQuery) {
  Exec("CREATE OR REPLACE TABLE pp_q (id INT)");
  Exec("INSERT INTO pp_q VALUES (1), (2)");

  arrow::Int32Builder id_builder;
  ARROW_EXPECT_OK(id_builder.AppendValues({1, 2}));
  std::shared_ptr<arrow::Array> id_arr;
  ARROW_EXPECT_OK(id_builder.Finish(&id_arr));
  auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("id", arrow::int32())}), 2, {id_arr});

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "SELECT id FROM pp_q WHERE id = ?"));
  ASSERT_ARROW_OK(prepared->SetParameters(batch));
  auto result = prepared->Execute(call_options_);
  ASSERT_FALSE(result.ok());
  EXPECT_NE(result.status().ToString().find("exactly one row"), std::string::npos)
      << result.status().ToString();
}

TEST_F(PreparedParamsFixture, TypedParametersKeepTheirTypes) {
  Exec(
      "CREATE OR REPLACE TABLE pp_typed (id BIGINT, ratio DOUBLE, d DATE, ts TIMESTAMP, "
      "b BLOB, flag BOOLEAN)");

  arrow::Int64Builder id_b;
  arrow::DoubleBuilder ratio_b;
  arrow::Date32Builder d_b;
  arrow::TimestampBuilder ts_b(arrow::timestamp(arrow::TimeUnit::MILLI),
                               arrow::default_memory_pool());
  arrow::BinaryBuilder b_b;
  arrow::BooleanBuilder flag_b;
  ARROW_EXPECT_OK(id_b.Append(9007199254740993LL));  // beyond double precision
  ARROW_EXPECT_OK(ratio_b.Append(0.25));
  ARROW_EXPECT_OK(d_b.Append(19723));           // 2024-01-01
  ARROW_EXPECT_OK(ts_b.Append(1704067200123));  // 2024-01-01 00:00:00.123
  ARROW_EXPECT_OK(b_b.Append(std::string("\x00\x01\xff", 3)));
  ARROW_EXPECT_OK(flag_b.Append(true));
  std::shared_ptr<arrow::Array> id_a, ratio_a, d_a, ts_a, b_a, flag_a;
  ARROW_EXPECT_OK(id_b.Finish(&id_a));
  ARROW_EXPECT_OK(ratio_b.Finish(&ratio_a));
  ARROW_EXPECT_OK(d_b.Finish(&d_a));
  ARROW_EXPECT_OK(ts_b.Finish(&ts_a));
  ARROW_EXPECT_OK(b_b.Finish(&b_a));
  ARROW_EXPECT_OK(flag_b.Finish(&flag_a));
  auto batch = arrow::RecordBatch::Make(
      arrow::schema(
          {arrow::field("id", arrow::int64()), arrow::field("ratio", arrow::float64()),
           arrow::field("d", arrow::date32()),
           arrow::field("ts", arrow::timestamp(arrow::TimeUnit::MILLI)),
           arrow::field("b", arrow::binary()), arrow::field("flag", arrow::boolean())}),
      1, {id_a, ratio_a, d_a, ts_a, b_a, flag_a});

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_,
                           "INSERT INTO pp_typed VALUES (?, ?, ?, ?, ?, ?)"));
  ASSERT_ARROW_OK(prepared->SetParameters(batch));
  ASSERT_ARROW_OK_AND_ASSIGN(auto affected, prepared->ExecuteUpdate(call_options_));
  EXPECT_EQ(affected, 1);

  EXPECT_EQ(Scalar("SELECT id FROM pp_typed"), "9007199254740993");
  EXPECT_EQ(Scalar("SELECT ratio FROM pp_typed"), "0.25");
  EXPECT_EQ(Scalar("SELECT CAST(d AS VARCHAR) FROM pp_typed"), "2024-01-01");
  EXPECT_EQ(Scalar("SELECT CAST(ts AS VARCHAR) FROM pp_typed"),
            "2024-01-01 00:00:00.123");
  EXPECT_EQ(Scalar("SELECT octet_length(b) FROM pp_typed"), "3");
  EXPECT_EQ(Scalar("SELECT flag FROM pp_typed"), "true");
}

TEST_F(PreparedParamsFixture, UntypedPlaceholderResolvesAtExecution) {
  // Previously Prepare failed with "Unexpected error in RPC handling" because
  // the unresolved placeholder type could not be converted to Arrow.
  ASSERT_ARROW_OK_AND_ASSIGN(auto prepared,
                             sql_client_->Prepare(call_options_, "SELECT ? AS x"));

  // Prepare-time schemas advertise VARCHAR placeholders.
  ASSERT_NE(prepared->dataset_schema(), nullptr);
  EXPECT_EQ(prepared->dataset_schema()->field(0)->type()->id(), arrow::Type::STRING);
  ASSERT_NE(prepared->parameter_schema(), nullptr);
  EXPECT_EQ(prepared->parameter_schema()->field(0)->type()->id(), arrow::Type::STRING);

  arrow::Int32Builder x_b;
  ARROW_EXPECT_OK(x_b.Append(42));
  std::shared_ptr<arrow::Array> x_a;
  ARROW_EXPECT_OK(x_b.Finish(&x_a));
  auto batch = arrow::RecordBatch::Make(
      arrow::schema({arrow::field("x", arrow::int32())}), 1, {x_a});
  ASSERT_ARROW_OK(prepared->SetParameters(batch));

  ASSERT_ARROW_OK_AND_ASSIGN(auto info, prepared->Execute(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());

  // The stream carries the real type resolved from the bound value.
  ASSERT_EQ(table->num_rows(), 1);
  EXPECT_EQ(table->schema()->field(0)->type()->id(), arrow::Type::INT32);
  ASSERT_ARROW_OK_AND_ASSIGN(auto scalar, table->column(0)->GetScalar(0));
  EXPECT_EQ(scalar->ToString(), "42");

  // Re-executing with a different type re-resolves.
  arrow::StringBuilder s_b;
  ARROW_EXPECT_OK(s_b.Append("hello"));
  std::shared_ptr<arrow::Array> s_a;
  ARROW_EXPECT_OK(s_b.Finish(&s_a));
  ASSERT_ARROW_OK(prepared->SetParameters(arrow::RecordBatch::Make(
      arrow::schema({arrow::field("x", arrow::utf8())}), 1, {s_a})));
  ASSERT_ARROW_OK_AND_ASSIGN(auto info2, prepared->Execute(call_options_));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto stream2, sql_client_->DoGet(call_options_, info2->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table2, stream2->ToTable());
  EXPECT_EQ(table2->schema()->field(0)->type()->id(), arrow::Type::STRING);
  ASSERT_ARROW_OK_AND_ASSIGN(auto scalar2, table2->column(0)->GetScalar(0));
  EXPECT_EQ(scalar2->ToString(), "hello");
}

TEST_F(PreparedParamsFixture, ParameterlessPreparedUpdateStillExecutesOnce) {
  Exec("CREATE OR REPLACE TABLE pp_noparam (id INT)");
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "INSERT INTO pp_noparam VALUES (7)"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto affected, prepared->ExecuteUpdate(call_options_));
  EXPECT_EQ(affected, 1);
  EXPECT_EQ(Scalar("SELECT COUNT(*) FROM pp_noparam"), "1");
}

TEST_F(PreparedParamsFixture, TypedPreparedQueryStillReportsSchemaInFlightInfo) {
  Exec("CREATE OR REPLACE TABLE pp_sel (id INT, name VARCHAR)");
  Exec("INSERT INTO pp_sel VALUES (1, 'a'), (2, 'b')");

  arrow::Int32Builder id_b;
  ARROW_EXPECT_OK(id_b.Append(2));
  std::shared_ptr<arrow::Array> id_a;
  ARROW_EXPECT_OK(id_b.Finish(&id_a));

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      sql_client_->Prepare(call_options_, "SELECT name FROM pp_sel WHERE id = ?"));
  ASSERT_ARROW_OK(prepared->SetParameters(arrow::RecordBatch::Make(
      arrow::schema({arrow::field("id", arrow::int32())}), 1, {id_a})));
  ASSERT_ARROW_OK_AND_ASSIGN(auto info, prepared->Execute(call_options_));
  // Resolved schemas are still embedded in the FlightInfo.
  arrow::ipc::DictionaryMemo memo;
  ASSERT_ARROW_OK_AND_ASSIGN(auto info_schema, info->GetSchema(&memo));
  EXPECT_EQ(info_schema->field(0)->type()->id(), arrow::Type::STRING);
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto stream, sql_client_->DoGet(call_options_, info->endpoints()[0].ticket));
  ASSERT_ARROW_OK_AND_ASSIGN(auto table, stream->ToTable());
  ASSERT_EQ(table->num_rows(), 1);
  ASSERT_ARROW_OK_AND_ASSIGN(auto scalar, table->column(0)->GetScalar(0));
  EXPECT_EQ(scalar->ToString(), "b");
}
