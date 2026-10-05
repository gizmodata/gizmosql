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

// VARIANT over Arrow (DuckDB 2.0+)
//
// DuckDB 2.0 exchanges VARIANT as Arrow's canonical `arrow.parquet.variant`
// extension type: struct<metadata: binary, value: binary> holding the Parquet
// Variant binary encoding. These tests check that GizmoSQL
//   - exports VARIANT columns with that tag and storage layout,
//   - ingests such columns (ExecuteIngest) back into a VARIANT column, and
//   - binds such values as prepared-statement parameters,
// each time comparing values through DuckDB's own VARIANT -> VARCHAR rendering.

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2

using arrow::flight::sql::FlightSqlClient;

namespace {

constexpr const char* kVariantExtension = "arrow.parquet.variant";

// One row per VARIANT shape we care about: scalars of several types, a nested
// struct/list, and a SQL NULL.
constexpr const char* kVariantRows =
    "SELECT * FROM (VALUES "
    "(1, 42::VARIANT), "
    "(2, 'hello'::VARIANT), "
    "(3, 3.5::DOUBLE::VARIANT), "
    "(4, {'a': 1, 'b': [true, NULL, false]}::VARIANT), "
    "(5, NULL::VARIANT)"
    ") AS t(id, v)";

class VariantServerFixture
    : public gizmosql::testing::ServerTestFixture<VariantServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31660,
        .health_port = 31661,
        .username = "variant_tester",
        .password = "variant_tester",
    };
  }

 protected:
  void SetUp() override {
    ASSERT_TRUE(IsServerReady()) << "Server not ready";
    ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                               arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                               arrow::flight::FlightClient::Connect(location));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer,
        flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options_.headers.push_back(bearer);
    client_ = std::make_unique<FlightSqlClient>(std::move(flight_client));
  }

  arrow::Result<std::shared_ptr<arrow::Table>> Query(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          client_->DoGet(call_options_, info->endpoints()[0].ticket));
    return reader->ToTable();
  }

  // Column `index` of `table` rendered as strings ("NULL" for nulls).
  static std::vector<std::string> Strings(const std::shared_ptr<arrow::Table>& table,
                                          int index) {
    std::vector<std::string> out;
    auto column = table->column(index);
    for (int64_t row = 0; row < column->length(); ++row) {
      auto scalar = column->GetScalar(row).ValueOrDie();
      out.push_back(scalar->is_valid ? scalar->ToString() : "NULL");
    }
    return out;
  }

  // DuckDB's own text rendering of the reference rows, for comparisons.
  std::vector<std::string> ExpectedText() {
    auto table = Query(std::string("SELECT v::VARCHAR FROM (") + kVariantRows +
                       ") ORDER BY id")
                     .ValueOrDie();
    return Strings(table, 0);
  }

  static void ExpectVariantField(const arrow::Field& field) {
    // This process does not register the extension type, so it arrives as its
    // storage type with the extension name in the field metadata.
    ASSERT_NE(field.metadata(), nullptr) << field.ToString();
    ASSERT_ARROW_OK_AND_ASSIGN(auto name, field.metadata()->Get("ARROW:extension:name"));
    EXPECT_EQ(name, kVariantExtension);
    ASSERT_EQ(field.type()->id(), arrow::Type::STRUCT) << field.type()->ToString();
    ASSERT_EQ(field.type()->num_fields(), 2);
    for (const char* child : {"metadata", "value"}) {
      auto child_field =
          static_cast<const arrow::StructType&>(*field.type()).GetFieldByName(child);
      ASSERT_NE(child_field, nullptr) << "missing '" << child << "' in " << field.ToString();
      const auto id = child_field->type()->id();
      EXPECT_TRUE(id == arrow::Type::BINARY || id == arrow::Type::LARGE_BINARY ||
                  id == arrow::Type::BINARY_VIEW)
          << child_field->ToString();
    }
  }

  arrow::flight::FlightCallOptions call_options_;
  std::unique_ptr<FlightSqlClient> client_;
};

}  // namespace

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<VariantServerFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<VariantServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<VariantServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<VariantServerFixture>::config_{};

TEST_F(VariantServerFixture, VariantExportsAsCanonicalArrowExtension) {
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto table, Query(std::string(kVariantRows) + " ORDER BY id"));
  ASSERT_EQ(table->num_rows(), 5);
  ExpectVariantField(*table->schema()->field(1));
  // Only the SQL NULL row is null at the top level.
  EXPECT_EQ(table->column(1)->null_count(), 1);
  EXPECT_FALSE(table->column(1)->GetScalar(4).ValueOrDie()->is_valid);
}

TEST_F(VariantServerFixture, VariantRoundTripsThroughIngest) {
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto exported, Query(std::string(kVariantRows) + " ORDER BY id"));

  arrow::flight::sql::TableDefinitionOptions options;
  options.if_not_exist =
      arrow::flight::sql::TableDefinitionOptionsTableNotExistOption::kCreate;
  options.if_exists = arrow::flight::sql::TableDefinitionOptionsTableExistsOption::kReplace;
  ASSERT_ARROW_OK_AND_ASSIGN(auto batches, exported->CombineChunksToBatch());
  ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                             arrow::RecordBatchReader::Make({batches}, exported->schema()));
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto rows, client_->ExecuteIngest(call_options_, reader, options, "variant_ingest",
                                        std::nullopt, std::nullopt, false,
                                        arrow::flight::sql::no_transaction(), {}));
  EXPECT_EQ(rows, 5);

  // The new column is a real VARIANT, and every value survived the trip.
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto column_type,
      Query("SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'variant_ingest' AND column_name = 'v'"));
  EXPECT_EQ(Strings(column_type, 0), std::vector<std::string>{"VARIANT"});
  ASSERT_ARROW_OK_AND_ASSIGN(auto ingested,
                             Query("SELECT v::VARCHAR FROM variant_ingest ORDER BY id"));
  EXPECT_EQ(Strings(ingested, 0), ExpectedText());
}

TEST_F(VariantServerFixture, VariantBindsAsPreparedStatementParameter) {
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto exported, Query(std::string(kVariantRows) + " ORDER BY id"));
  ASSERT_ARROW_OK_AND_ASSIGN(auto batch, exported->CombineChunksToBatch());

  ASSERT_ARROW_OK_AND_ASSIGN(
      auto prepared,
      client_->Prepare(call_options_, "SELECT typeof(?::VARIANT), ?::VARIANT::VARCHAR"));
  // Bind each exported VARIANT value (its field carries the extension tag).
  const auto expected = ExpectedText();
  for (int64_t row = 0; row < batch->num_rows(); ++row) {
    auto variant_field = batch->schema()->field(1);
    auto params_schema = arrow::schema(
        {variant_field->WithName("p1"), variant_field->WithName("p2")});
    auto value = batch->column(1)->Slice(row, 1);
    ASSERT_ARROW_OK(prepared->SetParameters(
        arrow::RecordBatch::Make(params_schema, 1, {value, value})));
    ASSERT_ARROW_OK_AND_ASSIGN(auto info, prepared->Execute(call_options_));
    ASSERT_ARROW_OK_AND_ASSIGN(auto reader,
                               client_->DoGet(call_options_, info->endpoints()[0].ticket));
    ASSERT_ARROW_OK_AND_ASSIGN(auto result, reader->ToTable());
    ASSERT_EQ(result->num_rows(), 1);
    EXPECT_EQ(Strings(result, 0), std::vector<std::string>{"VARIANT"}) << "row " << row;
    EXPECT_EQ(Strings(result, 1)[0], expected[row]) << "row " << row;
  }
  ASSERT_ARROW_OK(prepared->Close(call_options_));
}

TEST_F(VariantServerFixture, VariantParameterSchemaAdvertisesExtension) {
  ASSERT_ARROW_OK_AND_ASSIGN(auto prepared,
                             client_->Prepare(call_options_, "SELECT ?::VARIANT"));
  ASSERT_EQ(prepared->parameter_schema()->num_fields(), 1);
  ExpectVariantField(*prepared->parameter_schema()->field(0));
  ASSERT_ARROW_OK(prepared->Close(call_options_));
}

#endif  // GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
