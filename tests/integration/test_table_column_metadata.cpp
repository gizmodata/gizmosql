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

// Per-column metadata in Flight SQL GetTables(include_schema=true)
//
// JDBC's DatabaseMetaData.getColumns() (DBeaver, ...) and ADBC read NOT NULL,
// comments, defaults, auto-increment and type names from the per-table schema.
// GizmoSQL reads them from the table's (or view's) own catalog entry — it used
// to query duckdb_columns(), which enumerates every attached catalog for each
// table. These tests pin the values clients see, including for a same-named
// table in a second attached catalog.

#include <gtest/gtest.h>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/io/memory.h"
#include "arrow/ipc/api.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

class TableColumnMetadataFixture
    : public gizmosql::testing::ServerTestFixture<TableColumnMetadataFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31676,
        .health_port = 31677,
        .username = "tester",
        .password = "tester_password",
    };
  }

 protected:
  void SetUp() override {
    ASSERT_TRUE(IsServerReady()) << "Server not ready";
    ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                               arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
    ASSERT_ARROW_OK_AND_ASSIGN(auto flight, arrow::flight::FlightClient::Connect(location));
    ASSERT_ARROW_OK_AND_ASSIGN(
        auto bearer, flight->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
    call_options_.headers.push_back(bearer);
    client_ = std::make_unique<FlightSqlClient>(std::move(flight));
  }

  arrow::Status Run(const std::string& sql) {
    ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));
    for (const auto& endpoint : info->endpoints()) {
      ARROW_ASSIGN_OR_RAISE(auto reader, client_->DoGet(call_options_, endpoint.ticket));
      ARROW_RETURN_NOT_OK(reader->ToTable().status());
    }
    return arrow::Status::OK();
  }

  // The schema GetTables(include_schema=true) reports for catalog.main.table.
  arrow::Result<std::shared_ptr<arrow::Schema>> TableSchema(const std::string& catalog,
                                                            const std::string& table) {
    const std::string schema = "main";
    ARROW_ASSIGN_OR_RAISE(auto info, client_->GetTables(call_options_, &catalog, &schema,
                                                        &table, /*include_schema=*/true,
                                                        nullptr));
    ARROW_ASSIGN_OR_RAISE(auto reader,
                          client_->DoGet(call_options_, info->endpoints()[0].ticket));
    ARROW_ASSIGN_OR_RAISE(auto result, reader->ToTable());
    if (result->num_rows() != 1) {
      return arrow::Status::Invalid("expected 1 table row, got ", result->num_rows());
    }
    ARROW_ASSIGN_OR_RAISE(auto scalar,
                          result->GetColumnByName("table_schema")->GetScalar(0));
    const auto& bytes = static_cast<const arrow::BinaryScalar&>(*scalar).value;
    arrow::io::BufferReader buffer_reader(bytes);
    arrow::ipc::DictionaryMemo memo;
    return arrow::ipc::ReadSchema(&buffer_reader, &memo);
  }

  // Metadata value of `key` on `field`, or "<absent>".
  static std::string Meta(const arrow::Schema& schema, const std::string& field,
                          const std::string& key) {
    auto f = schema.GetFieldByName(field);
    if (!f) return "<no field " + field + ">";
    if (!f->metadata()) return "<absent>";
    auto value = f->metadata()->Get(key);
    return value.ok() ? *value : "<absent>";
  }

  static bool Nullable(const arrow::Schema& schema, const std::string& field) {
    return schema.GetFieldByName(field)->nullable();
  }

  arrow::flight::FlightCallOptions call_options_;
  std::unique_ptr<FlightSqlClient> client_;
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<TableColumnMetadataFixture>::server_{};
template <>
std::thread gizmosql::testing::ServerTestFixture<TableColumnMetadataFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<TableColumnMetadataFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<TableColumnMetadataFixture>::config_{};

TEST_F(TableColumnMetadataFixture, TableColumnsCarryConstraintsCommentsAndDefaults) {
  ASSERT_ARROW_OK(Run("CREATE OR REPLACE SEQUENCE colmeta_seq START 1"));
  ASSERT_ARROW_OK(Run(
      "CREATE OR REPLACE TABLE colmeta ("
      "  id INT PRIMARY KEY DEFAULT nextval('colmeta_seq'),"
      "  name VARCHAR NOT NULL,"
      "  salary DECIMAL(10,2) DEFAULT 50000.00,"
      "  note VARCHAR)"));
  ASSERT_ARROW_OK(Run("COMMENT ON COLUMN colmeta.name IS 'employee name'"));

  ASSERT_ARROW_OK_AND_ASSIGN(auto schema, TableSchema("memory", "colmeta"));
  // NOT NULL (a PRIMARY KEY implies it)
  EXPECT_FALSE(Nullable(*schema, "id"));
  EXPECT_FALSE(Nullable(*schema, "name"));
  EXPECT_TRUE(Nullable(*schema, "salary"));
  EXPECT_TRUE(Nullable(*schema, "note"));
  // Comments: only where set.
  EXPECT_EQ(Meta(*schema, "name", "ARROW:FLIGHT:SQL:REMARKS"), "employee name");
  EXPECT_EQ(Meta(*schema, "note", "ARROW:FLIGHT:SQL:REMARKS"), "<absent>");
  // Defaults and the nextval() auto-increment idiom.
  EXPECT_EQ(Meta(*schema, "id", "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"), "1");
  EXPECT_EQ(Meta(*schema, "name", "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"), "0");
  EXPECT_NE(Meta(*schema, "id", "GIZMOSQL:COLUMN_DEFAULT").find("nextval"), std::string::npos);
  EXPECT_NE(Meta(*schema, "salary", "GIZMOSQL:COLUMN_DEFAULT").find("50000"),
            std::string::npos);
  EXPECT_EQ(Meta(*schema, "note", "GIZMOSQL:COLUMN_DEFAULT"), "<absent>");
  // Base type names (no parameters).
  EXPECT_EQ(Meta(*schema, "salary", "ARROW:FLIGHT:SQL:TYPE_NAME"), "DECIMAL");
  EXPECT_EQ(Meta(*schema, "id", "ARROW:FLIGHT:SQL:TYPE_NAME"), "INTEGER");
}

TEST_F(TableColumnMetadataFixture, ViewColumnsCarryCommentsAndTypes) {
  ASSERT_ARROW_OK(Run("CREATE OR REPLACE TABLE colmeta_base (id INT NOT NULL, name VARCHAR)"));
  ASSERT_ARROW_OK(
      Run("CREATE OR REPLACE VIEW colmeta_view AS SELECT id, name AS employee FROM colmeta_base"));
  ASSERT_ARROW_OK(Run("COMMENT ON COLUMN colmeta_view.employee IS 'who'"));

  ASSERT_ARROW_OK_AND_ASSIGN(auto schema, TableSchema("memory", "colmeta_view"));
  EXPECT_EQ(Meta(*schema, "employee", "ARROW:FLIGHT:SQL:REMARKS"), "who");
  EXPECT_EQ(Meta(*schema, "employee", "ARROW:FLIGHT:SQL:TYPE_NAME"), "VARCHAR");
  EXPECT_EQ(Meta(*schema, "id", "ARROW:FLIGHT:SQL:TYPE_NAME"), "INTEGER");
  // Views carry no constraints: every column is reported nullable.
  EXPECT_TRUE(Nullable(*schema, "id"));
}

TEST_F(TableColumnMetadataFixture, MetadataComesFromTheRequestedCatalog) {
  ASSERT_ARROW_OK(Run("CREATE OR REPLACE TABLE twin (x INT, label VARCHAR)"));
  ASSERT_ARROW_OK(Run("ATTACH IF NOT EXISTS ':memory:' AS other_catalog"));
  ASSERT_ARROW_OK(
      Run("CREATE OR REPLACE TABLE other_catalog.main.twin (x INT NOT NULL, label VARCHAR)"));
  ASSERT_ARROW_OK(Run("COMMENT ON COLUMN other_catalog.main.twin.label IS 'other label'"));

  ASSERT_ARROW_OK_AND_ASSIGN(auto other, TableSchema("other_catalog", "twin"));
  EXPECT_FALSE(Nullable(*other, "x"));
  EXPECT_EQ(Meta(*other, "label", "ARROW:FLIGHT:SQL:REMARKS"), "other label");

  ASSERT_ARROW_OK_AND_ASSIGN(auto local, TableSchema("memory", "twin"));
  EXPECT_TRUE(Nullable(*local, "x"));
  EXPECT_EQ(Meta(*local, "label", "ARROW:FLIGHT:SQL:REMARKS"), "<absent>");
}
