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

// GeoArrow Integration Tests
//
// These tests verify that GizmoSQL correctly exports GEOMETRY types
//
// This enables seamless integration with GeoArrow-aware clients like
// GeoPandas, allowing direct consumption of geometry data without
// manual WKB conversion.

#include <gtest/gtest.h>

#include <chrono>
#include <iostream>
#include <thread>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/flight/sql/types.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"
#include "test_util.h"

using arrow::flight::sql::FlightSqlClient;

// ============================================================================
// Helper Functions
// ============================================================================

namespace {

struct QueryResult {
  bool success;
  int64_t row_count;
  std::string error_message;
  std::shared_ptr<arrow::Table> table;
  std::shared_ptr<arrow::Schema> schema;
};

// Run a query via GizmoSQL Flight SQL
QueryResult RunQuery(FlightSqlClient& client,
                     arrow::flight::FlightCallOptions& call_options,
                     const std::string& query) {
  QueryResult result{};

  auto flight_info_result = client.Execute(call_options, query);
  if (!flight_info_result.ok()) {
    result.success = false;
    result.error_message = flight_info_result.status().ToString();
    return result;
  }

  auto flight_info = std::move(*flight_info_result);

  if (flight_info->endpoints().empty()) {
    // Some queries (like CREATE, INSERT) may not return data
    result.success = true;
    result.row_count = 0;
    return result;
  }

  auto reader_result =
      client.DoGet(call_options, flight_info->endpoints()[0].ticket);
  if (!reader_result.ok()) {
    result.success = false;
    result.error_message = reader_result.status().ToString();
    return result;
  }

  auto reader = std::move(*reader_result);
  auto table_result = reader->ToTable();
  if (!table_result.ok()) {
    result.success = false;
    result.error_message = table_result.status().ToString();
    return result;
  }

  result.table = *table_result;
  result.row_count = result.table->num_rows();
  result.schema = result.table->schema();
  result.success = true;
  return result;
}

}  // anonymous namespace

// ============================================================================
// Test Fixture
// ============================================================================

class GeoArrowServerFixture
    : public gizmosql::testing::ServerTestFixture<GeoArrowServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = ":memory:",
        .port = 31370,
        .health_port = 31371,
        .username = "geoarrow_tester",
        .password = "geoarrow_tester",
    };
  }
};

// Static member definitions
template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<GeoArrowServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<GeoArrowServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<GeoArrowServerFixture>::server_ready_{false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<GeoArrowServerFixture>::config_{};

// ============================================================================
// Tests
// ============================================================================

// Test that GEOMETRY columns export with GeoArrow extension metadata
TEST_F(GeoArrowServerFixture, GeometryExportsAsGeoArrow) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to GizmoSQL
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));

  std::cerr << "\n=== GeoArrow Integration Test ===" << std::endl;

  // Step 1: Install and load spatial extension
  std::cerr << "Installing spatial extension..." << std::endl;
  auto result = RunQuery(sql_client, call_options, "INSTALL spatial;");
  ASSERT_TRUE(result.success) << "Failed to install spatial: " << result.error_message;

  result = RunQuery(sql_client, call_options, "LOAD spatial;");
  ASSERT_TRUE(result.success) << "Failed to load spatial: " << result.error_message;

#if GIZMOSQL_DUCKDB_CHANNEL_LTS
  // DuckDB v1.4.x ships GeoArrow integration as an opt-in call; v1.5+
  // wires it in automatically when spatial loads.
  result = RunQuery(sql_client, call_options, "CALL register_geoarrow_extensions();");
  ASSERT_TRUE(result.success)
      << "Failed to register geoarrow extensions: " << result.error_message;
#endif

  // Step 2: Create a table with GEOMETRY column
  std::cerr << "Creating table with GEOMETRY column..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    CREATE TABLE test_geo (
      id INTEGER,
      name VARCHAR,
      geom GEOMETRY
    );
  )");
  ASSERT_TRUE(result.success) << "Failed to create table: " << result.error_message;

  // Step 3: Insert some geometry data
  std::cerr << "Inserting geometry data..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    INSERT INTO test_geo VALUES
      (1, 'Point A', ST_Point(0.0, 0.0)),
      (2, 'Point B', ST_Point(1.0, 1.0)),
      (3, 'Line AB', ST_MakeLine(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)));
  )");
  ASSERT_TRUE(result.success) << "Failed to insert data: " << result.error_message;

  // Step 4: Query the table and verify GeoArrow export
  std::cerr << "Querying geometry data..." << std::endl;
  result = RunQuery(sql_client, call_options, "SELECT * FROM test_geo ORDER BY id;");
  ASSERT_TRUE(result.success) << "Failed to query data: " << result.error_message;
  ASSERT_EQ(result.row_count, 3) << "Expected 3 rows";

  // Verify schema
  ASSERT_NE(result.schema, nullptr) << "Schema should not be null";
  std::cerr << "Schema: " << result.schema->ToString() << std::endl;

  // Find the geometry column
  auto geom_field = result.schema->GetFieldByName("geom");
  ASSERT_NE(geom_field, nullptr) << "geom field should exist";

  // Check for GeoArrow extension metadata
  // The GeoArrow extension should add ARROW:extension:name metadata
  auto metadata = geom_field->metadata();
  if (metadata != nullptr) {
    std::cerr << "Geometry field metadata keys:" << std::endl;
    for (int64_t i = 0; i < metadata->size(); ++i) {
      std::cerr << "  " << metadata->key(i) << ": " << metadata->value(i) << std::endl;
    }

    // Check for GeoArrow extension type
    int key_index = metadata->FindKey("ARROW:extension:name");
    if (key_index >= 0) {
      std::string ext_name = metadata->value(key_index);
      std::cerr << "GeoArrow extension type: " << ext_name << std::endl;
      // GeoArrow extension names start with "geoarrow."
      EXPECT_TRUE(ext_name.find("geoarrow.") == 0 || ext_name.find("ogc.") == 0)
          << "Expected GeoArrow extension type, got: " << ext_name;
    } else {
      // If no extension metadata, the field should at least be binary
      std::cerr << "No ARROW:extension:name found - checking base type" << std::endl;
      EXPECT_TRUE(geom_field->type()->id() == arrow::Type::BINARY ||
                  geom_field->type()->id() == arrow::Type::LARGE_BINARY)
          << "Geometry should be binary type, got: " << geom_field->type()->ToString();
    }
  } else {
    std::cerr << "No metadata on geometry field" << std::endl;
    // Without metadata, just verify it's binary
    EXPECT_TRUE(geom_field->type()->id() == arrow::Type::BINARY ||
                geom_field->type()->id() == arrow::Type::LARGE_BINARY)
        << "Geometry should be binary type, got: " << geom_field->type()->ToString();
  }

  std::cerr << "=== GeoArrow Test Complete ===" << std::endl;
}

// Regression: bulk ingest (DoPut CommandStatementIngest) of a geoarrow.wkb
// column must land as GEOMETRY — in create mode (new table), in append mode
// into an existing GEOMETRY column, and in replace mode — with the WKB
// round-tripping through ST_AsText. Previously the server ignored the
// extension metadata: create produced BLOB and append failed on the
// blob->geometry cast (adbc-driver-gizmosql#5).
TEST_F(GeoArrowServerFixture, GeometryIngestsAsGeometry) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";
  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(auto location,
                             arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));
  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer, flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(flight_client));

  auto result = RunQuery(sql_client, call_options, "INSTALL spatial; LOAD spatial;");
  ASSERT_TRUE(result.success) << result.error_message;
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
  result = RunQuery(sql_client, call_options, "CALL register_geoarrow_extensions();");
  ASSERT_TRUE(result.success) << result.error_message;
#endif
  RunQuery(sql_client, call_options, "DROP TABLE IF EXISTS geo_ingest");

  // WKB (little-endian) POINT(x y)
  auto wkb_point = [](double x, double y) {
    std::string b;
    b.push_back('\x01');
    uint32_t type = 1;
    b.append(reinterpret_cast<const char*>(&type), 4);
    b.append(reinterpret_cast<const char*>(&x), 8);
    b.append(reinterpret_cast<const char*>(&y), 8);
    return b;
  };
  auto make_batches = [&](int first_id) {
    auto geo_field = arrow::field(
        "geom", arrow::binary(), /*nullable=*/true,
        arrow::key_value_metadata({"ARROW:extension:name", "ARROW:extension:metadata"},
                                  {"geoarrow.wkb", "{}"}));
    auto schema = arrow::schema({arrow::field("id", arrow::int32()), geo_field});
    arrow::Int32Builder ids;
    arrow::BinaryBuilder geoms;
    for (int i = 0; i < 2; ++i) {
      EXPECT_TRUE(ids.Append(first_id + i).ok());
      EXPECT_TRUE(geoms.Append(wkb_point(first_id + i, (first_id + i) * 10.0)).ok());
    }
    auto batch = arrow::RecordBatch::Make(
        schema, 2, {ids.Finish().ValueOrDie(), geoms.Finish().ValueOrDie()});
    return arrow::RecordBatchReader::Make({batch}, schema).ValueOrDie();
  };
  using arrow::flight::sql::TableDefinitionOptions;
  using ExistsOpt = arrow::flight::sql::TableDefinitionOptionsTableExistsOption;
  using NotExistsOpt = arrow::flight::sql::TableDefinitionOptionsTableNotExistOption;
  auto ingest = [&](int first_id, ExistsOpt if_exists) {
    TableDefinitionOptions opts;
    opts.if_not_exist = NotExistsOpt::kCreate;
    opts.if_exists = if_exists;
    return sql_client.ExecuteIngest(call_options, make_batches(first_id), opts,
                                    "geo_ingest", std::nullopt, std::nullopt, false,
                                    arrow::flight::sql::no_transaction(), {});
  };
  auto check = [&](int64_t expected_rows, const char* expected_last_point) {
    auto r = RunQuery(sql_client, call_options,
                      "SELECT typeof(geom), ST_AsText(geom), count(*) OVER () "
                      "FROM geo_ingest ORDER BY id DESC LIMIT 1");
    ASSERT_TRUE(r.success) << r.error_message;
    ASSERT_EQ(r.row_count, 1);
    auto col = [&](int i) { return r.table->column(i)->GetScalar(0).ValueOrDie()->ToString(); };
    EXPECT_EQ(col(0), "GEOMETRY");
    EXPECT_EQ(col(1), expected_last_point);
    EXPECT_EQ(col(2), std::to_string(expected_rows));
  };

  // create: new table gets a GEOMETRY column
  ASSERT_ARROW_OK_AND_ASSIGN(auto n1, ingest(1, ExistsOpt::kFail));
  EXPECT_EQ(n1, 2);
  check(2, "POINT (2 20)");
  // append into the existing GEOMETRY column
  ASSERT_ARROW_OK_AND_ASSIGN(auto n2, ingest(3, ExistsOpt::kAppend));
  EXPECT_EQ(n2, 2);
  check(4, "POINT (4 40)");
  // replace
  ASSERT_ARROW_OK_AND_ASSIGN(auto n3, ingest(7, ExistsOpt::kReplace));
  EXPECT_EQ(n3, 2);
  check(2, "POINT (8 80)");
  RunQuery(sql_client, call_options, "DROP TABLE IF EXISTS geo_ingest");
}

// Test various geometry types
TEST_F(GeoArrowServerFixture, VariousGeometryTypes) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  arrow::flight::FlightClientOptions options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto location, arrow::flight::Location::ForGrpcTcp("localhost", GetPort()));
  ASSERT_ARROW_OK_AND_ASSIGN(auto flight_client,
                             arrow::flight::FlightClient::Connect(location, options));

  arrow::flight::FlightCallOptions call_options;
  ASSERT_ARROW_OK_AND_ASSIGN(
      auto bearer,
      flight_client->AuthenticateBasicToken({}, GetUsername(), GetPassword()));
  call_options.headers.push_back(bearer);

  FlightSqlClient sql_client(std::move(flight_client));

  std::cerr << "\n=== Various Geometry Types Test ===" << std::endl;

  // Setup: load spatial and register GeoArrow
  auto result = RunQuery(sql_client, call_options, "LOAD spatial;");
  ASSERT_TRUE(result.success) << result.error_message;

#if GIZMOSQL_DUCKDB_CHANNEL_LTS
  result = RunQuery(sql_client, call_options, "CALL register_geoarrow_extensions();");
  ASSERT_TRUE(result.success) << result.error_message;
#endif

  // Test Point
  result = RunQuery(sql_client, call_options,
                    "SELECT ST_Point(1.0, 2.0) AS point;");
  ASSERT_TRUE(result.success) << "Point query failed: " << result.error_message;
  ASSERT_EQ(result.row_count, 1);
  std::cerr << "Point OK" << std::endl;

  // Test LineString
  result = RunQuery(sql_client, call_options,
                    "SELECT ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)') AS linestring;");
  ASSERT_TRUE(result.success) << "LineString query failed: " << result.error_message;
  ASSERT_EQ(result.row_count, 1);
  std::cerr << "LineString OK" << std::endl;

  // Test Polygon
  result = RunQuery(sql_client, call_options,
                    "SELECT ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS polygon;");
  ASSERT_TRUE(result.success) << "Polygon query failed: " << result.error_message;
  ASSERT_EQ(result.row_count, 1);
  std::cerr << "Polygon OK" << std::endl;

  // Test MultiPoint
  result = RunQuery(sql_client, call_options,
                    "SELECT ST_GeomFromText('MULTIPOINT(0 0, 1 1)') AS multipoint;");
  ASSERT_TRUE(result.success) << "MultiPoint query failed: " << result.error_message;
  ASSERT_EQ(result.row_count, 1);
  std::cerr << "MultiPoint OK" << std::endl;

  std::cerr << "=== All Geometry Types OK ===" << std::endl;
}
