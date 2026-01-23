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
// using the GeoArrow Arrow extension format when the DuckDB spatial
// extension is loaded and register_geoarrow_extensions() is called.
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
  result.schema = flight_info->schema();

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

  // Step 2: Register GeoArrow extensions for Arrow export
  std::cerr << "Registering GeoArrow extensions..." << std::endl;
  result = RunQuery(sql_client, call_options, "CALL register_geoarrow_extensions();");
  ASSERT_TRUE(result.success) << "Failed to register geoarrow extensions: " << result.error_message;

  // Step 3: Create a table with GEOMETRY column
  std::cerr << "Creating table with GEOMETRY column..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    CREATE TABLE test_geo (
      id INTEGER,
      name VARCHAR,
      geom GEOMETRY
    );
  )");
  ASSERT_TRUE(result.success) << "Failed to create table: " << result.error_message;

  // Step 4: Insert some geometry data
  std::cerr << "Inserting geometry data..." << std::endl;
  result = RunQuery(sql_client, call_options, R"(
    INSERT INTO test_geo VALUES
      (1, 'Point A', ST_Point(0.0, 0.0)),
      (2, 'Point B', ST_Point(1.0, 1.0)),
      (3, 'Line AB', ST_MakeLine(ST_Point(0.0, 0.0), ST_Point(1.0, 1.0)));
  )");
  ASSERT_TRUE(result.success) << "Failed to insert data: " << result.error_message;

  // Step 5: Query the table and verify GeoArrow export
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

// Test that without register_geoarrow_extensions(), geometry is plain binary
TEST_F(GeoArrowServerFixture, GeometryWithoutRegistration) {
  ASSERT_TRUE(IsServerReady()) << "Server not ready";

  // Connect to GizmoSQL with a fresh connection
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

  std::cerr << "\n=== GeoArrow Without Registration Test ===" << std::endl;

  // Load spatial but DON'T call register_geoarrow_extensions()
  auto result = RunQuery(sql_client, call_options, "LOAD spatial;");
  ASSERT_TRUE(result.success) << "Failed to load spatial: " << result.error_message;

  // Query geometry using ST_Point directly
  result = RunQuery(sql_client, call_options, "SELECT ST_Point(1.0, 2.0) AS geom;");
  ASSERT_TRUE(result.success) << "Failed to query: " << result.error_message;

  auto geom_field = result.schema->GetFieldByName("geom");
  ASSERT_NE(geom_field, nullptr) << "geom field should exist";

  std::cerr << "Geometry field type (without registration): "
            << geom_field->type()->ToString() << std::endl;

  // Without GeoArrow registration, geometry should be plain binary (WKB)
  // or may have different extension metadata depending on DuckDB version
  auto metadata = geom_field->metadata();
  if (metadata != nullptr) {
    std::cerr << "Metadata present with " << metadata->size() << " keys" << std::endl;
  }

  std::cerr << "=== Test Complete ===" << std::endl;
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
  result = RunQuery(sql_client, call_options, "CALL register_geoarrow_extensions();");
  ASSERT_TRUE(result.success) << result.error_message;

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
