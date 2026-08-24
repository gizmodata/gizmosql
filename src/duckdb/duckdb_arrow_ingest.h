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

// Streaming Arrow ingest for DoPut(CommandStatementIngest).
//
// Instead of converting every cell of every incoming RecordBatch into a
// duckdb::Value and pushing it through an Appender, the incoming Flight
// stream is exposed to DuckDB as an `arrow_scan` table function (over the
// Arrow C stream interface) and registered as a session-temporary view.
// The ingest then becomes a plain
//
//   INSERT INTO <target> BY NAME SELECT * FROM <view>
//
// which gives us DuckDB's own, vectorized Arrow->DuckDB conversion for
// every type — including Arrow extension types such as `geoarrow.wkb`,
// which DuckDB (with the spatial extension loaded) materializes directly
// as GEOMETRY. The stream is single-pass: it is consumed exactly once.

#pragma once

#include <duckdb.hpp>
#include <duckdb/common/arrow/arrow_wrapper.hpp>
#include <duckdb/function/table/arrow.hpp>

#include <atomic>
#include <memory>
#include <string>

#include <arrow/api.h>
#include <arrow/flight/server.h>

namespace gizmosql::ddb {

/// True if the field carries an `ARROW:extension:name` of `geoarrow.*`.
bool IsGeoArrowField(const arrow::Field& field);

/// RecordBatchReader over a Flight DoPut message stream. Metadata-only
/// chunks are skipped; rows and bytes are counted (and reported to
/// telemetry) as batches are pulled.
class FlightIngestBatchReader : public arrow::RecordBatchReader {
 public:
  FlightIngestBatchReader(arrow::flight::FlightMessageReader* reader,
                          std::shared_ptr<arrow::Schema> schema);

  std::shared_ptr<arrow::Schema> schema() const override { return schema_; }
  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override;

  int64_t total_rows() const { return total_rows_.load(); }

 private:
  arrow::flight::FlightMessageReader* reader_;
  std::shared_ptr<arrow::Schema> schema_;
  std::atomic<int64_t> total_rows_{0};
};

/// Owns the single-pass stream handed to DuckDB's arrow_scan. Must outlive
/// the view registered by RegisterView() and any statement that scans it.
class ArrowIngestStream {
 public:
  ArrowIngestStream(arrow::flight::FlightMessageReader* reader,
                    std::shared_ptr<arrow::Schema> schema);

  /// Register `view_name` as a temporary view over this stream on `conn`.
  arrow::Status RegisterView(duckdb::Connection& conn, const std::string& view_name);

  int64_t total_rows() const { return batch_reader_->total_rows(); }

 private:
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<FlightIngestBatchReader> batch_reader_;
  std::atomic<bool> produced_{false};

  // arrow_scan factory callbacks (C-style; `factory_ptr` is `this`).
  static duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> Produce(
      uintptr_t factory_ptr, duckdb::ArrowStreamParameters& parameters);
  static void GetSchema(ArrowArrayStream* factory_ptr, ArrowSchema& schema);
};

}  // namespace gizmosql::ddb
