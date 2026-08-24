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

#include "duckdb_arrow_ingest.h"

#include <duckdb/common/arrow/arrow_wrapper.hpp>
#include <duckdb/function/table/arrow.hpp>

#include <arrow/c/bridge.h>

#include "gizmosql_telemetry.h"

namespace gizmosql::ddb {

bool IsGeoArrowField(const arrow::Field& field) {
  const auto& metadata = field.metadata();
  if (!metadata) return false;
  const int idx = metadata->FindKey("ARROW:extension:name");
  return idx >= 0 && metadata->value(idx).rfind("geoarrow.", 0) == 0;
}

namespace {

int64_t RecordBatchSizeBytes(const std::shared_ptr<arrow::RecordBatch>& batch) {
  int64_t total = 0;
  std::function<void(const std::shared_ptr<arrow::ArrayData>&)> visit =
      [&](const std::shared_ptr<arrow::ArrayData>& data) {
        if (!data) return;
        for (const auto& buffer : data->buffers) {
          if (buffer) total += buffer->size();
        }
        for (const auto& child : data->child_data) visit(child);
        if (data->dictionary) visit(data->dictionary);
      };
  for (int i = 0; i < batch->num_columns(); ++i) visit(batch->column(i)->data());
  return total;
}

}  // namespace

FlightIngestBatchReader::FlightIngestBatchReader(arrow::flight::FlightMessageReader* reader,
                                                 std::shared_ptr<arrow::Schema> schema)
    : reader_(reader), schema_(std::move(schema)) {}

arrow::Status FlightIngestBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(auto chunk, reader_->Next());
    if (!chunk.data && !chunk.app_metadata) {
      *batch = nullptr;  // end of stream
      return arrow::Status::OK();
    }
    if (!chunk.data) continue;  // metadata-only chunk
    const int64_t rows = chunk.data->num_rows();
    total_rows_ += rows;
    if (::gizmosql::IsTelemetryEnabled()) {
      ::gizmosql::metrics::RecordRowsTransferred("inbound", rows);
      ::gizmosql::metrics::RecordBytesTransferred("inbound",
                                                  RecordBatchSizeBytes(chunk.data));
    }
    *batch = std::move(chunk.data);
    return arrow::Status::OK();
  }
}

ArrowIngestStream::ArrowIngestStream(arrow::flight::FlightMessageReader* reader,
                                     std::shared_ptr<arrow::Schema> schema)
    : schema_(std::move(schema)),
      batch_reader_(std::make_shared<FlightIngestBatchReader>(reader, schema_)) {}

duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ArrowIngestStream::Produce(
    uintptr_t factory_ptr, duckdb::ArrowStreamParameters& /*parameters*/) {
  auto* self = reinterpret_cast<ArrowIngestStream*>(factory_ptr);
  if (self->produced_.exchange(true)) {
    throw duckdb::InvalidInputException(
        "GizmoSQL ingest stream can only be scanned once");
  }
  auto wrapper = duckdb::make_uniq<duckdb::ArrowArrayStreamWrapper>();
  auto status = arrow::ExportRecordBatchReader(self->batch_reader_,
                                               &wrapper->arrow_array_stream);
  if (!status.ok()) {
    throw duckdb::IOException("Failed to export ingest stream: " + status.ToString());
  }
  wrapper->number_of_rows = -1;
  return wrapper;
}

void ArrowIngestStream::GetSchema(ArrowArrayStream* factory_ptr, ArrowSchema& schema) {
  auto* self = reinterpret_cast<ArrowIngestStream*>(factory_ptr);
  auto status = arrow::ExportSchema(*self->schema_, &schema);
  if (!status.ok()) {
    throw duckdb::IOException("Failed to export ingest schema: " + status.ToString());
  }
}

arrow::Status ArrowIngestStream::RegisterView(duckdb::Connection& conn,
                                              const std::string& view_name) {
  try {
    duckdb::vector<duckdb::Value> params;
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(this)));
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&Produce)));
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&GetSchema)));
    auto relation = conn.TableFunction("arrow_scan", params);
    relation->CreateView(view_name, /*replace=*/true, /*temporary=*/true);
  } catch (const std::exception& e) {
    return arrow::Status::Invalid(std::string("Failed to register ingest stream: ") +
                                  e.what());
  }
  return arrow::Status::OK();
}

}  // namespace gizmosql::ddb
