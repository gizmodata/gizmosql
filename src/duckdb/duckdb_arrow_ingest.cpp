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
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
#include <duckdb/common/types/variant/parquet_variant_iterator.hpp>
#endif

#include <arrow/c/bridge.h>

#include "gizmosql_telemetry.h"

namespace gizmosql::ddb {

bool IsGeoArrowField(const arrow::Field& field) {
  const auto& metadata = field.metadata();
  if (!metadata) return false;
  const int idx = metadata->FindKey("ARROW:extension:name");
  return idx >= 0 && metadata->value(idx).rfind("geoarrow.", 0) == 0;
}

bool IsArrowVariantField(const arrow::Field& field) {
  static const std::string kVariant = "arrow.parquet.variant";
  if (field.type()->id() == arrow::Type::EXTENSION) {
    return static_cast<const arrow::ExtensionType&>(*field.type()).extension_name() ==
           kVariant;
  }
  const auto& metadata = field.metadata();
  if (!metadata) return false;
  const int idx = metadata->FindKey("ARROW:extension:name");
  return idx >= 0 && metadata->value(idx) == kVariant;
}

arrow::Result<duckdb::Value> ArrowVariantScalarToDuckDBValue(const arrow::Scalar& scalar) {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
  const arrow::Scalar* storage = &scalar;
  if (storage->type->id() == arrow::Type::EXTENSION) {
    storage = static_cast<const arrow::ExtensionScalar&>(scalar).value.get();
  }
  if (storage == nullptr || !storage->is_valid) {
    return duckdb::Value(duckdb::LogicalType::VARIANT());
  }
  if (storage->type->id() != arrow::Type::STRUCT) {
    return arrow::Status::Invalid("arrow.parquet.variant value must be a struct, got ",
                                  storage->type->ToString());
  }
  const auto& parts = static_cast<const arrow::StructScalar&>(*storage);
  auto bytes_of = [&](const char* name) -> arrow::Result<std::string_view> {
    ARROW_ASSIGN_OR_RAISE(auto part, parts.field(name));
    const auto id = part->type->id();
    if (id != arrow::Type::BINARY && id != arrow::Type::LARGE_BINARY &&
        id != arrow::Type::BINARY_VIEW) {
      return arrow::Status::Invalid("arrow.parquet.variant field '", name,
                                    "' must be binary, got ", part->type->ToString());
    }
    if (!part->is_valid) {
      return arrow::Status::Invalid("arrow.parquet.variant field '", name, "' is null");
    }
    return static_cast<const arrow::BaseBinaryScalar&>(*part).view();
  };
  ARROW_ASSIGN_OR_RAISE(auto metadata, bytes_of("metadata"));
  ARROW_ASSIGN_OR_RAISE(auto value, bytes_of("value"));

  // The Variant binary form is self-delimiting: metadata followed by value,
  // which is what DuckDB's Parquet Variant decode reads.
  std::string encoded;
  encoded.reserve(metadata.size() + value.size());
  encoded.append(metadata).append(value);
  try {
    duckdb::Vector input(duckdb::LogicalType::BLOB, 1);
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(input)[0] =
        duckdb::StringVector::AddStringOrBlob(input, encoded.data(), encoded.size());
    duckdb::Vector result(duckdb::LogicalType::VARIANT(), 1);
    duckdb::ParquetVariantConversion::ConvertBinary(input, result, 1);
    return result.GetValue(0);
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Invalid arrow.parquet.variant value: ", e.what());
  }
#else
  (void)scalar;
  return arrow::Status::NotImplemented(
      "VARIANT values over Arrow require GizmoSQL built with DuckDB 2.0 or later");
#endif
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
#if GIZMOSQL_DUCKDB_MAJOR_VERSION < 2
  wrapper->number_of_rows = -1;  // 2.x dropped the row-count hint
#endif
  return wrapper;
}

void ArrowIngestStream::GetSchema(ArrowArrayStream* factory_ptr, ArrowSchema& schema) {
  auto* self = reinterpret_cast<ArrowIngestStream*>(factory_ptr);
  auto status = arrow::ExportSchema(*self->schema_, &schema);
  if (!status.ok()) {
    throw duckdb::IOException("Failed to export ingest schema: " + status.ToString());
  }
}

#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
namespace {
// DuckDB 2.x's arrow_scan takes its stream from an ArrowScanFactory bind input
// instead of three POINTER arguments (which it now rejects at bind time).
class IngestScanFactory : public duckdb::ArrowScanFactory {
 public:
  explicit IngestScanFactory(uintptr_t stream) : stream_(stream) {}

  void GetSchema(ArrowSchema& schema) override {
    ArrowIngestStream::GetSchema(reinterpret_cast<ArrowArrayStream*>(stream_), schema);
  }

  duckdb::unique_ptr<duckdb::ArrowArrayStreamWrapper> ProduceStream(
      duckdb::ArrowStreamParameters& parameters) override {
    return ArrowIngestStream::Produce(stream_, parameters);
  }

 private:
  uintptr_t stream_;
};
}  // namespace
#endif

arrow::Status ArrowIngestStream::RegisterView(duckdb::Connection& conn,
                                              const std::string& view_name) {
  try {
#if GIZMOSQL_DUCKDB_MAJOR_VERSION >= 2
    auto factory =
        duckdb::make_shared_ptr<IngestScanFactory>(reinterpret_cast<uintptr_t>(this));
    auto relation = conn.TableFunction("arrow_scan", {}, {}, std::move(factory));
    relation->CreateView(duckdb::Identifier(view_name), /*replace=*/true, /*temporary=*/true);
    return arrow::Status::OK();
#else
    duckdb::vector<duckdb::Value> params;
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(this)));
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&Produce)));
    params.emplace_back(duckdb::Value::POINTER(reinterpret_cast<uintptr_t>(&GetSchema)));
    auto relation = conn.TableFunction("arrow_scan", params);
    relation->CreateView(view_name, /*replace=*/true, /*temporary=*/true);
#endif
  } catch (const std::exception& e) {
    return arrow::Status::Invalid(std::string("Failed to register ingest stream: ") +
                                  e.what());
  }
  return arrow::Status::OK();
}

}  // namespace gizmosql::ddb
