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

#include "duckdb_statement_batch_reader.h"

#include <duckdb.h>

#include <iostream>

#include <arrow/builder.h>
#include <arrow/c/bridge.h>

#include "duckdb_statement.h"
#ifdef GIZMOSQL_ENTERPRISE
#include "enterprise/instrumentation/instrumentation_records.h"
#include "enterprise/metrics/metrics_registry.h"
#endif

namespace gizmosql::ddb {
// Batch size for SQLite statement results
static constexpr int kMaxBatchSize = 1024;

std::shared_ptr<arrow::Schema> DuckDBStatementBatchReader::schema() const {
  return schema_;
}

DuckDBStatementBatchReader::DuckDBStatementBatchReader(
    std::shared_ptr<DuckDBStatement> statement, std::shared_ptr<arrow::Schema> schema)
  : statement_(std::move(statement)),
    schema_(std::move(schema)),
    rc_(DuckDBSuccess),
    already_executed_(false),
    results_read_(false) {
#ifdef GIZMOSQL_ENTERPRISE
  metrics_ = statement_->GetMetricsRegistry();
#endif
}

DuckDBStatementBatchReader::~DuckDBStatementBatchReader() {
#ifdef GIZMOSQL_ENTERPRISE
  if (metrics_sending_)
    metrics_->At("gizmosql_statements_active", {{"wait", "client_send"}})
        .value.fetch_sub(1, std::memory_order_relaxed);
#endif
}

arrow::Result<std::shared_ptr<DuckDBStatementBatchReader>>
DuckDBStatementBatchReader::Create(const std::shared_ptr<DuckDBStatement>& statement_) {
  ARROW_ASSIGN_OR_RAISE(auto schema, statement_->GetSchema());

  bool already_executed = false;
  if (statement_->HasUnresolvedSchema()) {
    // Untyped placeholders: the stream schema is only known once the bound
    // parameters have been applied, so execute up front and take the schema
    // from the result rather than the prepare-time placeholder.
    ARROW_RETURN_NOT_OK(statement_->Execute());
    already_executed = true;
    ARROW_ASSIGN_OR_RAISE(schema, statement_->GetSchema());
  }

  std::shared_ptr<DuckDBStatementBatchReader> result(
      new DuckDBStatementBatchReader(statement_, schema));
  result->already_executed_ = already_executed;

  return result;
}

arrow::Result<std::shared_ptr<DuckDBStatementBatchReader>>
DuckDBStatementBatchReader::Create(const std::shared_ptr<DuckDBStatement>& statement,
                                   const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<DuckDBStatementBatchReader> result(
      new DuckDBStatementBatchReader(statement, schema));

  return result;
}

arrow::Status DuckDBStatementBatchReader::ReadNext(
    std::shared_ptr<arrow::RecordBatch>* out) {
  if (!already_executed_) {
    ARROW_RETURN_NOT_OK(statement_->Execute());
    already_executed_ = true;
  }

  ARROW_ASSIGN_OR_RAISE(*out, statement_->FetchResult());

#ifdef GIZMOSQL_ENTERPRISE
  // Retained materialized results awaiting further client downloads. Count the
  // stream until EOF or destruction, including cancellation/abandonment.
  if (metrics_) {
    const bool sending = static_cast<bool>(*out);
    if (sending != metrics_sending_) {
      metrics_->At("gizmosql_statements_active", {{"wait", "client_send"}})
          .value.fetch_add(sending ? 1 : -1, std::memory_order_relaxed);
      metrics_sending_ = sending;
    }
  }
  // Track rows fetched for instrumentation
  if (*out && statement_->GetExecutionInstrumentation()) {
    statement_->GetExecutionInstrumentation()->IncrementRowsFetched((*out)->num_rows());
  }
#endif

  return arrow::Status::OK();
}
}  // namespace gizmosql::ddb
