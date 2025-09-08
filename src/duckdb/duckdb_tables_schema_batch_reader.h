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

#pragma once

#include <duckdb.h>

#include <memory>
#include <string>

#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"
#include <arrow/record_batch.h>

namespace gizmosql::ddb {
class DuckDBTablesWithSchemaBatchReader : public arrow::RecordBatchReader {
private:
  std::shared_ptr<DuckDBStatementBatchReader> reader_;
  std::string main_query_;
  std::shared_ptr<ClientSession> client_session_;
  bool already_executed_;

public:
  /// Constructor for DuckDBTablesWithSchemaBatchReader class
/// \param reader an shared_ptr from a DuckDBStatementBatchReader.
/// \param main_query  SQL query that originated reader's data.
/// \param db     a pointer to the sqlite3 db.
  DuckDBTablesWithSchemaBatchReader(std::shared_ptr<DuckDBStatementBatchReader> reader,
                                    std::string main_query,
                                    std::shared_ptr<ClientSession> client_session)
    : reader_(std::move(reader)),
      main_query_(std::move(main_query)),
      client_session_(client_session),
      already_executed_(false) {
  }

  std::shared_ptr<arrow::Schema> schema() const override;

  arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override;
};
} // namespace gizmosql::ddb