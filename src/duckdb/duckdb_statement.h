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

#include <duckdb.hpp>

#include <memory>
#include <string>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/type_fwd.h>

#include "flight_sql_fwd.h"

namespace gizmosql::ddb {

std::shared_ptr<arrow::DataType> GetDataTypeFromDuckDbType(
    const duckdb::LogicalType duckdb_type);

/// \brief Create an object ColumnMetadata using the column type and
///        table name.
/// \param column_type  The DuckDB type.
/// \param table        The table name.
/// \return             A Column Metadata object.
flight::sql::ColumnMetadata GetColumnMetadata(int column_type, const char* table);

class DuckDBStatement {
 public:
  /// \brief Creates a duckdb statement.
  /// \param[in] db        duckdb database instance.
  /// \param[in] sql       SQL statement.
  /// \return              A DuckDBStatement object.
  static arrow::Result<std::shared_ptr<DuckDBStatement>> Create(
      std::shared_ptr<duckdb::Connection> con, const std::string& sql);

  ~DuckDBStatement();

  /// \brief Creates an Arrow Schema based on the results of this statement.
  /// \return              The resulting Schema.
  arrow::Result<std::shared_ptr<arrow::Schema>> GetSchema() const;

  arrow::Result<int> Execute();
  arrow::Result<std::shared_ptr<arrow::RecordBatch>> FetchResult();
  // arrow::Result<std::shared_ptr<Schema>> GetArrowSchema();

  std::shared_ptr<duckdb::PreparedStatement> GetDuckDBStmt() const;

  /// \brief Executes an UPDATE, INSERT or DELETE statement.
  /// \return              The number of rows changed by execution.
  arrow::Result<int64_t> ExecuteUpdate();

  duckdb::vector<duckdb::Value> bind_parameters;

 private:
  std::shared_ptr<duckdb::Connection> con_;
  std::shared_ptr<duckdb::PreparedStatement> stmt_;
  duckdb::unique_ptr<duckdb::QueryResult> query_result_;
  
  // Support for direct query execution (fallback for statements that can't be prepared)
  std::string sql_; // Original SQL for direct execution
  bool use_direct_execution_; // Flag to indicate whether to use direct query execution

  DuckDBStatement(std::shared_ptr<duckdb::Connection> con,
                  std::shared_ptr<duckdb::PreparedStatement> stmt) {
    con_ = con;
    stmt_ = stmt;
    use_direct_execution_ = false;
  }
  
  // Constructor for direct execution mode
  DuckDBStatement(std::shared_ptr<duckdb::Connection> con,
                  const std::string& sql) {
    con_ = con;
    sql_ = sql;
    use_direct_execution_ = true;
    stmt_ = nullptr;
  }
};

}  // namespace gizmosql::ddb
