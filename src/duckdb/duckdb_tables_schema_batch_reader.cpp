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

#include "duckdb_tables_schema_batch_reader.h"

#include <duckdb.h>

#include <sstream>

#include <arrow/array/builder_binary.h>
#include <arrow/flight/sql/column_metadata.h>
#include <arrow/flight/sql/server.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>

#include "flight_sql_fwd.h"

using arrow::Status;

namespace gizmosql::ddb {
std::shared_ptr<arrow::Schema> DuckDBTablesWithSchemaBatchReader::schema() const {
  return flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema();
}

Status DuckDBTablesWithSchemaBatchReader::ReadNext(
    std::shared_ptr<arrow::RecordBatch>* batch) {
  if (already_executed_) {
    *batch = NULLPTR;
    return Status::OK();
  } else {
    std::shared_ptr<DuckDBStatement> schema_statement;

    ARROW_ASSIGN_OR_RAISE(schema_statement,
                          DuckDBStatement::Create(client_session_, main_query_));

    std::shared_ptr<arrow::RecordBatch> first_batch;

    ARROW_RETURN_NOT_OK(reader_->ReadNext(&first_batch));

    if (!first_batch) {
      *batch = NULLPTR;
      return Status::OK();
    }

    const std::shared_ptr<arrow::Array> table_name_array =
        first_batch->GetColumnByName("table_name");

    arrow::BinaryBuilder schema_builder;

    auto* string_array = reinterpret_cast<arrow::StringArray*>(table_name_array.get());

    for (int table_name_index = 0; table_name_index < table_name_array->length();
         table_name_index++) {
      const std::string& table_name = string_array->GetString(table_name_index);

      // Just get the schema from a prepared statement
      std::shared_ptr<DuckDBStatement> table_schema_statement;
      ARROW_ASSIGN_OR_RAISE(
          table_schema_statement,
          DuckDBStatement::Create(client_session_,
            "SELECT * FROM " + table_name + " WHERE 1 = 0"));

      ARROW_ASSIGN_OR_RAISE(auto table_schema, table_schema_statement->GetSchema());

      const arrow::Result<std::shared_ptr<arrow::Buffer>>& value =
          arrow::ipc::SerializeSchema(*table_schema);

      std::shared_ptr<arrow::Buffer> schema_buffer;
      ARROW_ASSIGN_OR_RAISE(schema_buffer, value);

      ARROW_RETURN_NOT_OK(schema_builder.Append(::std::string_view(*schema_buffer)));
    }

    std::shared_ptr<arrow::Array> schema_array;
    ARROW_RETURN_NOT_OK(schema_builder.Finish(&schema_array));

    ARROW_ASSIGN_OR_RAISE(*batch,
                          first_batch->AddColumn(4, "table_schema", schema_array));
    already_executed_ = true;

    return Status::OK();
  }
}
} // namespace gizmosql::ddb