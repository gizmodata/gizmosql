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

#include "duckdb_statement.h"

#include <duckdb.h>
#include <duckdb/common/arrow/arrow_converter.hpp>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include <arrow/flight/sql/column_metadata.h>
#include <arrow/c/bridge.h>
#include "duckdb_server.h"

using duckdb::QueryResult;

namespace arrow {
    namespace flight {
        namespace sql {
            namespace duckdbflight {

                std::shared_ptr<DataType> GetDataTypeFromDuckDbType(
                        const duckdb::LogicalType duckdb_type
                ) {
                    const duckdb::LogicalTypeId column_type_id = duckdb_type.id();
                    switch (column_type_id) {
                        case duckdb::LogicalTypeId::INTEGER:
                            return int32();
                        case duckdb::LogicalTypeId::DECIMAL: {
                            uint8_t width = 0;
                            uint8_t scale = 0;
                            bool dec_properties = duckdb_type.GetDecimalProperties(width, scale);
                            return decimal(scale, width);
                        }
                        case duckdb::LogicalTypeId::FLOAT:
                            return float32();
                        case duckdb::LogicalTypeId::DOUBLE:
                            return float64();
                        case duckdb::LogicalTypeId::CHAR:
                        case duckdb::LogicalTypeId::VARCHAR:
                            return utf8();
                        case duckdb::LogicalTypeId::BLOB:
                            return binary();
                        case duckdb::LogicalTypeId::TINYINT:
                            return int8();
                        case duckdb::LogicalTypeId::SMALLINT:
                            return int16();
                        case duckdb::LogicalTypeId::BIGINT:
                            return int64();
                        case duckdb::LogicalTypeId::BOOLEAN:
                            return boolean();
                        case duckdb::LogicalTypeId::DATE:
                            return date32();
                        case duckdb::LogicalTypeId::TIME:
                        case duckdb::LogicalTypeId::TIMESTAMP_MS:
                            return timestamp(arrow::TimeUnit::MILLI);
                        case duckdb::LogicalTypeId::TIMESTAMP:
                            return timestamp(arrow::TimeUnit::MICRO);
                        case duckdb::LogicalTypeId::TIMESTAMP_SEC:
                            return timestamp(arrow::TimeUnit::SECOND);
                        case duckdb::LogicalTypeId::TIMESTAMP_NS:
                            return timestamp(arrow::TimeUnit::NANO);
                        case duckdb::LogicalTypeId::INTERVAL:
                            return duration(arrow::TimeUnit::MICRO); // ASSUMING MICRO AS DUCKDB's DOCS DOES NOT SPECIFY
                        case duckdb::LogicalTypeId::UTINYINT:
                            return uint8();
                        case duckdb::LogicalTypeId::USMALLINT:
                            return uint16();
                        case duckdb::LogicalTypeId::UINTEGER:
                            return uint32();
                        case duckdb::LogicalTypeId::UBIGINT:
                            return int64();
                        case duckdb::LogicalTypeId::INVALID:
                        case duckdb::LogicalTypeId::SQLNULL:
                        case duckdb::LogicalTypeId::UNKNOWN:
                        case duckdb::LogicalTypeId::ANY:
                        case duckdb::LogicalTypeId::USER:
                        case duckdb::LogicalTypeId::TIMESTAMP_TZ:
                        case duckdb::LogicalTypeId::TIME_TZ:
                        case duckdb::LogicalTypeId::HUGEINT:
                            return decimal128(38, 0);
                        case duckdb::LogicalTypeId::POINTER:
                        case duckdb::LogicalTypeId::VALIDITY:
                        case duckdb::LogicalTypeId::UUID:
                        case duckdb::LogicalTypeId::STRUCT:
                        case duckdb::LogicalTypeId::LIST:
                        case duckdb::LogicalTypeId::MAP:
                        case duckdb::LogicalTypeId::TABLE:
                        case duckdb::LogicalTypeId::ENUM:
                        default:
                            return null();
                    }
                }

                arrow::Result<std::shared_ptr<DuckDBStatement>> DuckDBStatement::Create(
                        std::shared_ptr<duckdb::Connection> con, const std::string &sql) {

                    std::shared_ptr<duckdb::PreparedStatement> stmt = con->Prepare(sql);

                    if (not stmt->success) {
                        std::string err_msg =
                                "Can't prepare statement: '" + sql + "' - Error: " + stmt->error.Message();
                        return Status::Invalid(err_msg);
                    }

                    std::shared_ptr<DuckDBStatement> result(new DuckDBStatement(con, stmt));

                    return result;
                }

                DuckDBStatement::~DuckDBStatement() {
                }

                arrow::Result<int> DuckDBStatement::Execute() {
                    query_result_ = stmt_->Execute(bind_parameters);

                    return 0;
                }

                arrow::Result<std::shared_ptr<RecordBatch>> DuckDBStatement::FetchResult() {
                    std::shared_ptr<RecordBatch> record_batch;
                    ArrowArray res_arr;
                    ArrowSchema res_schema;
                    duckdb::ClientProperties res_options;
                    res_options.time_zone = query_result_->client_properties.time_zone;

                    duckdb::ArrowConverter::ToArrowSchema(&res_schema, query_result_->types, query_result_->names,
                                                          res_options);

                    duckdb::unique_ptr<duckdb::DataChunk> data_chunk;
                    duckdb::PreservedError fetch_error;
                    auto fetch_success = query_result_->TryFetch(data_chunk, fetch_error);
                    if (!fetch_success) {
                        ARROW_RETURN_NOT_OK(arrow::Status::ExecutionError(fetch_error.Message()));
                    }

                    if (data_chunk != nullptr) {
                        duckdb::ArrowConverter::ToArrowArray(*data_chunk, &res_arr, res_options);
                        ARROW_ASSIGN_OR_RAISE(record_batch, arrow::ImportRecordBatch(&res_arr, &res_schema));
                    }

                    return record_batch;
                }

                std::shared_ptr<duckdb::PreparedStatement> DuckDBStatement::GetDuckDBStmt() const {
                    return stmt_;
                }

                arrow::Result<int64_t> DuckDBStatement::ExecuteUpdate() {
                    ARROW_RETURN_NOT_OK(Execute());
                    auto result = FetchResult();
                    return result->get()->num_rows();
                }

                arrow::Result<std::shared_ptr<Schema>> DuckDBStatement::GetSchema() const {
                    // get the names and types of the result schema
                    auto names = stmt_->GetNames();
                    auto types = stmt_->GetTypes();

                    auto &context = stmt_->context;
                    auto client_properties = context->GetClientProperties();

                    ArrowSchema arrow_schema;
                    duckdb::ArrowConverter::ToArrowSchema(&arrow_schema, types, names, client_properties);

                    auto return_value = arrow::ImportSchema(&arrow_schema);

                    return return_value;
                }

            }  // namespace sqlite
        }  // namespace sql
    }  // namespace flight
}  // namespace arrow
