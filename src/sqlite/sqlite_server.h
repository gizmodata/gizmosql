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

#include <sqlite3.h>

#include <cstdint>
#include <memory>
#include <string>

#include "sqlite_statement.h"
#include "sqlite_statement_batch_reader.h"
#include "arrow/flight/sql/server.h"
#include "arrow/flight/types.h"
#include "arrow/result.h"
#include "flight_sql_fwd.h"

namespace gizmosql::sqlite {

/// \brief Convert a column type to a ArrowType.
/// \param sqlite_type the sqlite type.
/// \return            The equivalent ArrowType.
arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(const char* sqlite_type);

/// \brief Convert a column type name to SQLite type.
/// \param type_name the type name.
/// \return          The equivalent SQLite type.
int32_t GetSqlTypeFromTypeName(const char* type_name);

/// \brief  Get the DataType used when parameter type is not known.
/// \return DataType used when parameter type is not known.
inline std::shared_ptr<arrow::DataType> GetUnknownColumnDataType() {
  return arrow::dense_union({
      field("string", arrow::utf8()),
      field("bytes", arrow::binary()),
      field("bigint", arrow::int64()),
      field("double", arrow::float64()),
  });
}

/// \brief Example implementation of FlightSqlServerBase backed by an in-memory SQLite3
///        database.
class SQLiteFlightSqlServer : public flight::sql::FlightSqlServerBase {
 public:
  ~SQLiteFlightSqlServer() override;

  static arrow::Result<std::shared_ptr<SQLiteFlightSqlServer>> Create(
      const std::string& path, const bool& read_only);

  /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
  ///        SQLite database.
  arrow::Status ExecuteSql(const std::string& sql);

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext& context,
      const flight::sql::StatementQuery& command,
      const flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetStatement(
      const flight::ServerCallContext& context,
      const flight::sql::StatementQueryTicket& command) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCatalogs(
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetCatalogs(
      const flight::ServerCallContext& context) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSchemas(
      const flight::ServerCallContext& context, const flight::sql::GetDbSchemas& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetDbSchemas(
      const flight::ServerCallContext& context,
      const flight::sql::GetDbSchemas& command) override;
  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const flight::ServerCallContext& context,
      const flight::sql::StatementUpdate& update) override;
  arrow::Result<flight::sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::ActionCreatePreparedStatementRequest& request) override;
  arrow::Status ClosePreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::ActionClosePreparedStatementRequest& request) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementQuery& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetPreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementQuery& command) override;
  arrow::Status DoPutPreparedStatementQuery(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementQuery& command,
      flight::FlightMessageReader* reader, flight::FlightMetadataWriter* writer) override;
  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementUpdate& command,
      flight::FlightMessageReader* reader) override;

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
      const flight::ServerCallContext& context, const flight::sql::GetTables& command,
      const flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTables(
      const flight::ServerCallContext& context,
      const flight::sql::GetTables& command) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoXdbcTypeInfo(
      const flight::ServerCallContext& context,
      const flight::sql::GetXdbcTypeInfo& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetXdbcTypeInfo(
      const flight::ServerCallContext& context,
      const flight::sql::GetXdbcTypeInfo& command) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTableTypes(
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTableTypes(
      const flight::ServerCallContext& context) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoImportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetImportedKeys& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetImportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetImportedKeys& command) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoExportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetExportedKeys& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetExportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetExportedKeys& command) override;
  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCrossReference(
      const flight::ServerCallContext& context,
      const flight::sql::GetCrossReference& command,
      const flight::FlightDescriptor& descriptor) override;
  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetCrossReference(
      const flight::ServerCallContext& context,
      const flight::sql::GetCrossReference& command) override;

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPrimaryKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetPrimaryKeys& command,
      const flight::FlightDescriptor& descriptor) override;

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetPrimaryKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetPrimaryKeys& command) override;

  arrow::Result<flight::sql::ActionBeginTransactionResult> BeginTransaction(
      const flight::ServerCallContext& context,
      const flight::sql::ActionBeginTransactionRequest& request) override;
  arrow::Status EndTransaction(
      const flight::ServerCallContext& context,
      const flight::sql::ActionEndTransactionRequest& request) override;

 private:
  class Impl;
  std::shared_ptr<Impl> impl_;

  explicit SQLiteFlightSqlServer(std::shared_ptr<Impl> impl);
};

}  // namespace gizmosql::sqlite
