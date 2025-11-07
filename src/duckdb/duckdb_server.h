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

#include <arrow/api.h>
#include <arrow/flight/sql/server.h>
#include "flight_sql_fwd.h"

namespace gizmosql::ddb {
/// \brief Convert a column type to a ArrowType.
/// \param duckdb_type the duckdb type.
/// \return            The equivalent ArrowType.
std::shared_ptr<arrow::DataType> GetArrowType(const char* duckdb_type);

/// \brief Example implementation of FlightSqlServerBase backed by an in-memory DuckDB
///        database.
class DuckDBFlightSqlServer : public flight::sql::FlightSqlServerBase {
 public:
  ~DuckDBFlightSqlServer() override;

  static arrow::Result<std::shared_ptr<DuckDBFlightSqlServer>> Create(
      const std::string& path, const bool& read_only, const bool& print_queries,
      const int32_t& query_timeout);

  /// \brief Auxiliary method used to execute an arbitrary SQL statement on the underlying
  ///        DuckDB database.
  arrow::Status ExecuteSql(const std::string& sql) const;

  /// \brief Execute SQL query and return string results from first column
  arrow::Result<std::vector<std::string>> ExecuteSqlAndGetStringVector(
      const std::string& sql) const;

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

  arrow::Result<flight::CancelFlightInfoResult> CancelFlightInfo(
      const flight::ServerCallContext& context,
      const flight::CancelFlightInfoRequest& request) override;

  arrow::Result<flight::sql::CancelResult> CancelQuery(
      const flight::ServerCallContext& context,
      const flight::sql::ActionCancelQueryRequest& request) override;

  arrow::Result<flight::SetSessionOptionsResult> SetSessionOptions(
      const flight::ServerCallContext& context,
      const flight::SetSessionOptionsRequest& request) override;

  arrow::Result<flight::GetSessionOptionsResult> GetSessionOptions(
      const flight::ServerCallContext& context,
      const flight::GetSessionOptionsRequest& request) override;

  arrow::Result<flight::CloseSessionResult> CloseSession(
      const flight::ServerCallContext& context,
      const flight::CloseSessionRequest& request) override;

 private:
  class Impl;

  std::shared_ptr<Impl> impl_;

  explicit DuckDBFlightSqlServer(std::shared_ptr<Impl> impl);
};
}  // namespace gizmosql::ddb
