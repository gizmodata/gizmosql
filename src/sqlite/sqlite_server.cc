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

#include "sqlite_server.h"

#define BOOST_NO_CXX98_FUNCTION_BASE  // ARROW-17805
#include <boost/algorithm/string.hpp>
#include <mutex>
#include <random>
#include <sstream>
#include <unordered_map>
#include <utility>
#include <iostream>

#include <sqlite3.h>

#include <arrow/array/builder_binary.h>
#include "sqlite_sql_info.h"
#include "sqlite_statement.h"
#include "sqlite_statement_batch_reader.h"
#include "sqlite_tables_schema_batch_reader.h"
#include "sqlite_type_info.h"
#include <arrow/flight/sql/server.h>
#include <arrow/scalar.h>
#include <arrow/util/logging.h>
#include "../common/include/detail/gizmosql_logging.h"

#include "../common/include/detail/flight_sql_fwd.h"

using arrow::Status;

namespace gizmosql::sqlite {

namespace {

std::string PrepareQueryForGetTables(const flight::sql::GetTables& command) {
  std::stringstream table_query;

  table_query << "SELECT 'main' as catalog_name, null as schema_name, name as "
                 "table_name, type as table_type FROM sqlite_master where 1=1";

  if (command.catalog.has_value()) {
    table_query << " and catalog_name='" << command.catalog.value() << "'";
  }

  if (command.db_schema_filter_pattern.has_value()) {
    table_query << " and schema_name LIKE '" << command.db_schema_filter_pattern.value()
                << "'";
  }

  if (command.table_name_filter_pattern.has_value()) {
    table_query << " and table_name LIKE '" << command.table_name_filter_pattern.value()
                << "'";
  }

  if (!command.table_types.empty()) {
    table_query << " and table_type IN (";
    size_t size = command.table_types.size();
    for (size_t i = 0; i < size; i++) {
      table_query << "'" << command.table_types[i] << "'";
      if (size - 1 != i) {
        table_query << ",";
      }
    }

    table_query << ")";
  }

  table_query << " order by table_name";
  return table_query.str();
}

template <typename Callback>
Status SetParametersOnSQLiteStatement(SqliteStatement* statement,
                                      flight::FlightMessageReader* reader,
                                      Callback callback) {
  sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
  while (true) {
    ARROW_ASSIGN_OR_RAISE(flight::FlightStreamChunk chunk, reader->Next());
    if (chunk.data == nullptr) break;

    const int64_t num_rows = chunk.data->num_rows();
    if (num_rows == 0) continue;

    ARROW_RETURN_NOT_OK(statement->SetParameters({std::move(chunk.data)}));
    for (int i = 0; i < num_rows; ++i) {
      if (sqlite3_clear_bindings(stmt) != SQLITE_OK) {
        return Status::Invalid("Failed to reset bindings on row ", i, ": ",
                               sqlite3_errmsg(statement->db()));
      }
      // batch_index is always 0 since we're calling SetParameters
      // with a single batch at a time
      ARROW_RETURN_NOT_OK(statement->Bind(/*batch_index=*/0, i));
      ARROW_RETURN_NOT_OK(callback());
    }
  }

  return Status::OK();
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetSQLiteQuery(
    sqlite3* db, const std::string& query, const std::shared_ptr<arrow::Schema>& schema) {
  std::shared_ptr<SqliteStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db, query));

  std::shared_ptr<SqliteStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement, schema));

  return std::make_unique<flight::RecordBatchStream>(reader);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoForCommand(
    const flight::FlightDescriptor& descriptor,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<flight::FlightEndpoint> endpoints{
      flight::FlightEndpoint{{descriptor.cmd}, {}, std::nullopt, ""}};
  ARROW_ASSIGN_OR_RAISE(auto result, flight::FlightInfo::Make(*schema, descriptor,
                                                              endpoints, -1, -1, false))

  return std::make_unique<flight::FlightInfo>(result);
}

std::string PrepareQueryForGetImportedOrExportedKeys(const std::string& filter) {
  return R"(SELECT * FROM (SELECT NULL AS pk_catalog_name,
    NULL AS pk_schema_name,
    p."table" AS pk_table_name,
    p."to" AS pk_column_name,
    NULL AS fk_catalog_name,
    NULL AS fk_schema_name,
    m.name AS fk_table_name,
    p."from" AS fk_column_name,
    p.seq AS key_sequence,
    NULL AS pk_key_name,
    NULL AS fk_key_name,
    CASE
        WHEN p.on_update = 'CASCADE' THEN 0
        WHEN p.on_update = 'RESTRICT' THEN 1
        WHEN p.on_update = 'SET NULL' THEN 2
        WHEN p.on_update = 'NO ACTION' THEN 3
        WHEN p.on_update = 'SET DEFAULT' THEN 4
    END AS update_rule,
    CASE
        WHEN p.on_delete = 'CASCADE' THEN 0
        WHEN p.on_delete = 'RESTRICT' THEN 1
        WHEN p.on_delete = 'SET NULL' THEN 2
        WHEN p.on_delete = 'NO ACTION' THEN 3
        WHEN p.on_delete = 'SET DEFAULT' THEN 4
    END AS delete_rule
  FROM sqlite_master m
  JOIN pragma_foreign_key_list(m.name) p ON m.name != p."table"
  WHERE m.type = 'table') WHERE )" +
         filter + R"( ORDER BY
  pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence)";
}

}  // namespace

arrow::Result<std::shared_ptr<arrow::DataType>> GetArrowType(const char* sqlite_type) {
  if (sqlite_type == nullptr || std::strlen(sqlite_type) == 0) {
    // SQLite may not know the column type yet.
    return arrow::null();
  }

  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
    return arrow::int64();
  } else if (boost::iequals(sqlite_type, "REAL")) {
    return arrow::float64();
  } else if (boost::iequals(sqlite_type, "BLOB")) {
    return arrow::binary();
  } else if (boost::iequals(sqlite_type, "TEXT") || boost::iequals(sqlite_type, "DATE") ||
             boost::istarts_with(sqlite_type, "char") ||
             boost::istarts_with(sqlite_type, "varchar")) {
    return arrow::utf8();
  }
  return Status::Invalid("Invalid SQLite type: ", sqlite_type);
}

int32_t GetSqlTypeFromTypeName(const char* sqlite_type) {
  if (sqlite_type == NULLPTR) {
    // SQLite may not know the column type yet.
    return SQLITE_NULL;
  }

  if (boost::iequals(sqlite_type, "int") || boost::iequals(sqlite_type, "integer")) {
    return SQLITE_INTEGER;
  } else if (boost::iequals(sqlite_type, "REAL")) {
    return SQLITE_FLOAT;
  } else if (boost::iequals(sqlite_type, "BLOB")) {
    return SQLITE_BLOB;
  } else if (boost::iequals(sqlite_type, "TEXT") || boost::iequals(sqlite_type, "DATE") ||
             boost::istarts_with(sqlite_type, "char") ||
             boost::istarts_with(sqlite_type, "varchar")) {
    return SQLITE_TEXT;
  } else {
    return SQLITE_NULL;
  }
}

class SQLiteFlightSqlServer::Impl {
 private:
  sqlite3* db_;
  const std::string db_uri_;
  std::mutex mutex_;
  std::unordered_map<std::string, std::shared_ptr<SqliteStatement>> prepared_statements_;
  std::unordered_map<std::string, sqlite3*> open_transactions_;
  std::default_random_engine gen_;

  arrow::Result<std::shared_ptr<SqliteStatement>> GetStatementByHandle(
      const std::string& handle) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto search = prepared_statements_.find(handle);
    if (search == prepared_statements_.end()) {
      return Status::KeyError("Prepared statement not found");
    }
    return search->second;
  }

  arrow::Result<sqlite3*> GetConnection(const std::string& transaction_id) {
    if (transaction_id.empty()) {
      ARROW_LOG(INFO) << "Using default connection";
      return db_;
    }

    std::lock_guard<std::mutex> guard(mutex_);
    auto it = open_transactions_.find(transaction_id);
    if (it == open_transactions_.end()) {
      return Status::KeyError("Unknown transaction ID: ", transaction_id);
    }
    ARROW_LOG(INFO) << "Using connection for transaction " << transaction_id;
    return it->second;
  }

  // Create a Ticket that combines a query and a transaction ID.
  arrow::Result<flight::Ticket> EncodeTransactionQuery(
      const std::string& query, const std::string& transaction_id) {
    std::string transaction_query = transaction_id;
    transaction_query += ':';
    transaction_query += query;
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                          flight::sql::CreateStatementQueryTicket(transaction_query));
    return flight::Ticket{std::move(ticket_string)};
  }

  arrow::Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
      const std::string& ticket) {
    auto divider = ticket.find(':');
    if (divider == std::string::npos) {
      return Status::Invalid("Malformed ticket");
    }
    std::string transaction_id = ticket.substr(0, divider);
    std::string query = ticket.substr(divider + 1);
    return std::make_pair(std::move(query), std::move(transaction_id));
  }

 public:
  explicit Impl(sqlite3* db) : db_(db) {}

  ~Impl() {
    sqlite3_close(db_);
    for (const auto& pair : open_transactions_) {
      sqlite3_close(pair.second);
    }
  }

  std::string GenerateRandomString() {
    uint32_t length = 16;

    // MSVC doesn't support char types here
    std::uniform_int_distribution<uint16_t> dist(static_cast<uint16_t>('0'),
                                                 static_cast<uint16_t>('Z'));
    std::string ret(length, 0);
    // Don't generate symbols to simplify parsing in DecodeTransactionQuery
    auto get_random_char = [&]() {
      char res;
      while (true) {
        res = static_cast<char>(dist(gen_));
        if (res <= '9' || res >= 'A') break;
      }
      return res;
    };
    std::generate_n(ret.begin(), length, get_random_char);
    return ret;
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext& context,
      const flight::sql::StatementQuery& command,
      const flight::FlightDescriptor& descriptor) {
    const std::string& query = command.query;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id));
    ARROW_ASSIGN_OR_RAISE(auto statement, SqliteStatement::Create(db, query));
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          EncodeTransactionQuery(query, command.transaction_id));
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
    // TODO: Set true only when "ORDER BY" is used in a main "SELECT"
    // in the given query.
    const bool ordered = false;
    ARROW_ASSIGN_OR_RAISE(
        auto result,
        flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1, ordered));

    return std::make_unique<flight::FlightInfo>(result);
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetStatement(
      const flight::ServerCallContext& context,
      const flight::sql::StatementQueryTicket& command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, DecodeTransactionQuery(command.statement_handle));
    const std::string& sql = pair.first;
    const std::string transaction_id = pair.second;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(transaction_id));

    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db, sql));

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCatalogs(
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetCatalogsSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetCatalogs(
      const flight::ServerCallContext& context) {
    // https://www.sqlite.org/cli.html
    // > The ".databases" command shows a list of all databases open
    // > in the current connection. There will always be at least
    // > 2. The first one is "main", the original database opened. The
    // > second is "temp", the database used for temporary tables.
    // For our purposes, return only "main" and ignore other databases.
    const std::shared_ptr<arrow::Schema>& schema =
        flight::sql::SqlSchema::GetCatalogsSchema();
    arrow::StringBuilder catalog_name_builder;
    ARROW_RETURN_NOT_OK(catalog_name_builder.Append("main"));
    ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());
    std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema, 1, {std::move(catalog_name)});
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSchemas(
      const flight::ServerCallContext& context, const flight::sql::GetDbSchemas& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetDbSchemasSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetDbSchemas(
      const flight::ServerCallContext& context,
      const flight::sql::GetDbSchemas& command) {
    // SQLite doesn't support schemas, so pretend we have a single
    // unnamed schema.
    const std::shared_ptr<arrow::Schema>& schema =
        flight::sql::SqlSchema::GetDbSchemasSchema();
    arrow::StringBuilder catalog_name_builder;
    arrow::StringBuilder schema_name_builder;

    int64_t length = 0;
    // XXX: we don't really implement the full pattern match here
    if ((!command.catalog || command.catalog == "main") &&
        (!command.db_schema_filter_pattern || command.db_schema_filter_pattern == "%")) {
      ARROW_RETURN_NOT_OK(catalog_name_builder.Append("main"));
      ARROW_RETURN_NOT_OK(schema_name_builder.AppendNull());
      length++;
    }

    ARROW_ASSIGN_OR_RAISE(auto catalog_name, catalog_name_builder.Finish());
    ARROW_ASSIGN_OR_RAISE(auto schema_name, schema_name_builder.Finish());
    std::shared_ptr<arrow::RecordBatch> batch = arrow::RecordBatch::Make(
        schema, length, {std::move(catalog_name), std::move(schema_name)});
    ARROW_ASSIGN_OR_RAISE(auto reader, arrow::RecordBatchReader::Make({batch}));
    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
      const flight::ServerCallContext& context, const flight::sql::GetTables& command,
      const flight::FlightDescriptor& descriptor) {
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{{descriptor.cmd}, {}, std::nullopt, ""}};

    bool include_schema = command.include_schema;
    ARROW_LOG(INFO) << "GetTables include_schema=" << include_schema;

    ARROW_ASSIGN_OR_RAISE(
        auto result,
        flight::FlightInfo::Make(
            include_schema ? *flight::sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                           : *flight::sql::SqlSchema::GetTablesSchema(),
            descriptor, endpoints, -1, -1, false))

    return std::make_unique<flight::FlightInfo>(std::move(result));
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTables(
      const flight::ServerCallContext& context, const flight::sql::GetTables& command) {
    std::string query = PrepareQueryForGetTables(command);
    ARROW_LOG(INFO) << "GetTables: " << query;

    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db_, query));

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader,
                          SqliteStatementBatchReader::Create(
                              statement, flight::sql::SqlSchema::GetTablesSchema()));

    if (command.include_schema) {
      std::shared_ptr<SqliteTablesWithSchemaBatchReader> table_schema_reader =
          std::make_shared<SqliteTablesWithSchemaBatchReader>(reader, query, db_);
      return std::make_unique<flight::RecordBatchStream>(table_schema_reader);
    } else {
      return std::make_unique<flight::RecordBatchStream>(reader);
    }
  }

  arrow::Result<int64_t> DoPutCommandStatementUpdate(
      const flight::ServerCallContext& context,
      const flight::sql::StatementUpdate& command) {
    const std::string& sql = command.query;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id));
    ARROW_LOG(INFO) << "Executing update: " << sql;
    ARROW_ASSIGN_OR_RAISE(auto statement, SqliteStatement::Create(db, sql));
    return statement->ExecuteUpdate();
  }

  arrow::Result<flight::sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::ActionCreatePreparedStatementRequest& request) {
    std::shared_ptr<SqliteStatement> statement;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(request.transaction_id));
    ARROW_LOG(INFO) << "Creating prepared statement: " << request.query;
    ARROW_ASSIGN_OR_RAISE(statement, SqliteStatement::Create(db, request.query));
    std::string handle = GenerateRandomString();

    {
      std::lock_guard<std::mutex> guard(mutex_);
      prepared_statements_[handle] = statement;
    }

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema());

    sqlite3_stmt* stmt = statement->GetSqlite3Stmt();
    const int parameter_count = sqlite3_bind_parameter_count(stmt);
    arrow::FieldVector parameter_fields;
    parameter_fields.reserve(parameter_count);

    // As SQLite doesn't know the parameter types before executing the query, the
    // example server is accepting any SQLite supported type as input by using a dense
    // union.
    const std::shared_ptr<arrow::DataType>& dense_union_type = GetUnknownColumnDataType();

    for (int i = 0; i < parameter_count; i++) {
      const char* parameter_name_chars = sqlite3_bind_parameter_name(stmt, i + 1);
      std::string parameter_name;
      if (parameter_name_chars == NULLPTR) {
        parameter_name = std::string("parameter_") + std::to_string(i + 1);
      } else {
        parameter_name = parameter_name_chars;
      }
      parameter_fields.push_back(field(parameter_name, dense_union_type));
    }

    std::shared_ptr<arrow::Schema> parameter_schema = arrow::schema(parameter_fields);
    return flight::sql::ActionCreatePreparedStatementResult{
        std::move(dataset_schema), std::move(parameter_schema), std::move(handle)};
  }

  Status ClosePreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::ActionClosePreparedStatementRequest& request) {
    std::lock_guard<std::mutex> guard(mutex_);
    const std::string& prepared_statement_handle = request.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search != prepared_statements_.end()) {
      prepared_statements_.erase(prepared_statement_handle);
    } else {
      return Status::Invalid("Prepared statement not found");
    }

    return Status::OK();
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementQuery& command,
      const flight::FlightDescriptor& descriptor) {
    std::lock_guard<std::mutex> guard(mutex_);
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<SqliteStatement> statement = search->second;

    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema());

    return GetFlightInfoForCommand(descriptor, schema);
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetPreparedStatement(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementQuery& command) {
    std::lock_guard<std::mutex> guard(mutex_);
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<SqliteStatement> statement = search->second;

    std::shared_ptr<SqliteStatementBatchReader> reader;
    ARROW_ASSIGN_OR_RAISE(reader, SqliteStatementBatchReader::Create(statement));

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Status DoPutPreparedStatementQuery(const flight::ServerCallContext& context,
                                     const flight::sql::PreparedStatementQuery& command,
                                     flight::FlightMessageReader* reader,
                                     flight::FlightMetadataWriter* writer) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(auto statement,
                          GetStatementByHandle(prepared_statement_handle));
    // Save params here and execute later
    ARROW_ASSIGN_OR_RAISE(auto batches, reader->ToRecordBatches());
    ARROW_RETURN_NOT_OK(statement->SetParameters(std::move(batches)));
    return Status::OK();
  }

  arrow::Result<int64_t> DoPutPreparedStatementUpdate(
      const flight::ServerCallContext& context,
      const flight::sql::PreparedStatementUpdate& command,
      flight::FlightMessageReader* reader) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<SqliteStatement> statement,
                          GetStatementByHandle(prepared_statement_handle));

    int64_t rows_affected = 0;
    if (sqlite3_bind_parameter_count(statement->GetSqlite3Stmt()) == 0) {
      ARROW_ASSIGN_OR_RAISE(rows_affected, statement->ExecuteUpdate());
    } else {
      ARROW_RETURN_NOT_OK(SetParametersOnSQLiteStatement(statement.get(), reader, [&]() {
        ARROW_ASSIGN_OR_RAISE(int64_t rows, statement->ExecuteUpdate());
        rows_affected += rows;
        return statement->Reset().status();
      }));
    }
    return rows_affected;
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTableTypes(
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTableTypes(
      const flight::ServerCallContext& context) {
    std::string query = "SELECT DISTINCT type as table_type FROM sqlite_master";

    return DoGetSQLiteQuery(db_, query, flight::sql::SqlSchema::GetTableTypesSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTypeInfo(
      const flight::ServerCallContext& context,
      const flight::sql::GetXdbcTypeInfo& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetXdbcTypeInfoSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetTypeInfo(
      const flight::ServerCallContext& context,
      const flight::sql::GetXdbcTypeInfo& command) {
    ARROW_ASSIGN_OR_RAISE(auto type_info_result,
                          command.data_type.has_value()
                              ? DoGetTypeInfoResult(command.data_type.value())
                              : DoGetTypeInfoResult());

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::RecordBatchReader::Make({type_info_result}));
    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPrimaryKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetPrimaryKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetPrimaryKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetPrimaryKeys& command) {
    std::stringstream table_query;

    // The field key_name cannot be recovered by the sqlite, so it is being set
    // to null following the same pattern for catalog_name and schema_name.
    table_query << "SELECT null as catalog_name, null as schema_name, table_name, "
                   "name as column_name,  pk as key_sequence, null as key_name\n"
                   "FROM pragma_table_info(table_name)\n"
                   "    JOIN (SELECT null as catalog_name, null as schema_name, name as "
                   "table_name, type as table_type\n"
                   "FROM sqlite_master) where 1=1 and pk != 0";

    const flight::sql::TableRef& table_ref = command.table_ref;
    if (table_ref.catalog.has_value()) {
      table_query << " and catalog_name LIKE '" << table_ref.catalog.value() << "'";
    }

    if (table_ref.db_schema.has_value()) {
      table_query << " and schema_name LIKE '" << table_ref.db_schema.value() << "'";
    }

    table_query << " and table_name LIKE '" << table_ref.table << "'";

    return DoGetSQLiteQuery(db_, table_query.str(),
                            flight::sql::SqlSchema::GetPrimaryKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoImportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetImportedKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetImportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetImportedKeys& command) {
    const flight::sql::TableRef& table_ref = command.table_ref;
    std::string filter = "fk_table_name = '" + table_ref.table + "'";
    if (table_ref.catalog.has_value()) {
      filter += " AND fk_catalog_name = '" + table_ref.catalog.value() + "'";
    }
    if (table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query, flight::sql::SqlSchema::GetImportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoExportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetExportedKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetExportedKeys(
      const flight::ServerCallContext& context,
      const flight::sql::GetExportedKeys& command) {
    const flight::sql::TableRef& table_ref = command.table_ref;
    std::string filter = "pk_table_name = '" + table_ref.table + "'";
    if (table_ref.catalog.has_value()) {
      filter += " AND pk_catalog_name = '" + table_ref.catalog.value() + "'";
    }
    if (table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query, flight::sql::SqlSchema::GetExportedKeysSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCrossReference(
      const flight::ServerCallContext& context,
      const flight::sql::GetCrossReference& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetCrossReferenceSchema());
  }

  arrow::Result<std::unique_ptr<flight::FlightDataStream>> DoGetCrossReference(
      const flight::ServerCallContext& context,
      const flight::sql::GetCrossReference& command) {
    const flight::sql::TableRef& pk_table_ref = command.pk_table_ref;
    std::string filter = "pk_table_name = '" + pk_table_ref.table + "'";
    if (pk_table_ref.catalog.has_value()) {
      filter += " AND pk_catalog_name = '" + pk_table_ref.catalog.value() + "'";
    }
    if (pk_table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + pk_table_ref.db_schema.value() + "'";
    }

    const flight::sql::TableRef& fk_table_ref = command.fk_table_ref;
    filter += " AND fk_table_name = '" + fk_table_ref.table + "'";
    if (fk_table_ref.catalog.has_value()) {
      filter += " AND fk_catalog_name = '" + fk_table_ref.catalog.value() + "'";
    }
    if (fk_table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + fk_table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetSQLiteQuery(db_, query,
                            flight::sql::SqlSchema::GetCrossReferenceSchema());
  }

  Status ExecuteSql(const std::string& sql) { return ExecuteSql(db_, sql); }

  Status ExecuteSql(sqlite3* db, const std::string& sql) {
    char* err_msg = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &err_msg);
    if (rc != SQLITE_OK) {
      std::string error_msg;
      if (err_msg != nullptr) {
        error_msg = err_msg;
        sqlite3_free(err_msg);
      }
      return Status::IOError(error_msg);
    }
    if (err_msg) sqlite3_free(err_msg);
    return Status::OK();
  }

  arrow::Result<flight::sql::ActionBeginTransactionResult> BeginTransaction(
      const flight::ServerCallContext& context,
      const flight::sql::ActionBeginTransactionRequest& request) {
    std::string handle = GenerateRandomString();
    sqlite3* new_db = nullptr;
    if (sqlite3_open_v2(db_uri_.c_str(), &new_db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                        /*zVfs=*/nullptr) != SQLITE_OK) {
      std::string error_message = "Can't open new connection: ";
      if (new_db) {
        error_message += sqlite3_errmsg(new_db);
        sqlite3_close(new_db);
      }
      return Status::Invalid(error_message);
    }

    ARROW_RETURN_NOT_OK(ExecuteSql(new_db, "BEGIN TRANSACTION"));

    ARROW_LOG(INFO) << "Beginning transaction on " << handle;

    std::lock_guard<std::mutex> guard(mutex_);
    open_transactions_[handle] = new_db;
    return flight::sql::ActionBeginTransactionResult{std::move(handle)};
  }

  Status EndTransaction(const flight::ServerCallContext& context,
                        const flight::sql::ActionEndTransactionRequest& request) {
    Status status;
    sqlite3* transaction = nullptr;
    {
      std::lock_guard<std::mutex> guard(mutex_);
      auto it = open_transactions_.find(request.transaction_id);
      if (it == open_transactions_.end()) {
        return Status::KeyError("Unknown transaction ID: ", request.transaction_id);
      }

      if (request.action == flight::sql::ActionEndTransactionRequest::kCommit) {
        ARROW_LOG(INFO) << "Committing on " << request.transaction_id;
        status = ExecuteSql(it->second, "COMMIT");
      } else {
        ARROW_LOG(INFO) << "Rolling back on " << request.transaction_id;
        status = ExecuteSql(it->second, "ROLLBACK");
      }
      transaction = it->second;
      open_transactions_.erase(it);
    }
    sqlite3_close(transaction);
    return status;
  }
};

// Give each server instance its own in-memory DB
std::atomic<int64_t> kDbCounter(0);

SQLiteFlightSqlServer::SQLiteFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

arrow::Result<std::shared_ptr<SQLiteFlightSqlServer>> SQLiteFlightSqlServer::Create(
    const std::string& path, const bool& read_only) {
  GIZMOSQL_LOG(INFO) << "SQLite version: " << sqlite3_libversion();

  sqlite3* db = nullptr;

  char* db_location;

  bool in_memory = path == ":memory:";

  if (in_memory) {
    db_location = (char*)":memory:";
  } else {
    db_location = (char*)path.c_str();
  }

  // Set the open flag per the read_only arg
  int open_flags;
  if (read_only) {
    open_flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_URI;
  } else {
    open_flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI;
  }

  if (sqlite3_open_v2(db_location, &db, open_flags,
                      /*zVfs=*/nullptr)) {
    std::string err_msg = "Can't open database: ";
    if (db != nullptr) {
      err_msg += sqlite3_errmsg(db);
      sqlite3_close(db);
    } else {
      err_msg += "Unable to start SQLite. Insufficient memory";
    }

    return Status::Invalid(err_msg);
  }

  std::shared_ptr<Impl> impl = std::make_shared<Impl>(db);

  std::shared_ptr<SQLiteFlightSqlServer> result(
      new SQLiteFlightSqlServer(std::move(impl)));
  for (const auto& id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }

  return result;
}

SQLiteFlightSqlServer::~SQLiteFlightSqlServer() = default;

Status SQLiteFlightSqlServer::ExecuteSql(const std::string& sql) {
  return impl_->ExecuteSql(sql);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext& context, const flight::sql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetStatement(const flight::ServerCallContext& context,
                                      const flight::sql::StatementQueryTicket& command) {
  return impl_->DoGetStatement(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoCatalogs(const flight::ServerCallContext& context,
                                             const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCatalogs(context, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetCatalogs(const flight::ServerCallContext& context) {
  return impl_->DoGetCatalogs(context);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoSchemas(const flight::ServerCallContext& context,
                                            const flight::sql::GetDbSchemas& command,
                                            const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoSchemas(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetDbSchemas(const flight::ServerCallContext& context,
                                      const flight::sql::GetDbSchemas& command) {
  return impl_->DoGetDbSchemas(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoTables(const flight::ServerCallContext& context,
                                           const flight::sql::GetTables& command,
                                           const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTables(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetTables(const flight::ServerCallContext& context,
                                   const flight::sql::GetTables& command) {
  return impl_->DoGetTables(context, command);
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutCommandStatementUpdate(
    const flight::ServerCallContext& context,
    const flight::sql::StatementUpdate& command) {
  return impl_->DoPutCommandStatementUpdate(context, command);
}

arrow::Result<flight::sql::ActionCreatePreparedStatementResult>
SQLiteFlightSqlServer::CreatePreparedStatement(
    const flight::ServerCallContext& context,
    const flight::sql::ActionCreatePreparedStatementRequest& request) {
  return impl_->CreatePreparedStatement(context, request);
}

Status SQLiteFlightSqlServer::ClosePreparedStatement(
    const flight::ServerCallContext& context,
    const flight::sql::ActionClosePreparedStatementRequest& request) {
  return impl_->ClosePreparedStatement(context, request);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoPreparedStatement(
    const flight::ServerCallContext& context,
    const flight::sql::PreparedStatementQuery& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetPreparedStatement(
    const flight::ServerCallContext& context,
    const flight::sql::PreparedStatementQuery& command) {
  return impl_->DoGetPreparedStatement(context, command);
}

Status SQLiteFlightSqlServer::DoPutPreparedStatementQuery(
    const flight::ServerCallContext& context,
    const flight::sql::PreparedStatementQuery& command,
    flight::FlightMessageReader* reader, flight::FlightMetadataWriter* writer) {
  return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
}

arrow::Result<int64_t> SQLiteFlightSqlServer::DoPutPreparedStatementUpdate(
    const flight::ServerCallContext& context,
    const flight::sql::PreparedStatementUpdate& command,
    flight::FlightMessageReader* reader) {
  return impl_->DoPutPreparedStatementUpdate(context, command, reader);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoTableTypes(
    const flight::ServerCallContext& context,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTableTypes(context, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetTableTypes(const flight::ServerCallContext& context) {
  return impl_->DoGetTableTypes(context);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoXdbcTypeInfo(
    const flight::ServerCallContext& context, const flight::sql::GetXdbcTypeInfo& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTypeInfo(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetXdbcTypeInfo(const flight::ServerCallContext& context,
                                         const flight::sql::GetXdbcTypeInfo& command) {
  return impl_->DoGetTypeInfo(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoPrimaryKeys(
    const flight::ServerCallContext& context, const flight::sql::GetPrimaryKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetPrimaryKeys(const flight::ServerCallContext& context,
                                        const flight::sql::GetPrimaryKeys& command) {
  return impl_->DoGetPrimaryKeys(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoImportedKeys(
    const flight::ServerCallContext& context, const flight::sql::GetImportedKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetImportedKeys(const flight::ServerCallContext& context,
                                         const flight::sql::GetImportedKeys& command) {
  return impl_->DoGetImportedKeys(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoExportedKeys(
    const flight::ServerCallContext& context, const flight::sql::GetExportedKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetExportedKeys(const flight::ServerCallContext& context,
                                         const flight::sql::GetExportedKeys& command) {
  return impl_->DoGetExportedKeys(context, command);
}

arrow::Result<std::unique_ptr<flight::FlightInfo>>
SQLiteFlightSqlServer::GetFlightInfoCrossReference(
    const flight::ServerCallContext& context,
    const flight::sql::GetCrossReference& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCrossReference(context, command, descriptor);
}

arrow::Result<std::unique_ptr<flight::FlightDataStream>>
SQLiteFlightSqlServer::DoGetCrossReference(
    const flight::ServerCallContext& context,
    const flight::sql::GetCrossReference& command) {
  return impl_->DoGetCrossReference(context, command);
}

arrow::Result<flight::sql::ActionBeginTransactionResult>
SQLiteFlightSqlServer::BeginTransaction(
    const flight::ServerCallContext& context,
    const flight::sql::ActionBeginTransactionRequest& request) {
  return impl_->BeginTransaction(context, request);
}
Status SQLiteFlightSqlServer::EndTransaction(
    const flight::ServerCallContext& context,
    const flight::sql::ActionEndTransactionRequest& request) {
  return impl_->EndTransaction(context, request);
}

}  // namespace gizmosql::sqlite
