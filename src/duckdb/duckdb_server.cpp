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

#include "duckdb_server.h"

#include <duckdb.hpp>

#include <boost/algorithm/string.hpp>
#include <map>
#include <random>
#include <sstream>
#include <iostream>
#include <mutex>

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/types.h>

#include "duckdb_sql_info.h"
#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"
#include "duckdb_tables_schema_batch_reader.h"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "flight_sql_fwd.h"

using arrow::Result;
using arrow::Status;

namespace sql = flight::sql;

namespace gizmosql::ddb {
namespace {

std::string PrepareQueryForGetTables(const sql::GetTables &command) {
  std::stringstream table_query;

  table_query
      << "SELECT table_catalog as catalog_name, table_schema as schema_name, table_name, "
         "table_type FROM information_schema.tables where 1=1";

  table_query << " and table_catalog = "
              << (command.catalog.has_value() ? "'" + command.catalog.value() + "'"
                                              : "CURRENT_DATABASE()");

  if (command.db_schema_filter_pattern.has_value()) {
    table_query << " and table_schema LIKE '" << command.db_schema_filter_pattern.value()
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

Status SetParametersOnDuckDBStatement(std::shared_ptr<DuckDBStatement> stmt,
                                      flight::FlightMessageReader *reader) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(flight::FlightStreamChunk chunk, reader->Next())
    const std::shared_ptr<arrow::RecordBatch> &record_batch = chunk.data;
    if (record_batch == nullptr) break;

    const int64_t num_rows = record_batch->num_rows();
    const int &num_columns = record_batch->num_columns();

    for (int row_index = 0; row_index < num_rows; ++row_index) {
      for (int column_index = 0; column_index < num_columns; ++column_index) {
        const std::shared_ptr<arrow::Array> &column = record_batch->column(column_index);
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Scalar> scalar,
                              column->GetScalar(row_index))

        stmt->bind_parameters.push_back(scalar->ToString());
      }
    }
  }

  return Status::OK();
}

Result<std::unique_ptr<flight::FlightDataStream>> DoGetDuckDBQuery(
    std::shared_ptr<duckdb::Connection> db, const std::string &query,
    const std::shared_ptr<arrow::Schema> &schema) {
  std::shared_ptr<DuckDBStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db, query))

  std::shared_ptr<DuckDBStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, DuckDBStatementBatchReader::Create(statement, schema))

  return std::make_unique<flight::RecordBatchStream>(reader);
}

Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoForCommand(
    const flight::FlightDescriptor &descriptor,
    const std::shared_ptr<arrow::Schema> &schema) {
  std::vector<flight::FlightEndpoint> endpoints{
      flight::FlightEndpoint{{descriptor.cmd}, {}}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  return std::make_unique<flight::FlightInfo>(result);
}

std::string PrepareQueryForGetImportedOrExportedKeys(const std::string &filter) {
  return R"(SELECT * FROM (
               SELECT fk.database_name                                                                         AS pk_catalog_name,
                      fk.schema_name                                                                           AS pk_schema_name,
                      fk.referenced_table                                                                      AS pk_table_name,
                      UNNEST(fk.referenced_column_names)                                                       AS pk_column_name,
                      fk.database_name                                                                         AS fk_catalog_name,
                      fk.schema_name                                                                           AS fk_schema_name,
                      fk.table_name                                                                            AS fk_table_name,
                      UNNEST(fk.constraint_column_names)                                                       AS fk_column_name,
                      UNNEST(fk.constraint_column_indexes)                                                     AS key_sequence,
                      fk.constraint_name                                                                       AS fk_key_name,
                      pk.constraint_name                                                                       AS pk_key_name,
                      1                                                                                        AS update_rule, /* DuckDB only supports RESTRICT */
                      1                                                                                        AS delete_rule /* DuckDB only supports RESTRICT */
               FROM duckdb_constraints() AS fk
                  JOIN
                    duckdb_constraints() AS pk
                  ON (fk.referenced_table = pk.table_name
                      AND fk.constraint_type = 'FOREIGN KEY'
                      AND pk.constraint_type = 'PRIMARY KEY'
                     )
            ) WHERE )" +
         filter + R"( ORDER BY
  pk_catalog_name, pk_schema_name, pk_table_name, pk_key_name, key_sequence)";
}

}  // namespace

class DuckDBFlightSqlServer::Impl {
 private:
  std::shared_ptr<duckdb::DuckDB> db_instance_;
  std::shared_ptr<duckdb::Connection> db_conn_;
  bool print_queries_;
  std::map<std::string, std::shared_ptr<DuckDBStatement>> prepared_statements_;
  std::unordered_map<std::string, std::shared_ptr<duckdb::Connection>> open_transactions_;
  std::default_random_engine gen_;
  std::mutex mutex_;

  Result<std::shared_ptr<DuckDBStatement>> GetStatementByHandle(
      const std::string &handle) {
    std::scoped_lock guard(mutex_);
    auto search = prepared_statements_.find(handle);
    if (search == prepared_statements_.end()) {
      return Status::KeyError("Prepared statement not found");
    }
    return search->second;
  }

  Result<std::shared_ptr<duckdb::Connection>> GetConnection(
      const std::string &transaction_id) {
    if (transaction_id.empty()) return db_conn_;

    std::scoped_lock guard(mutex_);
    auto it = open_transactions_.find(transaction_id);
    if (it == open_transactions_.end()) {
      return Status::KeyError("Unknown transaction ID: ", transaction_id);
    }
    return it->second;
  }

  // Create a Ticket that combines a query and a transaction ID.
  Result<flight::Ticket> EncodeTransactionQuery(const std::string &query,
                                                const std::string &transaction_id) {
    std::string transaction_query = transaction_id;
    transaction_query += ':';
    transaction_query += query;
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                          sql::CreateStatementQueryTicket(transaction_query));
    return flight::Ticket{std::move(ticket_string)};
  }

  Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
      const std::string &ticket) {
    auto divider = ticket.find(':');
    if (divider == std::string::npos) {
      return Status::Invalid("Malformed ticket");
    }
    std::string transaction_id = ticket.substr(0, divider);
    std::string query = ticket.substr(divider + 1);
    return std::make_pair(std::move(query), std::move(transaction_id));
  }

 public:
  explicit Impl(std::shared_ptr<duckdb::DuckDB> db_instance,
                std::shared_ptr<duckdb::Connection> db_connection,
                const bool &print_queries)
      : db_instance_(std::move(db_instance)),
        db_conn_(std::move(db_connection)),
        print_queries_(print_queries) {}

  ~Impl() = default;

  std::string GenerateRandomString(int length = 16) const {
    const char charset[] =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const int charsetLength = sizeof(charset) - 1;

    std::random_device rd;   // Create a random device to seed the generator
    std::mt19937 gen(rd());  // Create a Mersenne Twister generator
    std::uniform_int_distribution<> dis(
        0,
        charsetLength - 1);  // Create a uniform distribution over the character set

    std::string randomString;
    for (int i = 0; i < length; i++) {
      randomString += charset[dis(gen)];
    }

    return randomString;
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext &context, const sql::StatementQuery &command,
      const flight::FlightDescriptor &descriptor) {
    const std::string &query = command.query;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, query))
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          EncodeTransactionQuery(query, command.transaction_id))
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{std::move(ticket), {}}};
    ARROW_ASSIGN_OR_RAISE(
        auto result, flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))
    return std::make_unique<flight::FlightInfo>(result);
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetStatement(
      const flight::ServerCallContext &context,
      const sql::StatementQueryTicket &command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, DecodeTransactionQuery(command.statement_handle))
    const std::string &sql = pair.first;
    const std::string transaction_id = pair.second;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(transaction_id))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, sql))
    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(statement))

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCatalogs(
      const flight::ServerCallContext &context,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetCatalogsSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetCatalogs(
      const flight::ServerCallContext &context) {
    std::string query =
        "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY "
        "catalog_name";

    return DoGetDuckDBQuery(db_conn_, query, sql::SqlSchema::GetCatalogsSchema());
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSchemas(
      const flight::ServerCallContext &context, const sql::GetDbSchemas &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetDbSchemasSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetDbSchemas(
      const flight::ServerCallContext &context, const sql::GetDbSchemas &command) {
    std::stringstream query;
    query << "SELECT catalog_name, schema_name AS db_schema_name FROM "
             "information_schema.schemata WHERE 1 = 1";

    query << " AND catalog_name = "
          << (command.catalog.has_value() ? "'" + command.catalog.value() + "'"
                                          : "CURRENT_DATABASE()");
    if (command.db_schema_filter_pattern.has_value()) {
      query << " AND schema_name LIKE '" << command.db_schema_filter_pattern.value()
            << "'";
    }
    query << " ORDER BY catalog_name, db_schema_name";

    return DoGetDuckDBQuery(db_conn_, query.str(), sql::SqlSchema::GetDbSchemasSchema());
  }

  Result<sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const flight::ServerCallContext &context,
      const sql::ActionCreatePreparedStatementRequest &request) {
    std::scoped_lock guard(mutex_);
    std::shared_ptr<DuckDBStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, request.query))
    const std::string handle = GenerateRandomString();
    prepared_statements_[handle] = statement;

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema())

    std::shared_ptr<duckdb::PreparedStatement> stmt = statement->GetDuckDBStmt();
    const id_t parameter_count = stmt->named_param_map.size();
    arrow::FieldVector parameter_fields;
    parameter_fields.reserve(parameter_count);

    duckdb::shared_ptr<duckdb::PreparedStatementData> parameter_data = stmt->data;
    auto bind_parameter_map = parameter_data->value_map;

    for (id_t i = 0; i < parameter_count; i++) {
      std::string parameter_idx_str = std::to_string(i + 1);
      std::string parameter_name = std::string("parameter_") + parameter_idx_str;
      auto parameter_duckdb_type = parameter_data->GetType(parameter_idx_str);
      auto parameter_arrow_type = GetDataTypeFromDuckDbType(parameter_duckdb_type);
      parameter_fields.push_back(field(parameter_name, parameter_arrow_type));
    }

    const std::shared_ptr<arrow::Schema> &parameter_schema =
        arrow::schema(parameter_fields);

    sql::ActionCreatePreparedStatementResult result{.dataset_schema = dataset_schema,
                                                    .parameter_schema = parameter_schema,
                                                    .prepared_statement_handle = handle};

    if (print_queries_) {
      std::cout << "Client running SQL command: \n"
                << request.query << ";\n"
                << std::endl;
    }

    return result;
  }

  Status ClosePreparedStatement(const flight::ServerCallContext &context,
                                const sql::ActionClosePreparedStatementRequest &request) {
    std::scoped_lock guard(mutex_);
    const std::string &prepared_statement_handle = request.prepared_statement_handle;

    if (auto search = prepared_statements_.find(prepared_statement_handle);
        search != prepared_statements_.end()) {
      prepared_statements_.erase(prepared_statement_handle);
    } else {
      return Status::Invalid("Prepared statement not found");
    }

    return Status::OK();
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPreparedStatement(
      const flight::ServerCallContext &context,
      const sql::PreparedStatementQuery &command,
      const flight::FlightDescriptor &descriptor) {
    std::scoped_lock guard(mutex_);
    const std::string &prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<DuckDBStatement> statement = search->second;

    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())

    return GetFlightInfoForCommand(descriptor, schema);
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetPreparedStatement(
      const flight::ServerCallContext &context,
      const sql::PreparedStatementQuery &command) {
    std::scoped_lock guard(mutex_);
    const std::string &prepared_statement_handle = command.prepared_statement_handle;

    auto search = prepared_statements_.find(prepared_statement_handle);
    if (search == prepared_statements_.end()) {
      return Status::Invalid("Prepared statement not found");
    }

    std::shared_ptr<DuckDBStatement> statement = search->second;

    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(statement))

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Status DoPutPreparedStatementQuery(const flight::ServerCallContext &context,
                                     const sql::PreparedStatementQuery &command,
                                     flight::FlightMessageReader *reader,
                                     flight::FlightMetadataWriter *writer) {
    const std::string &prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(auto statement, GetStatementByHandle(prepared_statement_handle))

    ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

    return Status::OK();
  }

  Result<int64_t> DoPutPreparedStatementUpdate(
      const flight::ServerCallContext &context,
      const sql::PreparedStatementUpdate &command, flight::FlightMessageReader *reader) {
    const std::string &prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(auto statement, GetStatementByHandle(prepared_statement_handle))

    ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

    return statement->ExecuteUpdate();
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTables(
      const flight::ServerCallContext &context, const sql::GetTables &command) {
    std::string query = PrepareQueryForGetTables(command);
    std::shared_ptr<DuckDBStatement> statement;
    ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(db_conn_, query))

    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(
                                           statement, sql::SqlSchema::GetTablesSchema()))

    if (command.include_schema) {
      auto table_schema_reader =
          std::make_shared<DuckDBTablesWithSchemaBatchReader>(reader, query, db_conn_);
      return std::make_unique<flight::RecordBatchStream>(table_schema_reader);
    } else {
      return std::make_unique<flight::RecordBatchStream>(reader);
    }
  }

  Result<int64_t> DoPutCommandStatementUpdate(const flight::ServerCallContext &context,
                                              const sql::StatementUpdate &command) {
    const std::string &sql = command.query;
    ARROW_ASSIGN_OR_RAISE(auto db, GetConnection(command.transaction_id))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(db, sql))
    return statement->ExecuteUpdate();
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
      const flight::ServerCallContext &context, const sql::GetTables &command,
      const flight::FlightDescriptor &descriptor) {
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{{descriptor.cmd}, {}}};

    bool include_schema = command.include_schema;

    ARROW_ASSIGN_OR_RAISE(
        auto result,
        flight::FlightInfo::Make(
            include_schema ? *sql::SqlSchema::GetTablesSchemaWithIncludedSchema()
                           : *sql::SqlSchema::GetTablesSchema(),
            descriptor, endpoints, -1, -1))

    return std::make_unique<flight::FlightInfo>(result);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTableTypes(
      const flight::ServerCallContext &context,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetTableTypesSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTableTypes(
      const flight::ServerCallContext &context) {
    std::string query = "SELECT DISTINCT table_type FROM information_schema.tables";

    return DoGetDuckDBQuery(db_conn_, query, sql::SqlSchema::GetTableTypesSchema());
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPrimaryKeys(
      const flight::ServerCallContext &context, const sql::GetPrimaryKeys &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetPrimaryKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetPrimaryKeys(
      const flight::ServerCallContext &context, const sql::GetPrimaryKeys &command) {
    std::stringstream table_query;

    // The field key_name can not be recovered by the sqlite, so it is being set
    // to null following the same pattern for catalog_name and schema_name.
    table_query
        << "SELECT database_name AS catalog_name\n"
           "     , schema_name\n"
           "     , table_name\n"
           "     , column_name\n"
           "     , column_index + 1 AS key_sequence\n"
           "     , constraint_name  AS key_name\n"
           "   FROM (SELECT dc.*\n"
           "              , UNNEST(dc.constraint_column_indexes) AS column_index\n"
           "              , UNNEST(dc.constraint_column_names) AS column_name\n"
           "           FROM duckdb_constraints() AS dc\n"
           "          WHERE constraint_type = 'PRIMARY KEY'\n"
           "        ) WHERE 1 = 1";

    const sql::TableRef &table_ref = command.table_ref;
    table_query << " AND catalog_name = "
                << (table_ref.catalog.has_value() ? "'" + table_ref.catalog.value() + "'"
                                                  : "CURRENT_DATABASE()");

    if (table_ref.db_schema.has_value()) {
      table_query << " and schema_name LIKE '" << table_ref.db_schema.value() << "'";
    }

    table_query << " and table_name LIKE '" << table_ref.table << "'";

    return DoGetDuckDBQuery(db_conn_, table_query.str(),
                            sql::SqlSchema::GetPrimaryKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoImportedKeys(
      const flight::ServerCallContext &context, const sql::GetImportedKeys &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetImportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetImportedKeys(
      const flight::ServerCallContext &context, const sql::GetImportedKeys &command) {
    const sql::TableRef &table_ref = command.table_ref;
    std::string filter = "fk_table_name = '" + table_ref.table + "'";

    filter += " AND fk_catalog_name = " + (table_ref.catalog.has_value()
                                               ? "'" + table_ref.catalog.value() + "'"
                                               : "CURRENT_DATABASE()");
    if (table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetDuckDBQuery(db_conn_, query, sql::SqlSchema::GetImportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoExportedKeys(
      const flight::ServerCallContext &context, const sql::GetExportedKeys &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetExportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetExportedKeys(
      const flight::ServerCallContext &context, const sql::GetExportedKeys &command) {
    const sql::TableRef &table_ref = command.table_ref;
    std::string filter = "pk_table_name = '" + table_ref.table + "'";
    filter += " AND pk_catalog_name = " + (table_ref.catalog.has_value()
                                               ? "'" + table_ref.catalog.value() + "'"
                                               : "CURRENT_DATABASE()");
    if (table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetDuckDBQuery(db_conn_, query, sql::SqlSchema::GetExportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCrossReference(
      const flight::ServerCallContext &context, const sql::GetCrossReference &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetCrossReferenceSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetCrossReference(
      const flight::ServerCallContext &context, const sql::GetCrossReference &command) {
    const sql::TableRef &pk_table_ref = command.pk_table_ref;
    std::string filter = "pk_table_name = '" + pk_table_ref.table + "'";
    filter += " AND pk_catalog_name = " + (pk_table_ref.catalog.has_value()
                                               ? "'" + pk_table_ref.catalog.value() + "'"
                                               : "CURRENT_DATABASE()");
    if (pk_table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = '" + pk_table_ref.db_schema.value() + "'";
    }

    const sql::TableRef &fk_table_ref = command.fk_table_ref;
    filter += " AND fk_table_name = '" + fk_table_ref.table + "'";
    filter += " AND fk_catalog_name = " + (fk_table_ref.catalog.has_value()
                                               ? "'" + fk_table_ref.catalog.value() + "'"
                                               : "CURRENT_DATABASE()");
    if (fk_table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = '" + fk_table_ref.db_schema.value() + "'";
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    return DoGetDuckDBQuery(db_conn_, query, sql::SqlSchema::GetCrossReferenceSchema());
  }

  Result<sql::ActionBeginTransactionResult> BeginTransaction(
      const flight::ServerCallContext &context,
      const sql::ActionBeginTransactionRequest &request) {
    std::string handle = GenerateRandomString();
    auto new_db = std::make_shared<duckdb::Connection>(*db_instance_);
    ARROW_RETURN_NOT_OK(ExecuteSql(new_db, "BEGIN TRANSACTION"));

    std::scoped_lock guard(mutex_);
    open_transactions_[handle] = new_db;
    return sql::ActionBeginTransactionResult{std::move(handle)};
  }

  Status EndTransaction(const flight::ServerCallContext &context,
                        const sql::ActionEndTransactionRequest &request) {
    Status status;
    std::shared_ptr<duckdb::Connection> transaction = nullptr;
    {
      std::scoped_lock guard(mutex_);
      auto it = open_transactions_.find(request.transaction_id);
      if (it == open_transactions_.end()) {
        return Status::KeyError("Unknown transaction ID: ", request.transaction_id);
      }

      if (request.action == sql::ActionEndTransactionRequest::kCommit) {
        status = ExecuteSql(it->second, "COMMIT");
      } else {
        status = ExecuteSql(it->second, "ROLLBACK");
      }
      transaction = it->second;
      open_transactions_.erase(it);
    }
    transaction.reset();
    return status;
  }

  Status ExecuteSql(const std::string &sql) { return ExecuteSql(db_conn_, sql); }

  Status ExecuteSql(std::shared_ptr<duckdb::Connection> db_conn, const std::string &sql) {
    if (std::unique_ptr<duckdb::MaterializedQueryResult> result = db_conn->Query(sql);
        result->HasError()) {
      return Status::Invalid(result->GetError());
    }
    return Status::OK();
  }
};

DuckDBFlightSqlServer::DuckDBFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Result<std::shared_ptr<DuckDBFlightSqlServer>> DuckDBFlightSqlServer::Create(
    const std::string &path, const duckdb::DBConfig &config, const bool &print_queries) {
  std::cout << "DuckDB version: " << duckdb_library_version() << std::endl;

  std::shared_ptr<duckdb::DuckDB> db;
  std::shared_ptr<duckdb::Connection> con;

  db = std::make_shared<duckdb::DuckDB>(path);
  con = std::make_shared<duckdb::Connection>(*db);

  auto impl = std::make_shared<Impl>(db, con, print_queries);
  std::shared_ptr<DuckDBFlightSqlServer> result(new DuckDBFlightSqlServer(impl));

  for (const auto &id_to_result : GetSqlInfoResultMap()) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

Status DuckDBFlightSqlServer::ExecuteSql(const std::string &sql) {
  return impl_->ExecuteSql(sql);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext &context, const sql::StatementQuery &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(
    const flight::ServerCallContext &context, const sql::StatementQueryTicket &command) {
  return impl_->DoGetStatement(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoCatalogs(
    const flight::ServerCallContext &context,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoCatalogs(context, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetCatalogs(
    const flight::ServerCallContext &context) {
  return impl_->DoGetCatalogs(context);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoSchemas(
    const flight::ServerCallContext &context, const sql::GetDbSchemas &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoSchemas(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetDbSchemas(
    const flight::ServerCallContext &context, const sql::GetDbSchemas &command) {
  return impl_->DoGetDbSchemas(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTables(
    const flight::ServerCallContext &context, const sql::GetTables &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoTables(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(
    const flight::ServerCallContext &context, const sql::GetTables &command) {
  return impl_->DoGetTables(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoTableTypes(
    const flight::ServerCallContext &context,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoTableTypes(context, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetTableTypes(
    const flight::ServerCallContext &context) {
  return impl_->DoGetTableTypes(context);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(
    const flight::ServerCallContext &context, const sql::StatementUpdate &command) {
  return impl_->DoPutCommandStatementUpdate(context, command);
}

Result<sql::ActionCreatePreparedStatementResult>
DuckDBFlightSqlServer::CreatePreparedStatement(
    const flight::ServerCallContext &context,
    const sql::ActionCreatePreparedStatementRequest &request) {
  return impl_->CreatePreparedStatement(context, request);
}

Status DuckDBFlightSqlServer::ClosePreparedStatement(
    const flight::ServerCallContext &context,
    const sql::ActionClosePreparedStatementRequest &request) {
  return impl_->ClosePreparedStatement(context, request);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
    const flight::ServerCallContext &context, const sql::PreparedStatementQuery &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetPreparedStatement(
    const flight::ServerCallContext &context,
    const sql::PreparedStatementQuery &command) {
  return impl_->DoGetPreparedStatement(context, command);
}

Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(
    const flight::ServerCallContext &context, const sql::PreparedStatementQuery &command,
    flight::FlightMessageReader *reader, flight::FlightMetadataWriter *writer) {
  return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutPreparedStatementUpdate(
    const flight::ServerCallContext &context, const sql::PreparedStatementUpdate &command,
    flight::FlightMessageReader *reader) {
  return impl_->DoPutPreparedStatementUpdate(context, command, reader);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoPrimaryKeys(
    const flight::ServerCallContext &context, const sql::GetPrimaryKeys &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetPrimaryKeys(
    const flight::ServerCallContext &context, const sql::GetPrimaryKeys &command) {
  return impl_->DoGetPrimaryKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoImportedKeys(
    const flight::ServerCallContext &context, const sql::GetImportedKeys &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetImportedKeys(const flight::ServerCallContext &context,
                                         const sql::GetImportedKeys &command) {
  return impl_->DoGetImportedKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoExportedKeys(
    const flight::ServerCallContext &context, const sql::GetExportedKeys &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetExportedKeys(const flight::ServerCallContext &context,
                                         const sql::GetExportedKeys &command) {
  return impl_->DoGetExportedKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoCrossReference(
    const flight::ServerCallContext &context, const sql::GetCrossReference &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoCrossReference(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetCrossReference(const flight::ServerCallContext &context,
                                           const sql::GetCrossReference &command) {
  return impl_->DoGetCrossReference(context, command);
}

Result<sql::ActionBeginTransactionResult> DuckDBFlightSqlServer::BeginTransaction(
    const flight::ServerCallContext &context,
    const sql::ActionBeginTransactionRequest &request) {
  return impl_->BeginTransaction(context, request);
}

Status DuckDBFlightSqlServer::EndTransaction(
    const flight::ServerCallContext &context,
    const sql::ActionEndTransactionRequest &request) {
  return impl_->EndTransaction(context, request);
}

}  // namespace gizmosql::ddb
