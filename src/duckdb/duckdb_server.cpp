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
#include <mutex>
#include <chrono>
#include <iomanip>
#include <thread>

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/types.h>
#include <arrow/util/logging.h>

#include "duckdb_sql_info.h"
#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"
#include "duckdb_type_info.h"
#include "duckdb_tables_schema_batch_reader.h"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/main/prepared_statement_data.hpp"
#include "flight_sql_fwd.h"
#include <jwt-cpp/jwt.h>

using arrow::Result;
using arrow::Status;

namespace sql = flight::sql;

namespace gizmosql::ddb {

// Helper function to get current timestamp in ISO8601 format
std::string GetISO8601Timestamp() {
  auto now = std::chrono::system_clock::now();
  auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
  auto now_s = now_ns / 1000000000;
  auto now_ns_part = now_ns % 1000000000;
  
  std::time_t time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm* tm_now = std::gmtime(&time_t_now);
  
  std::ostringstream oss;
  oss << std::put_time(tm_now, "%Y-%m-%dT%H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(9) << now_ns_part << "Z";
  return oss.str();
}

// Helper function to get thread name
std::string GetThreadName() {
  std::ostringstream oss;
  oss << "thread-" << std::this_thread::get_id();
  return oss.str();
}

// Helper function for consistent logging with level checking
void LogMessage(const std::string &level, const std::string &message, 
                const std::string &log_format) {
  // Check if the log level is enabled using Arrow's log level
  arrow::util::ArrowLogLevel arrow_level;
  if (level == "ERROR") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_ERROR;
  } else if (level == "WARNING") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_WARNING;
  } else if (level == "INFO") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_INFO;
  } else {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_DEBUG;
  }
  
  // Only log if the level is enabled
  if (!arrow::util::ArrowLog::IsLevelEnabled(arrow_level)) {
    return;
  }
  
  if (log_format == "json") {
    std::cout << R"({)"
              << R"("@timestamp":")" << GetISO8601Timestamp() << R"(",)"
              << R"("@version":"1",)"
              << R"("message":")" << message << R"(",)"
              << R"("logger_name":"duckdb_server",)"
              << R"("thread_name":")" << GetThreadName() << R"(",)"
              << R"("level":")" << level << R"(")"
              << R"(})" << std::endl;
  } else {
    if (level == "ERROR") {
      ARROW_LOG(ERROR) << message;
    } else if (level == "WARNING") {
      ARROW_LOG(WARNING) << message;
    } else {
      ARROW_LOG(INFO) << message;
    }
  }
}

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

Status SetParametersOnDuckDBStatement(const std::shared_ptr<DuckDBStatement> &stmt,
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
    const std::shared_ptr<duckdb::Connection> &connection, const std::string &query,
    const std::shared_ptr<arrow::Schema> &schema) {
  std::shared_ptr<DuckDBStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(connection, query))

  std::shared_ptr<DuckDBStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, DuckDBStatementBatchReader::Create(statement, schema))

  return std::make_unique<flight::RecordBatchStream>(reader);
}

Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoForCommand(
    const flight::FlightDescriptor &descriptor,
    const std::shared_ptr<arrow::Schema> &schema) {
  std::vector<flight::FlightEndpoint> endpoints{
      flight::FlightEndpoint{flight::Ticket{descriptor.cmd}, {}, std::nullopt, ""}};
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
  bool print_queries_;
  std::string log_format_;
  std::map<std::string, std::shared_ptr<DuckDBStatement>> prepared_statements_;
  std::unordered_map<std::string, std::shared_ptr<duckdb::Connection>> open_sessions_;
  std::unordered_map<std::string, std::string> open_transactions_;
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

  // A function to get the auth token from the context
  static Result<std::string> GetAuthToken(const flight::ServerCallContext &context) {
    auto incoming_headers = context.incoming_headers();
    auto it = incoming_headers.find("authorization");
    if (it == incoming_headers.end()) {
      return Status::KeyError("Authorization header not found");
    }

    auto auth_header = it->second;
    std::vector<std::string> parts;
    boost::split(parts, auth_header, boost::is_any_of(" "));
    if (parts.size() != 2) {
      return Status::Invalid("Malformed authorization header");
    }

    return parts[1];
  }

  // A function to get the JSON from the decoded auth token
  static Result<std::string> GetAuthTokenKeyValue(const std::string &auth_token,
                                                  const std::string &key) {
    try {
      auto decoded = jwt::decode(auth_token);

      for (auto &e : decoded.get_payload_json()) {
        if (e.first == key) {
          return e.second.to_str();
        }
      }
      return Status::KeyError("Key: '", key, "' not found in token");
    } catch (const std::exception &e) {
      return Status::Invalid("Error decoding JWT token: ", e.what());
    }
  }

  static Result<std::string> GetSessionID(const flight::ServerCallContext &context) {
    auto auth_token = GetAuthToken(context);
    if (!auth_token.ok()) {
      return auth_token.status();
    }

    auto session_id = GetAuthTokenKeyValue(auth_token.ValueOrDie(), "session_id");
    if (!session_id.ok()) {
      return session_id.status();
    }

    return session_id.ValueOrDie();
  }

  // We get a new session per thread to be thread-safe...
  Result<std::shared_ptr<duckdb::Connection>> GetConnection(
      const flight::ServerCallContext &context) {
    ARROW_ASSIGN_OR_RAISE(auto session_id, GetSessionID(context));

    std::scoped_lock guard(mutex_);
    auto it = open_sessions_.find(session_id);
    if (it != open_sessions_.end()) {
      return it->second;
    } else {
      auto connection = std::make_shared<duckdb::Connection>(*db_instance_);
      open_sessions_[session_id] = connection;
      return connection;
    }
  }

  // Create a Ticket that combines a query and a transaction ID.
  static Result<flight::Ticket> EncodeTransactionQuery(
      const std::string &query, const std::string &transaction_id) {
    std::string transaction_query = transaction_id;
    transaction_query += ':';
    transaction_query += query;
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                          sql::CreateStatementQueryTicket(transaction_query));
    return flight::Ticket{std::move(ticket_string)};
  }

  static Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
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
  explicit Impl(std::shared_ptr<duckdb::DuckDB> db_instance, const bool &print_queries,
                const std::string &log_format)
      : db_instance_(std::move(db_instance)), print_queries_(print_queries),
        log_format_(log_format) {}

  ~Impl() = default;

  static std::string GenerateRandomString(int length = 16) {
    constexpr char charset[] =
        "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    constexpr int charsetLength = sizeof(charset) - 1;

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
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(connection, query))
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())
    ARROW_ASSIGN_OR_RAISE(auto ticket,
                          EncodeTransactionQuery(query, command.transaction_id))
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{std::move(ticket), {}, std::nullopt, ""}};
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
    
    if (print_queries_) {
      LogMessage("INFO", "Client executing SQL statement: " + sql + " [component=duckdb_server]", log_format_);
    }
    
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(connection, sql))
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context))
    return DoGetDuckDBQuery(connection, query, sql::SqlSchema::GetCatalogsSchema());
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context))
    return DoGetDuckDBQuery(connection, query.str(),
                            sql::SqlSchema::GetDbSchemasSchema());
  }

  Result<sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const flight::ServerCallContext &context,
      const sql::ActionCreatePreparedStatementRequest &request) {
    std::shared_ptr<DuckDBStatement> statement;

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(connection, request.query))
    std::scoped_lock guard(mutex_);
    const std::string handle = GenerateRandomString();
    prepared_statements_[handle] = statement;

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema())

    std::shared_ptr<duckdb::PreparedStatement> stmt = statement->GetDuckDBStmt();
    arrow::FieldVector parameter_fields;
    id_t parameter_count = 0;
    
    if (stmt != nullptr) {
      // Traditional prepared statement - extract parameter information
      parameter_count = stmt->named_param_map.size();
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
    }
    // For direct execution mode (stmt == nullptr), we have no parameters

    const std::shared_ptr<arrow::Schema> &parameter_schema =
        arrow::schema(parameter_fields);

    sql::ActionCreatePreparedStatementResult result{.dataset_schema = dataset_schema,
                                                    .parameter_schema = parameter_schema,
                                                    .prepared_statement_handle = handle};

    if (print_queries_) {
      LogMessage("INFO", "Client running SQL command: " + request.query + " [component=duckdb_server]", log_format_);
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
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    ARROW_ASSIGN_OR_RAISE(statement, DuckDBStatement::Create(connection, query))

    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(
                                           statement, sql::SqlSchema::GetTablesSchema()))

    if (command.include_schema) {
      auto table_schema_reader =
          std::make_shared<DuckDBTablesWithSchemaBatchReader>(reader, query, connection);
      return std::make_unique<flight::RecordBatchStream>(table_schema_reader);
    } else {
      return std::make_unique<flight::RecordBatchStream>(reader);
    }
  }

  Result<int64_t> DoPutCommandStatementUpdate(const flight::ServerCallContext &context,
                                              const sql::StatementUpdate &command) {
    const std::string &sql = command.query;
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context))
    ARROW_ASSIGN_OR_RAISE(auto statement, DuckDBStatement::Create(connection, sql))
    return statement->ExecuteUpdate();
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTypeInfo(
      const flight::ServerCallContext &context, const sql::GetXdbcTypeInfo &command,
      const flight::FlightDescriptor &descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetXdbcTypeInfoSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTypeInfo(
      const flight::ServerCallContext &context, const sql::GetXdbcTypeInfo &command) {
    ARROW_ASSIGN_OR_RAISE(auto type_info_result,
                          command.data_type.has_value()
                              ? DoGetTypeInfoResult(command.data_type.value())
                              : DoGetTypeInfoResult());

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::RecordBatchReader::Make({type_info_result}));
    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
      const flight::ServerCallContext &context, const sql::GetTables &command,
      const flight::FlightDescriptor &descriptor) {
    std::vector<flight::FlightEndpoint> endpoints{
        flight::FlightEndpoint{flight::Ticket{descriptor.cmd}, {}, std::nullopt, ""}};

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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    return DoGetDuckDBQuery(connection, query, sql::SqlSchema::GetTableTypesSchema());
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    return DoGetDuckDBQuery(connection, table_query.str(),
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    return DoGetDuckDBQuery(connection, query, sql::SqlSchema::GetImportedKeysSchema());
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    return DoGetDuckDBQuery(connection, query, sql::SqlSchema::GetExportedKeysSchema());
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

    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    return DoGetDuckDBQuery(connection, query, sql::SqlSchema::GetCrossReferenceSchema());
  }

  Result<sql::ActionBeginTransactionResult> BeginTransaction(
      const flight::ServerCallContext &context,
      const sql::ActionBeginTransactionRequest &request) {
    std::string handle = GenerateRandomString();
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    std::scoped_lock guard(mutex_);
    open_transactions_[handle] = "";

    ARROW_RETURN_NOT_OK(ExecuteSql(connection, "BEGIN TRANSACTION"));

    return sql::ActionBeginTransactionResult{std::move(handle)};
  }

  Status EndTransaction(const flight::ServerCallContext &context,
                        const sql::ActionEndTransactionRequest &request) {
    Status status;
    ARROW_ASSIGN_OR_RAISE(auto connection, GetConnection(context));
    {
      if (request.action == sql::ActionEndTransactionRequest::kCommit) {
        status = ExecuteSql(connection, "COMMIT");
      } else {
        status = ExecuteSql(connection, "ROLLBACK");
      }
      open_transactions_.erase(request.transaction_id);
    }
    return status;
  }

  Result<flight::CloseSessionResult> CloseSession(
      const flight::ServerCallContext &context,
      const flight::CloseSessionRequest &request) {
    ARROW_ASSIGN_OR_RAISE(auto session_id, GetSessionID(context));
    auto it = open_sessions_.find(session_id);
    if (it != open_sessions_.end()) {
      it->second.reset();
      open_sessions_.erase(it);
      return flight::CloseSessionResult(flight::CloseSessionStatus::kClosed);
    } else {
      return Status::KeyError("Session: '" + session_id + "' not found");
    }
  }

  static Status ExecuteSql(const std::shared_ptr<duckdb::Connection> &connection,
                           const std::string &sql) {
    if (std::unique_ptr<duckdb::MaterializedQueryResult> result = connection->Query(sql);
        result->HasError()) {
      return Status::Invalid(result->GetError());
    }
    return Status::OK();
  }

  Status ExecuteSql(const std::string &sql) {
    // We do not have a call context, so just grab a new connection to the instance
    auto connection = std::make_shared<duckdb::Connection>(*db_instance_);
    return ExecuteSql(connection, sql);
  }

  // Utility function to execute a query and return string results from the first column
  Result<std::vector<std::string>> ExecuteSqlAndGetStringVector(const std::string &sql) {
    auto connection = std::make_shared<duckdb::Connection>(*db_instance_);
    std::unique_ptr<duckdb::MaterializedQueryResult> result = connection->Query(sql);
    
    if (result->HasError()) {
      return Status::Invalid("SQL query failed: ", result->GetError());
    }
    
    std::vector<std::string> string_results;
    
    // Extract string values from the first column of all rows
    for (size_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
      auto value = result->GetValue(0, row_idx);  // First column
      if (!value.IsNull()) {
        string_results.push_back(value.ToString());
      }
    }
    
    return string_results;
  }
};

DuckDBFlightSqlServer::DuckDBFlightSqlServer(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

Result<std::shared_ptr<DuckDBFlightSqlServer>> DuckDBFlightSqlServer::Create(
    const std::string &path, const bool &read_only, const bool &print_queries,
    const std::string &log_format) {
  LogMessage("INFO", "DuckDB version: " + std::string(duckdb_library_version()) + " [component=duckdb_server]", log_format);

  bool in_memory = path == ":memory:";
  char *db_location;

  if (in_memory) {
    db_location = nullptr;
  } else {
    db_location = const_cast<char *>(path.c_str());
  }

  duckdb::DBConfig config;
  if (read_only) {
    config.options.access_mode = duckdb::AccessMode::READ_ONLY;
  }

  auto db = std::make_shared<duckdb::DuckDB>(db_location, &config);

  auto impl = std::make_shared<Impl>(db, print_queries, log_format);
  std::shared_ptr<DuckDBFlightSqlServer> result(new DuckDBFlightSqlServer(impl));

  // Use dynamic SQL info that queries DuckDB for keywords and functions
  for (const auto &id_to_result : GetSqlInfoResultMap(result.get())) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

Status DuckDBFlightSqlServer::ExecuteSql(const std::string &sql) const {
  return impl_->ExecuteSql(sql);
}

Result<std::vector<std::string>> DuckDBFlightSqlServer::ExecuteSqlAndGetStringVector(const std::string &sql) const {
  return impl_->ExecuteSqlAndGetStringVector(sql);
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
DuckDBFlightSqlServer::GetFlightInfoXdbcTypeInfo(
    const flight::ServerCallContext &context, const flight::sql::GetXdbcTypeInfo &command,
    const flight::FlightDescriptor &descriptor) {
  return impl_->GetFlightInfoTypeInfo(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetXdbcTypeInfo(const flight::ServerCallContext &context,
                                         const flight::sql::GetXdbcTypeInfo &command) {
  return impl_->DoGetTypeInfo(context, command);
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

Result<flight::CloseSessionResult> DuckDBFlightSqlServer::CloseSession(
    const flight::ServerCallContext &context,
    const flight::CloseSessionRequest &request) {
  return impl_->CloseSession(context, request);
}

}  // namespace gizmosql::ddb
