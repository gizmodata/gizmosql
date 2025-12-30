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
#include <shared_mutex>

#include <arrow/api.h>
#include <arrow/flight/server.h>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/types.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <duckdb/main/prepared_statement.hpp>
#include <duckdb/main/prepared_statement_data.hpp>
#include <jwt-cpp/jwt.h>

#include "duckdb_sql_info.h"
#include "duckdb_statement.h"
#include "duckdb_statement_batch_reader.h"
#include "duckdb_type_info.h"
#include "duckdb_tables_schema_batch_reader.h"
#include "gizmosql_security.h"
#include "gizmosql_logging.h"
#include "flight_sql_fwd.h"
#include "session_context.h"
#include "request_ctx.h"

using arrow::Result;
using arrow::Status;

namespace sql = flight::sql;

namespace gizmosql::ddb {
namespace {

class DuckDBTransactionGuard {
 public:
  explicit DuckDBTransactionGuard(duckdb::Connection& conn)
      : conn_(conn), active_(true), committed_(false) {
    conn_.BeginTransaction();
  }

  ~DuckDBTransactionGuard() {
    if (active_ && !committed_) {
      try {
        conn_.Rollback();
      } catch (...) {
        // Swallow exceptions in destructor; nothing we can do here.
      }
    }
  }

  arrow::Status Commit() {
    if (!active_) {
      return arrow::Status::Invalid("Transaction already finished");
    }
    if (committed_) {
      return arrow::Status::Invalid("Transaction already committed");
    }
    conn_.Commit();
    committed_ = true;
    active_ = false;
    return arrow::Status::OK();
  }

 private:
  duckdb::Connection& conn_;
  bool active_;
  bool committed_;
};

duckdb::LogicalType GetDuckDBTypeFromArrowType(
    const std::shared_ptr<arrow::DataType>& arrow_type) {
  using arrow::Type;

  switch (arrow_type->id()) {
    case Type::INT8:
      return duckdb::LogicalType::TINYINT;
    case Type::INT16:
      return duckdb::LogicalType::SMALLINT;
    case Type::INT32:
      return duckdb::LogicalType::INTEGER;
    case Type::INT64:
      return duckdb::LogicalType::BIGINT;

    case Type::UINT8:
      return duckdb::LogicalType::UTINYINT;
    case Type::UINT16:
      return duckdb::LogicalType::USMALLINT;
    case Type::UINT32:
      return duckdb::LogicalType::UINTEGER;
    case Type::UINT64:
      return duckdb::LogicalType::UBIGINT;

    case Type::FLOAT:
      return duckdb::LogicalType::FLOAT;
    case Type::DOUBLE:
      return duckdb::LogicalType::DOUBLE;

    case Type::BOOL:
      return duckdb::LogicalType::BOOLEAN;

    case Type::STRING:
    case Type::LARGE_STRING:
      return duckdb::LogicalType::VARCHAR;

    case Type::BINARY:
    case Type::LARGE_BINARY:
      return duckdb::LogicalType::BLOB;

    case Type::DATE32:
    case Type::DATE64:
      return duckdb::LogicalType::DATE;

    case Type::TIME32:
    case Type::TIME64:
      return duckdb::LogicalType::TIME;

    case Type::TIMESTAMP: {
      auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arrow_type);
      switch (ts_type->unit()) {
        case arrow::TimeUnit::SECOND:
          return duckdb::LogicalType::TIMESTAMP_S;
        case arrow::TimeUnit::MILLI:
          return duckdb::LogicalType::TIMESTAMP_MS;
        case arrow::TimeUnit::MICRO:
          return duckdb::LogicalType::TIMESTAMP;
        case arrow::TimeUnit::NANO:
          return duckdb::LogicalType::TIMESTAMP_NS;
      }
      return duckdb::LogicalType::TIMESTAMP;
    }

    case Type::DURATION:
      return duckdb::LogicalType::INTERVAL;

    case Type::DECIMAL128: {
      auto dec_type = std::static_pointer_cast<arrow::Decimal128Type>(arrow_type);
      return duckdb::LogicalType::DECIMAL(dec_type->precision(), dec_type->scale());
    }

    case Type::DECIMAL256: {
      auto dec_type = std::static_pointer_cast<arrow::Decimal256Type>(arrow_type);
      return duckdb::LogicalType::DECIMAL(dec_type->precision(), dec_type->scale());
    }

    case Type::NA:
      return duckdb::LogicalType::SQLNULL;

    default:
      return duckdb::LogicalType::VARCHAR;  // safe fallback
  }
}

std::string QuoteIdent(const std::string& name) {
  // simple double-quote + escape internal quotes
  std::string q = "\"";
  for (char c : name) {
    if (c == '"')
      q += "\"\"";
    else
      q += c;
  }
  q += "\"";
  return q;
}

Result<bool> TableExists(duckdb::Connection& conn,
                         const std::optional<std::string>& catalog_name,
                         const std::optional<std::string>& schema_name,
                         const std::string& table_name) {
  duckdb::vector<duckdb::Value> bind_parameters;

  std::string sql =
      "SELECT 1 "
      "FROM information_schema.tables "
      "WHERE 1 = 1 ";

  if (catalog_name.has_value()) {
    sql += "AND table_catalog = ? ";
    bind_parameters.emplace_back(catalog_name.value());
  } else {
    sql += "AND table_catalog = CURRENT_DATABASE() ";
  }

  if (schema_name.has_value()) {
    sql += "AND table_schema = ? ";
    bind_parameters.emplace_back(schema_name.value());
  } else {
    sql += "AND table_schema = CURRENT_SCHEMA() ";
  }

  sql +=
      "  AND table_name = ? "
      "LIMIT 1";
  bind_parameters.emplace_back(table_name);

  auto table_exists_statement = conn.Prepare(sql);
  auto query_result = table_exists_statement->Execute(bind_parameters);
  if (query_result->HasError()) {
    return Status::Invalid("DuckDB metadata query failed: " + query_result->GetError());
  }
  auto result_set = query_result->Fetch();
  return result_set && result_set->size() > 0;
}

Result<std::string> GenerateCreateTableSQLFromArrowSchema(
    const std::string& full_table_name, const std::shared_ptr<arrow::Schema>& schema,
    const bool& temporary, const bool& replace = false) {
  std::ostringstream oss;

  oss << "CREATE " << (replace ? "OR REPLACE " : "") << (temporary ? "TEMPORARY " : "")
      << "TABLE " << full_table_name << " (\n";

  for (int i = 0; i < schema->num_fields(); ++i) {
    const auto& field = schema->field(i);

    if (i > 0) {
      oss << ",\n";
    }

    const std::string col_name = QuoteIdent(field->name());
    const auto col_type = GetDuckDBTypeFromArrowType(field->type()).ToString();

    oss << "    " << col_name << " " << col_type;
    if (!field->nullable()) {
      oss << " NOT NULL";
    }
  }

  oss << "\n);";
  return oss.str();
}

duckdb::Value ConvertArrowCellToDuckDBValue(const std::shared_ptr<arrow::Array>& arr,
                                            int64_t row) {
  using arrow::Type;

  if (!arr || arr->IsNull(row)) {
    // NULL value
    return duckdb::Value();  // NULL
  }

  switch (arr->type_id()) {
    // -------- BOOL --------
    case Type::BOOL: {
      auto typed = std::static_pointer_cast<arrow::BooleanArray>(arr);
      return duckdb::Value::BOOLEAN(typed->Value(row));
    }

    // -------- SIGNED INTS --------
    case Type::INT8: {
      auto typed = std::static_pointer_cast<arrow::Int8Array>(arr);
      return duckdb::Value::TINYINT(typed->Value(row));
    }
    case Type::INT16: {
      auto typed = std::static_pointer_cast<arrow::Int16Array>(arr);
      return duckdb::Value::SMALLINT(typed->Value(row));
    }
    case Type::INT32: {
      auto typed = std::static_pointer_cast<arrow::Int32Array>(arr);
      return duckdb::Value::INTEGER(typed->Value(row));
    }
    case Type::INT64: {
      auto typed = std::static_pointer_cast<arrow::Int64Array>(arr);
      return duckdb::Value::BIGINT(typed->Value(row));
    }

    // -------- UNSIGNED INTS --------
    case Type::UINT8: {
      auto typed = std::static_pointer_cast<arrow::UInt8Array>(arr);
      return duckdb::Value::UTINYINT(typed->Value(row));
    }
    case Type::UINT16: {
      auto typed = std::static_pointer_cast<arrow::UInt16Array>(arr);
      return duckdb::Value::USMALLINT(typed->Value(row));
    }
    case Type::UINT32: {
      auto typed = std::static_pointer_cast<arrow::UInt32Array>(arr);
      return duckdb::Value::UINTEGER(typed->Value(row));
    }
    case Type::UINT64: {
      auto typed = std::static_pointer_cast<arrow::UInt64Array>(arr);
      return duckdb::Value::UBIGINT(typed->Value(row));
    }

    // -------- FLOATS --------
    case Type::FLOAT: {
      auto typed = std::static_pointer_cast<arrow::FloatArray>(arr);
      return duckdb::Value::FLOAT(typed->Value(row));
    }
    case Type::DOUBLE: {
      auto typed = std::static_pointer_cast<arrow::DoubleArray>(arr);
      return duckdb::Value::DOUBLE(typed->Value(row));
    }

    // -------- STRINGS --------
    case Type::STRING:
    case Type::LARGE_STRING: {
      auto typed = std::static_pointer_cast<arrow::StringArray>(arr);
      auto view = typed->GetView(row);
      // Arrow strings are NOT guaranteed null-terminated; use length-aware ctor.
      return duckdb::Value(std::string(view.data(), view.size()));
    }

    // -------- BINARY (store as BLOB or VARCHAR, here: BLOB) --------
    case Type::BINARY:
    case Type::LARGE_BINARY: {
      auto typed = std::static_pointer_cast<arrow::BinaryArray>(arr);
      int32_t len = 0;
      const uint8_t* data = typed->GetValue(row, &len);
      return duckdb::Value::BLOB(data, len);
    }

    // -------- DATE --------
    case Type::DATE32: {
      auto typed = std::static_pointer_cast<arrow::Date32Array>(arr);
      int32_t days = typed->Value(row);  // days since 1970-01-01
      int64_t epoch_seconds = static_cast<int64_t>(days) * 86400;
      duckdb::date_t d = duckdb::Date::EpochToDate(epoch_seconds);
      return duckdb::Value::DATE(d);
    }
    case Type::DATE64: {
      auto typed = std::static_pointer_cast<arrow::Date64Array>(arr);
      int64_t ms = typed->Value(row);  // ms since 1970-01-01
      int64_t epoch_seconds = ms / 1000;
      duckdb::date_t d = duckdb::Date::EpochToDate(epoch_seconds);
      return duckdb::Value::DATE(d);
    }

    // -------- TIME (TIME32/TIME64 -> microseconds since midnight) --------
    case Type::TIME32: {
      auto typed = std::static_pointer_cast<arrow::Time32Array>(arr);
      int32_t v = typed->Value(row);
      auto time_type = std::static_pointer_cast<arrow::Time32Type>(arr->type());

      int64_t micros = 0;
      switch (time_type->unit()) {
        case arrow::TimeUnit::SECOND:
          micros = static_cast<int64_t>(v) * 1'000'000;
          break;
        case arrow::TimeUnit::MILLI:
          micros = static_cast<int64_t>(v) * 1'000;
          break;
        default:
          throw std::runtime_error(
              "Unsupported Time32 unit for Arrow -> DuckDB conversion");
      }

      int64_t sec_total = micros / 1'000'000;
      int64_t usec = micros % 1'000'000;
      int64_t hour = sec_total / 3600;
      int64_t min = (sec_total / 60) % 60;
      int64_t sec = sec_total % 60;

      return duckdb::Value::TIME(static_cast<int32_t>(hour), static_cast<int32_t>(min),
                                 static_cast<int32_t>(sec), static_cast<int32_t>(usec));
    }

    case Type::TIME64: {
      auto typed = std::static_pointer_cast<arrow::Time64Array>(arr);
      int64_t v = typed->Value(row);
      auto time_type = std::static_pointer_cast<arrow::Time64Type>(arr->type());

      int64_t micros = 0;
      switch (time_type->unit()) {
        case arrow::TimeUnit::MICRO:
          micros = v;
          break;
        case arrow::TimeUnit::NANO:
          micros = v / 1000;
          break;  // truncate
        default:
          throw std::runtime_error(
              "Unsupported Time64 unit for Arrow -> DuckDB conversion");
      }

      int64_t sec_total = micros / 1'000'000;
      int64_t usec = micros % 1'000'000;
      int64_t hour = sec_total / 3600;
      int64_t min = (sec_total / 60) % 60;
      int64_t sec = sec_total % 60;

      return duckdb::Value::TIME(static_cast<int32_t>(hour), static_cast<int32_t>(min),
                                 static_cast<int32_t>(sec), static_cast<int32_t>(usec));
    }

    // -------- TIMESTAMP --------
    case Type::TIMESTAMP: {
      auto typed = std::static_pointer_cast<arrow::TimestampArray>(arr);
      int64_t v = typed->Value(row);
      auto ts_type = std::static_pointer_cast<arrow::TimestampType>(arr->type());

      int64_t micros_since_epoch = 0;
      switch (ts_type->unit()) {
        case arrow::TimeUnit::SECOND:
          micros_since_epoch = v * 1'000'000;
          break;
        case arrow::TimeUnit::MILLI:
          micros_since_epoch = v * 1'000;
          break;
        case arrow::TimeUnit::MICRO:
          micros_since_epoch = v;
          break;
        case arrow::TimeUnit::NANO:
          micros_since_epoch = v / 1'000;
          break;
        default:
          throw std::runtime_error(
              "Unsupported Timestamp unit for Arrow -> DuckDB conversion");
      }

      duckdb::timestamp_t ts(micros_since_epoch);
      return duckdb::Value::TIMESTAMP(ts);
    }

    // -------- DECIMAL128 -> DuckDB DECIMAL --------
    case arrow::Type::DECIMAL128: {
      auto typed = std::static_pointer_cast<arrow::Decimal128Array>(arr);
      auto dec_type = std::static_pointer_cast<arrow::Decimal128Type>(arr->type());
      int32_t scale = dec_type->scale();

      // String representation with the scale already applied, e.g. "123.45"
      std::string s = typed->FormatValue(row);

      // Parse to double – yes, this loses exactness but is simple + robust
      double d = std::stod(s);

      return duckdb::Value::DOUBLE(d);
    }

    // -------- FALLBACK: string representation --------
    default: {
      auto scalar_res = arr->GetScalar(row);
      if (!scalar_res.ok()) {
        throw std::runtime_error("Failed to get Arrow scalar for unsupported type: " +
                                 arr->type()->ToString());
      }
      auto scalar = scalar_res.ValueOrDie();
      return duckdb::Value(scalar->ToString());
    }
  }
}

arrow::Status AppendRecordBatchToDuckDB(
    duckdb::Appender& appender, const std::shared_ptr<arrow::RecordBatch>& batch) {
  if (!batch) {
    return arrow::Status::OK();
  }

  const int64_t nrows = batch->num_rows();
  const int ncols = batch->num_columns();

  for (int64_t row = 0; row < nrows; ++row) {
    appender.BeginRow();

    for (int col = 0; col < ncols; ++col) {
      const auto& arr = batch->column(col);
      duckdb::Value v = ConvertArrowCellToDuckDBValue(arr, row);
      appender.Append(v);
    }

    appender.EndRow();
  }

  return arrow::Status::OK();
}

std::string PrepareQueryForGetTables(const sql::GetTables& command,
                                     duckdb::vector<duckdb::Value>& bind_parameters) {
  std::stringstream table_query;

  table_query << "SELECT table_catalog as catalog_name, table_schema as db_schema_name, "
                 "table_name, "
                 "table_type FROM information_schema.tables where 1=1";

  table_query << " and table_catalog = ";
  if (command.catalog.has_value()) {
    table_query << "?";
    bind_parameters.emplace_back(command.catalog.value());
  } else {
    table_query << "CURRENT_DATABASE()";
  }

  if (command.db_schema_filter_pattern.has_value()) {
    table_query << " and table_schema LIKE ?";
    bind_parameters.emplace_back(command.db_schema_filter_pattern.value());
  }

  if (command.table_name_filter_pattern.has_value()) {
    table_query << " and table_name LIKE ?";
    bind_parameters.emplace_back(command.table_name_filter_pattern.value());
  }

  if (!command.table_types.empty()) {
    table_query << " and table_type IN (";
    const size_t size = command.table_types.size();
    for (size_t i = 0; i < size; i++) {
      table_query << "?";
      bind_parameters.emplace_back(command.table_types[i]);
      if (size - 1 != i) {
        table_query << ",";
      }
    }

    table_query << ")";
  }

  table_query << " order by table_name";
  return table_query.str();
}

Status SetParametersOnDuckDBStatement(const std::shared_ptr<DuckDBStatement>& stmt,
                                      flight::FlightMessageReader* reader) {
  while (true) {
    ARROW_ASSIGN_OR_RAISE(flight::FlightStreamChunk chunk, reader->Next())
    const std::shared_ptr<arrow::RecordBatch>& record_batch = chunk.data;
    if (record_batch == nullptr) break;

    const int64_t num_rows = record_batch->num_rows();
    const int& num_columns = record_batch->num_columns();

    for (int row_index = 0; row_index < num_rows; ++row_index) {
      for (int column_index = 0; column_index < num_columns; ++column_index) {
        const std::shared_ptr<arrow::Array>& column = record_batch->column(column_index);
        ARROW_ASSIGN_OR_RAISE(const std::shared_ptr<arrow::Scalar> scalar,
                              column->GetScalar(row_index))

        stmt->bind_parameters.emplace_back(scalar->ToString());
      }
    }
  }

  return Status::OK();
}

Result<std::unique_ptr<flight::FlightDataStream>> DoGetDuckDBQuery(
    const std::shared_ptr<ClientSession>& client_session, const std::string& query,
    const std::shared_ptr<arrow::Schema>& schema,
    const duckdb::vector<duckdb::Value>& bind_parameters, const bool& print_queries,
    const int32_t& query_timeout) {
  std::shared_ptr<DuckDBStatement> statement;

  ARROW_ASSIGN_OR_RAISE(statement,
                        DuckDBStatement::Create(client_session, query,
                                                arrow::util::ArrowLogLevel::ARROW_DEBUG,
                                                print_queries, schema))
  statement->bind_parameters = bind_parameters;

  std::shared_ptr<DuckDBStatementBatchReader> reader;
  ARROW_ASSIGN_OR_RAISE(reader, DuckDBStatementBatchReader::Create(statement, schema))

  return std::make_unique<flight::RecordBatchStream>(reader);
}

// Convenience method to omit bind variables
Result<std::unique_ptr<flight::FlightDataStream>> DoGetDuckDBQuery(
    const std::shared_ptr<ClientSession>& client_session, const std::string& query,
    const std::shared_ptr<arrow::Schema>& schema, const bool& print_queries,
    const int32_t& query_timeout) {
  const duckdb::vector<duckdb::Value> bind_parameters;
  return DoGetDuckDBQuery(client_session, query, schema, bind_parameters, print_queries,
                          query_timeout);
}

Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoForCommand(
    const flight::FlightDescriptor& descriptor,
    const std::shared_ptr<arrow::Schema>& schema) {
  std::vector<flight::FlightEndpoint> endpoints{
      flight::FlightEndpoint{flight::Ticket{descriptor.cmd}, {}, std::nullopt, ""}};
  ARROW_ASSIGN_OR_RAISE(auto result,
                        flight::FlightInfo::Make(*schema, descriptor, endpoints, -1, -1))

  return std::make_unique<flight::FlightInfo>(result);
}

std::string PrepareQueryForGetImportedOrExportedKeys(const std::string& filter) {
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
  DuckDBFlightSqlServer* outer_;
  std::shared_ptr<duckdb::DuckDB> db_instance_;
  bool print_queries_;
  int32_t query_timeout_;
  arrow::util::ArrowLogLevel query_log_level_;

  std::map<std::string, std::shared_ptr<DuckDBStatement>> prepared_statements_;
  std::unordered_map<std::string, std::shared_ptr<ClientSession>> client_sessions_;
  std::unordered_map<std::string, std::string> open_transactions_;
  std::default_random_engine gen_;
  std::shared_mutex sessions_mutex_;
  std::shared_mutex statements_mutex_;
  std::shared_mutex transactions_mutex_;
  std::shared_mutex config_mutex_;

  Result<std::shared_ptr<DuckDBStatement>> GetStatementByHandle(
      const std::string& handle) {
    std::shared_lock read_lock(statements_mutex_);
    auto search = prepared_statements_.find(handle);
    if (search == prepared_statements_.end()) {
      return Status::KeyError("Prepared statement not found");
    }
    return search->second;
  }

  static std::optional<std::string> SessionValueToString(
      const flight::SessionOptionValue& v) {
    if (auto p = std::get_if<std::string>(&v)) return *p;
    if (auto p = std::get_if<int64_t>(&v)) return std::to_string(*p);
    if (auto p = std::get_if<bool>(&v)) return *p ? "true" : "false";
    return std::nullopt;
  }

  static Result<std::string> GetSessionID() {
    if (tl_request_ctx.session_id.has_value()) {
      return tl_request_ctx.session_id.value();
    } else {
      return Status::Invalid("No session ID in request context");
    }
  }

  arrow::Result<std::shared_ptr<ClientSession>> GetClientSession(
      const flight::ServerCallContext& context) {
    ARROW_ASSIGN_OR_RAISE(auto session_id, GetSessionID());

    // Fast path: try to find existing session with shared lock
    {
      std::shared_lock read_lock(sessions_mutex_);
      if (auto it = client_sessions_.find(session_id); it != client_sessions_.end()) {
        return it->second;
      }
    }

    // Build the session *without* holding any lock
    auto new_session = std::make_shared<ClientSession>();

    new_session->server = outer_->shared_from_this();
    new_session->session_id = session_id;
    new_session->username = tl_request_ctx.username.value_or("");
    new_session->role = tl_request_ctx.role.value_or("");
    new_session->peer = tl_request_ctx.peer.value_or(context.peer());
    new_session->connection = std::make_shared<duckdb::Connection>(*db_instance_);
    new_session->query_timeout = query_timeout_;
    new_session->query_log_level = query_log_level_;

    // Slow path: take exclusive lock and check again
    {
      std::unique_lock write_lock(sessions_mutex_);

      if (auto it = client_sessions_.find(session_id); it != client_sessions_.end()) {
        // Another thread won the race – reuse its session
        return it->second;
      }

      client_sessions_[session_id] = new_session;
      return new_session;
    }
  }

  // Convenience method for retrieving just a database connection:
  arrow::Result<std::shared_ptr<duckdb::Connection>> GetConnection(
      const flight::ServerCallContext& context) {
    ARROW_ASSIGN_OR_RAISE(auto cs, GetClientSession(context));
    return cs->connection;
  }

  // Create a Ticket that combines a query and a transaction ID.
  static Result<flight::Ticket> EncodeTransactionQuery(
      const std::string& query, const std::string& transaction_id) {
    std::string transaction_query = transaction_id;
    transaction_query += ':';
    transaction_query += query;
    ARROW_ASSIGN_OR_RAISE(auto ticket_string,
                          sql::CreateStatementQueryTicket(transaction_query));
    return flight::Ticket{std::move(ticket_string)};
  }

  static Result<std::pair<std::string, std::string>> DecodeTransactionQuery(
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
  explicit Impl(DuckDBFlightSqlServer* outer, std::shared_ptr<duckdb::DuckDB> db_instance,
                const bool& print_queries, const int32_t& query_timeout,
                const arrow::util::ArrowLogLevel& query_log_level)
      : outer_(outer),
        db_instance_(std::move(db_instance)),
        print_queries_(print_queries),
        query_timeout_(query_timeout),
        query_log_level_(query_log_level) {}

  ~Impl() = default;

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoStatement(
      const flight::ServerCallContext& context, const sql::StatementQuery& command,
      const flight::FlightDescriptor& descriptor) {
    const std::string& query = command.query;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_ASSIGN_OR_RAISE(
        auto statement,
        DuckDBStatement::Create(client_session, query,
                                arrow::util::ArrowLogLevel::ARROW_DEBUG, print_queries_))
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
      const flight::ServerCallContext& context,
      const sql::StatementQueryTicket& command) {
    ARROW_ASSIGN_OR_RAISE(auto pair, DecodeTransactionQuery(command.statement_handle))
    const std::string& sql = pair.first;
    const std::string transaction_id = pair.second;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_ASSIGN_OR_RAISE(
        auto statement,
        DuckDBStatement::Create(client_session, sql, std::nullopt, print_queries_))
    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(statement))

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCatalogs(
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetCatalogsSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetCatalogs(
      const flight::ServerCallContext& context) {
    std::string query =
        "SELECT DISTINCT catalog_name FROM information_schema.schemata ORDER BY "
        "catalog_name";

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query, sql::SqlSchema::GetCatalogsSchema(),
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoSchemas(
      const flight::ServerCallContext& context, const sql::GetDbSchemas& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetDbSchemasSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetDbSchemas(
      const flight::ServerCallContext& context, const sql::GetDbSchemas& command) {
    std::stringstream query;
    duckdb::vector<duckdb::Value> bind_parameters;
    query << "SELECT catalog_name, schema_name AS db_schema_name FROM "
             "information_schema.schemata WHERE 1 = 1";

    query << " AND catalog_name = ";
    if (command.catalog.has_value()) {
      query << "?";
      bind_parameters.emplace_back(command.catalog.value());
    } else {
      query << "CURRENT_DATABASE()";
    }

    if (command.db_schema_filter_pattern.has_value()) {
      query << " AND schema_name LIKE ?";
      bind_parameters.emplace_back(command.db_schema_filter_pattern.value());
    }
    query << " ORDER BY catalog_name, db_schema_name";

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query.str(),
                            sql::SqlSchema::GetDbSchemasSchema(), bind_parameters,
                            print_queries_, query_timeout_);
  }

  Result<sql::ActionCreatePreparedStatementResult> CreatePreparedStatement(
      const flight::ServerCallContext& context,
      const sql::ActionCreatePreparedStatementRequest& request) {
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));

    const std::string handle =
        boost::uuids::to_string(boost::uuids::random_generator()());

    // We latch the statements mutex as briefly as possible to maximize concurrency...
    ARROW_ASSIGN_OR_RAISE(auto statement,
                          DuckDBStatement::Create(client_session, handle, request.query,
                                                  std::nullopt, print_queries_)) {
      std::unique_lock write_lock(statements_mutex_);
      prepared_statements_[handle] = statement;
    }

    ARROW_ASSIGN_OR_RAISE(auto dataset_schema, statement->GetSchema())

    std::shared_ptr<duckdb::PreparedStatement> stmt = statement->GetDuckDBStmt();
    arrow::FieldVector parameter_fields;
    id_t parameter_count = 0;

    if (stmt != nullptr) {
      // Traditional prepared statement - extract parameter information
      parameter_count = stmt->named_param_map.size();
      parameter_fields.reserve(parameter_count);

      duckdb::shared_ptr<duckdb::PreparedStatementData> prepared_statement_data =
          stmt->data;

      if (!prepared_statement_data->properties.IsReadOnly()) {
        if (client_session->role == "readonly") {
          return Status::ExecutionError(
              "User '" + client_session->username +
              "' has a readonly session and cannot run statements that modify state.");
        }
      }

      auto bind_parameter_map = prepared_statement_data->value_map;

      for (id_t i = 0; i < parameter_count; i++) {
        std::string parameter_idx_str = std::to_string(i + 1);
        std::string parameter_name = std::string("parameter_") + parameter_idx_str;
        auto parameter_duckdb_type = prepared_statement_data->GetType(parameter_idx_str);
        auto parameter_arrow_type = GetDataTypeFromDuckDbType(parameter_duckdb_type);
        parameter_fields.emplace_back(field(parameter_name, parameter_arrow_type));
      }
    }
    // For direct execution mode (stmt == nullptr), parameter_fields remains empty

    const std::shared_ptr<arrow::Schema>& parameter_schema =
        arrow::schema(parameter_fields);

    sql::ActionCreatePreparedStatementResult result{.dataset_schema = dataset_schema,
                                                    .parameter_schema = parameter_schema,
                                                    .prepared_statement_handle = handle};

    return result;
  }

  Status ClosePreparedStatement(const flight::ServerCallContext& context,
                                const sql::ActionClosePreparedStatementRequest& request) {
    const std::string& prepared_statement_handle = request.prepared_statement_handle;

    {
      std::unique_lock write_lock(statements_mutex_);
      if (auto search = prepared_statements_.find(prepared_statement_handle);
          search != prepared_statements_.end()) {
        prepared_statements_.erase(prepared_statement_handle);
      } else {
        return Status::Invalid("Prepared statement not found");
      }
    }

    return Status::OK();
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPreparedStatement(
      const flight::ServerCallContext& context,
      const sql::PreparedStatementQuery& command,
      const flight::FlightDescriptor& descriptor) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    ARROW_ASSIGN_OR_RAISE(auto statement,
                          GetStatementByHandle(prepared_statement_handle));
    ARROW_ASSIGN_OR_RAISE(auto schema, statement->GetSchema())

    return GetFlightInfoForCommand(descriptor, schema);
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetPreparedStatement(
      const flight::ServerCallContext& context,
      const sql::PreparedStatementQuery& command) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;

    ARROW_ASSIGN_OR_RAISE(auto statement,
                          GetStatementByHandle(prepared_statement_handle));

    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(statement))

    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Status DoPutPreparedStatementQuery(const flight::ServerCallContext& context,
                                     const sql::PreparedStatementQuery& command,
                                     flight::FlightMessageReader* reader,
                                     flight::FlightMetadataWriter* writer) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(auto statement, GetStatementByHandle(prepared_statement_handle))
    ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

    return Status::OK();
  }

  Result<int64_t> DoPutPreparedStatementUpdate(
      const flight::ServerCallContext& context,
      const sql::PreparedStatementUpdate& command, flight::FlightMessageReader* reader) {
    const std::string& prepared_statement_handle = command.prepared_statement_handle;
    ARROW_ASSIGN_OR_RAISE(auto statement, GetStatementByHandle(prepared_statement_handle))

    ARROW_RETURN_NOT_OK(SetParametersOnDuckDBStatement(statement, reader));

    return statement->ExecuteUpdate();
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTables(
      const flight::ServerCallContext& context, const sql::GetTables& command) {
    duckdb::vector<duckdb::Value> get_tables_bind_parameters;
    std::string get_tables_query =
        PrepareQueryForGetTables(command, get_tables_bind_parameters);
    std::shared_ptr<DuckDBStatement> statement;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_ASSIGN_OR_RAISE(
        statement,
        DuckDBStatement::Create(client_session, get_tables_query,
                                arrow::util::ArrowLogLevel::ARROW_DEBUG, print_queries_,
                                sql::SqlSchema::GetTablesSchema()))
    statement->bind_parameters = get_tables_bind_parameters;

    ARROW_ASSIGN_OR_RAISE(auto reader, DuckDBStatementBatchReader::Create(
                                           statement, sql::SqlSchema::GetTablesSchema()))

    if (command.include_schema) {
      auto table_schema_reader = std::make_shared<DuckDBTablesWithSchemaBatchReader>(
          reader, get_tables_query, client_session);
      return std::make_unique<flight::RecordBatchStream>(table_schema_reader);
    } else {
      return std::make_unique<flight::RecordBatchStream>(reader);
    }
  }

  Result<int64_t> DoPutCommandStatementUpdate(const flight::ServerCallContext& context,
                                              const sql::StatementUpdate& command) {
    const std::string& sql = command.query;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_ASSIGN_OR_RAISE(
        auto statement,
        DuckDBStatement::Create(client_session, sql, std::nullopt, print_queries_))
    return statement->ExecuteUpdate();
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTypeInfo(
      const flight::ServerCallContext& context, const sql::GetXdbcTypeInfo& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor,
                                   flight::sql::SqlSchema::GetXdbcTypeInfoSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTypeInfo(
      const flight::ServerCallContext& context, const sql::GetXdbcTypeInfo& command) {
    ARROW_ASSIGN_OR_RAISE(auto type_info_result,
                          command.data_type.has_value()
                              ? DoGetTypeInfoResult(command.data_type.value())
                              : DoGetTypeInfoResult());

    ARROW_ASSIGN_OR_RAISE(auto reader,
                          arrow::RecordBatchReader::Make({type_info_result}));
    return std::make_unique<flight::RecordBatchStream>(reader);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoTables(
      const flight::ServerCallContext& context, const sql::GetTables& command,
      const flight::FlightDescriptor& descriptor) {
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
      const flight::ServerCallContext& context,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetTableTypesSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetTableTypes(
      const flight::ServerCallContext& context) {
    std::string query =
        "SELECT * FROM VALUES ('BASE TABLE'), ('LOCAL TEMPORARY'), ('VIEW') AS "
        "table_types (table_type)";

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query, sql::SqlSchema::GetTableTypesSchema(),
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoPrimaryKeys(
      const flight::ServerCallContext& context, const sql::GetPrimaryKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetPrimaryKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetPrimaryKeys(
      const flight::ServerCallContext& context, const sql::GetPrimaryKeys& command) {
    std::stringstream table_query;
    duckdb::vector<duckdb::Value> bind_parameters;

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

    const sql::TableRef& table_ref = command.table_ref;
    table_query << " AND catalog_name = ";
    if (table_ref.catalog.has_value()) {
      table_query << "?";
      bind_parameters.emplace_back(table_ref.catalog.value());
    } else {
      table_query << "CURRENT_DATABASE()";
    }

    if (table_ref.db_schema.has_value()) {
      table_query << " and schema_name LIKE ?";
      bind_parameters.emplace_back(table_ref.db_schema.value());
    }

    table_query << " and table_name LIKE ?";
    bind_parameters.emplace_back(table_ref.table);

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, table_query.str(),
                            sql::SqlSchema::GetPrimaryKeysSchema(), bind_parameters,
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoImportedKeys(
      const flight::ServerCallContext& context, const sql::GetImportedKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetImportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetImportedKeys(
      const flight::ServerCallContext& context, const sql::GetImportedKeys& command) {
    const sql::TableRef& table_ref = command.table_ref;
    duckdb::vector<duckdb::Value> bind_parameters;

    std::string filter = "fk_table_name = ?";
    bind_parameters.emplace_back(table_ref.table);

    filter += " AND fk_catalog_name = ";
    if (table_ref.catalog.has_value()) {
      filter += "?";
      bind_parameters.emplace_back(table_ref.catalog.value());
    } else {
      filter += "CURRENT_DATABASE()";
    }

    if (table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = ?";
      bind_parameters.emplace_back(table_ref.db_schema.value());
    }

    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query,
                            sql::SqlSchema::GetImportedKeysSchema(), bind_parameters,
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoExportedKeys(
      const flight::ServerCallContext& context, const sql::GetExportedKeys& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetExportedKeysSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetExportedKeys(
      const flight::ServerCallContext& context, const sql::GetExportedKeys& command) {
    const sql::TableRef& table_ref = command.table_ref;
    duckdb::vector<duckdb::Value> bind_parameters;

    std::string filter = "pk_table_name = ?";
    bind_parameters.emplace_back(table_ref.table);

    filter += " AND pk_catalog_name = ";

    if (table_ref.catalog.has_value()) {
      filter += "?";
      bind_parameters.emplace_back(table_ref.catalog.value());
    } else {
      filter += "CURRENT_DATABASE()";
    }

    if (table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = ?";
      bind_parameters.emplace_back(table_ref.db_schema.value());
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query,
                            sql::SqlSchema::GetExportedKeysSchema(), bind_parameters,
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<flight::FlightInfo>> GetFlightInfoCrossReference(
      const flight::ServerCallContext& context, const sql::GetCrossReference& command,
      const flight::FlightDescriptor& descriptor) {
    return GetFlightInfoForCommand(descriptor, sql::SqlSchema::GetCrossReferenceSchema());
  }

  Result<std::unique_ptr<flight::FlightDataStream>> DoGetCrossReference(
      const flight::ServerCallContext& context, const sql::GetCrossReference& command) {
    const sql::TableRef& pk_table_ref = command.pk_table_ref;
    duckdb::vector<duckdb::Value> bind_parameters;

    std::string filter = "pk_table_name = ?";
    bind_parameters.emplace_back(pk_table_ref.table);

    filter += " AND pk_catalog_name = ";
    if (pk_table_ref.catalog.has_value()) {
      filter += "?";
      bind_parameters.emplace_back(pk_table_ref.catalog.value());
    } else {
      filter += "CURRENT_DATABASE()";
    }

    if (pk_table_ref.db_schema.has_value()) {
      filter += " AND pk_schema_name = ?";
      bind_parameters.emplace_back(pk_table_ref.db_schema.value());
    }

    const sql::TableRef& fk_table_ref = command.fk_table_ref;
    filter += " AND fk_table_name = ?";
    bind_parameters.emplace_back(fk_table_ref.table);

    filter += " AND fk_catalog_name = ";
    if (fk_table_ref.catalog.has_value()) {
      filter += "?";
      bind_parameters.emplace_back(fk_table_ref.catalog.value());
    } else {
      filter += "CURRENT_DATABASE()";
    }

    if (fk_table_ref.db_schema.has_value()) {
      filter += " AND fk_schema_name = ?";
      bind_parameters.emplace_back(fk_table_ref.db_schema.value());
    }
    std::string query = PrepareQueryForGetImportedOrExportedKeys(filter);

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    return DoGetDuckDBQuery(client_session, query,
                            sql::SqlSchema::GetCrossReferenceSchema(), bind_parameters,
                            print_queries_, query_timeout_);
  }

  Result<std::unique_ptr<duckdb::MaterializedQueryResult>> RunAndLogQueryWithResult(
      const std::shared_ptr<ClientSession>& client_session, const std::string& query) {
    std::string status;

    GIZMOSQL_LOG_SCOPE_STATUS(
        DEBUG, "RunAndLogQuery", status, {"peer", client_session->peer},
        {"session_id", client_session->session_id}, {"user", client_session->username},
        {"role", client_session->role}, {"query", redact_sql_for_logs(query)});

    auto res = client_session->connection->Query(query);
    if (res->HasError()) {
      return Status::Invalid("Failed to run DuckDB query: " + query + ": " +
                             res->GetError());
    }

    status = "success";
    return res;
  }

  Status RunAndLogQuery(const std::shared_ptr<ClientSession>& client_session,
                        const std::string& query) {
    ARROW_ASSIGN_OR_RAISE(auto res, RunAndLogQueryWithResult(client_session, query));
    return Status::OK();
  }

  Result<int64_t> DoPutCommandStatementIngest(const flight::ServerCallContext& context,
                                              const flight::sql::StatementIngest& command,
                                              flight::FlightMessageReader* reader) {
    std::string status;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));

    GIZMOSQL_LOG_SCOPE_STATUS(
        DEBUG, "DuckDBFlightSqlServer::DoPutCommandStatementIngest", status,
        {"peer", client_session->peer}, {"session_id", client_session->session_id},
        {"user", client_session->username}, {"role", client_session->role});

    // 1. Build a fully qualified target table name
    std::string target_table;
    if (command.catalog.has_value()) {
      target_table += QuoteIdent(command.catalog.value()) + ".";
    }
    if (command.schema.has_value()) {
      target_table += QuoteIdent(command.schema.value()) + ".";
    }
    target_table += QuoteIdent(command.table);
    std::optional<std::string> interim_table = std::nullopt;

    // 2. Basic metadata up front (no transaction yet)
    ARROW_ASSIGN_OR_RAISE(auto arrow_schema, reader->GetSchema());

    ARROW_ASSIGN_OR_RAISE(auto target_table_exists,
                          TableExists(*client_session->connection, command.catalog,
                                      command.schema, command.table));

    using ExistsOpt = sql::TableDefinitionOptionsTableExistsOption;
    using NotExistsOpt = sql::TableDefinitionOptionsTableNotExistOption;

    // If table does NOT exist and caller insisted that it must exist, fail early
    if (!target_table_exists) {
      switch (command.table_definition_options.if_not_exist) {
        case NotExistsOpt::kFail:
          return Status::Invalid("Table: " + target_table + " does not exist");
        case NotExistsOpt::kCreate:
        case NotExistsOpt::kUnspecified:
          // OK: nothing to do here
          break;
      }
    }

    // 3. Start transaction (RAII guard)
    DuckDBTransactionGuard txn(*client_session->connection);

    // 4. Handle existence options inside the transaction
    if (target_table_exists) {
      switch (command.table_definition_options.if_exists) {
        case ExistsOpt::kFail:
          // Rollback handled by txn destructor
          return Status::Invalid("Table: " + target_table + " already exists");

        case ExistsOpt::kAppend: {
          interim_table =
              command.table + "_interim_bulk_ingest_temp_" + client_session->session_id;
          ARROW_ASSIGN_OR_RAISE(
              auto create_table_sql,
              GenerateCreateTableSQLFromArrowSchema(QuoteIdent(interim_table.value()),
                                                    arrow_schema, true, true));
          ARROW_RETURN_NOT_OK(RunAndLogQuery(client_session, create_table_sql));
          break;
        }

        case ExistsOpt::kUnspecified:
          // OK: use existing table as-is
          break;

        case ExistsOpt::kReplace: {
          // DROP then re-CREATE inside the same transaction
          ARROW_RETURN_NOT_OK(
              RunAndLogQuery(client_session, "DROP TABLE " + target_table));
          target_table_exists = false;
          break;
        }
      }
    }

    // 5. If table doesn't exist (or was just dropped for replace), create it
    if (!target_table_exists) {
      ARROW_ASSIGN_OR_RAISE(auto create_table_sql,
                            GenerateCreateTableSQLFromArrowSchema(
                                target_table, arrow_schema, command.temporary));

      ARROW_RETURN_NOT_OK(RunAndLogQuery(client_session, create_table_sql));
    }

    // 6. Ingest rows via Appender
    int64_t total_rows = 0;
    flight::FlightStreamChunk chunk;

    std::optional<duckdb::Appender> appender;
    try {
      // Initialize appender
      if (interim_table.has_value()) {
        appender.emplace(*client_session->connection, interim_table.value());
      } else if (command.catalog.has_value() && command.schema.has_value()) {
        appender.emplace(*client_session->connection, command.catalog.value(),
                         command.schema.value(), command.table);
      } else if (command.schema.has_value()) {
        appender.emplace(*client_session->connection, command.schema.value(),
                         command.table);
      } else {
        appender.emplace(*client_session->connection, command.table);
      }

      // Ingest loop
      while (true) {
        ARROW_ASSIGN_OR_RAISE(auto next_chunk, reader->Next());
        chunk = std::move(next_chunk);

        if (!chunk.data && !chunk.app_metadata) {
          break;  // end-of-stream
        }

        if (chunk.data) {
          total_rows += chunk.data->num_rows();
          ARROW_RETURN_NOT_OK(AppendRecordBatchToDuckDB(*appender, chunk.data));
        }
      }

      appender->Close();

      // 7. If this was an append - copy the data from the interim temp table (to handle default columns values, etc.)
      if (interim_table.has_value()) {
        auto insert_sql = "INSERT INTO " + target_table + " BY NAME SELECT * FROM " +
                          QuoteIdent(interim_table.value());
        ARROW_ASSIGN_OR_RAISE(auto insert_res,
                              RunAndLogQueryWithResult(client_session, insert_sql));
        // This shouldn't happen - but just in case...
        auto interim_insert_row_count_value = insert_res->GetValue(0, 0);
        auto interim_insert_row_count =
            interim_insert_row_count_value.GetValue<int64_t>();
        if (interim_insert_row_count != total_rows) {
          return Status::Invalid(
              "Row count inserted from interim table: " + interim_table.value() + " (" +
              std::to_string(interim_insert_row_count) + ")" + " into: " + target_table +
              " was mis-matched from rows appended (" + std::to_string(total_rows) +
              ") !");
        }
        // Drop our interim table...
        ARROW_RETURN_NOT_OK(RunAndLogQuery(
            client_session, "DROP TABLE " + QuoteIdent(interim_table.value())));
      }

      // 8. Commit transaction – if this fails, caller's table stays intact due to rollback
      ARROW_RETURN_NOT_OK(txn.Commit());

      status = "success";
      return total_rows;
    } catch (const duckdb::Exception& ex) {
      // txn destructor will rollback
      return Status::Invalid("DuckDB ingest failed: " + std::string(ex.what()));
    }
  }

  Result<sql::ActionBeginTransactionResult> BeginTransaction(
      const flight::ServerCallContext& context,
      const sql::ActionBeginTransactionRequest& request) {
    std::string handle = boost::uuids::to_string(boost::uuids::random_generator()());
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    std::unique_lock write_lock(transactions_mutex_);
    open_transactions_[handle] = "";

    ARROW_RETURN_NOT_OK(ExecuteSql(client_session->connection, "BEGIN TRANSACTION"));

    return sql::ActionBeginTransactionResult{std::move(handle)};
  }

  Status EndTransaction(const flight::ServerCallContext& context,
                        const sql::ActionEndTransactionRequest& request) {
    Status status;
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    {
      if (request.action == sql::ActionEndTransactionRequest::kCommit) {
        status = ExecuteSql(client_session->connection, "COMMIT");
      } else {
        status = ExecuteSql(client_session->connection, "ROLLBACK");
      }
      std::unique_lock write_lock(transactions_mutex_);
      open_transactions_.erase(request.transaction_id);
    }
    return status;
  }

  Status DoCancelActiveStatement(const std::shared_ptr<ClientSession>& client_session) {
    if (!client_session->active_sql_handle.has_value() ||
        client_session->active_sql_handle->empty()) {
      return Status::Invalid("No active SQL statement to cancel.");
    }
    client_session->connection->Interrupt();
    GIZMOSQL_LOGKV(INFO, "SQL Statement was successfully canceled.",
                   {"peer", client_session->peer}, {"kind", "sql"},
                   {"status", "canceled"}, {"session_id", client_session->session_id},
                   {"user", client_session->username}, {"role", client_session->role},
                   {"statement_handle", client_session->active_sql_handle.value()});
    return Status::OK();
  }

  Result<flight::CancelFlightInfoResult> CancelFlightInfo(
      const flight::ServerCallContext& context,
      const flight::CancelFlightInfoRequest& request) {
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_RETURN_NOT_OK(DoCancelActiveStatement(client_session));
    return flight::CancelFlightInfoResult(flight::CancelStatus::kCancelled);
  }

  Result<flight::sql::CancelResult> CancelQuery(
      const flight::ServerCallContext& context,
      const flight::sql::ActionCancelQueryRequest& request) {
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    ARROW_RETURN_NOT_OK(DoCancelActiveStatement(client_session));
    return flight::sql::CancelResult(flight::sql::CancelResult::kCancelled);
  }

  Result<flight::SetSessionOptionsResult> SetSessionOptions(
      const flight::ServerCallContext& context,
      const flight::SetSessionOptionsRequest& request) {
    flight::SetSessionOptionsResult res;

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    for (const auto& [name, value] : request.session_options) {
      if (name == "catalog" || name == "schema") {
        std::string sanitized =
            boost::algorithm::erase_all_copy(std::get<std::string>(value), "\"");
        std::string quoted_identifier = "\"" + sanitized + "\"";
        ARROW_RETURN_NOT_OK(
            ExecuteSql(client_session->connection, "USE " + quoted_identifier));
      } else {
        res.errors.emplace(name, flight::SetSessionOptionsResult::Error{
                                     flight::SetSessionOptionErrorValue::kInvalidName});
      }
    }

    return res;
  }

  Result<flight::GetSessionOptionsResult> GetSessionOptions(
      const flight::ServerCallContext& context,
      const flight::GetSessionOptionsRequest& request) {
    flight::GetSessionOptionsResult res;

    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));

    auto catalog_result = ExecuteSqlAndGetStringVector(client_session->connection,
                                                       "SELECT current_catalog()");
    if (catalog_result.ok()) {
      auto current_catalog = catalog_result.ValueOrDie().front();
      res.session_options.emplace("catalog", current_catalog);
    } else {
      return Status::Invalid("Could not get current catalog");
    }

    auto schema_result = ExecuteSqlAndGetStringVector(client_session->connection,
                                                      "SELECT current_schema()");
    if (schema_result.ok()) {
      auto current_schema = schema_result.ValueOrDie().front();
      res.session_options.emplace("schema", current_schema);
    } else {
      return Status::Invalid("Could not get current schema");
    }

    return res;
  }

  Result<flight::CloseSessionResult> CloseSession(
      const flight::ServerCallContext& context,
      const flight::CloseSessionRequest& request) {
    ARROW_ASSIGN_OR_RAISE(auto client_session, GetClientSession(context));
    // We may have a SQL statement in-flight (or being interrupted / rolled-back) - we need to let it clean up safely
    std::unique_lock<std::mutex> connection_lock(client_session->connection_mutex);

    std::unique_lock write_lock(sessions_mutex_);
    auto it = client_sessions_.find(client_session->session_id);
    if (it != client_sessions_.end()) {
      client_sessions_.erase(it);
      GIZMOSQL_LOGKV(INFO, "Client session was successfully closed.",
                     {"peer", client_session->peer}, {"kind", "session_close"},
                     {"status", "success"}, {"session_id", client_session->session_id},
                     {"user", client_session->username}, {"role", client_session->role});
      return flight::CloseSessionResult(flight::CloseSessionStatus::kClosed);
    } else {
      GIZMOSQL_LOGKV(WARNING,
                     "Client session was NOT successfully closed - session not found.",
                     {"peer", client_session->peer}, {"kind", "session_close"},
                     {"status", "failure"}, {"session_id", client_session->session_id},
                     {"user", client_session->username}, {"role", client_session->role});
      return Status::KeyError("Session: '" + client_session->session_id + "' not found");
    }
  }

  static Status ExecuteSql(const std::shared_ptr<duckdb::Connection>& connection,
                           const std::string& sql) {
    if (std::unique_ptr<duckdb::MaterializedQueryResult> result = connection->Query(sql);
        result->HasError()) {
      return Status::Invalid(result->GetError());
    }
    return Status::OK();
  }

  Status ExecuteSql(const std::string& sql) {
    // We do not have a call context, so just grab a new connection to the instance
    auto connection = std::make_shared<duckdb::Connection>(*db_instance_);
    return ExecuteSql(connection, sql);
  }

  Result<std::vector<std::string>> ExecuteSqlAndGetStringVector(
      const std::shared_ptr<duckdb::Connection>& connection, const std::string& sql) {
    std::unique_ptr<duckdb::MaterializedQueryResult> result = connection->Query(sql);

    if (result->HasError()) {
      return Status::Invalid("SQL query failed: ", result->GetError());
    }

    std::vector<std::string> string_results;

    // Extract string values from the first column of all rows
    for (size_t row_idx = 0; row_idx < result->RowCount(); row_idx++) {
      auto value = result->GetValue(0, row_idx);  // First column
      if (!value.IsNull()) {
        string_results.emplace_back(value.ToString());
      }
    }

    return string_results;
  }

  // Utility function to execute a query and return string results from the first column
  Result<std::vector<std::string>> ExecuteSqlAndGetStringVector(const std::string& sql) {
    // We do not have a call context, so just grab a new connection to the instance
    auto connection = std::make_shared<duckdb::Connection>(*db_instance_);

    return ExecuteSqlAndGetStringVector(connection, sql);
  }

  Status SetQueryTimeout(const std::shared_ptr<ClientSession>& client_session,
                         const int& seconds) {
    if (client_session->role != "admin") {
      return Status::Invalid(
          "Only admin users can change the server query_timeout setting.");
    }
    std::unique_lock write_lock(config_mutex_);
    query_timeout_ = seconds;

    return Status::OK();
  }

  Result<int32_t> GetQueryTimeout(const std::shared_ptr<ClientSession>& client_session) {
    std::shared_lock read_lock(config_mutex_);
    return query_timeout_;
  }

  Status SetPrintQueries(const std::shared_ptr<ClientSession>& client_session,
                         const bool& enabled) {
    if (client_session->role != "admin") {
      return Status::Invalid("Only admin users can enable/disable server query logging.");
    }
    std::unique_lock write_lock(config_mutex_);
    print_queries_ = enabled;
    return Status::OK();
  }

  Result<bool> GetPrintQueries(const std::shared_ptr<ClientSession>& client_session) {
    std::shared_lock read_lock(config_mutex_);
    return print_queries_;
  }

  Status SetQueryLogLevel(const std::shared_ptr<ClientSession>& client_session,
                          const arrow::util::ArrowLogLevel& query_log_level) {
    if (client_session->role != "admin") {
      return Status::Invalid("Only admin users can set the server Query Log Level.");
    }
    std::unique_lock write_lock(config_mutex_);
    query_log_level_ = query_log_level;
    return Status::OK();
  }

  Result<arrow::util::ArrowLogLevel> GetQueryLogLevel(
      const std::shared_ptr<ClientSession>& client_session) {
    std::shared_lock read_lock(config_mutex_);
    return query_log_level_;
  }
};

Result<std::shared_ptr<DuckDBFlightSqlServer>> DuckDBFlightSqlServer::Create(
    const std::string& path, const bool& read_only, const bool& print_queries,
    const int32_t& query_timeout, const arrow::util::ArrowLogLevel& query_log_level) {
  GIZMOSQL_LOG(INFO) << "DuckDB version: " << duckdb_library_version();

  bool in_memory = path == ":memory:";
  char* db_location;

  if (in_memory) {
    db_location = nullptr;
  } else {
    db_location = const_cast<char*>(path.c_str());
  }

  duckdb::DBConfig config;
  if (read_only) {
    config.options.access_mode = duckdb::AccessMode::READ_ONLY;
  }

  auto db = std::make_shared<duckdb::DuckDB>(db_location, &config);

  auto result = std::make_shared<DuckDBFlightSqlServer>();
  result->impl_ = std::make_shared<DuckDBFlightSqlServer::Impl>(
      result.get(), db, print_queries, query_timeout, query_log_level);

  // Use dynamic SQL info that queries DuckDB for keywords and functions
  for (const auto& id_to_result : GetSqlInfoResultMap(result.get(), query_timeout)) {
    result->RegisterSqlInfo(id_to_result.first, id_to_result.second);
  }
  return result;
}

DuckDBFlightSqlServer::~DuckDBFlightSqlServer() = default;

Status DuckDBFlightSqlServer::ExecuteSql(const std::string& sql) const {
  return impl_->ExecuteSql(sql);
}

Result<std::vector<std::string>> DuckDBFlightSqlServer::ExecuteSqlAndGetStringVector(
    const std::string& sql) const {
  return impl_->ExecuteSqlAndGetStringVector(sql);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoStatement(
    const flight::ServerCallContext& context, const sql::StatementQuery& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoStatement(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetStatement(
    const flight::ServerCallContext& context, const sql::StatementQueryTicket& command) {
  return impl_->DoGetStatement(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoCatalogs(
    const flight::ServerCallContext& context,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCatalogs(context, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetCatalogs(
    const flight::ServerCallContext& context) {
  return impl_->DoGetCatalogs(context);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoSchemas(
    const flight::ServerCallContext& context, const sql::GetDbSchemas& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoSchemas(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetDbSchemas(
    const flight::ServerCallContext& context, const sql::GetDbSchemas& command) {
  return impl_->DoGetDbSchemas(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>> DuckDBFlightSqlServer::GetFlightInfoTables(
    const flight::ServerCallContext& context, const sql::GetTables& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTables(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetTables(
    const flight::ServerCallContext& context, const sql::GetTables& command) {
  return impl_->DoGetTables(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoXdbcTypeInfo(
    const flight::ServerCallContext& context, const flight::sql::GetXdbcTypeInfo& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTypeInfo(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetXdbcTypeInfo(const flight::ServerCallContext& context,
                                         const flight::sql::GetXdbcTypeInfo& command) {
  return impl_->DoGetTypeInfo(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoTableTypes(
    const flight::ServerCallContext& context,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoTableTypes(context, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetTableTypes(
    const flight::ServerCallContext& context) {
  return impl_->DoGetTableTypes(context);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementUpdate(
    const flight::ServerCallContext& context, const sql::StatementUpdate& command) {
  return impl_->DoPutCommandStatementUpdate(context, command);
}

Result<sql::ActionCreatePreparedStatementResult>
DuckDBFlightSqlServer::CreatePreparedStatement(
    const flight::ServerCallContext& context,
    const sql::ActionCreatePreparedStatementRequest& request) {
  return impl_->CreatePreparedStatement(context, request);
}

Status DuckDBFlightSqlServer::ClosePreparedStatement(
    const flight::ServerCallContext& context,
    const sql::ActionClosePreparedStatementRequest& request) {
  return impl_->ClosePreparedStatement(context, request);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoPreparedStatement(
    const flight::ServerCallContext& context, const sql::PreparedStatementQuery& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPreparedStatement(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetPreparedStatement(
    const flight::ServerCallContext& context,
    const sql::PreparedStatementQuery& command) {
  return impl_->DoGetPreparedStatement(context, command);
}

Status DuckDBFlightSqlServer::DoPutPreparedStatementQuery(
    const flight::ServerCallContext& context, const sql::PreparedStatementQuery& command,
    flight::FlightMessageReader* reader, flight::FlightMetadataWriter* writer) {
  return impl_->DoPutPreparedStatementQuery(context, command, reader, writer);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutPreparedStatementUpdate(
    const flight::ServerCallContext& context, const sql::PreparedStatementUpdate& command,
    flight::FlightMessageReader* reader) {
  return impl_->DoPutPreparedStatementUpdate(context, command, reader);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoPrimaryKeys(
    const flight::ServerCallContext& context, const sql::GetPrimaryKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoPrimaryKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>> DuckDBFlightSqlServer::DoGetPrimaryKeys(
    const flight::ServerCallContext& context, const sql::GetPrimaryKeys& command) {
  return impl_->DoGetPrimaryKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoImportedKeys(
    const flight::ServerCallContext& context, const sql::GetImportedKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoImportedKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetImportedKeys(const flight::ServerCallContext& context,
                                         const sql::GetImportedKeys& command) {
  return impl_->DoGetImportedKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoExportedKeys(
    const flight::ServerCallContext& context, const sql::GetExportedKeys& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoExportedKeys(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetExportedKeys(const flight::ServerCallContext& context,
                                         const sql::GetExportedKeys& command) {
  return impl_->DoGetExportedKeys(context, command);
}

Result<std::unique_ptr<flight::FlightInfo>>
DuckDBFlightSqlServer::GetFlightInfoCrossReference(
    const flight::ServerCallContext& context, const sql::GetCrossReference& command,
    const flight::FlightDescriptor& descriptor) {
  return impl_->GetFlightInfoCrossReference(context, command, descriptor);
}

Result<std::unique_ptr<flight::FlightDataStream>>
DuckDBFlightSqlServer::DoGetCrossReference(const flight::ServerCallContext& context,
                                           const sql::GetCrossReference& command) {
  return impl_->DoGetCrossReference(context, command);
}

Result<int64_t> DuckDBFlightSqlServer::DoPutCommandStatementIngest(
    const flight::ServerCallContext& context, const flight::sql::StatementIngest& command,
    flight::FlightMessageReader* reader) {
  return impl_->DoPutCommandStatementIngest(context, command, reader);
}

Result<sql::ActionBeginTransactionResult> DuckDBFlightSqlServer::BeginTransaction(
    const flight::ServerCallContext& context,
    const sql::ActionBeginTransactionRequest& request) {
  return impl_->BeginTransaction(context, request);
}

Status DuckDBFlightSqlServer::EndTransaction(
    const flight::ServerCallContext& context,
    const sql::ActionEndTransactionRequest& request) {
  return impl_->EndTransaction(context, request);
}

Result<flight::CancelFlightInfoResult> DuckDBFlightSqlServer::CancelFlightInfo(
    const flight::ServerCallContext& context,
    const flight::CancelFlightInfoRequest& request) {
  return impl_->CancelFlightInfo(context, request);
}

Result<flight::sql::CancelResult> DuckDBFlightSqlServer::CancelQuery(
    const flight::ServerCallContext& context,
    const flight::sql::ActionCancelQueryRequest& request) {
  return impl_->CancelQuery(context, request);
}

Result<flight::SetSessionOptionsResult> DuckDBFlightSqlServer::SetSessionOptions(
    const flight::ServerCallContext& context,
    const flight::SetSessionOptionsRequest& request) {
  return impl_->SetSessionOptions(context, request);
}

Result<flight::GetSessionOptionsResult> DuckDBFlightSqlServer::GetSessionOptions(
    const flight::ServerCallContext& context,
    const flight::GetSessionOptionsRequest& request) {
  return impl_->GetSessionOptions(context, request);
}

Result<flight::CloseSessionResult> DuckDBFlightSqlServer::CloseSession(
    const flight::ServerCallContext& context,
    const flight::CloseSessionRequest& request) {
  return impl_->CloseSession(context, request);
}

Status DuckDBFlightSqlServer::SetQueryTimeout(
    const std::shared_ptr<ClientSession>& client_session, const int& seconds) {
  return impl_->SetQueryTimeout(client_session, seconds);
}

Result<int32_t> DuckDBFlightSqlServer::GetQueryTimeout(
    const std::shared_ptr<ClientSession>& client_session) {
  return impl_->GetQueryTimeout(client_session);
}

Status DuckDBFlightSqlServer::SetPrintQueries(
    const std::shared_ptr<ClientSession>& client_session, const bool& enabled) {
  return impl_->SetPrintQueries(client_session, enabled);
}

Result<bool> DuckDBFlightSqlServer::GetPrintQueries(
    const std::shared_ptr<ClientSession>& client_session) {
  return impl_->GetPrintQueries(client_session);
}

arrow::Status DuckDBFlightSqlServer::SetQueryLogLevel(
    const std::shared_ptr<ClientSession>& client_session,
    const arrow::util::ArrowLogLevel& query_log_level) {
  return impl_->SetQueryLogLevel(client_session, query_log_level);
}

arrow::Result<arrow::util::ArrowLogLevel> DuckDBFlightSqlServer::GetQueryLogLevel(
    const std::shared_ptr<ClientSession>& client_session) {
  return impl_->GetQueryLogLevel(client_session);
}

}  // namespace gizmosql::ddb