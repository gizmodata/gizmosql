// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

#include "gizmosql_ios_bridge.h"
#include "gizmosql_library.h"

#include <chrono>
#include <csignal>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <filesystem>
#include <vector>

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/flight/sql/api.h>

namespace {
/// Convert a nullable C string to std::string (empty if NULL).
std::string safe_str(const char* s) { return s ? std::string(s) : std::string(); }

/// Convert a nullable C string to std::filesystem::path (empty if NULL).
std::filesystem::path safe_path(const char* s) {
    return s ? std::filesystem::path(s) : std::filesystem::path();
}
}  // namespace

extern "C" {

int gizmosql_server_start(const GizmoSQLServerConfig* config) {
    if (!config || !config->password) {
        return 1;
    }

    BackendType backend = (config->backend == 1) ? BackendType::sqlite : BackendType::duckdb;

    return RunFlightSQLServer(
        backend,
        safe_path(config->database_filename),
        safe_str(config->hostname),
        config->port > 0 ? config->port : DEFAULT_FLIGHT_PORT,
        safe_str(config->username),
        std::string(config->password),
        safe_str(config->secret_key),
        safe_path(config->tls_cert_path),
        safe_path(config->tls_key_path),
        std::filesystem::path(),  // mtls_ca_cert_path — not supported on iOS
        safe_str(config->init_sql_commands),
        std::filesystem::path(),  // init_sql_commands_file
        config->print_queries != 0,
        config->read_only != 0,
        "",                       // token_allowed_issuer
        "",                       // token_allowed_audience
        std::filesystem::path(),  // token_signature_verify_cert_path
        "",                       // token_jwks_uri
        "",                       // token_default_role
        "",                       // token_authorized_emails
        safe_str(config->log_level),
        safe_str(config->log_format),
        "",                       // access_log
        safe_str(config->log_file),
        config->query_timeout,
        "",                       // query_log_level (use default)
        "",                       // auth_log_level (use default)
        0,                        // health_port — disabled on iOS
        ""                        // health_check_query
        // Remaining enterprise/OAuth/OTel params use defaults (disabled)
    );
}

void gizmosql_server_request_shutdown(void) {
    // Call Shutdown() directly on the server via the library API.
    // This avoids raise(SIGINT) which triggers the Xcode debugger.
    ShutdownFlightServer();
}

void gizmosql_server_cleanup(void) {
    gizmosql::CleanupServerResources();
}

size_t gizmosql_server_active_sessions(void) {
    return GetActiveSessionCount();
}

const char* gizmosql_server_version(void) {
    // GIZMOSQL_SERVER_VERSION is a global std::string defined in gizmosql_library.h.
    // c_str() returns a pointer stable for the lifetime of the process.
    return GIZMOSQL_SERVER_VERSION.c_str();
}

// ---------------------------------------------------------------------------
// Embedded Flight SQL Client
// ---------------------------------------------------------------------------

struct ClientHandle {
    std::unique_ptr<arrow::flight::sql::FlightSqlClient> client;
    arrow::flight::FlightCallOptions call_options;
};

static std::string ReadFileContents(const std::string& path) {
    std::ifstream f(path);
    if (!f.good()) return {};
    std::stringstream ss;
    ss << f.rdbuf();
    return ss.str();
}

void* gizmosql_client_connect(const char* host, int port,
                               const char* username, const char* password,
                               const char* tls_cert_path, int tls_skip_verify,
                               char** out_error) {
    auto set_error = [&](const std::string& msg) {
        if (out_error) *out_error = strdup(msg.c_str());
    };

    bool use_tls = (tls_cert_path != nullptr && strlen(tls_cert_path) > 0);
    std::string h = host ? host : "localhost";

    auto loc_result = use_tls
        ? arrow::flight::Location::ForGrpcTls(h, port)
        : arrow::flight::Location::ForGrpcTcp(h, port);
    if (!loc_result.ok()) {
        set_error(loc_result.status().ToString());
        return nullptr;
    }

    arrow::flight::FlightClientOptions options;
    if (use_tls) {
        options.tls_root_certs = ReadFileContents(tls_cert_path);
        options.disable_server_verification = (tls_skip_verify != 0);
    }

    auto client_result = arrow::flight::FlightClient::Connect(*loc_result, options);
    if (!client_result.ok()) {
        set_error("Connection failed: " + client_result.status().ToString());
        return nullptr;
    }

    auto handle = new ClientHandle();
    handle->call_options = arrow::flight::FlightCallOptions{};

    // Authenticate with Basic Auth
    if (username && strlen(username) > 0) {
        auto auth_result = (*client_result)->AuthenticateBasicToken(
            handle->call_options, username, password ? password : "");
        if (!auth_result.ok()) {
            delete handle;
            set_error("Authentication failed: " + auth_result.status().ToString());
            return nullptr;
        }
        handle->call_options.headers.push_back(*auth_result);
    }

    handle->client = std::make_unique<arrow::flight::sql::FlightSqlClient>(
        std::move(*client_result));

    if (out_error) *out_error = nullptr;
    return handle;
}

/// Helper: get Arrow type name as a user-friendly string.
static std::string ArrowTypeName(const std::shared_ptr<arrow::DataType>& type) {
    if (!type) return "unknown";
    switch (type->id()) {
        case arrow::Type::BOOL: return "boolean";
        case arrow::Type::INT8: case arrow::Type::INT16:
        case arrow::Type::INT32: return "integer";
        case arrow::Type::INT64: return "bigint";
        case arrow::Type::UINT8: case arrow::Type::UINT16:
        case arrow::Type::UINT32: case arrow::Type::UINT64: return "uinteger";
        case arrow::Type::FLOAT: case arrow::Type::HALF_FLOAT: return "float";
        case arrow::Type::DOUBLE: return "double";
        case arrow::Type::STRING: case arrow::Type::LARGE_STRING: return "varchar";
        case arrow::Type::BINARY: case arrow::Type::LARGE_BINARY: return "blob";
        case arrow::Type::DATE32: case arrow::Type::DATE64: return "date";
        case arrow::Type::TIMESTAMP: return "timestamp";
        case arrow::Type::TIME32: case arrow::Type::TIME64: return "time";
        case arrow::Type::DECIMAL128: case arrow::Type::DECIMAL256: return "decimal";
        case arrow::Type::LIST: case arrow::Type::LARGE_LIST: return "list";
        case arrow::Type::STRUCT: return "struct";
        case arrow::Type::MAP: return "map";
        default: return type->ToString();
    }
}

/// Helper: get cell value as string.
static std::string GetCellString(const std::shared_ptr<arrow::ChunkedArray>& col,
                                  int64_t row) {
    int64_t offset = 0;
    for (const auto& chunk : col->chunks()) {
        if (row < offset + chunk->length()) {
            int64_t local = row - offset;
            if (chunk->IsNull(local)) return "NULL";
            auto scalar_result = chunk->GetScalar(local);
            if (!scalar_result.ok()) return "?";
            return (*scalar_result)->ToString();
        }
        offset += chunk->length();
    }
    return "?";
}

GizmoSQLQueryResult* gizmosql_client_execute(void* handle, const char* sql,
                                              int64_t max_rows) {
    auto* result = new GizmoSQLQueryResult{};
    result->rows_affected = -1;
    result->total_rows = -2;  // not applicable by default

    if (!handle || !sql) {
        result->error = strdup("Invalid handle or SQL");
        return result;
    }

    auto* ch = static_cast<ClientHandle*>(handle);
    auto start = std::chrono::steady_clock::now();

    // Try as a query first (Execute)
    auto info_result = ch->client->Execute(ch->call_options, sql);
    if (!info_result.ok()) {
        // Might be a DML statement — try ExecuteUpdate
        auto update_result = ch->client->ExecuteUpdate(ch->call_options, sql);
        auto end = std::chrono::steady_clock::now();
        result->elapsed_seconds = std::chrono::duration<double>(end - start).count();

        if (!update_result.ok()) {
            // Both failed — report the original Execute error
            result->error = strdup(info_result.status().message().c_str());
            return result;
        }
        result->rows_affected = *update_result;
        return result;
    }

    // Collect record batches, respecting max_rows
    auto& info = *info_result;
    int64_t server_total = info->total_records();  // -1 if unknown
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    std::shared_ptr<arrow::Schema> schema;
    int64_t rows_fetched = 0;
    bool hit_limit = false;

    for (const auto& endpoint : info->endpoints()) {
        if (hit_limit) break;

        auto stream_result = ch->client->DoGet(ch->call_options, endpoint.ticket);
        if (!stream_result.ok()) {
            result->error = strdup(stream_result.status().message().c_str());
            return result;
        }
        auto& stream = *stream_result;
        auto schema_result = stream->GetSchema();
        if (schema_result.ok() && !schema) schema = *schema_result;

        while (true) {
            auto chunk = stream->Next();
            if (!chunk.ok()) {
                result->error = strdup(chunk.status().message().c_str());
                return result;
            }
            if (chunk->data == nullptr) break;

            int64_t batch_rows = chunk->data->num_rows();
            if (max_rows > 0 && rows_fetched + batch_rows > max_rows) {
                // Slice the batch to fit
                int64_t take = max_rows - rows_fetched;
                if (take > 0) {
                    batches.push_back(chunk->data->Slice(0, take));
                    rows_fetched += take;
                }
                hit_limit = true;
                break;
            }
            batches.push_back(chunk->data);
            rows_fetched += batch_rows;
        }
    }

    auto end = std::chrono::steady_clock::now();
    result->elapsed_seconds = std::chrono::duration<double>(end - start).count();
    result->total_rows = server_total;
    result->truncated = hit_limit ? 1 : 0;

    if (!schema || batches.empty()) {
        // Query succeeded but returned no data (e.g., CREATE TABLE)
        if (schema) {
            result->num_columns = schema->num_fields();
            result->column_names = new const char*[result->num_columns];
            result->column_types = new const char*[result->num_columns];
            for (int i = 0; i < result->num_columns; i++) {
                result->column_names[i] = strdup(schema->field(i)->name().c_str());
                result->column_types[i] = strdup(
                    ArrowTypeName(schema->field(i)->type()).c_str());
            }
        }
        return result;
    }

    auto table_result = arrow::Table::FromRecordBatches(schema, batches);
    if (!table_result.ok()) {
        result->error = strdup(table_result.status().message().c_str());
        return result;
    }
    auto& table = *table_result;

    result->num_columns = table->num_columns();
    result->num_rows = table->num_rows();

    // Column names and types
    result->column_names = new const char*[result->num_columns];
    result->column_types = new const char*[result->num_columns];
    for (int i = 0; i < result->num_columns; i++) {
        result->column_names[i] = strdup(schema->field(i)->name().c_str());
        result->column_types[i] = strdup(
            ArrowTypeName(schema->field(i)->type()).c_str());
    }

    // Row data (row-major)
    int64_t total_cells = result->num_rows * result->num_columns;
    result->values = new const char*[total_cells];
    for (int64_t row = 0; row < result->num_rows; row++) {
        for (int col = 0; col < result->num_columns; col++) {
            auto val = GetCellString(table->column(col), row);
            result->values[row * result->num_columns + col] = strdup(val.c_str());
        }
    }

    return result;
}

void gizmosql_client_free_result(GizmoSQLQueryResult* result) {
    if (!result) return;
    for (int i = 0; i < result->num_columns; i++) {
        free(const_cast<char*>(result->column_names[i]));
        free(const_cast<char*>(result->column_types[i]));
    }
    int64_t total_cells = result->num_rows * result->num_columns;
    for (int64_t i = 0; i < total_cells; i++) {
        free(const_cast<char*>(result->values[i]));
    }
    delete[] result->column_names;
    delete[] result->column_types;
    delete[] result->values;
    free(const_cast<char*>(result->error));
    delete result;
}

void gizmosql_client_disconnect(void* handle) {
    if (!handle) return;
    auto* ch = static_cast<ClientHandle*>(handle);
    if (ch->client) {
        arrow::flight::CloseSessionRequest request;
        (void)ch->client->CloseSession(ch->call_options, request);
        (void)ch->client->Close();
    }
    delete ch;
}

}  // extern "C"
