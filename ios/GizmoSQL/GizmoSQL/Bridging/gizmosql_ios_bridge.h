// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.

#ifndef GIZMOSQL_IOS_BRIDGE_H
#define GIZMOSQL_IOS_BRIDGE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/// Server configuration passed from Swift to C.
/// NULL string fields use defaults; see individual field docs.
typedef struct {
    /// Database file path. NULL for in-memory database.
    const char* database_filename;

    /// Hostname to listen on. NULL defaults to "0.0.0.0".
    const char* hostname;

    /// Port for Flight SQL server. 0 defaults to 31337.
    int port;

    /// Username for basic auth. NULL defaults to "gizmosql_user".
    const char* username;

    /// Password for basic auth. Required — must not be NULL.
    const char* password;

    /// Secret key for JWT signing. NULL auto-generates a random key.
    const char* secret_key;

    /// Path to TLS certificate (PEM). NULL disables TLS.
    const char* tls_cert_path;

    /// Path to TLS private key (PEM). NULL disables TLS.
    const char* tls_key_path;

    /// Open database in read-only mode. 0 = read-write, 1 = read-only.
    int read_only;

    /// Print SQL queries to log. 0 = off, 1 = on.
    int print_queries;

    /// Log level: "debug", "info", "warn", "error". NULL defaults to "info".
    const char* log_level;

    /// Log format: "text" or "json". NULL defaults to "text".
    const char* log_format;

    /// Log file path. NULL logs to stderr.
    const char* log_file;

    /// Initial SQL commands to execute at startup. NULL for none.
    const char* init_sql_commands;

    /// Query timeout in seconds. 0 = unlimited.
    int query_timeout;

    /// Backend: 0 = DuckDB, 1 = SQLite.
    int backend;
} GizmoSQLServerConfig;

/// Start the GizmoSQL server. BLOCKING — call from a background thread.
/// Returns 0 on success, non-zero on error.
int gizmosql_server_start(const GizmoSQLServerConfig* config);

/// Request graceful server shutdown. Safe to call from any thread.
void gizmosql_server_request_shutdown(void);

/// Clean up global server resources after shutdown.
void gizmosql_server_cleanup(void);

/// Get the number of active client sessions. Thread-safe.
size_t gizmosql_server_active_sessions(void);

/// Get the server version string. Always safe to call.
const char* gizmosql_server_version(void);

// ---------------------------------------------------------------------------
// Embedded Flight SQL Client
// ---------------------------------------------------------------------------

/// Result of a query executed via the embedded Flight SQL client.
typedef struct {
    int num_columns;
    int64_t num_rows;
    const char** column_names;  ///< Array of num_columns strings.
    const char** column_types;  ///< Array of num_columns Arrow type strings.
    const char** values;        ///< Row-major: values[row * num_columns + col].
    const char* error;          ///< NULL on success, error message on failure.
    int64_t rows_affected;      ///< For DML statements (-1 if not applicable).
    int64_t total_rows;         ///< Server-reported total (-1 if unknown, -2 if not applicable).
    int truncated;              ///< 1 if results were truncated by max_rows limit.
    double elapsed_seconds;     ///< Wall-clock time for the query.
} GizmoSQLQueryResult;

/// Connect to a running GizmoSQL server via Flight SQL.
/// Returns an opaque handle (NULL on failure — check *out_error, caller must free).
void* gizmosql_client_connect(const char* host, int port,
                               const char* username, const char* password,
                               const char* tls_cert_path, int tls_skip_verify,
                               char** out_error);

/// Execute a SQL statement and return structured results.
/// max_rows limits the number of rows fetched (0 = unlimited).
/// Caller must free the result with gizmosql_client_free_result().
GizmoSQLQueryResult* gizmosql_client_execute(void* handle, const char* sql,
                                              int64_t max_rows);

/// Free a query result returned by gizmosql_client_execute().
void gizmosql_client_free_result(GizmoSQLQueryResult* result);

/// Disconnect and destroy the client handle.
void gizmosql_client_disconnect(void* handle);

#ifdef __cplusplus
}
#endif

#endif // GIZMOSQL_IOS_BRIDGE_H
