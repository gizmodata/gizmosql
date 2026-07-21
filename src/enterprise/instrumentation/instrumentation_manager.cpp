// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "instrumentation_manager.h"

#include <cstdlib>
#include <filesystem>

#include "gizmosql_logging.h"

namespace fs = std::filesystem;

namespace gizmosql::ddb {

std::string InstrumentationManager::GetDefaultDbPath(const std::string& database_filename) {
  const char* env_path = std::getenv("GIZMOSQL_INSTRUMENTATION_DB_PATH");
  if (env_path && env_path[0] != '\0') {
    return std::string(env_path);
  }

  // Determine the base directory for the instrumentation database
  fs::path base_dir = fs::current_path();

  if (!database_filename.empty()) {
    // Check if it's an in-memory database or uses extension syntax (e.g., "ducklake:...")
    bool is_memory = (database_filename == ":memory:");
    bool is_extension_syntax = (database_filename.find(':') != std::string::npos &&
                                 database_filename.find(':') > 0);  // Has colon but not at start

    if (!is_memory && !is_extension_syntax) {
      // Use the parent directory of the database file
      fs::path db_path(database_filename);
      if (db_path.has_parent_path()) {
        base_dir = db_path.parent_path();
      }
    }
  }

  return (base_dir / "gizmosql_instrumentation.db").string();
}

namespace {

// Template for schema SQL - {CATALOG} and {SCHEMA} will be replaced with actual values
// Uses VARCHAR with CHECK constraints instead of ENUMs for DuckLake compatibility
constexpr const char* kSchemaSQLTemplate = R"SQL(
-- Switch to the instrumentation database context for schema creation
USE {CATALOG}.{SCHEMA};

-- Server instance lifecycle
CREATE TABLE IF NOT EXISTS instances (
    instance_id UUID NOT NULL,  -- PRIMARY KEY (not supported in DuckLake)
    cluster_id UUID,  -- nullable: user-supplied cluster grouping (--cluster-id)
    gizmosql_version VARCHAR NOT NULL,
    gizmosql_edition VARCHAR NOT NULL,
    duckdb_version VARCHAR NOT NULL,
    arrow_version VARCHAR NOT NULL,
    hostname VARCHAR,
    hostname_arg VARCHAR,
    server_ip VARCHAR,
    port INTEGER,
    database_path VARCHAR,
    tls_enabled BOOLEAN NOT NULL,
    tls_cert_path VARCHAR,
    tls_key_path VARCHAR,
    mtls_required BOOLEAN NOT NULL,
    mtls_ca_cert_path VARCHAR,
    readonly BOOLEAN NOT NULL,
    -- System information
    os_platform VARCHAR,
    os_name VARCHAR,
    os_version VARCHAR,
    cpu_arch VARCHAR,
    cpu_model VARCHAR,
    cpu_count INTEGER,
    memory_total_bytes BIGINT,
    -- Timestamps and status
    -- TIMESTAMPTZ (TIMESTAMP WITH TIME ZONE) so callers can filter with
    -- `WHERE start_time > now() - INTERVAL '1 hour'` without an explicit cast
    -- (DuckDB's now() returns TIMESTAMPTZ).
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR NOT NULL,  -- CHECK (status IN ('running', 'stopped')) not supported in DuckLake
    stop_reason VARCHAR,
    instance_tag JSON
);

-- Client session lifecycle
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID NOT NULL,  -- PRIMARY KEY (not supported in DuckLake)
    instance_id UUID NOT NULL,
    username VARCHAR NOT NULL,
    role VARCHAR NOT NULL,
    auth_method VARCHAR NOT NULL,
    peer VARCHAR NOT NULL,
    peer_identity VARCHAR,
    user_agent VARCHAR,
    connection_protocol VARCHAR NOT NULL,  -- CHECK not supported in DuckLake
    start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    stop_time TIMESTAMP WITH TIME ZONE,
    status VARCHAR NOT NULL,  -- CHECK not supported in DuckLake
    stop_reason VARCHAR,
    session_tag JSON
);

-- SQL statement definitions (prepared statements)
CREATE TABLE IF NOT EXISTS sql_statements (
    statement_id UUID NOT NULL,  -- PRIMARY KEY (not supported in DuckLake)
    session_id UUID NOT NULL,
    sql_text VARCHAR NOT NULL,
    flight_method VARCHAR,
    is_internal BOOLEAN NOT NULL,
    prepare_success BOOLEAN NOT NULL,
    prepare_error VARCHAR,
    created_time TIMESTAMP WITH TIME ZONE NOT NULL,
    query_tag JSON
);

-- SQL statement executions (each execution of a statement)
CREATE TABLE IF NOT EXISTS sql_executions (
    execution_id UUID NOT NULL,  -- PRIMARY KEY (not supported in DuckLake)
    statement_id UUID NOT NULL,
    bind_parameters VARCHAR,
    execution_start_time TIMESTAMP WITH TIME ZONE NOT NULL,
    execution_end_time TIMESTAMP WITH TIME ZONE,  -- when the engine finished executing
    enqueue_time TIMESTAMP WITH TIME ZONE,  -- when the statement entered the admission queue (NULL if never queued)
    fetch_start_time TIMESTAMP WITH TIME ZONE,  -- first result batch delivered to the client (NULL if none)
    fetch_end_time TIMESTAMP WITH TIME ZONE,  -- last result batch delivered (>= execution_end_time)
    cursor_close_time TIMESTAMP WITH TIME ZONE,  -- client released the statement/stream
    rows_fetched BIGINT,
    status VARCHAR NOT NULL,  -- queued|executing|success|error|timeout|cancelled (CHECK not supported in DuckLake)
    error_message VARCHAR,
    duration_ms BIGINT,  -- engine execution time: execution_end_time - execution_start_time
    total_duration_ms BIGINT,  -- through result delivery: fetch_end_time - execution_start_time
    query_profile JSON  -- DuckDB native query profiling JSON (NULL unless capture enabled)
);

-- Schema migration: add tag columns to existing tables (safe to re-run)
ALTER TABLE instances ADD COLUMN IF NOT EXISTS instance_tag JSON;
ALTER TABLE instances ADD COLUMN IF NOT EXISTS cluster_id UUID;
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS session_tag JSON;
ALTER TABLE sql_statements ADD COLUMN IF NOT EXISTS query_tag JSON;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS enqueue_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS query_profile JSON;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS fetch_start_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS fetch_end_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS cursor_close_time TIMESTAMP WITH TIME ZONE;
ALTER TABLE sql_executions ADD COLUMN IF NOT EXISTS total_duration_ms BIGINT;

-- Indexes not supported in DuckLake, but would be useful for file-based mode:
-- CREATE INDEX IF NOT EXISTS idx_sessions_instance_id ON sessions(instance_id);
-- CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON sessions(start_time);
-- CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
-- CREATE INDEX IF NOT EXISTS idx_sql_statements_session_id ON sql_statements(session_id);
-- CREATE INDEX IF NOT EXISTS idx_sql_statements_created_time ON sql_statements(created_time);
-- CREATE INDEX IF NOT EXISTS idx_sql_executions_statement_id ON sql_executions(statement_id);
-- CREATE INDEX IF NOT EXISTS idx_sql_executions_execution_start_time ON sql_executions(execution_start_time);
-- CREATE INDEX IF NOT EXISTS idx_sql_executions_status ON sql_executions(status);

-- View: Complete session activity (with executions)
CREATE OR REPLACE VIEW session_activity AS
SELECT
    i.instance_id,
    i.gizmosql_version,
    i.gizmosql_edition,
    i.duckdb_version,
    i.arrow_version,
    i.hostname,
    i.hostname_arg,
    i.server_ip,
    i.port,
    i.database_path,
    i.start_time AS instance_start_time,
    i.stop_time AS instance_stop_time,
    i.status AS instance_status,
    i.stop_reason AS instance_stop_reason,
    i.instance_tag,
    s.session_id,
    s.username,
    s.role,
    s.auth_method,
    s.peer,
    s.start_time AS session_start_time,
    s.stop_time AS session_stop_time,
    s.status AS session_status,
    s.stop_reason AS session_stop_reason,
    s.session_tag,
    st.statement_id,
    st.sql_text,
    st.created_time AS statement_created_time,
    e.execution_id,
    e.bind_parameters,
    e.execution_start_time,
    e.execution_end_time,
    e.rows_fetched,
    e.status AS execution_status,
    e.error_message,
    e.duration_ms,
    e.query_profile,
    st.query_tag,
    i.cluster_id,
    e.fetch_start_time,
    e.fetch_end_time,
    e.cursor_close_time,
    e.total_duration_ms
FROM instances i
LEFT JOIN sessions s ON i.instance_id = s.instance_id
LEFT JOIN sql_statements st ON s.session_id = st.session_id
LEFT JOIN sql_executions e ON st.statement_id = e.statement_id;

-- View: Currently active sessions
CREATE OR REPLACE VIEW active_sessions AS
SELECT
    s.session_id,
    s.instance_id,
    s.username,
    s.role,
    s.auth_method,
    s.peer,
    s.peer_identity,
    s.user_agent,
    s.connection_protocol,
    s.start_time,
    s.status,
    i.hostname,
    i.hostname_arg,
    i.server_ip,
    i.port,
    i.database_path,
    s.session_tag,
    i.instance_tag,
    EPOCH(now()) - EPOCH(s.start_time) AS session_duration_seconds,
    i.cluster_id
FROM sessions s
JOIN instances i ON s.instance_id = i.instance_id
WHERE s.status = 'active'
  AND i.status = 'running';

-- View: Session statistics (aggregated from executions)
CREATE OR REPLACE VIEW session_stats AS
SELECT
    s.session_id,
    s.instance_id,
    s.username,
    s.role,
    s.auth_method,
    s.peer,
    s.start_time,
    s.stop_time,
    s.status AS session_status,
    COUNT(DISTINCT st.statement_id) AS total_statements,
    COUNT(e.execution_id) AS total_executions,
    SUM(CASE WHEN e.status = 'success' THEN 1 ELSE 0 END) AS successful_executions,
    SUM(CASE WHEN e.status = 'error' THEN 1 ELSE 0 END) AS failed_executions,
    SUM(CASE WHEN e.status = 'timeout' THEN 1 ELSE 0 END) AS timed_out_executions,
    SUM(CASE WHEN e.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_executions,
    SUM(e.rows_fetched) AS total_rows_fetched,
    AVG(e.duration_ms) AS avg_duration_ms,
    MAX(e.duration_ms) AS max_duration_ms,
    AVG(e.total_duration_ms) AS avg_total_duration_ms,
    MAX(e.total_duration_ms) AS max_total_duration_ms
FROM sessions s
LEFT JOIN sql_statements st ON s.session_id = st.session_id
LEFT JOIN sql_executions e ON st.statement_id = e.statement_id
GROUP BY ALL;

-- View: Execution details (joins statement with execution for convenience)
CREATE OR REPLACE VIEW execution_details AS
SELECT
    e.execution_id,
    e.statement_id,
    st.session_id,
    s.instance_id,
    st.sql_text,
    e.bind_parameters,
    e.execution_start_time,
    e.execution_end_time,
    e.enqueue_time,
    CASE WHEN e.enqueue_time IS NOT NULL
         THEN CAST(date_diff('millisecond', e.enqueue_time, e.execution_start_time) AS BIGINT)
    END AS queue_wait_ms,
    e.rows_fetched,
    e.status,
    e.error_message,
    e.duration_ms,
    e.query_profile,
    s.username,
    s.auth_method,
    s.peer,
    st.query_tag,
    s.session_tag,
    e.fetch_start_time,
    e.fetch_end_time,
    e.cursor_close_time,
    e.total_duration_ms
FROM sql_executions e
JOIN sql_statements st ON e.statement_id = st.statement_id
JOIN sessions s ON st.session_id = s.session_id;
)SQL";

/// Generate schema SQL with actual catalog and schema names
std::string GetSchemaSQL(const std::string& catalog, const std::string& schema) {
  std::string sql = kSchemaSQLTemplate;
  // Replace {CATALOG} with actual catalog name
  size_t pos = 0;
  while ((pos = sql.find("{CATALOG}", pos)) != std::string::npos) {
    sql.replace(pos, 9, catalog);
    pos += catalog.length();
  }
  // Replace {SCHEMA} with actual schema name
  pos = 0;
  while ((pos = sql.find("{SCHEMA}", pos)) != std::string::npos) {
    sql.replace(pos, 8, schema);
    pos += schema.length();
  }
  return sql;
}

// ---------------------------------------------------------------------------
// Full relational schema (file-based DuckDB and PostgreSQL)
//
// The DuckLake template above is intentionally constraint-free (DuckLake cannot
// enforce keys or indexes). Backends that DO support them — file-based DuckDB
// and plain PostgreSQL — get primary keys, foreign keys, CHECK constraints on
// status columns, and indexes on foreign-key + hot query columns. The schema is
// built from a single source of truth, parameterized by a qualification
// `prefix`, so the two backends never drift:
//   * DuckDB: prefix = "" — statements run after `USE <catalog>.<schema>`.
//   * PostgreSQL: prefix = "<schema>." — statements are schema-qualified and run
//     as native PG SQL via postgres_execute().
// Column types (UUID, VARCHAR, TIMESTAMP WITH TIME ZONE, BOOLEAN, INTEGER,
// BIGINT, JSON) and CHECK/EXTRACT(EPOCH ...) expressions are valid on both
// engines, so the same text works verbatim for each.

/// The four convenience views, parameterized by qualification prefix. Uses only
/// portable SQL: EXTRACT(EPOCH FROM ...) (not DuckDB's epoch()/date_diff()) and
/// an explicit GROUP BY (not GROUP BY ALL), so it creates on PostgreSQL too.
std::vector<std::string> BuildViewStatements(const std::string& p) {
  return {
      "CREATE OR REPLACE VIEW " + p + "session_activity AS SELECT "
      "i.instance_id, i.gizmosql_version, i.gizmosql_edition, i.duckdb_version, "
      "i.arrow_version, i.hostname, i.hostname_arg, i.server_ip, i.port, "
      "i.database_path, i.start_time AS instance_start_time, "
      "i.stop_time AS instance_stop_time, i.status AS instance_status, "
      "i.stop_reason AS instance_stop_reason, i.instance_tag, s.session_id, "
      "s.username, s.role, s.auth_method, s.peer, "
      "s.start_time AS session_start_time, s.stop_time AS session_stop_time, "
      "s.status AS session_status, s.stop_reason AS session_stop_reason, "
      "s.session_tag, st.statement_id, st.sql_text, "
      "st.created_time AS statement_created_time, e.execution_id, "
      "e.bind_parameters, e.execution_start_time, e.execution_end_time, "
      "e.rows_fetched, e.status AS execution_status, e.error_message, "
      "e.duration_ms, e.query_profile, st.query_tag, i.cluster_id, "
      "e.fetch_start_time, e.fetch_end_time, e.cursor_close_time, "
      "e.total_duration_ms "
      "FROM " + p + "instances i "
      "LEFT JOIN " + p + "sessions s ON i.instance_id = s.instance_id "
      "LEFT JOIN " + p + "sql_statements st ON s.session_id = st.session_id "
      "LEFT JOIN " + p + "sql_executions e ON st.statement_id = e.statement_id",

      "CREATE OR REPLACE VIEW " + p + "active_sessions AS SELECT "
      "s.session_id, s.instance_id, s.username, s.role, s.auth_method, s.peer, "
      "s.peer_identity, s.user_agent, s.connection_protocol, s.start_time, "
      "s.status, i.hostname, i.hostname_arg, i.server_ip, i.port, "
      "i.database_path, s.session_tag, i.instance_tag, "
      "EXTRACT(EPOCH FROM (now() - s.start_time)) AS session_duration_seconds, "
      "i.cluster_id "
      "FROM " + p + "sessions s "
      "JOIN " + p + "instances i ON s.instance_id = i.instance_id "
      "WHERE s.status = 'active' AND i.status = 'running'",

      "CREATE OR REPLACE VIEW " + p + "session_stats AS SELECT "
      "s.session_id, s.instance_id, s.username, s.role, s.auth_method, s.peer, "
      "s.start_time, s.stop_time, s.status AS session_status, "
      "COUNT(DISTINCT st.statement_id) AS total_statements, "
      "COUNT(e.execution_id) AS total_executions, "
      "SUM(CASE WHEN e.status = 'success' THEN 1 ELSE 0 END) AS successful_executions, "
      "SUM(CASE WHEN e.status = 'error' THEN 1 ELSE 0 END) AS failed_executions, "
      "SUM(CASE WHEN e.status = 'timeout' THEN 1 ELSE 0 END) AS timed_out_executions, "
      "SUM(CASE WHEN e.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_executions, "
      "SUM(e.rows_fetched) AS total_rows_fetched, "
      "AVG(e.duration_ms) AS avg_duration_ms, MAX(e.duration_ms) AS max_duration_ms, "
      "AVG(e.total_duration_ms) AS avg_total_duration_ms, "
      "MAX(e.total_duration_ms) AS max_total_duration_ms "
      "FROM " + p + "sessions s "
      "LEFT JOIN " + p + "sql_statements st ON s.session_id = st.session_id "
      "LEFT JOIN " + p + "sql_executions e ON st.statement_id = e.statement_id "
      "GROUP BY s.session_id, s.instance_id, s.username, s.role, s.auth_method, "
      "s.peer, s.start_time, s.stop_time, s.status",

      "CREATE OR REPLACE VIEW " + p + "execution_details AS SELECT "
      "e.execution_id, e.statement_id, st.session_id, s.instance_id, st.sql_text, "
      "e.bind_parameters, e.execution_start_time, e.execution_end_time, "
      "e.enqueue_time, CASE WHEN e.enqueue_time IS NOT NULL THEN "
      "CAST(EXTRACT(EPOCH FROM (e.execution_start_time - e.enqueue_time)) * 1000 AS BIGINT) "
      "END AS queue_wait_ms, e.rows_fetched, e.status, e.error_message, "
      "e.duration_ms, e.query_profile, s.username, s.auth_method, s.peer, "
      "st.query_tag, s.session_tag, "
      "e.fetch_start_time, e.fetch_end_time, e.cursor_close_time, "
      "e.total_duration_ms "
      "FROM " + p + "sql_executions e "
      "JOIN " + p + "sql_statements st ON e.statement_id = st.statement_id "
      "JOIN " + p + "sessions s ON st.session_id = s.session_id",
  };
}

/// All schema statements (tables, additive migrations, indexes, views) for a
/// constraint-capable backend.
///
/// Parameters:
///  - `p`: qualification prefix ("" for DuckDB after USE, "<schema>." for PG).
///  - `json_type`: type for JSON-valued columns. DuckDB uses its native JSON
///    type; PostgreSQL uses VARCHAR, because DuckDB's postgres extension cannot
///    bind a VARCHAR value to a PG json column in an UPDATE (the values are JSON
///    strings either way — query them on PG with a `::json` cast).
///  - `with_fk`: emit FOREIGN KEY clauses. Enabled for PostgreSQL; DISABLED for
///    file-based DuckDB, because DuckDB implements UPDATE as delete+insert and
///    so rejects updates to a parent row that is still referenced by a child
///    (its documented foreign-key limitation) — and the instrumentation
///    lifecycle updates parent rows (session/instance stop, stale cleanup).
std::vector<std::string> BuildSchemaStatements(const std::string& p,
                                               const std::string& json_type,
                                               bool with_fk) {
  // ON DELETE CASCADE so retention pruning is a single delete on the parent:
  // e.g. DELETE FROM <catalog>.<schema>.instances WHERE stop_time < <cutoff>
  // cascades to that instance's sessions, statements, and executions.
  auto fk = [&](const char* parent, const char* col) -> std::string {
    return with_fk ? (" REFERENCES " + p + parent + "(" + col + ") ON DELETE CASCADE")
                   : std::string();
  };
  std::vector<std::string> stmts = {
      "CREATE TABLE IF NOT EXISTS " + p + "instances ("
      "instance_id UUID PRIMARY KEY, cluster_id UUID, gizmosql_version VARCHAR NOT NULL, "
      "gizmosql_edition VARCHAR NOT NULL, duckdb_version VARCHAR NOT NULL, "
      "arrow_version VARCHAR NOT NULL, hostname VARCHAR, hostname_arg VARCHAR, "
      "server_ip VARCHAR, port INTEGER, database_path VARCHAR, "
      "tls_enabled BOOLEAN NOT NULL, tls_cert_path VARCHAR, tls_key_path VARCHAR, "
      "mtls_required BOOLEAN NOT NULL, mtls_ca_cert_path VARCHAR, "
      "readonly BOOLEAN NOT NULL, os_platform VARCHAR, os_name VARCHAR, "
      "os_version VARCHAR, cpu_arch VARCHAR, cpu_model VARCHAR, cpu_count INTEGER, "
      "memory_total_bytes BIGINT, start_time TIMESTAMP WITH TIME ZONE NOT NULL, "
      "stop_time TIMESTAMP WITH TIME ZONE, "
      "status VARCHAR NOT NULL CHECK (status IN ('running', 'stopped')), "
      "stop_reason VARCHAR, instance_tag JSON)",

      "CREATE TABLE IF NOT EXISTS " + p + "sessions ("
      "session_id UUID PRIMARY KEY, "
      "instance_id UUID NOT NULL" + fk("instances", "instance_id") + ", "
      "username VARCHAR NOT NULL, role VARCHAR NOT NULL, auth_method VARCHAR NOT NULL, "
      "peer VARCHAR NOT NULL, peer_identity VARCHAR, user_agent VARCHAR, "
      "connection_protocol VARCHAR NOT NULL, "
      "start_time TIMESTAMP WITH TIME ZONE NOT NULL, "
      "stop_time TIMESTAMP WITH TIME ZONE, "
      "status VARCHAR NOT NULL CHECK (status IN ('active', 'closed', 'killed', 'timeout', 'error')), "
      "stop_reason VARCHAR, session_tag JSON)",

      "CREATE TABLE IF NOT EXISTS " + p + "sql_statements ("
      "statement_id UUID PRIMARY KEY, "
      "session_id UUID NOT NULL" + fk("sessions", "session_id") + ", "
      "sql_text VARCHAR NOT NULL, flight_method VARCHAR, is_internal BOOLEAN NOT NULL, "
      "prepare_success BOOLEAN NOT NULL, prepare_error VARCHAR, "
      "created_time TIMESTAMP WITH TIME ZONE NOT NULL, query_tag JSON)",

      "CREATE TABLE IF NOT EXISTS " + p + "sql_executions ("
      "execution_id UUID PRIMARY KEY, "
      "statement_id UUID NOT NULL" + fk("sql_statements", "statement_id") + ", "
      "bind_parameters VARCHAR, "
      "execution_start_time TIMESTAMP WITH TIME ZONE NOT NULL, "
      "execution_end_time TIMESTAMP WITH TIME ZONE, "
      "enqueue_time TIMESTAMP WITH TIME ZONE, "
      "fetch_start_time TIMESTAMP WITH TIME ZONE, "
      "fetch_end_time TIMESTAMP WITH TIME ZONE, "
      "cursor_close_time TIMESTAMP WITH TIME ZONE, rows_fetched BIGINT, "
      "status VARCHAR NOT NULL CHECK (status IN ('queued', 'executing', 'success', 'error', 'timeout', 'cancelled')), "
      "error_message VARCHAR, duration_ms BIGINT, total_duration_ms BIGINT, "
      "query_profile JSON)",

      // Additive migrations (safe to re-run on an existing schema)
      "ALTER TABLE " + p + "instances ADD COLUMN IF NOT EXISTS instance_tag JSON",
      "ALTER TABLE " + p + "instances ADD COLUMN IF NOT EXISTS cluster_id UUID",
      "ALTER TABLE " + p + "sessions ADD COLUMN IF NOT EXISTS session_tag JSON",
      "ALTER TABLE " + p + "sql_statements ADD COLUMN IF NOT EXISTS query_tag JSON",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS enqueue_time TIMESTAMP WITH TIME ZONE",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS query_profile JSON",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS fetch_start_time TIMESTAMP WITH TIME ZONE",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS fetch_end_time TIMESTAMP WITH TIME ZONE",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS cursor_close_time TIMESTAMP WITH TIME ZONE",
      "ALTER TABLE " + p + "sql_executions ADD COLUMN IF NOT EXISTS total_duration_ms BIGINT",

      // Indexes on foreign-key (child) columns, status, and every TIMESTAMPTZ
      // lifecycle column. Indexing the timestamps keeps time-range queries and
      // time-based retention deletes fast — in particular the stop_time /
      // execution_end_time columns used to prune old data (e.g. DELETE FROM
      // instances WHERE stop_time < <cutoff>, which cascades to children).
      "CREATE INDEX IF NOT EXISTS idx_instances_start_time ON " + p + "instances(start_time)",
      "CREATE INDEX IF NOT EXISTS idx_instances_stop_time ON " + p + "instances(stop_time)",
      "CREATE INDEX IF NOT EXISTS idx_sessions_instance_id ON " + p + "sessions(instance_id)",
      "CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON " + p + "sessions(start_time)",
      "CREATE INDEX IF NOT EXISTS idx_sessions_stop_time ON " + p + "sessions(stop_time)",
      "CREATE INDEX IF NOT EXISTS idx_sessions_status ON " + p + "sessions(status)",
      "CREATE INDEX IF NOT EXISTS idx_sql_statements_session_id ON " + p + "sql_statements(session_id)",
      "CREATE INDEX IF NOT EXISTS idx_sql_statements_created_time ON " + p + "sql_statements(created_time)",
      "CREATE INDEX IF NOT EXISTS idx_sql_executions_statement_id ON " + p + "sql_executions(statement_id)",
      "CREATE INDEX IF NOT EXISTS idx_sql_executions_execution_start_time ON " + p + "sql_executions(execution_start_time)",
      "CREATE INDEX IF NOT EXISTS idx_sql_executions_execution_end_time ON " + p + "sql_executions(execution_end_time)",
      "CREATE INDEX IF NOT EXISTS idx_sql_executions_enqueue_time ON " + p + "sql_executions(enqueue_time)",
      "CREATE INDEX IF NOT EXISTS idx_sql_executions_status ON " + p + "sql_executions(status)",
  };
  // Substitute the JSON column type (the literals above use " JSON", which
  // only ever appears as a type token here — never in a value or identifier).
  if (json_type != "JSON") {
    const std::string from = " JSON";
    const std::string to = " " + json_type;
    for (auto& stmt : stmts) {
      size_t pos = 0;
      while ((pos = stmt.find(from, pos)) != std::string::npos) {
        stmt.replace(pos, from.size(), to);
        pos += to.size();
      }
    }
  }

  auto views = BuildViewStatements(p);
  stmts.insert(stmts.end(), views.begin(), views.end());
  return stmts;
}

/// Full relational schema as a single multi-statement DuckDB script (file-based
/// backend). Statements are unqualified and rely on the leading USE.
std::string GetDuckDBSchemaSQL(const std::string& catalog, const std::string& schema) {
  std::string sql = "USE " + catalog + "." + schema + ";\n";
  // No foreign keys on DuckDB: it implements UPDATE as delete+insert and rejects
  // updates to a referenced parent row, which the instrumentation lifecycle does.
  for (const auto& stmt :
       BuildSchemaStatements(/*prefix=*/"", /*json_type=*/"JSON", /*with_fk=*/false)) {
    sql += stmt + ";\n";
  }
  return sql;
}

}  // namespace

InstrumentationManager::InstrumentationManager(
    const std::string& db_path,
    const std::string& catalog,
    const std::string& schema,
    bool use_external_catalog,
    std::shared_ptr<duckdb::DuckDB> db_instance,
    std::unique_ptr<gizmosql::TrackedDuckDBConnection> writer_connection)
    : db_path_(db_path),
      catalog_(catalog),
      schema_(schema),
      use_external_catalog_(use_external_catalog),
      db_instance_(std::move(db_instance)),
      writer_connection_(std::move(writer_connection)) {}

InstrumentationManager::~InstrumentationManager() { Shutdown(); }

arrow::Result<std::shared_ptr<InstrumentationManager>> InstrumentationManager::Create(
    std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path,
    const std::string& catalog, const std::string& schema,
    bool use_external_catalog) {
  if (!db_instance) {
    return arrow::Status::Invalid("InstrumentationManager requires a valid DuckDB instance");
  }

  if (use_external_catalog) {
    GIZMOSQL_LOG(INFO) << "Initializing instrumentation manager with external catalog: "
                       << catalog << "." << schema;
  } else {
    GIZMOSQL_LOG(INFO) << "Initializing instrumentation manager with database at: " << db_path;
  }

  try {
    // Create a dedicated connection for instrumentation writes
    auto writer_connection = std::make_unique<gizmosql::TrackedDuckDBConnection>(*db_instance);

    if (!use_external_catalog) {
      // File-based mode: ATTACH the instrumentation database on this connection
      // (ATTACH is connection-specific in DuckDB)
      // (READ_WRITE) is required so instrumentation can record metrics even
      // when the main DuckDB database is opened in read-only mode.
      auto attach_result = writer_connection->Get().Query(
          "ATTACH IF NOT EXISTS '" + db_path + "' AS " + catalog + " (READ_WRITE)");
      if (attach_result->HasError()) {
        return arrow::Status::Invalid("Failed to attach instrumentation database: ",
                                      attach_result->GetError());
      }
    }

    // Check for schema compatibility - if the instances table exists but lacks new columns,
    // the user has an old schema and needs to rename their instrumentation database file.
    auto schema_check = writer_connection->Get().Query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = '" + catalog + "' AND table_name = 'instances' AND column_name = 'os_platform'");
    // Detect the pre-TIMESTAMPTZ schema, where start_time was a naive TIMESTAMP.
    // Legacy values were written by `now()` and stripped of tz on insert, so we
    // interpret them as UTC. We migrate via stage-drop-restore (not ALTER
    // COLUMN ... USING) because DuckLake rejects type changes that use an
    // expression. The plan, executed across the InitializeSchema call below:
    //   1. Stage each legacy table into a TEMP table with timing columns cast
    //      `<col> AT TIME ZONE 'UTC'` (yielding TIMESTAMPTZ).
    //   2. Drop the dependent views and the legacy tables.
    //   3. InitializeSchema() recreates fresh tz-aware tables + views.
    //   4. Restore rows from each TEMP table via `INSERT ... BY NAME`.
    // Note on parameterization: WHERE clauses bind values (catalog/table/column
    // names are compared as strings); DDL below interpolates the same names as
    // identifiers, which SQL does not allow parameters for.
    auto tz_check_stmt = writer_connection->Get().Prepare(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_catalog = $1 AND table_name = $2 AND column_name = $3");
    duckdb::vector<duckdb::Value> tz_check_args{
        duckdb::Value(catalog), duckdb::Value("instances"),
        duckdb::Value("start_time")};
    auto tz_check_raw =
        tz_check_stmt->Execute(tz_check_args, /*allow_stream_result=*/false);
    auto& tz_check = tz_check_raw->Cast<duckdb::MaterializedQueryResult>();
    // Tables (and their timing columns) we may need to restore from TEMP
    // staging after InitializeSchema reconstructs the schema.
    struct LegacyTable {
      std::string table;
      std::vector<std::string> tz_cols;
    };
    std::vector<LegacyTable> staged_for_restore;
    if (!schema_check->HasError()) {
      // Check if instances table exists (by checking for any column)
      auto table_exists = writer_connection->Get().Query(
          "SELECT column_name FROM information_schema.columns "
          "WHERE table_catalog = '" + catalog + "' AND table_name = 'instances' LIMIT 1");
      if (!table_exists->HasError() && table_exists->RowCount() > 0) {
        // Table exists - check timestamp column type for tz-awareness
        if (!tz_check.HasError() && tz_check.RowCount() > 0) {
          std::string start_time_type = tz_check.GetValue(0, 0).ToString();
          if (start_time_type.find("WITH TIME ZONE") == std::string::npos) {
            GIZMOSQL_LOG(INFO)
                << "Migrating instrumentation timing columns from TIMESTAMP to "
                   "TIMESTAMP WITH TIME ZONE (interpreting existing values as UTC) in "
                << (use_external_catalog ? ("catalog " + catalog + "." + schema)
                                         : ("database " + db_path));

            const std::string prefix =
                catalog + (use_external_catalog ? ("." + schema) : "");

            // Drop dependent views first (recreated by InitializeSchema below).
            for (const char* view : {"execution_details", "session_stats",
                                     "active_sessions", "session_activity"}) {
              auto drop_result = writer_connection->Get().Query(
                  std::string("DROP VIEW IF EXISTS ") + prefix + "." + view);
              if (drop_result->HasError()) {
                GIZMOSQL_LOG(WARNING)
                    << "Failed to drop view " << view << " during TZ migration: "
                    << drop_result->GetError();
              }
            }

            const std::vector<LegacyTable> tables = {
                {"instances", {"start_time", "stop_time"}},
                {"sessions", {"start_time", "stop_time"}},
                {"sql_statements", {"created_time"}},
                {"sql_executions", {"execution_start_time", "execution_end_time"}},
            };
            for (const auto& tbl : tables) {
              // Skip if this legacy table doesn't exist (partial schema) or has
              // already been migrated to TIMESTAMPTZ (idempotent re-run).
              auto col_check_stmt = writer_connection->Get().Prepare(
                  "SELECT data_type FROM information_schema.columns "
                  "WHERE table_catalog = $1 AND table_name = $2 AND column_name = $3");
              duckdb::vector<duckdb::Value> col_check_args{
                  duckdb::Value(catalog), duckdb::Value(tbl.table),
                  duckdb::Value(tbl.tz_cols.front())};
              auto col_check_raw = col_check_stmt->Execute(
                  col_check_args, /*allow_stream_result=*/false);
              auto& col_check =
                  col_check_raw->Cast<duckdb::MaterializedQueryResult>();
              if (col_check.HasError() || col_check.RowCount() == 0) continue;
              if (col_check.GetValue(0, 0).ToString().find("WITH TIME ZONE") !=
                  std::string::npos) {
                continue;
              }

              // Build: SELECT * EXCLUDE (<tz cols>), <col> AT TIME ZONE 'UTC' AS <col>, ...
              std::string exclude_list;
              std::string tz_select;
              for (size_t i = 0; i < tbl.tz_cols.size(); ++i) {
                if (i > 0) {
                  exclude_list += ", ";
                  tz_select += ", ";
                }
                exclude_list += tbl.tz_cols[i];
                tz_select += tbl.tz_cols[i] + " AT TIME ZONE 'UTC' AS " +
                             tbl.tz_cols[i];
              }
              const std::string staging = "_gizmosql_mig_" + tbl.table;
              std::string stage_sql = "CREATE TEMP TABLE " + staging +
                                      " AS SELECT * EXCLUDE (" + exclude_list +
                                      "), " + tz_select + " FROM " + prefix + "." +
                                      tbl.table;
              auto stage_r = writer_connection->Get().Query(stage_sql);
              if (stage_r->HasError()) {
                return arrow::Status::Invalid(
                    "Failed to stage legacy rows from ", tbl.table,
                    " during TIMESTAMPTZ migration: ", stage_r->GetError());
              }
              auto drop_r = writer_connection->Get().Query("DROP TABLE " + prefix +
                                                            "." + tbl.table);
              if (drop_r->HasError()) {
                return arrow::Status::Invalid(
                    "Failed to drop legacy table ", tbl.table,
                    " during TIMESTAMPTZ migration: ", drop_r->GetError());
              }
              staged_for_restore.push_back(tbl);
            }
          }
        }
        // Table exists - check if os_platform column exists
        if (schema_check->RowCount() == 0) {
          // Old schema detected - missing new columns
          if (use_external_catalog) {
            return arrow::Status::Invalid(
                "Instrumentation schema is outdated in catalog '", catalog, ".", schema,
                "'. The schema was created with an older version of GizmoSQL.\n"
                "Please drop the existing instrumentation tables from the catalog, "
                "and GizmoSQL will create new tables with the updated schema.");
          } else {
            return arrow::Status::Invalid(
                "Instrumentation database schema is outdated. The database at '", db_path,
                "' was created with an older version of GizmoSQL.\n"
                "Please rename or move the existing instrumentation database file to preserve your data, "
                "and GizmoSQL will create a new database with the updated schema.\n"
                "Example: mv '", db_path, "' '", db_path, ".backup'");
          }
        }
      }
    }

    auto manager = std::shared_ptr<InstrumentationManager>(new InstrumentationManager(
        db_path, catalog, schema, use_external_catalog,
        std::move(db_instance), std::move(writer_connection)));

    // Resolve the storage backend now that the catalog is attached; it selects
    // the schema dialect in InitializeSchema().
    manager->backend_ = manager->DetectBackend();
    if (manager->backend_ == Backend::kDuckLake) {
      GIZMOSQL_LOG(WARNING)
          << "DuckLake-backed instrumentation is DEPRECATED and not recommended: "
             "concurrent writes from multiple GizmoSQL instances to a shared "
             "DuckLake catalog can lose UPDATEs (e.g. execution finalization), "
             "leaving records permanently stuck (e.g. status='executing'). Use the "
             "default file-based instrumentation, or a plain PostgreSQL catalog "
             "(--instrumentation-catalog pointing at an attached postgres "
             "database) for multi-instance deployments. DuckLake support remains "
             "until the upstream concurrent-UPDATE issue is resolved.";
    }

    ARROW_RETURN_NOT_OK(manager->InitializeSchema());

    // Restore staged rows from the TIMESTAMPTZ migration. InitializeSchema has
    // recreated each table with tz-aware columns; we INSERT BY NAME so the
    // staged-column order is irrelevant.
    if (!staged_for_restore.empty()) {
      const std::string prefix =
          catalog + (use_external_catalog ? ("." + schema) : "");
      for (const auto& tbl : staged_for_restore) {
        const std::string staging = "_gizmosql_mig_" + tbl.table;
        auto restore_r = manager->writer_connection_->Get().Query(
            "INSERT INTO " + prefix + "." + tbl.table +
            " BY NAME SELECT * FROM " + staging);
        if (restore_r->HasError()) {
          return arrow::Status::Invalid(
              "Failed to restore legacy rows into ", tbl.table,
              " after TIMESTAMPTZ migration: ", restore_r->GetError());
        }
        manager->writer_connection_->Get().Query("DROP TABLE " + staging);
      }
      GIZMOSQL_LOG(INFO) << "Restored legacy rows into " << staged_for_restore.size()
                         << " table(s) after TIMESTAMPTZ migration";
    }

    ARROW_RETURN_NOT_OK(manager->CleanupStaleRecords());

    manager->writer_thread_ = std::thread(&InstrumentationManager::WriterThreadLoop, manager.get());

    GIZMOSQL_LOG(INFO) << "Instrumentation manager initialized successfully";
    return manager;

  } catch (const duckdb::Exception& ex) {
    return arrow::Status::Invalid("Failed to create instrumentation manager: ",
                                  ex.what());
  } catch (const std::exception& ex) {
    return arrow::Status::Invalid("Failed to create instrumentation manager: ", ex.what());
  }
}

InstrumentationManager::Backend InstrumentationManager::DetectBackend() {
  // Delegate to the shared detector (the same logic the catalog log sink uses).
  return DetectCatalogBackend(writer_connection_->Get(), catalog_);
}

arrow::Status InstrumentationManager::InitializeSchema() {
  try {
    // Ensure the json extension is loaded (required for JSON column type)
    auto json_result = writer_connection_->Get().Query("INSTALL json; LOAD json;");
    if (json_result->HasError()) {
      return arrow::Status::Invalid("Failed to load json extension: ",
                                    json_result->GetError());
    }

    // PostgreSQL gets the full relational schema built with native PG SQL via
    // postgres_execute() (see InitializePostgresSchema for why).
    if (backend_ == Backend::kPostgres) {
      return InitializePostgresSchema();
    }

    // File-based DuckDB gets the full relational schema (keys, CHECKs,
    // indexes); DuckLake keeps the constraint-free schema (it cannot enforce
    // them). Both run as a single multi-statement DuckDB script.
    const std::string schema_sql = (backend_ == Backend::kDuckLake)
                                       ? GetSchemaSQL(catalog_, schema_)
                                       : GetDuckDBSchemaSQL(catalog_, schema_);
    auto result = writer_connection_->Get().Query(schema_sql);
    if (result->HasError()) {
      return arrow::Status::Invalid("Failed to initialize instrumentation schema: ",
                                    result->GetError());
    }
    return arrow::Status::OK();
  } catch (const duckdb::Exception& ex) {
    return arrow::Status::Invalid("Failed to initialize instrumentation schema: ",
                                  ex.what());
  }
}

arrow::Status InstrumentationManager::InitializePostgresSchema() {
  auto& conn = writer_connection_->Get();

  // Ensure the target schema exists, then create every relational object with
  // native PostgreSQL SQL (schema-qualified) so DuckDB never has to translate
  // the DDL.
  ARROW_RETURN_NOT_OK(
      RunPostgresDDL(conn, catalog_, "CREATE SCHEMA IF NOT EXISTS " + schema_));
  for (const auto& stmt :
       BuildSchemaStatements(schema_ + ".", /*json_type=*/"VARCHAR", /*with_fk=*/true)) {
    ARROW_RETURN_NOT_OK(RunPostgresDDL(conn, catalog_, stmt));
  }

  // postgres_execute() created the schema and tables directly in PostgreSQL,
  // bypassing DuckDB — so DuckDB's catalog cache (populated at ATTACH) does not
  // yet know about them. Clear it so the USE below and the unqualified writes
  // can resolve the freshly-created objects.
  auto clear_cache = conn.Query("CALL pg_clear_cache()");
  if (clear_cache->HasError()) {
    return arrow::Status::Invalid("Failed to refresh PostgreSQL catalog cache: ",
                                  clear_cache->GetError());
  }

  // The queued write_fns reference tables unqualified, so set the writer
  // connection's default catalog/schema to the instrumentation catalog.
  auto use_result = conn.Query("USE " + catalog_ + "." + schema_);
  if (use_result->HasError()) {
    return arrow::Status::Invalid(
        "Failed to set default PostgreSQL instrumentation schema: ",
        use_result->GetError());
  }
  return arrow::Status::OK();
}

arrow::Status InstrumentationManager::CleanupStaleRecords() {
  // Skip stale record cleanup for external catalogs (e.g., DuckLake with shared storage).
  // In multi-instance deployments, other instances may be legitimately running.
  // Stale cleanup is only safe for file-based instrumentation where we know
  // only one instance uses the database file.
  if (use_external_catalog_) {
    GIZMOSQL_LOG(INFO) << "Skipping stale record cleanup for external catalog (multi-instance safe)";
    return arrow::Status::OK();
  }

  try {
    std::string prefix = GetQualifiedPrefix();

    // First, get the list of stale (running) instance IDs
    auto stale_instances = writer_connection_->Get().Query(
        "SELECT instance_id FROM " + prefix + ".instances WHERE status = 'running'");
    if (stale_instances->HasError()) {
      GIZMOSQL_LOG(WARNING) << "Failed to query stale instances: " << stale_instances->GetError();
      return arrow::Status::OK();
    }

    if (stale_instances->RowCount() == 0) {
      // No stale instances to clean up
      return arrow::Status::OK();
    }

    GIZMOSQL_LOG(INFO) << "Cleaning up " << stale_instances->RowCount()
                       << " stale instance(s) from previous unclean shutdown(s)";

    // Build a list of instance IDs for the WHERE clause
    std::string instance_ids;
    for (idx_t i = 0; i < stale_instances->RowCount(); i++) {
      if (i > 0) instance_ids += ", ";
      instance_ids += "'" + stale_instances->GetValue(0, i).ToString() + "'";
    }

    // Mark any 'executing' or 'queued' executions from stale instances as 'error'
    auto exec_result = writer_connection_->Get().Query(
        "UPDATE " + prefix + ".sql_executions "
        "SET execution_end_time = now(), "
        "    status = 'error', "
        "    error_message = 'Server shutdown unexpectedly' "
        "WHERE status IN ('executing', 'queued') "
        "  AND statement_id IN ("
        "    SELECT statement_id FROM " + prefix + ".sql_statements "
        "    WHERE session_id IN ("
        "      SELECT session_id FROM " + prefix + ".sessions "
        "      WHERE instance_id IN (" + instance_ids + ")))");
    if (exec_result->HasError()) {
      GIZMOSQL_LOG(WARNING) << "Failed to cleanup stale executions: " << exec_result->GetError();
    }

    // Mark any 'active' sessions from stale instances as 'closed'
    // Note: We update ALL sessions for stale instances, not just 'active' ones,
    // to ensure consistency
    auto session_result = writer_connection_->Get().Query(
        "UPDATE " + prefix + ".sessions "
        "SET stop_time = COALESCE(stop_time, now()), "
        "    status = 'closed', "
        "    stop_reason = COALESCE(stop_reason, 'unclean_shutdown') "
        "WHERE instance_id IN (" + instance_ids + ") "
        "  AND status = 'active'");
    if (session_result->HasError()) {
      GIZMOSQL_LOG(WARNING) << "Failed to cleanup stale sessions: " << session_result->GetError();
    }

    // Mark any 'running' instances as 'stopped'
    auto instance_result = writer_connection_->Get().Query(
        "UPDATE " + prefix + ".instances "
        "SET stop_time = now(), "
        "    status = 'stopped', "
        "    stop_reason = 'unclean_shutdown' "
        "WHERE instance_id IN (" + instance_ids + ")");
    if (instance_result->HasError()) {
      GIZMOSQL_LOG(WARNING) << "Failed to cleanup stale instances: " << instance_result->GetError();
    }

    return arrow::Status::OK();
  } catch (const duckdb::Exception& ex) {
    GIZMOSQL_LOG(WARNING) << "Exception during stale record cleanup: " << ex.what();
    return arrow::Status::OK();  // Non-fatal, continue startup
  }
}

void InstrumentationManager::QueueWrite(
    std::function<void(duckdb::Connection&)> write_fn) {
  if (!enabled_ || shutdown_requested_) {
    return;
  }

  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    write_queue_.push(std::move(write_fn));
  }
  queue_cv_.notify_one();
}

void InstrumentationManager::WriterThreadLoop() {
  while (true) {
    std::function<void(duckdb::Connection&)> write_fn;

    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] {
        return !write_queue_.empty() || shutdown_requested_;
      });

      if (shutdown_requested_ && write_queue_.empty()) {
        break;
      }

      if (!write_queue_.empty()) {
        write_fn = std::move(write_queue_.front());
        write_queue_.pop();
      }
    }

    if (write_fn) {
      // Run each write in its own explicit transaction. The guard commits on
      // success; on any failure the write throws (see ExecuteInstrumentationWrite)
      // and the guard's destructor rolls back — so the writer connection is never
      // left parked inside an open transaction while it waits for the next write
      // (the deterministic commit boundary that keeps a DuckLake catalog's
      // backing PostgreSQL connection from sitting "idle in transaction"), and a
      // failed write is rolled back in isolation without affecting its neighbors.
      // Rolling back explicitly (rather than relying on DuckDB discarding an
      // aborted transaction on COMMIT) keeps this correct for writes routed to
      // PostgreSQL via the postgres extension too.
      //
      // Commit/serialization conflicts are NOT retried here: DuckLake performs
      // its own optimistic-concurrency retry (ducklake_max_retry_count etc.), and
      // on PostgreSQL each instance writes its own (disjoint) rows so cross-
      // instance conflicts are rare. A failure that still surfaces is a
      // best-effort instrumentation drop, logged below.
      try {
        auto& conn = writer_connection_->Get();
        CatalogTxnGuard txn(conn);
        write_fn(conn);
        txn.Commit();
      } catch (const std::exception& ex) {
        GIZMOSQL_LOG(WARNING) << "Instrumentation write failed: " << ex.what();
      }
    }
  }
}

void InstrumentationManager::Shutdown() {
  if (shutdown_requested_.exchange(true)) {
    return;
  }

  queue_cv_.notify_all();

  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }

  GIZMOSQL_LOG(INFO) << "Instrumentation manager shutdown complete";
}

}  // namespace gizmosql::ddb
