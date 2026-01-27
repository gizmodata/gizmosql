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
constexpr const char* kSchemaSQLTemplate = R"SQL(
-- Switch to the instrumentation database context for schema creation
USE {CATALOG}.{SCHEMA};

-- ENUM types for status fields
CREATE TYPE IF NOT EXISTS instance_status AS ENUM ('running', 'stopped');
CREATE TYPE IF NOT EXISTS session_status AS ENUM ('active', 'closed', 'killed', 'timeout', 'error');
CREATE TYPE IF NOT EXISTS execution_status AS ENUM ('executing', 'success', 'error', 'timeout', 'cancelled');

-- Server instance lifecycle
CREATE TABLE IF NOT EXISTS instances (
    instance_id UUID PRIMARY KEY,
    gizmosql_version VARCHAR NOT NULL,
    gizmosql_edition VARCHAR NOT NULL,
    duckdb_version VARCHAR NOT NULL,
    arrow_version VARCHAR NOT NULL,
    hostname VARCHAR,
    hostname_arg VARCHAR,
    server_ip VARCHAR,
    port INTEGER,
    database_path VARCHAR,
    tls_enabled BOOLEAN NOT NULL DEFAULT false,
    tls_cert_path VARCHAR,
    tls_key_path VARCHAR,
    mtls_required BOOLEAN NOT NULL DEFAULT false,
    mtls_ca_cert_path VARCHAR,
    readonly BOOLEAN NOT NULL DEFAULT false,
    -- System information
    os_platform VARCHAR,
    os_name VARCHAR,
    os_version VARCHAR,
    cpu_arch VARCHAR,
    cpu_model VARCHAR,
    cpu_count INTEGER,
    memory_total_bytes BIGINT,
    -- Timestamps and status
    start_time TIMESTAMP NOT NULL DEFAULT now(),
    stop_time TIMESTAMP,
    status instance_status NOT NULL DEFAULT 'running',
    status_text VARCHAR GENERATED ALWAYS AS (CAST(status AS VARCHAR)) VIRTUAL,
    stop_reason VARCHAR
);

-- Connection protocol enum
CREATE TYPE IF NOT EXISTS connection_protocol AS ENUM ('plaintext', 'tls', 'mtls');

-- Client session lifecycle
CREATE TABLE IF NOT EXISTS sessions (
    session_id UUID PRIMARY KEY,
    instance_id UUID NOT NULL,
    username VARCHAR NOT NULL,
    role VARCHAR NOT NULL,
    auth_method VARCHAR NOT NULL,
    peer VARCHAR NOT NULL,
    peer_identity VARCHAR,
    user_agent VARCHAR,
    connection_protocol connection_protocol NOT NULL DEFAULT 'plaintext',
    connection_protocol_text VARCHAR GENERATED ALWAYS AS (CAST(connection_protocol AS VARCHAR)) VIRTUAL,
    start_time TIMESTAMP NOT NULL DEFAULT now(),
    stop_time TIMESTAMP,
    status session_status NOT NULL DEFAULT 'active',
    status_text VARCHAR GENERATED ALWAYS AS (CAST(status AS VARCHAR)) VIRTUAL,
    stop_reason VARCHAR
);

-- SQL statement definitions (prepared statements)
CREATE TABLE IF NOT EXISTS sql_statements (
    statement_id UUID PRIMARY KEY,
    session_id UUID NOT NULL,
    sql_text VARCHAR NOT NULL,
    flight_method VARCHAR,
    is_internal BOOLEAN NOT NULL DEFAULT false,
    prepare_success BOOLEAN NOT NULL DEFAULT true,
    prepare_error VARCHAR,
    created_time TIMESTAMP NOT NULL DEFAULT now()
);

-- SQL statement executions (each execution of a statement)
CREATE TABLE IF NOT EXISTS sql_executions (
    execution_id UUID PRIMARY KEY,
    statement_id UUID NOT NULL,
    bind_parameters VARCHAR,
    execution_start_time TIMESTAMP NOT NULL DEFAULT now(),
    execution_end_time TIMESTAMP,
    rows_fetched BIGINT DEFAULT 0,
    status execution_status NOT NULL DEFAULT 'executing',
    status_text VARCHAR GENERATED ALWAYS AS (CAST(status AS VARCHAR)) VIRTUAL,
    error_message VARCHAR,
    duration_ms BIGINT
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sessions_instance_id ON sessions(instance_id);
CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON sessions(start_time);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON sessions(status);
CREATE INDEX IF NOT EXISTS idx_sql_statements_session_id ON sql_statements(session_id);
CREATE INDEX IF NOT EXISTS idx_sql_statements_created_time ON sql_statements(created_time);
CREATE INDEX IF NOT EXISTS idx_sql_executions_statement_id ON sql_executions(statement_id);
CREATE INDEX IF NOT EXISTS idx_sql_executions_execution_start_time ON sql_executions(execution_start_time);
CREATE INDEX IF NOT EXISTS idx_sql_executions_status ON sql_executions(status);

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
    i.status_text AS instance_status_text,
    i.stop_reason AS instance_stop_reason,
    s.session_id,
    s.username,
    s.role,
    s.auth_method,
    s.peer,
    s.start_time AS session_start_time,
    s.stop_time AS session_stop_time,
    s.status AS session_status,
    s.status_text AS session_status_text,
    s.stop_reason AS session_stop_reason,
    st.statement_id,
    st.sql_text,
    st.created_time AS statement_created_time,
    e.execution_id,
    e.bind_parameters,
    e.execution_start_time,
    e.execution_end_time,
    e.rows_fetched,
    e.status AS execution_status,
    e.status_text AS execution_status_text,
    e.error_message,
    e.duration_ms
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
    s.status_text,
    i.hostname,
    i.hostname_arg,
    i.server_ip,
    i.port,
    i.database_path,
    EPOCH(now()) - EPOCH(s.start_time) AS session_duration_seconds
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
    s.status_text AS session_status_text,
    COUNT(DISTINCT st.statement_id) AS total_statements,
    COUNT(e.execution_id) AS total_executions,
    SUM(CASE WHEN e.status = 'success' THEN 1 ELSE 0 END) AS successful_executions,
    SUM(CASE WHEN e.status = 'error' THEN 1 ELSE 0 END) AS failed_executions,
    SUM(CASE WHEN e.status = 'timeout' THEN 1 ELSE 0 END) AS timed_out_executions,
    SUM(CASE WHEN e.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_executions,
    SUM(e.rows_fetched) AS total_rows_fetched,
    AVG(e.duration_ms) AS avg_duration_ms,
    MAX(e.duration_ms) AS max_duration_ms
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
    e.rows_fetched,
    e.status,
    e.status_text,
    e.error_message,
    e.duration_ms,
    s.username,
    s.auth_method,
    s.peer
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

}  // namespace

InstrumentationManager::InstrumentationManager(
    const std::string& db_path,
    const std::string& catalog,
    const std::string& schema,
    bool use_external_catalog,
    std::shared_ptr<duckdb::DuckDB> db_instance,
    std::unique_ptr<duckdb::Connection> writer_connection)
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
    auto writer_connection = std::make_unique<duckdb::Connection>(*db_instance);

    if (!use_external_catalog) {
      // File-based mode: ATTACH the instrumentation database on this connection
      // (ATTACH is connection-specific in DuckDB)
      auto attach_result = writer_connection->Query(
          "ATTACH IF NOT EXISTS '" + db_path + "' AS " + catalog);
      if (attach_result->HasError()) {
        return arrow::Status::Invalid("Failed to attach instrumentation database: ",
                                      attach_result->GetError());
      }
    }

    // Check for schema compatibility - if the instances table exists but lacks new columns,
    // the user has an old schema and needs to rename their instrumentation database file.
    auto schema_check = writer_connection->Query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog = '" + catalog + "' AND table_name = 'instances' AND column_name = 'os_platform'");
    if (!schema_check->HasError()) {
      // Check if instances table exists (by checking for any column)
      auto table_exists = writer_connection->Query(
          "SELECT column_name FROM information_schema.columns "
          "WHERE table_catalog = '" + catalog + "' AND table_name = 'instances' LIMIT 1");
      if (!table_exists->HasError() && table_exists->RowCount() > 0) {
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

    ARROW_RETURN_NOT_OK(manager->InitializeSchema());
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

arrow::Status InstrumentationManager::InitializeSchema() {
  try {
    // Generate schema SQL with actual catalog and schema names
    std::string schema_sql = GetSchemaSQL(catalog_, schema_);
    auto result = writer_connection_->Query(schema_sql);
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

arrow::Status InstrumentationManager::CleanupStaleRecords() {
  try {
    std::string prefix = GetQualifiedPrefix();

    // First, get the list of stale (running) instance IDs
    auto stale_instances = writer_connection_->Query(
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

    // Mark any 'executing' executions from stale instances as 'error'
    auto exec_result = writer_connection_->Query(
        "UPDATE " + prefix + ".sql_executions "
        "SET execution_end_time = now(), "
        "    status = 'error', "
        "    error_message = 'Server shutdown unexpectedly' "
        "WHERE status = 'executing' "
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
    auto session_result = writer_connection_->Query(
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
    auto instance_result = writer_connection_->Query(
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
      try {
        write_fn(*writer_connection_);
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
