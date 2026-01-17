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

constexpr const char* kSchemaSQL = R"SQL(
-- ENUM types for status fields (in the attached _gizmosql_instr database)
CREATE TYPE IF NOT EXISTS _gizmosql_instr.instance_status AS ENUM ('running', 'stopped');
CREATE TYPE IF NOT EXISTS _gizmosql_instr.session_status AS ENUM ('active', 'closed', 'killed', 'timeout', 'error');
CREATE TYPE IF NOT EXISTS _gizmosql_instr.sql_statement_status AS ENUM ('executing', 'success', 'error', 'timeout', 'cancelled');

-- Server instance lifecycle
CREATE TABLE IF NOT EXISTS _gizmosql_instr.instances (
    instance_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    gizmosql_version VARCHAR NOT NULL,
    duckdb_version VARCHAR NOT NULL,
    hostname VARCHAR,
    port INTEGER,
    database_path VARCHAR,
    start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    stop_time TIMESTAMPTZ,
    status _gizmosql_instr.instance_status NOT NULL DEFAULT 'running',
    stop_reason VARCHAR
);

-- Client session lifecycle
CREATE TABLE IF NOT EXISTS _gizmosql_instr.sessions (
    session_id UUID PRIMARY KEY,
    instance_id UUID NOT NULL,
    username VARCHAR NOT NULL,
    role VARCHAR NOT NULL,
    peer VARCHAR NOT NULL,
    start_time TIMESTAMPTZ NOT NULL DEFAULT now(),
    stop_time TIMESTAMPTZ,
    status _gizmosql_instr.session_status NOT NULL DEFAULT 'active',
    stop_reason VARCHAR
);

-- SQL statement execution
CREATE TABLE IF NOT EXISTS _gizmosql_instr.sql_statements (
    statement_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id UUID NOT NULL,
    sql_text VARCHAR NOT NULL,
    statement_handle VARCHAR,
    execution_start_time TIMESTAMPTZ NOT NULL,
    execution_end_time TIMESTAMPTZ,
    rows_fetched BIGINT DEFAULT 0,
    status _gizmosql_instr.sql_statement_status NOT NULL DEFAULT 'executing',
    error_message VARCHAR,
    duration_ms BIGINT
);

-- Create indexes for common queries
CREATE INDEX IF NOT EXISTS idx_sessions_instance_id ON _gizmosql_instr.sessions(instance_id);
CREATE INDEX IF NOT EXISTS idx_sessions_start_time ON _gizmosql_instr.sessions(start_time);
CREATE INDEX IF NOT EXISTS idx_sessions_status ON _gizmosql_instr.sessions(status);
CREATE INDEX IF NOT EXISTS idx_sql_statements_session_id ON _gizmosql_instr.sql_statements(session_id);
CREATE INDEX IF NOT EXISTS idx_sql_statements_execution_start_time ON _gizmosql_instr.sql_statements(execution_start_time);
CREATE INDEX IF NOT EXISTS idx_sql_statements_status ON _gizmosql_instr.sql_statements(status);

-- View: Complete session activity
CREATE OR REPLACE VIEW _gizmosql_instr.session_activity AS
SELECT
    i.instance_id,
    i.gizmosql_version,
    i.duckdb_version,
    i.hostname,
    i.port,
    i.database_path,
    i.start_time AS instance_start_time,
    i.stop_time AS instance_stop_time,
    i.status AS instance_status,
    i.stop_reason AS instance_stop_reason,
    s.session_id,
    s.username,
    s.role,
    s.peer,
    s.start_time AS session_start_time,
    s.stop_time AS session_stop_time,
    s.status AS session_status,
    s.stop_reason AS session_stop_reason,
    st.statement_id,
    st.sql_text,
    st.statement_handle,
    st.execution_start_time,
    st.execution_end_time,
    st.rows_fetched,
    st.status AS statement_status,
    st.error_message,
    st.duration_ms
FROM _gizmosql_instr.instances i
LEFT JOIN _gizmosql_instr.sessions s ON i.instance_id = s.instance_id
LEFT JOIN _gizmosql_instr.sql_statements st ON s.session_id = st.session_id;

-- View: Currently active sessions
CREATE OR REPLACE VIEW _gizmosql_instr.active_sessions AS
SELECT
    s.session_id,
    s.instance_id,
    s.username,
    s.role,
    s.peer,
    s.start_time,
    s.status,
    i.hostname,
    i.port,
    i.database_path,
    EPOCH(now()) - EPOCH(s.start_time) AS session_duration_seconds
FROM _gizmosql_instr.sessions s
JOIN _gizmosql_instr.instances i ON s.instance_id = i.instance_id
WHERE s.status = 'active'
  AND i.status = 'running';

-- View: Session statistics
CREATE OR REPLACE VIEW _gizmosql_instr.session_stats AS
SELECT
    s.session_id,
    s.username,
    s.role,
    s.peer,
    s.start_time,
    s.stop_time,
    s.status AS session_status,
    COUNT(st.statement_id) AS total_statements,
    SUM(CASE WHEN st.status = 'success' THEN 1 ELSE 0 END) AS successful_statements,
    SUM(CASE WHEN st.status = 'error' THEN 1 ELSE 0 END) AS failed_statements,
    SUM(CASE WHEN st.status = 'timeout' THEN 1 ELSE 0 END) AS timed_out_statements,
    SUM(CASE WHEN st.status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_statements,
    SUM(st.rows_fetched) AS total_rows_fetched,
    AVG(st.duration_ms) AS avg_duration_ms,
    MAX(st.duration_ms) AS max_duration_ms
FROM _gizmosql_instr.sessions s
LEFT JOIN _gizmosql_instr.sql_statements st ON s.session_id = st.session_id
GROUP BY s.session_id, s.username, s.role, s.peer, s.start_time, s.stop_time, s.status;
)SQL";

}  // namespace

InstrumentationManager::InstrumentationManager(
    const std::string& db_path, std::shared_ptr<duckdb::DuckDB> db_instance,
    std::unique_ptr<duckdb::Connection> writer_connection)
    : db_path_(db_path),
      db_instance_(std::move(db_instance)),
      writer_connection_(std::move(writer_connection)) {}

InstrumentationManager::~InstrumentationManager() { Shutdown(); }

arrow::Result<std::shared_ptr<InstrumentationManager>> InstrumentationManager::Create(
    std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path) {
  if (!db_instance) {
    return arrow::Status::Invalid("InstrumentationManager requires a valid DuckDB instance");
  }

  GIZMOSQL_LOG(INFO) << "Initializing instrumentation manager with database at: " << db_path;

  try {
    // Create a dedicated connection for instrumentation writes
    auto writer_connection = std::make_unique<duckdb::Connection>(*db_instance);

    auto manager = std::shared_ptr<InstrumentationManager>(new InstrumentationManager(
        db_path, std::move(db_instance), std::move(writer_connection)));

    ARROW_RETURN_NOT_OK(manager->InitializeSchema());

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
    auto result = writer_connection_->Query(kSchemaSQL);
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
