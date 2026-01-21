// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <duckdb.hpp>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <thread>

#include <arrow/result.h>
#include <arrow/status.h>

namespace gizmosql::ddb {

class InstrumentationManager {
 public:
  ~InstrumentationManager();

  /// Create an InstrumentationManager that uses an external DuckDB instance.
  /// The instrumentation database should already be ATTACHed as _gizmosql_instr.
  /// @param db_instance The main server's DuckDB instance
  /// @param db_path Path to the instrumentation database (for reference only)
  static arrow::Result<std::shared_ptr<InstrumentationManager>> Create(
      std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path);

  /// Get the default instrumentation database path.
  /// @param database_filename The user's database file path. If provided and not
  ///        an in-memory database, the instrumentation DB will be placed in the
  ///        same directory. Falls back to current working directory.
  static std::string GetDefaultDbPath(const std::string& database_filename = "");

  std::string GetDbPath() const { return db_path_; }

  void QueueWrite(std::function<void(duckdb::Connection&)> write_fn);

  void Shutdown();

  bool IsEnabled() const { return enabled_; }

 private:
  InstrumentationManager(const std::string& db_path,
                         std::shared_ptr<duckdb::DuckDB> db_instance,
                         std::unique_ptr<duckdb::Connection> writer_connection);

  arrow::Status InitializeSchema();

  /// Clean up any stale 'running' instances and 'active' sessions from previous
  /// unclean shutdowns. Called during startup.
  arrow::Status CleanupStaleRecords();

  void WriterThreadLoop();

  std::string db_path_;
  std::shared_ptr<duckdb::DuckDB> db_instance_;
  std::unique_ptr<duckdb::Connection> writer_connection_;

  std::queue<std::function<void(duckdb::Connection&)>> write_queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread writer_thread_;
  std::atomic<bool> shutdown_requested_{false};
  bool enabled_{true};
};

}  // namespace gizmosql::ddb
