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
  /// For file-based mode, the instrumentation database should already be ATTACHed as _gizmosql_instr.
  /// For external catalog mode, the catalog must be pre-attached by the user.
  /// @param db_instance The main server's DuckDB instance
  /// @param db_path Path to the instrumentation database (for reference only, ignored in external catalog mode)
  /// @param catalog The catalog name (e.g., "_gizmosql_instr" for file-based, or user-provided for DuckLake)
  /// @param schema The schema name within the catalog (default: "main")
  /// @param use_external_catalog If true, assumes catalog is pre-attached and skips ATTACH
  static arrow::Result<std::shared_ptr<InstrumentationManager>> Create(
      std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path,
      const std::string& catalog = "_gizmosql_instr",
      const std::string& schema = "main",
      bool use_external_catalog = false);

  /// Get the default instrumentation database path.
  /// @param database_filename The user's database file path. If provided and not
  ///        an in-memory database, the instrumentation DB will be placed in the
  ///        same directory. Falls back to current working directory.
  static std::string GetDefaultDbPath(const std::string& database_filename = "");

  std::string GetDbPath() const { return db_path_; }

  std::string GetCatalog() const { return catalog_; }

  std::string GetSchema() const { return schema_; }

  bool IsExternalCatalog() const { return use_external_catalog_; }

  /// Get the fully qualified prefix for instrumentation tables (catalog.schema)
  std::string GetQualifiedPrefix() const { return catalog_ + "." + schema_; }

  void QueueWrite(std::function<void(duckdb::Connection&)> write_fn);

  void Shutdown();

  bool IsEnabled() const { return enabled_; }

 private:
  InstrumentationManager(const std::string& db_path,
                         const std::string& catalog,
                         const std::string& schema,
                         bool use_external_catalog,
                         std::shared_ptr<duckdb::DuckDB> db_instance,
                         std::unique_ptr<duckdb::Connection> writer_connection);

  arrow::Status InitializeSchema();

  /// Clean up any stale 'running' instances and 'active' sessions from previous
  /// unclean shutdowns. Called during startup.
  arrow::Status CleanupStaleRecords();

  void WriterThreadLoop();

  std::string db_path_;
  std::string catalog_;
  std::string schema_;
  bool use_external_catalog_;
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
