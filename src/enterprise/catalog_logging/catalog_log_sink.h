// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <duckdb.hpp>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

#include "../catalog_backend.h"
#include "detail/tracked_duckdb_connection.h"
#include "gizmosql_logging.h"  // gizmosql::LogRecord

namespace gizmosql::enterprise {

/// Forks the server's logs to a "logs" table in an attached catalog (file-based
/// DuckDB, PostgreSQL, or DuckLake), IN ADDITION to the primary stdout/file
/// sink. Registered as a secondary sink via gizmosql::RegisterLogSink: the
/// logging thread calls Enqueue() (cheap, bounded, non-blocking) and a
/// dedicated writer thread batches the records into append-only INSERTs, each
/// batch wrapped in an explicit transaction (CatalogTxnGuard — never relying on
/// auto-commit, so the writer connection is never parked "idle in transaction"
/// and a failed batch is rolled back). Append-only writes make DuckLake a fine
/// fit here (unlike the UPDATE-heavy instrumentation manager).
///
/// The log catalog is treated as a system-managed, admin-read-only catalog by
/// the catalog-permissions handler (see GetCatalogAccess) — exactly like the
/// instrumentation catalog. Client sessions never write to it; only this sink's
/// dedicated writer connection does.
class CatalogLogSink {
 public:
  ~CatalogLogSink();

  /// Create the sink: open a dedicated writer connection on the shared DuckDB
  /// instance, (for file-based mode) ATTACH the log database, detect the
  /// backend, create the schema, and start the writer thread.
  /// @param db_instance     The main server's DuckDB instance (writes share it).
  /// @param db_path         File path for file-based mode (ignored if external).
  /// @param catalog         Catalog (attach) name, e.g. "_gizmosql_logs".
  /// @param schema          Schema within the catalog (default "main").
  /// @param use_external_catalog If true, the catalog is assumed pre-attached.
  /// @param max_queue_depth Bounded in-memory queue depth; records arriving when
  ///                        the queue is full are dropped and counted.
  static arrow::Result<std::shared_ptr<CatalogLogSink>> Create(
      std::shared_ptr<duckdb::DuckDB> db_instance, const std::string& db_path,
      const std::string& catalog = "_gizmosql_logs",
      const std::string& schema = "main", bool use_external_catalog = false,
      size_t max_queue_depth = 100000);

  /// Default file-based log database path: GIZMOSQL_LOG_DB_PATH env var, else a
  /// "gizmosql_logs.db" sibling of the main database file (or in the cwd).
  static std::string GetDefaultDbPath(const std::string& database_filename = "");

  std::string GetCatalog() const { return catalog_; }
  std::string GetSchema() const { return schema_; }

  /// Enqueue one record for asynchronous write. Called on the logging thread;
  /// must stay cheap and non-blocking. Drops (and counts) when the queue is full.
  void Enqueue(const gizmosql::LogRecord& record);

  /// Stop accepting work, wake the writer thread, drain the remaining queue, and
  /// join. Call gizmosql::ClearLogSinks() BEFORE this so no further records are
  /// dispatched into a half-torn-down sink.
  void Shutdown();

 private:
  CatalogLogSink(std::string db_path, std::string catalog, std::string schema,
                 bool use_external_catalog, size_t max_queue_depth,
                 std::shared_ptr<duckdb::DuckDB> db_instance,
                 std::unique_ptr<gizmosql::TrackedDuckDBConnection> writer_connection);

  CatalogBackend DetectBackend();
  arrow::Status InitializeSchema();
  arrow::Status InitializePostgresSchema();
  void WriterThreadLoop();
  // Write a batch in one transaction; on failure, retry each record in its own
  // transaction so a single poison record never drops the whole batch.
  void WriteBatch(std::vector<gizmosql::LogRecord>& batch);

  std::string db_path_;
  std::string catalog_;
  std::string schema_;
  bool use_external_catalog_;
  size_t max_queue_depth_;
  CatalogBackend backend_{CatalogBackend::kDuckDBFile};
  std::shared_ptr<duckdb::DuckDB> db_instance_;
  std::unique_ptr<gizmosql::TrackedDuckDBConnection> writer_connection_;

  std::deque<gizmosql::LogRecord> queue_;
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::thread writer_thread_;
  std::atomic<bool> shutdown_requested_{false};
  uint64_t dropped_count_{0};     // total records dropped (guarded by queue_mutex_)
  uint64_t dropped_reported_{0};  // last drop count reported (writer thread only)
};

}  // namespace gizmosql::enterprise
