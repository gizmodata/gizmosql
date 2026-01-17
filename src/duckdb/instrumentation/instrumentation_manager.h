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
