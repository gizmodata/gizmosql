// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
#pragma once
#include "metrics_registry.h"
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <thread>
#include <duckdb.hpp>
#include <arrow/status.h>
namespace httplib {
class Server;
}
namespace gizmosql {
struct ClientSession;
}
namespace gizmosql::enterprise {
class MetricsService {
 public:
  /// `read_only` servers never maintain the unclean-exit marker: DuckDB allows
  /// several read-only processes on one database file, so a shared marker
  /// would report each other's runs as unclean exits. Only the single writer
  /// owns it.
  MetricsService(std::shared_ptr<duckdb::DuckDB> db, std::filesystem::path database,
                 std::string certificate, int session_limit,
                 std::function<void(MetricsRegistry&)> sample_server,
                 bool read_only = false);
  ~MetricsService();
  arrow::Status Start(int port, const std::string& address);
  void Stop();
  std::shared_ptr<MetricsRegistry> Registry() const { return registry_; }

 private:
  void Collect();
  void WriteExitState(bool running);
  std::shared_ptr<MetricsRegistry> registry_;
  std::shared_ptr<duckdb::DuckDB> db_;
  std::unique_ptr<duckdb::Connection> connection_;
  std::filesystem::path database_;
  std::filesystem::path exit_state_;
  uint64_t unclean_exits_ = 0;
  bool exit_state_started_ = false;
  std::function<void(MetricsRegistry&)> sample_server_;
  std::unique_ptr<httplib::Server> http_;
  std::thread listener_, collector_;
  std::mutex mutex_;
  std::condition_variable wake_;
  std::atomic<bool> stopped_{false};
};
void RegisterMetricsFunction(duckdb::DuckDB& db);
void TrackMetricsSession(const std::shared_ptr<gizmosql::ClientSession>& session);
}  // namespace gizmosql::enterprise
