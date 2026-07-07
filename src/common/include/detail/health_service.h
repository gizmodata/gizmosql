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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <arrow/result.h>
#include <grpcpp/grpcpp.h>
#include "grpc/health/v1/health.grpc.pb.h"

namespace gizmosql {

/// Type alias for the health check function.
/// Returns true if the service is healthy (SERVING), false otherwise.
using HealthCheckFn = std::function<bool()>;

/// Custom gRPC Health service implementation for GizmoSQL.
/// Uses a background thread to periodically check health status, ensuring
/// that multiple Watch streams share a single health check rather than
/// each polling independently.
///
/// Hang detection: the health check function runs with no timeout (a hung
/// SQL query blocks the checker thread indefinitely), so health status is
/// staleness-aware — if no check has *completed* within the staleness
/// threshold, Check/Watch report NOT_SERVING even though the last completed
/// check succeeded. A hung health check query therefore reads as unhealthy
/// after the threshold elapses, and recovers automatically if the query
/// eventually returns successfully.
class GizmoSQLHealthServiceImpl final : public grpc::health::v1::Health::Service {
 public:
  /// Default health check polling interval in seconds.
  static constexpr int kDefaultPollIntervalSeconds = 5;

  /// Default staleness threshold, as a multiple of the poll interval.
  /// If no health check completes within (poll_interval * this multiplier),
  /// the service reports NOT_SERVING.
  static constexpr int kDefaultStalenessMultiplier = 3;

  /// Construct a health service with the given health check function.
  /// @param health_check_fn Function that returns true if the service is healthy.
  /// @param poll_interval_seconds Interval between health checks (default: 5 seconds).
  /// @param staleness_seconds Report NOT_SERVING if no health check has completed
  ///        within this many seconds (e.g. the health check query is hung).
  ///        0 (default) means poll_interval_seconds * kDefaultStalenessMultiplier.
  /// @param health_check_query_text The SQL text the health check function runs,
  ///        included in per-check log messages so operators can see the query
  ///        without scrolling back to startup. Logging only; may be empty.
  explicit GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn,
                                     int poll_interval_seconds = kDefaultPollIntervalSeconds,
                                     int staleness_seconds = 0,
                                     std::string health_check_query_text = "");

  ~GizmoSQLHealthServiceImpl() override;

  /// Perform a synchronous health check.
  /// Returns the cached health status from the background poller, downgraded
  /// to NOT_SERVING when the status is stale (see CurrentStatus()).
  /// @param context The gRPC server context.
  /// @param request The health check request (service name is ignored).
  /// @param response The health check response with serving status.
  /// @return gRPC status (always OK, the health status is in the response).
  grpc::Status Check(grpc::ServerContext* context,
                     const grpc::health::v1::HealthCheckRequest* request,
                     grpc::health::v1::HealthCheckResponse* response) override;

  /// Watch for health status changes (streaming).
  /// Waits on a shared condition variable for status changes, rather than
  /// polling independently. Multiple Watch streams share the same background
  /// health checker.
  /// @param context The gRPC server context.
  /// @param request The health check request (service name is ignored).
  /// @param writer The response writer for streaming status updates.
  /// @return gRPC status.
  grpc::Status Watch(grpc::ServerContext* context,
                     const grpc::health::v1::HealthCheckRequest* request,
                     grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) override;

  /// Signal shutdown to stop the background health checker and any active Watch
  /// streams. If the checker thread is stuck inside a hung health check query,
  /// it is detached (after a short wait) rather than hanging shutdown.
  void Shutdown();

  /// The effective health status: the last completed check result, downgraded
  /// to unhealthy (false) when no check has completed within the staleness
  /// threshold — i.e. the health check query is hung or the checker thread is
  /// wedged. Also exposed for tests.
  bool CurrentStatus();

 private:
  // State shared with the checker thread via shared_ptr so Shutdown() can
  // safely detach the thread when it is stuck inside a hung health check query.
  struct SharedState {
    std::mutex mutex;
    std::condition_variable status_changed_cv;
    bool cached_status{false};
    uint64_t status_version{0};  // Incremented on each status change
    bool first_check_done{false};
    std::chrono::steady_clock::time_point last_check_completed{};
    std::atomic<bool> shutdown{false};
    std::atomic<bool> check_in_flight{false};
    // Set when a reader logs the "health check is stale" warning; cleared when
    // a check completes, so each stall episode warns exactly once.
    std::atomic<bool> stale_warning_logged{false};
  };

  /// Background thread function that periodically runs the health check.
  static void HealthCheckLoop(std::shared_ptr<SharedState> state,
                              HealthCheckFn health_check_fn,
                              std::chrono::seconds poll_interval,
                              std::chrono::seconds staleness_threshold,
                              std::string query_log_suffix);

  std::chrono::seconds poll_interval_;
  std::chrono::seconds staleness_threshold_;
  // " - query: <sql>" suffix appended to health log messages (empty when no
  // query text was supplied).
  std::string query_log_suffix_;
  std::shared_ptr<SharedState> state_;
  std::thread health_check_thread_;
};

/// Lightweight plaintext gRPC server for Kubernetes health probes.
/// Runs on a separate port without TLS, serving only the Health service.
/// This allows Kubernetes to perform health checks without TLS support.
class PlaintextHealthServer {
 public:
  /// Start the plaintext health server on the given port.
  /// @param health_service The health service to register (shared with main server).
  /// @param port The port to listen on (e.g., 31338).
  /// @return A unique_ptr to the server, or an error status.
  static arrow::Result<std::unique_ptr<PlaintextHealthServer>> Start(
      std::shared_ptr<GizmoSQLHealthServiceImpl> health_service,
      int port);

  /// Shutdown the server and wait for it to stop.
  void Shutdown();

  ~PlaintextHealthServer();

  // Non-copyable, non-movable
  PlaintextHealthServer(const PlaintextHealthServer&) = delete;
  PlaintextHealthServer& operator=(const PlaintextHealthServer&) = delete;

 private:
  PlaintextHealthServer() = default;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace gizmosql
