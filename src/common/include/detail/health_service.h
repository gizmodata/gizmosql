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
class GizmoSQLHealthServiceImpl final : public grpc::health::v1::Health::Service {
 public:
  /// Default health check polling interval in seconds.
  static constexpr int kDefaultPollIntervalSeconds = 5;

  /// Construct a health service with the given health check function.
  /// @param health_check_fn Function that returns true if the service is healthy.
  /// @param poll_interval_seconds Interval between health checks (default: 5 seconds).
  explicit GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn,
                                     int poll_interval_seconds = kDefaultPollIntervalSeconds);

  ~GizmoSQLHealthServiceImpl() override;

  /// Perform a synchronous health check.
  /// Returns the cached health status from the background poller.
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

  /// Signal shutdown to stop the background health checker and any active Watch streams.
  void Shutdown();

 private:
  /// Background thread function that periodically checks health.
  void HealthCheckLoop();

  HealthCheckFn health_check_fn_;
  std::chrono::seconds poll_interval_;

  // Shared state protected by mutex
  std::mutex mutex_;
  std::condition_variable status_changed_cv_;
  bool cached_status_{false};
  uint64_t status_version_{0};  // Incremented on each status change

  // Background thread management
  std::thread health_check_thread_;
  std::atomic<bool> shutdown_{false};
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
