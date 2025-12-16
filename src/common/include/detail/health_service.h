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
#include <functional>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>
#include "grpc/health/v1/health.grpc.pb.h"

namespace gizmosql {

/// Type alias for the health check function.
/// Returns true if the service is healthy (SERVING), false otherwise.
using HealthCheckFn = std::function<bool()>;

/// Custom gRPC Health service implementation for GizmoSQL.
/// This service performs a real-time health check by executing "SELECT 1"
/// on the underlying database backend on each Check() call.
class GizmoSQLHealthServiceImpl final : public grpc::health::v1::Health::Service {
 public:
  /// Construct a health service with the given health check function.
  /// @param health_check_fn Function that returns true if the service is healthy.
  explicit GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn);

  ~GizmoSQLHealthServiceImpl() override = default;

  /// Perform a synchronous health check.
  /// Calls the health_check_fn_ to determine the serving status.
  /// @param context The gRPC server context.
  /// @param request The health check request (service name is ignored).
  /// @param response The health check response with serving status.
  /// @return gRPC status (always OK, the health status is in the response).
  grpc::Status Check(grpc::ServerContext* context,
                     const grpc::health::v1::HealthCheckRequest* request,
                     grpc::health::v1::HealthCheckResponse* response) override;

  /// Watch for health status changes (streaming).
  /// This implementation sends the current status and then waits until
  /// the context is cancelled.
  /// @param context The gRPC server context.
  /// @param request The health check request (service name is ignored).
  /// @param writer The response writer for streaming status updates.
  /// @return gRPC status.
  grpc::Status Watch(grpc::ServerContext* context,
                     const grpc::health::v1::HealthCheckRequest* request,
                     grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) override;

  /// Signal shutdown to stop any active Watch streams.
  void Shutdown();

 private:
  HealthCheckFn health_check_fn_;
  std::atomic<bool> shutdown_{false};
};

}  // namespace gizmosql
