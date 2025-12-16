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

#include "health_service.h"

#include <chrono>
#include <thread>

#include "gizmosql_logging.h"

namespace gizmosql {

GizmoSQLHealthServiceImpl::GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn)
    : health_check_fn_(std::move(health_check_fn)) {}

grpc::Status GizmoSQLHealthServiceImpl::Check(
    grpc::ServerContext* context,
    const grpc::health::v1::HealthCheckRequest* request,
    grpc::health::v1::HealthCheckResponse* response) {
  const auto& service_name = request->service();
  GIZMOSQL_LOG(DEBUG) << "Health check request from peer: " << context->peer()
                      << ", service: "
                      << (service_name.empty() ? "(empty/all)" : service_name);

  // Perform the health check by calling the provided function
  bool is_healthy = false;
  try {
    is_healthy = health_check_fn_();
  } catch (...) {
    // If the health check throws, consider the service unhealthy
    is_healthy = false;
  }

  if (is_healthy) {
    response->set_status(grpc::health::v1::HealthCheckResponse::SERVING);
  } else {
    response->set_status(grpc::health::v1::HealthCheckResponse::NOT_SERVING);
  }

  GIZMOSQL_LOG(DEBUG) << "Health check result: "
                      << (is_healthy ? "SERVING" : "NOT_SERVING");

  return grpc::Status::OK;
}

grpc::Status GizmoSQLHealthServiceImpl::Watch(
    grpc::ServerContext* context,
    const grpc::health::v1::HealthCheckRequest* request,
    grpc::ServerWriter<grpc::health::v1::HealthCheckResponse>* writer) {
  const auto& service_name = request->service();
  GIZMOSQL_LOG(DEBUG) << "Health watch stream started from peer: " << context->peer()
                      << ", service: "
                      << (service_name.empty() ? "(empty/all)" : service_name);

  // Send the initial status
  grpc::health::v1::HealthCheckResponse response;

  bool is_healthy = false;
  try {
    is_healthy = health_check_fn_();
  } catch (...) {
    is_healthy = false;
  }

  response.set_status(is_healthy ? grpc::health::v1::HealthCheckResponse::SERVING
                                 : grpc::health::v1::HealthCheckResponse::NOT_SERVING);

  GIZMOSQL_LOG(DEBUG) << "Health watch initial status: "
                      << (is_healthy ? "SERVING" : "NOT_SERVING");

  if (!writer->Write(response)) {
    GIZMOSQL_LOG(DEBUG) << "Health watch stream ended (write failed)";
    return grpc::Status::OK;
  }

  // Keep the stream open until cancelled or shutdown
  // Periodically check health and send updates on status changes
  bool last_status = is_healthy;
  while (!context->IsCancelled() && !shutdown_.load()) {
    std::this_thread::sleep_for(std::chrono::seconds(5));

    if (context->IsCancelled() || shutdown_.load()) {
      break;
    }

    try {
      is_healthy = health_check_fn_();
    } catch (...) {
      is_healthy = false;
    }

    // Only send update if status changed
    if (is_healthy != last_status) {
      GIZMOSQL_LOG(DEBUG) << "Health watch status changed: "
                          << (is_healthy ? "SERVING" : "NOT_SERVING");
      response.set_status(is_healthy ? grpc::health::v1::HealthCheckResponse::SERVING
                                     : grpc::health::v1::HealthCheckResponse::NOT_SERVING);
      if (!writer->Write(response)) {
        break;
      }
      last_status = is_healthy;
    }
  }

  GIZMOSQL_LOG(DEBUG) << "Health watch stream ended";
  return grpc::Status::OK;
}

void GizmoSQLHealthServiceImpl::Shutdown() {
  shutdown_.store(true);
}

}  // namespace gizmosql
