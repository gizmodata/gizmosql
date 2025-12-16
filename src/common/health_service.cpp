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

#include "gizmosql_logging.h"

namespace gizmosql {

GizmoSQLHealthServiceImpl::GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn,
                                                     int poll_interval_seconds)
    : health_check_fn_(std::move(health_check_fn)),
      poll_interval_(poll_interval_seconds) {
  // Perform initial health check
  try {
    cached_status_ = health_check_fn_();
  } catch (...) {
    cached_status_ = false;
  }

  // Start background health check thread
  health_check_thread_ = std::thread(&GizmoSQLHealthServiceImpl::HealthCheckLoop, this);
}

GizmoSQLHealthServiceImpl::~GizmoSQLHealthServiceImpl() {
  Shutdown();
}

void GizmoSQLHealthServiceImpl::HealthCheckLoop() {
  while (!shutdown_.load()) {
    // Wait for poll interval or shutdown
    {
      std::unique_lock<std::mutex> lock(mutex_);
      status_changed_cv_.wait_for(lock, poll_interval_, [this] {
        return shutdown_.load();
      });
    }

    if (shutdown_.load()) {
      break;
    }

    // Perform health check
    bool new_status = false;
    try {
      new_status = health_check_fn_();
    } catch (...) {
      new_status = false;
    }

    // Update cached status and notify watchers if changed
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (new_status != cached_status_) {
        GIZMOSQL_LOG(DEBUG) << "Health status changed: "
                            << (new_status ? "SERVING" : "NOT_SERVING");
        cached_status_ = new_status;
        ++status_version_;
      }
    }
    // Always notify to allow Watch streams to check for cancellation
    status_changed_cv_.notify_all();
  }
}

grpc::Status GizmoSQLHealthServiceImpl::Check(
    grpc::ServerContext* context,
    const grpc::health::v1::HealthCheckRequest* request,
    grpc::health::v1::HealthCheckResponse* response) {
  const auto& service_name = request->service();

  bool is_healthy;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    is_healthy = cached_status_;
  }

  GIZMOSQL_LOG(DEBUG) << "Health check request from peer: " << context->peer()
                      << ", service: "
                      << (service_name.empty() ? "(empty/all)" : service_name);

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

  grpc::health::v1::HealthCheckResponse response;
  bool last_status;
  uint64_t last_version;

  // Send initial status
  {
    std::lock_guard<std::mutex> lock(mutex_);
    last_status = cached_status_;
    last_version = status_version_;
  }

  response.set_status(last_status ? grpc::health::v1::HealthCheckResponse::SERVING
                                  : grpc::health::v1::HealthCheckResponse::NOT_SERVING);

  GIZMOSQL_LOG(DEBUG) << "Health watch initial status: "
                      << (last_status ? "SERVING" : "NOT_SERVING");

  if (!writer->Write(response)) {
    GIZMOSQL_LOG(DEBUG) << "Health watch stream ended (write failed)";
    return grpc::Status::OK;
  }

  // Wait for status changes and send updates
  while (!context->IsCancelled() && !shutdown_.load()) {
    bool current_status;
    uint64_t current_version;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      // Wait for status change, shutdown, or periodic wake-up to check cancellation
      status_changed_cv_.wait_for(lock, poll_interval_, [this, last_version] {
        return shutdown_.load() || status_version_ != last_version;
      });

      current_status = cached_status_;
      current_version = status_version_;
    }

    if (context->IsCancelled() || shutdown_.load()) {
      break;
    }

    // Only send update if status changed
    if (current_version != last_version && current_status != last_status) {
      GIZMOSQL_LOG(DEBUG) << "Health watch sending status change: "
                          << (current_status ? "SERVING" : "NOT_SERVING");
      response.set_status(current_status ? grpc::health::v1::HealthCheckResponse::SERVING
                                         : grpc::health::v1::HealthCheckResponse::NOT_SERVING);
      if (!writer->Write(response)) {
        break;
      }
      last_status = current_status;
    }
    last_version = current_version;
  }

  GIZMOSQL_LOG(DEBUG) << "Health watch stream ended";
  return grpc::Status::OK;
}

void GizmoSQLHealthServiceImpl::Shutdown() {
  bool expected = false;
  if (!shutdown_.compare_exchange_strong(expected, true)) {
    // Already shut down
    return;
  }

  // Wake up the background thread and all watchers
  status_changed_cv_.notify_all();

  // Wait for background thread to finish
  if (health_check_thread_.joinable()) {
    health_check_thread_.join();
  }
}

}  // namespace gizmosql
