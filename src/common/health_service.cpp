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

#include <grpcpp/grpcpp.h>

#include "gizmosql_logging.h"

namespace gizmosql {

GizmoSQLHealthServiceImpl::GizmoSQLHealthServiceImpl(HealthCheckFn health_check_fn,
                                                     int poll_interval_seconds,
                                                     int staleness_seconds,
                                                     std::string health_check_query_text)
    : poll_interval_(poll_interval_seconds),
      staleness_threshold_(staleness_seconds > 0
                               ? staleness_seconds
                               : poll_interval_seconds * kDefaultStalenessMultiplier),
      query_log_suffix_(health_check_query_text.empty()
                            ? ""
                            : " - query: " + health_check_query_text),
      state_(std::make_shared<SharedState>()) {
  state_->last_check_completed = std::chrono::steady_clock::now();

  // Start the background health check thread (it runs the first check
  // immediately).
  health_check_thread_ = std::thread(&GizmoSQLHealthServiceImpl::HealthCheckLoop, state_,
                                     std::move(health_check_fn), poll_interval_,
                                     staleness_threshold_, query_log_suffix_);

  // Wait (bounded) for the initial check so probes arriving right after
  // startup see an accurate status. If the health check query hangs at
  // startup, we proceed anyway — status reads NOT_SERVING via the staleness
  // check rather than blocking server startup indefinitely.
  std::unique_lock<std::mutex> lock(state_->mutex);
  if (!state_->status_changed_cv.wait_for(lock, staleness_threshold_, [this] {
        return state_->first_check_done;
      })) {
    GIZMOSQL_LOG(WARNING) << "Initial health check did not complete within "
                          << staleness_threshold_.count()
                          << "s - starting with health status NOT_SERVING"
                          << query_log_suffix_;
  }
}

GizmoSQLHealthServiceImpl::~GizmoSQLHealthServiceImpl() {
  Shutdown();
}

void GizmoSQLHealthServiceImpl::HealthCheckLoop(std::shared_ptr<SharedState> state,
                                                HealthCheckFn health_check_fn,
                                                std::chrono::seconds poll_interval,
                                                std::chrono::seconds staleness_threshold,
                                                std::string query_log_suffix) {
  while (!state->shutdown.load()) {
    // Perform health check (no timeout — a hung query blocks here, which
    // readers detect via the staleness threshold on last_check_completed).
    state->check_in_flight.store(true);
    const auto check_start = std::chrono::steady_clock::now();
    bool new_status = false;
    try {
      new_status = health_check_fn();
    } catch (...) {
      new_status = false;
    }
    const auto check_end = std::chrono::steady_clock::now();
    state->check_in_flight.store(false);

    const auto duration_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(check_end - check_start)
            .count();
    GIZMOSQL_LOG(DEBUG) << "Health check query completed in " << duration_ms
                        << " ms - result: "
                        << (new_status ? "SERVING" : "NOT_SERVING") << query_log_suffix;
    if (new_status && check_end - check_start > staleness_threshold) {
      // A *healthy* check that takes longer than the staleness threshold makes
      // the effective status flap: SERVING right after each completion, then
      // stale/NOT_SERVING until the next one. Surface the misconfiguration
      // even when nothing is probing the health port.
      GIZMOSQL_LOG(WARNING)
          << "Health check took " << duration_ms
          << " ms, exceeding the staleness threshold of " << staleness_threshold.count()
          << "s - health status will flap between SERVING and NOT_SERVING; "
             "increase --health-check-staleness-seconds / "
             "GIZMOSQL_HEALTH_CHECK_STALENESS_SECONDS (or use a faster health "
             "check query)"
          << query_log_suffix;
    }
    if (state->stale_warning_logged.exchange(false)) {
      GIZMOSQL_LOG(INFO) << "Health check completed after exceeding the staleness "
                            "threshold (took "
                         << duration_ms << " ms)";
    }

    // Update cached status / completion time and notify watchers
    {
      std::lock_guard<std::mutex> lock(state->mutex);
      state->last_check_completed = check_end;
      state->first_check_done = true;
      if (new_status != state->cached_status) {
        GIZMOSQL_LOG(INFO) << "Health status changed: "
                           << (new_status ? "SERVING" : "NOT_SERVING");
        state->cached_status = new_status;
        ++state->status_version;
      }
    }
    // Always notify to allow Watch streams to check for cancellation (and
    // Shutdown() to observe check completion).
    state->status_changed_cv.notify_all();

    if (state->shutdown.load()) {
      break;
    }

    // Wait for poll interval or shutdown
    {
      std::unique_lock<std::mutex> lock(state->mutex);
      state->status_changed_cv.wait_for(lock, poll_interval, [&state] {
        return state->shutdown.load();
      });
    }
  }
}

bool GizmoSQLHealthServiceImpl::CurrentStatus() {
  bool is_healthy;
  std::chrono::steady_clock::duration since_last_check;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    is_healthy = state_->cached_status;
    since_last_check = std::chrono::steady_clock::now() - state_->last_check_completed;
  }

  if (since_last_check > staleness_threshold_) {
    is_healthy = false;
    if (!state_->stale_warning_logged.exchange(true)) {
      GIZMOSQL_LOG(WARNING)
          << "No health check has completed in "
          << std::chrono::duration_cast<std::chrono::seconds>(since_last_check).count()
          << "s (staleness threshold: " << staleness_threshold_.count()
          << "s) - reporting NOT_SERVING; the health check query may be hung"
          << query_log_suffix_;
    }
  }

  return is_healthy;
}

grpc::Status GizmoSQLHealthServiceImpl::Check(
    grpc::ServerContext* context,
    const grpc::health::v1::HealthCheckRequest* request,
    grpc::health::v1::HealthCheckResponse* response) {
  const auto& service_name = request->service();

  GIZMOSQL_LOG(DEBUG) << "Health check request from peer: " << context->peer()
                      << ", service: "
                      << (service_name.empty() ? "(empty/all)" : service_name);

  const bool is_healthy = CurrentStatus();

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

  // Send initial status
  bool last_status = CurrentStatus();
  uint64_t last_version;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    last_version = state_->status_version;
  }

  response.set_status(last_status ? grpc::health::v1::HealthCheckResponse::SERVING
                                  : grpc::health::v1::HealthCheckResponse::NOT_SERVING);

  GIZMOSQL_LOG(DEBUG) << "Health watch initial status: "
                      << (last_status ? "SERVING" : "NOT_SERVING");

  if (!writer->Write(response)) {
    GIZMOSQL_LOG(DEBUG) << "Health watch stream ended (write failed)";
    return grpc::Status::OK;
  }

  // Wait for status changes and send updates. The periodic wake-up (every
  // poll_interval_) also lets us re-evaluate staleness, so a hung health
  // check query pushes a NOT_SERVING update to watchers even though the
  // cached status never changed.
  while (!context->IsCancelled() && !state_->shutdown.load()) {
    {
      std::unique_lock<std::mutex> lock(state_->mutex);
      // Wait for status change, shutdown, or periodic wake-up to check
      // cancellation and staleness
      state_->status_changed_cv.wait_for(lock, poll_interval_, [this, last_version] {
        return state_->shutdown.load() || state_->status_version != last_version;
      });
      last_version = state_->status_version;
    }

    if (context->IsCancelled() || state_->shutdown.load()) {
      break;
    }

    // Send update if the effective status differs from what we last sent
    const bool current_status = CurrentStatus();
    if (current_status != last_status) {
      GIZMOSQL_LOG(DEBUG) << "Health watch sending status change: "
                          << (current_status ? "SERVING" : "NOT_SERVING");
      response.set_status(current_status
                              ? grpc::health::v1::HealthCheckResponse::SERVING
                              : grpc::health::v1::HealthCheckResponse::NOT_SERVING);
      if (!writer->Write(response)) {
        break;
      }
      last_status = current_status;
    }
  }

  GIZMOSQL_LOG(DEBUG) << "Health watch stream ended";
  return grpc::Status::OK;
}

void GizmoSQLHealthServiceImpl::Shutdown() {
  bool expected = false;
  if (!state_->shutdown.compare_exchange_strong(expected, true)) {
    // Already shut down
    return;
  }

  // Wake up the background thread and all watchers
  state_->status_changed_cv.notify_all();

  if (!health_check_thread_.joinable()) {
    return;
  }

  // Give an in-flight health check a brief window to finish. If it is hung
  // inside the health check query, detach the thread rather than hanging
  // shutdown — the thread only touches SharedState (kept alive via the
  // shared_ptr it holds), so detaching is safe.
  bool check_finished;
  {
    std::unique_lock<std::mutex> lock(state_->mutex);
    check_finished =
        state_->status_changed_cv.wait_for(lock, std::chrono::seconds(2), [this] {
          return !state_->check_in_flight.load();
        });
  }

  if (check_finished) {
    health_check_thread_.join();
  } else {
    GIZMOSQL_LOG(WARNING) << "Health check query still running at shutdown - "
                             "detaching health checker thread";
    health_check_thread_.detach();
  }
}

// PlaintextHealthServer implementation

arrow::Result<std::unique_ptr<PlaintextHealthServer>> PlaintextHealthServer::Start(
    std::shared_ptr<GizmoSQLHealthServiceImpl> health_service,
    int port) {
  if (!health_service) {
    return arrow::Status::Invalid("Health service cannot be null");
  }

  if (port <= 0 || port > 65535) {
    return arrow::Status::Invalid("Invalid port number: " + std::to_string(port));
  }

  std::string address = "0.0.0.0:" + std::to_string(port);

  grpc::ServerBuilder builder;
  builder.AddListeningPort(address, grpc::InsecureServerCredentials());
  builder.RegisterService(health_service.get());

  // Note: Reflection is enabled via InitProtoReflectionServerBuilderPlugin()
  // which is called once globally in gizmosql_library.cpp before server creation.
  // The plugin automatically applies to all ServerBuilder instances.

  auto server = builder.BuildAndStart();
  if (!server) {
    return arrow::Status::Invalid("Failed to start plaintext health server on " + address);
  }

  GIZMOSQL_LOG(INFO) << "Plaintext health server (for Kubernetes probes) listening on "
                     << address;

  auto result = std::unique_ptr<PlaintextHealthServer>(new PlaintextHealthServer());
  result->server_ = std::move(server);
  return result;
}

void PlaintextHealthServer::Shutdown() {
  if (server_) {
    server_->Shutdown();
    server_.reset();
    GIZMOSQL_LOG(INFO) << "Plaintext health server shutdown complete";
  }
}

PlaintextHealthServer::~PlaintextHealthServer() {
  Shutdown();
}

}  // namespace gizmosql
