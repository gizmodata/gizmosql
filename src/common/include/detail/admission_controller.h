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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

namespace gizmosql {

class AdmissionController;

/// \brief RAII handle representing one held statement-queue slot.
///
/// Move-only. A default-constructed or moved-from handle owns nothing and
/// releases nothing on destruction. Hold the handle for the full duration of
/// statement execution; its destruction returns the slot to the controller.
class AdmissionSlot {
 public:
  AdmissionSlot() = default;
  AdmissionSlot(const AdmissionSlot&) = delete;
  AdmissionSlot& operator=(const AdmissionSlot&) = delete;

  AdmissionSlot(AdmissionSlot&& other) noexcept : controller_(other.controller_) {
    other.controller_ = nullptr;
  }
  AdmissionSlot& operator=(AdmissionSlot&& other) noexcept {
    if (this != &other) {
      Release();
      controller_ = other.controller_;
      other.controller_ = nullptr;
    }
    return *this;
  }
  ~AdmissionSlot() { Release(); }

  /// \brief True if this handle holds a real (tracked) slot, as opposed to an
  ///        inert handle returned when admission control is disabled/bypassed.
  bool holds_slot() const { return controller_ != nullptr; }

 private:
  friend class AdmissionController;
  explicit AdmissionSlot(AdmissionController* controller) : controller_(controller) {}
  inline void Release();  // defined below, after AdmissionController

  AdmissionController* controller_ = nullptr;  // non-null => holds a slot
};

/// \brief Server-wide statement admission controller.
///
/// Caps the number of statements *executing concurrently* to a configurable
/// limit. Statements beyond the limit block until a slot frees and — once the
/// optional waiter bound or per-call wait timeout is exceeded — are rejected.
///
/// Thread-safe. Backed by a condition-variable-guarded counter (deliberately
/// **not** a std::counting_semaphore) so that (1) the limit can be resized at
/// runtime via SET GLOBAL, and (2) the active/queued counts can be introspected
/// for a SQL-monitor view. A single controller is owned by the server and shared
/// across all sessions.
class AdmissionController {
 public:
  AdmissionController() = default;
  AdmissionController(const AdmissionController&) = delete;
  AdmissionController& operator=(const AdmissionController&) = delete;

  // --- configuration (safe to call at runtime) ---

  /// Max concurrently executing statements. <= 0 means unlimited (disabled).
  void SetLimit(int32_t limit) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      limit_ = limit < 0 ? 0 : limit;
    }
    // Raising the limit (or disabling it) can free capacity for blocked waiters,
    // so wake them all to re-check the predicate.
    cv_.notify_all();
  }

  /// Max statements that may wait for a slot at once. <= 0 means unbounded.
  void SetMaxQueued(int32_t max_queued) {
    std::lock_guard<std::mutex> lock(mutex_);
    max_queued_ = max_queued < 0 ? 0 : max_queued;
  }

  /// Default max seconds a statement may wait for a slot before being rejected.
  /// <= 0 means wait indefinitely. Callers that don't pass an explicit per-call
  /// wait can read this default via DefaultMaxQueueWaitSeconds().
  void SetDefaultMaxQueueWaitSeconds(int32_t seconds) {
    std::lock_guard<std::mutex> lock(mutex_);
    default_max_queue_wait_seconds_ = seconds < 0 ? 0 : seconds;
  }

  int32_t Limit() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return limit_;
  }
  int32_t MaxQueued() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return max_queued_;
  }
  int32_t DefaultMaxQueueWaitSeconds() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return default_max_queue_wait_seconds_;
  }
  /// Number of statements currently holding a slot.
  int32_t ActiveCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return active_;
  }
  /// Number of statements currently blocked waiting for a slot.
  int32_t QueuedCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return waiting_;
  }

  // --- admission ---

  /// \brief Acquire a slot for one statement execution.
  ///
  /// If \p enforce is false or the limit is <= 0, returns immediately with an
  /// inert handle (no gating, nothing tracked). Otherwise:
  ///   * if a slot is free, takes it and returns a holding handle;
  ///   * else, if a waiter bound is set and already reached, returns a
  ///     "queue full" error without waiting;
  ///   * else blocks until a slot frees, or — when \p max_queue_wait_seconds > 0
  ///     and that wait elapses first — returns a "queue wait timeout" error.
  ///
  /// The returned handle releases its slot on destruction (RAII). Hold it for the
  /// full duration of statement execution.
  arrow::Result<AdmissionSlot> Acquire(bool enforce, int32_t max_queue_wait_seconds) {
    if (!enforce) return AdmissionSlot{};

    std::unique_lock<std::mutex> lock(mutex_);

    if (limit_ <= 0) return AdmissionSlot{};  // disabled => unlimited

    // Fast path: capacity is available right now.
    if (active_ < limit_) {
      ++active_;
      return AdmissionSlot{this};
    }

    // No capacity. Enforce the waiter bound before we block.
    if (max_queued_ > 0 && waiting_ >= max_queued_) {
      // TODO(phase 2): surface this as a Flight UNAVAILABLE (retriable) error.
      return arrow::Status::Invalid(
          "Statement queue is full (max_concurrent_statements=" +
          std::to_string(limit_) +
          ", max_queued_statements=" + std::to_string(max_queued_) +
          "); retry shortly.");
    }

    // "Capacity" includes the case where the limit was disabled while we waited.
    const auto has_capacity = [this]() { return limit_ <= 0 || active_ < limit_; };

    ++waiting_;
    bool got_capacity;
    if (max_queue_wait_seconds <= 0) {
      cv_.wait(lock, has_capacity);
      got_capacity = true;
    } else {
      got_capacity = cv_.wait_for(
          lock, std::chrono::seconds(max_queue_wait_seconds), has_capacity);
    }
    --waiting_;

    if (!got_capacity) {
      // TODO(phase 2): surface this as a Flight UNAVAILABLE (retriable) error.
      return arrow::Status::Invalid(
          "Statement queued longer than max_queue_wait (" +
          std::to_string(max_queue_wait_seconds) + "s); retry shortly.");
    }

    // Re-check under the lock: the limit may have been disabled while we waited.
    if (limit_ <= 0) return AdmissionSlot{};  // became unlimited => inert handle
    ++active_;
    return AdmissionSlot{this};
  }

 private:
  friend class AdmissionSlot;

  void ReleaseSlot() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (active_ > 0) --active_;
    }
    cv_.notify_one();  // exactly one slot freed => wake exactly one waiter
  }

  mutable std::mutex mutex_;
  std::condition_variable cv_;
  int32_t limit_ = 0;       // 0 => unlimited / disabled
  int32_t max_queued_ = 0;  // 0 => unbounded waiters
  int32_t default_max_queue_wait_seconds_ = 0;  // 0 => wait indefinitely
  int32_t active_ = 0;      // statements currently holding a slot
  int32_t waiting_ = 0;     // statements currently blocked in Acquire()
};

inline void AdmissionSlot::Release() {
  if (controller_) {
    controller_->ReleaseSlot();
    controller_ = nullptr;
  }
}

}  // namespace gizmosql
