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
#include <functional>
#include <list>
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
/// Admission is **strict FIFO**: waiters are admitted in the exact order they
/// began waiting, and a newly-arriving statement never barges ahead of one that
/// is already queued (the fast path is taken only when the queue is empty). This
/// gives fairness and bounds worst-case wait — no statement can be starved by a
/// steady stream of later arrivals.
///
/// A waiter can also be **cancelled while queued** via an optional abort predicate
/// passed to Acquire() (e.g. "was this session killed?"). Flip the condition and
/// call WakeWaiters(), and the affected statement abandons the queue immediately —
/// returning Cancelled and taking no slot — instead of waiting for a slot it would
/// never use. This is how KILL SESSION reclaims a queued statement promptly.
///
/// Thread-safe. Backed by a counter guarded by a mutex plus a FIFO list of
/// per-waiter wait nodes — each waiter parks on its *own* condition variable, so a
/// freed slot wakes exactly the one oldest waiter (targeted wakeup, no thundering
/// herd). Deliberately **not** a std::counting_semaphore so that (1) the limit can
/// be resized at runtime via SET GLOBAL, (2) the active/queued counts can be
/// introspected for a SQL-monitor view, and (3) admission order is fair. A single
/// controller is owned by the server and shared across all sessions.
class AdmissionController {
 public:
  AdmissionController() = default;
  AdmissionController(const AdmissionController&) = delete;
  AdmissionController& operator=(const AdmissionController&) = delete;

  // --- configuration (safe to call at runtime) ---

  /// Max concurrently executing statements. <= 0 means unlimited (disabled).
  void SetLimit(int32_t limit) {
    std::lock_guard<std::mutex> lock(mutex_);
    limit_ = limit < 0 ? 0 : limit;
    if (limit_ <= 0) {
      // Disabled => unlimited: release every blocked waiter as an inert handle,
      // oldest first (order is immaterial here — they all proceed untracked).
      for (Waiter* w : waiters_) {
        w->status = WaiterStatus::kGrantedInert;
        w->cv.notify_one();
      }
      waiters_.clear();
    } else {
      // Raising the limit can free capacity; hand it to the oldest waiters first.
      PromoteWaiters();
    }
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
    return static_cast<int32_t>(waiters_.size());
  }

  // --- admission ---

  /// \brief Acquire a slot for one statement execution.
  ///
  /// If \p enforce is false or the limit is <= 0, returns immediately with an
  /// inert handle (no gating, nothing tracked). Otherwise:
  ///   * if \p is_aborted already returns true, returns Cancelled without queuing;
  ///   * if a slot is free and nobody is queued ahead, takes it and returns a
  ///     holding handle;
  ///   * else, if a waiter bound is set and already reached, returns a
  ///     "queue full" error without waiting;
  ///   * else joins the back of the FIFO queue and blocks until it reaches the
  ///     front and a slot frees, the bounded wait elapses (when
  ///     \p max_queue_wait_seconds > 0), or \p is_aborted fires.
  ///
  /// \p is_aborted, when set, is an external cancellation predicate — e.g. "has
  /// this statement's session been killed?". It is checked on entry and re-checked
  /// each time the waiter wakes, so a caller that flips it true and then calls
  /// WakeWaiters() cancels a queued statement *promptly*, without waiting for a
  /// slot it will never use. A cancelled acquire returns arrow::StatusCode::Cancelled
  /// and holds no slot. The predicate runs while the internal mutex is held, so it
  /// must be cheap and must not call back into the controller.
  ///
  /// Waiters are admitted in strict FIFO order. The returned handle releases its
  /// slot on destruction (RAII); hold it for the full duration of execution.
  arrow::Result<AdmissionSlot> Acquire(bool enforce, int32_t max_queue_wait_seconds,
                                       std::function<bool()> is_aborted = {}) {
    if (!enforce) return AdmissionSlot{};

    std::unique_lock<std::mutex> lock(mutex_);

    if (limit_ <= 0) return AdmissionSlot{};  // disabled => unlimited

    // Already cancelled (e.g. the session was killed before we got here) — bail
    // out without taking a slot or joining the queue.
    if (is_aborted && is_aborted()) return AbortedStatus();

    // Fast path: capacity is available right now AND nobody is queued ahead of us.
    // The empty-queue check is what makes admission FIFO — a fresh arrival never
    // jumps the queue. (Invariant: whenever capacity is free, the queue is empty,
    // because ReleaseSlot/SetLimit always drain free capacity to waiters first.)
    if (active_ < limit_ && waiters_.empty()) {
      ++active_;
      return AdmissionSlot{this};
    }

    // No slot for us right now. Enforce the waiter bound before we block.
    if (max_queued_ > 0 && static_cast<int32_t>(waiters_.size()) >= max_queued_) {
      // TODO(phase 2): surface this as a Flight UNAVAILABLE (retriable) error.
      return arrow::Status::Invalid(
          "Statement queue is full (max_concurrent_statements=" +
          std::to_string(limit_) +
          ", max_queued_statements=" + std::to_string(max_queued_) +
          "); retry shortly.");
    }

    // Join the back of the FIFO and block on our own condition variable until a
    // granter (ReleaseSlot / SetLimit) hands us a slot, our bounded wait elapses,
    // or our abort predicate fires (WakeWaiters() nudges us to notice it). A
    // granter signals us by setting `status` and removing us from the queue.
    Waiter self;
    waiters_.push_back(&self);
    const auto it = std::prev(waiters_.end());
    const auto done = [&] {
      return self.status != WaiterStatus::kWaiting || (is_aborted && is_aborted());
    };

    bool signaled;
    if (max_queue_wait_seconds <= 0) {
      self.cv.wait(lock, done);
      signaled = true;
    } else {
      signaled = self.cv.wait_for(
          lock, std::chrono::seconds(max_queue_wait_seconds), done);
    }

    // Cancelled while queued: the abort predicate fired but no granter touched our
    // status. We still hold `mutex_` and remain in the FIFO, so `it` is valid and
    // removing ourselves is ours to do. (Checked before the timeout branch so a
    // kill that races the deadline is reported as cancelled, not a wait timeout.)
    if (self.status == WaiterStatus::kWaiting && is_aborted && is_aborted()) {
      waiters_.erase(it);
      return AbortedStatus();
    }

    if (!signaled) {
      // Bounded wait elapsed and we were never granted. We still hold `mutex_`
      // and our status is still kWaiting (the predicate-returning wait_for
      // re-checks under the lock, so a concurrent grant would have flipped it to
      // true), which means no granter has touched us — `it` is valid and removing
      // ourselves from the FIFO is ours to do.
      waiters_.erase(it);
      // TODO(phase 2): surface this as a Flight UNAVAILABLE (retriable) error.
      return arrow::Status::Invalid(
          "Statement queued longer than max_queue_wait (" +
          std::to_string(max_queue_wait_seconds) + "s); retry shortly.");
    }

    // Granted: the granter already removed us from the FIFO and accounted for the
    // slot (kGrantedSlot => ++active_ done on our behalf; kGrantedInert => the
    // limit was disabled while we waited, so we proceed untracked/unlimited).
    if (self.status == WaiterStatus::kGrantedSlot) return AdmissionSlot{this};
    return AdmissionSlot{};
  }

  /// Wake every blocked waiter so it re-evaluates its abort predicate. Call this
  /// right after flipping an external abort condition (e.g. KILL SESSION sets
  /// kill_requested) so the affected queued statement cancels promptly instead of
  /// waiting for a slot it will never use. Rare (admin-triggered); the hot
  /// per-release path stays a single targeted wakeup, so this broadcast is the
  /// only place the queue is woken en masse.
  void WakeWaiters() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (Waiter* w : waiters_) w->cv.notify_one();
  }

 private:
  friend class AdmissionSlot;

  enum class WaiterStatus {
    kWaiting,       // still blocked in the FIFO
    kGrantedSlot,   // handed a real, tracked slot (active_ already incremented)
    kGrantedInert,  // released into unlimited mode; return an inert handle
  };
  struct Waiter {
    std::condition_variable cv;
    WaiterStatus status = WaiterStatus::kWaiting;
  };

  // Status returned when a queued statement is cancelled via its abort predicate
  // (e.g. its session was killed). A distinct StatusCode (Cancelled) lets the
  // caller map it to a Flight CANCELLED, separate from the UNAVAILABLE used for
  // queue-full / wait-timeout rejections.
  static arrow::Status AbortedStatus() {
    return arrow::Status::Cancelled(
        "Statement cancelled while queued (session was killed)");
  }

  /// Hand free capacity to the oldest waiters, in FIFO order, until the limit is
  /// reached or the queue drains. Caller must hold `mutex_`. Each promoted waiter
  /// is removed from the FIFO and counted into `active_` here, then woken via its
  /// own condition variable — a targeted wakeup, never a broadcast.
  void PromoteWaiters() {
    while (limit_ > 0 && active_ < limit_ && !waiters_.empty()) {
      Waiter* w = waiters_.front();
      waiters_.pop_front();
      w->status = WaiterStatus::kGrantedSlot;
      ++active_;
      w->cv.notify_one();
    }
  }

  void ReleaseSlot() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_ > 0) --active_;
    // The freed slot belongs to the oldest waiter (FIFO), if any.
    PromoteWaiters();
  }

  mutable std::mutex mutex_;
  int32_t limit_ = 0;       // 0 => unlimited / disabled
  int32_t max_queued_ = 0;  // 0 => unbounded waiters
  int32_t default_max_queue_wait_seconds_ = 0;  // 0 => wait indefinitely
  int32_t active_ = 0;      // statements currently holding a slot
  std::list<Waiter*> waiters_;  // FIFO order; front() == oldest waiter
};

inline void AdmissionSlot::Release() {
  if (controller_) {
    controller_->ReleaseSlot();
    controller_ = nullptr;
  }
}

}  // namespace gizmosql
