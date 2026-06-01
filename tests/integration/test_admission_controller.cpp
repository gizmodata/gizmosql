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

// Unit tests for the statement-queue AdmissionController. These exercise the
// concurrency-capping mechanism directly (no server / no license required); the
// end-to-end enforcement wiring is covered by integration tests that gate on a
// licensed "statement_queue" feature.

#include "admission_controller.h"

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

using gizmosql::AdmissionController;
using gizmosql::AdmissionSlot;

namespace {

// Poll `predicate` until true or the timeout elapses. Returns the final value.
template <typename Predicate>
bool WaitFor(Predicate predicate,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return predicate();
}

// Acquire a slot, asserting success, and return the (move-only) handle.
AdmissionSlot AcquireOk(AdmissionController& controller, int max_queue_wait_seconds = 0) {
  auto result = controller.Acquire(/*enforce=*/true, max_queue_wait_seconds);
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  return std::move(result).ValueOrDie();
}

}  // namespace

// A controller with the default limit (0) is disabled: every acquire returns an
// inert handle, nothing is tracked, and nothing ever blocks.
TEST(AdmissionControllerTest, DisabledLimitIsUnlimited) {
  AdmissionController controller;  // limit defaults to 0
  EXPECT_EQ(controller.Limit(), 0);

  std::vector<AdmissionSlot> slots;
  for (int i = 0; i < 100; ++i) slots.push_back(AcquireOk(controller));

  for (const auto& slot : slots) EXPECT_FALSE(slot.holds_slot());
  EXPECT_EQ(controller.ActiveCount(), 0);
  EXPECT_EQ(controller.QueuedCount(), 0);
}

// enforce=false bypasses the gate entirely, even when a limit is set.
TEST(AdmissionControllerTest, EnforceFalseBypasses) {
  AdmissionController controller;
  controller.SetLimit(1);

  auto a = controller.Acquire(/*enforce=*/false, 0);
  ASSERT_TRUE(a.ok());
  auto b = controller.Acquire(/*enforce=*/false, 0);
  ASSERT_TRUE(b.ok());

  EXPECT_FALSE(std::move(a).ValueOrDie().holds_slot());
  EXPECT_FALSE(std::move(b).ValueOrDie().holds_slot());
  EXPECT_EQ(controller.ActiveCount(), 0);
}

// Slots are tracked and capped at the limit; releasing frees capacity.
TEST(AdmissionControllerTest, TracksAndCapsActive) {
  AdmissionController controller;
  controller.SetLimit(2);

  auto s1 = AcquireOk(controller);
  auto s2 = AcquireOk(controller);
  EXPECT_TRUE(s1.holds_slot());
  EXPECT_TRUE(s2.holds_slot());
  EXPECT_EQ(controller.ActiveCount(), 2);

  {
    AdmissionSlot s3 = std::move(s1);  // move, not a new slot
    EXPECT_EQ(controller.ActiveCount(), 2);
  }  // s3 destroyed -> one slot released
  EXPECT_EQ(controller.ActiveCount(), 1);
}

// A statement that can't get a slot blocks until one frees.
TEST(AdmissionControllerTest, BlocksUntilSlotFrees) {
  AdmissionController controller;
  controller.SetLimit(1);

  auto held = AcquireOk(controller);
  ASSERT_EQ(controller.ActiveCount(), 1);

  std::atomic<bool> acquired{false};
  std::thread waiter([&] {
    auto slot = AcquireOk(controller);
    acquired.store(true);
    EXPECT_TRUE(slot.holds_slot());
  });

  // The waiter should be blocked (one statement queued), not yet acquired.
  ASSERT_TRUE(WaitFor([&] { return controller.QueuedCount() == 1; }));
  EXPECT_FALSE(acquired.load());

  // Free the slot; the waiter should now proceed.
  held = AdmissionSlot{};  // release
  waiter.join();
  EXPECT_TRUE(acquired.load());
  EXPECT_EQ(controller.QueuedCount(), 0);
}

// Raising the limit at runtime wakes blocked waiters (no release needed).
TEST(AdmissionControllerTest, ResizeUpWakesWaiters) {
  AdmissionController controller;
  controller.SetLimit(1);

  auto held = AcquireOk(controller);
  std::atomic<bool> acquired{false};
  std::atomic<bool> release_waiter{false};
  std::thread waiter([&] {
    auto slot = AcquireOk(controller);
    acquired.store(true);
    // Keep holding the slot until the test releases us, so ActiveCount() below
    // reflects the waiter's slot rather than racing its lambda-exit release.
    while (!release_waiter.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  });

  ASSERT_TRUE(WaitFor([&] { return controller.QueuedCount() == 1; }));
  EXPECT_FALSE(acquired.load());

  controller.SetLimit(2);  // capacity now available without releasing `held`
  ASSERT_TRUE(WaitFor([&] { return acquired.load(); }));  // waiter woke and acquired
  EXPECT_EQ(controller.ActiveCount(), 2);  // `held` + the waiter's slot

  release_waiter.store(true);
  waiter.join();
}

// Disabling the limit (set to 0) while waiters are blocked lets them through as
// inert (untracked) handles.
TEST(AdmissionControllerTest, DisableWhileWaitingReleasesWaiters) {
  AdmissionController controller;
  controller.SetLimit(1);

  auto held = AcquireOk(controller);
  std::atomic<bool> acquired{false};
  std::atomic<bool> tracked{true};
  std::thread waiter([&] {
    auto slot = AcquireOk(controller);
    tracked.store(slot.holds_slot());
    acquired.store(true);
  });

  ASSERT_TRUE(WaitFor([&] { return controller.QueuedCount() == 1; }));
  controller.SetLimit(0);  // disable => unlimited
  waiter.join();
  EXPECT_TRUE(acquired.load());
  EXPECT_FALSE(tracked.load());  // proceeded as an inert handle
}

// With a waiter bound, acquires beyond the bound are rejected immediately.
TEST(AdmissionControllerTest, MaxQueuedRejects) {
  AdmissionController controller;
  controller.SetLimit(1);
  controller.SetMaxQueued(1);

  auto held = AcquireOk(controller);  // occupies the only slot

  std::atomic<bool> waiter_done{false};
  std::thread waiter([&] {
    auto slot = controller.Acquire(/*enforce=*/true, 0);  // becomes the 1 allowed waiter
    waiter_done.store(true);
  });
  ASSERT_TRUE(WaitFor([&] { return controller.QueuedCount() == 1; }));

  // The next acquire exceeds max_queued -> rejected without blocking.
  auto rejected = controller.Acquire(/*enforce=*/true, 0);
  EXPECT_FALSE(rejected.ok());

  held = AdmissionSlot{};  // release so the queued waiter can finish
  waiter.join();
  EXPECT_TRUE(waiter_done.load());
}

// A bounded wait that elapses returns an error.
TEST(AdmissionControllerTest, MaxQueueWaitTimesOut) {
  AdmissionController controller;
  controller.SetLimit(1);

  auto held = AcquireOk(controller);  // occupy the slot for the whole test

  auto start = std::chrono::steady_clock::now();
  auto result = controller.Acquire(/*enforce=*/true, /*max_queue_wait_seconds=*/1);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_FALSE(result.ok());
  EXPECT_GE(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(), 900);
  EXPECT_EQ(controller.QueuedCount(), 0);
}

// Stress: under a fixed limit, the number of concurrently held slots never
// exceeds the limit, and all work completes.
TEST(AdmissionControllerTest, ConcurrencyNeverExceedsLimit) {
  AdmissionController controller;
  constexpr int kLimit = 4;
  constexpr int kWorkers = 32;
  controller.SetLimit(kLimit);

  std::atomic<int> active{0};
  std::atomic<int> peak{0};
  std::atomic<int> completed{0};

  std::vector<std::thread> workers;
  workers.reserve(kWorkers);
  for (int i = 0; i < kWorkers; ++i) {
    workers.emplace_back([&] {
      auto slot = AcquireOk(controller);
      int now = active.fetch_add(1) + 1;
      int prev_peak = peak.load();
      while (now > prev_peak && !peak.compare_exchange_weak(prev_peak, now)) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(3));
      active.fetch_sub(1);
      completed.fetch_add(1);
    });
  }
  for (auto& w : workers) w.join();

  EXPECT_EQ(completed.load(), kWorkers);
  EXPECT_LE(peak.load(), kLimit);
  EXPECT_GE(peak.load(), 1);
  EXPECT_EQ(controller.ActiveCount(), 0);
  EXPECT_EQ(controller.QueuedCount(), 0);
}
