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

// Verifies that RegisterAdmissionQueueGauges() wires AdmissionController's live
// active/queued counts into the OTLP metrics export as an observable gauge
// (gizmosql.statement_queue.depth{state="active"|"queued"}). Only built when
// WITH_OPENTELEMETRY is on (see tests/CMakeLists.txt) since it exercises the real
// OTel SDK. This test drives the real OTLP/HTTP exporter against a local fake
// collector rather than mocking it out, so it also protects against the exporter
// silently dropping the async instrument. The exported body is OTLP protobuf
// (not JSON) — metric/attribute names are still length-prefixed raw UTF-8 within
// it, so a substring search is sufficient to confirm the gauge was exported without
// a full protobuf decode.

#include "admission_controller.h"
#include "gizmosql_telemetry.h"

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>

#include <gtest/gtest.h>
#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>

#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>

using gizmosql::AdmissionController;
using gizmosql::AdmissionSlot;
using gizmosql::TelemetryConfig;

namespace {

// Poll `predicate` until true or the timeout elapses. Returns the final value.
template <typename Predicate>
bool WaitFor(Predicate predicate,
             std::chrono::milliseconds timeout = std::chrono::milliseconds(5000)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return predicate();
}

// Acquire a slot, asserting success, and return the (move-only) handle.
AdmissionSlot AcquireOk(AdmissionController& controller, int max_queue_wait_seconds = 0) {
  auto result = controller.Acquire(/*enforce=*/true, max_queue_wait_seconds);
  EXPECT_TRUE(result.ok()) << result.status().ToString();
  return std::move(result).ValueOrDie();
}

// A minimal local OTLP/HTTP collector: just captures POST bodies, it doesn't parse
// them. Binds an ephemeral port so parallel test runs never collide.
class FakeOtlpCollector {
 public:
  FakeOtlpCollector() {
    server_.Post("/v1/metrics",
                 [this](const httplib::Request& req, httplib::Response& res) {
                   {
                     std::lock_guard<std::mutex> lock(mutex_);
                     last_metrics_body_ = req.body;
                     ++request_count_;
                   }
                   res.status = 200;
                 });
    port_ = server_.bind_to_any_port("127.0.0.1");
    thread_ = std::thread([this] { server_.listen_after_bind(); });
    while (!server_.is_running()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  ~FakeOtlpCollector() {
    server_.stop();
    if (thread_.joinable()) thread_.join();
  }

  int port() const { return port_; }

  std::string LastMetricsBody() {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_metrics_body_;
  }

  int RequestCount() {
    std::lock_guard<std::mutex> lock(mutex_);
    return request_count_;
  }

 private:
  httplib::Server server_;
  std::thread thread_;
  int port_ = 0;
  std::mutex mutex_;
  std::string last_metrics_body_;
  int request_count_ = 0;
};

}  // namespace

class AdmissionQueueTelemetryTest : public ::testing::Test {
 protected:
  // Telemetry is process-global state (see gizmosql_telemetry.cpp); always leave it
  // shut down so other test binaries in this suite aren't affected.
  void TearDown() override { gizmosql::ShutdownTelemetry(); }
};

TEST_F(AdmissionQueueTelemetryTest, ReportsActiveAndQueuedCounts) {
  FakeOtlpCollector collector;

  TelemetryConfig config;
  config.enabled = true;
  config.exporter_type = gizmosql::OtlpExporterType::kHttp;
  config.endpoint = "http://127.0.0.1:" + std::to_string(collector.port());
  config.service_name = "gizmosql-test";
  // The SDK's periodic reader requires timeout < interval (otherwise it logs a
  // warning and falls back to its own defaults); ForceFlush() below is what
  // actually drives collection in this test, so these just need to be valid.
  config.export_interval = std::chrono::milliseconds(60000);
  config.export_timeout = std::chrono::milliseconds(5000);
  config.traces_enabled = false;
  config.metrics_enabled = true;

  gizmosql::InitTelemetry(config);
  ASSERT_TRUE(gizmosql::IsTelemetryEnabled());

  AdmissionController controller;
  controller.SetLimit(1);
  controller.SetMaxQueued(10);

  gizmosql::metrics::RegisterAdmissionQueueGauges(controller);

  // Hold the single slot, then queue a second statement behind it on another
  // thread so both `state=active` and `state=queued` are non-zero.
  AdmissionSlot active_slot = AcquireOk(controller);
  ASSERT_EQ(controller.ActiveCount(), 1);

  std::atomic<bool> waiter_acquired{false};
  std::thread waiter([&] {
    AdmissionSlot slot = AcquireOk(controller);
    waiter_acquired.store(true);
  });

  ASSERT_TRUE(WaitFor([&] { return controller.QueuedCount() == 1; }));
  EXPECT_FALSE(waiter_acquired.load());

  // Force an immediate collection so the observable-gauge callback runs without
  // waiting for the periodic export interval to elapse.
  if (auto provider = opentelemetry::metrics::Provider::GetMeterProvider()) {
    if (auto* sdk_provider =
            dynamic_cast<opentelemetry::sdk::metrics::MeterProvider*>(provider.get())) {
      sdk_provider->ForceFlush();
    }
  }

  ASSERT_TRUE(WaitFor([&] { return collector.RequestCount() > 0; }));
  const std::string body = collector.LastMetricsBody();
  EXPECT_NE(body.find("gizmosql.statement_queue.depth"), std::string::npos);
  EXPECT_NE(body.find("active"), std::string::npos);
  EXPECT_NE(body.find("queued"), std::string::npos);

  // Release the active slot so the queued waiter is admitted, then join it.
  active_slot = AdmissionSlot{};
  waiter.join();
  EXPECT_TRUE(waiter_acquired.load());
}

TEST(AdmissionQueueTelemetryDisabledTest, NoOpWhenTelemetryDisabled) {
  // No InitTelemetry() call in this process at this point (or a prior test already
  // shut it down) — registering gauges against a live controller must not create
  // instruments or crash.
  AdmissionController controller;
  controller.SetLimit(1);
  gizmosql::metrics::RegisterAdmissionQueueGauges(controller);
  EXPECT_FALSE(gizmosql::IsTelemetryEnabled());
}
