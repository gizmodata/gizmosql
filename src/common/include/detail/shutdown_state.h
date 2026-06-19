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
#include <cstdint>
#include <functional>

// --- Graceful shutdown (drain) state ----------------------------------------
//
// When graceful shutdown is enabled (--graceful-shutdown / GIZMOSQL_GRACEFUL_SHUTDOWN),
// the first SIGINT/SIGTERM does NOT stop the server immediately. Instead the
// server enters a "draining" state:
//
//   * new sessions and new statement executions are rejected with a Flight
//     UNAVAILABLE error ("instance is shutting down"), while
//   * already-running queries and their in-progress result fetches are allowed
//     to finish (or hit their per-query timeout).
//
// A watcher thread (see RunFlightSQLServer in gizmosql_library.cpp) waits for
// in-flight work to drain — bounded by --shutdown-grace-period-seconds — and
// then performs the real shutdown.
//
// These globals are process-wide because the async signal handler, the watcher
// thread (in the library) and the request handlers (in the DuckDB backend) all
// need to see the same state without a shared object reference. They are inline
// variables so a single definition is shared across translation units.

namespace gizmosql {

/// Whether graceful drain-on-signal is enabled. Seeded at startup from
/// --graceful-shutdown / GIZMOSQL_GRACEFUL_SHUTDOWN and live-adjustable at runtime
/// via `SET GLOBAL gizmosql.graceful_shutdown` (admin only). The drain watcher
/// thread is started unconditionally; it consults this flag when the first
/// shutdown signal arrives — true ⇒ drain in-flight work, false ⇒ stop the server
/// immediately (the historical default behavior). Changing this mid-drain has no
/// effect: the choice is latched when the drain begins.
inline std::atomic<bool> g_graceful_enabled{false};

/// Maximum seconds to wait for in-flight work to drain before forcing shutdown
/// (0 = wait indefinitely). Seeded at startup from --shutdown-grace-period-seconds
/// / GIZMOSQL_SHUTDOWN_GRACE_PERIOD_SECONDS and live-adjustable at runtime via
/// `SET GLOBAL gizmosql.shutdown_grace_period_seconds` (admin only). The drain
/// watcher re-reads this every loop iteration, so a change takes effect even
/// while a drain is already in progress (e.g. extend the cap when a drain is
/// taking longer than expected).
inline std::atomic<int32_t> g_grace_period_seconds{300};

/// True once a graceful drain has begun. Request handlers consult this to reject
/// new work. Set by the drain watcher thread (never by the async signal handler,
/// which only flips a sig_atomic counter).
inline std::atomic<bool> g_draining{false};

/// Number of in-flight query executions + result-stream fetches currently
/// running. The drain watcher waits for this to reach zero before stopping the
/// server (or until the grace period elapses).
inline std::atomic<int64_t> g_inflight_requests{0};

/// Optional hook the backend registers to interrupt every in-flight query when a
/// FORCED shutdown is requested (second signal, or grace-period elapsed with work
/// still running). gRPC's Shutdown() — even with an immediate deadline — waits for
/// a synchronous query handler to unwind rather than preempting it, so without
/// interrupting the running queries a "force" would still block until they finish.
/// The hook (DuckDB: interrupt all session connections) makes those handlers return
/// promptly. Registered by RunFlightSQLServer() for backends that support it and
/// cleared after the server stops; empty when unsupported (e.g. SQLite) or unset.
inline std::function<void()> g_force_interrupt_hook;

/// True while the server is draining and rejecting new work.
inline bool IsDraining() noexcept { return g_draining.load(std::memory_order_acquire); }

/// Current count of in-flight query executions + fetches.
inline int64_t InFlightRequestCount() noexcept {
  return g_inflight_requests.load(std::memory_order_acquire);
}

/// Whether graceful drain-on-signal is currently enabled.
inline bool GracefulShutdownEnabled() noexcept {
  return g_graceful_enabled.load(std::memory_order_acquire);
}

/// Current drain grace cap in seconds (0 = wait indefinitely).
inline int32_t GracefulShutdownGracePeriodSeconds() noexcept {
  return g_grace_period_seconds.load(std::memory_order_acquire);
}

/// RAII counter for one in-flight query/fetch. Held for the full duration of a
/// query's execution AND the streaming of its results back to the client, so the
/// drain watcher does not stop the server out from under an active query. The
/// result-stream reader holds one of these for its whole lifetime; update
/// handlers (which have no result stream) hold one for the handler scope.
class InFlightGuard {
 public:
  InFlightGuard() noexcept { g_inflight_requests.fetch_add(1, std::memory_order_acq_rel); }
  ~InFlightGuard() { g_inflight_requests.fetch_sub(1, std::memory_order_acq_rel); }
  InFlightGuard(const InFlightGuard&) = delete;
  InFlightGuard& operator=(const InFlightGuard&) = delete;
  InFlightGuard(InFlightGuard&&) = delete;
  InFlightGuard& operator=(InFlightGuard&&) = delete;
};

}  // namespace gizmosql
