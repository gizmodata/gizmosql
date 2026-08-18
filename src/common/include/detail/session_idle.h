#pragma once

#include <chrono>
#include <cstdint>

namespace gizmosql {

// Pure idle-eviction decision for --session-idle-timeout.
// timeout_seconds <= 0 means the feature is off (never evict).
inline bool ShouldEvictIdleSession(
    int32_t timeout_seconds,
    std::chrono::steady_clock::time_point last_sql_activity,
    std::chrono::steady_clock::time_point now, bool has_in_flight_sql) {
  if (timeout_seconds <= 0) {
    return false;
  }
  if (has_in_flight_sql) {
    return false;
  }
  return now - last_sql_activity >= std::chrono::seconds(timeout_seconds);
}

}  // namespace gizmosql
