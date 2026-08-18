#include <gtest/gtest.h>

#include <chrono>

#include "detail/session_idle.h"

using gizmosql::ShouldEvictIdleSession;

TEST(SessionIdleTest, TimeoutZeroNeverEvicts) {
  const auto now = std::chrono::steady_clock::now();
  const auto last = now - std::chrono::hours(24);
  EXPECT_FALSE(ShouldEvictIdleSession(0, last, now, false));
}

TEST(SessionIdleTest, EvictsWhenPastTimeoutWithoutInFlightSql) {
  const auto now = std::chrono::steady_clock::now();
  const auto last = now - std::chrono::seconds(30);
  EXPECT_TRUE(ShouldEvictIdleSession(10, last, now, false));
}

TEST(SessionIdleTest, DoesNotEvictBeforeTimeout) {
  const auto now = std::chrono::steady_clock::now();
  const auto last = now - std::chrono::seconds(5);
  EXPECT_FALSE(ShouldEvictIdleSession(10, last, now, false));
}

TEST(SessionIdleTest, SqlActivityRefreshesClock) {
  const auto now = std::chrono::steady_clock::now();
  const auto old_last = now - std::chrono::seconds(30);
  EXPECT_TRUE(ShouldEvictIdleSession(10, old_last, now, false));
  const auto refreshed = now - std::chrono::seconds(2);
  EXPECT_FALSE(ShouldEvictIdleSession(10, refreshed, now, false));
}

TEST(SessionIdleTest, InFlightSqlNotIdle) {
  const auto now = std::chrono::steady_clock::now();
  const auto last = now - std::chrono::seconds(30);
  EXPECT_FALSE(ShouldEvictIdleSession(10, last, now, true));
}
