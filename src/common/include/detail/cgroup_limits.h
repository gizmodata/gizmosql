// Copyright (c) 2026 GizmoData LLC. Licensed under the Apache License, Version 2.0.
//
// Container (cgroup) resource limits, for the boot-time system summary.
//
// Inside a container, sysconf() reports the HOST's RAM and CPUs, which is what
// the "Memory:" / "CPU:" boot lines used to show — misleading when diagnosing
// an OOM-killed pod (a 512 GB node printed "Memory: 494.8 GB" for a container
// capped at 476 GB). DuckDB itself is cgroup-aware (its default memory_limit
// is 80% of min(physical, cgroup limit); threads follow the CPU quota), so the
// log should show the same numbers the engine actually works with.
//
// Reads cgroup v2 (/sys/fs/cgroup/memory.max, cpu.max) with a cgroup v1
// fallback (memory/memory.limit_in_bytes, cpu/cpu.cfs_quota_us +
// cpu.cfs_period_us). Header-only and root-relative so it can be unit-tested
// against a scratch directory. Linux-only in practice; on other platforms the
// files simply don't exist and "no limit" is reported.
#pragma once

#include <cstdint>
#include <fstream>
#include <limits>
#include <string>

namespace gizmosql {

struct CGroupLimits {
  // -1 when the cgroup imposes no limit (or none could be read).
  int64_t memory_limit_bytes = -1;
  // Fractional CPUs the cgroup allows (quota / period); -1 when unlimited.
  double cpu_quota = -1.0;

  bool has_memory_limit() const { return memory_limit_bytes > 0; }
  bool has_cpu_quota() const { return cpu_quota > 0.0; }
};

namespace detail {

inline bool ReadFirstLine(const std::string& path, std::string* out) {
  std::ifstream in(path);
  if (!in.is_open()) return false;
  std::string line;
  if (!std::getline(in, line)) return false;
  // Trim trailing whitespace / newline.
  while (!line.empty() && (line.back() == '\n' || line.back() == '\r' || line.back() == ' ')) {
    line.pop_back();
  }
  *out = line;
  return true;
}

inline bool ParseInt64(const std::string& text, int64_t* out) {
  if (text.empty()) return false;
  try {
    size_t consumed = 0;
    const long long v = std::stoll(text, &consumed);
    if (consumed == 0) return false;
    *out = static_cast<int64_t>(v);
    return true;
  } catch (...) {
    return false;
  }
}

// cgroup v1 reports "no limit" as a huge number (PAGE_COUNTER_MAX rounded to
// the page size), not as a word — treat anything ≥ 2^62 as unlimited.
inline bool IsUnlimitedBytes(int64_t v) {
  return v <= 0 || v >= (int64_t{1} << 62);
}

}  // namespace detail

// `root` is the cgroup filesystem mount ("/sys/fs/cgroup" in production).
inline CGroupLimits ReadCGroupLimits(const std::string& root = "/sys/fs/cgroup") {
  CGroupLimits limits;
  std::string line;

  // ---- Memory ---------------------------------------------------------------
  // v2: memory.max is "max" or bytes.
  if (detail::ReadFirstLine(root + "/memory.max", &line)) {
    int64_t v = 0;
    if (line != "max" && detail::ParseInt64(line, &v) && !detail::IsUnlimitedBytes(v)) {
      limits.memory_limit_bytes = v;
    }
  } else if (detail::ReadFirstLine(root + "/memory/memory.limit_in_bytes", &line)) {
    // v1
    int64_t v = 0;
    if (detail::ParseInt64(line, &v) && !detail::IsUnlimitedBytes(v)) {
      limits.memory_limit_bytes = v;
    }
  }

  // ---- CPU ------------------------------------------------------------------
  // v2: cpu.max is "<quota> <period>" or "max <period>".
  if (detail::ReadFirstLine(root + "/cpu.max", &line)) {
    const auto space = line.find(' ');
    if (space != std::string::npos) {
      const std::string quota_text = line.substr(0, space);
      int64_t quota = 0, period = 0;
      if (quota_text != "max" && detail::ParseInt64(quota_text, &quota) &&
          detail::ParseInt64(line.substr(space + 1), &period) && quota > 0 && period > 0) {
        limits.cpu_quota = static_cast<double>(quota) / static_cast<double>(period);
      }
    }
  } else {
    // v1: cfs_quota_us (-1 = unlimited) / cfs_period_us.
    std::string quota_line, period_line;
    if (detail::ReadFirstLine(root + "/cpu/cpu.cfs_quota_us", &quota_line) &&
        detail::ReadFirstLine(root + "/cpu/cpu.cfs_period_us", &period_line)) {
      int64_t quota = 0, period = 0;
      if (detail::ParseInt64(quota_line, &quota) && detail::ParseInt64(period_line, &period) &&
          quota > 0 && period > 0) {
        limits.cpu_quota = static_cast<double>(quota) / static_cast<double>(period);
      }
    }
  }

  return limits;
}

}  // namespace gizmosql
