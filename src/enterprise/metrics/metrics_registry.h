// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
#pragma once
#include <array>
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <duckdb/common/enums/statement_type.hpp>

namespace gizmosql::enterprise {
using MetricLabels = std::map<std::string, std::string>;
struct MetricSample {
  std::string name, kind;
  MetricLabels labels;
  double value;
  std::string help;
  std::string family;
};
enum class StatementKind { Read, Write, Ddl, Other };
enum class StatementError {
  OutOfMemory,
  Io,
  Catalog,
  Syntax,
  Permission,
  Cancelled,
  Other
};
StatementKind ClassifyStatement(duckdb::StatementType type);
StatementError ClassifyStatementError(const std::string& error);

class MetricsRegistry {
 public:
  struct Series {
    std::string name, kind, help;
    MetricLabels labels;
    std::atomic<double> value{0};
  };
  // Registration happens before the registry is published to query threads.
  MetricsRegistry();
  Series& Add(std::string name, std::string kind, std::string help,
              MetricLabels labels = {}, double initial = 0);
  Series& At(const std::string& name, MetricLabels labels = {});
  std::vector<MetricSample> Snapshot() const;
  static std::string Serialize(const std::vector<MetricSample>& samples);
  void StatementFinished(StatementKind kind, const std::string& error, double seconds);
  void ObserveQueueWait(double seconds);
  static std::shared_ptr<MetricsRegistry> Current();
  static void Publish(std::shared_ptr<MetricsRegistry> registry);

 private:
  struct Histogram {
    std::array<std::atomic<uint64_t>, 15> buckets{};
    std::atomic<double> sum{0};
    void Observe(double seconds);
  };
  static constexpr std::array<double, 14> kBounds{.005, .01, .025, .05, .1, .25, .5,
                                                  1,    2.5, 5,    10,  30, 60,  300};
  std::map<std::pair<std::string, MetricLabels>, std::unique_ptr<Series>> series_;
  std::array<Histogram, 4> durations_;
  Histogram queue_wait_;
  std::array<Series*, 3> outcomes_{};
  std::array<Series*, 7> errors_{};
  std::chrono::steady_clock::time_point started_ = std::chrono::steady_clock::now();
};
}  // namespace gizmosql::enterprise
