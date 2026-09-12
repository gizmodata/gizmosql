// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.
#include "metrics_registry.h"
#include <algorithm>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <locale>
#include <sstream>

namespace gizmosql::enterprise {
namespace {
std::shared_ptr<MetricsRegistry> current;
constexpr const char* kinds[] = {"read", "write", "ddl", "other"};
constexpr const char* errors[] = {"out_of_memory", "io",        "catalog", "syntax",
                                  "permission",    "cancelled", "other"};
std::string Escape(const std::string& value, bool label) {
  std::string out;
  for (char c : value) {
    if (c == '\\')
      out += "\\\\";
    else if (c == '\n')
      out += "\\n";
    else if (c == '"' && label)
      out += "\\\"";
    else
      out += c;
  }
  return out;
}
}  // namespace
StatementKind ClassifyStatement(duckdb::StatementType type) {
  using T = duckdb::StatementType;
  switch (type) {
    case T::SELECT_STATEMENT:
    case T::EXPLAIN_STATEMENT:
      return StatementKind::Read;
    case T::INSERT_STATEMENT:
    case T::UPDATE_STATEMENT:
    case T::DELETE_STATEMENT:
    case T::MERGE_INTO_STATEMENT:
    case T::COPY_STATEMENT:
      return StatementKind::Write;
    case T::CREATE_STATEMENT:
    case T::ALTER_STATEMENT:
    case T::DROP_STATEMENT:
    case T::ATTACH_STATEMENT:
    case T::DETACH_STATEMENT:
      return StatementKind::Ddl;
    default:
      return StatementKind::Other;
  }
}
StatementError ClassifyStatementError(const std::string& error) {
  std::string s = error;
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (s.find("out of memory") != s.npos || s.find("outofmemory") != s.npos)
    return StatementError::OutOfMemory;
  if (s.find("cancel") != s.npos || s.find("interrupt") != s.npos)
    return StatementError::Cancelled;
  if (s.find("permission") != s.npos || s.find("not authorized") != s.npos ||
      s.find("access denied") != s.npos)
    return StatementError::Permission;
  if (s.find("io error") != s.npos || s.find("i/o error") != s.npos ||
      s.find("disk full") != s.npos)
    return StatementError::Io;
  if (s.find("catalog") != s.npos || s.find("binder error") != s.npos)
    return StatementError::Catalog;
  if (s.find("parser error") != s.npos || s.find("syntax") != s.npos)
    return StatementError::Syntax;
  return StatementError::Other;
}
MetricsRegistry::Series& MetricsRegistry::Add(std::string name, std::string kind,
                                              std::string help, MetricLabels labels,
                                              double initial) {
  auto series = std::make_unique<Series>();
  series->name = name;
  series->kind = std::move(kind);
  series->help = std::move(help);
  series->labels = labels;
  series->value.store(initial);
  auto [it, inserted] = series_.emplace(std::make_pair(name, labels), std::move(series));
  if (!inserted) throw std::invalid_argument("Duplicate metric series: " + name);
  return *it->second;
}
MetricsRegistry::Series& MetricsRegistry::At(const std::string& name,
                                             MetricLabels labels) {
  return *series_.at({name, labels});
}
MetricsRegistry::MetricsRegistry() {
  for (const auto* state :
       {"active", "idle", "idle_in_transaction", "idle_in_transaction_aborted"})
    Add("gizmosql_sessions", "gauge", "Open sessions by execution and transaction state.",
        {{"state", state}});
  Add("gizmosql_sessions_opened_total", "counter",
      "Sessions created since server startup.");
  for (const auto* reason : {"idle_timeout", "max_lifetime", "client_close",
                             "client_abandoned", "server_shutdown"})
    Add("gizmosql_sessions_reaped_total", "counter", "Sessions ended by reason.",
        {{"reason", reason}});
  for (const auto* wait : {"none", "client_send", "io", "lock", "queue"})
    Add("gizmosql_statements_active", "gauge",
        "Statements in flight by observed wait state.", {{"wait", wait}},
        std::string(wait) == "io" || std::string(wait) == "lock" ? NAN : 0);
  const char* outcomes[] = {"ok", "error", "cancelled"};
  for (size_t i = 0; i < 3; ++i)
    outcomes_[i] =
        &Add("gizmosql_statements_total", "counter",
             "Completed user statement executions.", {{"status", outcomes[i]}});
  for (size_t i = 0; i < 7; ++i)
    errors_[i] = &Add("gizmosql_statement_errors_total", "counter",
                      "Failed user statements by error class.", {{"class", errors[i]}});
  Add("gizmosql_queue_depth", "gauge", "Statements waiting for admission.");
  for (auto reason : {"full", "timeout"})
    Add("gizmosql_queue_rejected_total", "counter", "Queue rejections by reason.",
        {{"reason", reason}});
  Add("gizmosql_queue_admin_bypass_total", "counter",
      "Statements using administrator admission bypass.");
  Add("gizmosql_session_limit", "gauge",
      "Configured session limit; zero means unlimited.");
  Add("gizmosql_concurrency_limit", "gauge",
      "Configured statement concurrency limit; zero means unlimited.");
}
void MetricsRegistry::Histogram::Observe(double seconds) {
  const auto bucket =
      std::lower_bound(kBounds.begin(), kBounds.end(), seconds) - kBounds.begin();
  buckets[bucket].fetch_add(1, std::memory_order_relaxed);
  sum.fetch_add(seconds, std::memory_order_relaxed);
}
void MetricsRegistry::StatementFinished(StatementKind kind, const std::string& error,
                                        double seconds) {
  size_t outcome = 0;
  if (!error.empty()) {
    const auto e = ClassifyStatementError(error);
    errors_[static_cast<size_t>(e)]->value.fetch_add(1, std::memory_order_relaxed);
    outcome = e == StatementError::Cancelled ? 2 : 1;
  }
  outcomes_[outcome]->value.fetch_add(1, std::memory_order_relaxed);
  durations_[static_cast<size_t>(kind)].Observe(seconds);
}
void MetricsRegistry::ObserveQueueWait(double seconds) { queue_wait_.Observe(seconds); }
std::shared_ptr<MetricsRegistry> MetricsRegistry::Current() {
  return std::atomic_load(&current);
}
void MetricsRegistry::Publish(std::shared_ptr<MetricsRegistry> registry) {
  std::atomic_store(&current, std::move(registry));
}
std::vector<MetricSample> MetricsRegistry::Snapshot() const {
  std::vector<MetricSample> out;
  for (const auto& [key, s] : series_) {
    const double value = s->value.load(std::memory_order_relaxed);
    if (!std::isnan(value))
      out.push_back({s->name, s->kind, s->labels, value, s->help, s->name});
  }
  out.push_back(
      {"gizmosql_up_seconds",
       "gauge",
       {},
       std::chrono::duration<double>(std::chrono::steady_clock::now() - started_).count(),
       "Seconds since server startup.",
       "gizmosql_up_seconds"});
  auto histogram = [&](const Histogram& h, const std::string& name, MetricLabels labels,
                       const std::string& help) {
    uint64_t count = 0;
    for (size_t i = 0; i < h.buckets.size(); ++i) {
      count += h.buckets[i].load(std::memory_order_relaxed);
      std::ostringstream le;
      le.imbue(std::locale::classic());
      if (i == kBounds.size())
        le << "+Inf";
      else
        le << kBounds[i];
      auto bucket_labels = labels;
      bucket_labels["le"] = le.str();
      out.push_back({name + "_bucket", "histogram", bucket_labels,
                     static_cast<double>(count), help, name});
    }
    out.push_back({name + "_sum", "histogram", labels,
                   h.sum.load(std::memory_order_relaxed), help, name});
    out.push_back(
        {name + "_count", "histogram", labels, static_cast<double>(count), help, name});
  };
  for (size_t i = 0; i < 4; ++i)
    histogram(durations_[i], "gizmosql_statement_duration_seconds", {{"kind", kinds[i]}},
              "Wall time of completed user statements.");
  histogram(queue_wait_, "gizmosql_queue_wait_seconds", {},
            "Time spent waiting for statement admission.");
  return out;
}
std::string MetricsRegistry::Serialize(const std::vector<MetricSample>& samples) {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << std::setprecision(17);
  std::string family;
  for (const auto& s : samples) {
    if (s.family != family) {
      family = s.family;
      out << "# HELP " << family << ' ' << Escape(s.help, false) << '\n';
      out << "# TYPE " << family << ' ' << s.kind << '\n';
    }
    out << s.name;
    if (!s.labels.empty()) {
      out << '{';
      bool first = true;
      for (const auto& [key, value] : s.labels) {
        if (!first) out << ',';
        first = false;
        out << key << "=\"" << Escape(value, true) << '"';
      }
      out << '}';
    }
    out << ' ' << s.value << '\n';
  }
  return out.str();
}
}  // namespace gizmosql::enterprise
