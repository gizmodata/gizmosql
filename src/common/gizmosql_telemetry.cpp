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

#include "gizmosql_telemetry.h"

#include "gizmosql_library.h"
#include "gizmosql_logging.h"

#include <atomic>
#include <cctype>
#include <map>
#include <mutex>
#include <utility>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/context.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter.h>
#include <opentelemetry/metrics/provider.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/view/view_registry.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/propagation/http_trace_context.h>

namespace context_propagation_api = opentelemetry::context::propagation;
namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace otlp = opentelemetry::exporter::otlp;
namespace resource = opentelemetry::sdk::resource;
namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
#endif

namespace gizmosql {

static std::atomic<bool> g_telemetry_initialized{false};
static std::atomic<bool> g_telemetry_enabled{false};
static std::mutex g_init_mutex;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
static std::mutex g_metrics_mutex;
static bool g_metrics_initialized{false};
static opentelemetry::nostd::unique_ptr<metrics_api::Histogram<double>>
    g_rpc_duration_histogram;
static opentelemetry::nostd::unique_ptr<metrics_api::Histogram<double>>
    g_query_duration_histogram;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>>
    g_rpc_count_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>>
    g_query_count_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> g_bytes_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> g_rows_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::UpDownCounter<int64_t>>
    g_active_connections;
static opentelemetry::nostd::unique_ptr<metrics_api::UpDownCounter<int64_t>>
    g_open_duckdb_connections;

static otlp::OtlpHeaders ParseHeaders(const std::string& headers_str) {
  otlp::OtlpHeaders headers;
  if (headers_str.empty()) return headers;

  std::string remaining = headers_str;
  while (!remaining.empty()) {
    const size_t comma = remaining.find(',');
    const std::string pair =
        (comma == std::string::npos) ? remaining : remaining.substr(0, comma);

    const size_t eq = pair.find('=');
    if (eq != std::string::npos) {
      std::string key = pair.substr(0, eq);
      std::string value = pair.substr(eq + 1);
      while (!key.empty() && (key.front() == ' ' || key.front() == '\t')) key.erase(0, 1);
      while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) key.pop_back();
      while (!value.empty() && (value.front() == ' ' || value.front() == '\t'))
        value.erase(0, 1);
      while (!value.empty() && (value.back() == ' ' || value.back() == '\t'))
        value.pop_back();
      if (!key.empty()) {
        headers.insert({key, value});
      }
    }

    if (comma == std::string::npos) break;
    remaining = remaining.substr(comma + 1);
  }
  return headers;
}

static resource::Resource CreateResource(const TelemetryConfig& config) {
  resource::ResourceAttributes attrs;
  attrs["service.name"] = config.service_name;
  if (!config.service_version.empty()) {
    attrs["service.version"] = config.service_version;
  }
  if (!config.deployment_environment.empty()) {
    attrs["deployment.environment"] = config.deployment_environment;
  }
  return resource::Resource::Create(attrs);
}
#endif

void InitTelemetry(const TelemetryConfig& config) {
  std::lock_guard<std::mutex> lock(g_init_mutex);

  if (g_telemetry_initialized.load()) {
    return;
  }

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (!config.enabled || config.exporter_type == OtlpExporterType::kNone) {
    GIZMOSQL_LOG(INFO) << "OpenTelemetry disabled";
    g_telemetry_initialized.store(true);
    g_telemetry_enabled.store(false);
    return;
  }

  const auto resource_attrs = CreateResource(config);
  const auto headers = ParseHeaders(config.headers);
  const std::string endpoint = config.endpoint.empty()
                                   ? GetDefaultEndpoint(config.exporter_type)
                                   : config.endpoint;

  bool initialized_signal = false;
  GIZMOSQL_LOG(INFO) << "Initializing OpenTelemetry with HTTP exporter, endpoint: "
                     << endpoint;

  if (config.traces_enabled) {
    otlp::OtlpHttpExporterOptions opts;
    opts.url = endpoint + "/v1/traces";
    opts.http_headers = headers;
    opts.timeout = config.export_timeout;

    auto trace_exporter = std::make_unique<otlp::OtlpHttpExporter>(opts);

    trace_sdk::BatchSpanProcessorOptions processor_opts;
    processor_opts.schedule_delay_millis = config.export_interval;
    auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(
        std::move(trace_exporter), processor_opts);
    auto provider =
        std::make_shared<trace_sdk::TracerProvider>(std::move(processor), resource_attrs);
    trace_api::Provider::SetTracerProvider(
        opentelemetry::nostd::shared_ptr<trace_api::TracerProvider>(provider));
    auto propagator =
        opentelemetry::nostd::shared_ptr<context_propagation_api::TextMapPropagator>(
            new opentelemetry::trace::propagation::HttpTraceContext());
    context_propagation_api::GlobalTextMapPropagator::SetGlobalPropagator(propagator);
    initialized_signal = true;
    GIZMOSQL_LOG(INFO) << "OpenTelemetry tracing enabled";
    GIZMOSQL_LOG(INFO) << "OpenTelemetry trace context propagation enabled (W3C tracecontext)";
  }

  if (config.metrics_enabled) {
    otlp::OtlpHttpMetricExporterOptions opts;
    opts.url = endpoint + "/v1/metrics";
    opts.http_headers = headers;
    opts.timeout = config.export_timeout;

    auto metric_exporter = std::make_unique<otlp::OtlpHttpMetricExporter>(opts);

    metrics_sdk::PeriodicExportingMetricReaderOptions reader_opts;
    reader_opts.export_interval_millis = config.export_interval;
    reader_opts.export_timeout_millis = config.export_timeout;

    auto reader = std::make_unique<metrics_sdk::PeriodicExportingMetricReader>(
        std::move(metric_exporter), reader_opts);
    auto provider = std::make_shared<metrics_sdk::MeterProvider>(
        std::make_unique<metrics_sdk::ViewRegistry>(), resource_attrs);
    provider->AddMetricReader(std::move(reader));
    metrics_api::Provider::SetMeterProvider(
        opentelemetry::nostd::shared_ptr<metrics_api::MeterProvider>(provider));
    initialized_signal = true;
    GIZMOSQL_LOG(INFO) << "OpenTelemetry metrics enabled";
  }

  g_telemetry_initialized.store(true);
  g_telemetry_enabled.store(initialized_signal);
  if (initialized_signal) {
    GIZMOSQL_LOG(INFO) << "OpenTelemetry initialization complete";
  } else {
    GIZMOSQL_LOG(WARNING) << "OpenTelemetry was enabled but no signal was configured "
                             "(traces/metrics both off)";
  }
#else
  (void)config;
  GIZMOSQL_LOG(INFO)
      << "OpenTelemetry support not compiled in (build with -DWITH_OPENTELEMETRY=ON)";
  g_telemetry_initialized.store(true);
  g_telemetry_enabled.store(false);
#endif
}

void ShutdownTelemetry() {
  std::lock_guard<std::mutex> lock(g_init_mutex);

  if (!g_telemetry_initialized.load()) {
    return;
  }

  g_telemetry_enabled.store(false);

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (auto provider = trace_api::Provider::GetTracerProvider()) {
    if (auto* sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(provider.get())) {
      sdk_provider->Shutdown();
    }
  }

  if (auto provider = metrics_api::Provider::GetMeterProvider()) {
    if (auto* sdk_provider = dynamic_cast<metrics_sdk::MeterProvider*>(provider.get())) {
      sdk_provider->Shutdown();
    }
  }

  {
    std::lock_guard<std::mutex> metrics_lock(g_metrics_mutex);
    g_rpc_duration_histogram.reset();
    g_query_duration_histogram.reset();
    g_rpc_count_counter.reset();
    g_query_count_counter.reset();
    g_bytes_counter.reset();
    g_rows_counter.reset();
    g_active_connections.reset();
    g_open_duckdb_connections.reset();
    g_metrics_initialized = false;
  }

  GIZMOSQL_LOG(INFO) << "OpenTelemetry shutdown complete";
#endif

  g_telemetry_initialized.store(false);
}

bool IsTelemetryEnabled() noexcept {
  return g_telemetry_enabled.load(std::memory_order_acquire);
}

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
opentelemetry::nostd::shared_ptr<trace_api::Tracer> GetTracer() {
  return trace_api::Provider::GetTracerProvider()->GetTracer("gizmosql",
                                                             GIZMOSQL_SERVER_VERSION);
}

opentelemetry::nostd::shared_ptr<metrics_api::Meter> GetMeter() {
  return metrics_api::Provider::GetMeterProvider()->GetMeter("gizmosql",
                                                             GIZMOSQL_SERVER_VERSION);
}

ScopedSpan::ScopedSpan(const std::string& name)
    : span_(GetTracer()->StartSpan(name)), scope_(span_) {}

ScopedSpan::~ScopedSpan() {
  if (span_) {
    span_->End();
  }
}

void ScopedSpan::SetAttribute(const std::string& key, const std::string& value) {
  if (span_) span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, int64_t value) {
  if (span_) span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, double value) {
  if (span_) span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, bool value) {
  if (span_) span_->SetAttribute(key, value);
}

void ScopedSpan::RecordError(const std::string& error_message) {
  if (!span_) return;
  span_->AddEvent("exception", {{"exception.message", error_message}});
  span_->SetStatus(trace_api::StatusCode::kError, error_message);
}

void ScopedSpan::SetStatus(bool success, const std::string& description) {
  if (!span_) return;
  span_->SetStatus(success ? trace_api::StatusCode::kOk : trace_api::StatusCode::kError,
                   description);
}

trace_api::Span& ScopedSpan::GetSpan() { return *span_; }

trace_api::SpanContext ScopedSpan::GetContext() const {
  return span_ ? span_->GetContext() : trace_api::SpanContext::GetInvalid();
}

static void InitMetricsInstruments() {
  std::lock_guard<std::mutex> lock(g_metrics_mutex);
  if (g_metrics_initialized) return;

  auto meter = GetMeter();
  g_rpc_duration_histogram = meter->CreateDoubleHistogram(
      "gizmosql.rpc.duration", "Duration of RPC calls in milliseconds", "ms");
  g_query_duration_histogram = meter->CreateDoubleHistogram(
      "gizmosql.query.duration", "Duration of query executions in milliseconds", "ms");
  g_rpc_count_counter =
      meter->CreateUInt64Counter("gizmosql.rpc.count", "Number of RPC calls", "1");
  g_query_count_counter = meter->CreateUInt64Counter("gizmosql.query.count",
                                                     "Number of query executions", "1");
  g_bytes_counter = meter->CreateUInt64Counter("gizmosql.bytes.transferred",
                                               "Number of bytes transferred", "By");
  g_rows_counter = meter->CreateUInt64Counter("gizmosql.rows.transferred",
                                              "Number of rows transferred", "1");
  g_active_connections = meter->CreateInt64UpDownCounter(
      "gizmosql.connections.active", "Number of active connections", "1");
  g_open_duckdb_connections = meter->CreateInt64UpDownCounter(
      "gizmosql.duckdb.connections.open", "Number of open DuckDB connections", "1");
  g_metrics_initialized = true;
}

namespace metrics {

void RecordRpcCall(const std::string& method, const std::string& status,
                   double duration_ms) {
  if (!IsTelemetryEnabled()) return;
  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {{"rpc.method", method},
                                               {"rpc.status", status}};

  g_rpc_duration_histogram->Record(duration_ms, labels,
                                   opentelemetry::context::Context{});
  g_rpc_count_counter->Add(1, labels, opentelemetry::context::Context{});
}

void RecordQueryExecution(const std::string& operation, const std::string& status,
                          double duration_ms) {
  if (!IsTelemetryEnabled()) return;
  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {{"db.operation", operation},
                                               {"db.status", status}};

  g_query_duration_histogram->Record(duration_ms, labels,
                                     opentelemetry::context::Context{});
  g_query_count_counter->Add(1, labels, opentelemetry::context::Context{});
}

void RecordActiveConnections(int64_t count) {
  if (!IsTelemetryEnabled()) return;
  InitMetricsInstruments();
  g_active_connections->Add(count, {}, opentelemetry::context::Context{});
}

void RecordOpenDuckDBConnections(int64_t count) {
  if (!IsTelemetryEnabled()) return;
  InitMetricsInstruments();
  g_open_duckdb_connections->Add(count, {}, opentelemetry::context::Context{});
}

void RecordBytesTransferred(const std::string& direction, int64_t bytes) {
  if (!IsTelemetryEnabled() || bytes < 0) return;
  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {{"direction", direction}};
  g_bytes_counter->Add(static_cast<uint64_t>(bytes), labels,
                       opentelemetry::context::Context{});
}

void RecordRowsTransferred(const std::string& direction, int64_t rows) {
  if (!IsTelemetryEnabled() || rows < 0) return;
  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {{"direction", direction}};
  g_rows_counter->Add(static_cast<uint64_t>(rows), labels,
                      opentelemetry::context::Context{});
}

}  // namespace metrics

#else

namespace metrics {

void RecordRpcCall(const std::string&, const std::string&, double) {}
void RecordQueryExecution(const std::string&, const std::string&, double) {}
void RecordActiveConnections(int64_t) {}
void RecordOpenDuckDBConnections(int64_t) {}
void RecordBytesTransferred(const std::string&, int64_t) {}
void RecordRowsTransferred(const std::string&, int64_t) {}

}  // namespace metrics

#endif

OtlpExporterType ParseExporterType(const std::string& type_str) {
  std::string lowered = type_str;
  for (auto& c : lowered) {
    c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  }

  if (lowered.empty() || lowered == "none" || lowered == "off" || lowered == "disabled")
    return OtlpExporterType::kNone;
  if (lowered == "http") return OtlpExporterType::kHttp;

  GIZMOSQL_LOG(WARNING) << "Unknown OTLP exporter type '" << type_str
                        << "', defaulting to HTTP";
  return OtlpExporterType::kHttp;
}

std::string GetDefaultEndpoint(OtlpExporterType type) {
  switch (type) {
    case OtlpExporterType::kHttp:
      return "http://localhost:4318";
    case OtlpExporterType::kNone:
    default:
      return "";
  }
}

}  // namespace gizmosql
