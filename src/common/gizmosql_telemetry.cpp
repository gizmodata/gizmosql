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
#include "gizmosql_logging.h"
#include "gizmosql_library.h"

#include <atomic>
#include <mutex>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY

// OpenTelemetry SDK headers
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/metrics/meter_provider.h>
#include <opentelemetry/sdk/metrics/export/periodic_exporting_metric_reader.h>
#include <opentelemetry/sdk/resource/semantic_conventions.h>
#include <opentelemetry/sdk/resource/resource.h>

// OTLP HTTP Exporters
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter.h>

// Global provider access
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/metrics/provider.h>

namespace trace_api = opentelemetry::trace;
namespace trace_sdk = opentelemetry::sdk::trace;
namespace metrics_api = opentelemetry::metrics;
namespace metrics_sdk = opentelemetry::sdk::metrics;
namespace resource = opentelemetry::sdk::resource;
namespace otlp = opentelemetry::exporter::otlp;

#endif  // GIZMOSQL_WITH_OPENTELEMETRY

namespace gizmosql {

// -----------------------------------------------------------------------------
// Global State
// -----------------------------------------------------------------------------

static std::atomic<bool> g_telemetry_initialized{false};
static std::atomic<bool> g_telemetry_enabled{false};
static std::mutex g_init_mutex;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY

// Metric instruments (created lazily, protected by g_metrics_mutex)
static std::mutex g_metrics_mutex;
static bool g_metrics_initialized{false};
static opentelemetry::nostd::unique_ptr<metrics_api::Histogram<double>> g_rpc_duration_histogram;
static opentelemetry::nostd::unique_ptr<metrics_api::Histogram<double>> g_query_duration_histogram;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> g_rpc_count_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::Counter<uint64_t>> g_bytes_counter;
static opentelemetry::nostd::unique_ptr<metrics_api::UpDownCounter<int64_t>> g_active_connections;

// -----------------------------------------------------------------------------
// Helper: Parse headers string into map
// Format: "key1=value1,key2=value2"
// -----------------------------------------------------------------------------
static otlp::OtlpHeaders ParseHeaders(const std::string& headers_str) {
  otlp::OtlpHeaders headers;
  if (headers_str.empty()) return headers;

  std::string remaining = headers_str;
  while (!remaining.empty()) {
    size_t comma = remaining.find(',');
    std::string pair = (comma == std::string::npos) ? remaining : remaining.substr(0, comma);

    size_t eq = pair.find('=');
    if (eq != std::string::npos) {
      std::string key = pair.substr(0, eq);
      std::string value = pair.substr(eq + 1);
      // Trim whitespace
      while (!key.empty() && (key.front() == ' ' || key.front() == '\t')) key.erase(0, 1);
      while (!key.empty() && (key.back() == ' ' || key.back() == '\t')) key.pop_back();
      while (!value.empty() && (value.front() == ' ' || value.front() == '\t')) value.erase(0, 1);
      while (!value.empty() && (value.back() == ' ' || value.back() == '\t')) value.pop_back();
      if (!key.empty()) {
        headers.insert({key, value});
      }
    }

    if (comma == std::string::npos) break;
    remaining = remaining.substr(comma + 1);
  }
  return headers;
}

// -----------------------------------------------------------------------------
// Helper: Create resource with service info
// -----------------------------------------------------------------------------
static resource::Resource CreateResource(const TelemetryConfig& config) {
  resource::ResourceAttributes attrs;
  attrs[resource::SemanticConventions::kServiceName] = config.service_name;

  if (!config.service_version.empty()) {
    attrs[resource::SemanticConventions::kServiceVersion] = config.service_version;
  }
  if (!config.deployment_environment.empty()) {
    attrs[resource::SemanticConventions::kDeploymentEnvironment] = config.deployment_environment;
  }

  return resource::Resource::Create(attrs);
}

#endif  // GIZMOSQL_WITH_OPENTELEMETRY

// -----------------------------------------------------------------------------
// Initialization
// -----------------------------------------------------------------------------

void InitTelemetry(const TelemetryConfig& config) {
  std::lock_guard<std::mutex> lock(g_init_mutex);

  if (g_telemetry_initialized.load()) {
    return;  // Already initialized
  }

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (!config.enabled || config.exporter_type == OtlpExporterType::kNone) {
    GIZMOSQL_LOG(INFO) << "OpenTelemetry disabled";
    g_telemetry_initialized.store(true);
    g_telemetry_enabled.store(false);
    return;
  }

  auto resource_attrs = CreateResource(config);
  auto headers = ParseHeaders(config.headers);

  // Determine endpoint
  std::string endpoint = config.endpoint.empty() ? GetDefaultEndpoint(config.exporter_type)
                                                  : config.endpoint;

  GIZMOSQL_LOG(INFO) << "Initializing OpenTelemetry with HTTP exporter, endpoint: " << endpoint;

  // ----- Initialize Tracing -----
  if (config.traces_enabled) {
    otlp::OtlpHttpExporterOptions opts;
    opts.url = endpoint + "/v1/traces";
    opts.http_headers = headers;
    opts.timeout = config.export_timeout;
    auto trace_exporter = std::make_unique<otlp::OtlpHttpExporter>(opts);

    // Use batch processor for better performance
    trace_sdk::BatchSpanProcessorOptions processor_opts;
    processor_opts.schedule_delay_millis = config.export_interval;
    auto processor = std::make_unique<trace_sdk::BatchSpanProcessor>(
        std::move(trace_exporter), processor_opts);

    auto provider = std::make_shared<trace_sdk::TracerProvider>(
        std::move(processor), resource_attrs);

    trace_api::Provider::SetTracerProvider(provider);
    GIZMOSQL_LOG(INFO) << "OpenTelemetry tracing enabled";
  }

  // ----- Initialize Metrics -----
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

    metrics_api::Provider::SetMeterProvider(provider);
    GIZMOSQL_LOG(INFO) << "OpenTelemetry metrics enabled";
  }

  g_telemetry_enabled.store(true);
  g_telemetry_initialized.store(true);

  GIZMOSQL_LOG(INFO) << "OpenTelemetry initialization complete";
#else
  // OpenTelemetry not compiled in - just mark as initialized but disabled
  (void)config;  // Suppress unused parameter warning
  GIZMOSQL_LOG(INFO) << "OpenTelemetry support not compiled in (build with -DWITH_OPENTELEMETRY=ON)";
  g_telemetry_initialized.store(true);
  g_telemetry_enabled.store(false);
#endif
}

void ShutdownTelemetry() {
  std::lock_guard<std::mutex> lock(g_init_mutex);

  if (!g_telemetry_initialized.load()) {
    return;
  }

  // Disable telemetry first to prevent new operations from starting
  g_telemetry_enabled.store(false);

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  GIZMOSQL_LOG(INFO) << "Shutting down OpenTelemetry...";

  // Shutdown tracer provider (flushes pending spans)
  if (auto provider = trace_api::Provider::GetTracerProvider()) {
    if (auto* sdk_provider = dynamic_cast<trace_sdk::TracerProvider*>(provider.get())) {
      sdk_provider->Shutdown();
    }
  }

  // Shutdown meter provider (flushes pending metrics)
  if (auto provider = metrics_api::Provider::GetMeterProvider()) {
    if (auto* sdk_provider = dynamic_cast<metrics_sdk::MeterProvider*>(provider.get())) {
      sdk_provider->Shutdown();
    }
  }

  // Reset metric instruments so they can be recreated on re-initialization
  {
    std::lock_guard<std::mutex> metrics_lock(g_metrics_mutex);
    g_rpc_duration_histogram.reset();
    g_query_duration_histogram.reset();
    g_rpc_count_counter.reset();
    g_bytes_counter.reset();
    g_active_connections.reset();
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

// -----------------------------------------------------------------------------
// Tracer / Meter Access
// -----------------------------------------------------------------------------

opentelemetry::nostd::shared_ptr<trace_api::Tracer> GetTracer() {
  return trace_api::Provider::GetTracerProvider()->GetTracer("gizmosql", GIZMOSQL_SERVER_VERSION);
}

opentelemetry::nostd::shared_ptr<metrics_api::Meter> GetMeter() {
  return metrics_api::Provider::GetMeterProvider()->GetMeter("gizmosql", GIZMOSQL_SERVER_VERSION);
}

// -----------------------------------------------------------------------------
// ScopedSpan Implementation
// -----------------------------------------------------------------------------

ScopedSpan::ScopedSpan(const std::string& name)
    : span_(GetTracer()->StartSpan(name)), scope_(span_) {}

ScopedSpan::ScopedSpan(const std::string& name,
                       const trace_api::SpanContext& parent_context)
    : span_(GetTracer()->StartSpan(
          name,
          {},
          trace_api::SpanContext(parent_context))),
      scope_(span_) {}

ScopedSpan::~ScopedSpan() {
  span_->End();
}

void ScopedSpan::SetAttribute(const std::string& key, const std::string& value) {
  span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, int64_t value) {
  span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, double value) {
  span_->SetAttribute(key, value);
}

void ScopedSpan::SetAttribute(const std::string& key, bool value) {
  span_->SetAttribute(key, value);
}

void ScopedSpan::RecordError(const std::string& error_message) {
  span_->AddEvent("exception", {{"exception.message", error_message}});
  span_->SetStatus(trace_api::StatusCode::kError, error_message);
}

void ScopedSpan::SetStatus(bool success, const std::string& description) {
  span_->SetStatus(success ? trace_api::StatusCode::kOk : trace_api::StatusCode::kError,
                   description);
}

trace_api::Span& ScopedSpan::GetSpan() {
  return *span_;
}

trace_api::SpanContext ScopedSpan::GetContext() const {
  return span_->GetContext();
}

// -----------------------------------------------------------------------------
// Metrics Implementation
// -----------------------------------------------------------------------------

static void InitMetricsInstruments() {
  std::lock_guard<std::mutex> lock(g_metrics_mutex);
  if (g_metrics_initialized) return;

  auto meter = GetMeter();

  // RPC duration histogram (in milliseconds)
  g_rpc_duration_histogram = meter->CreateDoubleHistogram(
      "gizmosql.rpc.duration",
      "Duration of RPC calls in milliseconds",
      "ms");

  // Query duration histogram (in milliseconds)
  g_query_duration_histogram = meter->CreateDoubleHistogram(
      "gizmosql.query.duration",
      "Duration of query executions in milliseconds",
      "ms");

  // RPC count counter
  g_rpc_count_counter = meter->CreateUInt64Counter(
      "gizmosql.rpc.count",
      "Number of RPC calls",
      "1");

  // Bytes transferred counter
  g_bytes_counter = meter->CreateUInt64Counter(
      "gizmosql.bytes.transferred",
      "Number of bytes transferred",
      "By");

  // Active connections gauge
  g_active_connections = meter->CreateInt64UpDownCounter(
      "gizmosql.connections.active",
      "Number of active connections",
      "1");

  g_metrics_initialized = true;
}

namespace metrics {

void RecordRpcCall(const std::string& method,
                   const std::string& status,
                   double duration_ms) {
  if (!IsTelemetryEnabled()) return;

  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {
      {"rpc.method", method},
      {"rpc.status", status}
  };

  g_rpc_duration_histogram->Record(duration_ms, labels, opentelemetry::context::Context{});
  g_rpc_count_counter->Add(1, labels, opentelemetry::context::Context{});
}

void RecordQueryExecution(const std::string& operation,
                          const std::string& status,
                          double duration_ms) {
  if (!IsTelemetryEnabled()) return;

  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {
      {"db.operation", operation},
      {"db.status", status}
  };

  g_query_duration_histogram->Record(duration_ms, labels, opentelemetry::context::Context{});
}

void RecordActiveConnections(int64_t count) {
  if (!IsTelemetryEnabled()) return;

  InitMetricsInstruments();

  g_active_connections->Add(count, {}, opentelemetry::context::Context{});
}

void RecordBytesTransferred(const std::string& direction, int64_t bytes) {
  if (!IsTelemetryEnabled()) return;
  if (bytes < 0) return;  // Ignore negative values

  InitMetricsInstruments();

  std::map<std::string, std::string> labels = {
      {"direction", direction}
  };

  g_bytes_counter->Add(static_cast<uint64_t>(bytes), labels, opentelemetry::context::Context{});
}

}  // namespace metrics

#else  // !GIZMOSQL_WITH_OPENTELEMETRY

// Stub implementations when OpenTelemetry is not available
namespace metrics {

void RecordRpcCall(const std::string& /*method*/,
                   const std::string& /*status*/,
                   double /*duration_ms*/) {
  // No-op when OpenTelemetry is not compiled in
}

void RecordQueryExecution(const std::string& /*operation*/,
                          const std::string& /*status*/,
                          double /*duration_ms*/) {
  // No-op when OpenTelemetry is not compiled in
}

void RecordActiveConnections(int64_t /*count*/) {
  // No-op when OpenTelemetry is not compiled in
}

void RecordBytesTransferred(const std::string& /*direction*/, int64_t /*bytes*/) {
  // No-op when OpenTelemetry is not compiled in
}

}  // namespace metrics

#endif  // GIZMOSQL_WITH_OPENTELEMETRY

// -----------------------------------------------------------------------------
// Configuration Helpers (always available)
// -----------------------------------------------------------------------------

OtlpExporterType ParseExporterType(const std::string& type_str) {
  std::string lower = type_str;
  for (auto& c : lower) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));

  if (lower == "http") return OtlpExporterType::kHttp;
  if (lower == "none" || lower == "off" || lower == "disabled" || lower.empty()) {
    return OtlpExporterType::kNone;
  }

  GIZMOSQL_LOG(WARNING) << "Unknown OTLP exporter type '" << type_str
                        << "', defaulting to HTTP";
  return OtlpExporterType::kHttp;
}

std::string GetDefaultEndpoint(OtlpExporterType type) {
  switch (type) {
    case OtlpExporterType::kHttp:
      return "http://localhost:4318";
    default:
      return "";
  }
}

}  // namespace gizmosql
