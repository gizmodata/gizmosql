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

#include <memory>
#include <string>
#include <chrono>
#include <optional>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/metrics/meter.h>
#endif

namespace gizmosql {

// -----------------------------------------------------------------------------
// Telemetry Configuration
// -----------------------------------------------------------------------------

enum class OtlpExporterType {
  kNone,  // Telemetry disabled
  kHttp   // OTLP/HTTP exporter
};

struct TelemetryConfig {
  bool enabled = false;
  OtlpExporterType exporter_type = OtlpExporterType::kHttp;

  // OTLP endpoint (default: localhost:4318 for HTTP)
  std::string endpoint;

  // Service identification
  std::string service_name = "gizmosql";
  std::string service_version;
  std::string deployment_environment;

  // Export intervals
  std::chrono::milliseconds export_interval{5000};
  std::chrono::milliseconds export_timeout{30000};

  // Optional headers for authentication (e.g., DD-API-KEY)
  std::string headers;

  // Enable/disable specific signals
  bool traces_enabled = true;
  bool metrics_enabled = true;
};

// -----------------------------------------------------------------------------
// Telemetry Initialization / Shutdown
// -----------------------------------------------------------------------------

// Initialize OpenTelemetry with the given configuration.
// Must be called before any tracing/metrics operations.
// Safe to call multiple times; subsequent calls are no-ops.
void InitTelemetry(const TelemetryConfig& config);

// Gracefully shutdown OpenTelemetry, flushing any pending data.
// Should be called before application exit.
void ShutdownTelemetry();

// Check if telemetry is currently enabled and initialized.
bool IsTelemetryEnabled() noexcept;

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
// -----------------------------------------------------------------------------
// Tracer Access (only available with OpenTelemetry)
// -----------------------------------------------------------------------------

// Get the global tracer for creating spans.
// Returns a no-op tracer if telemetry is disabled.
opentelemetry::nostd::shared_ptr<opentelemetry::trace::Tracer> GetTracer();

// -----------------------------------------------------------------------------
// Meter Access (only available with OpenTelemetry)
// -----------------------------------------------------------------------------

// Get the global meter for creating metrics instruments.
// Returns a no-op meter if telemetry is disabled.
opentelemetry::nostd::shared_ptr<opentelemetry::metrics::Meter> GetMeter();

// -----------------------------------------------------------------------------
// Span Helpers (only available with OpenTelemetry)
// -----------------------------------------------------------------------------

// RAII wrapper for creating and managing spans
class ScopedSpan {
 public:
  // Create a new span with the given name
  explicit ScopedSpan(const std::string& name);

  // Create a child span under an existing span
  ScopedSpan(const std::string& name,
             const opentelemetry::trace::SpanContext& parent_context);

  ~ScopedSpan();

  // Non-copyable and non-movable (span lifecycle is tied to this object)
  ScopedSpan(const ScopedSpan&) = delete;
  ScopedSpan& operator=(const ScopedSpan&) = delete;
  ScopedSpan(ScopedSpan&&) = delete;
  ScopedSpan& operator=(ScopedSpan&&) = delete;

  // Set span attributes
  void SetAttribute(const std::string& key, const std::string& value);
  void SetAttribute(const std::string& key, int64_t value);
  void SetAttribute(const std::string& key, double value);
  void SetAttribute(const std::string& key, bool value);

  // Record an error on the span
  void RecordError(const std::string& error_message);

  // Set span status
  void SetStatus(bool success, const std::string& description = "");

  // Get the underlying span for advanced operations
  opentelemetry::trace::Span& GetSpan();

  // Get span context for propagation
  opentelemetry::trace::SpanContext GetContext() const;

 private:
  opentelemetry::nostd::shared_ptr<opentelemetry::trace::Span> span_;
  opentelemetry::trace::Scope scope_;
};

#endif  // GIZMOSQL_WITH_OPENTELEMETRY

// -----------------------------------------------------------------------------
// Metric Instruments (initialized lazily on first use)
// These are available even without OpenTelemetry (as no-ops)
// -----------------------------------------------------------------------------

// Pre-defined metrics for common operations
namespace metrics {

// Record an RPC call with duration
void RecordRpcCall(const std::string& method,
                   const std::string& status,
                   double duration_ms);

// Record a database query with duration
void RecordQueryExecution(const std::string& operation,
                          const std::string& status,
                          double duration_ms);

// Record active connections
void RecordActiveConnections(int64_t count);

// Record bytes transferred
void RecordBytesTransferred(const std::string& direction, int64_t bytes);

}  // namespace metrics

// -----------------------------------------------------------------------------
// Configuration Parsing Helpers
// -----------------------------------------------------------------------------

// Parse exporter type from string (http, none)
OtlpExporterType ParseExporterType(const std::string& type_str);

// Get default endpoint for the given exporter type
std::string GetDefaultEndpoint(OtlpExporterType type);

}  // namespace gizmosql
