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

#include "telemetry_middleware.h"
#include "gizmosql_telemetry.h"
#include "gizmosql_logging.h"

#include <arrow/flight/server.h>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/semantic_conventions.h>

namespace trace_api = opentelemetry::trace;
namespace trace_semconv = opentelemetry::trace::SemanticConventions;
#endif

namespace gizmosql {

// -----------------------------------------------------------------------------
// Helper: Convert FlightMethod to string
// -----------------------------------------------------------------------------
static const char* FlightMethodName(flight::FlightMethod m) {
  switch (m) {
    case flight::FlightMethod::Handshake:
      return "Handshake";
    case flight::FlightMethod::ListFlights:
      return "ListFlights";
    case flight::FlightMethod::GetFlightInfo:
      return "GetFlightInfo";
    case flight::FlightMethod::GetSchema:
      return "GetSchema";
    case flight::FlightMethod::DoGet:
      return "DoGet";
    case flight::FlightMethod::DoPut:
      return "DoPut";
    case flight::FlightMethod::DoAction:
      return "DoAction";
    case flight::FlightMethod::ListActions:
      return "ListActions";
    case flight::FlightMethod::DoExchange:
      return "DoExchange";
    case flight::FlightMethod::PollFlightInfo:
      return "PollFlightInfo";
    default:
      return "Unknown";
  }
}

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
// -----------------------------------------------------------------------------
// SpanHolder: Encapsulates span lifecycle
// -----------------------------------------------------------------------------
struct TelemetryMiddleware::SpanHolder {
  opentelemetry::nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope> scope;

  SpanHolder(opentelemetry::nostd::shared_ptr<trace_api::Span> s)
      : span(std::move(s)), scope(std::make_unique<trace_api::Scope>(span)) {}

  ~SpanHolder() {
    if (span) {
      span->End();
    }
  }
};
#else
// Dummy SpanHolder when OpenTelemetry is not available
struct TelemetryMiddleware::SpanHolder {
  // Empty placeholder
};
#endif

// -----------------------------------------------------------------------------
// TelemetryMiddleware Implementation
// -----------------------------------------------------------------------------

TelemetryMiddleware::TelemetryMiddleware(flight::FlightMethod method, std::string peer)
    : method_(method),
      peer_(std::move(peer)),
      start_time_(std::chrono::steady_clock::now()) {

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (!IsTelemetryEnabled()) {
    return;
  }

  // Create span for this RPC
  auto tracer = GetTracer();
  std::string span_name = std::string("gizmosql.") + FlightMethodName(method_);

  trace_api::StartSpanOptions opts;
  opts.kind = trace_api::SpanKind::kServer;

  auto span = tracer->StartSpan(span_name, {}, opts);

  // Set standard RPC attributes (following OpenTelemetry semantic conventions)
  span->SetAttribute(trace_semconv::kRpcSystem, "grpc");
  span->SetAttribute(trace_semconv::kRpcService, "arrow.flight.protocol.FlightService");
  span->SetAttribute(trace_semconv::kRpcMethod, FlightMethodName(method_));

  // Parse peer address (format: "ipv4:address:port" or "ipv6:[address]:port")
  if (!peer_.empty()) {
    span->SetAttribute(trace_semconv::kNetPeerName, peer_);
  }

  span_holder_ = std::make_unique<SpanHolder>(std::move(span));
#endif
}

TelemetryMiddleware::~TelemetryMiddleware() = default;

void TelemetryMiddleware::SendingHeaders(flight::AddCallHeaders* /*outgoing_headers*/) {
  // Could add trace context to response headers here for bi-directional propagation
  // For now, we don't need to propagate context back to clients
}

void TelemetryMiddleware::CallCompleted(const arrow::Status& status) {
  auto end_time = std::chrono::steady_clock::now();
  auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         end_time - start_time_)
                         .count();

  // Determine status string for metrics
  std::string status_str = status.ok() ? "OK" : status.CodeAsString();

  // Record metrics (works even without OpenTelemetry compiled in, as no-op)
  metrics::RecordRpcCall(FlightMethodName(method_), status_str, static_cast<double>(duration_ms));

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  // Update span if telemetry is enabled
  if (span_holder_ && span_holder_->span) {
    auto& span = span_holder_->span;

    // Record duration as attribute
    span->SetAttribute("duration_ms", static_cast<int64_t>(duration_ms));

    // Set span status based on Arrow status
    if (status.ok()) {
      span->SetStatus(trace_api::StatusCode::kOk);
    } else {
      span->SetStatus(trace_api::StatusCode::kError, status.ToString());
      span->AddEvent("error", {
          {"exception.type", status.CodeAsString()},
          {"exception.message", status.message()}
      });
    }

    // Record gRPC status code (approximation from Arrow status)
    // See https://grpc.github.io/grpc/core/md_doc_statuscodes.html
    int grpc_code = 0;  // OK
    if (!status.ok()) {
      // Map Arrow status to approximate gRPC codes
      switch (status.code()) {
        case arrow::StatusCode::Invalid:
        case arrow::StatusCode::TypeError:
        case arrow::StatusCode::SerializationError:
          grpc_code = 3;  // INVALID_ARGUMENT
          break;
        case arrow::StatusCode::KeyError:
        case arrow::StatusCode::IndexError:
          grpc_code = 5;  // NOT_FOUND
          break;
        case arrow::StatusCode::AlreadyExists:
          grpc_code = 6;  // ALREADY_EXISTS
          break;
        case arrow::StatusCode::OutOfMemory:
        case arrow::StatusCode::CapacityError:
          grpc_code = 8;  // RESOURCE_EXHAUSTED
          break;
        case arrow::StatusCode::Cancelled:
          grpc_code = 1;  // CANCELLED
          break;
        case arrow::StatusCode::NotImplemented:
          grpc_code = 12;  // UNIMPLEMENTED
          break;
        case arrow::StatusCode::IOError:
          grpc_code = 14;  // UNAVAILABLE
          break;
        case arrow::StatusCode::UnknownError:
        default:
          grpc_code = 2;  // UNKNOWN
          break;
      }
    }
    span->SetAttribute(trace_semconv::kRpcGrpcStatusCode, grpc_code);
  }
#else
  (void)status;  // Suppress unused warning when not using OpenTelemetry
#endif

  // SpanHolder destructor will end the span
}

// -----------------------------------------------------------------------------
// TelemetryMiddlewareFactory Implementation
// -----------------------------------------------------------------------------

arrow::Status TelemetryMiddlewareFactory::StartCall(
    const flight::CallInfo& info,
    const flight::ServerCallContext& ctx,
    std::shared_ptr<flight::ServerMiddleware>* out) {

  *out = std::make_shared<TelemetryMiddleware>(info.method, ctx.peer());
  return arrow::Status::OK();
}

}  // namespace gizmosql
