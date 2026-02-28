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

#include <algorithm>
#include <arrow/flight/server.h>
#include <cctype>
#include <cstdlib>
#include <string_view>
#include <utility>

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/context/propagation/text_map_propagator.h>
#include <opentelemetry/context/runtime_context.h>
#include <opentelemetry/trace/context.h>
#include <opentelemetry/trace/scope.h>
#include <opentelemetry/trace/span.h>
#include <opentelemetry/trace/tracer.h>

namespace context_api = opentelemetry::context;
namespace context_propagation_api = opentelemetry::context::propagation;
namespace trace_api = opentelemetry::trace;
#endif

namespace gizmosql {

static bool EqualsIgnoreCase(std::string_view left, std::string_view right) {
  return left.size() == right.size() &&
         std::equal(left.begin(), left.end(), right.begin(), right.end(),
                    [](char l, char r) {
                      return std::tolower(static_cast<unsigned char>(l)) ==
                             std::tolower(static_cast<unsigned char>(r));
                    });
}

static bool HasHeaderIgnoreCase(const flight::CallHeaders& incoming_headers,
                                std::string_view key) {
  for (auto iter = incoming_headers.begin(); iter != incoming_headers.end(); ++iter) {
    if (EqualsIgnoreCase(iter->first, key)) {
      return true;
    }
  }
  return false;
}

static const char* FlightMethodName(flight::FlightMethod method) {
  switch (method) {
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
class FlightCallHeadersCarrier final : public context_propagation_api::TextMapCarrier {
 public:
  explicit FlightCallHeadersCarrier(const flight::CallHeaders& incoming_headers)
      : incoming_headers_(incoming_headers) {}

  opentelemetry::nostd::string_view Get(
      opentelemetry::nostd::string_view key) const noexcept override {
    const std::string key_str(key.data(), key.size());
    auto iter = incoming_headers_.find(key_str);
    if (iter != incoming_headers_.end()) {
      cached_value_ = std::string(iter->second);
      return cached_value_;
    }

    for (auto header_iter = incoming_headers_.begin(); header_iter != incoming_headers_.end();
         ++header_iter) {
      if (gizmosql::EqualsIgnoreCase(header_iter->first, key_str)) {
        cached_value_ = std::string(header_iter->second);
        return cached_value_;
      }
    }

    cached_value_.clear();
    return {};
  }

  void Set(opentelemetry::nostd::string_view /*key*/,
           opentelemetry::nostd::string_view /*value*/) noexcept override {}

 private:
  const flight::CallHeaders& incoming_headers_;
  mutable std::string cached_value_;
};

class TelemetrySpanScope {
 public:
  explicit TelemetrySpanScope(const opentelemetry::nostd::shared_ptr<trace_api::Span>& span)
      : scope_(span) {}

 private:
  trace_api::Scope scope_;
};

struct TelemetryMiddleware::SpanHolder {
  explicit SpanHolder(opentelemetry::nostd::shared_ptr<trace_api::Span> input_span)
      : span(std::move(input_span)), scope(std::make_unique<trace_api::Scope>(span)) {}

  ~SpanHolder() {
    if (span) {
      span->End();
    }
  }

  opentelemetry::nostd::shared_ptr<trace_api::Span> span;
  std::unique_ptr<trace_api::Scope> scope;
};
#else
struct TelemetryMiddleware::SpanHolder {};

class TelemetrySpanScope {
 public:
  TelemetrySpanScope() = default;
};
#endif

TelemetryMiddleware::TelemetryMiddleware(flight::FlightMethod method, std::string peer,
                                         const flight::CallHeaders& incoming_headers)
    : method_(method),
      peer_(std::move(peer)),
      start_time_(std::chrono::steady_clock::now()) {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (!IsTelemetryEnabled()) {
    return;
  }

  auto tracer = GetTracer();
  trace_api::StartSpanOptions span_options;
  span_options.kind = trace_api::SpanKind::kServer;

  FlightCallHeadersCarrier carrier(incoming_headers);
  auto current_context = context_api::RuntimeContext::GetCurrent();
  auto propagator = context_propagation_api::GlobalTextMapPropagator::GetGlobalPropagator();
  auto parent_context = propagator ? propagator->Extract(carrier, current_context)
                                   : current_context;
  auto parent_span_context = trace_api::GetSpan(parent_context)->GetContext();
  const bool tracecontext_parent_present = parent_span_context.IsValid();
  const bool has_parent_context = parent_span_context.IsValid();
  auto parent_context_token = context_api::RuntimeContext::Attach(parent_context);
  (void)parent_context_token;
  auto span = tracer->StartSpan(std::string("gizmosql.") + FlightMethodName(method_), {},
                                span_options);

  span->SetAttribute("rpc.system", "grpc");
  span->SetAttribute("rpc.service", "arrow.flight.protocol.FlightService");
  span->SetAttribute("rpc.method", FlightMethodName(method_));
  span->SetAttribute("gizmosql.trace.parent_present", has_parent_context);
  span->SetAttribute("gizmosql.trace.tracecontext_parent_present",
                     tracecontext_parent_present);
  span->SetAttribute("gizmosql.trace.parent_format",
                     tracecontext_parent_present ? "tracecontext" : "none");
  span->SetAttribute("gizmosql.trace.traceparent_present",
                     HasHeaderIgnoreCase(incoming_headers, "traceparent"));

  if (const char* service_version = std::getenv("GIZMOSQL_OTEL_SERVICE_VERSION");
      service_version != nullptr && service_version[0] != '\0') {
    span->SetAttribute("service.version", service_version);
  }

  if (!peer_.empty()) {
    span->SetAttribute("net.peer.name", peer_);
  }

  span_holder_ = std::make_unique<SpanHolder>(std::move(span));
#endif
}

TelemetryMiddleware::~TelemetryMiddleware() = default;

std::shared_ptr<TelemetrySpanScope> TelemetryMiddleware::ActivateSpanForCurrentThread() const {
#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (span_holder_ && span_holder_->span) {
    return std::make_shared<TelemetrySpanScope>(span_holder_->span);
  }
#endif
  return nullptr;
}

void TelemetryMiddleware::SendingHeaders(flight::AddCallHeaders* /*outgoing_headers*/) {}

void TelemetryMiddleware::CallCompleted(const arrow::Status& status) {
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start_time_)
                              .count();
  const std::string status_label = status.ok() ? "OK" : status.CodeAsString();
  metrics::RecordRpcCall(FlightMethodName(method_), status_label,
                         static_cast<double>(elapsed_ms));

#ifdef GIZMOSQL_WITH_OPENTELEMETRY
  if (span_holder_ && span_holder_->span) {
    span_holder_->span->SetAttribute("duration_ms", static_cast<int64_t>(elapsed_ms));
    if (status.ok()) {
      span_holder_->span->SetStatus(trace_api::StatusCode::kOk);
      span_holder_->span->SetAttribute("rpc.grpc.status_code", 0);
      return;
    }

    span_holder_->span->SetStatus(trace_api::StatusCode::kError, status.ToString());
    span_holder_->span->AddEvent("error", {{"exception.type", status.CodeAsString()},
                                           {"exception.message", status.message()}});

    int grpc_code = 2;
    switch (status.code()) {
      case arrow::StatusCode::Invalid:
      case arrow::StatusCode::TypeError:
      case arrow::StatusCode::SerializationError:
        grpc_code = 3;
        break;
      case arrow::StatusCode::KeyError:
      case arrow::StatusCode::IndexError:
        grpc_code = 5;
        break;
      case arrow::StatusCode::AlreadyExists:
        grpc_code = 6;
        break;
      case arrow::StatusCode::OutOfMemory:
      case arrow::StatusCode::CapacityError:
        grpc_code = 8;
        break;
      case arrow::StatusCode::Cancelled:
        grpc_code = 1;
        break;
      case arrow::StatusCode::NotImplemented:
        grpc_code = 12;
        break;
      case arrow::StatusCode::IOError:
        grpc_code = 14;
        break;
      case arrow::StatusCode::UnknownError:
      default:
        grpc_code = 2;
        break;
    }
    span_holder_->span->SetAttribute("rpc.grpc.status_code", grpc_code);
  }
#else
  (void)status;
#endif
}

arrow::Status TelemetryMiddlewareFactory::StartCall(
    const flight::CallInfo& info, const flight::ServerCallContext& ctx,
    std::shared_ptr<flight::ServerMiddleware>* out) {
  *out = std::make_shared<TelemetryMiddleware>(info.method, ctx.peer(), ctx.incoming_headers());
  return arrow::Status::OK();
}

std::shared_ptr<TelemetrySpanScope> ActivateTelemetrySpan(
    const flight::ServerCallContext& ctx) {
  auto* middleware = ctx.GetMiddleware("telemetry");
  if (!middleware) {
    return nullptr;
  }

  auto* telemetry_middleware = dynamic_cast<TelemetryMiddleware*>(middleware);
  if (!telemetry_middleware) {
    return nullptr;
  }
  return telemetry_middleware->ActivateSpanForCurrentThread();
}

}  // namespace gizmosql
