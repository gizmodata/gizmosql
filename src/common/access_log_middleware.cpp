#include "include/detail/access_log_middleware.h"

// Only the .cpp needs logging; keep the header light.
#include "include/detail/gizmosql_logging.h"
#include "include/detail/request_ctx.h"

#include <chrono>
#include <string>
#include <utility>
#include <arrow/flight/server.h>
#include <arrow/flight/server_middleware.h>

namespace gizmosql {
using arrow::Status;
using flight::AddCallHeaders;
using flight::CallInfo;
using flight::FlightMethod;
using flight::ServerCallContext;
using std::chrono::duration_cast;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

// Optional: small helper to stringify FlightMethod (Arrow doesn’t expose a public ToString in all versions)
static const char* MethodName(FlightMethod m) {
  switch (m) {
    case FlightMethod::Handshake:
      return "Handshake";
    case FlightMethod::ListFlights:
      return "ListFlights";
    case FlightMethod::GetFlightInfo:
      return "GetFlightInfo";
    case FlightMethod::GetSchema:
      return "GetSchema";
    case FlightMethod::DoGet:
      return "DoGet";
    case FlightMethod::DoPut:
      return "DoPut";
    case FlightMethod::DoAction:
      return "DoAction";
    case FlightMethod::ListActions:
      return "ListActions";
    case FlightMethod::DoExchange:
      return "DoExchange";
    case FlightMethod::PollFlightInfo:
      return "PollFlightInfo";
    default:
      return "Unknown";
  }
}

AccessLogMiddleware::AccessLogMiddleware(FlightMethod method, std::string peer)
  : method_(method), peer_(std::move(peer)), t0_(steady_clock::now()) {
}

void AccessLogMiddleware::SendingHeaders(AddCallHeaders* /*out*/) {
  // no-op (could add request-id header here)
}

void AccessLogMiddleware::CallCompleted(const Status& status) {
  auto dur_ms = duration_cast<milliseconds>(steady_clock::now() - t0_).count();

  // Goes through your unified gizmo logger (JSON or text based on runtime config)
  GIZMOSQL_LOGF(INFO) << "access"
      << " method=" << MethodName(method_)
      << " peer=" << (peer_.empty() ? "unknown" : peer_)
      << " status=" << status.ToString() << " duration_ms=" << dur_ms;
}

arrow::Status AccessLogFactory::StartCall(
    const CallInfo& info, const ServerCallContext& ctx,
    std::shared_ptr<flight::ServerMiddleware>* out) {
  tl_request_ctx.peer = ctx.peer();

  *out = std::make_shared<AccessLogMiddleware>(info.method, *tl_request_ctx.peer);
  return arrow::Status::OK();
}
} // namespace gizmosql
