#pragma once

#include <arrow/flight/server_middleware.h>
#include <chrono>
#include <memory>
#include <string>
#include "flight_sql_fwd.h"

namespace gizmosql {

class AccessLogMiddleware : public flight::ServerMiddleware {
 public:
  AccessLogMiddleware(flight::FlightMethod method, std::string peer);

  void SendingHeaders(flight::AddCallHeaders* /*out*/) override;
  void CallCompleted(const arrow::Status& status) override;
  std::string name() const override { return "access_log"; }

 private:
  flight::FlightMethod method_;
  std::string peer_;
  std::chrono::steady_clock::time_point t0_;
};

class AccessLogFactory : public flight::ServerMiddlewareFactory {
 public:
  arrow::Status StartCall(const flight::CallInfo& info,
                          const flight::ServerCallContext& ctx,
                          std::shared_ptr<flight::ServerMiddleware>* out) override;
};

}  // namespace gizmosql