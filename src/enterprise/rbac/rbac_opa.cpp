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

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "rbac_opa.h"

#include "httplib.h"
#include "system_ca_certs.h"

namespace gizmosql::enterprise::rbac {

PolicyService::PolicyService(std::string url, std::chrono::milliseconds timeout)
    : url_(std::move(url)), timeout_(timeout) {
  // httplib::Client wants scheme://host[:port]; the rest is the path.
  const auto scheme_end = url_.find("://");
  const std::string after =
      scheme_end == std::string::npos ? url_ : url_.substr(scheme_end + 3);
  const std::string scheme =
      scheme_end == std::string::npos ? "http" : url_.substr(0, scheme_end);
  const auto slash = after.find('/');
  base_ = scheme + "://" + (slash == std::string::npos ? after : after.substr(0, slash));
  path_ = slash == std::string::npos ? "/" : after.substr(slash);
}

PolicyService::Decision PolicyService::Decide(const nlohmann::json& input) const {
  try {
    httplib::Client client(base_);
    client.set_connection_timeout(timeout_);
    client.set_read_timeout(timeout_);
    client.set_write_timeout(timeout_);
    if (auto ca = gizmosql::FindSystemCaCertFile()) client.set_ca_cert_path(ca->c_str());
    const auto body = nlohmann::json{{"input", input}}.dump();
    auto res = client.Post(path_, body, "application/json");
    if (!res || res->status != 200) return Decision::kNoAnswer;
    const auto reply =
        nlohmann::json::parse(res->body, nullptr, /*allow_exceptions=*/false);
    if (!reply.is_object() || !reply.contains("result") ||
        !reply["result"].is_boolean()) {
      return Decision::kNoAnswer;
    }
    return reply["result"].get<bool>() ? Decision::kAllow : Decision::kDeny;
  } catch (...) {
    return Decision::kNoAnswer;
  }
}

}  // namespace gizmosql::enterprise::rbac
