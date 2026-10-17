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

// An external policy service under access control: Open Policy Agent's data
// API (POST {"input": ...} -> {"result": true | false}). It is asked after a
// store user's statement passed GizmoSQL's own check, and when their session
// starts, so it can only refuse more. Anything but {"result": true} refuses.

#pragma once

#include <chrono>
#include <string>

#include <nlohmann/json.hpp>

namespace gizmosql::enterprise::rbac {

class PolicyService {
 public:
  enum class Decision { kAllow, kDeny, kNoAnswer };

  // url: the decision endpoint, e.g. http://127.0.0.1:8181/v1/data/gizmosql/allow
  PolicyService(std::string url, std::chrono::milliseconds timeout);

  // Thread-safe; each call makes its own request.
  Decision Decide(const nlohmann::json& input) const;
  const std::string& url() const { return url_; }

 private:
  std::string url_, base_, path_;
  std::chrono::milliseconds timeout_;
};

}  // namespace gizmosql::enterprise::rbac
