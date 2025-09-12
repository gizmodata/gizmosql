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

// ServerSessionMiddlewareFactory, factored into a separate header for testability

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <utility>
#include <vector>

#include "gizmosql_session_middleware.h"
#include "flight_sql_fwd.h"

namespace gizmosql {

/// \brief A factory for ServerSessionMiddleware, itself storing session data.
class GizmoSQLSessionMiddlewareFactory
    : public flight::ServerMiddlewareFactory {
 protected:
  std::map<std::string, std::shared_ptr<GizmoSQLSession>> session_store_;
  std::shared_mutex session_store_lock_;
  std::function<std::string()> id_generator_;
  bool allow_expired_session_cookies_;

  static std::vector<std::pair<std::string, std::string>> ParseCookieString(
      const std::string_view& s);

 public:
  explicit GizmoSQLSessionMiddlewareFactory(std::function<std::string()> id_gen, bool allow_expired_session_cookies)
      : id_generator_(id_gen), allow_expired_session_cookies_(allow_expired_session_cookies) {}
  arrow::Status StartCall(const flight::CallInfo&, const flight::ServerCallContext& context,
                   std::shared_ptr<flight::ServerMiddleware>* middleware) override;

  /// \brief Get a new, empty session option map and its id key.
  std::shared_ptr<GizmoSQLSession> CreateNewSession();
  /// \brief Close the session identified by 'id'.
  /// \param id The string id of the session to close.
  arrow::Status CloseSession(std::string id);
};

}  // namespace gizmosql
