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

// Middleware for handling Flight SQL Sessions including session cookie handling.

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <shared_mutex>
#include <string_view>

#include <arrow/flight/server_middleware.h>
#include <arrow/flight/sql/types.h>
#include <arrow/status.h>

#include "flight_sql_fwd.h"

namespace gizmosql {
static constexpr char const kSessionCookieName[] = "gizmosql_session_id";

class GizmoSQLSession {
protected:
  std::string id_;
  std::map<std::string, flight::SessionOptionValue> map_;
  std::shared_mutex map_lock_;

public:
  explicit GizmoSQLSession(std::string id) : id_(std::move(id)) {
  }

  virtual ~GizmoSQLSession() = default;

  std::string id() const { return id_; }

  /// \brief Get session option by name
  std::optional<flight::SessionOptionValue> GetSessionOption(const std::string& name);
  /// \brief Get a copy of the session options map.
  ///
  /// The returned options map may be modified by further calls to this FlightSession
  std::map<std::string, flight::SessionOptionValue> GetSessionOptions();
  /// \brief Set session option by name to given value
  void SetSessionOption(const std::string& name, const flight::SessionOptionValue value);
  /// \brief Idempotently remove name from this session
  void EraseSessionOption(const std::string& name);
};

/// \brief A middleware to handle session option persistence and related cookie headers.
///
/// WARNING that client cookie invalidation does not currently work due to a gRPC
/// transport bug.
class GizmoSQLSessionMiddleware : public flight::ServerMiddleware {
public:
  static constexpr char const kMiddlewareName[] =
      "GizmoSQLSessionMiddleware";

  std::string name() const override { return kMiddlewareName; }

  /// \brief Is there an existing session (either existing or new)
  virtual bool HasSession() const = 0;
  /// \brief Get existing or new call-associated session
  ///
  /// May return NULLPTR if there is an id generation collision.

  virtual arrow::Result<std::shared_ptr<GizmoSQLSession>> GetSession() = 0;
  /// Close the current session.
  ///
  /// This is presently unsupported in C++ until middleware handling can be fixed.
  virtual arrow::Status CloseSession() = 0;
  /// \brief Get request headers, in lieu of a provided or created session.
  virtual const flight::CallHeaders& GetCallHeaders() const = 0;
};

/// \brief Returns a flight::ServerMiddlewareFactory that handles session option storage.
/// \param[in] id_gen A thread-safe, collision-free generator for session id strings.
std::shared_ptr<flight::ServerMiddlewareFactory>
MakeGizmoSQLSessionMiddlewareFactory(std::function<std::string()> id_gen,
                                     bool allow_expired_session_cookies = false);
} // namespace gizmosql