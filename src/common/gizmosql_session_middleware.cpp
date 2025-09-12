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

#include <mutex>

#include <arrow/flight/server.h>

#include "gizmosql_logging.h"
#include "gizmosql_session_middleware.h"
#include "gizmosql_session_middleware_factory.h"
#include "flight_sql_fwd.h"

namespace gizmosql {
class GizmoSQLSessionMiddlewareImpl : public GizmoSQLSessionMiddleware {
protected:
  std::shared_mutex mutex_;
  GizmoSQLSessionMiddlewareFactory* factory_;
  const flight::CallHeaders& headers_;
  std::shared_ptr<GizmoSQLSession> session_;
  std::string closed_session_id_;
  bool existing_session_;

public:
  GizmoSQLSessionMiddlewareImpl(GizmoSQLSessionMiddlewareFactory* factory,
                                const flight::CallHeaders& headers)
    : factory_(factory), headers_(headers), existing_session_(false) {
  }

  GizmoSQLSessionMiddlewareImpl(GizmoSQLSessionMiddlewareFactory* factory,
                                const flight::CallHeaders& headers,
                                std::shared_ptr<GizmoSQLSession> session,
                                bool existing_session = true)
    : factory_(factory),
      headers_(headers),
      session_(std::move(session)),
      existing_session_(existing_session) {
  }

  void SendingHeaders(flight::AddCallHeaders* add_call_headers) override {
    if (!existing_session_ && session_) {
      add_call_headers->AddHeader(
          "set-cookie",
          static_cast<std::string>(kSessionCookieName) + "=" + session_->id());
    }
    if (!closed_session_id_.empty()) {
      add_call_headers->AddHeader(
          "set-cookie",
          static_cast<std::string>(kSessionCookieName) + "=" + session_->id() +
          "; Max-Age=0");
    }
  }

  void CallCompleted(const arrow::Status&) override {
  }

  bool HasSession() const override { return static_cast<bool>(session_); }

  arrow::Result<std::shared_ptr<GizmoSQLSession>> GetSession() override {
    const std::lock_guard<std::shared_mutex> l(mutex_);
    if (!session_) {
      auto s = factory_->CreateNewSession();
      session_ = std::move(s);
    }
    if (!static_cast<bool>(session_)) {
      return arrow::Status::UnknownError("Error creating session.");
    }
    return session_;
  }

  arrow::Status CloseSession() override {
    const std::lock_guard<std::shared_mutex> l(mutex_);
    if (!static_cast<bool>(session_)) {
      return arrow::Status::Invalid("Nonexistent session cannot be closed.");
    }
    ARROW_RETURN_NOT_OK(factory_->CloseSession(session_->id()));
    closed_session_id_ = std::move(session_->id());
    session_.reset();
    existing_session_ = false;

    return arrow::Status::OK();
  }

  const flight::CallHeaders& GetCallHeaders() const override { return headers_; }
};

std::vector<std::pair<std::string, std::string>>
GizmoSQLSessionMiddlewareFactory::ParseCookieString(const std::string_view& s) {
  const std::string list_sep = "; ";
  const std::string pair_sep = "=";

  std::vector<std::pair<std::string, std::string>> result;

  size_t cur = 0;
  while (cur < s.length()) {
    const size_t end = s.find(list_sep, cur);
    const bool further_pairs = end != std::string::npos;
    const size_t len = further_pairs ? end - cur : std::string::npos;
    const std::string_view tok = s.substr(cur, len);
    cur = further_pairs ? end + list_sep.length() : s.length();

    const size_t val_pos = tok.find(pair_sep);
    if (val_pos == std::string::npos) {
      // The cookie header is somewhat malformed; ignore the key and continue parsing
      continue;
    }
    const std::string_view cookie_name = tok.substr(0, val_pos);
    std::string_view cookie_value =
        tok.substr(val_pos + pair_sep.length(), std::string::npos);
    if (cookie_name.empty()) {
      continue;
    }
    // Strip doublequotes
    if (cookie_value.length() >= 2 && cookie_value.front() == '"' &&
        cookie_value.back() == '"') {
      cookie_value.remove_prefix(1);
      cookie_value.remove_suffix(1);
    }
    result.emplace_back(cookie_name, cookie_value);
  }

  return result;
}

arrow::Status GizmoSQLSessionMiddlewareFactory::StartCall(
    const flight::CallInfo&, const flight::ServerCallContext& context,
    std::shared_ptr<flight::ServerMiddleware>* middleware) {
  std::string session_id;

  const std::pair<flight::CallHeaders::const_iterator,
                  flight::CallHeaders::const_iterator>&
      headers_it_pr = context.incoming_headers().equal_range("cookie");
  for (auto itr = headers_it_pr.first; itr != headers_it_pr.second; ++itr) {
    const std::string_view& cookie_header = itr->second;
    const std::vector<std::pair<std::string, std::string>> cookies =
        ParseCookieString(cookie_header);
    for (const std::pair<std::string, std::string>& cookie : cookies) {
      if (cookie.first == kSessionCookieName) {
        if (cookie.second.empty())
          return arrow::Status::Invalid("Empty ", kSessionCookieName, " cookie value.");
        session_id = std::move(cookie.second);
      }
    }
    if (!session_id.empty()) break;
  }

  if (session_id.empty()) {
    // No cookie was found
    // Temporary workaround until middleware handling fixed
    auto s = CreateNewSession();
    *middleware = std::make_shared<GizmoSQLSessionMiddlewareImpl>(
        this, context.incoming_headers(), std::move(s), false);
  } else {
    const std::shared_lock<std::shared_mutex> l(session_store_lock_);
    if (auto it = session_store_.find(session_id); it == session_store_.end()) {
      if (!allow_expired_session_cookies_) {
        return arrow::Status::Invalid("Invalid or expired ", kSessionCookieName,
                                      " cookie.");
      } else {
        // Just create a new session
        auto s = CreateNewSession();
        *middleware = std::make_shared<GizmoSQLSessionMiddlewareImpl>(
            this, context.incoming_headers(), std::move(s), false);
      }
    } else {
      auto session = it->second;
      *middleware = std::make_shared<GizmoSQLSessionMiddlewareImpl>(
          this, context.incoming_headers(), std::move(session));
    }
  }

  return arrow::Status::OK();
}

/// \brief Get a new, empty session option map & its id key; {"",NULLPTR} on collision.
std::shared_ptr<GizmoSQLSession>
GizmoSQLSessionMiddlewareFactory::CreateNewSession() {
  auto new_id = id_generator_();
  auto session = std::make_shared<GizmoSQLSession>(new_id);

  const std::lock_guard<std::shared_mutex> l(session_store_lock_);
  if (session_store_.count(new_id)) {
    // Collision
    return NULLPTR;
  }
  session_store_[new_id] = session;

  return session;
}

arrow::Status GizmoSQLSessionMiddlewareFactory::CloseSession(std::string id) {
  const std::lock_guard<std::shared_mutex> l(session_store_lock_);
  if (!session_store_.erase(id)) {
    return arrow::Status::KeyError("Invalid or nonexistent session cannot be closed.");
  }
  return arrow::Status::OK();
}

std::shared_ptr<flight::ServerMiddlewareFactory> MakeGizmoSQLSessionMiddlewareFactory(
    std::function<std::string()> id_gen,
    bool allow_expired_session_cookies) {
  return std::make_shared<GizmoSQLSessionMiddlewareFactory>(
      std::move(id_gen), allow_expired_session_cookies);
}

std::optional<flight::SessionOptionValue> GizmoSQLSession::GetSessionOption(
    const std::string& name) {
  const std::shared_lock<std::shared_mutex> l(map_lock_);
  auto it = map_.find(name);
  if (it != map_.end()) {
    return it->second;
  } else {
    return std::nullopt;
  }
}

std::map<std::string, flight::SessionOptionValue> GizmoSQLSession::GetSessionOptions() {
  const std::shared_lock<std::shared_mutex> l(map_lock_);
  return map_;
}

void GizmoSQLSession::SetSessionOption(const std::string& name,
                                       const flight::SessionOptionValue value) {
  const std::lock_guard<std::shared_mutex> l(map_lock_);
  map_[name] = std::move(value);
}

void GizmoSQLSession::EraseSessionOption(const std::string& name) {
  const std::lock_guard<std::shared_mutex> l(map_lock_);
  map_.erase(name);
}
} // namespace gizmosql