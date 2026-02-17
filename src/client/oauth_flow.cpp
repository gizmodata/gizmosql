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

#include "oauth_flow.hpp"

#include <chrono>
#include <iostream>
#include <thread>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include <httplib.h>
#include <nlohmann/json.hpp>

namespace gizmosql::client {

namespace {
constexpr int kPollIntervalMs = 1000;
constexpr int kPollTimeoutSeconds = 300;  // 5 minutes
}  // namespace

arrow::Result<std::string> OAuthFlow::Authenticate(const std::string& oauth_base_url) {
  ARROW_ASSIGN_OR_RAISE(auto initiate_result, Initiate(oauth_base_url));
  auto& session_uuid = initiate_result.first;
  auto& auth_url = initiate_result.second;

  auto status = OpenBrowser(auth_url);
  if (!status.ok()) {
    std::cerr << "Could not open browser automatically." << std::endl;
    std::cerr << "Please open this URL manually:" << std::endl;
    std::cerr << auth_url << std::endl;
  }

  std::cerr << "Waiting for browser login... (press Ctrl+C to cancel)"
            << std::endl;

  return PollForToken(oauth_base_url, session_uuid);
}

arrow::Result<std::pair<std::string, std::string>> OAuthFlow::Initiate(
    const std::string& oauth_base_url) {
  httplib::Client cli(oauth_base_url);
  cli.set_connection_timeout(10);
  cli.set_read_timeout(10);

  auto res = cli.Get("/oauth/initiate");
  if (!res) {
    return arrow::Status::IOError(
        "Failed to connect to OAuth server at " + oauth_base_url +
        ": " + httplib::to_string(res.error()));
  }

  if (res->status != 200) {
    return arrow::Status::IOError(
        "OAuth initiate failed with HTTP " + std::to_string(res->status) +
        ": " + res->body);
  }

  auto json = nlohmann::json::parse(res->body, nullptr, false);
  if (json.is_discarded()) {
    return arrow::Status::IOError("Failed to parse OAuth initiate response");
  }

  if (!json.contains("session_uuid") || !json.contains("auth_url")) {
    return arrow::Status::IOError(
        "OAuth initiate response missing required fields");
  }

  return std::make_pair(json["session_uuid"].get<std::string>(),
                        json["auth_url"].get<std::string>());
}

arrow::Status OAuthFlow::OpenBrowser(const std::string& url) {
#ifdef __APPLE__
  std::string cmd = "open \"" + url + "\"";
#elif defined(__linux__)
  std::string cmd = "xdg-open \"" + url + "\"";
#else
  return arrow::Status::NotImplemented("Browser opening not supported on this platform");
#endif
  int ret = std::system(cmd.c_str());
  if (ret != 0) {
    return arrow::Status::IOError("Failed to open browser");
  }
  return arrow::Status::OK();
}

arrow::Result<std::string> OAuthFlow::PollForToken(
    const std::string& oauth_base_url, const std::string& uuid) {
  httplib::Client cli(oauth_base_url);
  cli.set_connection_timeout(10);
  cli.set_read_timeout(10);

  auto start = std::chrono::steady_clock::now();
  std::string poll_path = "/oauth/token/" + uuid;

  while (true) {
    auto elapsed = std::chrono::steady_clock::now() - start;
    if (std::chrono::duration_cast<std::chrono::seconds>(elapsed).count() >=
        kPollTimeoutSeconds) {
      return arrow::Status::IOError(
          "OAuth login timed out after " +
          std::to_string(kPollTimeoutSeconds) + " seconds");
    }

    auto res = cli.Get(poll_path);
    if (!res) {
      return arrow::Status::IOError(
          "Failed to poll OAuth token: " + httplib::to_string(res.error()));
    }

    if (res->status != 200) {
      return arrow::Status::IOError(
          "OAuth token poll failed with HTTP " + std::to_string(res->status));
    }

    auto json = nlohmann::json::parse(res->body, nullptr, false);
    if (json.is_discarded()) {
      return arrow::Status::IOError("Failed to parse OAuth token response");
    }

    std::string poll_status = json.value("status", "");

    if (poll_status == "complete") {
      if (!json.contains("token")) {
        return arrow::Status::IOError(
            "OAuth response has status 'complete' but no token");
      }
      return json["token"].get<std::string>();
    } else if (poll_status == "error") {
      std::string msg = json.value("message", "Unknown OAuth error");
      return arrow::Status::IOError("OAuth login failed: " + msg);
    } else if (poll_status == "pending") {
      std::this_thread::sleep_for(std::chrono::milliseconds(kPollIntervalMs));
      continue;
    } else {
      return arrow::Status::IOError("Unknown OAuth poll status: " + poll_status);
    }
  }
}

}  // namespace gizmosql::client
