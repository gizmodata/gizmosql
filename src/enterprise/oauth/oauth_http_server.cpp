// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "oauth_http_server.h"
#include "oauth_html_templates.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include <nlohmann/json.hpp>
#include "jwt-cpp/jwt.h"
#include "gizmosql_logging.h"
#include "gizmosql_security.h"

namespace gizmosql::enterprise {

namespace {
/// URL-encode a string for use as a query parameter value.
std::string UrlEncode(const std::string& value) {
  std::ostringstream encoded;
  encoded.fill('0');
  encoded << std::hex;
  for (unsigned char c : value) {
    if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
      encoded << c;
    } else {
      encoded << '%' << std::uppercase << std::setw(2)
              << static_cast<int>(c) << std::nouppercase;
    }
  }
  return encoded.str();
}
}  // namespace

OAuthHttpServer::OAuthHttpServer(Config config)
    : config_(std::move(config)) {}

OAuthHttpServer::~OAuthHttpServer() {
  Shutdown();
}

arrow::Status OAuthHttpServer::Start() {
  if (config_.client_id.empty()) {
    return arrow::Status::Invalid("OAuth client ID is required");
  }
  if (config_.authorization_endpoint.empty() || config_.token_endpoint.empty()) {
    return arrow::Status::Invalid(
        "OAuth authorization_endpoint and token_endpoint are required "
        "(from OIDC discovery)");
  }

  // Auto-construct redirect URI if not set
  bool use_tls = !config_.tls_cert_path.empty() && !config_.tls_key_path.empty() &&
                 !config_.disable_tls;
  if (config_.redirect_uri.empty()) {
    std::string scheme = use_tls ? "https" : "http";
    config_.redirect_uri =
        scheme + "://localhost:" + std::to_string(config_.port) + "/oauth/callback";
    GIZMOSQL_LOG(INFO) << "OAuth redirect URI auto-constructed: " << config_.redirect_uri;
  }

  if (config_.disable_tls) {
    GIZMOSQL_LOG(WARNING) << "OAuth HTTP server TLS is DISABLED. "
                          << "This should ONLY be used for localhost development/testing.";
  }

  // Create server (SSL if TLS configured and not disabled)
  if (use_tls) {
    auto ssl_server = std::make_unique<httplib::SSLServer>(
        config_.tls_cert_path.c_str(), config_.tls_key_path.c_str());
    server_ = std::move(ssl_server);
  } else {
    server_ = std::make_unique<httplib::Server>();
  }

  // Register routes
  server_->Get("/oauth/initiate", [this](const httplib::Request& req, httplib::Response& res) {
    HandleInitiate(req, res);
  });

  server_->Get("/oauth/start", [this](const httplib::Request& req, httplib::Response& res) {
    HandleStart(req, res);
  });

  server_->Get("/oauth/callback", [this](const httplib::Request& req, httplib::Response& res) {
    HandleCallback(req, res);
  });

  server_->Get(R"(/oauth/token/([a-f0-9\-]{36}))",
               [this](const httplib::Request& req, httplib::Response& res) {
                 HandleTokenPoll(req, res);
               });

  // Start server thread
  server_thread_ = std::thread([this]() {
    GIZMOSQL_LOG(INFO) << "OAuth HTTP server listening on port " << config_.port;
    if (!server_->listen("0.0.0.0", config_.port)) {
      if (!shutdown_) {
        GIZMOSQL_LOG(ERROR) << "OAuth HTTP server failed to listen on port " << config_.port;
      }
    }
  });

  // Start cleanup thread
  cleanup_thread_ = std::thread([this]() {
    CleanupExpiredSessions();
  });

  return arrow::Status::OK();
}

void OAuthHttpServer::Shutdown() {
  if (shutdown_.exchange(true)) {
    return;  // Already shutting down
  }

  if (server_) {
    server_->stop();
  }

  if (server_thread_.joinable()) {
    server_thread_.join();
  }

  if (cleanup_thread_.joinable()) {
    cleanup_thread_.join();
  }

  GIZMOSQL_LOG(INFO) << "OAuth HTTP server shutdown complete";
}

void OAuthHttpServer::HandleInitiate(const httplib::Request& req, httplib::Response& res) {
  // Generate a random UUID
  boost::uuids::random_generator gen;
  std::string uuid = boost::uuids::to_string(gen());

  // Compute session hash = HMAC-SHA256(secret_key, uuid)
  std::string session_hash =
      gizmosql::SecurityUtilities::HMAC_SHA256(config_.secret_key, uuid);

  // Store pending auth
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    pending_auths_[session_hash] = PendingAuth{
        .created_at = std::chrono::steady_clock::now(),
    };
  }

  // Build IdP authorization URL
  std::string auth_url = config_.authorization_endpoint;
  auth_url += "?response_type=code";
  auth_url += "&client_id=" + UrlEncode(config_.client_id);
  auth_url += "&redirect_uri=" + UrlEncode(config_.redirect_uri);
  auth_url += "&scope=" + UrlEncode(config_.scopes);
  auth_url += "&state=" + session_hash;

  GIZMOSQL_LOG(DEBUG) << "OAuth: Initiated session for UUID: "
                      << uuid.substr(0, 8) << "...";

  nlohmann::json response;
  response["session_uuid"] = uuid;
  response["auth_url"] = auth_url;

  res.set_content(response.dump(), "application/json");
}

void OAuthHttpServer::HandleStart(const httplib::Request& req, httplib::Response& res) {
  auto session_it = req.params.find("session");
  if (session_it == req.params.end() || session_it->second.empty()) {
    res.status = 400;
    res.set_content(RenderPage(kOAuthErrorPage, "Missing session parameter"), "text/html");
    return;
  }

  const std::string& session_hash = session_it->second;

  // Store pending auth
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    if (pending_auths_.count(session_hash)) {
      // Session already exists, reject duplicate
      res.status = 409;
      res.set_content(RenderPage(kOAuthErrorPage, "Session already in progress"), "text/html");
      return;
    }
    pending_auths_[session_hash] = PendingAuth{
        .created_at = std::chrono::steady_clock::now(),
    };
  }

  // Build IdP authorization URL
  std::string auth_url = config_.authorization_endpoint;
  auth_url += "?response_type=code";
  auth_url += "&client_id=" + UrlEncode(config_.client_id);
  auth_url += "&redirect_uri=" + UrlEncode(config_.redirect_uri);
  auth_url += "&scope=" + UrlEncode(config_.scopes);
  auth_url += "&state=" + session_hash;

  GIZMOSQL_LOG(DEBUG) << "OAuth: Redirecting to IdP for session hash: "
                      << session_hash.substr(0, 8) << "...";

  // HTTP 302 redirect to IdP
  res.status = 302;
  res.set_header("Location", auth_url);
}

void OAuthHttpServer::HandleCallback(const httplib::Request& req, httplib::Response& res) {
  auto code_it = req.params.find("code");
  auto state_it = req.params.find("state");
  auto error_it = req.params.find("error");

  // Check for IdP-reported error
  if (error_it != req.params.end()) {
    std::string error_desc;
    auto desc_it = req.params.find("error_description");
    if (desc_it != req.params.end()) {
      error_desc = desc_it->second;
    } else {
      error_desc = error_it->second;
    }

    GIZMOSQL_LOG(WARNING) << "OAuth: IdP returned error: " << error_desc;

    if (state_it != req.params.end()) {
      std::lock_guard<std::mutex> lock(pending_mutex_);
      auto it = pending_auths_.find(state_it->second);
      if (it != pending_auths_.end()) {
        it->second.error = error_desc;
      }
    }

    res.set_content(RenderPage(kOAuthErrorPage, error_desc), "text/html");
    return;
  }

  if (code_it == req.params.end() || state_it == req.params.end()) {
    res.status = 400;
    res.set_content(RenderPage(kOAuthErrorPage, "Missing code or state parameter"), "text/html");
    return;
  }

  const std::string& code = code_it->second;
  const std::string& session_hash = state_it->second;

  // Verify the session hash exists and is still pending
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it == pending_auths_.end()) {
      res.set_content(RenderPage(kOAuthExpiredPage), "text/html");
      return;
    }
    if (it->second.id_token.has_value() || it->second.error.has_value()) {
      res.status = 409;
      res.set_content(RenderPage(kOAuthErrorPage, "Session already completed"), "text/html");
      return;
    }
  }

  // Exchange code for tokens
  auto id_token_result = ExchangeCodeForTokens(code);
  if (!id_token_result.ok()) {
    std::string err = id_token_result.status().message();
    GIZMOSQL_LOG(WARNING) << "OAuth: Code exchange failed: " << err;

    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it != pending_auths_.end()) {
      it->second.error = err;
    }
    res.set_content(RenderPage(kOAuthErrorPage, err), "text/html");
    return;
  }

  // Check email authorization (lightweight, no cryptographic verification —
  // full JWKS/issuer/audience validation happens in VerifyAndDecodeBootstrapToken)
  auto email_check = CheckEmailAuthorization(*id_token_result);
  if (!email_check.ok()) {
    std::string err = std::string(email_check.message());
    GIZMOSQL_LOG(WARNING) << "OAuth: Email authorization failed: " << err;

    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it != pending_auths_.end()) {
      it->second.error = err;
    }
    res.set_content(RenderPage(kOAuthErrorPage, err), "text/html");
    return;
  }

  // Store the raw ID token for polling — the client will send this via Basic Auth
  // and VerifyAndDecodeBootstrapToken will do full cryptographic verification.
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it != pending_auths_.end()) {
      it->second.id_token = *id_token_result;
    }
  }

  GIZMOSQL_LOG(INFO) << "OAuth: Authentication successful for session hash: "
                     << session_hash.substr(0, 8) << "...";

  res.set_content(RenderPage(kOAuthSuccessPage), "text/html");
}

void OAuthHttpServer::HandleTokenPoll(const httplib::Request& req, httplib::Response& res) {
  // Extract UUID from path match
  std::string uuid = req.matches[1];

  // Compute HASH = HMAC-SHA256(secret_key, uuid)
  std::string session_hash =
      gizmosql::SecurityUtilities::HMAC_SHA256(config_.secret_key, uuid);

  nlohmann::json response;

  std::lock_guard<std::mutex> lock(pending_mutex_);
  auto it = pending_auths_.find(session_hash);
  if (it == pending_auths_.end()) {
    response["status"] = "not_found";
    res.status = 404;
  } else if (it->second.error.has_value()) {
    response["status"] = "error";
    response["error"] = *it->second.error;
    res.status = 200;
  } else if (it->second.id_token.has_value()) {
    response["status"] = "complete";
    response["token"] = *it->second.id_token;
    res.status = 200;
    // Remove the entry after successful retrieval (one-time use)
    pending_auths_.erase(it);
  } else {
    response["status"] = "pending";
    res.status = 200;
  }

  res.set_content(response.dump(), "application/json");
}

arrow::Result<std::string> OAuthHttpServer::ExchangeCodeForTokens(
    const std::string& code) {
  // Parse token endpoint URL
  std::string url = config_.token_endpoint;
  size_t scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    return arrow::Status::Invalid("Invalid token endpoint URL: " + url);
  }

  std::string rest = url.substr(scheme_end + 3);
  size_t path_start = rest.find('/');
  std::string host_port = (path_start == std::string::npos) ? rest : rest.substr(0, path_start);
  std::string path = (path_start == std::string::npos) ? "/" : rest.substr(path_start);

  std::string base_url = url.substr(0, scheme_end + 3) + host_port;

  httplib::Client client(base_url);
  client.set_follow_location(true);
  client.set_connection_timeout(std::chrono::seconds(10));
  client.set_read_timeout(std::chrono::seconds(10));

  // Build POST body
  httplib::Params params;
  params.emplace("grant_type", "authorization_code");
  params.emplace("code", code);
  params.emplace("redirect_uri", config_.redirect_uri);
  params.emplace("client_id", config_.client_id);
  params.emplace("client_secret", config_.client_secret);

  auto result = client.Post(path, params);
  if (!result) {
    return arrow::Status::IOError("Token exchange HTTP request failed: " +
                                  httplib::to_string(result.error()));
  }

  if (result->status != 200) {
    return arrow::Status::IOError("Token endpoint returned status " +
                                  std::to_string(result->status) + ": " + result->body);
  }

  try {
    auto json = nlohmann::json::parse(result->body);

    if (json.contains("error")) {
      std::string err = json["error"].get<std::string>();
      if (json.contains("error_description")) {
        err += ": " + json["error_description"].get<std::string>();
      }
      return arrow::Status::Invalid("Token exchange error: " + err);
    }

    if (!json.contains("id_token") || !json["id_token"].is_string()) {
      return arrow::Status::Invalid(
          "Token endpoint response missing 'id_token' field");
    }

    return json["id_token"].get<std::string>();
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to parse token endpoint response: " +
                                  std::string(e.what()));
  }
}

arrow::Status OAuthHttpServer::CheckEmailAuthorization(const std::string& id_token) {
  try {
    // Decode without cryptographic verification — just extract claims for email check.
    // Full JWKS/issuer/audience verification happens in VerifyAndDecodeBootstrapToken.
    auto decoded = jwt::decode(id_token);

    // Extract username (prefer email, fallback to sub)
    std::string email;
    if (decoded.has_payload_claim("email")) {
      try {
        email = decoded.get_payload_claim("email").as_string();
      } catch (...) {}
    }
    std::string username = email.empty() ? decoded.get_subject() : email;

    // Check email authorization (early rejection for better browser UX)
    if (!IsEmailAuthorized(username)) {
      GIZMOSQL_LOGKV(WARNING,
                     "OAuth: User '" + username + "' not in authorized email list",
                     {"user", username}, {"kind", "authentication"},
                     {"authentication_type", "oauth"}, {"result", "failure"},
                     {"reason", "unauthorized_email"});
      return arrow::Status::Invalid("User '" + username + "' is not authorized");
    }

    GIZMOSQL_LOGKV(INFO,
                   "OAuth: Code exchange successful for user '" + username + "'",
                   {"user", username}, {"kind", "authentication"},
                   {"authentication_type", "oauth"}, {"authentication_method", "code_exchange"},
                   {"result", "success"});

    return arrow::Status::OK();
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to decode ID token: " +
                                  std::string(e.what()));
  }
}

bool OAuthHttpServer::IsEmailAuthorized(const std::string& email) const {
  if (config_.authorized_email_patterns.empty()) return true;

  std::string lower_email = email;
  std::transform(lower_email.begin(), lower_email.end(), lower_email.begin(), ::tolower);

  for (const auto& pattern : config_.authorized_email_patterns) {
    std::string lower_pattern = pattern;
    std::transform(lower_pattern.begin(), lower_pattern.end(), lower_pattern.begin(), ::tolower);

    if (lower_pattern == "*") return true;
    if (lower_pattern.front() == '*') {
      // Suffix match: *@domain.com
      std::string suffix = lower_pattern.substr(1);
      if (lower_email.size() >= suffix.size() &&
          lower_email.compare(lower_email.size() - suffix.size(), suffix.size(), suffix) == 0) {
        return true;
      }
    } else if (lower_email == lower_pattern) {
      return true;
    }
  }
  return false;
}

std::string OAuthHttpServer::RenderPage(const char* tmpl, const std::string& error) const {
  std::string html(tmpl);

  // Replace style placeholder
  auto styles_pos = html.find("%STYLES%");
  if (styles_pos != std::string::npos) {
    html.replace(styles_pos, 8, kOAuthPageStyles);
  }

  // Replace logo placeholder
  auto logo_pos = html.find("%LOGO%");
  if (logo_pos != std::string::npos) {
    html.replace(logo_pos, 6, kGizmoSQLLogoBase64);
  }

  // Replace error placeholder
  auto error_pos = html.find("%ERROR%");
  if (error_pos != std::string::npos) {
    html.replace(error_pos, 7, error);
  }

  return html;
}

void OAuthHttpServer::CleanupExpiredSessions() {
  while (!shutdown_) {
    // Sleep in small increments to allow quick shutdown
    for (int i = 0; i < 60 && !shutdown_; ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    if (shutdown_) break;

    auto now = std::chrono::steady_clock::now();
    auto timeout = std::chrono::minutes(kChallengeTimeoutMinutes);

    std::lock_guard<std::mutex> lock(pending_mutex_);
    for (auto it = pending_auths_.begin(); it != pending_auths_.end();) {
      if (now - it->second.created_at > timeout) {
        GIZMOSQL_LOG(DEBUG) << "OAuth: Cleaning up expired session: "
                            << it->first.substr(0, 8) << "...";
        it = pending_auths_.erase(it);
      } else {
        ++it;
      }
    }
  }
}

}  // namespace gizmosql::enterprise
