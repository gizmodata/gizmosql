// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "oauth_http_server.h"
#include "oauth_html_templates.h"

#include <algorithm>
#include <sstream>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include <nlohmann/json.hpp>
#include "jwt-cpp/jwt.h"
#include "gizmosql_logging.h"
#include "gizmosql_security.h"
#include "enterprise/jwks/jwks_manager.h"

namespace gizmosql::enterprise {

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
  if (config_.redirect_uri.empty()) {
    std::string scheme = config_.tls_cert_path.empty() ? "http" : "https";
    config_.redirect_uri =
        scheme + "://localhost:" + std::to_string(config_.port) + "/oauth/callback";
    GIZMOSQL_LOG(INFO) << "OAuth redirect URI auto-constructed: " << config_.redirect_uri;
  }

  // Create server (SSL if TLS configured)
  if (!config_.tls_cert_path.empty() && !config_.tls_key_path.empty()) {
    auto ssl_server = std::make_unique<httplib::SSLServer>(
        config_.tls_cert_path.c_str(), config_.tls_key_path.c_str());
    server_ = std::move(ssl_server);
  } else {
    server_ = std::make_unique<httplib::Server>();
  }

  // Register routes
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
  auth_url += "&client_id=" + config_.client_id;
  auth_url += "&redirect_uri=" + config_.redirect_uri;
  auth_url += "&scope=" + config_.scopes;
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
    if (it->second.gizmosql_jwt.has_value() || it->second.error.has_value()) {
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

  // Validate ID token and create GizmoSQL JWT
  auto jwt_result = ValidateAndCreateSession(*id_token_result);
  if (!jwt_result.ok()) {
    std::string err = jwt_result.status().message();
    GIZMOSQL_LOG(WARNING) << "OAuth: Token validation failed: " << err;

    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it != pending_auths_.end()) {
      it->second.error = err;
    }
    res.set_content(RenderPage(kOAuthErrorPage, err), "text/html");
    return;
  }

  // Store the JWT for polling
  {
    std::lock_guard<std::mutex> lock(pending_mutex_);
    auto it = pending_auths_.find(session_hash);
    if (it != pending_auths_.end()) {
      it->second.gizmosql_jwt = *jwt_result;
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
  } else if (it->second.gizmosql_jwt.has_value()) {
    response["status"] = "complete";
    response["token"] = *it->second.gizmosql_jwt;
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

arrow::Result<std::string> OAuthHttpServer::ValidateAndCreateSession(
    const std::string& id_token) {
  try {
    auto decoded = jwt::decode(id_token);

    // Verify issuer
    auto iss = decoded.get_issuer();
    if (iss != config_.token_allowed_issuer) {
      return arrow::Status::Invalid("ID token issuer mismatch: expected '" +
                                    config_.token_allowed_issuer + "', got '" + iss + "'");
    }

    // Build verifier
    auto verifier = jwt::verify()
                        .with_issuer(config_.token_allowed_issuer)
                        .with_audience(config_.token_allowed_audience);

    // JWKS-based verification
    if (!config_.jwks_manager) {
      return arrow::Status::Invalid("JWKS manager not configured for OAuth verification");
    }

    // Extract kid from JWT header
    std::string kid;
    if (decoded.has_header_claim("kid")) {
      kid = decoded.get_header_claim("kid").as_string();
    }
    if (kid.empty()) {
      return arrow::Status::Invalid(
          "ID token missing 'kid' header claim, required for JWKS verification");
    }

    // Get the public key
    auto key_result = config_.jwks_manager->GetKeyForKid(kid);
    if (!key_result.ok()) {
      return arrow::Status::Invalid("Failed to find public key for kid '" + kid +
                                    "': " + key_result.status().ToString());
    }

    // Get algorithm
    auto alg_result = config_.jwks_manager->GetAlgorithmForKid(kid);
    std::string alg = alg_result.ok() ? *alg_result : "";
    if (alg.empty() && decoded.has_header_claim("alg")) {
      alg = decoded.get_header_claim("alg").as_string();
    }

    // Apply appropriate algorithm
    if (alg == "RS256" || alg.empty()) {
      verifier.allow_algorithm(jwt::algorithm::rs256(*key_result, "", "", ""));
    } else if (alg == "RS384") {
      verifier.allow_algorithm(jwt::algorithm::rs384(*key_result, "", "", ""));
    } else if (alg == "RS512") {
      verifier.allow_algorithm(jwt::algorithm::rs512(*key_result, "", "", ""));
    } else if (alg == "ES256") {
      verifier.allow_algorithm(jwt::algorithm::es256(*key_result, "", "", ""));
    } else if (alg == "ES384") {
      verifier.allow_algorithm(jwt::algorithm::es384(*key_result, "", "", ""));
    } else if (alg == "ES512") {
      verifier.allow_algorithm(jwt::algorithm::es512(*key_result, "", "", ""));
    } else {
      return arrow::Status::Invalid("Unsupported JWT algorithm: " + alg);
    }

    verifier.verify(decoded);

    // Extract username (prefer email, fallback to sub)
    std::string email;
    if (decoded.has_payload_claim("email")) {
      try {
        email = decoded.get_payload_claim("email").as_string();
      } catch (...) {}
    }
    std::string username = email.empty() ? decoded.get_subject() : email;

    // Check email authorization
    if (!IsEmailAuthorized(username)) {
      GIZMOSQL_LOGKV(WARNING,
                     "OAuth: User '" + username + "' not in authorized email list",
                     {"user", username}, {"kind", "authentication"},
                     {"authentication_type", "oauth"}, {"result", "failure"},
                     {"reason", "unauthorized_email"});
      return arrow::Status::Invalid("User '" + username + "' is not authorized");
    }

    // Determine role
    std::string role;
    if (decoded.has_payload_claim("role")) {
      role = decoded.get_payload_claim("role").as_string();
    } else {
      role = config_.token_default_role;
    }
    if (role.empty()) {
      return arrow::Status::Invalid(
          "ID token missing 'role' claim and no default role configured");
    }

    // Create GizmoSQL session JWT
    auto gizmosql_jwt = gizmosql::CreateGizmoSQLJWT(
        username, role, "OAuthCodeExchange", config_.secret_key, config_.instance_id);

    GIZMOSQL_LOGKV(INFO,
                   "OAuth: Session created for user '" + username + "' with role '" + role + "'",
                   {"user", username}, {"role", role}, {"kind", "authentication"},
                   {"authentication_type", "oauth"}, {"authentication_method", "code_exchange"},
                   {"result", "success"});

    return gizmosql_jwt;
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("ID token verification failed: " +
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
