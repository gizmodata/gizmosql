// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <arrow/result.h>
#include <arrow/status.h>

namespace httplib {
class Server;
class SSLServer;
class Request;
class Response;
}  // namespace httplib

namespace gizmosql::enterprise {

/// HTTP server that handles the OAuth authorization code exchange flow.
/// The server owns the client ID/secret, handles browser redirects, and exchanges
/// authorization codes for ID tokens from the Identity Provider.
///
/// Flow:
///   1. Client generates UUID, opens browser to /oauth/start?session=HASH
///   2. Server redirects to IdP authorization URL
///   3. IdP redirects back to /oauth/callback with authorization code
///   4. Server exchanges code for ID token, checks email authorization
///   5. Client polls /oauth/token/:uuid to retrieve the raw ID token
///   6. Client sends ID token via Basic Auth (username="token") to Flight server
///   7. VerifyAndDecodeBootstrapToken validates issuer/audience/JWKS and issues session JWT
class OAuthHttpServer {
 public:
  struct Config {
    int port = 0;
    std::string client_id;
    std::string client_secret;
    std::string scopes;
    std::string redirect_uri;  // Auto-constructed if empty
    std::string secret_key;    // For HMAC session hashing only
    std::vector<std::string> authorized_email_patterns;
    bool disable_tls = false;    // Run plain HTTP even when main server has TLS
    std::string tls_cert_path;
    std::string tls_key_path;
    std::string authorization_endpoint;  // From OIDC discovery
    std::string token_endpoint;          // From OIDC discovery
  };

  explicit OAuthHttpServer(Config config);
  ~OAuthHttpServer();

  /// Start the HTTP server. Returns error if unable to bind.
  arrow::Status Start();

  /// Shut down the HTTP server and cleanup thread.
  void Shutdown();


 private:
  /// GET /oauth/initiate — Generate UUID+hash, return JSON with auth URL
  void HandleInitiate(const httplib::Request& req, httplib::Response& res);

  /// GET /oauth/start?session=HASH — Redirect to IdP authorization URL
  void HandleStart(const httplib::Request& req, httplib::Response& res);

  /// GET /oauth/callback?code=...&state=HASH — Exchange code for tokens
  void HandleCallback(const httplib::Request& req, httplib::Response& res);

  /// GET /oauth/token/:uuid — Poll for completed authentication
  void HandleTokenPoll(const httplib::Request& req, httplib::Response& res);

  /// Exchange an authorization code for tokens via the IdP's token endpoint.
  /// Returns the raw ID token string on success.
  arrow::Result<std::string> ExchangeCodeForTokens(const std::string& code);

  /// Decode an ID token (without cryptographic verification) and check email authorization.
  /// Returns OK if the email is authorized, error otherwise.
  /// Full cryptographic verification happens later in VerifyAndDecodeBootstrapToken.
  arrow::Status CheckEmailAuthorization(const std::string& id_token);

  /// Check if an email matches the authorized email patterns.
  bool IsEmailAuthorized(const std::string& email) const;

  /// Render an HTML template by replacing placeholders.
  std::string RenderPage(const char* tmpl, const std::string& error = "") const;

  /// Background thread that cleans up expired pending auth sessions.
  void CleanupExpiredSessions();

  struct PendingAuth {
    std::chrono::steady_clock::time_point created_at;
    std::optional<std::string> id_token;  // Raw IdP ID token, set on success
    std::optional<std::string> error;          // Set on failure
  };

  std::mutex pending_mutex_;
  std::unordered_map<std::string, PendingAuth> pending_auths_;  // Keyed by HASH

  Config config_;
  std::unique_ptr<httplib::Server> server_;
  std::thread server_thread_;
  std::thread cleanup_thread_;
  std::atomic<bool> shutdown_{false};

  static constexpr int kChallengeTimeoutMinutes = 15;
};

}  // namespace gizmosql::enterprise
