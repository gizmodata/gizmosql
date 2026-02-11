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

#include <filesystem>
#include <optional>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/server_auth.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/util/base64.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include <iomanip>
#include "jwt-cpp/jwt.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <shared_mutex>
#include <unordered_set>
#include <vector>
#include <openssl/hmac.h>
#include <openssl/evp.h>

#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"

#ifdef GIZMOSQL_ENTERPRISE
namespace gizmosql::enterprise {
class JwksManager;
}  // namespace gizmosql::enterprise
#endif

namespace gizmosql {

/// Create a GizmoSQL-issued JWT token with the given claims.
/// Used by both Basic Auth (core) and OAuth code exchange (enterprise) to issue session tokens.
std::string CreateGizmoSQLJWT(
    const std::string& username, const std::string& role,
    const std::string& auth_method, const std::string& secret_key,
    const std::string& instance_id,
    const std::optional<std::string>& catalog_access_json = std::nullopt);

class SecurityUtilities {
 public:
  static arrow::Status FlightServerTlsCertificates(const std::filesystem::path& cert_path,
                                                   const std::filesystem::path& key_path,
                                                   std::vector<flight::CertKeyPair>* out);

  static arrow::Status FlightServerMtlsCACertificate(const std::string& cert_path,
                                                     std::string* out);

  static std::string FindKeyValPrefixInCallHeaders(
      const flight::CallHeaders& incoming_headers, const std::string& key,
      const std::string& prefix);

  static arrow::Status GetAuthHeaderType(const flight::CallHeaders& incoming_headers,
                                         std::string* out);

  static void ParseBasicHeader(const flight::CallHeaders& incoming_headers,
                               std::string& username, std::string& password);

  // Compute HMAC-SHA256 of data using the provided key, return hex-encoded result
  static std::string HMAC_SHA256(const std::string& key, const std::string& data);
};

class BasicAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  BasicAuthServerMiddleware(const std::string& username, const std::string& role,
                            const std::string& auth_method,
                            const std::string& secret_key,
                            const std::string& instance_id,
                            std::optional<std::string> catalog_access_json = std::nullopt);

  const jwt::decoded_jwt<jwt::traits::kazuho_picojson> GetJWT();
  const std::string GetUsername() const;
  const std::string GetRole() const;

  void SendingHeaders(flight::AddCallHeaders* outgoing_headers) override;

  void CallCompleted(const arrow::Status& status) override;

  std::string name() const override;

 private:
  std::string username_;
  std::string role_;
  std::string auth_method_;
  std::string secret_key_;
  std::string instance_id_;
  std::optional<std::string> catalog_access_json_;

  std::string CreateJWTToken() const;
};

class BasicAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  BasicAuthServerMiddlewareFactory(
      const std::string& username, const std::string& password,
      const std::string& secret_key, const std::string& token_allowed_issuer,
      const std::string& token_allowed_audience,
      const std::filesystem::path& token_signature_verify_cert_path,
      const arrow::util::ArrowLogLevel& auth_log_level,
      bool tls_enabled = false, bool mtls_enabled = false);

  /// Set the instance ID for JWT tokens. Must be called before any requests are processed.
  void SetInstanceId(const std::string& instance_id) { instance_id_ = instance_id; }

  /// Set the default role for tokens that lack a 'role' claim (e.g., IdP tokens).
  void SetTokenDefaultRole(const std::string& role) { token_default_role_ = role; }

  /// Set the OAuth base URL for client discovery via Handshake.
  /// When a client sends username="__discover__", the server returns this URL.
  void SetOAuthBaseUrl(const std::string& url) { oauth_base_url_ = url; }

  /// Set authorized email patterns for OIDC user filtering (Enterprise feature).
  /// Accepts a comma-separated string of patterns with wildcard support.
  void SetTokenAuthorizedEmails(const std::string& patterns);

#ifdef GIZMOSQL_ENTERPRISE
  /// Set the JWKS manager for validating externally-issued tokens via JWKS.
  void SetJwksManager(std::shared_ptr<gizmosql::enterprise::JwksManager> manager) {
    jwks_manager_ = std::move(manager);
  }
#endif

  arrow::Status StartCall(const flight::CallInfo& info,
                          const flight::ServerCallContext& context,
                          std::shared_ptr<flight::ServerMiddleware>* middleware) override;

 private:
  std::string username_;
  std::string password_;  // Stores SHA256 hash of password, not plaintext
  std::string secret_key_;
  std::string token_allowed_issuer_;
  std::string token_allowed_audience_;
  std::filesystem::path token_signature_verify_cert_path_;
  std::string token_signature_verify_cert_file_contents_;
  bool token_auth_enabled_ = false;
  bool tls_enabled_ = false;
  bool mtls_enabled_ = false;
  arrow::util::ArrowLogLevel auth_log_level_;
  std::string instance_id_;
  std::string token_default_role_;
  std::vector<std::string> token_authorized_email_patterns_;
  std::string oauth_base_url_;  // OAuth server URL for client discovery

  /// Check if an email matches the authorized email patterns.
  bool IsEmailAuthorized(const std::string& email) const;

#ifdef GIZMOSQL_ENTERPRISE
  std::shared_ptr<gizmosql::enterprise::JwksManager> jwks_manager_;
#endif

  arrow::Result<jwt::decoded_jwt<jwt::traits::kazuho_picojson>>
  VerifyAndDecodeBootstrapToken(const std::string& token,
                                const flight::ServerCallContext& context) const;
};

class BearerAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  explicit BearerAuthServerMiddleware(
      const jwt::decoded_jwt<jwt::traits::kazuho_picojson> decoded_jwt);

  const jwt::decoded_jwt<jwt::traits::kazuho_picojson> GetJWT() const;
  const std::string GetUsername() const;
  const std::string GetRole() const;

  void SendingHeaders(flight::AddCallHeaders* outgoing_headers) override;

  void CallCompleted(const arrow::Status& status) override;

  std::string name() const override;

 private:
  jwt::decoded_jwt<jwt::traits::kazuho_picojson> decoded_jwt_;
};

class BearerAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  explicit BearerAuthServerMiddlewareFactory(
      const std::string& secret_key, const arrow::util::ArrowLogLevel& auth_log_level);

  /// Set the instance ID for JWT validation. Must be called before any requests are processed.
  void SetInstanceId(const std::string& instance_id) { instance_id_ = instance_id; }

  /// Allow tokens issued by other server instances (with the same secret key) to be accepted.
  /// When true, the instance_id check is skipped (useful for load-balanced deployments).
  /// Default is false (strict mode - tokens must be from this instance).
  void SetAllowCrossInstanceTokens(bool allow) { allow_cross_instance_tokens_ = allow; }

  arrow::Status StartCall(const flight::CallInfo& info,
                          const flight::ServerCallContext& context,
                          std::shared_ptr<flight::ServerMiddleware>* middleware) override;

 private:
  std::string secret_key_;
  arrow::util::ArrowLogLevel auth_log_level_;
  std::string instance_id_;
  bool allow_cross_instance_tokens_ = false;

  // Track tokens we've already logged as successfully validated
  mutable std::shared_mutex token_log_mutex_;
  mutable std::unordered_set<std::string> logged_token_ids_;

  arrow::util::ArrowLogLevel GetTokenLogLevel(
      const jwt::decoded_jwt<jwt::traits::kazuho_picojson>& decoded) const;

  arrow::Result<jwt::decoded_jwt<jwt::traits::kazuho_picojson>> VerifyAndDecodeToken(
      const std::string& token, const flight::ServerCallContext& context) const;
};
}  // namespace gizmosql