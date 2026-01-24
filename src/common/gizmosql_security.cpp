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
#include <shared_mutex>

#include "gizmosql_security.h"
#include "request_ctx.h"
#include "enterprise/enterprise_features.h"

namespace fs = std::filesystem;

using arrow::Status;

namespace gizmosql {
const std::string kServerJWTIssuer = "gizmosql";
const int kJWTExpiration = 24 * 3600;
const std::string kValidUsername = "gizmosql_username";
const std::string kTokenUsername = "token";
const std::string kBasicPrefix = "Basic ";
const std::string kBearerPrefix = "Bearer ";
const std::string kAuthHeader = "authorization";
const int kMaxLoggedTokens = 50000;

// Helper function to parse catalog_access claim from JWT token
// Expected format: [{"catalog": "name", "access": "read|write|none"}, ...]
std::optional<std::vector<CatalogAccessRule>> ParseCatalogAccessClaim(
    const jwt::decoded_jwt<jwt::traits::kazuho_picojson>& decoded) {
  if (!decoded.has_payload_claim("catalog_access")) {
    return std::nullopt;
  }

  try {
    auto claim = decoded.get_payload_claim("catalog_access");
    auto json_value = claim.to_json();

    if (!json_value.is<picojson::array>()) {
      GIZMOSQL_LOG(WARNING) << "catalog_access claim is not an array";
      return std::nullopt;
    }

    std::vector<CatalogAccessRule> rules;
    const auto& arr = json_value.get<picojson::array>();

    for (const auto& item : arr) {
      if (!item.is<picojson::object>()) {
        GIZMOSQL_LOG(WARNING) << "catalog_access item is not an object";
        continue;
      }

      const auto& obj = item.get<picojson::object>();

      auto catalog_it = obj.find("catalog");
      auto access_it = obj.find("access");

      if (catalog_it == obj.end() || access_it == obj.end()) {
        GIZMOSQL_LOG(WARNING) << "catalog_access item missing 'catalog' or 'access' field";
        continue;
      }

      if (!catalog_it->second.is<std::string>() || !access_it->second.is<std::string>()) {
        GIZMOSQL_LOG(WARNING) << "catalog_access 'catalog' or 'access' is not a string";
        continue;
      }

      CatalogAccessRule rule;
      rule.catalog = catalog_it->second.get<std::string>();
      const auto& access_str = access_it->second.get<std::string>();

      if (access_str == "write") {
        rule.access = CatalogAccessLevel::kWrite;
      } else if (access_str == "read") {
        rule.access = CatalogAccessLevel::kRead;
      } else if (access_str == "none") {
        rule.access = CatalogAccessLevel::kNone;
      } else {
        GIZMOSQL_LOG(WARNING) << "Unknown access level: " << access_str << " - defaulting to none";
        rule.access = CatalogAccessLevel::kNone;
      }

      rules.push_back(std::move(rule));
    }

    return rules;
  } catch (const std::exception& e) {
    GIZMOSQL_LOG(WARNING) << "Failed to parse catalog_access claim: " << e.what();
    return std::nullopt;
  }
}

// ----------------------------------------
Status SecurityUtilities::FlightServerTlsCertificates(
    const fs::path& cert_path, const fs::path& key_path,
    std::vector<flight::CertKeyPair>* out) {
  GIZMOSQL_LOG(INFO) << "Using TLS Cert file: " << cert_path;
  GIZMOSQL_LOG(INFO) << "Using TLS Key file: " << key_path;

  *out = std::vector<flight::CertKeyPair>();
  try {
    std::ifstream cert_file(cert_path);
    if (!cert_file) {
      return Status::IOError("Could not open certificate: " + cert_path.string());
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();

    std::ifstream key_file(key_path);
    if (!key_file) {
      return Status::IOError("Could not open key: " + key_path.string());
    }
    std::stringstream key;
    key << key_file.rdbuf();

    out->push_back(flight::CertKeyPair{cert.str(), key.str()});
  } catch (const std::ifstream::failure& e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

Status SecurityUtilities::FlightServerMtlsCACertificate(const std::string& cert_path,
                                                        std::string* out) {
  try {
    std::ifstream cert_file(cert_path);
    if (!cert_file) {
      return Status::IOError("Could not open MTLS CA certificate: " + cert_path);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();

    *out = cert.str();
  } catch (const std::ifstream::failure& e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

// Function to look in CallHeaders for a key that has a value starting with prefix and
// return the rest of the value after the prefix.
std::string SecurityUtilities::FindKeyValPrefixInCallHeaders(
    const flight::CallHeaders& incoming_headers, const std::string& key,
    const std::string& prefix) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char& char1, const char& char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  auto iter = incoming_headers.find(key);
  if (iter == incoming_headers.end()) {
    return "";
  }
  const std::string val(iter->second);
  if (val.size() > prefix.length()) {
    if (std::equal(val.begin(), val.begin() + prefix.length(), prefix.begin(),
                   char_compare)) {
      return val.substr(prefix.length());
    }
  }
  return "";
}

Status SecurityUtilities::GetAuthHeaderType(const flight::CallHeaders& incoming_headers,
                                            std::string* out) {
  if (!FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix)
           .empty()) {
    *out = "Basic";
  } else if (!FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBearerPrefix)
                  .empty()) {
    *out = "Bearer";
  } else {
    return Status::IOError("Invalid Authorization Header type!");
  }
  return Status::OK();
}

void SecurityUtilities::ParseBasicHeader(const flight::CallHeaders& incoming_headers,
                                         std::string& username, std::string& password) {
  std::string encoded_credentials =
      FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
  std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
  std::getline(decoded_stream, username, ':');
  std::getline(decoded_stream, password, ':');
}

std::string SecurityUtilities::HMAC_SHA256(const std::string& key,
                                           const std::string& data) {
  unsigned char hash[EVP_MAX_MD_SIZE];
  unsigned int hash_len = 0;

  HMAC(EVP_sha256(), key.c_str(), static_cast<int>(key.length()),
       reinterpret_cast<const unsigned char*>(data.c_str()), data.length(), hash,
       &hash_len);

  std::stringstream ss;
  for (unsigned int i = 0; i < hash_len; i++) {
    ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
  }
  return ss.str();
}

// ----------------------------------------
BasicAuthServerMiddleware::BasicAuthServerMiddleware(const std::string& username,
                                                     const std::string& role,
                                                     const std::string& auth_method,
                                                     const std::string& secret_key,
                                                     const std::string& instance_id,
                                                     std::optional<std::string> catalog_access_json)
    : username_(username),
      role_(role),
      auth_method_(auth_method),
      secret_key_(secret_key),
      instance_id_(instance_id),
      catalog_access_json_(std::move(catalog_access_json)) {}

void BasicAuthServerMiddleware::SendingHeaders(flight::AddCallHeaders* outgoing_headers) {
  auto token = CreateJWTToken();
  outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + token);
}

void BasicAuthServerMiddleware::CallCompleted(const Status& status) {}

std::string BasicAuthServerMiddleware::name() const {
  return "BasicAuthServerMiddleware";
}

std::string BasicAuthServerMiddleware::CreateJWTToken() const {
  auto builder =
      jwt::create()
          .set_issuer(std::string(kServerJWTIssuer))
          .set_type("JWT")
          .set_id("gizmosql-server-" +
                  boost::uuids::to_string(boost::uuids::random_generator()()))
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() +
                          std::chrono::seconds{kJWTExpiration})
          .set_payload_claim("sub", jwt::claim(username_))
          .set_payload_claim("role", jwt::claim(role_))
          .set_payload_claim("auth_method", jwt::claim(auth_method_))
          .set_payload_claim("instance_id", jwt::claim(instance_id_))
          .set_payload_claim(
              "session_id",
              jwt::claim(boost::uuids::to_string(boost::uuids::random_generator()())));

  // Include catalog_access claim if present (propagated from bootstrap token)
  if (catalog_access_json_.has_value()) {
    picojson::value json_val;
    std::string err = picojson::parse(json_val, catalog_access_json_.value());
    if (err.empty()) {
      builder.set_payload_claim("catalog_access", jwt::claim(json_val));
    }
  }

  return builder.sign(jwt::algorithm::hs256{secret_key_});
}

// ----------------------------------------
BasicAuthServerMiddlewareFactory::BasicAuthServerMiddlewareFactory(
    const std::string& username, const std::string& password,
    const std::string& secret_key, const std::string& token_allowed_issuer,
    const std::string& token_allowed_audience,
    const std::filesystem::path& token_signature_verify_cert_path,
    const arrow::util::ArrowLogLevel& auth_log_level,
    bool tls_enabled, bool mtls_enabled)
    : username_(username),
      password_(SecurityUtilities::HMAC_SHA256(secret_key, password)),  // Store HMAC-hashed password
      secret_key_(secret_key),
      token_allowed_issuer_(token_allowed_issuer),
      token_allowed_audience_(token_allowed_audience),
      token_signature_verify_cert_path_(token_signature_verify_cert_path),
      tls_enabled_(tls_enabled),
      mtls_enabled_(mtls_enabled),
      auth_log_level_(auth_log_level) {
  if (username_ == kTokenUsername) {
    throw std::runtime_error("You cannot use username: '" + kTokenUsername +
                             "' for basic authentication, because it is reserved for JWT "
                             "token-based authentication");
  }

  if (!token_allowed_issuer_.empty() && !token_allowed_audience_.empty() &&
      !token_signature_verify_cert_path_.empty()) {
    // Load the cert file into a private string member
    if (!token_signature_verify_cert_path_.empty()) {
      std::ifstream cert_file(token_signature_verify_cert_path_);
      if (!cert_file) {
        // Raise error here, can't return from constructor
        throw std::runtime_error("Could not open certificate file: " +
                                 token_signature_verify_cert_path_.string());
      } else {
        std::stringstream cert;
        cert << cert_file.rdbuf();
        token_signature_verify_cert_file_contents_ = cert.str();
      }
    }
    token_auth_enabled_ = true;
    GIZMOSQL_LOG(INFO) << "Token auth is enabled on the server - Allowed Issuer: '"
                       << token_allowed_issuer_ << "' - Allowed Audience: '"
                       << token_allowed_audience_ << "' - Signature Verify Cert Path: '"
                       << token_signature_verify_cert_path_.string() << "'";
  }
}

Status BasicAuthServerMiddlewareFactory::StartCall(
    const flight::CallInfo& info, const flight::ServerCallContext& context,
    std::shared_ptr<flight::ServerMiddleware>* middleware) {
  std::string auth_header_type;

  auto incoming_headers = context.incoming_headers();

  // Extract user-agent header for client identification
  auto user_agent_iter = incoming_headers.find("user-agent");
  if (user_agent_iter != incoming_headers.end()) {
    tl_request_ctx.user_agent = std::string(user_agent_iter->second);
  } else {
    tl_request_ctx.user_agent = std::nullopt;
  }

  // Extract peer identity (mTLS client certificate CN)
  auto peer_identity = context.peer_identity();
  if (!peer_identity.empty()) {
    tl_request_ctx.peer_identity = peer_identity;
  } else {
    tl_request_ctx.peer_identity = std::nullopt;
  }

  // Determine connection protocol
  if (mtls_enabled_ && !peer_identity.empty()) {
    tl_request_ctx.connection_protocol = "mtls";
  } else if (tls_enabled_) {
    tl_request_ctx.connection_protocol = "tls";
  } else {
    tl_request_ctx.connection_protocol = "plaintext";
  }

  ARROW_RETURN_NOT_OK(
      SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
  if (auth_header_type == "Basic") {
    std::string username;
    std::string password;

    SecurityUtilities::ParseBasicHeader(incoming_headers, username, password);

    // If the username has "};PWD={" in it, it is from the Flight SQL ODBC driver -
    // we need to split it into username and password.
    if (username.find("};PWD={") != std::string::npos) {
      std::string username_pwd = username;
      std::string delimiter = "};PWD={";
      size_t pos = 0;
      while ((pos = username_pwd.find(delimiter)) != std::string::npos) {
        username = username_pwd.substr(0, pos);
        username_pwd.erase(0, pos + delimiter.length());
      }
      password = username_pwd;
    }

    if (username.empty() or password.empty()) {
      return MakeFlightError(flight::FlightStatusCode::Unauthenticated,
                             "No Username and/or Password supplied");
    }

    if (username != kTokenUsername) {
      // HMAC-hash the incoming password with secret_key and compare with stored hash
      std::string password_hash = SecurityUtilities::HMAC_SHA256(secret_key_, password);
      if ((username == username_) && (password_hash == password_)) {
        *middleware = std::make_shared<BasicAuthServerMiddleware>(username, "admin",
                                                                  "Basic", secret_key_,
                                                                  instance_id_);
        GIZMOSQL_LOGKV_DYNAMIC(
            auth_log_level_,
            "User: " + username + " (peer " + context.peer() +
                ") - Successfully Basic authenticated via Username / Password",
            {"user", username}, {"peer", context.peer()}, {"kind", "authentication"},
            {"authentication_type", "basic"},
            {"authentication_method", "username/password"}, {"result", "success"});
      } else {
        GIZMOSQL_LOGKV(WARNING,
                       "User: " + username + " (peer " + context.peer() +
                           ") - Failed Basic authentication via Username / Password - "
                           "reason: user provided invalid credentials",
                       {"user", username}, {"peer", context.peer()},
                       {"kind", "authentication"}, {"authentication_type", "basic"},
                       {"result", "failure"}, {"reason", "invalid_credentials"});
        return MakeFlightError(flight::FlightStatusCode::Unauthenticated,
                               "Invalid credentials");
      }
    }
    // If the username is "token" - it is assumed that the user is using token auth - use the password field as the bootstrap token
    else {
      if (!token_auth_enabled_) {
        return MakeFlightError(flight::FlightStatusCode::Unauthenticated,
                               "Token auth is not enabled on the server");
      }
      ARROW_ASSIGN_OR_RAISE(auto bootstrap_decoded_token,
                            VerifyAndDecodeBootstrapToken(password, context));
      // Extract catalog_access claim as JSON string if present
      std::optional<std::string> catalog_access_json;
      if (bootstrap_decoded_token.has_payload_claim("catalog_access")) {
        auto claim = bootstrap_decoded_token.get_payload_claim("catalog_access");
        catalog_access_json = claim.to_json().serialize();
      }
      *middleware = std::make_shared<BasicAuthServerMiddleware>(
          bootstrap_decoded_token.get_subject(),
          bootstrap_decoded_token.get_payload_claim("role").as_string(), "BootstrapToken",
          secret_key_, instance_id_, catalog_access_json);
    }
  }
  return Status::OK();
}

arrow::Result<jwt::decoded_jwt<jwt::traits::kazuho_picojson>>
BasicAuthServerMiddlewareFactory::VerifyAndDecodeBootstrapToken(
    const std::string& token, const flight::ServerCallContext& context) const {
  if (token.empty()) {
    return Status::Invalid("Bearer Token is empty");
  }

  try {
    auto decoded = jwt::decode(token);

    const auto iss = decoded.get_issuer();

    auto verifier = jwt::verify();
    if (iss == token_allowed_issuer_) {
      verifier = verifier
                     .allow_algorithm(jwt::algorithm::rs256(
                         token_signature_verify_cert_file_contents_, "", "", ""))
                     .with_issuer(std::string(token_allowed_issuer_))
                     .with_audience(token_allowed_audience_);
    } else {
      GIZMOSQL_LOGKV(
          WARNING,
          "peer=" + context.peer() +
              " - Bootstrap Bearer Token has an invalid 'iss' claim value of: " + iss +
              " - token_claims=(id=" + decoded.get_id() +
              " sub=" + decoded.get_subject() + " iss=" + decoded.get_issuer() + ")",
          {"peer", context.peer()}, {"kind", "authentication"},
          {"authentication_type", "bearer"}, {"result", "failure"},
          {"reason", "invalid_issuer"}, {"token_id", decoded.get_id()},
          {"token_sub", decoded.get_subject()}, {"token_iss", decoded.get_issuer()});
      return Status::Invalid("Invalid token issuer");
    }

    verifier.verify(decoded);

    if (!decoded.has_payload_claim("role")) {
      return Status::Invalid("Bootstrap Bearer Token MUST have a 'role' claim");
    }

    // Check if token has catalog_access claim - this requires enterprise license
    if (decoded.has_payload_claim("catalog_access")) {
      if (!gizmosql::enterprise::EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
        GIZMOSQL_LOGKV(WARNING,
                       "peer=" + context.peer() +
                           " - Bootstrap Token contains 'catalog_access' claim but "
                           "per-catalog permissions is an Enterprise feature",
                       {"peer", context.peer()}, {"kind", "authentication"},
                       {"authentication_type", "bearer"}, {"result", "failure"},
                       {"reason", "enterprise_feature_required"});
        return MakeFlightError(
            flight::FlightStatusCode::Unauthenticated,
            "Per-catalog permissions (catalog_access claim) is an Enterprise feature. "
            "Please obtain an Enterprise license or remove the catalog_access claim from your token.");
      }
    }

    GIZMOSQL_LOGKV_DYNAMIC(
        auth_log_level_,
        "peer=" + context.peer() +
            " - Bootstrap Bearer Token was validated successfully" +
            " - token_claims=(id=" + decoded.get_id() + " sub=" + decoded.get_subject() +
            " iss=" + decoded.get_issuer() +
            " role=" + decoded.get_payload_claim("role").as_string() + ")",
        {"peer", context.peer()}, {"kind", "authentication"},
        {"authentication_type", "bearer"}, {"result", "success"},
        {"token_id", decoded.get_id()}, {"token_sub", decoded.get_subject()},
        {"token_role", decoded.get_payload_claim("role").as_string()},
        {"token_iss", decoded.get_issuer()});

    return decoded;
  } catch (const std::exception& e) {
    auto error_message = e.what();
    GIZMOSQL_LOGKV(WARNING,
                   "peer=" + context.peer() +
                       " - Bootstrap Bearer Token verification failed with exception: " +
                       error_message,
                   {"peer", context.peer()}, {"kind", "authentication"},
                   {"authentication_type", "bearer"}, {"result", "failure"},
                   {"reason", error_message});

    return Status::Invalid("Bootstrap Token verification failed with error: " +
                           std::string(error_message));
  }
}

// ----------------------------------------
BearerAuthServerMiddleware::BearerAuthServerMiddleware(
    const jwt::decoded_jwt<jwt::traits::kazuho_picojson> decoded_jwt)
    : decoded_jwt_(decoded_jwt) {}

const jwt::decoded_jwt<jwt::traits::kazuho_picojson> BearerAuthServerMiddleware::GetJWT()
    const {
  return decoded_jwt_;
}

const std::string BearerAuthServerMiddleware::GetUsername() const {
  return decoded_jwt_.get_subject();
}

const std::string BearerAuthServerMiddleware::GetRole() const {
  return decoded_jwt_.get_payload_claim("role").as_string();
}

void BearerAuthServerMiddleware::SendingHeaders(
    flight::AddCallHeaders* outgoing_headers) {
  outgoing_headers->AddHeader("x-username", GetUsername());
  outgoing_headers->AddHeader("x-role", GetRole());
}

void BearerAuthServerMiddleware::CallCompleted(const Status& status) {
  // Clear on completion to avoid leakage across threads
  tl_request_ctx = {};
}

std::string BearerAuthServerMiddleware::name() const {
  return "BearerAuthServerMiddleware";
}

// ----------------------------------------
BearerAuthServerMiddlewareFactory::BearerAuthServerMiddlewareFactory(
    const std::string& secret_key, const arrow::util::ArrowLogLevel& auth_log_level)
    : secret_key_(secret_key), auth_log_level_(auth_log_level) {}

arrow::util::ArrowLogLevel BearerAuthServerMiddlewareFactory::GetTokenLogLevel(
    const jwt::decoded_jwt<jwt::traits::kazuho_picojson>& decoded) const {
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_DEBUG;
  const std::string& token_id = decoded.get_id();
  if (token_id.empty()) {
    return level;
  }

  {
    std::shared_lock read_lock(token_log_mutex_);
    if (logged_token_ids_.find(token_id) != logged_token_ids_.end()) {
      return level;  // already seen → DEBUG
    }
  }

  {
    std::unique_lock write_lock(token_log_mutex_);
    auto [it, inserted] = logged_token_ids_.insert(token_id);
    if (!inserted) {
      return level;  // someone else raced and inserted it
    }

    if (logged_token_ids_.size() > kMaxLoggedTokens) {
      logged_token_ids_.clear();
      logged_token_ids_.insert(token_id);
    }

    return auth_log_level_;
  }
}

arrow::Result<jwt::decoded_jwt<jwt::traits::kazuho_picojson>>
BearerAuthServerMiddlewareFactory::VerifyAndDecodeToken(
    const std::string& token, const flight::ServerCallContext& context) const {
  if (token.empty()) {
    return Status::Invalid("Bearer Token is empty");
  }

  try {
    auto decoded = jwt::decode(token);

    const auto iss = decoded.get_issuer();

    auto verifier = jwt::verify();
    if (iss == kServerJWTIssuer) {
      verifier = verifier.allow_algorithm(jwt::algorithm::hs256{secret_key_})
                     .with_issuer(std::string(kServerJWTIssuer));
    } else {
      GIZMOSQL_LOGKV(
          WARNING,
          "peer=" + context.peer() +
              " - Bearer Token has an invalid 'iss' claim value of: " + iss +
              " - token_claims=(id=" + decoded.get_id() +
              " sub=" + decoded.get_subject() + " iss=" + decoded.get_issuer() + ")",
          {"peer", context.peer()}, {"kind", "authentication"},
          {"authentication_type", "bearer"}, {"result", "failure"},
          {"reason", "invalid_issuer"}, {"token_id", decoded.get_id()},
          {"token_sub", decoded.get_subject()}, {"token_iss", decoded.get_issuer()});
      return Status::Invalid("Invalid token issuer");
    }

    verifier.verify(decoded);

    // Validate instance_id: token must be from this server instance
    if (!instance_id_.empty() && decoded.has_payload_claim("instance_id")) {
      auto token_instance_id = decoded.get_payload_claim("instance_id").as_string();
      if (token_instance_id != instance_id_) {
        GIZMOSQL_LOGKV(
            WARNING,
            "peer=" + context.peer() +
                " - Bearer Token instance_id mismatch: token was issued by instance " +
                token_instance_id + " but this is instance " + instance_id_,
            {"peer", context.peer()}, {"kind", "authentication"},
            {"authentication_type", "bearer"}, {"result", "failure"},
            {"reason", "instance_id_mismatch"}, {"token_id", decoded.get_id()},
            {"token_sub", decoded.get_subject()}, {"token_instance_id", token_instance_id},
            {"server_instance_id", instance_id_});
        return MakeFlightError(
            flight::FlightStatusCode::Unauthenticated,
            "Session not associated with this server instance (" + instance_id_ +
                "). Please reconnect to establish a new session.");
      }
    }

    auto token_log_level = GetTokenLogLevel(decoded);

    GIZMOSQL_LOGKV_DYNAMIC(
        token_log_level,
        "peer=" + context.peer() + " - Bearer Token was validated successfully" +
            " - token_claims=(id=" + decoded.get_id() + " sub=" + decoded.get_subject() +
            " iss=" + decoded.get_issuer() + ")",
        {"peer", context.peer()}, {"kind", "authentication"},
        {"authentication_type", "bearer"}, {"result", "success"},
        {"token_id", decoded.get_id()}, {"token_sub", decoded.get_subject()},
        {"token_iss", decoded.get_issuer()});

    return decoded;
  } catch (const std::exception& e) {
    auto error_message = e.what();
    GIZMOSQL_LOGKV(
        WARNING,
        "peer=" + context.peer() +
            " - Bearer Token verification failed with exception: " + error_message,
        {"peer", context.peer()}, {"kind", "authentication"},
        {"authentication_type", "bearer"}, {"result", "failure"},
        {"reason", error_message});

    return Status::Invalid("Token verification failed with error: " +
                           std::string(error_message));
  }
}

Status BearerAuthServerMiddlewareFactory::StartCall(
    const flight::CallInfo& info, const flight::ServerCallContext& context,
    std::shared_ptr<flight::ServerMiddleware>* middleware) {
  auto incoming_headers = context.incoming_headers();
  if (const std::pair<flight::CallHeaders::const_iterator,
                      flight::CallHeaders::const_iterator>& iter_pair =
          incoming_headers.equal_range(kAuthHeader);
      iter_pair.first != iter_pair.second) {
    std::string auth_header_type;
    ARROW_RETURN_NOT_OK(
        SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
    if (auth_header_type == "Bearer") {
      std::string bearer_token = SecurityUtilities::FindKeyValPrefixInCallHeaders(
          incoming_headers, kAuthHeader, kBearerPrefix);
      ARROW_ASSIGN_OR_RAISE(auto decoded_jwt,
                            VerifyAndDecodeToken(bearer_token, context));

      *middleware = std::make_shared<BearerAuthServerMiddleware>(decoded_jwt);

      // Update our thread local context
      tl_request_ctx.username = decoded_jwt.get_subject();
      tl_request_ctx.role = decoded_jwt.get_payload_claim("role").as_string();
      tl_request_ctx.peer = context.peer();
      tl_request_ctx.session_id = decoded_jwt.get_payload_claim("session_id").as_string();
      tl_request_ctx.auth_method = decoded_jwt.get_payload_claim("auth_method").as_string();
      tl_request_ctx.catalog_access = ParseCatalogAccessClaim(decoded_jwt);
    }
  }
  return Status::OK();
}
}  // namespace gizmosql