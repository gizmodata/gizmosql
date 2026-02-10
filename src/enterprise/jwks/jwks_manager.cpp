// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "jwks_manager.h"

#include <nlohmann/json.hpp>
#include <sstream>

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.h"

#include "jwt-cpp/jwt.h"
#include "gizmosql_logging.h"

namespace gizmosql::enterprise {

JwksManager::JwksManager(std::string jwks_uri, std::chrono::minutes cache_ttl)
    : jwks_uri_(std::move(jwks_uri)), cache_ttl_(cache_ttl) {}

arrow::Result<std::unique_ptr<JwksManager>> JwksManager::CreateFromIssuer(
    const std::string& issuer, std::chrono::minutes cache_ttl) {
  ARROW_ASSIGN_OR_RAISE(auto jwks_uri, DiscoverJwksUri(issuer));
  GIZMOSQL_LOG(INFO) << "Discovered JWKS URI from issuer '" << issuer
                     << "': " << jwks_uri;
  return std::make_unique<JwksManager>(std::move(jwks_uri), cache_ttl);
}

arrow::Result<std::string> JwksManager::GetKeyForKid(const std::string& kid) {
  // First try with a shared (read) lock
  {
    std::shared_lock read_lock(mutex_);
    if (fetched_at_least_once_) {
      auto elapsed = std::chrono::steady_clock::now() - last_fetch_time_;
      if (elapsed < cache_ttl_) {
        auto it = key_cache_.find(kid);
        if (it != key_cache_.end()) {
          return it->second.pem_key;
        }
      }
    }
  }

  // Cache miss or stale - refresh under exclusive lock
  ARROW_RETURN_NOT_OK(RefreshKeys());

  // Try again after refresh
  {
    std::shared_lock read_lock(mutex_);
    auto it = key_cache_.find(kid);
    if (it != key_cache_.end()) {
      return it->second.pem_key;
    }
  }

  return arrow::Status::KeyError("Key ID (kid) '" + kid +
                                 "' not found in JWKS from " + jwks_uri_);
}

arrow::Result<std::string> JwksManager::GetAlgorithmForKid(const std::string& kid) {
  // First try with a shared (read) lock
  {
    std::shared_lock read_lock(mutex_);
    if (fetched_at_least_once_) {
      auto elapsed = std::chrono::steady_clock::now() - last_fetch_time_;
      if (elapsed < cache_ttl_) {
        auto it = key_cache_.find(kid);
        if (it != key_cache_.end()) {
          return it->second.algorithm;
        }
      }
    }
  }

  // Cache miss or stale - refresh
  ARROW_RETURN_NOT_OK(RefreshKeys());

  {
    std::shared_lock read_lock(mutex_);
    auto it = key_cache_.find(kid);
    if (it != key_cache_.end()) {
      return it->second.algorithm;
    }
  }

  return arrow::Status::KeyError("Key ID (kid) '" + kid +
                                 "' not found in JWKS from " + jwks_uri_);
}

arrow::Status JwksManager::RefreshKeys() {
  std::unique_lock write_lock(mutex_);

  // Double-check: another thread may have refreshed while we waited for the lock
  if (fetched_at_least_once_) {
    auto elapsed = std::chrono::steady_clock::now() - last_fetch_time_;
    if (elapsed < std::chrono::seconds(5)) {
      // Refreshed very recently by another thread, skip
      return arrow::Status::OK();
    }
  }

  return FetchAndParseJwks();
}

arrow::Status JwksManager::FetchAndParseJwks() {
  // Called under exclusive lock
  GIZMOSQL_LOG(DEBUG) << "Fetching JWKS from: " << jwks_uri_;

  arrow::Result<std::string> body_result = HttpGet(jwks_uri_);
  if (!body_result.ok()) {
    return arrow::Status::IOError("Failed to fetch JWKS from " + jwks_uri_ + ": " +
                                  body_result.status().ToString());
  }

  try {
    auto jwks = jwt::parse_jwks(body_result.ValueOrDie());
    std::unordered_map<std::string, CachedKey> new_cache;

    // jwt-cpp's parse_jwks returns a jwks object that we can iterate
    // We need to convert each JWK to a PEM public key
    auto json = nlohmann::json::parse(body_result.ValueOrDie());
    if (!json.contains("keys") || !json["keys"].is_array()) {
      return arrow::Status::Invalid("JWKS response missing 'keys' array");
    }

    for (const auto& key_json : json["keys"]) {
      if (!key_json.contains("kid")) {
        continue;  // Skip keys without kid
      }

      std::string kid = key_json["kid"].get<std::string>();
      std::string kty = key_json.value("kty", "");
      std::string alg = key_json.value("alg", "");

      CachedKey cached;
      cached.algorithm = alg;

      if (kty == "RSA") {
        // Convert RSA JWK to PEM using jwt-cpp
        std::string n = key_json.value("n", "");
        std::string e = key_json.value("e", "");
        if (!n.empty() && !e.empty()) {
          try {
            cached.pem_key = jwt::helper::create_public_key_from_rsa_components(n, e);
            new_cache[kid] = std::move(cached);
          } catch (const std::exception& ex) {
            GIZMOSQL_LOG(WARNING) << "Failed to convert RSA JWK (kid=" << kid
                                  << ") to PEM: " << ex.what();
          }
        }
      } else if (kty == "EC") {
        // Convert EC JWK to PEM using jwt-cpp
        std::string x = key_json.value("x", "");
        std::string y = key_json.value("y", "");
        std::string crv = key_json.value("crv", "");
        if (!x.empty() && !y.empty() && !crv.empty()) {
          try {
            cached.pem_key = jwt::helper::create_public_key_from_ec_components(x, y, crv);
            new_cache[kid] = std::move(cached);
          } catch (const std::exception& ex) {
            GIZMOSQL_LOG(WARNING) << "Failed to convert EC JWK (kid=" << kid
                                  << ") to PEM: " << ex.what();
          }
        }
      } else {
        GIZMOSQL_LOG(DEBUG) << "Skipping JWKS key with unsupported type: " << kty
                            << " (kid=" << kid << ")";
      }
    }

    if (new_cache.empty()) {
      return arrow::Status::Invalid(
          "No usable keys found in JWKS response from " + jwks_uri_);
    }

    key_cache_ = std::move(new_cache);
    last_fetch_time_ = std::chrono::steady_clock::now();
    fetched_at_least_once_ = true;

    GIZMOSQL_LOG(INFO) << "JWKS cache refreshed: " << key_cache_.size()
                       << " keys loaded from " << jwks_uri_;

    return arrow::Status::OK();
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to parse JWKS response: " +
                                  std::string(e.what()));
  }
}

arrow::Result<JwksManager::OidcDiscoveryResult> JwksManager::DiscoverOidcEndpoints(
    const std::string& issuer) {
  // Strip trailing slash from issuer
  std::string base = issuer;
  while (!base.empty() && base.back() == '/') {
    base.pop_back();
  }

  std::string discovery_url = base + "/.well-known/openid-configuration";
  GIZMOSQL_LOG(DEBUG) << "Fetching OIDC discovery document from: " << discovery_url;

  ARROW_ASSIGN_OR_RAISE(auto body, HttpGet(discovery_url));

  try {
    auto json = nlohmann::json::parse(body);
    if (!json.contains("jwks_uri") || !json["jwks_uri"].is_string()) {
      return arrow::Status::Invalid(
          "OIDC discovery document missing 'jwks_uri' field from: " + discovery_url);
    }

    OidcDiscoveryResult result;
    result.jwks_uri = json["jwks_uri"].get<std::string>();

    if (json.contains("authorization_endpoint") && json["authorization_endpoint"].is_string()) {
      result.authorization_endpoint = json["authorization_endpoint"].get<std::string>();
    }
    if (json.contains("token_endpoint") && json["token_endpoint"].is_string()) {
      result.token_endpoint = json["token_endpoint"].get<std::string>();
    }

    return result;
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to parse OIDC discovery document from " +
                                  discovery_url + ": " + e.what());
  }
}

arrow::Result<std::string> JwksManager::DiscoverJwksUri(const std::string& issuer) {
  ARROW_ASSIGN_OR_RAISE(auto result, DiscoverOidcEndpoints(issuer));
  return result.jwks_uri;
}

arrow::Result<std::string> JwksManager::HttpGet(const std::string& url) {
  // Parse the URL to extract scheme, host, and path
  std::string scheme, host, path;
  int port = -1;

  // Simple URL parsing
  size_t scheme_end = url.find("://");
  if (scheme_end == std::string::npos) {
    return arrow::Status::Invalid("Invalid URL (no scheme): " + url);
  }
  scheme = url.substr(0, scheme_end);
  std::string rest = url.substr(scheme_end + 3);

  size_t path_start = rest.find('/');
  if (path_start == std::string::npos) {
    host = rest;
    path = "/";
  } else {
    host = rest.substr(0, path_start);
    path = rest.substr(path_start);
  }

  // Extract port if present
  size_t port_start = host.find(':');
  if (port_start != std::string::npos) {
    try {
      port = std::stoi(host.substr(port_start + 1));
    } catch (...) {
      return arrow::Status::Invalid("Invalid port in URL: " + url);
    }
    host = host.substr(0, port_start);
  }

  std::string base_url = scheme + "://" + host;
  if (port > 0) {
    base_url += ":" + std::to_string(port);
  }

  httplib::Client client(base_url);
  client.set_follow_location(true);
  client.set_connection_timeout(std::chrono::seconds(10));
  client.set_read_timeout(std::chrono::seconds(10));

  auto res = client.Get(path);
  if (!res) {
    auto err = res.error();
    return arrow::Status::IOError("HTTP GET failed for " + url + ": " +
                                  httplib::to_string(err));
  }

  if (res->status != 200) {
    return arrow::Status::IOError("HTTP GET " + url + " returned status " +
                                  std::to_string(res->status));
  }

  return res->body;
}

}  // namespace gizmosql::enterprise
