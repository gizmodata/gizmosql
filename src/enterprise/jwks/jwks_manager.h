// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <chrono>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include <arrow/result.h>
#include <arrow/status.h>

namespace gizmosql::enterprise {

/// Manages JWKS (JSON Web Key Set) fetching, caching, and key lookup for
/// validating externally-issued JWT tokens (e.g., from OIDC identity providers).
///
/// Thread-safe: uses shared_mutex for read-heavy access patterns.
class JwksManager {
 public:
  /// Create a JwksManager that fetches keys from the given JWKS URI.
  /// @param jwks_uri Direct URL to the JWKS endpoint (e.g., https://idp.example.com/.well-known/jwks.json)
  /// @param cache_ttl How long to cache keys before re-fetching (default 5 minutes)
  explicit JwksManager(std::string jwks_uri,
                       std::chrono::minutes cache_ttl = std::chrono::minutes(5));

  /// Create a JwksManager by auto-discovering the JWKS URI from an OIDC issuer.
  /// Fetches {issuer}/.well-known/openid-configuration and extracts jwks_uri.
  /// @param issuer The OIDC issuer URL (e.g., https://accounts.google.com)
  /// @param cache_ttl How long to cache keys before re-fetching
  /// @return A JwksManager configured with the discovered JWKS URI, or error
  static arrow::Result<std::unique_ptr<JwksManager>> CreateFromIssuer(
      const std::string& issuer,
      std::chrono::minutes cache_ttl = std::chrono::minutes(5));

  /// Get the PEM-encoded public key for a given key ID (kid).
  /// If the kid is not in the cache, or the cache is stale, re-fetches JWKS.
  /// @param kid The key ID from the JWT header
  /// @return The PEM-encoded public key string, or error
  arrow::Result<std::string> GetKeyForKid(const std::string& kid);

  /// Get the algorithm name for a given key ID (kid).
  /// @param kid The key ID from the JWT header
  /// @return Algorithm string (e.g., "RS256", "ES256"), or error
  arrow::Result<std::string> GetAlgorithmForKid(const std::string& kid);

  /// Force refresh the JWKS cache, regardless of TTL.
  arrow::Status RefreshKeys();

  /// Get the JWKS URI being used.
  const std::string& GetJwksUri() const { return jwks_uri_; }

 private:
  struct CachedKey {
    std::string pem_key;     // PEM-encoded public key
    std::string algorithm;   // Algorithm (RS256, ES256, etc.)
  };

  /// Fetch JWKS from the URI and parse keys into the cache.
  arrow::Status FetchAndParseJwks();

  /// Discover JWKS URI from OIDC issuer's .well-known/openid-configuration.
  static arrow::Result<std::string> DiscoverJwksUri(const std::string& issuer);

  /// Perform an HTTPS GET request and return the response body.
  static arrow::Result<std::string> HttpGet(const std::string& url);

  std::string jwks_uri_;
  std::chrono::minutes cache_ttl_;

  mutable std::shared_mutex mutex_;
  std::unordered_map<std::string, CachedKey> key_cache_;
  std::chrono::steady_clock::time_point last_fetch_time_;
  bool fetched_at_least_once_ = false;
};

}  // namespace gizmosql::enterprise
