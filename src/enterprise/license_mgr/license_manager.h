// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

namespace gizmosql::enterprise {

/// Represents a validated GizmoSQL Enterprise license
struct LicenseInfo {
  std::string license_id;          // jti - unique license identifier
  std::string issuer;              // iss - "GizmoData LLC"
  std::string subject;             // sub - customer email
  std::string audience;            // aud - company name
  std::chrono::system_clock::time_point issued_at;   // iat
  std::chrono::system_clock::time_point expires_at;  // exp
  std::set<std::string> features;  // Licensed features (e.g., "instrumentation", "kill_session")

  bool HasFeature(const std::string& feature) const {
    return features.count(feature) > 0;
  }

  bool IsExpired() const {
    return std::chrono::system_clock::now() > expires_at;
  }
};

/// Manages license validation for GizmoSQL Enterprise features
class LicenseManager {
 public:
  /// Create a LicenseManager instance
  static std::unique_ptr<LicenseManager> Create();

  /// Load and validate a license from a file
  /// @param license_file_path Path to the JWT license file
  /// @return LicenseInfo if valid, or error status if invalid/expired
  arrow::Result<LicenseInfo> LoadLicenseFromFile(const std::string& license_file_path);

  /// Load and validate a license from a JWT string
  /// @param license_jwt The JWT license string
  /// @return LicenseInfo if valid, or error status if invalid/expired
  arrow::Result<LicenseInfo> ValidateLicense(const std::string& license_jwt);

  /// Get the currently loaded license info (if any)
  std::optional<LicenseInfo> GetCurrentLicense() const { return current_license_; }

  /// Check if a specific feature is licensed
  bool IsFeatureLicensed(const std::string& feature) const;

  /// Check if any valid license is loaded
  bool HasValidLicense() const;

  /// Format license info for display
  static std::string FormatLicenseForDisplay(const LicenseInfo& license);

 private:
  LicenseManager() = default;

  std::optional<LicenseInfo> current_license_;
};

}  // namespace gizmosql::enterprise
