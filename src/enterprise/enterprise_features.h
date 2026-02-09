// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <memory>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

#include "license_mgr/license_manager.h"

namespace gizmosql::enterprise {

// Feature name constants
constexpr const char* kFeatureInstrumentation = "instrumentation";
constexpr const char* kFeatureKillSession = "kill_session";
constexpr const char* kFeatureCatalogPermissions = "catalog_permissions";
constexpr const char* kFeatureExternalAuth = "external_auth";

/// Singleton class for managing enterprise feature availability
class EnterpriseFeatures {
 public:
  /// Get the singleton instance
  static EnterpriseFeatures& Instance();

  /// Initialize enterprise features with a license file
  /// @param license_file_path Path to the JWT license file (empty = no license)
  /// @return Status indicating success or failure
  arrow::Status Initialize(const std::string& license_file_path);

  /// Check if running in enterprise edition (valid license loaded)
  bool IsEnterpriseEdition() const;

  /// Check if a specific feature is available
  /// @param feature_name The feature to check (e.g., "instrumentation", "kill_session")
  bool IsFeatureAvailable(const std::string& feature_name) const;

  /// Check if instrumentation feature is available
  bool IsInstrumentationAvailable() const;

  /// Check if kill session feature is available
  bool IsKillSessionAvailable() const;

  /// Check if catalog permissions feature is available (per-catalog access control via bootstrap tokens)
  bool IsCatalogPermissionsAvailable() const;

  /// Check if external auth feature is available (JWKS/OIDC SSO authentication)
  bool IsExternalAuthAvailable() const;

  /// Get the license manager (for advanced usage)
  LicenseManager* GetLicenseManager() const { return license_manager_.get(); }

  /// Get copyright banner appropriate for the edition
  std::string GetCopyrightBanner() const;

  /// Get the edition name ("Core" or "Enterprise")
  std::string GetEditionName() const;

  /// Get the error message for when an enterprise feature is requested without license
  static std::string GetLicenseRequiredError(const std::string& feature_name);

 private:
  EnterpriseFeatures();
  ~EnterpriseFeatures() = default;

  // Non-copyable
  EnterpriseFeatures(const EnterpriseFeatures&) = delete;
  EnterpriseFeatures& operator=(const EnterpriseFeatures&) = delete;

  std::unique_ptr<LicenseManager> license_manager_;
  bool initialized_{false};
};

}  // namespace gizmosql::enterprise
