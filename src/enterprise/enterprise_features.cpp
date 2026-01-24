// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "enterprise_features.h"

#include <chrono>
#include <ctime>
#include <sstream>

namespace gizmosql::enterprise {

EnterpriseFeatures::EnterpriseFeatures()
    : license_manager_(LicenseManager::Create()) {}

EnterpriseFeatures& EnterpriseFeatures::Instance() {
  static EnterpriseFeatures instance;
  return instance;
}

arrow::Status EnterpriseFeatures::Initialize(const std::string& license_file_path) {
  initialized_ = true;

  if (license_file_path.empty()) {
    // No license file provided - running as Core edition
    return arrow::Status::OK();
  }

  // Load and validate license
  auto result = license_manager_->LoadLicenseFromFile(license_file_path);
  if (!result.ok()) {
    return result.status();
  }

  return arrow::Status::OK();
}

bool EnterpriseFeatures::IsEnterpriseEdition() const {
  return license_manager_ && license_manager_->HasValidLicense();
}

bool EnterpriseFeatures::IsFeatureAvailable(const std::string& feature_name) const {
  if (!IsEnterpriseEdition()) {
    return false;
  }
  return license_manager_->IsFeatureLicensed(feature_name);
}

bool EnterpriseFeatures::IsInstrumentationAvailable() const {
  return IsFeatureAvailable(kFeatureInstrumentation);
}

bool EnterpriseFeatures::IsKillSessionAvailable() const {
  return IsFeatureAvailable(kFeatureKillSession);
}

bool EnterpriseFeatures::IsCatalogPermissionsAvailable() const {
  return IsFeatureAvailable(kFeatureCatalogPermissions);
}

std::string EnterpriseFeatures::GetCopyrightBanner() const {
  auto now = std::chrono::system_clock::now();
  std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
  std::tm* localTime = std::localtime(&currentTime);
  int year = 1900 + localTime->tm_year;

  std::ostringstream oss;

  if (IsEnterpriseEdition()) {
    // Enterprise edition with valid license
    auto license = license_manager_->GetCurrentLicense();
    if (license.has_value()) {
      oss << LicenseManager::FormatLicenseForDisplay(license.value());
    }
  } else {
    // Core edition (no license)
    oss << "GizmoSQL Core - Copyright (c) " << year << " GizmoData LLC\n";
    oss << " Licensed under the Apache License, Version 2.0\n";
    oss << " https://www.apache.org/licenses/LICENSE-2.0";
  }

  return oss.str();
}

std::string EnterpriseFeatures::GetEditionName() const {
  if (IsEnterpriseEdition()) {
    return "Enterprise";
  }
  return "Core";
}

std::string EnterpriseFeatures::GetLicenseRequiredError(const std::string& feature_name) {
  std::ostringstream oss;
  oss << "Error: " << feature_name << " is a commercially licensed enterprise feature.\n";
  oss << "       Please provide a valid license key file via --license-key-file\n";
  oss << "       or contact GizmoData sales at sales@gizmodata.com to obtain a license.";
  return oss.str();
}

}  // namespace gizmosql::enterprise
