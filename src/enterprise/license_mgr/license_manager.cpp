// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "license_manager.h"
#include "license_public_key.h"

#include <fstream>
#include <iomanip>
#include <sstream>

#include <jwt-cpp/jwt.h>

namespace gizmosql::enterprise {

std::unique_ptr<LicenseManager> LicenseManager::Create() {
  return std::unique_ptr<LicenseManager>(new LicenseManager());
}

arrow::Result<LicenseInfo> LicenseManager::LoadLicenseFromFile(
    const std::string& license_file_path) {
  std::ifstream file(license_file_path);
  if (!file.is_open()) {
    return arrow::Status::Invalid("Cannot open license file: " + license_file_path);
  }

  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string license_jwt = buffer.str();

  // Trim whitespace
  license_jwt.erase(0, license_jwt.find_first_not_of(" \t\n\r"));
  license_jwt.erase(license_jwt.find_last_not_of(" \t\n\r") + 1);

  if (license_jwt.empty()) {
    return arrow::Status::Invalid("License file is empty: " + license_file_path);
  }

  return ValidateLicense(license_jwt);
}

arrow::Result<LicenseInfo> LicenseManager::ValidateLicense(const std::string& license_jwt) {
  try {
    // Decode the JWT first to inspect it
    auto decoded = jwt::decode(license_jwt);

    // Create verifier with RSA-256 and GizmoData public key
    auto verifier = jwt::verify()
                        .allow_algorithm(jwt::algorithm::rs256(kGizmoDataPublicKey, "", "", ""))
                        .with_issuer("GizmoData LLC");

    // Verify signature and claims
    verifier.verify(decoded);

    // Extract license information
    LicenseInfo info;

    // Required claims
    if (!decoded.has_id()) {
      return arrow::Status::Invalid("License missing required claim: jti (license ID)");
    }
    info.license_id = decoded.get_id();

    if (!decoded.has_issuer()) {
      return arrow::Status::Invalid("License missing required claim: iss (issuer)");
    }
    info.issuer = decoded.get_issuer();

    if (!decoded.has_subject()) {
      return arrow::Status::Invalid("License missing required claim: sub (subject/email)");
    }
    info.subject = decoded.get_subject();

    if (!decoded.has_audience()) {
      return arrow::Status::Invalid("License missing required claim: aud (audience/company)");
    }
    auto audiences = decoded.get_audience();
    if (!audiences.empty()) {
      info.audience = *audiences.begin();
    }

    if (!decoded.has_issued_at()) {
      return arrow::Status::Invalid("License missing required claim: iat (issued at)");
    }
    info.issued_at = decoded.get_issued_at();

    if (!decoded.has_expires_at()) {
      return arrow::Status::Invalid("License missing required claim: exp (expires at)");
    }
    info.expires_at = decoded.get_expires_at();

    // Check expiration
    if (info.IsExpired()) {
      return arrow::Status::Invalid("License has expired. Please contact sales@gizmodata.com to renew.");
    }

    // Extract features array
    if (decoded.has_payload_claim("features")) {
      auto features_claim = decoded.get_payload_claim("features");
      auto features_array = features_claim.as_array();
      for (const auto& feature : features_array) {
        info.features.insert(feature.get<std::string>());
      }
    }

    // Store as current license
    current_license_ = info;

    return info;

  } catch (const jwt::error::signature_verification_exception& e) {
    return arrow::Status::Invalid("License signature verification failed: " + std::string(e.what()));
  } catch (const jwt::error::token_verification_exception& e) {
    return arrow::Status::Invalid("License verification failed: " + std::string(e.what()));
  } catch (const std::exception& e) {
    return arrow::Status::Invalid("Failed to parse license: " + std::string(e.what()));
  }
}

bool LicenseManager::IsFeatureLicensed(const std::string& feature) const {
  if (!current_license_.has_value()) {
    return false;
  }
  return current_license_->HasFeature(feature);
}

bool LicenseManager::HasValidLicense() const {
  if (!current_license_.has_value()) {
    return false;
  }
  return !current_license_->IsExpired();
}

std::string LicenseManager::FormatLicenseForDisplay(const LicenseInfo& license) {
  std::ostringstream oss;

  // Format timestamps
  auto format_time = [](const std::chrono::system_clock::time_point& tp) -> std::string {
    auto time_t_val = std::chrono::system_clock::to_time_t(tp);
    std::tm tm_val = *std::localtime(&time_t_val);
    std::ostringstream ss;
    ss << std::put_time(&tm_val, "%Y-%m-%d");
    return ss.str();
  };

  oss << "GizmoSQL Enterprise Edition - Copyright (c) 2026 GizmoData LLC\n";
  oss << " License ID: " << license.license_id << "\n";
  oss << " Licensed to: " << license.audience << " (" << license.subject << ")\n";
  oss << " License issued: " << format_time(license.issued_at) << "\n";
  oss << " License expires: " << format_time(license.expires_at) << "\n";
  oss << " Licensed by: " << license.issuer;

  return oss.str();
}

}  // namespace gizmosql::enterprise
