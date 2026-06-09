// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

// Tests for the inline license key support (--license-key / GIZMOSQL_LICENSE_KEY),
// which lets operators pass the literal JWT value instead of a file path, and the
// precedence rule that an inline key wins over a license key file.
//
// The "valid license" cases require a real license JWT. They read it from the
// file pointed to by GIZMOSQL_LICENSE_KEY_FILE (the same env var the enterprise
// test suite already uses) and skip when it isn't set.

#include <gtest/gtest.h>

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include "enterprise_features.h"
#include "license_mgr/license_manager.h"

namespace {

// Read the real license JWT from GIZMOSQL_LICENSE_KEY_FILE, or "" if unset/unreadable.
std::string ReadLicenseFromEnvFile() {
  const char* path = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  if (path == nullptr || path[0] == '\0') {
    return "";
  }
  std::ifstream file(path);
  if (!file.is_open()) {
    return "";
  }
  std::stringstream buffer;
  buffer << file.rdbuf();
  std::string jwt = buffer.str();
  jwt.erase(0, jwt.find_first_not_of(" \t\n\r"));
  jwt.erase(jwt.find_last_not_of(" \t\n\r") + 1);
  return jwt;
}

#define SKIP_IF_NO_LICENSE(jwt)                                                  \
  if ((jwt).empty()) {                                                           \
    GTEST_SKIP() << "No license available (set GIZMOSQL_LICENSE_KEY_FILE to a "  \
                    "valid license JWT to run this test).";                      \
  }

}  // namespace

// ---------------------------------------------------------------------------
// LicenseManager::LoadLicenseFromString — the new inline-value entry point.
// Uses a fresh LicenseManager so these cases don't touch global singleton state.
// ---------------------------------------------------------------------------

TEST(InlineLicenseKey, EmptyStringIsInvalid) {
  auto mgr = gizmosql::enterprise::LicenseManager::Create();
  auto result = mgr->LoadLicenseFromString("");
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(mgr->HasValidLicense());
}

TEST(InlineLicenseKey, WhitespaceOnlyStringIsInvalid) {
  auto mgr = gizmosql::enterprise::LicenseManager::Create();
  auto result = mgr->LoadLicenseFromString("   \n\t  ");
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(mgr->HasValidLicense());
}

TEST(InlineLicenseKey, GarbageJwtIsRejected) {
  auto mgr = gizmosql::enterprise::LicenseManager::Create();
  auto result = mgr->LoadLicenseFromString("not.a.valid.jwt");
  EXPECT_FALSE(result.ok());
  EXPECT_FALSE(mgr->HasValidLicense());
}

TEST(InlineLicenseKey, ValidInlineJwtValidatesLikeFile) {
  const std::string jwt = ReadLicenseFromEnvFile();
  SKIP_IF_NO_LICENSE(jwt);

  auto mgr = gizmosql::enterprise::LicenseManager::Create();
  auto result = mgr->LoadLicenseFromString(jwt);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_TRUE(mgr->HasValidLicense());
}

TEST(InlineLicenseKey, TrailingWhitespaceIsTrimmed) {
  const std::string jwt = ReadLicenseFromEnvFile();
  SKIP_IF_NO_LICENSE(jwt);

  // A value picked up from `$(cat license.jwt)` may carry a trailing newline;
  // it must validate the same as the trimmed value.
  auto mgr = gizmosql::enterprise::LicenseManager::Create();
  auto result = mgr->LoadLicenseFromString(jwt + "\n");
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_TRUE(mgr->HasValidLicense());
}

// ---------------------------------------------------------------------------
// EnterpriseFeatures::Initialize precedence — inline key wins over the file.
// These mutate the EnterpriseFeatures singleton, so each restores it from the
// env file afterward to avoid leaking state into later test suites.
// ---------------------------------------------------------------------------

namespace {
void RestoreSingletonFromEnv() {
  const char* path = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  auto status = gizmosql::enterprise::EnterpriseFeatures::Instance().Initialize(
      path ? path : "", /*license_key=*/"");
  (void)status;
}
}  // namespace

TEST(InlineLicenseKey, InlineKeyWinsOverBogusFile) {
  const std::string jwt = ReadLicenseFromEnvFile();
  SKIP_IF_NO_LICENSE(jwt);

  auto& ent = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto status = ent.Initialize("/nonexistent/path/to/license.jwt", /*license_key=*/jwt);
  EXPECT_TRUE(status.ok()) << status.ToString();
  EXPECT_TRUE(ent.IsEnterpriseEdition());

  RestoreSingletonFromEnv();
}

TEST(InlineLicenseKey, InlineKeyTakesPrecedenceEvenWhenInvalid) {
  const std::string jwt = ReadLicenseFromEnvFile();
  SKIP_IF_NO_LICENSE(jwt);

  // A valid file path is provided, but a (bogus) inline key is also set. Because
  // the inline key wins, initialization must FAIL rather than silently fall back
  // to the file — that's the contract operators rely on.
  const char* valid_file = std::getenv("GIZMOSQL_LICENSE_KEY_FILE");
  auto& ent = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto status = ent.Initialize(valid_file, /*license_key=*/"not.a.valid.jwt");
  EXPECT_FALSE(status.ok());

  RestoreSingletonFromEnv();
}

TEST(InlineLicenseKey, NoLicenseIsCoreEdition) {
  auto& ent = gizmosql::enterprise::EnterpriseFeatures::Instance();
  auto status = ent.Initialize(/*license_file_path=*/"", /*license_key=*/"");
  EXPECT_TRUE(status.ok()) << status.ToString();

  RestoreSingletonFromEnv();
}
