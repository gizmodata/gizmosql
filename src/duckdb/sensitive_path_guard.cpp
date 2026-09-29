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

#include "sensitive_path_guard.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <system_error>

#include <arrow/flight/types.h>

#ifndef _WIN32
#include <pwd.h>
#include <unistd.h>
#endif

namespace gizmosql::ddb {

namespace {

namespace dd = duckdb;
namespace fs = std::filesystem;

constexpr const char* kSystemAccounts = "system account files";
constexpr const char* kProcFs = "process information (/proc)";
constexpr const char* kContainerSecrets = "container and Kubernetes secrets";
constexpr const char* kUserCredentials = "user credential store";
constexpr const char* kDuckDBSecrets = "DuckDB persistent secrets";
constexpr const char* kServerCredentials = "GizmoSQL server credentials";
constexpr const char* kOperatorPath = "operator-protected path";

// Case-insensitive file systems (macOS, Windows) open /ETC/PASSWD as
// /etc/passwd, so their comparison keys are case-folded.
std::string ComparisonKey(std::string s) {
  std::replace(s.begin(), s.end(), '\\', '/');
  while (s.size() > 1 && s.back() == '/') s.pop_back();
#if defined(__APPLE__) || defined(_WIN32)
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
#endif
  return s;
}

bool StartsWithCaseInsensitive(const std::string& s, const std::string& prefix) {
  return s.size() >= prefix.size() &&
         std::equal(prefix.begin(), prefix.end(), s.begin(), [](char a, char b) {
           return std::tolower(static_cast<unsigned char>(a)) ==
                  std::tolower(static_cast<unsigned char>(b));
         });
}

// "s3://...", "https://...", "gcs://..." are not local files. file:// is.
bool IsRemote(const std::string& path) {
  const auto pos = path.find("://");
  if (pos == std::string::npos || pos == 0) return false;
  return !StartsWithCaseInsensitive(path, "file://");
}

std::string StripFileScheme(const std::string& path) {
  if (!StartsWithCaseInsensitive(path, "file://")) return path;
  std::string rest = path.substr(7);
  if (StartsWithCaseInsensitive(rest, "localhost/")) rest = rest.substr(9);
  return rest;
}

// Every spelling of `raw` a protected entry must be compared against: the
// absolute, lexically normalized path (collapses "." / ".." / "//") and the
// same path with symlinks resolved as far as it exists (/etc -> /private/etc
// on macOS, /var/run -> /run on Linux, a user-planted symlink, ...).
std::vector<std::string> Keys(const std::string& raw) {
  std::vector<std::string> keys;
  if (raw.empty()) return keys;
  std::error_code ec;
  fs::path path(raw);
  fs::path absolute = path.is_absolute() ? path : fs::absolute(path, ec);
  if (ec) absolute = path;
  absolute = absolute.lexically_normal();
  keys.push_back(ComparisonKey(absolute.generic_string()));
  fs::path resolved = fs::weakly_canonical(absolute, ec);
  if (!ec) {
    auto key = ComparisonKey(resolved.generic_string());
    if (key != keys.front()) keys.push_back(std::move(key));
  }
  return keys;
}

bool KeyCovers(const std::string& entry, const std::string& candidate) {
  if (candidate.size() < entry.size()) return false;
  if (candidate.compare(0, entry.size(), entry) != 0) return false;
  if (candidate.size() == entry.size() || entry == "/") return true;
  const char next = candidate[entry.size()];
#ifdef _WIN32
  // NTFS alternate data streams: "C:/Users/x/.git-credentials::$DATA" opens
  // the protected file itself.
  if (next == ':') return true;
#endif
  return next == '/';
}

std::string DefaultHome() {
#ifdef _WIN32
  if (const char* profile = std::getenv("USERPROFILE"); profile && *profile)
    return profile;
#else
  if (const char* home = std::getenv("HOME"); home && *home) return home;
  if (const passwd* pw = getpwuid(getuid()); pw && pw->pw_dir) return pw->pw_dir;
#endif
  return {};
}

// The fixed leading directory of a glob pattern ("/home/*/.ssh/*" -> "/home/").
std::string GlobFixedPrefix(const std::string& pattern) {
  const auto wildcard = pattern.find_first_of("*?[{");
  if (wildcard == std::string::npos) return pattern;
  const auto slash = pattern.find_last_of("/\\", wildcard);
  return slash == std::string::npos ? std::string() : pattern.substr(0, slash + 1);
}

}  // namespace

SensitivePathPolicy::SensitivePathPolicy(const SensitivePathOptions& options) {
#ifndef _WIN32
  for (const char* path : {"/etc/passwd", "/etc/shadow", "/etc/gshadow",
                           "/etc/master.passwd", "/etc/sudoers", "/etc/sudoers.d"}) {
    AddProtectedPath(path, kSystemAccounts);
  }
  AddProtectedPath("/proc", kProcFs);
  // DuckDB reads this at start-up (through the guarded file system) to size its
  // default memory limit from the container's cgroup. It holds only cgroup
  // membership; the rest of /proc stays protected, including other processes'.
  allowed_keys_ = Keys("/proc/self/cgroup");
  // Re-opens the server's own open file descriptors (a device on macOS, a
  // link through /proc/self/fd on Linux).
  AddProtectedPath("/dev/fd", kProcFs);
  AddProtectedPath("/var/run/secrets", kContainerSecrets);
  AddProtectedPath("/run/secrets", kContainerSecrets);
#else
  if (const char* root = std::getenv("SystemRoot"); root && *root) {
    AddProtectedPath(std::string(root) + "/System32/config", kSystemAccounts);
  }
#endif

  const std::string home =
      options.home_directory.empty() ? DefaultHome() : options.home_directory;
  if (!home.empty()) {
    for (const char* rel :
         {".ssh", ".aws", ".azure", ".config/gcloud", ".kube", ".docker/config.json",
          ".netrc", ".pgpass", ".git-credentials", ".gnupg"}) {
      AddProtectedPath(home + "/" + rel, kUserCredentials);
    }
    AddProtectedPath(home + "/.duckdb/stored_secrets", kDuckDBSecrets);
  }

  for (const auto& path : options.server_files) AddServerCredentialFile(path);
  for (const auto& path : options.extra_paths) AddProtectedPath(path, kOperatorPath);
}

void SensitivePathPolicy::AddProtectedPath(const std::string& path,
                                           const std::string& category) {
  auto keys = Keys(StripFileScheme(path));
  std::unique_lock lock(mutex_);
  for (auto& key : keys) {
    const bool present = std::any_of(entries_.begin(), entries_.end(),
                                     [&](const Entry& e) { return e.key == key; });
    if (!present) entries_.push_back({std::move(key), category});
  }
}

void SensitivePathPolicy::AddServerCredentialFile(const std::string& path) {
  if (!path.empty()) AddProtectedPath(path, kServerCredentials);
}

void SensitivePathPolicy::AddSecretDirectory(const std::string& path) {
  if (!path.empty()) AddProtectedPath(path, kDuckDBSecrets);
}

std::optional<std::string> SensitivePathPolicy::Check(const std::string& path) const {
  if (path.empty() || IsRemote(path)) return std::nullopt;
  const auto candidates = Keys(StripFileScheme(path));
  std::shared_lock lock(mutex_);
  for (const auto& candidate : candidates) {
    if (std::find(allowed_keys_.begin(), allowed_keys_.end(), candidate) !=
        allowed_keys_.end()) {
      return std::nullopt;
    }
  }
  for (const auto& candidate : candidates) {
    for (const auto& entry : entries_) {
      if (KeyCovers(entry.key, candidate)) return entry.category;
    }
  }
  return std::nullopt;
}

std::string SensitivePathPolicy::DeniedMessage(const std::string& path,
                                               const std::string& category) {
  return "GizmoSQL blocked access to '" + path + "' (" + category +
         "). GizmoSQL keeps credential and system files on the server host out of reach "
         "of every client, including admins, so a shared server cannot be used to read "
         "the host's secrets. (Server operators: see --block-sensitive-paths)";
}

std::optional<arrow::Status> SensitivePathRefusal(const std::string& duckdb_error) {
  static const std::string kStart = "GizmoSQL blocked access to ";
  static const std::string kEnd = "--block-sensitive-paths)";
  const auto start = duckdb_error.find(kStart);
  if (start == std::string::npos) return std::nullopt;
  const auto end = duckdb_error.find(kEnd, start);
  const std::string message = end == std::string::npos
                                  ? duckdb_error.substr(start)
                                  : duckdb_error.substr(start, end + kEnd.size() - start);
  return arrow::flight::MakeFlightError(arrow::flight::FlightStatusCode::Unauthorized,
                                        "Permission denied: " + message);
}

SensitivePathGuardFileSystem::SensitivePathGuardFileSystem(
    std::shared_ptr<const SensitivePathPolicy> policy,
    dd::unique_ptr<dd::FileSystem>&& inner)
    : dd::VirtualFileSystem(std::move(inner)), policy_(std::move(policy)) {}

bool SensitivePathGuardFileSystem::IsProtected(const dd::string& path,
                                               dd::optional_ptr<dd::FileOpener> opener) {
  // Expand ~ with DuckDB's own rules (home_directory setting), exactly as the
  // local file system will, so changing home_directory cannot dodge the check.
  return policy_->Check(dd::FileSystem::ExpandPath(path, opener)).has_value();
}

void SensitivePathGuardFileSystem::Enforce(const dd::string& path,
                                           dd::optional_ptr<dd::FileOpener> opener) {
  const auto expanded = dd::FileSystem::ExpandPath(path, opener);
  if (auto category = policy_->Check(expanded)) {
    throw dd::PermissionException(SensitivePathPolicy::DeniedMessage(path, *category));
  }
}

void SensitivePathGuardFileSystem::EnforceGlobPrefix(
    const dd::string& pattern, dd::optional_ptr<dd::FileOpener> opener) {
  const auto prefix = GlobFixedPrefix(pattern);
  if (!prefix.empty()) Enforce(prefix, opener);
}

dd::unique_ptr<dd::FileHandle> SensitivePathGuardFileSystem::OpenFileExtended(
    const dd::OpenFileInfo& file, dd::FileOpenFlags flags,
    dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(file.path, opener);
  return dd::VirtualFileSystem::OpenFileExtended(file, flags, opener);
}

bool SensitivePathGuardFileSystem::DirectoryExists(
    const dd::string& directory, dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(directory, opener);
  return dd::VirtualFileSystem::DirectoryExists(directory, opener);
}

void SensitivePathGuardFileSystem::CreateDirectory(
    const dd::string& directory, dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(directory, opener);
  dd::VirtualFileSystem::CreateDirectory(directory, opener);
}

void SensitivePathGuardFileSystem::RemoveDirectory(
    const dd::string& directory, dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(directory, opener);
  dd::VirtualFileSystem::RemoveDirectory(directory, opener);
}

void SensitivePathGuardFileSystem::MoveFile(const dd::string& source,
                                            const dd::string& target,
                                            dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(source, opener);
  Enforce(target, opener);
  dd::VirtualFileSystem::MoveFile(source, target, opener);
}

bool SensitivePathGuardFileSystem::FileExists(const dd::string& filename,
                                              dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(filename, opener);
  return dd::VirtualFileSystem::FileExists(filename, opener);
}

bool SensitivePathGuardFileSystem::IsPipe(const dd::string& filename,
                                          dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(filename, opener);
  return dd::VirtualFileSystem::IsPipe(filename, opener);
}

void SensitivePathGuardFileSystem::RemoveFile(const dd::string& filename,
                                              dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(filename, opener);
  dd::VirtualFileSystem::RemoveFile(filename, opener);
}

bool SensitivePathGuardFileSystem::TryRemoveFile(
    const dd::string& filename, dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(filename, opener);
  return dd::VirtualFileSystem::TryRemoveFile(filename, opener);
}

void SensitivePathGuardFileSystem::RemoveFiles(const dd::vector<dd::string>& filenames,
                                               dd::optional_ptr<dd::FileOpener> opener) {
  for (const auto& filename : filenames) Enforce(filename, opener);
  dd::VirtualFileSystem::RemoveFiles(filenames, opener);
}

bool SensitivePathGuardFileSystem::ListFilesExtended(
    const dd::string& directory,
    const std::function<void(dd::OpenFileInfo& info)>& callback,
    dd::optional_ptr<dd::FileOpener> opener) {
  Enforce(directory, opener);
  // Entries are reported by name; hide the ones that are protected paths
  // (listing a home directory must not reveal .ssh / .aws / ...).
  return dd::VirtualFileSystem::ListFilesExtended(
      directory,
      [&](dd::OpenFileInfo& info) {
        if (!IsProtected(JoinPath(directory, info.path), opener)) callback(info);
      },
      opener);
}

#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
dd::unique_ptr<dd::MultiFileList> SensitivePathGuardFileSystem::GlobFilesExtended(
    const dd::string& path, const dd::FileGlobInput& input,
    dd::optional_ptr<dd::FileOpener> opener) {
  EnforceGlobPrefix(path, opener);
  auto result = dd::VirtualFileSystem::GlobFilesExtended(path, input, opener);
  if (IsRemote(path) || !result) return result;
  // A wildcard can reach into a protected directory from above
  // ("/home/*/.ssh/*"): drop those matches, as if they did not exist.
  dd::vector<dd::OpenFileInfo> allowed;
  for (auto& file : result->GetAllFiles()) {
    if (!IsProtected(file.path, opener)) allowed.push_back(file);
  }
  return dd::make_uniq<dd::SimpleMultiFileList>(std::move(allowed));
}
#else
dd::vector<dd::OpenFileInfo> SensitivePathGuardFileSystem::Glob(const dd::string& path,
                                                                dd::FileOpener* opener) {
  EnforceGlobPrefix(path, opener);
  auto files = dd::VirtualFileSystem::Glob(path, opener);
  if (IsRemote(path)) return files;
  dd::vector<dd::OpenFileInfo> allowed;
  for (auto& file : files) {
    if (!IsProtected(file.path, opener)) allowed.push_back(file);
  }
  return allowed;
}
#endif

}  // namespace gizmosql::ddb
