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

#pragma once

// Sensitive-path guard: keeps credential and system files on the server host
// out of reach of SQL, for every role (basic-auth users are always admin).
//
// Enforcement point: DuckDB's DBConfig::file_system. Every file a query touches
// goes through it — read_*() / glob() / COPY / ATTACH / replacement scans /
// extension readers — so the guard sees the path no matter which SQL produced
// it. DuckDB loads and writes its own persistent secrets through a separate
// local file system (FileSystem::GetLocal), so the guard never interferes with
// the secret manager itself.
//
// A path is checked after `~` expansion (DuckDB's own ExpandPath, so a client
// changing home_directory cannot dodge it), made absolute, normalized, and with
// symlinks resolved; it is refused if it is, or is inside, a protected path.
// The refusal happens before the file is touched, so the error is identical
// whether or not the file exists.

#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include <arrow/status.h>
#include <duckdb.hpp>
#include <duckdb/common/virtual_file_system.hpp>
#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
#include <duckdb/common/multi_file/multi_file_list.hpp>
#endif

namespace gizmosql::ddb {

struct SensitivePathOptions {
  /// false turns the guard off entirely (--block-sensitive-paths false).
  bool enabled = true;
  /// Operator-supplied extra paths (--sensitive-paths / GIZMOSQL_SENSITIVE_PATHS).
  std::vector<std::string> extra_paths;
  /// GizmoSQL's own credential files (TLS key, license key, init-SQL file).
  std::vector<std::string> server_files;
  /// Home directory whose credential stores are protected; empty = the
  /// server process's HOME / USERPROFILE.
  std::string home_directory;
};

class SensitivePathPolicy {
 public:
  /// The default protected paths plus the options' extra and server paths.
  explicit SensitivePathPolicy(const SensitivePathOptions& options);

  /// Adds a protected file or directory (anything inside a directory is
  /// protected too). Relative paths are resolved against the working directory.
  /// Thread-safe: the live DuckDB secret_directory is added after start-up.
  void AddProtectedPath(const std::string& path, const std::string& category);

  /// GizmoSQL's own credential file (TLS key, license key, init-SQL file).
  void AddServerCredentialFile(const std::string& path);
  /// The DuckDB secret_directory actually in use (read after init SQL ran).
  void AddSecretDirectory(const std::string& path);

  /// The category of protected path `path` falls under ("system account files",
  /// "DuckDB persistent secrets", ...), or std::nullopt if it may be accessed.
  /// `path` must already be `~`-expanded. Remote URLs (s3://, https://, ...)
  /// are never protected here; file:// is treated as a local path.
  std::optional<std::string> Check(const std::string& path) const;

  /// The client-facing error for a refused path.
  static std::string DeniedMessage(const std::string& path, const std::string& category);

 private:
  struct Entry {
    std::string key;  // normalized comparison key, no trailing separator
    std::string category;
  };
  mutable std::shared_mutex mutex_;
  std::vector<Entry> entries_;
  // Exact paths inside protected locations that stay readable (DuckDB reads
  // /proc/self/cgroup at start-up to find the container's memory limit).
  std::vector<std::string> allowed_keys_;
};

/// If `duckdb_error` is a sensitive-path refusal raised by the guard, the Flight
/// Unauthorized status to send the client, carrying only GizmoSQL's message (not
/// DuckDB's "Permission Error:" prefix or query-position frame). Otherwise
/// std::nullopt, and the caller reports the error as usual.
std::optional<arrow::Status> SensitivePathRefusal(const std::string& duckdb_error);

/// DuckDB VirtualFileSystem that refuses protected paths and hides them from
/// directory listings and glob results.
class SensitivePathGuardFileSystem : public duckdb::VirtualFileSystem {
 public:
  SensitivePathGuardFileSystem(std::shared_ptr<const SensitivePathPolicy> policy,
                               duckdb::unique_ptr<duckdb::FileSystem>&& inner);

  bool DirectoryExists(const duckdb::string& directory,
                       duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  void CreateDirectory(const duckdb::string& directory,
                       duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  void RemoveDirectory(const duckdb::string& directory,
                       duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  void MoveFile(const duckdb::string& source, const duckdb::string& target,
                duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  bool FileExists(const duckdb::string& filename,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  bool IsPipe(const duckdb::string& filename,
              duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  void RemoveFile(const duckdb::string& filename,
                  duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  bool TryRemoveFile(const duckdb::string& filename,
                     duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  void RemoveFiles(const duckdb::vector<duckdb::string>& filenames,
                   duckdb::optional_ptr<duckdb::FileOpener> opener) override;
#if GIZMOSQL_DUCKDB_CHANNEL_LTS
  duckdb::vector<duckdb::OpenFileInfo> Glob(
      const duckdb::string& path, duckdb::FileOpener* opener = nullptr) override;
#endif

 protected:
  duckdb::unique_ptr<duckdb::FileHandle> OpenFileExtended(
      const duckdb::OpenFileInfo& file, duckdb::FileOpenFlags flags,
      duckdb::optional_ptr<duckdb::FileOpener> opener) override;
  bool ListFilesExtended(const duckdb::string& directory,
                         const std::function<void(duckdb::OpenFileInfo& info)>& callback,
                         duckdb::optional_ptr<duckdb::FileOpener> opener) override;
#if !GIZMOSQL_DUCKDB_CHANNEL_LTS
  duckdb::unique_ptr<duckdb::MultiFileList> GlobFilesExtended(
      const duckdb::string& path, const duckdb::FileGlobInput& input,
      duckdb::optional_ptr<duckdb::FileOpener> opener) override;
#endif

 private:
  /// Throws duckdb::PermissionException if `path` is protected.
  void Enforce(const duckdb::string& path,
               duckdb::optional_ptr<duckdb::FileOpener> opener);
  bool IsProtected(const duckdb::string& path,
                   duckdb::optional_ptr<duckdb::FileOpener> opener);
  /// Refuses a glob whose fixed leading directory is protected.
  void EnforceGlobPrefix(const duckdb::string& pattern,
                         duckdb::optional_ptr<duckdb::FileOpener> opener);

  std::shared_ptr<const SensitivePathPolicy> policy_;
};

}  // namespace gizmosql::ddb
