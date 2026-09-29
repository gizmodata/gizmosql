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

// Sensitive-path guard (--block-sensitive-paths): policy unit tests, plus
// server tests that the guard refuses protected paths for an admin session.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "arrow/api.h"
#include "arrow/flight/sql/client.h"
#include "arrow/testing/gtest_util.h"
#include "test_server_fixture.h"

#include "sensitive_path_guard.h"

namespace fs = std::filesystem;
using arrow::flight::sql::FlightSqlClient;
using gizmosql::ddb::SensitivePathOptions;
using gizmosql::ddb::SensitivePathPolicy;

namespace {

// A scratch directory standing in for $HOME, so no test touches the real one.
class SensitivePathPolicyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::temp_directory_path() /
            ("gizmosql_spg_" +
             std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(root_);
    fs::create_directories(home() / ".ssh");
    fs::create_directories(root_ / "data");
    std::ofstream(root_ / "data" / "ok.csv") << "a\n1\n";
    std::ofstream(home() / ".ssh" / "key") << "x";
    SensitivePathOptions options;
    options.home_directory = home().string();
    options.extra_paths = {(root_ / "extra").string()};
    options.server_files = {(root_ / "server.key").string()};
    policy_ = std::make_unique<SensitivePathPolicy>(options);
  }
  void TearDown() override { fs::remove_all(root_); }

  fs::path home() const { return root_ / "home"; }
  std::optional<std::string> Check(const fs::path& p) const {
    return policy_->Check(p.string());
  }

  fs::path root_;
  std::unique_ptr<SensitivePathPolicy> policy_;
};

}  // namespace

TEST_F(SensitivePathPolicyTest, DefaultCategories) {
  EXPECT_EQ(Check(home() / ".ssh" / "key"), "user credential store");
  EXPECT_EQ(Check(home() / ".aws" / "credentials"), "user credential store");
  EXPECT_EQ(Check(home() / ".duckdb" / "stored_secrets" / "s.duckdb_secret"),
            "DuckDB persistent secrets");
  EXPECT_EQ(Check(root_ / "server.key"), "GizmoSQL server credentials");
  EXPECT_EQ(Check(root_ / "extra" / "any" / "file"), "operator-protected path");
#ifndef _WIN32
  EXPECT_EQ(Check("/etc/passwd"), "system account files");
  EXPECT_EQ(Check("/proc/self/environ"), "process information (/proc)");
  EXPECT_EQ(Check("/run/secrets/token"), "container and Kubernetes secrets");
#endif
}

#ifndef _WIN32
// DuckDB reads /proc/self/cgroup at start-up to size its memory limit (Linux);
// only that exact file is readable, not the rest of /proc.
TEST_F(SensitivePathPolicyTest, OnlyOwnCgroupFileIsReadableInProc) {
  EXPECT_FALSE(Check("/proc/self/cgroup").has_value());
  EXPECT_EQ(Check("/proc/self/environ"), "process information (/proc)");
  EXPECT_EQ(Check("/proc/1/cgroup"), "process information (/proc)");
}
#endif

TEST_F(SensitivePathPolicyTest, OrdinaryPathsAreAllowed) {
  EXPECT_FALSE(Check(root_ / "data" / "ok.csv").has_value());
  EXPECT_FALSE(Check(home() / "notes.txt").has_value());
  EXPECT_FALSE(Check(home() / ".sshfoo").has_value());  // a sibling, not inside .ssh
#ifndef _WIN32
  EXPECT_FALSE(Check("/etc/passwd2").has_value());
  EXPECT_FALSE(Check("/procfs/x").has_value());
#endif
  // Remote objects are not local files.
  EXPECT_FALSE(policy_->Check("s3://bucket/etc/passwd").has_value());
  EXPECT_FALSE(policy_->Check("https://example.com/.ssh/key").has_value());
}

TEST_F(SensitivePathPolicyTest, OtherSpellingsResolveToTheSamePath) {
  const auto key = home() / ".ssh" / "key";
  EXPECT_TRUE(Check(root_ / "data" / ".." / "home" / ".ssh" / "key").has_value());
  EXPECT_TRUE(policy_->Check("file://" + key.generic_string()).has_value());
  // A symlink elsewhere that points at a protected file is still refused.
  std::error_code ec;
  fs::create_symlink(key, root_ / "data" / "innocent.csv", ec);
  if (!ec) EXPECT_TRUE(Check(root_ / "data" / "innocent.csv").has_value());
  // Relative to the working directory.
  const auto cwd = fs::current_path();
  fs::current_path(root_ / "data");
  EXPECT_TRUE(policy_->Check("../home/.ssh/key").has_value());
  fs::current_path(cwd);
#if defined(__APPLE__) || defined(_WIN32)
  // Case-insensitive file systems open this as the same file.
  auto upper = key.generic_string();
  upper.replace(upper.find(".ssh"), 4, ".SSH");
  EXPECT_TRUE(policy_->Check(upper).has_value());
#endif
}

TEST_F(SensitivePathPolicyTest, SecretDirectoryAddedAfterStartup) {
  const auto dir = root_ / "custom_secrets";
  EXPECT_FALSE(Check(dir / "s.duckdb_secret").has_value());
  policy_->AddSecretDirectory(dir.string());
  EXPECT_EQ(Check(dir / "s.duckdb_secret"), "DuckDB persistent secrets");
}

#ifndef _WIN32

namespace {
arrow::Status RunQuery(int port, const std::string& user, const std::string& password,
                       const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto loc, arrow::flight::Location::ForGrpcTcp("localhost", port));
  ARROW_ASSIGN_OR_RAISE(auto client, arrow::flight::FlightClient::Connect(loc, {}));
  ARROW_ASSIGN_OR_RAISE(auto bearer, client->AuthenticateBasicToken({}, user, password));
  arrow::flight::FlightCallOptions co;
  co.headers.push_back(bearer);
  FlightSqlClient sql_client(std::move(client));
  ARROW_ASSIGN_OR_RAISE(auto info, sql_client.Execute(co, sql));
  for (const auto& ep : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto reader, sql_client.DoGet(co, ep.ticket));
    ARROW_ASSIGN_OR_RAISE(auto table, reader->ToTable());
    (void)table;
  }
  return arrow::Status::OK();
}

const fs::path& GuardTestDir() {
  static const fs::path dir = [] {
    auto d = fs::temp_directory_path() / "gizmosql_spg_server";
    fs::remove_all(d);
    fs::create_directories(d / "protected");
    std::ofstream(d / "protected" / "token.txt") << "do-not-read";
    std::ofstream(d / "public.csv") << "a\n1\n";
    return d;
  }();
  return dir;
}
}  // namespace

class SensitivePathServerFixture
    : public gizmosql::testing::ServerTestFixture<SensitivePathServerFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "sensitive_path_guard_test.db",
        .port = 31650,
        .health_port = 31651,
        .username = "admin_user",
        .password = "admin_pass",
        .sensitive_paths = {(GuardTestDir() / "protected").string()},
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SensitivePathServerFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<SensitivePathServerFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SensitivePathServerFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SensitivePathServerFixture>::config_{};

TEST_F(SensitivePathServerFixture, AdminIsRefusedProtectedPaths) {
  ASSERT_TRUE(IsServerReady());
  const auto token = (GuardTestDir() / "protected" / "token.txt").string();
  for (const std::string& sql : {
           "SELECT content FROM read_text('" + token + "')",
           "SELECT * FROM read_csv('" + token + "')",
           std::string("SELECT content FROM read_text('/etc/passwd')"),
       }) {
    auto st = RunQuery(GetPort(), GetUsername(), GetPassword(), sql);
    ASSERT_FALSE(st.ok()) << sql;
    EXPECT_NE(st.ToString().find("GizmoSQL blocked access to"), std::string::npos)
        << st.ToString();
    EXPECT_NE(st.ToString().find("Permission denied"), std::string::npos)
        << st.ToString();
  }
}

TEST_F(SensitivePathServerFixture, OrdinaryFilesStillReadable) {
  ASSERT_TRUE(IsServerReady());
  const auto csv = (GuardTestDir() / "public.csv").string();
  EXPECT_TRUE(RunQuery(GetPort(), GetUsername(), GetPassword(),
                       "SELECT * FROM read_csv('" + csv + "')")
                  .ok());
}

TEST_F(SensitivePathServerFixture, ProtectedEntriesHiddenFromGlob) {
  ASSERT_TRUE(IsServerReady());
  const auto pattern = (GuardTestDir() / "*" / "*").string();
  // A wildcard over the parent must not reveal the protected directory's file:
  // the query raises an error if it does.
  auto st =
      RunQuery(GetPort(), GetUsername(), GetPassword(),
               "SELECT CASE WHEN count(*) > 0 THEN error('protected file listed') END "
               "FROM glob('" +
                   pattern + "') WHERE file LIKE '%token.txt'");
  EXPECT_TRUE(st.ok()) << st.ToString();
}

TEST_F(SensitivePathServerFixture, SecretDirectoryCannotBeChanged) {
  ASSERT_TRUE(IsServerReady());
  auto st = RunQuery(GetPort(), GetUsername(), GetPassword(),
                     "SET secret_directory = '/tmp/elsewhere'");
  ASSERT_FALSE(st.ok());
  EXPECT_NE(st.ToString().find("blocked changing secret_directory"), std::string::npos)
      << st.ToString();
}

class SensitivePathGuardOffFixture
    : public gizmosql::testing::ServerTestFixture<SensitivePathGuardOffFixture> {
 public:
  static gizmosql::testing::TestServerConfig GetConfig() {
    return {
        .database_filename = "sensitive_path_guard_off_test.db",
        .port = 31652,
        .health_port = 31653,
        .username = "admin_user",
        .password = "admin_pass",
        .block_sensitive_paths = false,
        .sensitive_paths = {(GuardTestDir() / "protected").string()},
    };
  }
};

template <>
std::shared_ptr<arrow::flight::sql::FlightSqlServerBase>
    gizmosql::testing::ServerTestFixture<SensitivePathGuardOffFixture>::server_{};
template <>
std::thread
    gizmosql::testing::ServerTestFixture<SensitivePathGuardOffFixture>::server_thread_{};
template <>
std::atomic<bool>
    gizmosql::testing::ServerTestFixture<SensitivePathGuardOffFixture>::server_ready_{
        false};
template <>
gizmosql::testing::TestServerConfig
    gizmosql::testing::ServerTestFixture<SensitivePathGuardOffFixture>::config_{};

TEST_F(SensitivePathGuardOffFixture, GuardOffAllowsAccess) {
  ASSERT_TRUE(IsServerReady());
  const auto token = (GuardTestDir() / "protected" / "token.txt").string();
  EXPECT_TRUE(RunQuery(GetPort(), GetUsername(), GetPassword(),
                       "SELECT content FROM read_text('" + token + "')")
                  .ok());
}

#endif  // !_WIN32
