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

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "client_config.hpp"

namespace {

// Cross-platform env-var helpers: POSIX has setenv/unsetenv; MSVC only
// has _putenv_s ("VAR=value" to set, "VAR=" to unset).
inline void EnvSet(const char* name, const std::string& value) {
#ifdef _WIN32
  _putenv_s(name, value.c_str());
#else
  ::setenv(name, value.c_str(), /*overwrite=*/1);
#endif
}

inline void EnvUnset(const char* name) {
#ifdef _WIN32
  _putenv_s(name, "");
#else
  ::unsetenv(name);
#endif
}

// RAII helper: scope-bounded set/unset so each test starts from a clean
// slate regardless of how the parent shell or earlier tests left the
// relevant env vars.
class ScopedEnv {
 public:
  explicit ScopedEnv(const char* name) : name_(name) {
    if (const char* old = std::getenv(name)) {
      had_old_ = true;
      old_value_ = old;
    }
    Unset();
  }

  ~ScopedEnv() {
    if (had_old_) {
      EnvSet(name_, old_value_);
    } else {
      EnvUnset(name_);
    }
  }

  void Set(const std::string& value) { EnvSet(name_, value); }
  void Unset() { EnvUnset(name_); }

 private:
  const char* name_;
  bool had_old_ = false;
  std::string old_value_;
};

}  // namespace

namespace gizmosql::client {

// GIZMOSQL_USER is the documented client env var; populate it directly.
TEST(ClientConfigEnvTest, UsernameFromGizmosqlUser) {
  ScopedEnv user("GIZMOSQL_USER");
  ScopedEnv username("GIZMOSQL_USERNAME");
  user.Set("alice");

  ClientConfig config;
  ResolveEnvironmentVariables(config);
  EXPECT_EQ(config.username, "alice");
}

// GIZMOSQL_USERNAME is the SERVER's env var; the client now accepts it as
// a fallback so a single env-var setup works for both binaries.
TEST(ClientConfigEnvTest, UsernameFallsBackToGizmosqlUsername) {
  ScopedEnv user("GIZMOSQL_USER");
  ScopedEnv username("GIZMOSQL_USERNAME");
  username.Set("bob");

  ClientConfig config;
  ResolveEnvironmentVariables(config);
  EXPECT_EQ(config.username, "bob");
}

// When BOTH are set the historical client name (GIZMOSQL_USER) wins —
// this preserves prior behavior for anyone who already exports both.
TEST(ClientConfigEnvTest, GizmosqlUserWinsOverGizmosqlUsername) {
  ScopedEnv user("GIZMOSQL_USER");
  ScopedEnv username("GIZMOSQL_USERNAME");
  user.Set("alice");
  username.Set("bob");

  ClientConfig config;
  ResolveEnvironmentVariables(config);
  EXPECT_EQ(config.username, "alice");
}

// An explicit --username (already set on the config) is not clobbered
// by either env var.
TEST(ClientConfigEnvTest, ExplicitUsernameNotOverriddenByEnv) {
  ScopedEnv user("GIZMOSQL_USER");
  ScopedEnv username("GIZMOSQL_USERNAME");
  user.Set("alice");
  username.Set("bob");

  ClientConfig config;
  config.username = "carol";
  ResolveEnvironmentVariables(config);
  EXPECT_EQ(config.username, "carol");
}

// With neither env var set, username stays empty (CLI-only config).
TEST(ClientConfigEnvTest, NoEnvLeavesUsernameEmpty) {
  ScopedEnv user("GIZMOSQL_USER");
  ScopedEnv username("GIZMOSQL_USERNAME");
  // both unset by ScopedEnv ctor

  ClientConfig config;
  ResolveEnvironmentVariables(config);
  EXPECT_EQ(config.username, "");
}

}  // namespace gizmosql::client
