// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

// Unit tests for the system CA-bundle probe used to give our cpp-httplib
// HTTPS clients (JWKS/OAuth) an explicit trust store, because the portable
// static OpenSSL's compiled-in CA path doesn't exist at runtime. The
// end-to-end TLS behavior on the actual portable binary is guarded by the
// `--verify-tls` smoke test in CI; this guards the path-resolution logic.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <string>

#include "system_ca_certs.h"

namespace {

// RAII for an env var (set on construct, restore on destruct).
class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    const char* prev = std::getenv(name);
    had_prev_ = prev != nullptr;
    if (had_prev_) prev_ = prev;
    setenv(name, value, /*overwrite=*/1);
  }
  ~ScopedEnv() {
    if (had_prev_) {
      setenv(name_, prev_.c_str(), 1);
    } else {
      unsetenv(name_);
    }
  }

 private:
  const char* name_;
  bool had_prev_ = false;
  std::string prev_;
};

}  // namespace

// SSL_CERT_FILE, when it points at an existing file, takes precedence.
TEST(SystemCaCerts, RespectsSslCertFileEnv) {
  // Create a temp file to stand in for a CA bundle.
  std::string tmp = std::string(std::tmpnam(nullptr)) + "_ca.pem";
  {
    std::ofstream f(tmp);
    f << "# test bundle\n";
  }
  {
    ScopedEnv env("SSL_CERT_FILE", tmp.c_str());
    auto found = gizmosql::FindSystemCaCertFile();
    ASSERT_TRUE(found.has_value());
    EXPECT_EQ(*found, tmp);
  }
  std::remove(tmp.c_str());
}

// A non-existent SSL_CERT_FILE is ignored; the probe falls through to the
// system bundle list.
TEST(SystemCaCerts, IgnoresNonexistentSslCertFileEnv) {
  ScopedEnv env("SSL_CERT_FILE", "/definitely/does/not/exist/ca.pem");
  auto found = gizmosql::FindSystemCaCertFile();
  if (found.has_value()) {
    EXPECT_NE(*found, "/definitely/does/not/exist/ca.pem");
  }
}

// On any normal Linux/macOS host (incl. CI runners) one of the well-known
// bundle paths must exist — this guards against the probe list going stale.
TEST(SystemCaCerts, FindsASystemBundle) {
  // Make sure SSL_CERT_FILE isn't masking the path-list logic.
  const char* prev = std::getenv("SSL_CERT_FILE");
  if (prev) unsetenv("SSL_CERT_FILE");

  auto found = gizmosql::FindSystemCaCertFile();
  ASSERT_TRUE(found.has_value())
      << "no system CA bundle found — the probe list in system_ca_certs.h is "
         "stale for this OS";
  std::ifstream f(*found);
  EXPECT_TRUE(f.good()) << "probe returned a non-readable path: " << *found;

  if (prev) setenv("SSL_CERT_FILE", prev, 1);
}
