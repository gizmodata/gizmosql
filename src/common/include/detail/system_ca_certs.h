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

#include <cstdlib>
#include <fstream>
#include <optional>
#include <string>

namespace gizmosql {

// Finds the path to the first existing system CA-certificate bundle file, or
// std::nullopt if none is found.
//
// Why this exists: the portable Linux release binaries statically link a
// self-built OpenSSL (see scripts/build_portable_linux.sh) whose compiled-in
// default CA directory (OPENSSLDIR) is a *build-time* path that does not exist
// at runtime. As a result, OpenSSL's default-verify-paths logic finds no trust
// store and every outbound HTTPS verification fails ("SSL server verification
// failed"). To fix this robustly across distros, our cpp-httplib clients call
// `client.set_ca_cert_path(FindSystemCaCertFile())` so the system bundle is used
// regardless of OPENSSLDIR.
//
// Resolution order:
//   1. $SSL_CERT_FILE (operator override; also honored by OpenSSL itself),
//   2. well-known bundle locations for Debian/Ubuntu/Alpine, RHEL/Amazon Linux,
//      OpenSUSE, and macOS (including Homebrew OpenSSL).
inline std::optional<std::string> FindSystemCaCertFile() {
  if (const char* env = std::getenv("SSL_CERT_FILE")) {
    if (env[0] != '\0') {
      std::ifstream f(env);
      if (f.good()) return std::string(env);
    }
  }
  static const char* kPaths[] = {
      "/etc/ssl/certs/ca-certificates.crt",    // Debian, Ubuntu, Alpine
      "/etc/pki/tls/certs/ca-bundle.crt",      // RHEL, CentOS, Fedora, Amazon Linux
      "/etc/ssl/ca-bundle.pem",                // OpenSUSE
      "/etc/pki/tls/cacert.pem",               // OpenELEC
      "/etc/ssl/cert.pem",                     // macOS, Alpine, BSD
      "/usr/local/etc/openssl@3/cert.pem",     // macOS Homebrew (Intel)
      "/opt/homebrew/etc/openssl@3/cert.pem",  // macOS Homebrew (Apple Silicon)
  };
  for (const char* path : kPaths) {
    std::ifstream f(path);
    if (f.good()) return std::string(path);
  }
  return std::nullopt;
}

}  // namespace gizmosql
