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

#include "rbac_password.h"

#include <vector>

#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

namespace gizmosql::enterprise::rbac {

namespace {

constexpr const char* kScheme = "pbkdf2-sha256";
constexpr size_t kSaltBytes = 16;
constexpr size_t kHashBytes = 32;

std::string Base64Encode(const unsigned char* data, size_t len) {
  std::string out(4 * ((len + 2) / 3), '\0');
  const int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), data,
                                static_cast<int>(len));
  out.resize(n < 0 ? 0 : static_cast<size_t>(n));
  return out;
}

bool Base64Decode(const std::string& text, std::vector<unsigned char>& out) {
  if (text.empty() || text.size() % 4 != 0) return false;
  out.assign(3 * text.size() / 4, 0);
  const int n =
      EVP_DecodeBlock(out.data(), reinterpret_cast<const unsigned char*>(text.data()),
                      static_cast<int>(text.size()));
  if (n < 0) return false;
  size_t padding = 0;
  if (text.back() == '=') ++padding;
  if (text.size() > 1 && text[text.size() - 2] == '=') ++padding;
  out.resize(static_cast<size_t>(n) - padding);
  return true;
}

bool Derive(const std::string& password, const unsigned char* salt, size_t salt_len,
            int iterations, unsigned char* out, size_t out_len) {
  return PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()), salt,
                           static_cast<int>(salt_len), iterations, EVP_sha256(),
                           static_cast<int>(out_len), out) == 1;
}

}  // namespace

arrow::Result<std::string> HashPassword(const std::string& password, int iterations) {
  if (password.empty()) {
    return arrow::Status::Invalid("GizmoSQL access control: a password cannot be empty");
  }
  if (iterations < 1000) {
    return arrow::Status::Invalid(
        "GizmoSQL access control: too few password hash iterations");
  }
  unsigned char salt[kSaltBytes];
  if (RAND_bytes(salt, sizeof(salt)) != 1) {
    return arrow::Status::IOError(
        "GizmoSQL access control: no secure random source for a salt");
  }
  unsigned char hash[kHashBytes];
  if (!Derive(password, salt, sizeof(salt), iterations, hash, sizeof(hash))) {
    return arrow::Status::IOError("GizmoSQL access control: password hashing failed");
  }
  return std::string(kScheme) + "$" + std::to_string(iterations) + "$" +
         Base64Encode(salt, sizeof(salt)) + "$" + Base64Encode(hash, sizeof(hash));
}

bool VerifyPassword(const std::string& password, const std::string& stored_hash) {
  // pbkdf2-sha256$<iterations>$<salt>$<hash>
  std::vector<std::string> parts;
  size_t start = 0;
  for (size_t i = 0; i <= stored_hash.size(); ++i) {
    if (i == stored_hash.size() || stored_hash[i] == '$') {
      parts.push_back(stored_hash.substr(start, i - start));
      start = i + 1;
    }
  }
  if (parts.size() != 4 || parts[0] != kScheme) return false;
  int iterations = 0;
  try {
    iterations = std::stoi(parts[1]);
  } catch (...) {
    return false;
  }
  if (iterations < 1000 || iterations > 100000000) return false;
  std::vector<unsigned char> salt, expected;
  if (!Base64Decode(parts[2], salt) || !Base64Decode(parts[3], expected)) return false;
  if (expected.size() != kHashBytes) return false;
  unsigned char actual[kHashBytes];
  if (!Derive(password, salt.data(), salt.size(), iterations, actual, sizeof(actual)))
    return false;
  return CRYPTO_memcmp(actual, expected.data(), kHashBytes) == 0;
}

}  // namespace gizmosql::enterprise::rbac
