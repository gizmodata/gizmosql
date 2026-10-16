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

// Password hashing for access-control users: PBKDF2-HMAC-SHA256 with a random
// 16-byte salt, stored as "pbkdf2-sha256$<iterations>$<salt b64>$<hash b64>".

#pragma once

#include <string>

#include <arrow/result.h>

namespace gizmosql::enterprise::rbac {

// OWASP's current recommendation for PBKDF2-HMAC-SHA256.
constexpr int kDefaultPasswordIterations = 600000;

arrow::Result<std::string> HashPassword(const std::string& password,
                                        int iterations = kDefaultPasswordIterations);

// Constant-time comparison; false for a malformed stored hash.
bool VerifyPassword(const std::string& password, const std::string& stored_hash);

}  // namespace gizmosql::enterprise::rbac
