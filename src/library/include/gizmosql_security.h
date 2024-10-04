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

#include <filesystem>
#include <arrow/flight/sql/server.h>
#include <arrow/flight/server_auth.h>
#include <arrow/flight/middleware.h>
#include <arrow/flight/server_middleware.h>
#include <arrow/util/base64.h>
#include <sstream>
#include <iostream>
#include <fstream>
#include "jwt-cpp/jwt.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "flight_sql_fwd.h"

namespace gizmosql {

class SecurityUtilities {
 public:
  static arrow::Status FlightServerTlsCertificates(const std::filesystem::path &cert_path,
                                                   const std::filesystem::path &key_path,
                                                   std::vector<flight::CertKeyPair> *out);

  static arrow::Status FlightServerMtlsCACertificate(const std::string &cert_path,
                                                     std::string *out);

  static std::string FindKeyValPrefixInCallHeaders(
      const flight::CallHeaders &incoming_headers, const std::string &key,
      const std::string &prefix);

  static arrow::Status GetAuthHeaderType(const flight::CallHeaders &incoming_headers,
                                         std::string *out);

  static void ParseBasicHeader(const flight::CallHeaders &incoming_headers,
                               std::string &username, std::string &password);
};

class HeaderAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  HeaderAuthServerMiddleware(const std::string &username, const std::string &secret_key);

  void SendingHeaders(flight::AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const arrow::Status &status) override;

  std::string name() const override;

 private:
  std::string username_;
  std::string secret_key_;

  std::string CreateJWTToken() const;
};

class HeaderAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  HeaderAuthServerMiddlewareFactory(const std::string &username,
                                    const std::string &password,
                                    const std::string &secret_key);

  arrow::Status StartCall(const flight::CallInfo &info,
                          const flight::CallHeaders &incoming_headers,
                          std::shared_ptr<flight::ServerMiddleware> *middleware) override;

 private:
  std::string username_;
  std::string password_;
  std::string secret_key_;
};

class BearerAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  explicit BearerAuthServerMiddleware(const std::string &secret_key,
                                      const flight::CallHeaders &incoming_headers,
                                      std::optional<bool> *isValid);

  void SendingHeaders(flight::AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const arrow::Status &status) override;

  std::string name() const override;

 private:
  std::string secret_key_;
  flight::CallHeaders incoming_headers_;
  std::optional<bool> *isValid_;

  bool VerifyToken(const std::string &token) const;
};

class BearerAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  explicit BearerAuthServerMiddlewareFactory(const std::string &secret_key);

  arrow::Status StartCall(const flight::CallInfo &info,
                          const flight::CallHeaders &incoming_headers,
                          std::shared_ptr<flight::ServerMiddleware> *middleware) override;

  std::optional<bool> GetIsValid();

 private:
  std::optional<bool> isValid_;
  std::string secret_key_;
};

}  // namespace gizmosql
