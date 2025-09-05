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

class BasicAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  BasicAuthServerMiddleware(const std::string &username, const std::string &secret_key);

  const jwt::decoded_jwt<jwt::traits::kazuho_picojson> GetJWT();
  const std::string GetUsername() const;
  const std::string GetRole() const;

  void SendingHeaders(flight::AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const arrow::Status &status) override;

  std::string name() const override;

 private:
  std::string username_;
  std::string secret_key_;

  std::string CreateJWTToken() const;
};

class BasicAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  BasicAuthServerMiddlewareFactory(const std::string &username,
                                   const std::string &password,
                                   const std::string &secret_key);

  arrow::Status StartCall(const flight::CallInfo &info,
                          const flight::ServerCallContext &context,
                          std::shared_ptr<flight::ServerMiddleware> *middleware) override;

 private:
  std::string username_;
  std::string password_;
  std::string secret_key_;
};

class BearerAuthServerMiddleware : public flight::ServerMiddleware {
 public:
  explicit BearerAuthServerMiddleware(
      const jwt::decoded_jwt<jwt::traits::kazuho_picojson> decoded_jwt);

  const jwt::decoded_jwt<jwt::traits::kazuho_picojson> GetJWT() const;
  const std::string GetUsername() const;
  const std::string GetRole() const;

  void SendingHeaders(flight::AddCallHeaders *outgoing_headers) override;

  void CallCompleted(const arrow::Status &status) override;

  std::string name() const override;

 private:
  jwt::decoded_jwt<jwt::traits::kazuho_picojson> decoded_jwt_;
};

class BearerAuthServerMiddlewareFactory : public flight::ServerMiddlewareFactory {
 public:
  explicit BearerAuthServerMiddlewareFactory(
      const std::string &secret_key, const std::string &token_allowed_issuer,
      const std::string &token_allowed_audience,
      const std::filesystem::path &token_signature_verify_cert_path);

  arrow::Status StartCall(const flight::CallInfo &info,
                          const flight::ServerCallContext &context,
                          std::shared_ptr<flight::ServerMiddleware> *middleware) override;

 private:
  std::string secret_key_;
  std::string token_allowed_issuer_;
  std::string token_allowed_audience_;
  std::filesystem::path token_signature_verify_cert_path_;
  std::string token_signature_verify_cert_file_contents_;

  arrow::Result<jwt::decoded_jwt<jwt::traits::kazuho_picojson>> VerifyAndDecodeToken(
      const std::string &token,
      const flight::ServerCallContext& context) const;
};

}  // namespace gizmosql
