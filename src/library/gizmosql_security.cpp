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

#include "include/gizmosql_security.h"

namespace fs = std::filesystem;

using arrow::Status;

namespace gizmosql {

const std::string kJWTIssuer = "gizmosql";
const int kJWTExpiration = 24 * 3600;
const std::string kValidUsername = "gizmosql_username";
const std::string kBasicPrefix = "Basic ";
const std::string kBearerPrefix = "Bearer ";
const std::string kAuthHeader = "authorization";

// ----------------------------------------
Status SecurityUtilities::FlightServerTlsCertificates(
    const fs::path &cert_path, const fs::path &key_path,
    std::vector<flight::CertKeyPair> *out) {
  std::cout << "Using TLS Cert file: " << cert_path << std::endl;
  std::cout << "Using TLS Key file: " << key_path << std::endl;

  *out = std::vector<flight::CertKeyPair>();
  try {
    std::ifstream cert_file(cert_path);
    if (!cert_file) {
      return Status::IOError("Could not open certificate: " + cert_path.string());
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();

    std::ifstream key_file(key_path);
    if (!key_file) {
      return Status::IOError("Could not open key: " + key_path.string());
    }
    std::stringstream key;
    key << key_file.rdbuf();

    out->push_back(flight::CertKeyPair{cert.str(), key.str()});
  } catch (const std::ifstream::failure &e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

Status SecurityUtilities::FlightServerMtlsCACertificate(const std::string &cert_path,
                                                        std::string *out) {
  try {
    std::ifstream cert_file(cert_path);
    if (!cert_file) {
      return Status::IOError("Could not open MTLS CA certificate: " + cert_path);
    }
    std::stringstream cert;
    cert << cert_file.rdbuf();

    *out = cert.str();
  } catch (const std::ifstream::failure &e) {
    return Status::IOError(e.what());
  }
  return Status::OK();
}

// Function to look in CallHeaders for a key that has a value starting with prefix and
// return the rest of the value after the prefix.
std::string SecurityUtilities::FindKeyValPrefixInCallHeaders(
    const flight::CallHeaders &incoming_headers, const std::string &key,
    const std::string &prefix) {
  // Lambda function to compare characters without case sensitivity.
  auto char_compare = [](const char &char1, const char &char2) {
    return (::toupper(char1) == ::toupper(char2));
  };

  auto iter = incoming_headers.find(key);
  if (iter == incoming_headers.end()) {
    return "";
  }
  const std::string val(iter->second);
  if (val.size() > prefix.length()) {
    if (std::equal(val.begin(), val.begin() + prefix.length(), prefix.begin(),
                   char_compare)) {
      return val.substr(prefix.length());
    }
  }
  return "";
}

Status SecurityUtilities::GetAuthHeaderType(const flight::CallHeaders &incoming_headers,
                                            std::string *out) {
  if (!FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix)
           .empty()) {
    *out = "Basic";
  } else if (!FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBearerPrefix)
                  .empty()) {
    *out = "Bearer";
  } else {
    return Status::IOError("Invalid Authorization Header type!");
  }
  return Status::OK();
}

void SecurityUtilities::ParseBasicHeader(const flight::CallHeaders &incoming_headers,
                                         std::string &username, std::string &password) {
  std::string encoded_credentials =
      FindKeyValPrefixInCallHeaders(incoming_headers, kAuthHeader, kBasicPrefix);
  std::stringstream decoded_stream(arrow::util::base64_decode(encoded_credentials));
  std::getline(decoded_stream, username, ':');
  std::getline(decoded_stream, password, ':');
}

// ----------------------------------------
HeaderAuthServerMiddleware::HeaderAuthServerMiddleware(const std::string &username,
                                                       const std::string &secret_key)
    : username_(username), secret_key_(secret_key) {}

void HeaderAuthServerMiddleware::SendingHeaders(
    flight::AddCallHeaders *outgoing_headers) {
  auto token = CreateJWTToken();
  outgoing_headers->AddHeader(kAuthHeader, std::string(kBearerPrefix) + token);
}

void HeaderAuthServerMiddleware::CallCompleted(const Status &status) {}

std::string HeaderAuthServerMiddleware::name() const {
  return "HeaderAuthServerMiddleware";
}

std::string HeaderAuthServerMiddleware::CreateJWTToken() const {
  auto token =
      jwt::create()
          .set_issuer(std::string(kJWTIssuer))
          .set_type("JWT")
          .set_id("gizmosql-server-" +
                  boost::uuids::to_string(boost::uuids::random_generator()()))
          .set_issued_at(std::chrono::system_clock::now())
          .set_expires_at(std::chrono::system_clock::now() +
                          std::chrono::seconds{kJWTExpiration})
          .set_payload_claim("sub", jwt::claim(username_))
          .set_payload_claim("role", jwt::claim(std::string("admin")))
          .set_payload_claim(
              "session_id",
              jwt::claim(boost::uuids::to_string(boost::uuids::random_generator()())))
          .sign(jwt::algorithm::hs256{secret_key_});

  return token;
}

// ----------------------------------------
HeaderAuthServerMiddlewareFactory::HeaderAuthServerMiddlewareFactory(
    const std::string &username, const std::string &password,
    const std::string &secret_key)
    : username_(username), password_(password), secret_key_(secret_key) {}

Status HeaderAuthServerMiddlewareFactory::StartCall(
    const flight::CallInfo &info, const flight::ServerCallContext &context,
    std::shared_ptr<flight::ServerMiddleware> *middleware) {
  std::string auth_header_type;

  auto incoming_headers = context.incoming_headers();

  ARROW_RETURN_NOT_OK(
      SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
  if (auth_header_type == "Basic") {
    std::string username;
    std::string password;

    SecurityUtilities::ParseBasicHeader(incoming_headers, username, password);

    // If the username has "};PWD={" in it, it is from the Flight SQL ODBC driver -
    // we need to split it into username and password.
    if (username.find("};PWD={") != std::string::npos) {
      std::string username_pwd = username;
      std::string delimiter = "};PWD={";
      size_t pos = 0;
      while ((pos = username_pwd.find(delimiter)) != std::string::npos) {
        username = username_pwd.substr(0, pos);
        username_pwd.erase(0, pos + delimiter.length());
      }
      password = username_pwd;
    }

    if ((username == username_) && (password == password_)) {
      *middleware = std::make_shared<HeaderAuthServerMiddleware>(username, secret_key_);
    } else {
      return MakeFlightError(flight::FlightStatusCode::Unauthenticated,
                             "Invalid credentials");
    }
  }
  return Status::OK();
}

// ----------------------------------------
BearerAuthServerMiddleware::BearerAuthServerMiddleware(
    const std::string &secret_key, const flight::ServerCallContext &context,
    std::optional<bool> *isValid)
    : secret_key_(secret_key),
      incoming_headers_(context.incoming_headers()),
      isValid_(isValid) {}

void BearerAuthServerMiddleware::SendingHeaders(
    flight::AddCallHeaders *outgoing_headers) {
  std::string bearer_token = SecurityUtilities::FindKeyValPrefixInCallHeaders(
      incoming_headers_, kAuthHeader, kBearerPrefix);
  *isValid_ = (VerifyToken(bearer_token));
}

void BearerAuthServerMiddleware::CallCompleted(const Status &status) {}

std::string BearerAuthServerMiddleware::name() const {
  return "BearerAuthServerMiddleware";
}

bool BearerAuthServerMiddleware::VerifyToken(const std::string &token) const {
  if (token.empty()) {
    return false;
  }
  auto verify = jwt::verify()
                    .allow_algorithm(jwt::algorithm::hs256{secret_key_})
                    .with_issuer(std::string(kJWTIssuer));

  try {
    auto decoded = jwt::decode(token);
    verify.verify(decoded);
    // If we got this far, the token verified successfully...
    return true;
  } catch (const std::exception &e) {
    std::cout << "Bearer Token verification failed with exception: " << e.what()
              << std::endl;
    return false;
  }
}

// ----------------------------------------
BearerAuthServerMiddlewareFactory::BearerAuthServerMiddlewareFactory(
    const std::string &secret_key)
    : secret_key_(secret_key) {}

Status BearerAuthServerMiddlewareFactory::StartCall(
    const flight::CallInfo &info, const flight::ServerCallContext &context,
    std::shared_ptr<flight::ServerMiddleware> *middleware) {
  auto incoming_headers = context.incoming_headers();
  if (const std::pair<flight::CallHeaders::const_iterator,
                      flight::CallHeaders::const_iterator> &iter_pair =
          incoming_headers.equal_range(kAuthHeader);
      iter_pair.first != iter_pair.second) {
    std::string auth_header_type;
    ARROW_RETURN_NOT_OK(
        SecurityUtilities::GetAuthHeaderType(incoming_headers, &auth_header_type));
    if (auth_header_type == "Bearer") {
      *middleware =
          std::make_shared<BearerAuthServerMiddleware>(secret_key_, context, &isValid_);
    }
  }
  if (isValid_.has_value() && !*isValid_) {
    isValid_.reset();

    return MakeFlightError(flight::FlightStatusCode::Unauthenticated,
                           "Invalid bearer token provided");
  }

  return Status::OK();
}

std::optional<bool> BearerAuthServerMiddlewareFactory::GetIsValid() { return isValid_; }

}  // namespace gizmosql
