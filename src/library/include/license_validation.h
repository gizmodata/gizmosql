#ifndef LICENSE_VERIFIER_H
#define LICENSE_VERIFIER_H

#include <fstream>
#include <sstream>
#include <string>
#include <stdexcept>
#include <iostream>
#include <chrono>
#include <ctime>
#include "jwt-cpp/jwt.h"
#include "license_public_key.h"

using arrow::Status;

namespace gizmosql {
class LicenseFileVerifier {
 public:
  explicit LicenseFileVerifier() {}

  static Status ValidateLicenseKey(const std::string& filePath) {
    std::cout << "Using License Key file: " << filePath << std::endl;
    std::string jwtToken = readFile(filePath);

    try {
      auto verifier =
          jwt::verify()
              .allow_algorithm(jwt::algorithm::rs256(LICENSE_PUBLIC_KEY, "", "", ""))
              .with_issuer("Gizmo Data LLC");

      auto decodedToken = jwt::decode(jwtToken);
      verifier.verify(decodedToken);

      // Print out license details
      std::cout << "License file verification successful!" << std::endl;
      std::cout << "License ID: " << decodedToken.get_payload_claim("jti").to_json()
                << std::endl;
      std::cout << "Licensed to Customer: "
                << decodedToken.get_payload_claim("aud").to_json() << std::endl;
      std::cout << "Licensed to User: "
                << decodedToken.get_payload_claim("email").to_json() << std::endl;

      // Convert `iat` and `exp` claims to human-readable format
      auto issueDate = decodedToken.get_payload_claim("iat").as_integer();
      auto expiryDate = decodedToken.get_payload_claim("exp").as_integer();

      std::cout << "License issue date: " << formatTimestampToReadableDate(issueDate)
                << std::endl;
      std::cout << "License expiration date: "
                << formatTimestampToReadableDate(expiryDate) << std::endl;

      std::cout << "Licensed by: " << decodedToken.get_payload_claim("iss").to_json()
                << std::endl;

      return Status::OK();
    } catch (const std::exception& e) {
      return Status::ExecutionError("License file JWT verification failed: " +
                                    std::string(e.what()));
    }
  }

 private:
  static std::string readFile(const std::string& filePath) {
    std::ifstream file(filePath);
    if (!file.is_open()) {
      throw std::runtime_error("Could not open file: " + filePath);
    }

    std::ostringstream contents;
    contents << file.rdbuf();
    return contents.str();
  }

  static std::string formatTimestampToReadableDate(int64_t timestamp) {
    // Convert the timestamp to a time_t
    std::time_t time = static_cast<std::time_t>(timestamp);

    // Convert to local time
    char buffer[100];
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S",
                      std::localtime(&time))) {
      return std::string(buffer);
    }
    return "Invalid timestamp";
  }
};
}  // namespace gizmosql

#endif  // LICENSE_VERIFIER_H
