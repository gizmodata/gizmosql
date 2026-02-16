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

#include "flight_connection.hpp"
#include "oauth_flow.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <vector>

#include <arrow/table.h>

namespace gizmosql::client {

arrow::Status ReadPEMFile(const std::string& path, std::string& contents) {
  std::ifstream file(path);
  if (!file.is_open()) {
    return arrow::Status::IOError("Could not open file: " + path);
  }
  std::stringstream ss;
  ss << file.rdbuf();
  contents = ss.str();
  return arrow::Status::OK();
}

void FlightConnection::Disconnect() {
  client_.reset();
  call_options_ = arrow::flight::FlightCallOptions{};
}

arrow::Status FlightConnection::Connect(const ClientConfig& config) {
  Disconnect();

  ARROW_ASSIGN_OR_RAISE(
      auto location,
      config.use_tls
          ? arrow::flight::Location::ForGrpcTls(config.host, config.port)
          : arrow::flight::Location::ForGrpcTcp(config.host, config.port));

  arrow::flight::FlightClientOptions options;

  if (!config.tls_roots.empty()) {
    ARROW_RETURN_NOT_OK(ReadPEMFile(config.tls_roots, options.tls_root_certs));
  }

  options.disable_server_verification = config.tls_skip_verify;

  if (!config.mtls_cert.empty()) {
    ARROW_RETURN_NOT_OK(ReadPEMFile(config.mtls_cert, options.cert_chain));
    if (!config.mtls_key.empty()) {
      ARROW_RETURN_NOT_OK(ReadPEMFile(config.mtls_key, options.private_key));
    } else {
      return arrow::Status::Invalid(
          "mTLS private key must be provided with mTLS certificate");
    }
  }

  ARROW_ASSIGN_OR_RAISE(auto flight_client,
                         arrow::flight::FlightClient::Connect(location, options));

  // Helper to translate cryptic gRPC errors into user-friendly messages
  std::string scheme = config.use_tls ? "grpc+tls" : "grpc";
  std::string server_addr =
      scheme + "://" + config.host + ":" + std::to_string(config.port);
  auto friendly_error = [&](const arrow::Status& s) -> arrow::Status {
    std::string msg = s.ToString();
    if (msg.find("Could not finish writing") != std::string::npos ||
        msg.find("failed to connect to all addresses") != std::string::npos ||
        msg.find("Connection refused") != std::string::npos ||
        msg.find("Unavailable") != std::string::npos) {
      return arrow::Status::IOError(
          "Could not connect to " + server_addr +
          " — is the server running?");
    }
    if (msg.find("DNS resolution failed") != std::string::npos) {
      return arrow::Status::IOError(
          "Could not resolve host '" + config.host + "'");
    }
    if (msg.find("Invalid credentials") != std::string::npos ||
        msg.find("Unauthenticated") != std::string::npos) {
      return arrow::Status::IOError(
          "Authentication failed for " + server_addr +
          " — check username and password");
    }
    return s;
  };

  // Authenticate on the raw FlightClient before wrapping in FlightSqlClient
  if (config.auth_type_external) {
    OAuthFlow oauth;
    auto id_token_result = oauth.Authenticate(config);
    if (!id_token_result.ok()) return friendly_error(id_token_result.status());
    auto bearer_result = flight_client->AuthenticateBasicToken(
        call_options_, "token", *id_token_result);
    if (!bearer_result.ok()) return friendly_error(bearer_result.status());
    call_options_.headers.push_back(*bearer_result);
  } else if (!config.username.empty()) {
    auto bearer_result = flight_client->AuthenticateBasicToken(
        call_options_, config.username, config.password);
    if (!bearer_result.ok()) return friendly_error(bearer_result.status());
    call_options_.headers.push_back(*bearer_result);
  }

  client_ = std::make_unique<arrow::flight::sql::FlightSqlClient>(
      std::move(flight_client));

  return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::CollectResults(
    const std::unique_ptr<arrow::flight::FlightInfo>& info) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  std::shared_ptr<arrow::Schema> schema;

  for (const auto& endpoint : info->endpoints()) {
    ARROW_ASSIGN_OR_RAISE(auto stream,
                           client_->DoGet(call_options_, endpoint.ticket));
    ARROW_ASSIGN_OR_RAISE(auto stream_schema, stream->GetSchema());
    if (!schema) {
      schema = stream_schema;
    }

    while (true) {
      ARROW_ASSIGN_OR_RAISE(auto chunk, stream->Next());
      if (chunk.data == nullptr) break;
      all_batches.push_back(chunk.data);
    }
  }

  if (!schema) {
    return arrow::Status::Invalid("No schema returned from query");
  }

  if (all_batches.empty()) {
    return arrow::Table::MakeEmpty(schema);
  }

  return arrow::Table::FromRecordBatches(schema, all_batches);
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::ExecuteQuery(
    const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto info, client_->Execute(call_options_, sql));
  return CollectResults(info);
}

arrow::Result<int64_t> FlightConnection::ExecuteUpdate(const std::string& sql) {
  return client_->ExecuteUpdate(call_options_, sql);
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::GetTables(
    const std::string& catalog_pattern,
    const std::string& schema_pattern,
    const std::string& table_pattern) {
  std::string cat = catalog_pattern.empty() ? "" : catalog_pattern;
  std::string sch = schema_pattern.empty() ? "" : schema_pattern;
  std::string tab = table_pattern.empty() ? "" : table_pattern;

  ARROW_ASSIGN_OR_RAISE(
      auto info,
      client_->GetTables(call_options_,
                         catalog_pattern.empty() ? nullptr : &cat,
                         schema_pattern.empty() ? nullptr : &sch,
                         table_pattern.empty() ? nullptr : &tab,
                         false, nullptr));
  return CollectResults(info);
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::GetDbSchemas(
    const std::string& catalog_pattern,
    const std::string& schema_pattern) {
  std::string cat = catalog_pattern.empty() ? "" : catalog_pattern;
  std::string sch = schema_pattern.empty() ? "" : schema_pattern;

  ARROW_ASSIGN_OR_RAISE(
      auto info,
      client_->GetDbSchemas(call_options_,
                            catalog_pattern.empty() ? nullptr : &cat,
                            schema_pattern.empty() ? nullptr : &sch));
  return CollectResults(info);
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::GetCatalogs() {
  ARROW_ASSIGN_OR_RAISE(auto info, client_->GetCatalogs(call_options_));
  return CollectResults(info);
}

}  // namespace gizmosql::client
