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
#include <iostream>
#include <sstream>
#include <vector>

#include <arrow/table.h>
#include <nlohmann/json.hpp>

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
  if (client_) {
    // Notify the server to close the session
    arrow::flight::CloseSessionRequest request;
    auto result = client_->CloseSession(call_options_, request);
    (void)result;  // Best-effort; ignore errors on disconnect
    client_->Close();
  }
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
    // Step 1: Discovery handshake — ask server for OAuth endpoint URL
    std::string oauth_base_url;
    auto discover_result = flight_client->AuthenticateBasicToken(
        call_options_, "__discover__", "");
    if (discover_result.ok()) {
      // Bearer token value is: "Bearer {"oauth_url":"http://host:port"}"
      std::string bearer_value = discover_result->second;
      const std::string bearer_prefix = "Bearer ";
      if (bearer_value.size() > bearer_prefix.size() &&
          bearer_value.substr(0, bearer_prefix.size()) == bearer_prefix) {
        auto json = nlohmann::json::parse(
            bearer_value.substr(bearer_prefix.size()), nullptr, false);
        if (!json.is_discarded() && json.contains("oauth_url")) {
          oauth_base_url = json["oauth_url"].get<std::string>();
        }
      }
    }

    // Step 2: Fallback if discovery failed (e.g., older server)
    if (oauth_base_url.empty()) {
      std::string oauth_scheme = config.use_tls ? "https" : "http";
      oauth_base_url = oauth_scheme + "://" + config.host + ":" +
                       std::to_string(config.oauth_port);
      std::cerr << "OAuth discovery unavailable; using fallback URL: "
                << oauth_base_url << std::endl;
    }

    // Step 3: OAuth browser flow
    OAuthFlow oauth;
    auto id_token_result = oauth.Authenticate(oauth_base_url);
    if (!id_token_result.ok()) return friendly_error(id_token_result.status());

    // Step 4: Exchange token via Basic Auth
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
  // Set up cancellation token for this query
  stop_source_.Reset();
  auto opts = call_options_;
  opts.stop_token = stop_source_.token();
  query_active_.store(true);

  // Helper: tell the server to cancel the active query for this session
  auto cancel_on_server = [&]() {
    arrow::flight::CancelFlightInfoRequest cancel_req{""};
    (void)client_->CancelFlightInfo(call_options_, cancel_req);
  };

  auto info_result = client_->Execute(opts, sql);
  if (!info_result.ok()) {
    query_active_.store(false);
    if (info_result.status().IsCancelled()) {
      cancel_on_server();
      return arrow::Status::Cancelled("Query cancelled");
    }
    return info_result.status();
  }
  auto& info = *info_result;

  // Collect results with cancellation support
  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  std::shared_ptr<arrow::Schema> schema;

  for (const auto& endpoint : info->endpoints()) {
    auto stream_result = client_->DoGet(opts, endpoint.ticket);
    if (!stream_result.ok()) {
      query_active_.store(false);
      if (stream_result.status().IsCancelled()) {
        cancel_on_server();
        return arrow::Status::Cancelled("Query cancelled");
      }
      return stream_result.status();
    }
    auto& stream = *stream_result;

    auto schema_result = stream->GetSchema();
    if (!schema_result.ok()) {
      query_active_.store(false);
      return schema_result.status();
    }
    if (!schema) schema = *schema_result;

    while (true) {
      auto chunk_result = stream->Next();
      if (!chunk_result.ok()) {
        query_active_.store(false);
        if (chunk_result.status().IsCancelled()) {
          cancel_on_server();
          return arrow::Status::Cancelled("Query cancelled");
        }
        return chunk_result.status();
      }
      if (chunk_result->data == nullptr) break;
      all_batches.push_back(chunk_result->data);
    }
  }

  query_active_.store(false);

  if (!schema) {
    return arrow::Status::Invalid("No schema returned from query");
  }
  if (all_batches.empty()) {
    return arrow::Table::MakeEmpty(schema);
  }
  return arrow::Table::FromRecordBatches(schema, all_batches);
}

arrow::Result<int64_t> FlightConnection::ExecuteUpdate(const std::string& sql) {
  stop_source_.Reset();
  auto opts = call_options_;
  opts.stop_token = stop_source_.token();
  query_active_.store(true);
  auto result = client_->ExecuteUpdate(opts, sql);
  query_active_.store(false);
  if (!result.ok() && result.status().IsCancelled()) {
    arrow::flight::CancelFlightInfoRequest cancel_req{""};
    (void)client_->CancelFlightInfo(call_options_, cancel_req);
    return arrow::Status::Cancelled("Query cancelled");
  }
  return result;
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

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::GetSqlInfo(
    const std::vector<int>& info) {
  ARROW_ASSIGN_OR_RAISE(auto flight_info, client_->GetSqlInfo(call_options_, info));
  return CollectResults(flight_info);
}

}  // namespace gizmosql::client
