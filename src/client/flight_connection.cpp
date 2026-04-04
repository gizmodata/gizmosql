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

#ifdef _WIN32
#define NOMINMAX
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#include <wincrypt.h>
#pragma comment(lib, "crypt32.lib")
#endif

#include <arrow/scalar.h>
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

/// Load system root CA certificates into a PEM string.
/// Returns an empty string if no system certs can be found.
static std::string LoadSystemCACerts() {
#ifdef _WIN32
  // Read root certificates from the Windows Certificate Store
  std::string pem;
  HCERTSTORE store = CertOpenSystemStoreA(0, "ROOT");
  if (!store) return {};

  PCCERT_CONTEXT cert = nullptr;
  while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
    DWORD pem_size = 0;
    if (!CryptBinaryToStringA(cert->pbCertEncoded, cert->cbCertEncoded,
                               CRYPT_STRING_BASE64HEADER, nullptr, &pem_size)) {
      continue;
    }
    std::string buf(pem_size, '\0');
    if (CryptBinaryToStringA(cert->pbCertEncoded, cert->cbCertEncoded,
                              CRYPT_STRING_BASE64HEADER, buf.data(), &pem_size)) {
      buf.resize(pem_size);
      pem += buf;
    }
  }
  CertCloseStore(store, 0);
  return pem;
#else
  // Try well-known CA bundle paths (macOS, Linux distros)
  static const char* paths[] = {
      "/etc/ssl/cert.pem",                   // macOS, Alpine
      "/etc/ssl/certs/ca-certificates.crt",  // Debian, Ubuntu
      "/etc/pki/tls/certs/ca-bundle.crt",    // RHEL, CentOS, Fedora
      "/etc/ssl/ca-bundle.pem",              // OpenSUSE
  };
  for (const char* path : paths) {
    std::ifstream f(path);
    if (f.good()) {
      std::stringstream ss;
      ss << f.rdbuf();
      return ss.str();
    }
  }
  return {};
#endif
}

void FlightConnection::Disconnect() {
  if (client_) {
    // Notify the server to close the session
    arrow::flight::CloseSessionRequest request;
    auto result = client_->CloseSession(call_options_, request);
    (void)result;  // Best-effort; ignore errors on disconnect
    (void)client_->Close();
  }
  client_.reset();
  if (cancel_client_) {
    (void)cancel_client_->Close();
    cancel_client_.reset();
  }
  call_options_ = arrow::flight::FlightCallOptions{};
  cancel_call_options_ = arrow::flight::FlightCallOptions{};
}

void FlightConnection::SendCancelToServer() {
  // Called from the sigwait thread (NOT from a signal handler).
  // Uses cancel_client_ (a separate gRPC connection) to avoid thread-safety
  // issues with the main client_ that's currently streaming results.
  //
  // We use DoAction directly instead of CancelFlightInfo() because the latter
  // requires a non-null FlightInfo in the request (for serialization), but our
  // server ignores it — it cancels by session, not by query descriptor.
  // So we send a minimal CancelFlightInfo action with a dummy FlightInfo.
  if (cancel_client_) {
    // Build a minimal FlightInfo to satisfy serialization
    arrow::flight::FlightInfo::Data data;
    data.schema = "";
    data.descriptor = arrow::flight::FlightDescriptor::Command("");
    data.total_records = -1;
    data.total_bytes = -1;
    auto info = std::make_unique<arrow::flight::FlightInfo>(std::move(data));

    arrow::flight::CancelFlightInfoRequest cancel_req{std::move(info)};
    (void)cancel_client_->CancelFlightInfo(cancel_call_options_, cancel_req);
  }
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
  } else if (config.use_tls && !config.tls_skip_verify) {
    // No explicit root certs provided — load system CA bundle so gRPC can
    // verify the server certificate (gRPC's built-in default path
    // /usr/share/grpc/roots.pem often doesn't exist).
    options.tls_root_certs = LoadSystemCACerts();
    if (options.tls_root_certs.empty()) {
      return arrow::Status::IOError(
          "TLS enabled but no root certificates found. "
          "Use --tls-roots to provide a CA bundle.");
    }
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

  // Create a separate FlightClient for cancellation (thread-safe: own gRPC channel)
  ARROW_ASSIGN_OR_RAISE(cancel_client_,
                         arrow::flight::FlightClient::Connect(location, options));
  cancel_call_options_ = call_options_;  // Same bearer token / headers

  // Verify the connection is actually usable by making a lightweight Flight SQL
  // call. This catches cases where the gRPC channel opened successfully but
  // authentication is missing or invalid (e.g. no username provided).
  auto verify_result = client_->GetSqlInfo(call_options_, {});
  if (!verify_result.ok()) {
    Disconnect();
    bool auth_was_attempted =
        config.auth_type_external || !config.username.empty();
    std::string hint = auth_was_attempted
        ? " — check your credentials"
        : " — authentication required. Provide --username or set GIZMOSQL_USER";
    return arrow::Status::IOError(
        "Connection to " + server_addr + " failed" + hint);
  }

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

// Helper: detect cancellation from gRPC CANCELLED or server INTERRUPT error
static bool IsCancelledStatus(const arrow::Status& st) {
  return st.IsCancelled() ||
         st.ToString().find("INTERRUPT") != std::string::npos;
}


arrow::Result<QueryResult> FlightConnection::ExecuteQuery(
    const std::string& sql, int64_t row_limit) {
  cancel_requested_.store(false);

  // Helper: check if an error was caused by our SIGINT cancel
  auto check_cancel = [this](const arrow::Status& st)
      -> arrow::Result<QueryResult> {
    if (cancel_requested_.load(std::memory_order_relaxed) ||
        IsCancelledStatus(st)) {
      SendCancelToServer();
      return arrow::Status::Cancelled("Query cancelled");
    }
    return st;
  };

  auto info_result = client_->Execute(call_options_, sql);
  if (!info_result.ok()) {
    return check_cancel(info_result.status());
  }
  auto& info = *info_result;

  int64_t server_total_records = info->total_records();

  std::vector<std::shared_ptr<arrow::RecordBatch>> all_batches;
  std::shared_ptr<arrow::Schema> schema;
  int64_t rows_fetched = 0;
  bool hit_limit = false;

  for (const auto& endpoint : info->endpoints()) {
    if (hit_limit) break;

    auto stream_result = client_->DoGet(call_options_, endpoint.ticket);
    if (!stream_result.ok()) {
      return check_cancel(stream_result.status());
    }
    auto& stream = *stream_result;

    auto schema_result = stream->GetSchema();
    if (!schema_result.ok()) {
      return check_cancel(schema_result.status());
    }
    if (!schema) schema = *schema_result;

    while (true) {
      auto chunk_result = stream->Next();
      if (!chunk_result.ok()) {
        return check_cancel(chunk_result.status());
      }
      if (chunk_result->data == nullptr) break;

      auto batch = chunk_result->data;

      if (row_limit > 0 && rows_fetched + batch->num_rows() > row_limit) {
        // Slice the batch to only take what we need
        int64_t needed = row_limit - rows_fetched;
        all_batches.push_back(batch->Slice(0, needed));
        rows_fetched += needed;
        hit_limit = true;
        break;
      }

      all_batches.push_back(batch);
      rows_fetched += batch->num_rows();

      if (row_limit > 0 && rows_fetched >= row_limit) {
        hit_limit = true;
        break;
      }
    }
  }

  if (!schema) {
    return arrow::Status::Invalid("No schema returned from query");
  }

  std::shared_ptr<arrow::Table> table;
  if (all_batches.empty()) {
    ARROW_ASSIGN_OR_RAISE(table, arrow::Table::MakeEmpty(schema));
  } else {
    ARROW_ASSIGN_OR_RAISE(table,
                           arrow::Table::FromRecordBatches(schema, all_batches));
  }

  // Determine total_rows
  int64_t total_rows = table->num_rows();

  if (hit_limit) {
    // We stopped early — need the real total
    if (server_total_records >= 0) {
      total_rows = server_total_records;
    } else {
      // Fallback: run a COUNT(*) query
      std::string count_sql =
          "SELECT COUNT(*) FROM (" + sql + ") AS __count__";
      auto count_result = client_->Execute(call_options_, count_sql);
      if (count_result.ok()) {
        auto& count_info = *count_result;
        // Collect the count result
        bool got_count = false;
        for (const auto& ep : count_info->endpoints()) {
          auto cs = client_->DoGet(call_options_, ep.ticket);
          if (!cs.ok()) break;
          while (true) {
            auto cr = (*cs)->Next();
            if (!cr.ok() || cr->data == nullptr) break;
            if (cr->data->num_rows() > 0 && cr->data->num_columns() > 0) {
              auto scalar_result = cr->data->column(0)->GetScalar(0);
              if (scalar_result.ok()) {
                auto val = std::dynamic_pointer_cast<arrow::Int64Scalar>(
                    *scalar_result);
                if (val && val->is_valid) {
                  total_rows = val->value;
                  got_count = true;
                }
              }
            }
          }
          if (got_count) break;
        }
        if (!got_count) {
          total_rows = -1;  // Unknown
        }
      } else {
        total_rows = -1;  // COUNT query failed, unknown total
      }
    }
  }

  return QueryResult{std::move(table), total_rows};
}

arrow::Result<int64_t> FlightConnection::ExecuteUpdate(const std::string& sql) {
  cancel_requested_.store(false);
  auto result = client_->ExecuteUpdate(call_options_, sql);
  if (!result.ok()) {
    if (cancel_requested_.load(std::memory_order_relaxed) ||
        IsCancelledStatus(result.status())) {
      SendCancelToServer();
      return arrow::Status::Cancelled("Query cancelled");
    }
  }
  return result;
}

arrow::Result<std::shared_ptr<arrow::Table>> FlightConnection::GetTables(
    const std::string& catalog_pattern,
    const std::string& schema_pattern,
    const std::string& table_pattern,
    bool include_schema) {
  std::string cat = catalog_pattern.empty() ? "" : catalog_pattern;
  std::string sch = schema_pattern.empty() ? "" : schema_pattern;
  std::string tab = table_pattern.empty() ? "" : table_pattern;

  ARROW_ASSIGN_OR_RAISE(
      auto info,
      client_->GetTables(call_options_,
                         catalog_pattern.empty() ? nullptr : &cat,
                         schema_pattern.empty() ? nullptr : &sch,
                         table_pattern.empty() ? nullptr : &tab,
                         include_schema, nullptr));
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

arrow::Status FlightConnection::UploadLastResult(
    const std::shared_ptr<arrow::Table>& table, const std::string& table_name) {
  if (!client_) {
    return arrow::Status::Invalid("Not connected to a server");
  }
  if (!table || table->num_rows() == 0) {
    return arrow::Status::Invalid("No result to upload");
  }

  // Drop existing temp table first — DuckDB's information_schema.tables doesn't
  // include temp tables, so the server's TableExists check misses them and
  // the CREATE_OR_REPLACE path in DoPutCommandStatementIngest never fires.
  auto drop_result = client_->ExecuteUpdate(
      call_options_, "DROP TABLE IF EXISTS \"" + table_name + "\"");
  // Ignore errors — table might not exist

  auto reader = std::make_shared<arrow::TableBatchReader>(*table);

  namespace sql = arrow::flight::sql;
  sql::TableDefinitionOptions table_def_options;
  table_def_options.if_not_exist =
      sql::TableDefinitionOptionsTableNotExistOption::kCreate;
  table_def_options.if_exists =
      sql::TableDefinitionOptionsTableExistsOption::kReplace;

  ARROW_ASSIGN_OR_RAISE(
      auto rows_written,
      client_->ExecuteIngest(call_options_, reader, table_def_options,
                             table_name, std::nullopt, std::nullopt,
                             /*temporary=*/true));

  return arrow::Status::OK();
}

}  // namespace gizmosql::client
