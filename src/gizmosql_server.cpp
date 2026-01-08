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

#include "gizmosql_library.h"
#include "common/include/detail/gizmosql_logging.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace po = boost::program_options;
namespace fs = std::filesystem;

int main(int argc, char** argv) {
  std::vector<std::string> tls_token_values;

  // Declare the supported options.
  po::options_description desc("Allowed options");
  // clang-format off
    desc.add_options()
            ("help", "produce this help message")
            ("version", "Print the version and exit")
            ("backend,B", po::value<std::string>()->default_value("duckdb"),
             "Specify the database backend. Allowed options: duckdb, sqlite.")
            ("hostname,H", po::value<std::string>()->default_value(""),
             "Specify the hostname to listen on for the GizmoSQL Server.  If not set, we will use env var: 'GIZMOSQL_HOSTNAME'.  "
             "If that isn't set, we will use the default of: '0.0.0.0'.")
            ("port,R", po::value<int>()->default_value(DEFAULT_FLIGHT_PORT),
             "Specify the port to listen on for the GizmoSQL Server.")
            ("database-filename,D", po::value<std::string>()->default_value(""),
             "Specify the database filename (absolute or relative to the current working directory).  If not set, we will open an in-memory database.")
            ("username,U", po::value<std::string>()->default_value(""),
             "Specify the username to allow to connect to the GizmoSQL Server for clients.  If not set, we will use env var: 'GIZMOSQL_USERNAME'.  "
             "If that isn't set, we will use the default of: 'gizmosql_username'.")
            ("password,P", po::value<std::string>()->default_value(""),
             "Specify the password to set on the GizmoSQL Server for clients to connect with.  If not set, we will use env var: 'GIZMOSQL_PASSWORD'.  "
             "If that isn't set, the server will exit with failure.")
            ("secret-key,S", po::value<std::string>()->default_value(""),
             "Specify the secret key used to sign JWTs issued by the GizmoSQL Server. "
             "If it isn't set, we use env var: 'SECRET_KEY'.  If that isn't set, the server will create a random secret key.")
            ("tls,T", po::value<std::vector<std::string>>(&tls_token_values)->multitoken()->default_value(
                     std::vector<std::string>{"", ""}, ""),
             "Specify the TLS certificate and key file paths.")
            ("init-sql-commands,I", po::value<std::string>()->default_value(""),
             "Specify the SQL commands to run on server startup.  "
             "If not set, we will use env var: 'INIT_SQL_COMMANDS'.")
            ("init-sql-commands-file,F", po::value<std::string>()->default_value(""),
             "Specify a file containing SQL commands to run on server startup.  "
             "If not set, we will use env var: 'INIT_SQL_COMMANDS_FILE'.")
            ("mtls-ca-cert-filename,M", po::value<std::string>()->default_value(""),
             "Specify an optional mTLS CA certificate path used to verify clients.  The certificate MUST be in PEM format.")
            ("print-queries,Q", po::bool_switch()->default_value(false), "Print queries run by clients to stdout")
            ("readonly,O", po::bool_switch()->default_value(false), "Open the database in read-only mode")
            ("token-allowed-issuer", po::value<std::string>()->default_value(""),
             "Specify the allowed token issuer for JWT token-based authentication - see docs for details.  "
             "If not set, we will use env var: 'TOKEN_ALLOWED_ISSUER'.")
            ("token-allowed-audience", po::value<std::string>()->default_value(""),
             "Specify the allowed token audience for JWT token-based authentication - see docs for details.  "
              "If not set, we will use env var: 'TOKEN_ALLOWED_AUDIENCE'.")
            ("token-signature-verify-cert-path", po::value<std::string>()->default_value(""),
             "Specify the RSA PEM certificate file used for verifying tokens used in JWT token-based authentication - see docs for details.  "
              "If not set, we will use env var: 'TOKEN_SIGNATURE_VERIFY_CERT_PATH'.")
            // -------- Logging controls (raw strings; library normalizes) --------
            ("log-level",  po::value<std::string>()->default_value(""),
             "Log level: debug|info|warn|error|fatal. If empty, uses env GIZMOSQL_LOG_LEVEL or defaults to info.")
            ("log-format", po::value<std::string>()->default_value(""),
             "Log format: text|json. If empty, uses env GIZMOSQL_LOG_FORMAT or defaults to text.")
            ("access-log", po::value<std::string>()->default_value(""),
             "Per-RPC access logging: on|off. If empty, uses env GIZMOSQL_ACCESS_LOG or defaults to off (it is very verbose).")
            ("log-file",   po::value<std::string>()->default_value(""),
             "Log file path; use '-' for stdout; empty => stderr. Can also use env GIZMOSQL_LOG_FILE.")
            ("query-timeout",   po::value<int32_t>()->default_value(DEFAULT_QUERY_TIMEOUT_SECONDS),
             "The Query Timeout limit in seconds.  A value of 0 means unlimited.")
            ("query-log-level",  po::value<std::string>()->default_value(""),
             "Query Log level: debug|info|warn|error|fatal. If empty, uses env GIZMOSQL_QUERY_LOG_LEVEL or defaults to info.")
            ("auth-log-level",  po::value<std::string>()->default_value(""),
              "Authentication Log level: debug|info|warn|error|fatal. If empty, uses env GIZMOSQL_AUTH_LOG_LEVEL or defaults to info.")
            ("health-port",  po::value<int>()->default_value(DEFAULT_HEALTH_PORT),
              "Port for plaintext gRPC health check server (for Kubernetes probes). Set to 0 to disable.")
            // -------- OpenTelemetry controls --------
            ("otel-enabled", po::value<std::string>()->default_value(""),
             "Enable OpenTelemetry: on|off. If empty, uses env GIZMOSQL_OTEL_ENABLED or defaults to off.")
            ("otel-exporter", po::value<std::string>()->default_value(""),
             "OTLP exporter type: http. If empty, uses env GIZMOSQL_OTEL_EXPORTER or defaults to http.")
            ("otel-endpoint", po::value<std::string>()->default_value(""),
             "OTLP endpoint (e.g., http://localhost:4318). "
             "If empty, uses env GIZMOSQL_OTEL_ENDPOINT or defaults to http://localhost:4318.")
            ("otel-service-name", po::value<std::string>()->default_value(""),
             "Service name for telemetry. If empty, uses env GIZMOSQL_OTEL_SERVICE_NAME or defaults to 'gizmosql'.")
            ("otel-headers", po::value<std::string>()->default_value(""),
             "Additional headers for OTLP exporter (format: key1=value1,key2=value2). "
             "If empty, uses env GIZMOSQL_OTEL_HEADERS. Useful for authentication (e.g., DD-API-KEY=xxx).");

  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    GIZMOSQL_LOG(INFO) << desc << "\n";
    return 0;
  }

  if (vm.count("version")) {
    GIZMOSQL_LOG(INFO) << "GizmoSQL Server CLI: " << GIZMOSQL_SERVER_VERSION;
    return 0;
  }

  std::string backend_str = vm["backend"].as<std::string>();
  BackendType backend;
  if (backend_str == "duckdb") {
    backend = BackendType::duckdb;
  } else if (backend_str == "sqlite") {
    backend = BackendType::sqlite;
  } else {
    GIZMOSQL_LOG(INFO) << "Invalid backend: " << backend_str;
    return 1;
  }

  auto database_filename = fs::path(vm["database-filename"].as<std::string>());

  std::string hostname = "";
  if (vm.count("hostname")) {
    hostname = vm["hostname"].as<std::string>();
  }

  int port = vm["port"].as<int>();

  std::string username = "";
  if (vm.count("username")) {
    username = vm["username"].as<std::string>();
  }

  std::string password = "";
  if (vm.count("password")) {
    password = vm["password"].as<std::string>();
  }

  std::string secret_key = "";
  if (vm.count("secret-key")) {
    secret_key = vm["secret-key"].as<std::string>();
  }

  auto tls_cert_path = fs::path();
  auto tls_key_path = fs::path();
  if (vm.count("tls")) {
    std::vector<std::string> tls_tokens = tls_token_values;
    if (tls_tokens.size() != 2) {
      GIZMOSQL_LOG(INFO) << "--tls requires 2 entries - separated by a space!";
      return 1;
    }
    tls_cert_path = fs::path(tls_tokens[0]);
    tls_key_path = fs::path(tls_tokens[1]);
  }

  std::string init_sql_commands = "";
  if (vm.count("init-sql-commands")) {
    init_sql_commands = vm["init-sql-commands"].as<std::string>();
  }

  std::string init_sql_commands_file = "";
  if (vm.count("init-sql-commands-file")) {
    init_sql_commands_file = fs::path(vm["init-sql-commands-file"].as<std::string>());
  }

  fs::path mtls_ca_cert_path;
  if (vm.count("mtls-ca-cert-filename")) {
    mtls_ca_cert_path = fs::path(vm["mtls-ca-cert-filename"].as<std::string>());
  }

  bool print_queries = vm["print-queries"].as<bool>();

  bool read_only = vm["readonly"].as<bool>();

  std::string token_allowed_issuer = "";
  if (vm.count("token-allowed-issuer")) {
    token_allowed_issuer = vm["token-allowed-issuer"].as<std::string>();
  }

  std::string token_allowed_audience = "";
  if (vm.count("token-allowed-audience")) {
    token_allowed_audience = vm["token-allowed-audience"].as<std::string>();
  }

  fs::path token_signature_verify_cert_path;
  if (vm.count("token-signature-verify-cert-path")) {
    token_signature_verify_cert_path =
        fs::path(vm["token-signature-verify-cert-path"].as<std::string>());
  }

  std::string log_level = vm.count("log-level") ? vm["log-level"].as<std::string>() : "";
  std::string log_format =
      vm.count("log-format") ? vm["log-format"].as<std::string>() : "";
  std::string access_log =
      vm.count("access-log") ? vm["access-log"].as<std::string>() : "";
  std::string log_file = vm.count("log-file") ? vm["log-file"].as<std::string>() : "";

  int32_t query_timeout =
      vm.count("query-timeout") ? vm["query-timeout"].as<int32_t>() : 0;

  std::string query_log_level =
      vm.count("query-log-level") ? vm["query-log-level"].as<std::string>() : "";
  std::string auth_log_level =
      vm.count("auth-log-level") ? vm["auth-log-level"].as<std::string>() : "";

  int health_port = vm["health-port"].as<int>();

  // OpenTelemetry options
  std::string otel_enabled =
      vm.count("otel-enabled") ? vm["otel-enabled"].as<std::string>() : "";
  std::string otel_exporter =
      vm.count("otel-exporter") ? vm["otel-exporter"].as<std::string>() : "";
  std::string otel_endpoint =
      vm.count("otel-endpoint") ? vm["otel-endpoint"].as<std::string>() : "";
  std::string otel_service_name =
      vm.count("otel-service-name") ? vm["otel-service-name"].as<std::string>() : "";
  std::string otel_headers =
      vm.count("otel-headers") ? vm["otel-headers"].as<std::string>() : "";

  return RunFlightSQLServer(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands,
      init_sql_commands_file, print_queries, read_only, token_allowed_issuer,
      token_allowed_audience, token_signature_verify_cert_path, log_level, log_format,
      access_log, log_file, query_timeout, query_log_level, auth_log_level, health_port,
      otel_enabled, otel_exporter, otel_endpoint, otel_service_name, otel_headers);
}