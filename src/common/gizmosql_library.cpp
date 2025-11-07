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

#include <cstdlib>
#include <csignal>
#include <iostream>
#include <filesystem>
#include <regex>
#include <vector>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/server.h>
#include <boost/algorithm/string.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "sqlite_server.h"
#include "duckdb_server.h"
#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"
#include "gizmosql_security.h"
#include "access_log_middleware.h"

namespace fs = std::filesystem;

namespace gizmosql {
const int port = 31337;

#define RUN_INIT_COMMANDS(serverType, init_sql_commands)                           \
  do {                                                                             \
    if (init_sql_commands != "") {                                                 \
      std::regex regex_pattern(";(?=(?:[^']*'[^']*')*[^']*$)");                    \
      std::sregex_token_iterator iter(init_sql_commands.begin(),                   \
                                      init_sql_commands.end(), regex_pattern, -1); \
      std::sregex_token_iterator end;                                              \
      while (iter != end) {                                                        \
        std::string init_sql_command = *iter;                                      \
        if (init_sql_command.empty()) continue;                                    \
        auto logged_init_sql_command = redact_sql_for_logs(init_sql_command);      \
        GIZMOSQL_LOG(INFO) << "Running Init SQL command: \n"                       \
                           << logged_init_sql_command << ";";                      \
        ARROW_RETURN_NOT_OK(serverType->ExecuteSql(init_sql_command));             \
        ++iter;                                                                    \
      }                                                                            \
    }                                                                              \
  } while (false)

static std::string MakeSessionId() {
  return boost::uuids::to_string(boost::uuids::random_generator()());
}

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>> FlightSQLServerBuilder(
    const BackendType backend, const fs::path& database_filename,
    const std::string& hostname, const int& port, const std::string& username,
    const std::string& password, const std::string& secret_key,
    const fs::path& tls_cert_path, const fs::path& tls_key_path,
    const fs::path& mtls_ca_cert_path, const std::string& init_sql_commands,
    const bool& read_only, const bool& print_queries,
    const std::string& token_allowed_issuer, const std::string& token_allowed_audience,
    const fs::path& token_signature_verify_cert_path, const bool& access_logging_enabled,
    const int32_t& query_timeout) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        (!tls_cert_path.empty())
                            ? flight::Location::ForGrpcTls(hostname, port)
                            : flight::Location::ForGrpcTcp(hostname, port));

  GIZMOSQL_LOG(INFO) << "----------------------------------------------";
  GIZMOSQL_LOG(INFO) << "Apache Arrow version: " << ARROW_VERSION_STRING;

  flight::FlightServerOptions options(location);

  if (!tls_cert_path.empty() && !tls_key_path.empty()) {
    ARROW_CHECK_OK(gizmosql::SecurityUtilities::FlightServerTlsCertificates(
        tls_cert_path, tls_key_path, &options.tls_certificates));
  } else {
    GIZMOSQL_LOG(INFO)
        << "WARNING - TLS is disabled for the GizmoSQL server - this is NOT secure.";
  }

  // Setup authentication middleware (using the same TLS certificate keypair)
  auto header_middleware = std::make_shared<gizmosql::BasicAuthServerMiddlewareFactory>(
      username, password, secret_key, token_allowed_issuer, token_allowed_audience,
      token_signature_verify_cert_path);
  auto bearer_middleware =
      std::make_shared<gizmosql::BearerAuthServerMiddlewareFactory>(secret_key);

  options.auth_handler = std::make_unique<flight::NoOpAuthHandler>();
  options.middleware.push_back({"header-auth-server", header_middleware});
  options.middleware.push_back({"bearer-auth-server", bearer_middleware});

  // Access log middleware (toggle)
  if (access_logging_enabled) {
    options.middleware.push_back({"access_log", std::make_shared<AccessLogFactory>()});
    GIZMOSQL_LOG(INFO) << "Access logging enabled";
  } else {
    GIZMOSQL_LOG(INFO) << "Access logging disabled";
  }

  if (!mtls_ca_cert_path.empty()) {
    GIZMOSQL_LOG(INFO) << "Using mTLS CA certificate: " << mtls_ca_cert_path;
    ARROW_CHECK_OK(gizmosql::SecurityUtilities::FlightServerMtlsCACertificate(
        mtls_ca_cert_path, &options.root_certificates));
    options.verify_client = true;
  }

  std::shared_ptr<flight::sql::FlightSqlServerBase> server = nullptr;

  std::string db_type = "";
  if (backend == BackendType::sqlite) {
    db_type = "SQLite";
    std::shared_ptr<gizmosql::sqlite::SQLiteFlightSqlServer> sqlite_server = nullptr;
    ARROW_ASSIGN_OR_RAISE(sqlite_server, gizmosql::sqlite::SQLiteFlightSqlServer::Create(
                                             database_filename, read_only));
    RUN_INIT_COMMANDS(sqlite_server, init_sql_commands);
    server = sqlite_server;
  } else if (backend == BackendType::duckdb) {
    db_type = "DuckDB";
    std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> duckdb_server = nullptr;
    ARROW_ASSIGN_OR_RAISE(duckdb_server,
                          gizmosql::ddb::DuckDBFlightSqlServer::Create(
                              database_filename, read_only, print_queries, query_timeout))
    // Run additional commands (first) for the DuckDB back-end...
    auto duckdb_init_sql_commands =
        "SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;" +
        init_sql_commands;
    RUN_INIT_COMMANDS(duckdb_server, duckdb_init_sql_commands);
    server = duckdb_server;
  }

  GIZMOSQL_LOG(INFO) << "Using database file: " << database_filename;

  GIZMOSQL_LOG(INFO) << "Print Queries option is set to: " << std::boolalpha
                     << print_queries;

  if (server != nullptr) {
    ARROW_CHECK_OK(server->Init(options));

    // Exit with a clean error code (0) on SIGTERM or SIGINT
    ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM, SIGINT}));

    GIZMOSQL_LOG(INFO) << "GizmoSQL server version: " << GIZMOSQL_SERVER_VERSION
                       << " - with engine: " << db_type << " - will listen on "
                       << server->location().ToString();

    return server;
  } else {
    std::string err_msg = "Unable to create the GizmoSQL Server";
    return arrow::Status::Invalid(err_msg);
  }
}

std::string SafeGetEnvVarValue(const std::string& env_var_name) {
  auto env_var_value = std::getenv(env_var_name.c_str());
  if (env_var_value) {
    return std::string(env_var_value);
  } else {
    return "";
  }
}

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>> CreateFlightSQLServer(
    const BackendType backend, fs::path& database_filename, std::string hostname,
    const int& port, std::string username, std::string password, std::string secret_key,
    fs::path tls_cert_path, fs::path tls_key_path, fs::path mtls_ca_cert_path,
    std::string init_sql_commands, fs::path init_sql_commands_file,
    const bool& print_queries, const bool& read_only, std::string token_allowed_issuer,
    std::string token_allowed_audience, fs::path token_signature_verify_cert_path,
    const bool& access_logging_enabled, const int32_t& query_timeout) {
  // Validate and default the arguments to env var values where applicable
  if (database_filename.empty()) {
    GIZMOSQL_LOG(INFO)
        << "WARNING - The database filename was not provided, opening an in-memory "
           "database...";
    database_filename = ":memory:";
  } else if (database_filename.u8string().find(':') == std::string::npos) {
    // DuckDB supports '<extension_name>:...' syntax, for example 'ducklake:/path/to/lake.db'
    // We do not check for existence of the database file, b/c they may want to create a new one
    database_filename = fs::absolute(database_filename);
  }

  if (hostname.empty()) {
    hostname = SafeGetEnvVarValue("GIZMOSQL_HOSTNAME");
    if (hostname.empty()) {
      hostname = DEFAULT_GIZMOSQL_HOSTNAME;
    }
  }

  if (username.empty()) {
    username = SafeGetEnvVarValue("GIZMOSQL_USERNAME");
    if (username.empty()) {
      username = DEFAULT_GIZMOSQL_USERNAME;
    }
  }

  if (password.empty()) {
    password = SafeGetEnvVarValue("GIZMOSQL_PASSWORD");
    if (password.empty()) {
      return arrow::Status::Invalid(
          "The GizmoSQL Server password is empty and env var: 'GIZMOSQL_PASSWORD' is not "
          "set.  Pass a value to this argument to secure the server.");
    }
  }

  if (secret_key.empty()) {
    secret_key = SafeGetEnvVarValue("SECRET_KEY");
    if (secret_key.empty()) {
      // Generate a random secret key
      boost::uuids::uuid uuid = boost::uuids::random_generator()();
      secret_key = "SECRET-" + boost::uuids::to_string(uuid);
    }
  }

  if (!tls_cert_path.empty()) {
    tls_cert_path = fs::absolute(tls_cert_path);
    if (!fs::exists(tls_cert_path)) {
      return arrow::Status::Invalid("TLS certificate file does not exist: " +
                                    tls_cert_path.string());
    }

    if (tls_key_path.empty()) {
      return arrow::Status::Invalid(
          "tls_key_path was not specified (when tls_cert_path WAS specified)");
    } else {
      tls_key_path = fs::absolute(tls_key_path);
      if (!fs::exists(tls_key_path)) {
        return arrow::Status::Invalid("TLS key file does not exist: " +
                                      tls_key_path.string());
      }
    }
  }

  if (init_sql_commands.empty()) {
    init_sql_commands = SafeGetEnvVarValue("INIT_SQL_COMMANDS");
  }

  if (init_sql_commands_file.empty()) {
    init_sql_commands_file = fs::path(SafeGetEnvVarValue("INIT_SQL_COMMANDS_FILE"));
    if (!init_sql_commands_file.empty()) {
      init_sql_commands_file = fs::absolute(init_sql_commands_file);
      if (!fs::exists(init_sql_commands_file)) {
        return arrow::Status::Invalid("INIT_SQL_COMMANDS_FILE does not exist: " +
                                      init_sql_commands_file.string());
      } else {
        std::ifstream ifs(init_sql_commands_file);
        std::string init_sql_commands_file_contents((std::istreambuf_iterator<char>(ifs)),
                                                    (std::istreambuf_iterator<char>()));
        init_sql_commands += init_sql_commands_file_contents;
      }
    }
  }

  if (!mtls_ca_cert_path.empty()) {
    mtls_ca_cert_path = fs::absolute(mtls_ca_cert_path);
    if (!fs::exists(mtls_ca_cert_path)) {
      return arrow::Status::Invalid("mTLS CA certificate file does not exist: " +
                                    mtls_ca_cert_path.string());
    }
  }

  if (read_only) {
    GIZMOSQL_LOG(INFO)
        << "WARNING - Running in Read-Only mode - no changes will be persisted to the "
           "database.";
  }

  if (token_allowed_issuer.empty()) {
    token_allowed_issuer = SafeGetEnvVarValue("TOKEN_ALLOWED_ISSUER");
  }
  if (token_allowed_audience.empty()) {
    token_allowed_audience = SafeGetEnvVarValue("TOKEN_ALLOWED_AUDIENCE");
  }
  if (token_signature_verify_cert_path.empty()) {
    token_signature_verify_cert_path =
        fs::path(SafeGetEnvVarValue("TOKEN_SIGNATURE_VERIFY_CERT_PATH"));
    if (!token_signature_verify_cert_path.empty()) {
      token_signature_verify_cert_path = fs::absolute(token_signature_verify_cert_path);
      if (!fs::exists(token_signature_verify_cert_path)) {
        return arrow::Status::Invalid(
            "Token signature verification certificate file does "
            "not exist: " +
            token_signature_verify_cert_path.string());
      }
    }
  }

  if (!token_allowed_issuer.empty()) {
    GIZMOSQL_LOG(INFO)
        << "INFO - Using token authentication - the token allowed issuer is set to: '"
        << token_allowed_issuer << "'";
    if (token_allowed_audience.empty()) {
      return arrow::Status::Invalid(
          "The token allowed issuer is set, but audience is not set.  "
          "Pass a value to this argument to secure the server.");
    }
    if (token_signature_verify_cert_path.empty()) {
      return arrow::Status::Invalid(
          "The token allowed issuer is set, but token signature verification certificate "
          "path is not set.");
    }
    GIZMOSQL_LOG(INFO) << "INFO - The token audience is set to: '"
                       << token_allowed_audience << "'";
    GIZMOSQL_LOG(INFO)
        << "INFO - The token signature verification certificate path is set to: "
        << token_signature_verify_cert_path.string();
  }

  GIZMOSQL_LOG(INFO) << "Query timeout (in seconds) is set to: " << query_timeout
                     << (query_timeout == 0 ? " (unlimited)" : "");

  return FlightSQLServerBuilder(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands, read_only,
      print_queries, token_allowed_issuer, token_allowed_audience,
      token_signature_verify_cert_path, access_logging_enabled, query_timeout);
}

arrow::Status StartFlightSQLServer(
    std::shared_ptr<flight::sql::FlightSqlServerBase> server) {
  return arrow::Status::OK();
}
}  // namespace gizmosql

extern "C" {
int RunFlightSQLServer(const BackendType backend, fs::path database_filename,
                       std::string hostname, const int& port, std::string username,
                       std::string password, std::string secret_key,
                       fs::path tls_cert_path, fs::path tls_key_path,
                       fs::path mtls_ca_cert_path, std::string init_sql_commands,
                       fs::path init_sql_commands_file, const bool& print_queries,
                       const bool& read_only, std::string token_allowed_issuer,
                       std::string token_allowed_audience,
                       fs::path token_signature_verify_cert_path, std::string log_level,
                       std::string log_format, std::string access_log,
                       std::string log_file, int32_t query_timeout) {
  // ---- Logging normalization (library-owned) ----------------
  auto lower = [](std::string s) {
    for (auto& c : s) c = static_cast<char>(std::tolower((unsigned char)c));
    return s;
  };

  auto pick = [&](std::string v, const char* env_name, std::string def) -> std::string {
    if (!v.empty()) return v;
    auto env = gizmosql::SafeGetEnvVarValue(env_name);
    if (!env.empty()) return env;
    return def;
  };

  std::string lvl_s = pick(log_level, "GIZMOSQL_LOG_LEVEL", "info");
  std::string fmt_s = pick(log_format, "GIZMOSQL_LOG_FORMAT", "text");
  std::string acc_s = pick(access_log, "GIZMOSQL_ACCESS_LOG", "off");
  std::string file_s = pick(log_file, "GIZMOSQL_LOG_FILE", "");

  // level
  arrow::util::ArrowLogLevel level = arrow::util::ArrowLogLevel::ARROW_INFO;
  {
    auto v = lower(lvl_s);
    using L = arrow::util::ArrowLogLevel;
    if (v == "debug")
      level = L::ARROW_DEBUG;
    else if (v == "info")
      level = L::ARROW_INFO;
    else if (v == "warn" || v == "warning")
      level = L::ARROW_WARNING;
    else if (v == "error")
      level = L::ARROW_ERROR;
    else if (v == "fatal")
      level = L::ARROW_FATAL;
    else
      std::cerr << "Unknown log-level '" << lvl_s << "', defaulting to INFO\n";
  }

  // format
  gizmosql::LogFormat fmt = gizmosql::LogFormat::kText;
  {
    auto v = lower(fmt_s);
    if (v == "json") fmt = gizmosql::LogFormat::kJson;
  }

  // access on/off
  auto parse_bool = [&](std::string s, bool& out) -> bool {
    s = lower(std::move(s));
    if (s == "1" || s == "true" || s == "on" || s == "yes") {
      out = true;
      return true;
    }
    if (s == "0" || s == "false" || s == "off" || s == "no") {
      out = false;
      return true;
    }
    return false;
  };
  bool access_logging_enabled = true;
  if (!parse_bool(acc_s, access_logging_enabled)) {
    std::cerr << "Unknown access-log '" << acc_s << "', defaulting to on\n";
  }

  // build logger config and install (Arrow v21+)
  gizmosql::LogConfig log_config;
  log_config.level = level;
  log_config.format = fmt;
  log_config.component = std::string{"gizmosql_server"};
  if (!file_s.empty())
    log_config.file_path = file_s;  // "-" => stdout handled in InitLogging
  // ----------------------------------------------------------
  gizmosql::InitLogging(log_config);

  auto now = std::chrono::system_clock::now();
  std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
  std::tm* localTime = std::localtime(&currentTime);

  GIZMOSQL_LOG(INFO) << "GizmoSQL - Copyright © " << (1900 + localTime->tm_year)
                     << " GizmoData LLC"
                     << "\n Licensed under the Apache License, Version 2.0"
                     << "\n https://www.apache.org/licenses/LICENSE-2.0";

  auto create_server_result = gizmosql::CreateFlightSQLServer(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands,
      init_sql_commands_file, print_queries, read_only, token_allowed_issuer,
      token_allowed_audience, token_signature_verify_cert_path, access_logging_enabled,
      query_timeout);

  if (create_server_result.ok()) {
    auto server_ptr = create_server_result.ValueOrDie();
    GIZMOSQL_LOG(INFO) << "GizmoSQL server - started";
    ARROW_CHECK_OK(server_ptr->Serve());
    return EXIT_SUCCESS;
  } else {
    // Handle the error
    std::cerr << "Error: " << create_server_result.status().ToString();
    return EXIT_FAILURE;
  }
}
}