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

#include "include/gizmosql_library.h"

#include <cstdlib>
#include <csignal>
#include <iostream>
#include <filesystem>
#include <regex>
#include <vector>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <thread>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/server.h>
#include <arrow/util/logging.h>
#include <arrow/record_batch.h>
#include <boost/algorithm/string.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "sqlite_server.h"
#include "duckdb_server.h"
#include "include/flight_sql_fwd.h"
#include "include/gizmosql_security.h"

namespace fs = std::filesystem;

namespace gizmosql {

const int port = 31337;

// Helper function to get current timestamp in ISO8601 format
std::string GetISO8601Timestamp() {
  auto now = std::chrono::system_clock::now();
  auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
  auto now_s = now_ns / 1000000000;
  auto now_ns_part = now_ns % 1000000000;
  
  std::time_t time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm* tm_now = std::gmtime(&time_t_now);
  
  std::ostringstream oss;
  oss << std::put_time(tm_now, "%Y-%m-%dT%H:%M:%S");
  oss << "." << std::setfill('0') << std::setw(9) << now_ns_part << "Z";
  return oss.str();
}

// Helper function to get thread name
std::string GetThreadName() {
  std::ostringstream oss;
  oss << "thread-" << std::this_thread::get_id();
  return oss.str();
}

// Helper function for consistent logging with level checking
void LogMessage(const std::string &level, const std::string &message, 
                const std::string &log_format, 
                const std::map<std::string, std::string> &extra_fields = {}) {
  // Check if the log level is enabled using Arrow's log level
  arrow::util::ArrowLogLevel arrow_level;
  if (level == "ERROR") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_ERROR;
  } else if (level == "WARNING") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_WARNING;
  } else if (level == "INFO") {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_INFO;
  } else {
    arrow_level = arrow::util::ArrowLogLevel::ARROW_DEBUG;
  }
  
  // Only log if the level is enabled
  if (!arrow::util::ArrowLog::IsLevelEnabled(arrow_level)) {
    return;
  }
  
  if (log_format == "json") {
    // Build message with extra fields for readability
    std::string full_message = message;
    if (!extra_fields.empty()) {
      full_message += " [";
      bool first = true;
      for (const auto& [key, value] : extra_fields) {
        if (!first) full_message += ", ";
        full_message += key + "=" + value;
        first = false;
      }
      full_message += "]";
    }
    
    // Output in the required format
    std::cout << R"({)"
              << R"("@timestamp":")" << GetISO8601Timestamp() << R"(",)"
              << R"("@version":"1",)"
              << R"("message":")" << full_message << R"(",)"
              << R"("logger_name":"gizmosql_server",)"
              << R"("thread_name":")" << GetThreadName() << R"(",)"
              << R"("level":")" << level << R"(")"
              << R"(})" << std::endl;
  } else {
    // Use Arrow's logging for non-JSON format
    if (level == "ERROR") {
      ARROW_LOG(ERROR) << message;
    } else if (level == "WARNING") {
      ARROW_LOG(WARNING) << message;
    } else {
      ARROW_LOG(INFO) << message;
    }
  }
}

#define RUN_INIT_COMMANDS(serverType, init_sql_commands, log_format)               \
  do {                                                                             \
    if (init_sql_commands != "") {                                                 \
      std::regex regex_pattern(";(?=(?:[^']*'[^']*')*[^']*$)");                    \
      std::sregex_token_iterator iter(init_sql_commands.begin(),                   \
                                      init_sql_commands.end(), regex_pattern, -1); \
      std::sregex_token_iterator end;                                              \
      while (iter != end) {                                                        \
        std::string init_sql_command = *iter;                                      \
        if (init_sql_command.empty()) continue;                                    \
        gizmosql::LogMessage("INFO", "Running Init SQL command: " + init_sql_command,        \
                   log_format);                                                     \
        ARROW_RETURN_NOT_OK(serverType->ExecuteSql(init_sql_command));             \
        ++iter;                                                                    \
      }                                                                            \
    }                                                                              \
  } while (false)

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>> FlightSQLServerBuilder(
    const BackendType backend, const fs::path &database_filename,
    const std::string &hostname, const int &port, const std::string &username,
    const std::string &password, const std::string &secret_key,
    const fs::path &tls_cert_path, const fs::path &tls_key_path,
    const fs::path &mtls_ca_cert_path, const std::string &init_sql_commands,
    const bool &read_only, const bool &print_queries, const std::string &log_format) {
  ARROW_ASSIGN_OR_RAISE(auto location,
                        (!tls_cert_path.empty())
                            ? flight::Location::ForGrpcTls(hostname, port)
                            : flight::Location::ForGrpcTcp(hostname, port));

  LogMessage("INFO", "Apache Arrow version: " + std::string(ARROW_VERSION_STRING), log_format);

  flight::FlightServerOptions options(location);

  if (!tls_cert_path.empty() && !tls_key_path.empty()) {
    ARROW_CHECK_OK(gizmosql::SecurityUtilities::FlightServerTlsCertificates(
        tls_cert_path, tls_key_path, &options.tls_certificates));
  } else {
    LogMessage("WARNING", "TLS is disabled for the GizmoSQL server - this is NOT secure", log_format);
  }

  // Setup authentication middleware (using the same TLS certificate keypair)
  auto header_middleware = std::make_shared<gizmosql::HeaderAuthServerMiddlewareFactory>(
      username, password, secret_key);
  auto bearer_middleware =
      std::make_shared<gizmosql::BearerAuthServerMiddlewareFactory>(secret_key);

  options.auth_handler = std::make_unique<flight::NoOpAuthHandler>();
  options.middleware.push_back({"header-auth-server", header_middleware});
  options.middleware.push_back({"bearer-auth-server", bearer_middleware});

  if (!mtls_ca_cert_path.empty()) {
    LogMessage("INFO", "Using mTLS CA certificate: " + mtls_ca_cert_path.string(), log_format);
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
    RUN_INIT_COMMANDS(sqlite_server, init_sql_commands, log_format);
    server = sqlite_server;
  } else if (backend == BackendType::duckdb) {
    db_type = "DuckDB";
    std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> duckdb_server = nullptr;
    ARROW_ASSIGN_OR_RAISE(duckdb_server, gizmosql::ddb::DuckDBFlightSqlServer::Create(
                                             database_filename, read_only, print_queries, log_format))
    // Run additional commands (first) for the DuckDB back-end...
    auto duckdb_init_sql_commands =
        "SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;" +
        init_sql_commands;
    RUN_INIT_COMMANDS(duckdb_server, duckdb_init_sql_commands, log_format);
    server = duckdb_server;
  }

  LogMessage("INFO", "Using database file: " + database_filename.string(), log_format);
  LogMessage("INFO", "Print queries option: " + std::string(print_queries ? "enabled" : "disabled"), log_format);

  if (server != nullptr) {
    ARROW_CHECK_OK(server->Init(options));

    // Exit with a clean error code (0) on SIGTERM or SIGINT
    ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM, SIGINT}));

    LogMessage("INFO", "GizmoSQL server starting [version=" + std::string(GIZMOSQL_SERVER_VERSION) + 
               ", engine=" + db_type + ", location=" + server->location().ToString() + "]", log_format);

    return server;
  } else {
    std::string err_msg = "Unable to create the GizmoSQL Server";
    return arrow::Status::Invalid(err_msg);
  }
}

std::string SafeGetEnvVarValue(const std::string &env_var_name) {
  auto env_var_value = std::getenv(env_var_name.c_str());
  if (env_var_value) {
    return std::string(env_var_value);
  } else {
    return "";
  }
}

arrow::Result<std::shared_ptr<flight::sql::FlightSqlServerBase>> CreateFlightSQLServer(
    const BackendType backend, fs::path &database_filename, std::string hostname,
    const int &port, std::string username, std::string password, std::string secret_key,
    fs::path tls_cert_path, fs::path tls_key_path, fs::path mtls_ca_cert_path,
    std::string init_sql_commands, fs::path init_sql_commands_file,
    const bool &print_queries, const std::string &log_format, const bool &read_only) {

  // Validate and default the arguments to env var values where applicable
  if (database_filename.empty()) {
    LogMessage("WARNING", "The database filename was not provided, opening an in-memory database", log_format);
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
    LogMessage("WARNING", "Running in Read-Only mode - no changes will be persisted to the database", log_format);
  }

  return FlightSQLServerBuilder(backend, database_filename, hostname, port, username,
                                password, secret_key, tls_cert_path, tls_key_path,
                                mtls_ca_cert_path, init_sql_commands, read_only,
                                print_queries, log_format);
}

arrow::Status StartFlightSQLServer(
    std::shared_ptr<flight::sql::FlightSqlServerBase> server) {
  return arrow::Status::OK();
}

}  // namespace gizmosql

extern "C" {

int RunFlightSQLServer(const BackendType backend, fs::path database_filename,
                       std::string hostname, const int &port, std::string username,
                       std::string password, std::string secret_key,
                       fs::path tls_cert_path, fs::path tls_key_path,
                       fs::path mtls_ca_cert_path, std::string init_sql_commands,
                       fs::path init_sql_commands_file, const bool &print_queries,
                       const std::string &log_format, const bool &read_only) {
  auto now = std::chrono::system_clock::now();
  std::time_t currentTime = std::chrono::system_clock::to_time_t(now);
  std::tm *localTime = std::localtime(&currentTime);

  gizmosql::LogMessage("INFO", "GizmoSQL starting [copyright_year=" + std::to_string(1900 + localTime->tm_year) + 
             ", license=Apache License 2.0]", log_format);

  auto create_server_result = gizmosql::CreateFlightSQLServer(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands,
      init_sql_commands_file, print_queries, log_format, read_only);

  if (create_server_result.ok()) {
    auto server_ptr = create_server_result.ValueOrDie();
    gizmosql::LogMessage("INFO", "GizmoSQL server started", log_format);
    ARROW_CHECK_OK(server_ptr->Serve());
    return EXIT_SUCCESS;
  } else {
    // Handle the error
    gizmosql::LogMessage("ERROR", "Failed to start GizmoSQL server: " + create_server_result.status().ToString(), log_format);
    return EXIT_FAILURE;
  }
}
}
