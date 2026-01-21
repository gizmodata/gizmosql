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
#include <unistd.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <arrow/flight/client.h>
#include <arrow/flight/sql/server.h>
#include <arrow/util/config.h>
#include <boost/algorithm/string.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "sqlite_server.h"
#include "duckdb_server.h"
#include "flight_sql_fwd.h"
#include "gizmosql_logging.h"
#include "gizmosql_security.h"
#include "access_log_middleware.h"
#include "health_service.h"
#include "instrumentation_manager.h"
#include "instrumentation_records.h"
#include "version.h"

#include <grpcpp/grpcpp.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>

namespace fs = std::filesystem;

namespace gizmosql {
int port = 31337;

// Static storage for health service to keep it alive for the lifetime of the server
// This is safe because only one GizmoSQL server runs per process
static std::shared_ptr<GizmoSQLHealthServiceImpl> g_health_service;
static std::unique_ptr<PlaintextHealthServer> g_plaintext_health_server;

// Static storage for instrumentation
static std::shared_ptr<gizmosql::ddb::InstrumentationManager> g_instrumentation_manager;

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

// Get the actual hostname of the machine from the OS
static std::string GetActualHostname() {
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) == 0) {
    return std::string(hostname);
  }
  return "";
}

// Get the primary IP address of the server
// Prioritizes common primary interface names (en0/eth0), falls back to any non-loopback IPv4
static std::string GetPrimaryIPAddress() {
  struct ifaddrs* ifaddr = nullptr;
  if (getifaddrs(&ifaddr) == -1) {
    return "";
  }

  std::string primary_ip;
  std::string fallback_ip;

  for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == nullptr) continue;

    // Only consider IPv4 addresses
    if (ifa->ifa_addr->sa_family != AF_INET) continue;

    auto* addr = reinterpret_cast<struct sockaddr_in*>(ifa->ifa_addr);
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr->sin_addr, ip_str, sizeof(ip_str));

    std::string ip(ip_str);
    std::string name(ifa->ifa_name);

    // Skip loopback
    if (ip.rfind("127.", 0) == 0) continue;

    // Prioritize common primary interfaces (macOS: en0, Linux: eth0, ens*, enp*)
    if (name == "en0" || name == "eth0" || name.rfind("ens", 0) == 0 ||
        name.rfind("enp", 0) == 0) {
      primary_ip = ip;
      break;  // Found a primary interface, use it
    }

    // Keep track of any non-loopback IP as fallback
    if (fallback_ip.empty()) {
      fallback_ip = ip;
    }
  }

  freeifaddrs(ifaddr);
  return primary_ip.empty() ? fallback_ip : primary_ip;
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
    const int32_t& query_timeout, const arrow::util::ArrowLogLevel& query_log_level,
    const arrow::util::ArrowLogLevel& auth_log_level, const int& health_port,
    const bool& enable_instrumentation,
    const std::string& instrumentation_db_path) {
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
  bool tls_enabled = !tls_cert_path.empty();
  bool mtls_enabled = !mtls_ca_cert_path.empty();
  auto header_middleware = std::make_shared<gizmosql::BasicAuthServerMiddlewareFactory>(
      username, password, secret_key, token_allowed_issuer, token_allowed_audience,
      token_signature_verify_cert_path, auth_log_level, tls_enabled, mtls_enabled);
  auto bearer_middleware = std::make_shared<gizmosql::BearerAuthServerMiddlewareFactory>(
      secret_key, auth_log_level);

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

    // Create DuckDB server first (without instrumentation manager)
    std::shared_ptr<gizmosql::ddb::DuckDBFlightSqlServer> duckdb_server = nullptr;
    ARROW_ASSIGN_OR_RAISE(duckdb_server, gizmosql::ddb::DuckDBFlightSqlServer::Create(
                                             database_filename, read_only, print_queries,
                                             query_timeout, query_log_level,
                                             nullptr))  // No instrumentation manager yet

    // Set instance_id for all future log entries (enables log correlation)
    auto instance_id = duckdb_server->GetInstanceId();
    gizmosql::SetInstanceId(instance_id);

    // Set instance_id on auth middleware for JWT token creation and validation
    header_middleware->SetInstanceId(instance_id);
    bearer_middleware->SetInstanceId(instance_id);

    GIZMOSQL_LOG(INFO) << "Server instance created";

    // Run DuckDB init commands first
    std::string duckdb_init_sql_commands =
        "SET autoinstall_known_extensions = true; SET autoload_known_extensions = true;";

    // Instrumentation setup (conditional)
    std::string instr_db_path;
    if (enable_instrumentation) {
      // Get instrumentation DB path: CLI arg > env var > default
      if (!instrumentation_db_path.empty()) {
        instr_db_path = instrumentation_db_path;
      } else {
        // GetDefaultDbPath checks GIZMOSQL_INSTRUMENTATION_DB_PATH env var internally
        instr_db_path = gizmosql::ddb::InstrumentationManager::GetDefaultDbPath(
            database_filename.string());
      }
      GIZMOSQL_LOG(INFO) << "Instrumentation database path: " << instr_db_path;
      duckdb_init_sql_commands += "ATTACH '" + instr_db_path + "' AS _gizmosql_instr;";
    } else {
      GIZMOSQL_LOG(INFO) << "Instrumentation is disabled";
    }

    duckdb_init_sql_commands += init_sql_commands;
    RUN_INIT_COMMANDS(duckdb_server, duckdb_init_sql_commands);

    // Now initialize instrumentation manager using the server's DuckDB instance (if enabled)
    if (enable_instrumentation) {
      auto db_instance = duckdb_server->GetDuckDBInstance();
      auto instr_result = gizmosql::ddb::InstrumentationManager::Create(db_instance, instr_db_path);
      if (instr_result.ok()) {
        g_instrumentation_manager = *instr_result;
        duckdb_server->SetInstrumentationManager(g_instrumentation_manager);
        GIZMOSQL_LOG(INFO) << "Instrumentation enabled at: "
                           << g_instrumentation_manager->GetDbPath();

        // Create instance instrumentation record and pass to server
        // The server owns the instance_id - instrumentation receives it, not generates it
        gizmosql::ddb::InstanceConfig instance_config{
            .instance_id = duckdb_server->GetInstanceId(),
            .gizmosql_version = PROJECT_VERSION,
            .duckdb_version = duckdb_library_version(),
            .arrow_version = ARROW_VERSION_STRING,
            .hostname = GetActualHostname(),
            .hostname_arg = hostname,
            .server_ip = GetPrimaryIPAddress(),
            .port = port,
            .database_path = database_filename.string(),
            .tls_enabled = !tls_cert_path.empty(),
            .tls_cert_path = tls_cert_path.string(),
            .tls_key_path = tls_key_path.string(),
            .mtls_required = !mtls_ca_cert_path.empty(),
            .mtls_ca_cert_path = mtls_ca_cert_path.string(),
            .readonly = read_only,
        };
        auto instance_instr = std::make_unique<gizmosql::ddb::InstanceInstrumentation>(
            g_instrumentation_manager, instance_config);
        duckdb_server->SetInstanceInstrumentation(std::move(instance_instr));
      } else {
        GIZMOSQL_LOG(WARNING) << "Failed to initialize instrumentation: "
                              << instr_result.status().ToString();
      }
    }

    server = duckdb_server;
  }

  GIZMOSQL_LOG(INFO) << "Using database file: " << database_filename;

  GIZMOSQL_LOG(INFO) << "Print Queries option is set to: " << std::boolalpha
                     << print_queries;

  if (server != nullptr) {
    // Create health check service
    // The health check function will execute "SELECT 1" on the backend to verify connectivity
    // Using g_health_service (static) to ensure the service lives as long as the server
    if (backend == BackendType::sqlite) {
      auto sqlite_server =
          std::dynamic_pointer_cast<gizmosql::sqlite::SQLiteFlightSqlServer>(server);
      g_health_service = std::make_shared<GizmoSQLHealthServiceImpl>(
          [weak_server = std::weak_ptr<gizmosql::sqlite::SQLiteFlightSqlServer>(
               sqlite_server)]() -> bool {
            if (auto s = weak_server.lock()) {
              return s->ExecuteSql("SELECT 1").ok();
            }
            return false;
          });
    } else if (backend == BackendType::duckdb) {
      auto duckdb_server =
          std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server);
      g_health_service = std::make_shared<GizmoSQLHealthServiceImpl>(
          [weak_server = std::weak_ptr<gizmosql::ddb::DuckDBFlightSqlServer>(
               duckdb_server)]() -> bool {
            if (auto s = weak_server.lock()) {
              return s->ExecuteSql("SELECT 1").ok();
            }
            return false;
          });
    }

    // Set up builder_hook to register the health service and reflection with the gRPC server
    if (g_health_service) {
      // Enable gRPC reflection (must be called before building the server)
      grpc::reflection::InitProtoReflectionServerBuilderPlugin();

      options.builder_hook = [health_service = g_health_service](void* raw_builder) {
        auto* builder = reinterpret_cast<grpc::ServerBuilder*>(raw_builder);
        builder->RegisterService(health_service.get());
      };
      GIZMOSQL_LOG(INFO) << "gRPC Health service enabled (on main TLS port)";
      GIZMOSQL_LOG(INFO) << "gRPC Reflection service enabled";
    }

    ARROW_CHECK_OK(server->Init(options));

    // Exit with a clean error code (0) on SIGTERM or SIGINT
    ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM, SIGINT}));

    // Start plaintext health server for Kubernetes probes AFTER main server init succeeds.
    // This ensures we don't leave orphaned servers if main server initialization fails.
    if (g_health_service) {
      if (health_port > 0) {
        auto plaintext_result = PlaintextHealthServer::Start(g_health_service, health_port);
        if (plaintext_result.ok()) {
          g_plaintext_health_server = std::move(*plaintext_result);
        } else {
          GIZMOSQL_LOG(WARNING) << "Failed to start plaintext health server: "
                                << plaintext_result.status().ToString();
        }
      } else {
        GIZMOSQL_LOG(INFO) << "Plaintext health server disabled (health_port=0)";
      }
    }

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
  int port, std::string username, std::string password, std::string secret_key,
    fs::path tls_cert_path, fs::path tls_key_path, fs::path mtls_ca_cert_path,
    std::string init_sql_commands, fs::path init_sql_commands_file,
    const bool& print_queries, const bool& read_only, std::string token_allowed_issuer,
    std::string token_allowed_audience, fs::path token_signature_verify_cert_path,
    const bool& access_logging_enabled, const int32_t& query_timeout,
    const arrow::util::ArrowLogLevel& query_log_level,
    const arrow::util::ArrowLogLevel& auth_log_level, const int& health_port,
    const bool& enable_instrumentation,
    std::string instrumentation_db_path) {
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

  if (SafeGetEnvVarValue("GIZMOSQL_PORT") != "") {
    try {
      int env_port = std::stoi(SafeGetEnvVarValue("GIZMOSQL_PORT"));
      if (env_port == health_port) {
        return arrow::Status::Invalid(
            "GIZMOSQL_PORT environment variable cannot be the same as health_port");
      }
      if (env_port > 0 && env_port < 65536) {
        GIZMOSQL_LOG(INFO) << "Using GizmoSQL port from env var GIZMOSQL_PORT: " << env_port;
        port = env_port;
      } else {
        return arrow::Status::Invalid(
            "GIZMOSQL_PORT environment variable is set but is not a valid port number");
      }
    } catch (const std::exception& e) {
      return arrow::Status::Invalid(
          "GIZMOSQL_PORT environment variable is set but is not a valid integer");
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

  GIZMOSQL_LOG(INFO) << "Query Log Level is set to: "
                     << log_level_arrow_log_level_to_string(query_log_level);
  GIZMOSQL_LOG(INFO) << "Authentication Log Level is set to: "
                     << log_level_arrow_log_level_to_string(auth_log_level);

  return FlightSQLServerBuilder(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands, read_only,
      print_queries, token_allowed_issuer, token_allowed_audience,
      token_signature_verify_cert_path, access_logging_enabled, query_timeout,
      query_log_level, auth_log_level, health_port, enable_instrumentation,
      instrumentation_db_path);
}

arrow::Status StartFlightSQLServer(
    std::shared_ptr<flight::sql::FlightSqlServerBase> server) {
  return arrow::Status::OK();
}

void CleanupServerResources() {
  // Shutdown and reset health services
  if (g_plaintext_health_server) {
    g_plaintext_health_server->Shutdown();
    g_plaintext_health_server.reset();
  }
  if (g_health_service) {
    g_health_service->Shutdown();
    g_health_service.reset();
  }

  // Shutdown and reset instrumentation manager
  if (g_instrumentation_manager) {
    g_instrumentation_manager->Shutdown();
    g_instrumentation_manager.reset();
  }
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
                       std::string log_file, int32_t query_timeout,
                       std::string query_log_level, std::string auth_log_level,
                       int health_port, const bool& enable_instrumentation,
                       std::string instrumentation_db_path) {
  // ---- Logging normalization (library-owned) ----------------
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
  std::string query_lvl_s = pick(query_log_level, "GIZMOSQL_QUERY_LOG_LEVEL", "info");
  std::string auth_lvl_s = pick(auth_log_level, "GIZMOSQL_AUTH_LOG_LEVEL", "info");

  auto level = gizmosql::log_level_string_to_arrow_log_level(lvl_s);
  auto query_level = gizmosql::log_level_string_to_arrow_log_level(query_lvl_s);
  auto auth_level = gizmosql::log_level_string_to_arrow_log_level(auth_lvl_s);

  // format
  gizmosql::LogFormat fmt = gizmosql::LogFormat::kText;
  {
    auto v = gizmosql::lower(fmt_s);
    if (v == "json") fmt = gizmosql::LogFormat::kJson;
  }

  // access on/off
  auto parse_bool = [&](std::string s, bool& out) -> bool {
    s = gizmosql::lower(std::move(s));
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

  GIZMOSQL_LOG(INFO) << "Overall Log Level is set to: "
                     << lvl_s;

  auto create_server_result = gizmosql::CreateFlightSQLServer(
      backend, database_filename, hostname, port, username, password, secret_key,
      tls_cert_path, tls_key_path, mtls_ca_cert_path, init_sql_commands,
      init_sql_commands_file, print_queries, read_only, token_allowed_issuer,
      token_allowed_audience, token_signature_verify_cert_path, access_logging_enabled,
      query_timeout, query_level, auth_level, health_port, enable_instrumentation,
      instrumentation_db_path);

  if (create_server_result.ok()) {
    auto server_ptr = create_server_result.ValueOrDie();
    GIZMOSQL_LOG(INFO) << "GizmoSQL server - started";
    ARROW_CHECK_OK(server_ptr->Serve());

    // Gracefully shutdown health services
    if (gizmosql::g_plaintext_health_server) {
      gizmosql::g_plaintext_health_server->Shutdown();
      gizmosql::g_plaintext_health_server.reset();
    }
    if (gizmosql::g_health_service) {
      gizmosql::g_health_service->Shutdown();
      GIZMOSQL_LOG(INFO) << "Health service shutdown complete";
    }

    // Release sessions, statements, and instance instrumentation BEFORE shutting down the manager.
    // This ensures all instrumentation records are queued while the manager can still accept writes.
    // The manager's Shutdown() will then drain the queue.
    // Order: statements -> sessions -> instance (child records before parent records)
    if (auto duckdb_server = std::dynamic_pointer_cast<gizmosql::ddb::DuckDBFlightSqlServer>(server_ptr)) {
      auto instance_id = duckdb_server->GetInstanceId();
      // Release statements and sessions first (closes their instrumentation records)
      duckdb_server->ReleaseAllSessions();
      // Then release instance instrumentation
      duckdb_server->ReleaseInstanceInstrumentation();
      GIZMOSQL_LOG(INFO) << "Instance " << instance_id << " instrumentation released";
    }

    if (gizmosql::g_instrumentation_manager) {
      gizmosql::g_instrumentation_manager->Shutdown();
      gizmosql::g_instrumentation_manager.reset();
      GIZMOSQL_LOG(INFO) << "Instrumentation manager shutdown complete";
    }

    // Now safe to destroy the server
    server_ptr.reset();

    return EXIT_SUCCESS;
  } else {
    // Handle the error
    std::cerr << "Error: " << create_server_result.status().ToString();
    return EXIT_FAILURE;
  }
}
}