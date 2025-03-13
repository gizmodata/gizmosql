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

#include "library/include/gizmosql_library.h"
#include <iostream>
#include <boost/program_options.hpp>

namespace po = boost::program_options;
namespace fs = std::filesystem;

int main(int argc, char **argv) {
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
            ("license-key-filename,L", po::value<std::string>()->default_value(""),
              "Specify the license key file path.  "
              "If not set, we will use env var: 'LICENSE_KEY_FILENAME'.");
  // clang-format on

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 0;
  }

  if (vm.count("version")) {
    std::cout << "GizmoSQL Server CLI: " << GIZMOSQL_SERVER_VERSION << std::endl;
    return 0;
  }

  std::string backend_str = vm["backend"].as<std::string>();
  BackendType backend;
  if (backend_str == "duckdb") {
    backend = BackendType::duckdb;
  } else if (backend_str == "sqlite") {
    backend = BackendType::sqlite;
  } else {
    std::cout << "Invalid backend: " << backend_str << std::endl;
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
      std::cout << "--tls requires 2 entries - separated by a space!" << std::endl;
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

  auto license_key_filename = fs::path(vm["license-key-filename"].as<std::string>());

  return RunFlightSQLServer(backend, database_filename, hostname, port, username,
                            password, secret_key, tls_cert_path, tls_key_path,
                            mtls_ca_cert_path, init_sql_commands, init_sql_commands_file,
                            print_queries, read_only, license_key_filename);
}
