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

#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#define BOOST_NO_CXX98_FUNCTION_BASE
#include <boost/program_options.hpp>

#include "client_config.hpp"
#include "command_processor.hpp"
#include "flight_connection.hpp"
#include "output_renderer.hpp"
#include "password_prompt.hpp"
#include "shell_loop.hpp"
#include "sql_processor.hpp"
#include "version.h"

namespace po = boost::program_options;
using namespace gizmosql::client;

int main(int argc, char** argv) {
  ClientConfig config;

  po::options_description desc("GizmoSQL Client Options");
  desc.add_options()
      ("help,?", "Show this help message")
      ("version,v", "Show version")

      // Connection
      ("host,h", po::value<std::string>(&config.host)->default_value("localhost"),
       "Server host (env: GIZMOSQL_HOST)")
      ("port,p", po::value<int>(&config.port)->default_value(31337),
       "Server port (env: GIZMOSQL_PORT)")
      ("username,u", po::value<std::string>(&config.username),
       "Username (env: GIZMOSQL_USER)")
      ("password,W", "Force password prompt (env: GIZMOSQL_PASSWORD)")

      // TLS
      ("tls", po::bool_switch(&config.use_tls),
       "Use TLS for connection (env: GIZMOSQL_TLS)")
      ("tls-roots", po::value<std::string>(&config.tls_roots),
       "Path to TLS root certificates (PEM) (env: GIZMOSQL_TLS_ROOTS)")
      ("tls-skip-verify", po::bool_switch(&config.tls_skip_verify),
       "Skip TLS server certificate verification")
      ("mtls-cert", po::value<std::string>(&config.mtls_cert),
       "Path to mTLS client certificate (PEM)")
      ("mtls-key", po::value<std::string>(&config.mtls_key),
       "Path to mTLS client private key (PEM)")

      // Auth type
      ("auth-type", po::value<std::string>()->default_value("password"),
       "Auth type: 'password' (default) or 'external' (OAuth/SSO)")
      ("oauth-port", po::value<int>(&config.oauth_port)->default_value(31339),
       "OAuth server port (env: GIZMOSQL_OAUTH_PORT)")

      // Input/Output
      ("command,c", po::value<std::string>(),
       "Execute SQL command and exit")
      ("file,f", po::value<std::string>(),
       "Execute SQL from file and exit")
      ("output,o", po::value<std::string>(),
       "Write output to file")
      ("init", po::value<std::string>(),
       "Init file to execute on startup (default: ~/.gizmosqlrc)")
      ("no-init", po::bool_switch(&config.no_init),
       "Do not execute init file")

      // Output mode shortcuts
      ("csv", "Set output mode to CSV")
      ("json", "Set output mode to JSON")
      ("table", "Set output mode to table (ASCII borders)")
      ("box", "Set output mode to box (Unicode borders, default)")
      ("markdown", "Set output mode to markdown")
      ("no-header", "Disable column headers")

      // URI
      ("uri", po::value<std::string>(),
       "Connection URI: gizmosql://HOST:PORT[?params] "
       "(cannot be combined with --host, --port, --username, --tls, etc.)")

      // Display
      ("quiet,q", po::bool_switch(&config.quiet),
       "Suppress welcome banner and informational messages")
      ("echo,e", po::bool_switch(&config.echo),
       "Echo executed SQL statements")
      ("bail", po::bool_switch(&config.bail_on_error),
       "Stop on first error")
      ("null", po::value<std::string>(&config.null_value)->default_value("NULL"),
       "String to display for NULL values")
  ;

  // Allow URI as positional argument
  po::positional_options_description pos;
  pos.add("uri", 1);

  po::variables_map vm;

  try {
    po::store(po::command_line_parser(argc, argv)
                  .options(desc)
                  .positional(pos)
                  .run(),
              vm);
    po::notify(vm);
  } catch (const po::error& e) {
    std::cerr << "Error: " << e.what() << std::endl;
    std::cerr << "Try '" << argv[0] << " --help' for more information."
              << std::endl;
    return 1;
  }

  if (vm.count("help")) {
    std::cout << "Usage: gizmosql_client [OPTIONS] [URI]" << std::endl;
    std::cout << std::endl;
    std::cout << desc << std::endl;
    return 0;
  }

  if (vm.count("version")) {
    std::cout << "GizmoSQL Client " << PROJECT_VERSION << std::endl;
    return 0;
  }

  // Resolve auth type
  std::string auth_type = vm["auth-type"].as<std::string>();
  if (auth_type == "external") {
    config.auth_type_external = true;
  } else if (auth_type != "password") {
    std::cerr << "Error: unknown auth-type '" << auth_type
              << "'. Use 'password' or 'external'." << std::endl;
    return 1;
  }

  // Parse connection URI if provided
  bool uri_provided = false;
  if (vm.count("uri")) {
    // --uri is mutually exclusive with individual connection flags
    std::vector<std::string> conflicts;
    if (vm.count("host") && !vm["host"].defaulted()) conflicts.push_back("--host");
    if (vm.count("port") && !vm["port"].defaulted()) conflicts.push_back("--port");
    if (vm.count("username"))   conflicts.push_back("--username");
    if (vm.count("password"))   conflicts.push_back("--password");
    if (vm.count("tls") && config.use_tls) conflicts.push_back("--tls");
    if (vm.count("tls-roots"))  conflicts.push_back("--tls-roots");
    if (vm.count("tls-skip-verify") && config.tls_skip_verify)
      conflicts.push_back("--tls-skip-verify");
    if (vm.count("auth-type") && !vm["auth-type"].defaulted())
      conflicts.push_back("--auth-type");

    if (!conflicts.empty()) {
      std::cerr << "Error: --uri cannot be combined with";
      for (size_t i = 0; i < conflicts.size(); ++i) {
        if (i > 0) std::cerr << ",";
        std::cerr << " " << conflicts[i];
      }
      std::cerr << std::endl;
      return 1;
    }

    std::string uri = vm["uri"].as<std::string>();
    if (!ParseConnectionURI(uri, config)) {
      return 1;
    }
    uri_provided = true;
  }

  // Resolve environment variables for unset options
  ResolveEnvironmentVariables(config);

  // Output mode shortcuts
  if (vm.count("csv"))      config.output_mode = OutputMode::CSV;
  if (vm.count("json"))     config.output_mode = OutputMode::JSON;
  if (vm.count("table"))    config.output_mode = OutputMode::TABLE;
  if (vm.count("box"))      config.output_mode = OutputMode::BOX;
  if (vm.count("markdown")) config.output_mode = OutputMode::MARKDOWN;
  if (vm.count("no-header")) config.show_headers = false;

  // Handle -c and -f
  if (vm.count("command")) config.command = vm["command"].as<std::string>();
  if (vm.count("file"))    config.input_file = vm["file"].as<std::string>();
  if (vm.count("output"))  config.output_file = vm["output"].as<std::string>();
  if (vm.count("init"))    config.init_file = vm["init"].as<std::string>();

  // Password resolution (only for password auth)
  // Like psql, --password/-W only forces the interactive prompt.
  // Password sources: GIZMOSQL_PASSWORD env var, or interactive prompt.
  if (!config.auth_type_external) {
    if (vm.count("password")) {
      // -W flag: force interactive prompt
      if (IsTerminal()) {
        config.password = PromptPassword();
        config.password_provided = true;
      } else {
        std::cerr << "Error: password prompt requires a terminal" << std::endl;
        return 1;
      }
    }

    // After env var resolution, if we still need a password and have a username
    if (!config.password_provided && !config.username.empty()) {
      if (IsTerminal()) {
        config.password = PromptPassword();
        config.password_provided = true;
      }
    }
  }

  // Determine if interactive
  config.is_interactive = !config.command.has_value() &&
                          !config.input_file.has_value() &&
                          IsTerminal();

  // Set truncation defaults for interactive box/table mode
  if (config.is_interactive && !config.output_file.has_value()) {
    config.max_rows = 40;
    config.max_width = GetTerminalWidth();
  }

  // Set up output file redirect
  std::ofstream output_file_stream;
  if (config.output_file.has_value()) {
    output_file_stream.open(*config.output_file);
    if (!output_file_stream.is_open()) {
      std::cerr << "Error: cannot open output file '" << *config.output_file
                << "'" << std::endl;
      return 1;
    }
    config.output_stream = &output_file_stream;
  }

  // Determine whether connection params were explicitly provided.
  // In interactive mode, we allow starting without a connection.
  auto env_set = [](const char* name) {
    const char* val = std::getenv(name);
    return val != nullptr && val[0] != '\0';
  };
  bool host_explicit = (vm.count("host") > 0 && !vm["host"].defaulted()) ||
                       env_set("GIZMOSQL_HOST");
  bool user_explicit = vm.count("username") > 0 ||
                       env_set("GIZMOSQL_USER");
  bool has_connection_params = host_explicit || user_explicit ||
                               config.auth_type_external || uri_provided;

  FlightConnection conn;

  if (has_connection_params) {
    auto connect_status = conn.Connect(config);
    if (!connect_status.ok()) {
      std::cerr << "Error: " << connect_status.ToString() << std::endl;
      return 1;
    }
  }
  // else: start in disconnected state — user can use .connect later

  SqlProcessor sql_proc;
  CommandProcessor cmd_proc(conn, config);

  if (config.is_interactive) {
    return RunShellLoop(conn, cmd_proc, sql_proc, config);
  } else {
    return RunNonInteractive(conn, cmd_proc, sql_proc, config);
  }
}
