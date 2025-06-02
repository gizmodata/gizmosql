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

#pragma once

#include <filesystem>
#include "version.h"

// Constants
const std::string GIZMOSQL_SERVER_VERSION = PROJECT_VERSION;
const std::string DEFAULT_GIZMOSQL_HOSTNAME = "0.0.0.0";
const std::string DEFAULT_GIZMOSQL_USERNAME = "gizmosql_username";
const int DEFAULT_FLIGHT_PORT = 31337;

enum class BackendType { duckdb, sqlite };

/**
 * @brief Run a GizmoSQL Server with the specified configuration.
 *
 * This function initializes and runs a GizmoSQL Server with the given parameters.
 *
 * @param backend The backend to use (duckdb or sqlite).
 * @param database_filename The path to the database file.
 * @param hostname The hostname for the GizmoSQL Server. Default is "" - if so, we use environment variable: "GIZMOSQL_HOSTNAME",
 *   and fallback to: DEFAULT_GIZMOSQL_HOSTNAME if that is not set.
 * @param port The port to listen on for the GizmoSQL Server. Default is DEFAULT_FLIGHT_PORT
 * @param username The username to use for authentication. Default is now "" - if not set, we use environment variable: "GIZMOSQL_USERNAME",
 *   if this is not defined we set this to "gizmosql_username" again in gizmosql_library.
 * @param password The password for authentication. Default is "" - if so, we use environment variable: "GIZMOSQL_PASSWORD",
 *   if both are not set, we exit with an error.
 * @param secret_key The secret key for authentication. Default is "", if so, we use environment variable: "SECRET_KEY",
     and fallback to a random string if both are not set.
 * @param tls_cert_path The path to the TLS certificate file (PEM format). Default is an empty path.
 * @param tls_key_path The path to the TLS private key file (PEM format). Default is an empty path.
 * @param mtls_ca_cert_path The path to the mTLS CA certificate file used to verify clients (in PEM format). Default is an empty path.
 * @param init_sql_commands The initial SQL commands to execute. Default is "" - if not set, we use environment variable: "INIT_SQL_COMMANDS".
 * @param init_sql_commands_file The path to a file containing initial SQL commands. Default is an empty path - if not set, we use environment variable: "INIT_SQL_COMMANDS_FILE"
 * @param print_queries Set to true if SQL queries should be printed; false otherwise. Default is false.
 *
 * @return Returns an integer status code. 0 indicates success, and non-zero values indicate errors.
 */

extern "C" {
int RunFlightSQLServer(
    const BackendType backend,
    std::filesystem::path database_filename = std::filesystem::path(),
    std::string hostname = "", const int &port = DEFAULT_FLIGHT_PORT,
    std::string username = "", std::string password = "", std::string secret_key = "",
    std::filesystem::path tls_cert_path = std::filesystem::path(),
    std::filesystem::path tls_key_path = std::filesystem::path(),
    std::filesystem::path mtls_ca_cert_path = std::filesystem::path(),
    std::string init_sql_commands = "",
    std::filesystem::path init_sql_commands_file = std::filesystem::path(),
    const bool &print_queries = false, const bool &read_only = false);
}
