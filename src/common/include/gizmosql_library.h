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
const int DEFAULT_HEALTH_PORT = 31338;  // Plaintext health check port for Kubernetes
const int DEFAULT_OAUTH_PORT = 31339;  // OAuth HTTP server port
const int32_t DEFAULT_QUERY_TIMEOUT_SECONDS = 0;  // Unlimited timeout

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
 * @param read_only Set to true to open the database in read-only mode; false otherwise. Default is false.
 * @param token_allowed_issuer The allowed token issuer for JWT token-based authentication. Default is an empty string.
 * @param token_allowed_audience The allowed token audience for JWT token-based authentication. Default is an empty string.
 * @param token_signature_verify_cert_path The path to the RSA PEM certificate file used for verifying tokens in JWT token-based authentication. Default is an empty path.
 * @param token_jwks_uri [Enterprise] Direct URL to a JWKS endpoint for token verification. If not set, uses env var GIZMOSQL_TOKEN_JWKS_URI. When token_allowed_issuer is set without a cert path or JWKS URI, auto-discovers JWKS from the issuer's .well-known/openid-configuration.
 * @param token_default_role Default role to assign when an external token lacks a 'role' claim. If not set, uses env var GIZMOSQL_TOKEN_DEFAULT_ROLE. If a token has no 'role' claim and no default role is configured, the token is rejected.
 * @param token_authorized_emails [Enterprise] Comma-separated list of authorized email patterns for OIDC user filtering. Supports wildcards (e.g., "*@company.com,admin@partner.com"). Default is "*" (all authenticated users allowed). If not set, uses env var GIZMOSQL_TOKEN_AUTHORIZED_EMAILS.
 * @param log_level The logging level to use for the server.  Default is: info
 * @param log_format The logging format to use.  Default is: text
 * @param access_log Whether or not to log client access to the server.  It is VERY verbose.  Default is: off
 * @param log_file The log file to use.  If not set, we log to stdout.  Default is: none (stdout)
 * @param query_timeout The query timeout in seconds.  Queries running longer than this will be canceled by the server.  Default is: 0 (no timeout)
 * @param query_log_level The logging level to use for the queries run by users.  Default is: info
 * @param auth_log_level The logging level to use for authentication to the server.  Default is: info
 * @param health_port The port for the plaintext gRPC health check server (for Kubernetes probes). Default is DEFAULT_HEALTH_PORT (31338). Set to 0 to disable.
 * @param health_check_query The SQL query to use for health checks. If empty, uses env var GIZMOSQL_HEALTH_CHECK_QUERY, or defaults to "SELECT 1".
 * @param enable_instrumentation [Enterprise] Whether to enable session instrumentation (tracking instances, sessions, SQL statements). Default is false. Requires valid enterprise license.
 * @param instrumentation_db_path [Enterprise] Path for the instrumentation database. If empty, uses env var GIZMOSQL_INSTRUMENTATION_DB_PATH, or defaults to gizmosql_instrumentation.db in the same directory as the main database. Ignored if instrumentation_catalog is set.
 * @param instrumentation_catalog [Enterprise] Catalog name for instrumentation. If set, uses a pre-attached catalog (e.g., DuckLake) instead of a file. The catalog must be attached via init_sql_commands. If empty, uses env var GIZMOSQL_INSTRUMENTATION_CATALOG.
 * @param instrumentation_schema [Enterprise] Schema within the instrumentation catalog. Default is "main". If empty, uses env var GIZMOSQL_INSTRUMENTATION_SCHEMA, or defaults to "main".
 * @param license_key_file Path to the GizmoSQL Enterprise license key file (JWT format). If empty, uses env var GIZMOSQL_LICENSE_KEY_FILE. Required for enterprise features.
 * @param allow_cross_instance_tokens Allow tokens issued by other server instances (with the same secret key) to be accepted. Default is false (strict mode). Useful for load-balanced deployments where clients may reconnect to different instances.
 * @param oauth_client_id [Enterprise] OAuth client ID. Setting this enables the server-side OAuth code exchange flow via a dedicated HTTP server. The server becomes a confidential OAuth client, handling browser redirects and token exchange. Requires --token-allowed-issuer and --token-allowed-audience. If not set, uses env var GIZMOSQL_OAUTH_CLIENT_ID.
 * @param oauth_client_secret [Enterprise] OAuth client secret (confidential, stays on server). If not set, uses env var GIZMOSQL_OAUTH_CLIENT_SECRET.
 * @param oauth_scopes [Enterprise] OAuth scopes to request during authorization. Default is "openid profile email". If not set, uses env var GIZMOSQL_OAUTH_SCOPES.
 * @param oauth_port [Enterprise] Port for the OAuth HTTP(S) server. Default is DEFAULT_OAUTH_PORT (31339). Set to 0 to disable. If not set, uses env var GIZMOSQL_OAUTH_PORT.
 * @param oauth_base_url [Enterprise] Override the base URL for the OAuth HTTP server (e.g., "https://my-proxy:443"). When set, the redirect URI (/oauth/callback) and discovery URL advertised to clients are derived from this. Auto-constructed from scheme + localhost + oauth_port if empty. Use this when the server is behind a reverse proxy or accessed remotely. If not set, uses env var GIZMOSQL_OAUTH_BASE_URL.
 * @param oauth_disable_tls [Enterprise] Disable TLS on the OAuth HTTP callback server even when the main Flight server uses TLS. WARNING: This should ONLY be used for localhost development/testing. If not set, uses env var GIZMOSQL_OAUTH_DISABLE_TLS (1/true to enable).
 *
 * @return Returns an integer status code. 0 indicates success, and non-zero values indicate errors.
 */

namespace gizmosql {
/// Clean up global server resources (health services, instrumentation manager).
/// This is primarily for testing use where multiple servers may be created.
void CleanupServerResources();
}  // namespace gizmosql

extern "C" {
int RunFlightSQLServer(
    const BackendType backend,
    std::filesystem::path database_filename = std::filesystem::path(),
    std::string hostname = "", const int& port = DEFAULT_FLIGHT_PORT,
    std::string username = "", std::string password = "", std::string secret_key = "",
    std::filesystem::path tls_cert_path = std::filesystem::path(),
    std::filesystem::path tls_key_path = std::filesystem::path(),
    std::filesystem::path mtls_ca_cert_path = std::filesystem::path(),
    std::string init_sql_commands = "",
    std::filesystem::path init_sql_commands_file = std::filesystem::path(),
    const bool& print_queries = false, const bool& read_only = false,
    std::string token_allowed_issuer = "", std::string token_allowed_audience = "",
    std::filesystem::path token_signature_verify_cert_path = std::filesystem::path(),
    std::string token_jwks_uri = "",
    std::string token_default_role = "",
    std::string token_authorized_emails = "",
    std::string log_level = "", std::string log_format = "", std::string access_log = "",
    std::string log_file = "", int32_t query_timeout = 0,
    std::string query_log_level = "", std::string auth_log_level = "",
    int health_port = DEFAULT_HEALTH_PORT,
    std::string health_check_query = "",
    const bool& enable_instrumentation = false,
    std::string instrumentation_db_path = "",
    std::string instrumentation_catalog = "",
    std::string instrumentation_schema = "",
    std::string license_key_file = "",
    const bool& allow_cross_instance_tokens = false,
    std::string oauth_client_id = "",
    std::string oauth_client_secret = "",
    std::string oauth_scopes = "",
    int oauth_port = 0,
    std::string oauth_base_url = "",
    const bool& oauth_disable_tls = false);
}