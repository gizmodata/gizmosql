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

#include <iostream>
#include <optional>
#include <string>

namespace gizmosql::client {

enum class OutputMode {
  TABLE,
  BOX,
  CSV,
  TABS,
  JSON,
  JSONLINES,
  MARKDOWN,
  LINE,
  LIST,
  HTML,
  LATEX,
  INSERT,
  QUOTE,
  ASCII,
  TRASH
};

std::string OutputModeToString(OutputMode mode);
std::optional<OutputMode> OutputModeFromString(const std::string& name);

struct ClientConfig {
  // Connection
  std::string host = "localhost";
  int port = 31337;
  std::string username;
  std::string password;
  bool password_provided = false;

  // TLS
  bool use_tls = false;
  std::string tls_roots;
  bool tls_skip_verify = false;
  std::string mtls_cert;
  std::string mtls_key;

  // OAuth/External Auth
  bool auth_type_external = false;
  int oauth_port = 31339;

  // Input/Output
  std::optional<std::string> command;
  std::optional<std::string> input_file;
  std::optional<std::string> output_file;
  std::optional<std::string> init_file;
  bool no_init = false;

  // Display
  OutputMode output_mode = OutputMode::BOX;
  bool show_headers = true;
  std::string null_value = "NULL";
  bool show_timer = false;
  bool quiet = false;
  bool echo = false;
  bool bail_on_error = false;
  std::string separator_col = "|";
  std::string separator_row = "\n";
  std::string insert_table = "table";
  int max_rows = 0;
  int max_width = 0;
  bool auto_width = false;  // When true, re-read terminal width before each render

  // Syntax highlighting
  bool syntax_highlighting = true;

  // Tags (sent as SET gizmosql.* after connecting)
  std::string session_tag;
  std::string query_tag;

  // Dynamic prompt (show catalog.schema)
  bool dynamic_prompt = true;

  // Built-in pager
  bool pager_enabled = true;
  int pager_threshold = 50;  // Minimum rows to trigger pager

  // Runtime
  bool is_interactive = false;
  std::string main_prompt = "gizmosql> ";
  std::string continuation_prompt = "       -> ";
  std::ostream* output_stream = &std::cout;
};

void ResolveEnvironmentVariables(ClientConfig& config);

/// Parse a gizmosql:// URI into config fields.
/// Returns true on success, false on parse error (writes to stderr).
bool ParseConnectionURI(const std::string& uri, ClientConfig& config);

/// Build a gizmosql:// URI from the current config (for display in .show).
/// Does not include password.
std::string BuildConnectionURI(const ClientConfig& config);

}  // namespace gizmosql::client
