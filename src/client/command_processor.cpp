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

#include "command_processor.hpp"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <vector>

#include <unistd.h>

#include <arrow/array.h>
#include <arrow/flight/sql/types.h>

#include "output_renderer.hpp"
#include "password_prompt.hpp"

namespace gizmosql::client {

namespace {

std::vector<std::string> SplitArgs(const std::string& line) {
  std::vector<std::string> args;
  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    args.push_back(token);
  }
  return args;
}

std::string ToLower(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

void RenderTable(const std::shared_ptr<arrow::Table>& table,
                 ClientConfig& config) {
  auto renderer = CreateRenderer(config.output_mode, config);
  renderer->Render(*table, *config.output_stream);
}

// Extract a string value from a SqlInfo result table for a given info_name key.
// The table has columns: info_name (uint32), value (dense union).
// Returns empty string if not found or on error.
std::string ExtractSqlInfoString(const std::shared_ptr<arrow::Table>& table,
                                 int info_name) {
  if (!table || table->num_rows() == 0 || table->num_columns() < 2) return "";

  auto name_col = table->column(0);   // uint32
  auto value_col = table->column(1);   // dense union

  for (int chunk_idx = 0; chunk_idx < name_col->num_chunks(); ++chunk_idx) {
    auto name_array =
        std::static_pointer_cast<arrow::UInt32Array>(name_col->chunk(chunk_idx));
    auto union_array =
        std::static_pointer_cast<arrow::DenseUnionArray>(value_col->chunk(chunk_idx));

    for (int64_t r = 0; r < name_array->length(); ++r) {
      if (static_cast<int>(name_array->Value(r)) == info_name) {
        // Type code 0 = string in the SqlInfo union schema
        int8_t type_code = union_array->type_code(r);
        if (type_code == 0) {
          auto str_array = std::static_pointer_cast<arrow::StringArray>(
              union_array->field(0));
          return str_array->GetString(union_array->value_offset(r));
        }
        return "";
      }
    }
  }
  return "";
}

}  // namespace

bool CommandProcessor::IsDotCommand(const std::string& line) {
  size_t pos = line.find_first_not_of(" \t");
  return pos != std::string::npos && line[pos] == '.';
}

CommandResult CommandProcessor::Process(const std::string& line) {
  auto args = SplitArgs(line);
  if (args.empty()) return CommandResult::OK;

  std::string cmd = ToLower(args[0]);

  // .quit / .exit
  if (cmd == ".quit" || cmd == ".exit") {
    return CommandResult::EXIT;
  }

  // .connect HOST PORT USERNAME [PASSWORD]
  if (cmd == ".connect") {
    return HandleConnect(args);
  }

  // .help [PATTERN]
  if (cmd == ".help") {
    ShowHelp(args.size() > 1 ? args[1] : "");
    return CommandResult::OK;
  }

  // .tables [PATTERN]
  if (cmd == ".tables") {
    if (!conn_.IsConnected()) {
      std::cerr << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << std::endl;
      return CommandResult::ERROR;
    }
    std::string pattern = args.size() > 1 ? args[1] : "";
    auto result = conn_.GetTables("", "", pattern.empty() ? "" : "%" + pattern + "%");
    if (result.ok()) {
      RenderTable(*result, config_);
    } else {
      std::cerr << "Error: " << result.status().ToString() << std::endl;
    }
    return CommandResult::OK;
  }

  // .schema [PATTERN]
  if (cmd == ".schema") {
    if (!conn_.IsConnected()) {
      std::cerr << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << std::endl;
      return CommandResult::ERROR;
    }
    std::string pattern = args.size() > 1 ? args[1] : "";
    auto result = conn_.GetDbSchemas("", pattern.empty() ? "" : "%" + pattern + "%");
    if (result.ok()) {
      RenderTable(*result, config_);
    } else {
      std::cerr << "Error: " << result.status().ToString() << std::endl;
    }
    return CommandResult::OK;
  }

  // .catalogs
  if (cmd == ".catalogs") {
    if (!conn_.IsConnected()) {
      std::cerr << "Error: not connected to a server. Use '.connect HOST PORT "
                   "USERNAME' to connect."
                << std::endl;
      return CommandResult::ERROR;
    }
    auto result = conn_.GetCatalogs();
    if (result.ok()) {
      RenderTable(*result, config_);
    } else {
      std::cerr << "Error: " << result.status().ToString() << std::endl;
    }
    return CommandResult::OK;
  }

  // .mode MODE
  if (cmd == ".mode") {
    if (args.size() < 2) {
      *config_.output_stream << "current mode: "
                              << OutputModeToString(config_.output_mode)
                              << std::endl;
      return CommandResult::OK;
    }
    auto mode = OutputModeFromString(args[1]);
    if (mode.has_value()) {
      config_.output_mode = *mode;
    } else {
      std::cerr << "Error: unknown mode '" << args[1] << "'. Available modes:\n"
                << "  table box csv tabs json jsonlines markdown line list\n"
                << "  html latex insert quote ascii trash" << std::endl;
    }
    return CommandResult::OK;
  }

  // .headers on|off
  if (cmd == ".headers") {
    if (args.size() < 2) {
      *config_.output_stream << "headers: "
                              << (config_.show_headers ? "on" : "off")
                              << std::endl;
    } else {
      config_.show_headers = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .timer on|off
  if (cmd == ".timer") {
    if (args.size() < 2) {
      *config_.output_stream << "timer: "
                              << (config_.show_timer ? "on" : "off")
                              << std::endl;
    } else {
      config_.show_timer = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .output [FILE]
  if (cmd == ".output") {
    if (args.size() < 2 || args[1] == "stdout") {
      // Reset to stdout
      if (output_file_) {
        delete output_file_;
        output_file_ = nullptr;
      }
      config_.output_stream = &std::cout;
    } else {
      if (output_file_) {
        delete output_file_;
      }
      output_file_ = new std::ofstream(args[1]);
      if (!output_file_->is_open()) {
        std::cerr << "Error: cannot open file '" << args[1] << "'" << std::endl;
        delete output_file_;
        output_file_ = nullptr;
      } else {
        config_.output_stream = output_file_;
      }
    }
    return CommandResult::OK;
  }

  // .once FILE
  if (cmd == ".once") {
    if (args.size() < 2) {
      std::cerr << "Usage: .once FILE" << std::endl;
    } else {
      if (output_file_) {
        delete output_file_;
      }
      output_file_ = new std::ofstream(args[1]);
      if (!output_file_->is_open()) {
        std::cerr << "Error: cannot open file '" << args[1] << "'" << std::endl;
        delete output_file_;
        output_file_ = nullptr;
      } else {
        config_.output_stream = output_file_;
        once_mode_ = true;
      }
    }
    return CommandResult::OK;
  }

  // .nullvalue STRING
  if (cmd == ".nullvalue") {
    if (args.size() < 2) {
      *config_.output_stream << "nullvalue: \"" << config_.null_value << "\""
                              << std::endl;
    } else {
      config_.null_value = args[1];
    }
    return CommandResult::OK;
  }

  // .separator COL [ROW]
  if (cmd == ".separator") {
    if (args.size() < 2) {
      *config_.output_stream << "separator: \"" << config_.separator_col
                              << "\" \"" << config_.separator_row << "\""
                              << std::endl;
    } else {
      config_.separator_col = args[1];
      if (args.size() > 2) {
        config_.separator_row = args[2];
      }
    }
    return CommandResult::OK;
  }

  // .show
  if (cmd == ".show") {
    ShowSettings();
    return CommandResult::OK;
  }

  // .echo on|off
  if (cmd == ".echo") {
    if (args.size() < 2) {
      *config_.output_stream << "echo: " << (config_.echo ? "on" : "off")
                              << std::endl;
    } else {
      config_.echo = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .bail on|off
  if (cmd == ".bail") {
    if (args.size() < 2) {
      *config_.output_stream << "bail: "
                              << (config_.bail_on_error ? "on" : "off")
                              << std::endl;
    } else {
      config_.bail_on_error = (ToLower(args[1]) == "on");
    }
    return CommandResult::OK;
  }

  // .maxrows [N]
  if (cmd == ".maxrows") {
    if (args.size() < 2) {
      *config_.output_stream << "maxrows: " << config_.max_rows << std::endl;
    } else {
      try {
        config_.max_rows = std::stoi(args[1]);
      } catch (...) {
        std::cerr << "Error: invalid number '" << args[1] << "'" << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .maxwidth [N]
  if (cmd == ".maxwidth") {
    if (args.size() < 2) {
      *config_.output_stream << "maxwidth: " << config_.max_width << std::endl;
    } else {
      try {
        int val = std::stoi(args[1]);
        if (val == 0) {
          config_.max_width = GetTerminalWidth();
        } else {
          config_.max_width = val;
        }
      } catch (...) {
        std::cerr << "Error: invalid number '" << args[1] << "'" << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .read FILE
  if (cmd == ".read") {
    if (args.size() < 2) {
      std::cerr << "Usage: .read FILE" << std::endl;
      return CommandResult::OK;
    }
    // This is handled in the shell loop since it needs SQL processor context
    // For now, just print a message
    std::cerr << "Error: .read is handled by the shell loop" << std::endl;
    return CommandResult::OK;
  }

  // .shell CMD...
  if (cmd == ".shell") {
    if (args.size() < 2) {
      std::cerr << "Usage: .shell CMD..." << std::endl;
    } else {
      std::string shell_cmd;
      for (size_t i = 1; i < args.size(); ++i) {
        if (i > 1) shell_cmd += " ";
        shell_cmd += args[i];
      }
      std::system(shell_cmd.c_str());
    }
    return CommandResult::OK;
  }

  // .cd DIR
  if (cmd == ".cd") {
    if (args.size() < 2) {
      // cd to home
      const char* home = std::getenv("HOME");
      if (home) chdir(home);
    } else {
      if (chdir(args[1].c_str()) != 0) {
        std::cerr << "Error: cannot change to directory '" << args[1] << "'"
                  << std::endl;
      }
    }
    return CommandResult::OK;
  }

  // .prompt MAIN [CONT]
  if (cmd == ".prompt") {
    if (args.size() >= 2) {
      config_.main_prompt = args[1] + " ";
    }
    if (args.size() >= 3) {
      config_.continuation_prompt = args[2] + " ";
    }
    return CommandResult::OK;
  }

  // .refresh
  if (cmd == ".refresh") {
    if (refresh_callback_) refresh_callback_();
    *config_.output_stream << "Schema cache refreshed." << std::endl;
    return CommandResult::OK;
  }

  std::cerr << "Error: unknown command '" << cmd
            << "'. Type '.help' for available commands." << std::endl;
  return CommandResult::ERROR;
}

void CommandProcessor::SetRefreshCallback(std::function<void()> callback) {
  refresh_callback_ = std::move(callback);
}

void CommandProcessor::ShowHelp(const std::string& pattern) {
  struct HelpEntry {
    std::string command;
    std::string description;
  };

  std::vector<HelpEntry> entries = {
      {".bail on|off", "Stop on error. Default: off"},
      {".catalogs", "List all catalogs"},
      {".cd DIR", "Change working directory"},
      {".connect URI | HOST PORT USER",
       "Connect to a GizmoSQL server"},
      {".echo on|off", "Echo input commands. Default: off"},
      {".exit", "Exit this program (same as .quit)"},
      {".headers on|off", "Toggle column headers. Default: on"},
      {".help [PATTERN]", "Show this help or commands matching PATTERN"},
      {".maxrows [N]", "Show or set max rows displayed (0=unlimited)"},
      {".maxwidth [N]", "Show or set max width (0=auto from terminal)"},
      {".mode MODE", "Set output mode (box table csv json markdown ...)"},
      {".nullvalue STRING", "Set string for NULL values. Default: \"NULL\""},
      {".once FILE", "Redirect next query output to FILE"},
      {".output [FILE]", "Redirect output to FILE or stdout"},
      {".prompt MAIN [CONT]", "Set prompt strings"},
      {".quit", "Exit this program"},
      {".read FILE", "Execute SQL from FILE"},
      {".refresh", "Refresh tab-completion schema cache"},
      {".schema [PATTERN]", "Show database schemas"},
      {".separator COL [ROW]", "Set separators for list/csv mode"},
      {".shell CMD...", "Execute CMD in system shell"},
      {".show", "Show current settings"},
      {".tables [PATTERN]", "List tables"},
      {".timer on|off", "Show query execution time. Default: off"},
  };

  std::string filter = ToLower(pattern);
  auto& out = *config_.output_stream;

  for (const auto& entry : entries) {
    if (filter.empty() || entry.command.find(filter) != std::string::npos ||
        entry.description.find(filter) != std::string::npos) {
      out << std::left << std::setw(25) << entry.command << entry.description
          << "\n";
    }
  }
}

void CommandProcessor::ShowSettings() {
  auto& out = *config_.output_stream;

  // --- Server ---
  if (conn_.IsConnected()) {
    out << "--- Server ---\n";

    // Fetch engine and arrow version from FlightSQL SqlInfo metadata
    namespace sql = arrow::flight::sql;
    auto sql_info_result = conn_.GetSqlInfo(
        {sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION,
         sql::SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION});

    auto result = conn_.ExecuteQuery(
        "SELECT GIZMOSQL_VERSION(), GIZMOSQL_EDITION(), GIZMOSQL_CURRENT_INSTANCE()");
    if (result.ok()) {
      auto table = *result;
      if (table->num_rows() > 0) {
        out << "     version: " << GetCellValue(table->column(0), 0, "") << "\n";
        out << "     edition: " << GetCellValue(table->column(1), 0, "") << "\n";
        out << " instance_id: " << GetCellValue(table->column(2), 0, "") << "\n";
      }
    }

    if (sql_info_result.ok()) {
      auto info_table = *sql_info_result;
      auto engine = ExtractSqlInfoString(
          info_table, sql::SqlInfoOptions::FLIGHT_SQL_SERVER_VERSION);
      auto arrow_ver = ExtractSqlInfoString(
          info_table, sql::SqlInfoOptions::FLIGHT_SQL_SERVER_ARROW_VERSION);
      if (!engine.empty()) {
        out << "      engine: " << engine << "\n";
      }
      if (!arrow_ver.empty()) {
        out << "       arrow: " << arrow_ver << "\n";
      }
    }
  }

  // --- Session ---
  out << "--- Session ---\n";
  out << "   connected: " << (conn_.IsConnected() ? "yes" : "no") << "\n";
  out << "         uri: " << BuildConnectionURI(config_) << "\n";
  out << "        host: " << config_.host << "\n";
  out << "        port: " << config_.port << "\n";
  out << "         tls: " << (config_.use_tls ? "on" : "off") << "\n";

  if (conn_.IsConnected()) {
    auto session_result = conn_.ExecuteQuery(
        "SELECT GIZMOSQL_CURRENT_SESSION(), GIZMOSQL_ROLE(), "
        "CURRENT_CATALOG(), CURRENT_SCHEMA(), GIZMOSQL_USER()");
    if (session_result.ok()) {
      auto table = *session_result;
      if (table->num_rows() > 0) {
        out << "    username: " << GetCellValue(table->column(4), 0, "") << "\n";
        out << "  session_id: " << GetCellValue(table->column(0), 0, "") << "\n";
        out << "        role: " << GetCellValue(table->column(1), 0, "") << "\n";
        out << "     catalog: " << GetCellValue(table->column(2), 0, "") << "\n";
        out << "      schema: " << GetCellValue(table->column(3), 0, "") << "\n";
      }
    }
  } else {
    out << "    username: " << config_.username << "\n";
  }

  // --- Settings ---
  out << "--- Settings ---\n";
  out << "        mode: " << OutputModeToString(config_.output_mode) << "\n";
  out << "     headers: " << (config_.show_headers ? "on" : "off") << "\n";
  out << "   nullvalue: \"" << config_.null_value << "\"\n";
  out << "   separator: \"" << config_.separator_col << "\" \""
      << config_.separator_row << "\"\n";
  out << "       timer: " << (config_.show_timer ? "on" : "off") << "\n";
  out << "        echo: " << (config_.echo ? "on" : "off") << "\n";
  out << "        bail: " << (config_.bail_on_error ? "on" : "off") << "\n";
  out << "     maxrows: " << config_.max_rows << "\n";
  out << "    maxwidth: " << config_.max_width << "\n";
}

CommandResult CommandProcessor::HandleConnect(
    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr
        << "Usage: .connect gizmosql://HOST:PORT[?params...]\n"
        << "       .connect HOST PORT USERNAME [PASSWORD]\n"
        << "\n"
        << "URI parameters:\n"
        << "  username=USER              Username for authentication\n"
        << "  useEncryption=true|false   Enable TLS (default: false)\n"
        << "  disableCertificateVerification=true|false\n"
        << "                             Skip TLS cert verification\n"
        << "  tlsRoots=PATH              Path to CA certificate (PEM)\n"
        << "  authType=password|external Auth type (default: password)\n"
        << std::endl;
    return CommandResult::ERROR;
  }

  ClientConfig new_config = config_;

  if (args[1].find("://") != std::string::npos) {
    // URI format: gizmosql://HOST:PORT[?params...]
    if (!ParseConnectionURI(args[1], new_config)) {
      return CommandResult::ERROR;
    }

    // Prompt for password if needed (non-external auth)
    if (!new_config.auth_type_external && !new_config.username.empty()) {
      if (IsTerminal()) {
        new_config.password = PromptPassword();
        new_config.password_provided = true;
      }
    }
  } else {
    // Positional format: .connect HOST PORT USERNAME [PASSWORD]
    if (args.size() < 4) {
      std::cerr << "Usage: .connect HOST PORT USERNAME [PASSWORD]" << std::endl;
      return CommandResult::ERROR;
    }

    new_config.host = args[1];
    try {
      new_config.port = std::stoi(args[2]);
    } catch (...) {
      std::cerr << "Error: invalid port '" << args[2] << "'" << std::endl;
      return CommandResult::ERROR;
    }
    new_config.username = args[3];

    if (args.size() >= 5) {
      new_config.password = args[4];
      new_config.password_provided = true;
    } else if (IsTerminal()) {
      new_config.password = PromptPassword();
      new_config.password_provided = true;
    }
  }

  auto status = conn_.Connect(new_config);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    return CommandResult::ERROR;
  }

  // Update config with the new connection params
  config_.host = new_config.host;
  config_.port = new_config.port;
  config_.username = new_config.username;
  config_.password = new_config.password;
  config_.password_provided = new_config.password_provided;
  config_.use_tls = new_config.use_tls;
  config_.tls_skip_verify = new_config.tls_skip_verify;
  config_.tls_roots = new_config.tls_roots;
  config_.auth_type_external = new_config.auth_type_external;

  std::cout << "Connected to " << config_.host << ":" << config_.port
            << std::endl;

  if (refresh_callback_) refresh_callback_();

  return CommandResult::OK;
}

}  // namespace gizmosql::client
