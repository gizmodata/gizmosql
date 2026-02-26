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

#include "shell_loop.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include <replxx.hxx>

#include "output_renderer.hpp"
#include "shell_completer.hpp"
#include "version.h"

namespace gizmosql::client {

namespace {

constexpr int kHistorySize = 1000;

std::string GetHistoryPath() {
#ifdef _WIN32
  const char* home = std::getenv("USERPROFILE");
#else
  const char* home = std::getenv("HOME");
#endif
  if (home) {
    return std::string(home) + "/.gizmosql_history";
  }
  return ".gizmosql_history";
}

bool ExecuteStatement(FlightConnection& conn, ClientConfig& config,
                      const std::string& sql) {
  if (!conn.IsConnected()) {
    std::cerr << "Error: not connected to a server. Use '.connect HOST PORT "
                 "USERNAME' to connect."
              << std::endl;
    return false;
  }

  if (config.echo) {
    *config.output_stream << sql << ";" << std::endl;
  }

  auto start = std::chrono::steady_clock::now();

  if (SqlProcessor::IsUpdateStatement(sql)) {
    auto result = conn.ExecuteUpdate(sql);
    if (result.ok()) {
      auto elapsed = std::chrono::steady_clock::now() - start;
      auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
      *config.output_stream << "OK";
      if (*result > 0) {
        *config.output_stream << ", " << *result << " rows affected";
      }
      *config.output_stream << std::endl;
      if (config.show_timer) {
        std::cerr << "Run Time: " << ms / 1000.0 << "s" << std::endl;
      }
      return true;
    } else {
      if (result.status().IsCancelled()) {
        std::cerr << "Query cancelled" << std::endl;
      } else {
        std::cerr << "Error: " << result.status().ToString() << std::endl;
      }
      return false;
    }
  } else {
    // Pass max_rows as row_limit so we only fetch what we need
    int64_t row_limit = config.max_rows > 0 ? config.max_rows : 0;
    auto result = conn.ExecuteQuery(sql, row_limit);
    if (result.ok()) {
      auto elapsed = std::chrono::steady_clock::now() - start;
      auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

      auto& query_result = *result;

      // Re-read terminal width if in auto-width mode (tracks window resizes)
      if (config.auto_width) {
        config.max_width = GetTerminalWidth();
      }

      auto renderer = CreateRenderer(config.output_mode, config);
      renderer->total_rows = query_result.total_rows;
      auto render_result =
          renderer->Render(*query_result.table, *config.output_stream);

      // Box/table renderers include an in-table footer; skip external footer
      if (!render_result.footer_rendered) {
        int64_t total_rows = query_result.total_rows >= 0
                                 ? query_result.total_rows
                                 : query_result.table->num_rows();
        int total_cols = query_result.table->num_columns();
        int64_t table_rows = query_result.table->num_rows();
        bool rows_truncated =
            render_result.rows_rendered < total_rows ||
            (query_result.total_rows == -1 &&
             render_result.rows_rendered < table_rows);
        bool cols_truncated = render_result.columns_rendered < total_cols;

        if (query_result.total_rows == -1 &&
            table_rows == row_limit && row_limit > 0) {
          // Unknown total — show "N+ rows"
          *config.output_stream << table_rows << "+ row"
                                << (table_rows != 1 ? "s" : "");
        } else if (!rows_truncated && !cols_truncated) {
          *config.output_stream << total_rows << " row"
                                << (total_rows != 1 ? "s" : "") << " ("
                                << total_cols << " column"
                                << (total_cols != 1 ? "s" : "") << ")";
        } else {
          *config.output_stream << total_rows << " row"
                                << (total_rows != 1 ? "s" : "");
          if (rows_truncated) {
            *config.output_stream << " (" << render_result.rows_rendered
                                  << " shown)";
          }
          *config.output_stream << "  " << total_cols << " column"
                                << (total_cols != 1 ? "s" : "");
          if (cols_truncated) {
            *config.output_stream << " (" << render_result.columns_rendered
                                  << " shown)";
          }
        }
        *config.output_stream << std::endl;
      }

      if (config.show_timer) {
        std::cerr << "Run Time: " << ms / 1000.0 << "s" << std::endl;
      }
      return true;
    } else {
      if (result.status().IsCancelled()) {
        std::cerr << "Query cancelled" << std::endl;
      } else {
        std::cerr << "Error: " << result.status().ToString() << std::endl;
      }
      return false;
    }
  }
}

int ProcessLines(FlightConnection& conn, CommandProcessor& cmd_proc,
                 SqlProcessor& sql_proc, ClientConfig& config,
                 std::istream& input) {
  std::string line;
  int exit_code = 0;

  while (std::getline(input, line)) {
    // Skip empty lines if not accumulating
    if (line.empty() && !sql_proc.IsAccumulating()) continue;

    // Check for dot commands only when not accumulating SQL
    if (!sql_proc.IsAccumulating() && CommandProcessor::IsDotCommand(line)) {
      auto result = cmd_proc.Process(line);
      if (result == CommandResult::EXIT) return 0;
      if (result == CommandResult::ERROR && config.bail_on_error) return 1;
      continue;
    }

    if (sql_proc.AccumulateLine(line)) {
      std::string stmt = sql_proc.GetStatement();
      std::string remainder = sql_proc.GetRemainder();
      sql_proc.Reset();

      if (!stmt.empty()) {
        if (!ExecuteStatement(conn, config, stmt)) {
          exit_code = 1;
          if (config.bail_on_error) return exit_code;
        }
      }

      // Process any remaining text after the semicolon on the same line
      while (!remainder.empty()) {
        if (CommandProcessor::IsDotCommand(remainder)) {
          auto result = cmd_proc.Process(remainder);
          if (result == CommandResult::EXIT) return 0;
          break;
        }
        if (sql_proc.AccumulateLine(remainder)) {
          stmt = sql_proc.GetStatement();
          remainder = sql_proc.GetRemainder();
          sql_proc.Reset();
          if (!stmt.empty()) {
            if (!ExecuteStatement(conn, config, stmt)) {
              exit_code = 1;
              if (config.bail_on_error) return exit_code;
            }
          }
        } else {
          break;  // Incomplete statement, will continue with next line
        }
      }
    }
  }

  // Handle remaining accumulated SQL (without semicolon at EOF)
  if (sql_proc.IsAccumulating()) {
    std::string stmt = sql_proc.GetStatement();
    sql_proc.Reset();
    if (!stmt.empty()) {
      if (!ExecuteStatement(conn, config, stmt)) {
        exit_code = 1;
      }
    }
  }

  return exit_code;
}

}  // namespace

int RunShellLoop(FlightConnection& conn, CommandProcessor& cmd_proc,
                 SqlProcessor& sql_proc, ClientConfig& config) {
  replxx::Replxx rx;
  rx.set_max_history_size(kHistorySize);

  std::string history_path = GetHistoryPath();
  rx.history_load(history_path);

  // Tab completion and hints
  ShellCompleter completer(conn);

  rx.set_word_break_characters(" \t\n\r\"'`@$><=;|&{(,+-*/%~!^)}");
  rx.set_completion_count_cutoff(100);
  rx.set_double_tab_completion(false);
  rx.set_complete_on_empty(false);
  rx.set_max_hint_rows(4);

  rx.set_completion_callback(
      [&completer](const std::string& input, int& ctx) {
        return completer.Complete(input, ctx);
      });
  rx.set_hint_callback(
      [&completer](const std::string& input, int& ctx,
                    replxx::Replxx::Color& color) {
        return completer.Hint(input, ctx, color);
      });

  cmd_proc.SetRefreshCallback([&completer]() { completer.InvalidateCache(); });

  if (!config.quiet) {
    std::cout << "GizmoSQL Client " << PROJECT_VERSION << std::endl;
    if (conn.IsConnected()) {
      std::cout << "Connected to " << config.host << ":" << config.port
                << std::endl;
    } else {
      std::cout << "Not connected. Use '.connect HOST PORT USERNAME' to "
                   "connect."
                << std::endl;
    }
    std::cout << "Type '.help' for help, '.quit' to exit." << std::endl;
    std::cout << std::endl;
  }

  // Process init file
  if (!config.no_init && !config.init_file.has_value()) {
    std::string default_init = GetHistoryPath();
    // Replace history filename with init filename
    auto pos = default_init.rfind('/');
    if (pos != std::string::npos) {
      default_init = default_init.substr(0, pos) + "/.gizmosqlrc";
    } else {
      default_init = ".gizmosqlrc";
    }
    std::ifstream init(default_init);
    if (init.is_open()) {
      ProcessLines(conn, cmd_proc, sql_proc, config, init);
    }
  } else if (config.init_file.has_value()) {
    std::ifstream init(*config.init_file);
    if (init.is_open()) {
      ProcessLines(conn, cmd_proc, sql_proc, config, init);
    } else {
      std::cerr << "Warning: could not open init file '"
                << *config.init_file << "'" << std::endl;
    }
  }

  while (true) {
    const char* prompt = sql_proc.IsAccumulating()
                             ? config.continuation_prompt.c_str()
                             : config.main_prompt.c_str();

    const char* input = rx.input(prompt);
    if (input == nullptr) {
      // EOF (Ctrl+D)
      if (!config.quiet) {
        std::cout << std::endl;
      }
      break;
    }

    std::string line(input);

    // Skip empty lines when not accumulating
    if (line.empty() && !sql_proc.IsAccumulating()) continue;

    // Check for dot commands only when not accumulating SQL
    if (!sql_proc.IsAccumulating() && CommandProcessor::IsDotCommand(line)) {
      rx.history_add(line);
      auto result = cmd_proc.Process(line);
      if (result == CommandResult::EXIT) break;
      continue;
    }

    if (sql_proc.AccumulateLine(line)) {
      std::string stmt = sql_proc.GetStatement();
      std::string remainder = sql_proc.GetRemainder();

      // Add the full statement to history
      std::string full_line = stmt + ";";
      rx.history_add(full_line);

      sql_proc.Reset();

      if (!stmt.empty()) {
        ExecuteStatement(conn, config, stmt);
        if (SqlProcessor::IsDdlStatement(stmt)) {
          completer.InvalidateCache();
        }
      }

      // Process any remaining text after the semicolon
      while (!remainder.empty()) {
        if (sql_proc.AccumulateLine(remainder)) {
          stmt = sql_proc.GetStatement();
          remainder = sql_proc.GetRemainder();
          sql_proc.Reset();
          if (!stmt.empty()) {
            ExecuteStatement(conn, config, stmt);
            if (SqlProcessor::IsDdlStatement(stmt)) {
              completer.InvalidateCache();
            }
          }
        } else {
          break;
        }
      }
    }
  }

  rx.history_save(history_path);
  return 0;
}

int RunNonInteractive(FlightConnection& conn, CommandProcessor& cmd_proc,
                      SqlProcessor& sql_proc, ClientConfig& config) {
  // -c "SQL" mode
  if (config.command.has_value()) {
    std::istringstream input(*config.command);
    return ProcessLines(conn, cmd_proc, sql_proc, config, input);
  }

  // -f FILE mode
  if (config.input_file.has_value()) {
    std::ifstream input(*config.input_file);
    if (!input.is_open()) {
      std::cerr << "Error: cannot open file '" << *config.input_file << "'"
                << std::endl;
      return 1;
    }
    return ProcessLines(conn, cmd_proc, sql_proc, config, input);
  }

  // Stdin pipe/heredoc
  return ProcessLines(conn, cmd_proc, sql_proc, config, std::cin);
}

}  // namespace gizmosql::client
