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
#include <memory>
#include <regex>
#include <sstream>
#include <string>

#include <replxx.hxx>

#include "output_renderer.hpp"
#include "pager.hpp"
#include "shell_completer.hpp"
#include "syntax_highlighter.hpp"
#include "version.h"

namespace gizmosql::client {

namespace {

constexpr int kHistorySize = 1000;

// ANSI red for error messages (matching DuckDB's error styling)
constexpr const char* kRedBold = "\033[1;31m";
constexpr const char* kReset = "\033[0m";

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

// Detect if SQL references `_` as a table name (standalone, not part of an identifier).
// Matches `_` preceded by FROM/JOIN/, and not part of a longer identifier.
bool HasUnderscoreRef(const std::string& sql) {
  // Match `_` that is used as a table reference
  static const std::regex re(
      R"((?:FROM|JOIN|,)\s+_(?:\s|$|;|,|\)))",
      std::regex_constants::icase);
  // Also match bare `_` as the entire statement
  std::string trimmed = sql;
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.front())))
    trimmed.erase(trimmed.begin());
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.back())))
    trimmed.pop_back();
  if (trimmed == "_" || trimmed == "SELECT * FROM _") return true;
  return std::regex_search(sql, re);
}

// Replace standalone `_` table references with the temp table name
std::string RewriteUnderscore(const std::string& sql) {
  // Replace bare `_` statement
  std::string trimmed = sql;
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.front())))
    trimmed.erase(trimmed.begin());
  while (!trimmed.empty() && std::isspace(static_cast<unsigned char>(trimmed.back())))
    trimmed.pop_back();
  if (trimmed == "_") return "SELECT * FROM _last_result";

  // Replace `_` as a table name (word boundary check)
  std::string result;
  result.reserve(sql.size() + 32);
  for (size_t i = 0; i < sql.size(); ++i) {
    if (sql[i] == '_') {
      bool prev_is_ident = (i > 0 && (std::isalnum(static_cast<unsigned char>(sql[i - 1])) ||
                                        sql[i - 1] == '_'));
      bool next_is_ident = (i + 1 < sql.size() &&
                             (std::isalnum(static_cast<unsigned char>(sql[i + 1])) ||
                              sql[i + 1] == '_'));
      if (!prev_is_ident && !next_is_ident) {
        result += "_last_result";
        continue;
      }
    }
    result += sql[i];
  }
  return result;
}

// Render a query result to a string (for pager support)
std::string RenderToString(const QueryResult& query_result, ClientConfig& config,
                            int64_t row_limit) {
  std::ostringstream oss;
  auto renderer = CreateRenderer(config.output_mode, config);
  renderer->total_rows = query_result.total_rows;
  auto render_result = renderer->Render(*query_result.table, oss);

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
      oss << table_rows << "+ row" << (table_rows != 1 ? "s" : "");
    } else if (!rows_truncated && !cols_truncated) {
      oss << total_rows << " row" << (total_rows != 1 ? "s" : "") << " ("
          << total_cols << " column" << (total_cols != 1 ? "s" : "") << ")";
    } else {
      oss << total_rows << " row" << (total_rows != 1 ? "s" : "");
      if (rows_truncated) {
        oss << " (" << render_result.rows_rendered << " shown)";
      }
      oss << "  " << total_cols << " column"
          << (total_cols != 1 ? "s" : "");
      if (cols_truncated) {
        oss << " (" << render_result.columns_rendered << " shown)";
      }
    }
    oss << "\n";
  }
  return oss.str();
}

bool ExecuteStatement(FlightConnection& conn, ClientConfig& config,
                      const std::string& sql,
                      std::shared_ptr<arrow::Table>* last_result = nullptr) {
  if (!conn.IsConnected()) {
    std::cerr << kRedBold << "Error: not connected to a server. Use '.connect HOST PORT "
                 "USERNAME' to connect."
              << kReset << std::endl;
    return false;
  }

  // Handle `_` references — upload last result and rewrite SQL
  std::string effective_sql = sql;
  if (last_result && *last_result && HasUnderscoreRef(sql)) {
    auto status = conn.UploadLastResult(*last_result);
    if (!status.ok()) {
      std::cerr << kRedBold << "Error uploading last result: " << status.ToString() << kReset
                << std::endl;
      return false;
    }
    effective_sql = RewriteUnderscore(sql);
  }

  if (config.echo) {
    *config.output_stream << effective_sql << ";" << std::endl;
  }

  auto start = std::chrono::steady_clock::now();

  if (SqlProcessor::IsUpdateStatement(effective_sql)) {
    auto result = conn.ExecuteUpdate(effective_sql);
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
        std::cerr << kRedBold << "Query cancelled" << kReset << std::endl;
      } else {
        std::cerr << kRedBold << "Error: " << result.status().ToString() << kReset << std::endl;
      }
      return false;
    }
  } else {
    // Determine how many rows to fetch from the server.
    // When the pager is eligible, fetch a reasonable buffer (not unlimited)
    // so we don't download millions of rows for large tables.
    bool pager_eligible = config.is_interactive && config.pager_enabled &&
                          config.output_stream == &std::cout;
    int64_t row_limit = 0;
    if (pager_eligible) {
      // Fetch enough rows for comfortable browsing: threshold * 20 pages
      row_limit = static_cast<int64_t>(config.pager_threshold) * 20;
    } else if (config.max_rows > 0) {
      row_limit = config.max_rows;
    }
    auto result = conn.ExecuteQuery(effective_sql, row_limit);
    if (result.ok()) {
      auto elapsed = std::chrono::steady_clock::now() - start;
      auto ms =
          std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();

      auto& query_result = *result;

      // Cache last result
      if (last_result) {
        *last_result = query_result.table;
      }

      // Re-read terminal width if in auto-width mode (tracks window resizes)
      if (config.auto_width) {
        config.max_width = GetTerminalWidth();
      }

      // Use pager if result exceeds threshold
      bool use_pager = pager_eligible &&
                       query_result.table->num_rows() >= config.pager_threshold;

      if (use_pager) {
        std::string rendered = RenderToString(query_result, config, row_limit);
        Pager::Display(rendered);
      } else {
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
      }

      if (config.show_timer) {
        std::cerr << "Run Time: " << ms / 1000.0 << "s" << std::endl;
      }
      return true;
    } else {
      if (result.status().IsCancelled()) {
        std::cerr << kRedBold << "Query cancelled" << kReset << std::endl;
      } else {
        std::cerr << kRedBold << "Error: " << result.status().ToString() << kReset << std::endl;
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

// Refresh the dynamic prompt by querying the server for current catalog and schema
void RefreshDynamicPrompt(FlightConnection& conn, ClientConfig& config) {
  if (!config.dynamic_prompt || !conn.IsConnected()) return;
  auto result = conn.ExecuteQuery("SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()");
  if (result.ok() && result->table->num_rows() > 0) {
    std::string catalog = GetCellValue(result->table->column(0), 0, "");
    std::string schema = GetCellValue(result->table->column(1), 0, "");
    std::string base = catalog + "." + schema;

    // Colored prompt: bold dark orange catalog.schema (matching DuckDB's PROMPT color)
    // Replxx natively handles ANSI escape sequences in prompts — no markers needed
    config.main_prompt = std::string("\033[1m\033[38;5;208m") + base +
                         std::string("\033[0m") + "> ";

    // Continuation prompt: spaces matching visible width + "-> "
    int visible_len = static_cast<int>(base.size()) + 2;  // "base> "
    if (visible_len > 3) {
      config.continuation_prompt =
          std::string(visible_len - 3, ' ') + "-> ";
    } else {
      config.continuation_prompt = "-> ";
    }
  }
}

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

  // Syntax highlighting
  SyntaxHighlighter highlighter;
  rx.set_highlighter_callback(
      [&highlighter, &config](const std::string& input,
                               replxx::Replxx::colors_t& colors) {
        if (config.syntax_highlighting) {
          highlighter.Highlight(input, colors);
        }
      });

  // Last result cache
  std::shared_ptr<arrow::Table> last_result;
  cmd_proc.SetLastResult(&last_result);

  cmd_proc.SetRefreshCallback([&completer]() { completer.InvalidateCache(); });

  // Prompt refresh callback (called after .connect)
  cmd_proc.SetPromptRefreshCallback(
      [&conn, &config]() { RefreshDynamicPrompt(conn, config); });

  // Initial dynamic prompt
  RefreshDynamicPrompt(conn, config);

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
        ExecuteStatement(conn, config, stmt, &last_result);
        if (SqlProcessor::IsDdlStatement(stmt)) {
          completer.InvalidateCache();
          RefreshDynamicPrompt(conn, config);
        }
      }

      // Process any remaining text after the semicolon
      while (!remainder.empty()) {
        if (sql_proc.AccumulateLine(remainder)) {
          stmt = sql_proc.GetStatement();
          remainder = sql_proc.GetRemainder();
          sql_proc.Reset();
          if (!stmt.empty()) {
            ExecuteStatement(conn, config, stmt, &last_result);
            if (SqlProcessor::IsDdlStatement(stmt)) {
              completer.InvalidateCache();
              RefreshDynamicPrompt(conn, config);
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
      std::cerr << kRedBold << "Error: cannot open file '" << *config.input_file << "'" << kReset
                << std::endl;
      return 1;
    }
    return ProcessLines(conn, cmd_proc, sql_proc, config, input);
  }

  // Stdin pipe/heredoc
  return ProcessLines(conn, cmd_proc, sql_proc, config, std::cin);
}

}  // namespace gizmosql::client
