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

#include "shell_completer.hpp"

#include <algorithm>
#include <cctype>

#include "output_renderer.hpp"

namespace gizmosql::client {

namespace {

bool IsIdentChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.';
}

std::string ToUpper(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), ::toupper);
  return result;
}

std::string ToLower(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

bool StartsWith(const std::string& haystack, const std::string& needle) {
  if (needle.size() > haystack.size()) return false;
  for (size_t i = 0; i < needle.size(); ++i) {
    if (std::toupper(static_cast<unsigned char>(haystack[i])) !=
        std::toupper(static_cast<unsigned char>(needle[i]))) {
      return false;
    }
  }
  return true;
}

// Find the last SQL keyword before the prefix in the input
std::string FindPrecedingKeyword(const std::string& input,
                                  size_t prefix_start) {
  // Walk backward from prefix_start, skipping whitespace
  size_t pos = prefix_start;
  while (pos > 0 && std::isspace(static_cast<unsigned char>(input[pos - 1]))) {
    --pos;
  }
  // Now collect the word before that
  size_t word_end = pos;
  while (pos > 0 &&
         std::isalpha(static_cast<unsigned char>(input[pos - 1]))) {
    --pos;
  }
  if (pos == word_end) return "";
  return ToUpper(input.substr(pos, word_end - pos));
}

}  // namespace

// Static SQL keywords list
const std::vector<std::string>& ShellCompleter::GetSqlKeywords() {
  static const std::vector<std::string> keywords = {
      "SELECT",    "FROM",       "WHERE",      "INSERT",     "INTO",
      "UPDATE",    "DELETE",     "CREATE",     "DROP",       "ALTER",
      "TABLE",     "VIEW",       "INDEX",      "JOIN",       "LEFT",
      "RIGHT",     "INNER",      "OUTER",      "FULL",       "CROSS",
      "ON",        "USING",      "GROUP",      "BY",         "ORDER",
      "ASC",       "DESC",       "LIMIT",      "OFFSET",     "HAVING",
      "UNION",     "ALL",        "DISTINCT",   "AS",         "AND",
      "OR",        "NOT",        "IN",         "EXISTS",     "BETWEEN",
      "LIKE",      "IS",         "NULL",       "TRUE",       "FALSE",
      "CASE",      "WHEN",       "THEN",       "ELSE",       "END",
      "SET",       "VALUES",     "WITH",       "EXCEPT",     "INTERSECT",
      "COPY",      "LOAD",       "INSTALL",    "ATTACH",     "DETACH",
      "EXPLAIN",   "ANALYZE",    "DESCRIBE",   "SHOW",       "PRAGMA",
      "PIVOT",     "UNPIVOT",    "WINDOW",     "OVER",       "PARTITION",
      "ROWS",      "RANGE",      "FILTER",     "REPLACE",    "RETURNING",
      "PRIMARY",   "KEY",        "FOREIGN",    "REFERENCES", "UNIQUE",
      "CHECK",     "CONSTRAINT", "DEFAULT",    "TEMPORARY",  "TEMP",
      "IF",        "CASCADE",    "RESTRICT",   "BEGIN",      "COMMIT",
      "ROLLBACK",  "RECURSIVE",  "LATERAL",    "NATURAL",    "QUALIFY",
      "EXCLUDE",   "RENAME",     "COLUMN",     "DATABASE",   "SCHEMA",
      "GRANT",     "REVOKE",     "TRIGGER",    "SEQUENCE",   "TYPE",
      "ENUM",      "ILIKE",      "GLOB",       "SIMILAR",    "TABLESAMPLE",
      "POSITIONAL","STRUCT",     "MAP",        "LIST",       "ARRAY",
      "GENERATE_SERIES", "UNNEST", "SUMMARIZE", "CALL",      "EXPORT",
      "IMPORT",    "USE",
  };
  return keywords;
}

// Static dot commands list
const std::vector<std::string>& ShellCompleter::GetDotCommands() {
  static const std::vector<std::string> commands = {
      ".about",      ".bail",       ".catalogs",   ".cd",         ".connect",    ".describe",
      ".echo",       ".exit",       ".export_last",".headers",    ".help",
      ".highlight",  ".last",       ".maxrows",    ".maxwidth",   ".mode",
      ".nullvalue",  ".once",       ".output",     ".pager",      ".prompt",
      ".quit",       ".read",       ".refresh",    ".schema",     ".separator",
      ".shell",      ".show",       ".tables",     ".timer",
  };
  return commands;
}

ShellCompleter::ShellCompleter(FlightConnection& conn) : conn_(conn) {}

void ShellCompleter::InvalidateCache() {
  cache_populated_ = false;
  tables_.clear();
  schemas_.clear();
}

void ShellCompleter::EnsureCachePopulated() {
  if (cache_populated_) return;
  if (!conn_.IsConnected()) return;

  // Fetch tables
  auto tables_result = conn_.GetTables();
  if (tables_result.ok()) {
    auto table = *tables_result;
    for (int64_t r = 0; r < table->num_rows(); ++r) {
      TableEntry entry;
      if (table->num_columns() > 0) {
        entry.catalog = GetCellValue(table->column(0), r, "");
      }
      if (table->num_columns() > 1) {
        entry.schema = GetCellValue(table->column(1), r, "");
      }
      if (table->num_columns() > 2) {
        entry.table_name = GetCellValue(table->column(2), r, "");
      }
      if (table->num_columns() > 3) {
        entry.table_type = GetCellValue(table->column(3), r, "");
      }
      tables_.push_back(std::move(entry));
    }
  }

  // Fetch schemas
  auto schemas_result = conn_.GetDbSchemas();
  if (schemas_result.ok()) {
    auto table = *schemas_result;
    for (int64_t r = 0; r < table->num_rows(); ++r) {
      SchemaEntry entry;
      if (table->num_columns() > 0) {
        entry.catalog = GetCellValue(table->column(0), r, "");
      }
      if (table->num_columns() > 1) {
        entry.schema_name = GetCellValue(table->column(1), r, "");
      }
      schemas_.push_back(std::move(entry));
    }
  }

  cache_populated_ = true;
}

std::string ShellCompleter::ExtractPrefix(const std::string& input,
                                           int& context_len) const {
  int pos = static_cast<int>(input.size()) - 1;
  while (pos >= 0 && IsIdentChar(input[pos])) {
    --pos;
  }
  context_len = static_cast<int>(input.size()) - pos - 1;
  if (context_len > 0) {
    return input.substr(pos + 1);
  }

  // Check for dot command prefix — require at least one char after the dot
  // to avoid triggering completion on bare "."
  if (pos >= 0 && input[pos] == '.' &&
      pos + 1 < static_cast<int>(input.size())) {
    context_len = static_cast<int>(input.size()) - pos;
    return input.substr(pos);
  }

  return "";
}

ShellCompleter::CompletionContext ShellCompleter::DetermineContext(
    const std::string& input, const std::string& prefix) const {
  // Dot command context: line starts with '.'
  std::string trimmed = input;
  size_t first_non_ws = trimmed.find_first_not_of(" \t");
  if (first_non_ws != std::string::npos && trimmed[first_non_ws] == '.') {
    return CompletionContext::DOT_COMMAND;
  }

  // Schema-qualified: prefix contains '.'
  if (prefix.find('.') != std::string::npos) {
    return CompletionContext::SCHEMA_QUALIFIED;
  }

  // Check preceding keyword for table name context
  size_t prefix_start = input.size() - prefix.size();
  std::string prev_kw = FindPrecedingKeyword(input, prefix_start);
  if (prev_kw == "FROM" || prev_kw == "JOIN" || prev_kw == "INTO" ||
      prev_kw == "UPDATE" || prev_kw == "TABLE" || prev_kw == "DESCRIBE") {
    return CompletionContext::TABLE_NAME;
  }

  return CompletionContext::SQL_GENERAL;
}

std::string ShellCompleter::MatchCase(const std::string& candidate,
                                       const std::string& prefix) {
  if (prefix.empty()) return candidate;

  // Check if prefix is all lowercase
  bool all_lower = true;
  bool all_upper = true;
  for (char c : prefix) {
    if (std::isalpha(static_cast<unsigned char>(c))) {
      if (std::isupper(static_cast<unsigned char>(c))) all_lower = false;
      if (std::islower(static_cast<unsigned char>(c))) all_upper = false;
    }
  }

  if (all_lower) return ToLower(candidate);
  if (all_upper) return ToUpper(candidate);
  return candidate;  // mixed case: return original
}

replxx::Replxx::completions_t ShellCompleter::Complete(
    const std::string& input, int& context_len) {
  replxx::Replxx::completions_t completions;

  std::string prefix = ExtractPrefix(input, context_len);
  if (prefix.empty() && context_len == 0) return completions;

  auto ctx = DetermineContext(input, prefix);

  switch (ctx) {
    case CompletionContext::DOT_COMMAND: {
      // Don't complete on bare "." — wait for at least one more character
      if (prefix == ".") break;
      for (const auto& cmd : GetDotCommands()) {
        if (StartsWith(cmd, prefix)) {
          completions.emplace_back(cmd,
                                   replxx::Replxx::Color::GREEN);
        }
      }
      break;
    }

    case CompletionContext::SCHEMA_QUALIFIED: {
      EnsureCachePopulated();
      // Split prefix into schema_part.table_part
      auto dot_pos = prefix.rfind('.');
      std::string schema_part = prefix.substr(0, dot_pos);
      std::string table_part = prefix.substr(dot_pos + 1);

      for (const auto& entry : tables_) {
        if (StartsWith(entry.schema, schema_part) &&
            StartsWith(entry.table_name, table_part)) {
          std::string full = entry.schema + "." + entry.table_name;
          completions.emplace_back(full);
        }
      }
      break;
    }

    case CompletionContext::TABLE_NAME: {
      EnsureCachePopulated();
      for (const auto& entry : tables_) {
        if (StartsWith(entry.table_name, prefix)) {
          completions.emplace_back(entry.table_name);
        }
      }
      break;
    }

    case CompletionContext::SQL_GENERAL: {
      // SQL keywords
      for (const auto& kw : GetSqlKeywords()) {
        if (StartsWith(kw, prefix)) {
          completions.emplace_back(MatchCase(kw, prefix),
                                   replxx::Replxx::Color::CYAN);
        }
      }

      // Table names
      EnsureCachePopulated();
      for (const auto& entry : tables_) {
        if (StartsWith(entry.table_name, prefix)) {
          completions.emplace_back(entry.table_name);
        }
      }
      break;
    }
  }

  return completions;
}

replxx::Replxx::hints_t ShellCompleter::Hint(const std::string& input,
                                              int& context_len,
                                              replxx::Replxx::Color& color) {
  replxx::Replxx::hints_t hints;

  auto completions = Complete(input, context_len);
  if (completions.size() == 1) {
    color = replxx::Replxx::Color::GRAY;
    hints.emplace_back(completions[0].text());
  }

  return hints;
}

}  // namespace gizmosql::client
