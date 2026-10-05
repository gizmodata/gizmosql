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

#include <cctype>
#include <string>
#include <vector>

namespace gizmosql {

/// Splits a SQL script into its statements at the `;`s that end them,
/// following DuckDB's (PostgreSQL's) lexical rules, so a `;` inside any of
/// these does not split a statement:
///   - 'single-quoted strings' ('' escapes a quote; E'...' also allows
///     backslash escapes)
///   - "double-quoted identifiers" ("" escapes a quote)
///   - -- line comments and /* block comments */ (which may nest)
///   - $$dollar-quoted strings$$ and $tag$...$tag$ (a `$` that does not open
///     one — `$1`, `$name` parameters — is ordinary text)
/// Each statement is returned without its terminating `;` and with surrounding
/// whitespace trimmed; comments inside it are kept. Fragments holding nothing
/// but whitespace and comments (e.g. a trailing `-- done`) are dropped. An
/// unterminated quote or comment runs to the end of the script, so DuckDB
/// reports the real error for that statement.
inline std::vector<std::string> SplitSqlStatements(const std::string& sql) {
  const size_t n = sql.size();
  auto is_ident_char = [](char c) {
    return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
  };

  std::vector<std::string> statements;
  size_t start = 0;       // start of the current statement
  bool has_code = false;  // current statement has more than whitespace/comments

  auto finish = [&](size_t end) {
    if (has_code) {
      const auto first = sql.find_first_not_of(" \t\r\n\f\v", start);
      const auto last = sql.find_last_not_of(" \t\r\n\f\v", end - 1);
      statements.push_back(sql.substr(first, last - first + 1));
    }
    start = end + 1;
    has_code = false;
  };

  size_t i = 0;
  while (i < n) {
    const char c = sql[i];
    const char next = i + 1 < n ? sql[i + 1] : '\0';

    if (c == '-' && next == '-') {  // line comment
      const auto eol = sql.find('\n', i);
      i = eol == std::string::npos ? n : eol + 1;
      continue;
    }
    if (c == '/' && next == '*') {  // block comment, possibly nested
      int depth = 1;
      i += 2;
      while (i < n && depth > 0) {
        if (sql[i] == '/' && i + 1 < n && sql[i + 1] == '*') {
          ++depth;
          i += 2;
        } else if (sql[i] == '*' && i + 1 < n && sql[i + 1] == '/') {
          --depth;
          i += 2;
        } else {
          ++i;
        }
      }
      continue;
    }

    if (c == ';') {
      finish(i);
      ++i;
      continue;
    }

    has_code = has_code || !std::isspace(static_cast<unsigned char>(c));

    if (c == '\'') {  // string literal
      const bool backslash_escapes = i > 0 && (sql[i - 1] == 'E' || sql[i - 1] == 'e') &&
                                     (i < 2 || !is_ident_char(sql[i - 2]));
      ++i;
      while (i < n) {
        if (backslash_escapes && sql[i] == '\\') {
          i += 2;
        } else if (sql[i] == '\'') {
          if (i + 1 < n && sql[i + 1] == '\'') {
            i += 2;
          } else {
            ++i;
            break;
          }
        } else {
          ++i;
        }
      }
      continue;
    }
    if (c == '"') {  // quoted identifier
      ++i;
      while (i < n) {
        if (sql[i] == '"') {
          if (i + 1 < n && sql[i + 1] == '"') {
            i += 2;
          } else {
            ++i;
            break;
          }
        } else {
          ++i;
        }
      }
      continue;
    }
    if (c == '$' && (i == 0 || !is_ident_char(sql[i - 1]))) {  // $tag$ ... $tag$
      size_t j = i + 1;
      if (j < n && (std::isalpha(static_cast<unsigned char>(sql[j])) || sql[j] == '_')) {
        while (j < n && is_ident_char(sql[j])) ++j;
      }
      if (j < n && sql[j] == '$') {
        const std::string tag = sql.substr(i, j - i + 1);
        const auto close = sql.find(tag, j + 1);
        i = close == std::string::npos ? n : close + tag.size();
        continue;
      }
    }
    ++i;
  }
  finish(n);
  return statements;
}

}  // namespace gizmosql
