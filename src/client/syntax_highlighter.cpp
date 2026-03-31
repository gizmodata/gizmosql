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

#include "syntax_highlighter.hpp"

#include <algorithm>
#include <cctype>

#include "shell_completer.hpp"

namespace gizmosql::client {

using Color = replxx::Replxx::Color;

// Common SQL functions for highlighting
static const char* kBuiltinFunctions[] = {
    "ABS",         "AVG",         "COALESCE",    "CONCAT",
    "COUNT",       "CURRENT_DATE","CURRENT_TIME","CURRENT_TIMESTAMP",
    "DATE_PART",   "DATE_TRUNC",  "EXTRACT",     "FIRST",
    "GREATEST",    "IFNULL",      "LAST",        "LEAST",
    "LENGTH",      "LIST_AGG",    "LOWER",       "MAX",
    "MIN",         "NOW",         "NULLIF",      "PRINTF",
    "REPLACE",     "ROUND",       "ROW_NUMBER",  "STRING_AGG",
    "SUBSTR",      "SUBSTRING",   "SUM",         "TRIM",
    "TYPEOF",      "UPPER",       "CAST",        "TRY_CAST",
    "RANK",        "DENSE_RANK",  "NTILE",       "LAG",
    "LEAD",        "FIRST_VALUE", "LAST_VALUE",  "NTH_VALUE",
    "ARRAY_AGG",   "BIT_AND",     "BIT_OR",      "BIT_XOR",
    "BOOL_AND",    "BOOL_OR",     "LIST",        "MAP",
    "STRUCT_PACK", "UNNEST",      "GENERATE_SERIES",
    "REGEXP_MATCHES", "REGEXP_REPLACE", "REGEXP_EXTRACT",
    "STRFTIME",    "STRPTIME",    "DATE_DIFF",   "DATE_ADD",
    "DATE_SUB",    "EPOCH",       "EPOCH_MS",
};

SyntaxHighlighter::SyntaxHighlighter() {
  // Build keyword set from the completer's keyword list
  for (const auto& kw : ShellCompleter::GetSqlKeywords()) {
    keywords_.insert(kw);
  }
  // Build function set
  for (const auto& fn : kBuiltinFunctions) {
    functions_.insert(fn);
  }
}

namespace {

inline bool IsIdentChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

std::string ToUpper(const std::string& s) {
  std::string result = s;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

// Count the number of Unicode code points in a UTF-8 string up to byte_pos
size_t ByteToCodepoint(const std::string& s, size_t byte_pos) {
  size_t cp = 0;
  for (size_t i = 0; i < byte_pos && i < s.size();) {
    unsigned char c = s[i];
    if (c < 0x80) {
      i += 1;
    } else if ((c & 0xE0) == 0xC0) {
      i += 2;
    } else if ((c & 0xF0) == 0xE0) {
      i += 3;
    } else {
      i += 4;
    }
    ++cp;
  }
  return cp;
}

// Get byte position of the nth codepoint
size_t CodepointToByte(const std::string& s, size_t cp_pos) {
  size_t byte = 0;
  for (size_t cp = 0; cp < cp_pos && byte < s.size(); ++cp) {
    unsigned char c = s[byte];
    if (c < 0x80) {
      byte += 1;
    } else if ((c & 0xE0) == 0xC0) {
      byte += 2;
    } else if ((c & 0xF0) == 0xE0) {
      byte += 3;
    } else {
      byte += 4;
    }
  }
  return byte;
}

}  // namespace

void SyntaxHighlighter::Highlight(const std::string& input,
                                   replxx::Replxx::colors_t& colors) {
  if (!enabled_) return;

  // Colors vector is sized by replxx to match the number of Unicode code points.
  // We work in byte positions and convert to codepoint positions when assigning.
  size_t len = input.size();
  size_t num_codepoints = colors.size();

  // Helper to set color for a byte range
  auto set_color = [&](size_t byte_start, size_t byte_end, Color color) {
    size_t cp_start = ByteToCodepoint(input, byte_start);
    size_t cp_end = ByteToCodepoint(input, byte_end);
    for (size_t i = cp_start; i < cp_end && i < num_codepoints; ++i) {
      colors[i] = color;
    }
  };

  // Track bracket depth for error detection
  int paren_depth = 0;
  bool in_single_quote = false;
  bool in_double_quote = false;
  bool in_block_comment = false;
  bool in_line_comment = false;

  size_t i = 0;
  while (i < len) {
    char c = input[i];

    // Line comment
    if (!in_single_quote && !in_double_quote && !in_block_comment &&
        c == '-' && i + 1 < len && input[i + 1] == '-') {
      set_color(i, len, Color::GRAY);
      in_line_comment = true;
      break;  // Rest of line is comment
    }

    // Block comment start
    if (!in_single_quote && !in_double_quote && !in_block_comment &&
        c == '/' && i + 1 < len && input[i + 1] == '*') {
      in_block_comment = true;
      set_color(i, i + 2, Color::GRAY);
      i += 2;
      continue;
    }

    // Block comment end
    if (in_block_comment) {
      if (c == '*' && i + 1 < len && input[i + 1] == '/') {
        set_color(i, i + 2, Color::GRAY);
        i += 2;
        in_block_comment = false;
      } else {
        set_color(i, i + 1, Color::GRAY);
        ++i;
      }
      continue;
    }

    // Single-quoted string
    if (c == '\'' && !in_double_quote) {
      if (in_single_quote) {
        // Check for escaped quote ('')
        if (i + 1 < len && input[i + 1] == '\'') {
          set_color(i, i + 2, Color::YELLOW);
          i += 2;
          continue;
        }
        set_color(i, i + 1, Color::YELLOW);
        in_single_quote = false;
        ++i;
        continue;
      } else {
        in_single_quote = true;
        set_color(i, i + 1, Color::YELLOW);
        ++i;
        continue;
      }
    }

    if (in_single_quote) {
      set_color(i, i + 1, Color::YELLOW);
      ++i;
      continue;
    }

    // Double-quoted identifier
    if (c == '"' && !in_single_quote) {
      if (in_double_quote) {
        set_color(i, i + 1, Color::DEFAULT);
        in_double_quote = false;
        ++i;
        continue;
      } else {
        in_double_quote = true;
        set_color(i, i + 1, Color::DEFAULT);
        ++i;
        continue;
      }
    }

    if (in_double_quote) {
      set_color(i, i + 1, Color::DEFAULT);
      ++i;
      continue;
    }

    // Numeric literals
    if (std::isdigit(static_cast<unsigned char>(c)) ||
        (c == '.' && i + 1 < len &&
         std::isdigit(static_cast<unsigned char>(input[i + 1])))) {
      // Check that we're not in the middle of an identifier
      if (i > 0 && IsIdentChar(input[i - 1])) {
        set_color(i, i + 1, Color::DEFAULT);
        ++i;
        continue;
      }
      size_t start = i;
      bool has_dot = (c == '.');
      ++i;
      while (i < len) {
        char nc = input[i];
        if (std::isdigit(static_cast<unsigned char>(nc))) {
          ++i;
        } else if (nc == '.' && !has_dot) {
          has_dot = true;
          ++i;
        } else if ((nc == 'e' || nc == 'E') && i + 1 < len) {
          ++i;
          if (i < len && (input[i] == '+' || input[i] == '-')) ++i;
        } else {
          break;
        }
      }
      // Make sure the number isn't followed by an identifier character
      if (i < len && IsIdentChar(input[i])) {
        // This is actually an identifier, not a number
        set_color(start, i, Color::DEFAULT);
      } else {
        set_color(start, i, Color::MAGENTA);
      }
      continue;
    }

    // Identifiers and keywords
    if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
      size_t start = i;
      ++i;
      while (i < len && IsIdentChar(input[i])) ++i;
      std::string word = input.substr(start, i - start);
      std::string upper_word = ToUpper(word);

      // Look ahead past whitespace for '(' to detect function calls
      size_t lookahead = i;
      while (lookahead < len &&
             std::isspace(static_cast<unsigned char>(input[lookahead]))) {
        ++lookahead;
      }
      bool followed_by_paren = (lookahead < len && input[lookahead] == '(');

      if (followed_by_paren &&
          (functions_.count(upper_word) || keywords_.count(upper_word))) {
        set_color(start, i, Color::CYAN);
      } else if (keywords_.count(upper_word)) {
        set_color(start, i, Color::GREEN);
      } else {
        set_color(start, i, Color::DEFAULT);
      }
      continue;
    }

    // Parentheses — track depth
    if (c == '(') {
      ++paren_depth;
      set_color(i, i + 1, Color::DEFAULT);
      ++i;
      continue;
    }
    if (c == ')') {
      if (paren_depth <= 0) {
        set_color(i, i + 1, Color::RED);  // Unmatched closing paren
      } else {
        --paren_depth;
        set_color(i, i + 1, Color::DEFAULT);
      }
      ++i;
      continue;
    }

    // Operators and punctuation
    set_color(i, i + 1, Color::DEFAULT);
    ++i;
  }

  // Mark unclosed strings as errors
  if (in_single_quote) {
    // Find the opening quote and color everything from it as error
    for (size_t cp = 0; cp < num_codepoints; ++cp) {
      if (colors[cp] == Color::YELLOW) {
        // Re-color as bright red from here
        colors[cp] = Color::BRIGHTRED;
      }
    }
  }

  // Mark unclosed block comments
  if (in_block_comment) {
    // Comments are already colored GRAY — leave as-is (continuation expected)
  }
}

}  // namespace gizmosql::client
