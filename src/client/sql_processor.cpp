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

#include "sql_processor.hpp"

#include <algorithm>
#include <cctype>

namespace gizmosql::client {

bool SqlProcessor::AccumulateLine(const std::string& line) {
  if (!buffer_.empty()) {
    buffer_ += '\n';
  }
  buffer_ += line;

  // Scan the newly added line for state changes
  for (size_t i = buffer_.size() - line.size(); i < buffer_.size(); ++i) {
    char c = buffer_[i];
    char next = (i + 1 < buffer_.size()) ? buffer_[i + 1] : '\0';

    if (in_block_comment_) {
      if (c == '*' && next == '/') {
        in_block_comment_ = false;
        ++i;  // skip '/'
      }
      continue;
    }

    if (in_single_quote_) {
      if (c == '\'' && next == '\'') {
        ++i;  // escaped quote
      } else if (c == '\'') {
        in_single_quote_ = false;
      }
      continue;
    }

    if (in_double_quote_) {
      if (c == '"' && next == '"') {
        ++i;  // escaped quote
      } else if (c == '"') {
        in_double_quote_ = false;
      }
      continue;
    }

    // Not in any quoted/comment context
    if (c == '\'') {
      in_single_quote_ = true;
    } else if (c == '"') {
      in_double_quote_ = true;
    } else if (c == '-' && next == '-') {
      // Line comment: rest of line is comment, stop processing
      break;
    } else if (c == '/' && next == '*') {
      in_block_comment_ = true;
      ++i;  // skip '*'
    } else if (c == ';') {
      semicolon_pos_ = i;
      return true;  // Statement complete
    }
  }

  return false;
}

std::string SqlProcessor::GetStatement() const {
  // Return everything up to (but not including) the semicolon
  std::string stmt;
  if (semicolon_pos_ != std::string::npos) {
    stmt = buffer_.substr(0, semicolon_pos_);
  } else {
    stmt = buffer_;
  }
  // Trim trailing whitespace
  while (!stmt.empty() && std::isspace(stmt.back())) {
    stmt.pop_back();
  }
  return stmt;
}

std::string SqlProcessor::GetRemainder() const {
  if (semicolon_pos_ != std::string::npos && semicolon_pos_ + 1 < buffer_.size()) {
    std::string remainder = buffer_.substr(semicolon_pos_ + 1);
    // Trim leading whitespace
    size_t start = remainder.find_first_not_of(" \t\n\r");
    if (start == std::string::npos) return "";
    return remainder.substr(start);
  }
  return "";
}

void SqlProcessor::Reset() {
  buffer_.clear();
  semicolon_pos_ = std::string::npos;
  in_single_quote_ = false;
  in_double_quote_ = false;
  in_block_comment_ = false;
}

bool SqlProcessor::IsAccumulating() const {
  return !buffer_.empty();
}

bool SqlProcessor::IsUpdateStatement(const std::string& sql) {
  // Find the first non-whitespace character
  std::string trimmed = sql;
  size_t start = trimmed.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) return false;
  trimmed = trimmed.substr(start);

  // Convert first 10 chars to uppercase for prefix matching
  std::string prefix = trimmed.substr(0, 10);
  std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);

  return prefix.find("INSERT") == 0 ||
         prefix.find("UPDATE") == 0 ||
         prefix.find("DELETE") == 0 ||
         prefix.find("CREATE") == 0 ||
         prefix.find("DROP") == 0 ||
         prefix.find("ALTER") == 0 ||
         prefix.find("BEGIN") == 0 ||
         prefix.find("COMMIT") == 0 ||
         prefix.find("ROLLBACK") == 0 ||
         prefix.find("SET") == 0 ||
         prefix.find("LOAD") == 0 ||
         prefix.find("INSTALL") == 0 ||
         prefix.find("COPY") == 0 ||
         prefix.find("ATTACH") == 0 ||
         prefix.find("DETACH") == 0 ||
         prefix.find("USE") == 0;
}

bool SqlProcessor::IsDdlStatement(const std::string& sql) {
  std::string trimmed = sql;
  size_t start = trimmed.find_first_not_of(" \t\n\r");
  if (start == std::string::npos) return false;
  trimmed = trimmed.substr(start);

  std::string prefix = trimmed.substr(0, 10);
  std::transform(prefix.begin(), prefix.end(), prefix.begin(), ::toupper);

  return prefix.find("CREATE") == 0 ||
         prefix.find("DROP") == 0 ||
         prefix.find("ALTER") == 0 ||
         prefix.find("ATTACH") == 0 ||
         prefix.find("DETACH") == 0 ||
         prefix.find("USE") == 0 ||
         prefix.find("CALL") == 0;
}

}  // namespace gizmosql::client
