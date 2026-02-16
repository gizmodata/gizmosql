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

#include <string>

namespace gizmosql::client {

class SqlProcessor {
 public:
  // Returns true when a complete statement is ready (terminated by semicolon).
  bool AccumulateLine(const std::string& line);

  std::string GetStatement() const;
  std::string GetRemainder() const;
  void Reset();
  bool IsAccumulating() const;

  // Check if a SQL statement is a DML/DDL command that should use ExecuteUpdate.
  static bool IsUpdateStatement(const std::string& sql);

 private:
  std::string buffer_;
  size_t semicolon_pos_ = std::string::npos;
  bool in_single_quote_ = false;
  bool in_double_quote_ = false;
  bool in_block_comment_ = false;
};

}  // namespace gizmosql::client
