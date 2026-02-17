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
#include <vector>

#include <replxx.hxx>

#include "flight_connection.hpp"

namespace gizmosql::client {

struct TableEntry {
  std::string catalog;
  std::string schema;
  std::string table_name;
  std::string table_type;
};

struct SchemaEntry {
  std::string catalog;
  std::string schema_name;
};

class ShellCompleter {
 public:
  explicit ShellCompleter(FlightConnection& conn);

  // Completion callback for replxx
  replxx::Replxx::completions_t Complete(const std::string& input,
                                         int& context_len);

  // Hint callback for replxx
  replxx::Replxx::hints_t Hint(const std::string& input, int& context_len,
                                replxx::Replxx::Color& color);

  // Invalidate the schema cache (call after DDL or .connect)
  void InvalidateCache();

  // Get the list of SQL keywords (for testing)
  static const std::vector<std::string>& GetSqlKeywords();

  // Get the list of dot commands (for testing)
  static const std::vector<std::string>& GetDotCommands();

 private:
  enum class CompletionContext {
    DOT_COMMAND,
    SCHEMA_QUALIFIED,
    TABLE_NAME,
    SQL_GENERAL,
  };

  void EnsureCachePopulated();
  CompletionContext DetermineContext(const std::string& input,
                                    const std::string& prefix) const;
  std::string ExtractPrefix(const std::string& input, int& context_len) const;

  // Apply case matching: if user typed lowercase prefix, return lowercase
  static std::string MatchCase(const std::string& candidate,
                                const std::string& prefix);

  FlightConnection& conn_;
  std::vector<TableEntry> tables_;
  std::vector<SchemaEntry> schemas_;
  bool cache_populated_ = false;
};

}  // namespace gizmosql::client
