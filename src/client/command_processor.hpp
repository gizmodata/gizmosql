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

#include "client_config.hpp"
#include "flight_connection.hpp"

namespace gizmosql::client {

enum class CommandResult {
  OK,
  EXIT,
  ERROR,
  NOT_A_COMMAND,
};

class CommandProcessor {
 public:
  CommandProcessor(FlightConnection& conn, ClientConfig& config)
      : conn_(conn), config_(config) {}

  // Returns true if the line starts with '.' (dot command)
  static bool IsDotCommand(const std::string& line);

  // Process a dot command. Returns the result.
  CommandResult Process(const std::string& line);

 private:
  void ShowHelp(const std::string& pattern);
  void ShowSettings();
  CommandResult HandleConnect(const std::vector<std::string>& args);

  FlightConnection& conn_;
  ClientConfig& config_;
  std::ofstream* output_file_ = nullptr;
  bool once_mode_ = false;
};

}  // namespace gizmosql::client
