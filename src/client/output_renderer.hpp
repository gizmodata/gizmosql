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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <arrow/table.h>

#include "client_config.hpp"

namespace gizmosql::client {

struct RenderResult {
  int64_t rows_rendered = 0;
  int columns_rendered = 0;
  bool footer_rendered = false;  // true if footer was rendered inside the table
};

class OutputRenderer {
 public:
  virtual ~OutputRenderer() = default;
  virtual RenderResult Render(const arrow::Table& table, std::ostream& out) = 0;

  bool show_headers = true;
  std::string null_value = "NULL";
  int max_rows = 0;   // 0 = unlimited
  int max_width = 0;  // 0 = unlimited
};

std::unique_ptr<OutputRenderer> CreateRenderer(OutputMode mode,
                                                const ClientConfig& config);

// Utility: get string value of a cell from a chunked array
std::string GetCellValue(const std::shared_ptr<arrow::ChunkedArray>& column,
                         int64_t row_index, const std::string& null_value);

// Utility: get terminal width
int GetTerminalWidth();

// Map Arrow type to DuckDB-friendly display name (e.g., "varchar", "bigint")
std::string FriendlyTypeName(const std::shared_ptr<arrow::DataType>& type);

}  // namespace gizmosql::client
