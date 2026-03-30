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
#include <unordered_set>

#include <replxx.hxx>

namespace gizmosql::client {

class SyntaxHighlighter {
 public:
  SyntaxHighlighter();

  /// Highlight callback for replxx. Assigns colors to each character position.
  void Highlight(const std::string& input, replxx::Replxx::colors_t& colors);

  /// Enable or disable highlighting. When disabled, all characters get DEFAULT.
  void SetEnabled(bool enabled) { enabled_ = enabled; }
  bool IsEnabled() const { return enabled_; }

 private:
  bool enabled_ = true;
  std::unordered_set<std::string> keywords_;
  std::unordered_set<std::string> functions_;
};

}  // namespace gizmosql::client
