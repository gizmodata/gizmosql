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

#include "rbac_types.h"

#include <cctype>

namespace gizmosql::enterprise::rbac {

std::string PrivilegeName(Privilege privilege) {
  switch (privilege) {
    case Privilege::kSelect:
      return "SELECT";
    case Privilege::kInsert:
      return "INSERT";
    case Privilege::kUpdate:
      return "UPDATE";
    case Privilege::kDelete:
      return "DELETE";
    case Privilege::kCreateTable:
      return "CREATE TABLE";
    case Privilege::kCreateView:
      return "CREATE VIEW";
    case Privilege::kCreateSchema:
      return "CREATE SCHEMA";
    case Privilege::kDropTable:
      return "DROP TABLE";
    case Privilege::kDropView:
      return "DROP VIEW";
    case Privilege::kDropSchema:
      return "DROP SCHEMA";
    case Privilege::kAlterTable:
      return "ALTER TABLE";
    case Privilege::kAlterView:
      return "ALTER VIEW";
    case Privilege::kAll:
      return "ALL";
  }
  return "UNKNOWN";
}

const std::vector<Privilege>& TableLevelPrivileges() {
  static const std::vector<Privilege> kPrivileges = {
      Privilege::kSelect, Privilege::kInsert, Privilege::kUpdate, Privilege::kDelete};
  return kPrivileges;
}

// DuckDB compares identifiers case-insensitively by ASCII lowercasing, whether
// or not they were quoted; anything outside ASCII compares byte for byte.
std::string FoldIdentifier(const std::string& identifier) {
  std::string folded = identifier;
  for (auto& c : folded) {
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  }
  return folded;
}

bool IdentifiersEqual(const std::string& a, const std::string& b) {
  return FoldIdentifier(a) == FoldIdentifier(b);
}

std::string QuoteIdentifierIfNeeded(const std::string& identifier) {
  bool plain = !identifier.empty() &&
               (std::isalpha(static_cast<unsigned char>(identifier[0])) || identifier[0] == '_');
  for (char c : identifier) {
    if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) plain = false;
  }
  if (plain) return identifier;
  std::string quoted = "\"";
  for (char c : identifier) {
    if (c == '"') quoted += '"';
    quoted += c;
  }
  return quoted + "\"";
}

std::string ObjectPath::ToString() const {
  std::string out = QuoteIdentifierIfNeeded(catalog);
  for (const auto& part : schema_path) out += "." + QuoteIdentifierIfNeeded(part);
  if (!object_name.empty()) out += "." + QuoteIdentifierIfNeeded(object_name);
  return out;
}

}  // namespace gizmosql::enterprise::rbac
