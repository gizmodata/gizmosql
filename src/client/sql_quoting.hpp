// Licensed under the Apache License, Version 2.0.
#pragma once

#include <string>
#include <string_view>

namespace gizmosql::client {

// Identifiers cannot be bound. Quote each component of a qualified name separately.
inline std::string QuoteSqlIdentifier(std::string_view value) {
  std::string result = "\"";
  for (char ch : value) {
    result += ch;
    if (ch == '"') result += '"';
  }
  return result + '"';
}

// Prefer bind parameters. Use this only for SQL output or statements such as
// GizmoSQL SET whose server-side parser requires a literal expression.
inline std::string QuoteSqlLiteral(std::string_view value) {
  std::string result = "'";
  for (char ch : value) {
    result += ch;
    if (ch == '\'') result += '\'';
  }
  return result + '\'';
}

}  // namespace gizmosql::client
