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

#include "rbac_ddl.h"

#include <algorithm>
#include <cctype>
#include <cstring>

namespace gizmosql::enterprise::rbac {

namespace {

enum class TokenType { kWord, kQuotedIdentifier, kString, kNumber, kSymbol, kEnd };

struct Token {
  TokenType type;
  std::string text;  // words as written; identifiers/strings unquoted
  size_t offset;
};

bool IsWordStart(unsigned char c) { return std::isalpha(c) || c == '_' || c >= 0x80; }
bool IsWordChar(unsigned char c) {
  return std::isalnum(c) || c == '_' || c == '$' || c >= 0x80;
}

arrow::Status SyntaxError(const std::string& message) {
  return arrow::Status::Invalid("GizmoSQL access control: " + message);
}

// Comment and whitespace skipping shared by the tokenizer and the quick
// classifier. Returns false on an unterminated block comment.
bool SkipSpaceAndComments(const std::string& sql, size_t& pos) {
  while (pos < sql.size()) {
    const unsigned char c = sql[pos];
    if (std::isspace(c)) {
      ++pos;
    } else if (c == '-' && pos + 1 < sql.size() && sql[pos + 1] == '-') {
      while (pos < sql.size() && sql[pos] != '\n') ++pos;
    } else if (c == '/' && pos + 1 < sql.size() && sql[pos + 1] == '*') {
      // Block comments nest, as in DuckDB and Postgres.
      int depth = 1;
      pos += 2;
      while (pos < sql.size() && depth > 0) {
        if (sql[pos] == '/' && pos + 1 < sql.size() && sql[pos + 1] == '*') {
          ++depth;
          pos += 2;
        } else if (sql[pos] == '*' && pos + 1 < sql.size() && sql[pos + 1] == '/') {
          --depth;
          pos += 2;
        } else {
          ++pos;
        }
      }
      if (depth > 0) return false;
    } else {
      break;
    }
  }
  return true;
}

arrow::Result<std::vector<Token>> Tokenize(const std::string& sql) {
  std::vector<Token> tokens;
  size_t pos = 0;
  while (true) {
    if (!SkipSpaceAndComments(sql, pos)) return SyntaxError("unterminated comment");
    if (pos >= sql.size()) break;
    const size_t start = pos;
    const unsigned char c = sql[pos];
    if (IsWordStart(c)) {
      while (pos < sql.size() && IsWordChar(sql[pos])) ++pos;
      tokens.push_back({TokenType::kWord, sql.substr(start, pos - start), start});
    } else if (c == '"' || c == '\'') {
      const char quote = static_cast<char>(c);
      std::string value;
      ++pos;
      bool closed = false;
      while (pos < sql.size()) {
        if (sql[pos] == quote) {
          if (pos + 1 < sql.size() && sql[pos + 1] == quote) {
            value += quote;
            pos += 2;
            continue;
          }
          ++pos;
          closed = true;
          break;
        }
        value += sql[pos++];
      }
      if (!closed) {
        return SyntaxError(quote == '"' ? "unterminated quoted identifier"
                                        : "unterminated string literal");
      }
      if (quote == '"' && value.empty()) return SyntaxError("empty quoted identifier");
      tokens.push_back(
          {quote == '"' ? TokenType::kQuotedIdentifier : TokenType::kString, value, start});
    } else if (std::isdigit(c)) {
      // Not part of any access-control statement, but tokenized so trailing
      // SQL gets the "one statement at a time" error rather than a lexer one.
      while (pos < sql.size() && (std::isalnum(static_cast<unsigned char>(sql[pos])) ||
                                  sql[pos] == '.')) {
        ++pos;
      }
      tokens.push_back({TokenType::kNumber, sql.substr(start, pos - start), start});
    } else if (c == '.' || c == ',' || c == '(' || c == ')' || c == ';') {
      tokens.push_back({TokenType::kSymbol, std::string(1, static_cast<char>(c)), start});
      ++pos;
    } else if (std::strchr("=<>!+-*/%|&~^:[]{}", c) != nullptr) {
      // Operators appear only inside a policy's SQL expression, which is taken
      // as written and parsed by DuckDB. ($ and \ stay errors: no dollar quoting.)
      tokens.push_back({TokenType::kSymbol, std::string(1, static_cast<char>(c)), start});
      ++pos;
    } else {
      return SyntaxError(std::string("unexpected character '") + static_cast<char>(c) +
                         "' at position " + std::to_string(pos + 1));
    }
  }
  tokens.push_back({TokenType::kEnd, "", sql.size()});
  return tokens;
}

bool KeywordEquals(const std::string& word, const char* keyword) {
  size_t i = 0;
  for (; keyword[i] != '\0'; ++i) {
    if (i >= word.size()) return false;
    if (std::toupper(static_cast<unsigned char>(word[i])) != keyword[i]) return false;
  }
  return i == word.size();
}

class Parser {
 public:
  Parser(std::vector<Token> tokens, std::string sql)
      : tokens_(std::move(tokens)), sql_(std::move(sql)) {}

  arrow::Result<RbacCommand> ParseStatement() {
    ARROW_ASSIGN_OR_RAISE(auto command, ParseCommand());
    // One statement only: allow a single trailing semicolon, nothing else.
    if (PeekSymbol(";")) ++pos_;
    if (Peek().type != TokenType::kEnd) {
      return SyntaxError("unexpected '" + Describe(Peek()) +
                         "' after the end of the statement. Access-control statements "
                         "run one at a time");
    }
    return command;
  }

 private:
  const Token& Peek(size_t ahead = 0) const {
    const size_t i = std::min(pos_ + ahead, tokens_.size() - 1);
    return tokens_[i];
  }

  // A keyword is an unquoted word; a quoted identifier never matches one.
  bool PeekKeyword(const char* keyword, size_t ahead = 0) const {
    const auto& t = Peek(ahead);
    return t.type == TokenType::kWord && KeywordEquals(t.text, keyword);
  }
  bool PeekSymbol(const char* symbol) const {
    return Peek().type == TokenType::kSymbol && Peek().text == symbol;
  }
  bool AcceptKeyword(const char* keyword) {
    if (!PeekKeyword(keyword)) return false;
    ++pos_;
    return true;
  }
  arrow::Status ExpectKeyword(const char* keyword) {
    if (AcceptKeyword(keyword)) return arrow::Status::OK();
    return SyntaxError(std::string("expected ") + keyword + " but found '" + Describe(Peek()) +
                       "'");
  }
  arrow::Status ExpectSymbol(const char* symbol) {
    if (PeekSymbol(symbol)) {
      ++pos_;
      return arrow::Status::OK();
    }
    return SyntaxError(std::string("expected '") + symbol + "' but found '" +
                       Describe(Peek()) + "'");
  }

  static std::string Describe(const Token& t) {
    if (t.type == TokenType::kEnd) return "end of statement";
    if (t.type == TokenType::kString) return "'" + t.text + "'";
    if (t.type == TokenType::kQuotedIdentifier) return "\"" + t.text + "\"";
    return t.text;
  }

  arrow::Result<std::string> ParseIdentifier(const char* what) {
    const auto& t = Peek();
    if (t.type == TokenType::kWord || t.type == TokenType::kQuotedIdentifier) {
      ++pos_;
      return t.text;
    }
    return SyntaxError(std::string("expected ") + what + " but found '" + Describe(t) + "'");
  }

  // A user or role name: name, or tenant/name for a tenant's users and roles.
  arrow::Result<std::string> ParsePrincipalName(const char* what) {
    ARROW_ASSIGN_OR_RAISE(auto first, ParseIdentifier(what));
    if (!PeekSymbol("/")) return first;
    ++pos_;
    ARROW_ASSIGN_OR_RAISE(auto second, ParseIdentifier(what));
    return first + "/" + second;
  }

  arrow::Result<QualifiedName> ParseQualifiedName(const char* what) {
    QualifiedName name;
    ARROW_ASSIGN_OR_RAISE(auto first, ParseIdentifier(what));
    name.parts.push_back(std::move(first));
    while (PeekSymbol(".")) {
      ++pos_;
      ARROW_ASSIGN_OR_RAISE(auto next, ParseIdentifier(what));
      name.parts.push_back(std::move(next));
    }
    return name;
  }

  arrow::Result<std::string> ParseStringLiteral(const char* what) {
    const auto& t = Peek();
    if (t.type == TokenType::kString) {
      ++pos_;
      return t.text;
    }
    return SyntaxError(std::string("expected ") + what + " as a quoted string but found '" +
                       Describe(t) + "'");
  }

  bool ParseIfNotExists() {
    if (PeekKeyword("IF") && PeekKeyword("NOT", 1) && PeekKeyword("EXISTS", 2)) {
      pos_ += 3;
      return true;
    }
    return false;
  }
  bool ParseIfExists() {
    if (PeekKeyword("IF") && PeekKeyword("EXISTS", 1)) {
      pos_ += 2;
      return true;
    }
    return false;
  }

  arrow::Result<RbacCommand> ParseCommand() {
    if (AcceptKeyword("CREATE")) {
      bool or_replace = false;
      if (PeekKeyword("OR") && PeekKeyword("REPLACE", 1)) {
        pos_ += 2;
        or_replace = true;
      }
      if (PeekKeyword("ROW") && PeekKeyword("POLICY", 1)) {
        pos_ += 2;
        return ParseCreateRowPolicy(or_replace);
      }
      if (PeekKeyword("MASKING") && PeekKeyword("POLICY", 1)) {
        pos_ += 2;
        return ParseCreateMaskingPolicy(or_replace);
      }
      if (or_replace) return SyntaxError("expected ROW POLICY or MASKING POLICY after CREATE OR REPLACE");
      if (PeekKeyword("GROUP") && PeekKeyword("MAPPING", 1)) {
        pos_ += 2;
        return ParseGroupMapping(GroupMappingCommand::Kind::kCreate);
      }
      if (PeekKeyword("SCOPED") && PeekKeyword("TOKEN", 1)) {
        pos_ += 2;
        return ParseScopedToken(ScopedTokenCommand::Kind::kCreate);
      }
      if (AcceptKeyword("TENANT")) {
        TenantCommand cmd;
        cmd.if_exists = ParseIfNotExists();
        ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a tenant name"));
        return cmd;
      }
      if (AcceptKeyword("USER")) return ParseCreateUser();
      if (AcceptKeyword("ROLE")) return ParseCreateRole();
      return SyntaxError("expected USER or ROLE after CREATE");
    }
    if (AcceptKeyword("DROP")) {
      if (PeekKeyword("GROUP") && PeekKeyword("MAPPING", 1)) {
        pos_ += 2;
        return ParseGroupMapping(GroupMappingCommand::Kind::kDrop);
      }
      if (PeekKeyword("SCOPED") && PeekKeyword("TOKEN", 1)) {
        pos_ += 2;
        return ParseScopedToken(ScopedTokenCommand::Kind::kDrop);
      }
      if (AcceptKeyword("TENANT")) {
        TenantCommand cmd;
        cmd.create = false;
        cmd.if_exists = ParseIfExists();
        ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a tenant name"));
        return cmd;
      }
      if (PeekKeyword("ROW") && PeekKeyword("POLICY", 1)) {
        pos_ += 2;
        DropRowPolicyCommand cmd;
        cmd.if_exists = ParseIfExists();
        ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a policy name"));
        ARROW_RETURN_NOT_OK(ExpectKeyword("ON"));
        ARROW_ASSIGN_OR_RAISE(cmd.table, ParseQualifiedName("a table name"));
        return cmd;
      }
      if (PeekKeyword("MASKING") && PeekKeyword("POLICY", 1)) {
        pos_ += 2;
        DropMaskingPolicyCommand cmd;
        cmd.if_exists = ParseIfExists();
        ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a policy name"));
        return cmd;
      }
      const bool is_user = PeekKeyword("USER");
      if (!is_user && !PeekKeyword("ROLE")) return SyntaxError("expected USER or ROLE after DROP");
      ++pos_;
      const bool if_exists = ParseIfExists();
      ARROW_ASSIGN_OR_RAISE(auto name, ParsePrincipalName(is_user ? "a user name" : "a role name"));
      if (is_user) return DropUserCommand{std::move(name), if_exists};
      return DropRoleCommand{std::move(name), if_exists};
    }
    if (AcceptKeyword("ALTER")) {
      if (AcceptKeyword("USER")) return ParseAlterUser();
      if (AcceptKeyword("CATALOG") || AcceptKeyword("DATABASE")) {
        CatalogTenantCommand cmd;
        ARROW_ASSIGN_OR_RAISE(cmd.catalog, ParseIdentifier("a catalog name"));
        if (AcceptKeyword("UNSET")) {
          ARROW_RETURN_NOT_OK(ExpectKeyword("TENANT"));
          return cmd;
        }
        ARROW_RETURN_NOT_OK(ExpectKeyword("SET"));
        ARROW_RETURN_NOT_OK(ExpectKeyword("TENANT"));
        ARROW_ASSIGN_OR_RAISE(auto tenant, ParseIdentifier("a tenant name"));
        cmd.tenant = std::move(tenant);
        return cmd;
      }
      if (AcceptKeyword("TENANT")) {
        TenantDefaultCatalogCommand cmd;
        ARROW_ASSIGN_OR_RAISE(cmd.tenant, ParseIdentifier("a tenant name"));
        const bool unset = AcceptKeyword("UNSET");
        if (!unset) ARROW_RETURN_NOT_OK(ExpectKeyword("SET"));
        ARROW_RETURN_NOT_OK(ExpectKeyword("DEFAULT"));
        ARROW_RETURN_NOT_OK(ExpectKeyword("CATALOG"));
        if (!unset) {
          ARROW_ASSIGN_OR_RAISE(cmd.catalog, ParseIdentifier("a catalog name"));
        }
        return cmd;
      }
      const bool is_view = PeekKeyword("VIEW");
      if (is_view || PeekKeyword("TABLE")) {
        ++pos_;
        if (!is_view) {
          // ALTER TABLE t MODIFY [COLUMN] c SET | UNSET MASKING POLICY ...
          const size_t at = pos_;
          auto table = ParseQualifiedName("a table name");
          if (table.ok() && AcceptKeyword("MODIFY")) return ParseSetColumnMask(*table);
          pos_ = at;
        }
        return ParseAlterOwner(is_view);
      }
      return SyntaxError("expected USER, TABLE or VIEW after ALTER");
    }
    if (AcceptKeyword("SET")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("ROLE"));
      SetRoleCommand cmd;
      if (AcceptKeyword("ALL") || AcceptKeyword("NONE")) return cmd;
      ARROW_ASSIGN_OR_RAISE(auto name, ParsePrincipalName("a role name"));
      cmd.role = std::move(name);
      return cmd;
    }
    if (AcceptKeyword("RESET")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("ROLE"));
      return SetRoleCommand{};
    }
    if (AcceptKeyword("GRANT")) return ParseGrantOrRevoke(/*grant=*/true);
    if (AcceptKeyword("REVOKE")) return ParseGrantOrRevoke(/*grant=*/false);
    if (AcceptKeyword("SHOW")) {
      if (PeekKeyword("ROW") && PeekKeyword("POLICIES", 1)) {
        pos_ += 2;
        ShowPoliciesCommand cmd;
        if (AcceptKeyword("ON")) {
          ARROW_ASSIGN_OR_RAISE(auto table, ParseQualifiedName("a table name"));
          cmd.table = std::move(table);
        }
        return cmd;
      }
      if (AcceptKeyword("TENANTS")) return ShowTenantsCommand{};
      if (PeekKeyword("GROUP") && PeekKeyword("MAPPINGS", 1)) {
        pos_ += 2;
        return GroupMappingCommand{};
      }
      if (PeekKeyword("SCOPED") && PeekKeyword("TOKENS", 1)) {
        pos_ += 2;
        return ScopedTokenCommand{};
      }
      if (PeekKeyword("MASKING") && PeekKeyword("POLICIES", 1)) {
        pos_ += 2;
        ShowPoliciesCommand cmd;
        cmd.masking = true;
        return cmd;
      }
      ARROW_RETURN_NOT_OK(ExpectKeyword("GRANTS"));
      return ParseShowGrants();
    }
    return SyntaxError("not an access-control statement");
  }

  arrow::Result<RbacCommand> ParseScopedToken(ScopedTokenCommand::Kind kind) {
    ScopedTokenCommand cmd;
    cmd.kind = kind;
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a token name"));
    if (AcceptKeyword("FOR")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("USER"));
      ARROW_ASSIGN_OR_RAISE(auto user, ParsePrincipalName("a user name"));
      cmd.for_user = std::move(user);
    }
    if (kind == ScopedTokenCommand::Kind::kDrop) return cmd;
    ARROW_RETURN_NOT_OK(ExpectKeyword("ALLOW"));
    do {
      Privilege p;
      if (AcceptKeyword("SELECT")) {
        p = Privilege::kSelect;
      } else if (AcceptKeyword("INSERT")) {
        p = Privilege::kInsert;
      } else if (AcceptKeyword("UPDATE")) {
        p = Privilege::kUpdate;
      } else if (AcceptKeyword("DELETE")) {
        p = Privilege::kDelete;
      } else {
        return SyntaxError("a scoped token allows SELECT, INSERT, UPDATE or DELETE, not '" +
                           Describe(Peek()) + "'");
      }
      if (std::find(cmd.privileges.begin(), cmd.privileges.end(), p) == cmd.privileges.end()) {
        cmd.privileges.push_back(p);
      }
    } while (PeekSymbol(",") && (++pos_, true));
    if (AcceptKeyword("ON")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("CATALOG"));
      do {
        ARROW_ASSIGN_OR_RAISE(auto catalog, ParseIdentifier("a catalog name"));
        cmd.catalogs.push_back(std::move(catalog));
      } while (PeekSymbol(",") && (++pos_, true));
    }
    if (AcceptKeyword("VALID")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("FOR"));
      ARROW_ASSIGN_OR_RAISE(auto valid_for, ParseStringLiteral("a duration such as '8 hours'"));
      cmd.valid_for = std::move(valid_for);
    }
    // FOR USER may also come last.
    if (!cmd.for_user && AcceptKeyword("FOR")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("USER"));
      ARROW_ASSIGN_OR_RAISE(auto user, ParsePrincipalName("a user name"));
      cmd.for_user = std::move(user);
    }
    return cmd;
  }

  arrow::Result<RbacCommand> ParseGroupMapping(GroupMappingCommand::Kind kind) {
    GroupMappingCommand cmd;
    cmd.kind = kind;
    ARROW_ASSIGN_OR_RAISE(cmd.group, ParseStringLiteral("the group, as a string"));
    if (cmd.group.empty()) return SyntaxError("the group cannot be empty");
    if (AcceptKeyword("ISSUER")) {
      ARROW_ASSIGN_OR_RAISE(cmd.issuer, ParseStringLiteral("the issuer, as a string"));
    }
    if (AcceptKeyword("TO")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("ROLE"));
      ARROW_ASSIGN_OR_RAISE(auto role, ParsePrincipalName("a role name"));
      cmd.role = std::move(role);
    } else if (kind == GroupMappingCommand::Kind::kCreate) {
      return SyntaxError("expected TO ROLE role after the group");
    }
    return cmd;
  }

  arrow::Result<RbacCommand> ParseCreateUser() {
    CreateUserCommand cmd;
    cmd.if_not_exists = ParseIfNotExists();
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParsePrincipalName("a user name"));
    AcceptKeyword("WITH");
    if (AcceptKeyword("PASSWORD")) {
      ARROW_ASSIGN_OR_RAISE(auto password, ParseStringLiteral("the password"));
      cmd.password = std::move(password);
    }
    return cmd;
  }

  arrow::Result<RbacCommand> ParseCreateRole() {
    CreateRoleCommand cmd;
    cmd.if_not_exists = ParseIfNotExists();
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParsePrincipalName("a role name"));
    return cmd;
  }

  arrow::Result<RbacCommand> ParseAlterUser() {
    AlterUserCommand cmd;
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParsePrincipalName("a user name"));
    AcceptKeyword("WITH");
    if (AcceptKeyword("PASSWORD")) {
      if (AcceptKeyword("NULL")) {
        cmd.action = AlterUserCommand::Action::kClearPassword;
      } else {
        cmd.action = AlterUserCommand::Action::kSetPassword;
        ARROW_ASSIGN_OR_RAISE(cmd.password, ParseStringLiteral("the password"));
      }
    } else if (AcceptKeyword("ENABLE")) {
      cmd.action = AlterUserCommand::Action::kEnable;
    } else if (AcceptKeyword("DISABLE")) {
      cmd.action = AlterUserCommand::Action::kDisable;
    } else if (PeekKeyword("SET") && PeekKeyword("ATTRIBUTE", 1)) {
      pos_ += 2;
      cmd.action = AlterUserCommand::Action::kSetAttribute;
      ARROW_ASSIGN_OR_RAISE(cmd.attribute, ParseIdentifier("an attribute name"));
      ARROW_RETURN_NOT_OK(ExpectSymbol("="));
      ARROW_ASSIGN_OR_RAISE(cmd.value, ParseStringLiteral("the attribute value"));
    } else if (PeekKeyword("UNSET") && PeekKeyword("ATTRIBUTE", 1)) {
      pos_ += 2;
      cmd.action = AlterUserCommand::Action::kUnsetAttribute;
      ARROW_ASSIGN_OR_RAISE(cmd.attribute, ParseIdentifier("an attribute name"));
    } else {
      return SyntaxError("expected PASSWORD, ENABLE, DISABLE, SET ATTRIBUTE or UNSET ATTRIBUTE "
                         "after ALTER USER " + cmd.name);
    }
    return cmd;
  }

  // The SQL text of a parenthesized group starting at the current "(": what is
  // between the parentheses, as written. Leaves the position after the ")".
  arrow::Result<std::string> ParseParenthesizedSql(const char* what) {
    if (!PeekSymbol("(")) {
      return SyntaxError(std::string("expected ( before ") + what);
    }
    const size_t open = Peek().offset;
    int depth = 0;
    while (Peek().type != TokenType::kEnd) {
      if (PeekSymbol("(")) ++depth;
      if (PeekSymbol(")") && --depth == 0) {
        const size_t close = Peek().offset;
        ++pos_;
        std::string text = sql_.substr(open + 1, close - open - 1);
        if (text.find_first_not_of(" \t\r\n") == std::string::npos) {
          return SyntaxError(std::string("empty ") + what);
        }
        return text;
      }
      ++pos_;
    }
    return SyntaxError(std::string("unbalanced parentheses in ") + what);
  }

  arrow::Result<RbacCommand> ParseCreateRowPolicy(bool or_replace) {
    CreateRowPolicyCommand cmd;
    cmd.or_replace = or_replace;
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a policy name"));
    ARROW_RETURN_NOT_OK(ExpectKeyword("ON"));
    ARROW_ASSIGN_OR_RAISE(cmd.table, ParseQualifiedName("a table name"));
    if (AcceptKeyword("AS")) {
      if (AcceptKeyword("RESTRICTIVE")) {
        cmd.permissive = false;
      } else {
        ARROW_RETURN_NOT_OK(ExpectKeyword("PERMISSIVE"));
      }
    }
    if (AcceptKeyword("TO")) {
      do {
        ARROW_ASSIGN_OR_RAISE(auto role, ParsePrincipalName("a role name"));
        cmd.roles.push_back(std::move(role));
      } while (PeekSymbol(",") && (++pos_, true));
    }
    ARROW_RETURN_NOT_OK(ExpectKeyword("USING"));
    ARROW_ASSIGN_OR_RAISE(cmd.predicate, ParseParenthesizedSql("the policy's USING predicate"));
    return cmd;
  }

  arrow::Result<RbacCommand> ParseCreateMaskingPolicy(bool or_replace) {
    CreateMaskingPolicyCommand cmd;
    cmd.or_replace = or_replace;
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a policy name"));
    ARROW_RETURN_NOT_OK(ExpectKeyword("AS"));
    ARROW_RETURN_NOT_OK(ExpectSymbol("("));
    ARROW_ASSIGN_OR_RAISE(cmd.parameter, ParseIdentifier("the parameter name"));
    // The parameter type runs to the matching ")".
    const size_t type_start = Peek().offset;
    int depth = 1;
    while (Peek().type != TokenType::kEnd) {
      if (PeekSymbol("(")) ++depth;
      if (PeekSymbol(")") && --depth == 0) break;
      ++pos_;
    }
    if (!PeekSymbol(")")) return SyntaxError("unbalanced parentheses in the parameter list");
    cmd.parameter_type = Trim(sql_.substr(type_start, Peek().offset - type_start));
    ++pos_;
    ARROW_RETURN_NOT_OK(ExpectKeyword("RETURNS"));
    // The return type runs to "->".
    const size_t ret_start = Peek().offset;
    while (Peek().type != TokenType::kEnd && !(PeekSymbol("-") && Peek(1).text == ">")) ++pos_;
    if (Peek().type == TokenType::kEnd) return SyntaxError("expected -> before the policy body");
    cmd.return_type = Trim(sql_.substr(ret_start, Peek().offset - ret_start));
    pos_ += 2;
    // The body is the rest of the statement (a trailing ";" is not part of it).
    const size_t body_start = Peek().offset;
    size_t body_end = sql_.size();
    size_t last = tokens_.size() - 1;  // kEnd
    if (last > 0 && tokens_[last - 1].type == TokenType::kSymbol && tokens_[last - 1].text == ";") {
      body_end = tokens_[last - 1].offset;
      last -= 1;
    }
    cmd.body = Trim(sql_.substr(body_start, body_end - body_start));
    if (cmd.parameter_type.empty() || cmd.return_type.empty() || cmd.body.empty()) {
      return SyntaxError("a masking policy needs a parameter type, a return type and a body");
    }
    pos_ = last;  // the body was taken whole
    return cmd;
  }

  arrow::Result<RbacCommand> ParseSetColumnMask(QualifiedName table) {
    SetColumnMaskCommand cmd;
    cmd.table = std::move(table);
    AcceptKeyword("COLUMN");
    ARROW_ASSIGN_OR_RAISE(cmd.column, ParseIdentifier("a column name"));
    if (AcceptKeyword("UNSET")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("MASKING"));
      ARROW_RETURN_NOT_OK(ExpectKeyword("POLICY"));
      return cmd;
    }
    ARROW_RETURN_NOT_OK(ExpectKeyword("SET"));
    ARROW_RETURN_NOT_OK(ExpectKeyword("MASKING"));
    ARROW_RETURN_NOT_OK(ExpectKeyword("POLICY"));
    ARROW_ASSIGN_OR_RAISE(auto policy, ParseIdentifier("a masking policy name"));
    cmd.policy = std::move(policy);
    if (AcceptKeyword("EXEMPT")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("ROLE"));
      do {
        ARROW_ASSIGN_OR_RAISE(auto role, ParsePrincipalName("a role name"));
        cmd.exempt_roles.push_back(std::move(role));
      } while (PeekSymbol(",") && (++pos_, true));
    }
    return cmd;
  }

  static std::string Trim(const std::string& s) {
    const auto b = s.find_first_not_of(" \t\r\n");
    if (b == std::string::npos) return "";
    const auto e = s.find_last_not_of(" \t\r\n");
    return s.substr(b, e - b + 1);
  }

  arrow::Result<RbacCommand> ParseAlterOwner(bool is_view) {
    AlterOwnerCommand cmd;
    cmd.is_view = is_view;
    ARROW_ASSIGN_OR_RAISE(cmd.target, ParseQualifiedName(is_view ? "a view name" : "a table name"));
    if (!(PeekKeyword("OWNER") && PeekKeyword("TO", 1))) {
      return SyntaxError(std::string("GizmoSQL only handles ALTER ") +
                         (is_view ? "VIEW" : "TABLE") + " ... OWNER TO here");
    }
    pos_ += 2;
    ARROW_ASSIGN_OR_RAISE(cmd.new_owner, ParseGrantee());
    return cmd;
  }

  arrow::Result<RbacCommand> ParseShowGrants() {
    ShowGrantsCommand cmd;
    if (AcceptKeyword("FOR") || AcceptKeyword("TO") || AcceptKeyword("OF")) {
      ARROW_ASSIGN_OR_RAISE(auto grantee, ParseGrantee());
      cmd.principal = std::move(grantee);
    }
    return cmd;
  }

  // [USER | ROLE] name. USER / ROLE are kind keywords only when another name
  // follows, so a principal literally named "role" still parses.
  arrow::Result<Grantee> ParseGrantee() {
    Grantee g;
    const auto& next = Peek(1);
    const bool name_follows =
        next.type == TokenType::kWord || next.type == TokenType::kQuotedIdentifier;
    if (name_follows && PeekKeyword("USER")) {
      g.kind = GranteeKind::kUser;
      ++pos_;
    } else if (name_follows && PeekKeyword("ROLE")) {
      g.kind = GranteeKind::kRole;
      ++pos_;
    }
    ARROW_ASSIGN_OR_RAISE(g.name, ParsePrincipalName("a user or role name"));
    return g;
  }

  arrow::Result<std::vector<Grantee>> ParseGranteeList() {
    std::vector<Grantee> grantees;
    do {
      ARROW_ASSIGN_OR_RAISE(auto g, ParseGrantee());
      grantees.push_back(std::move(g));
    } while (PeekSymbol(",") && (++pos_, true));
    return grantees;
  }

  // The longest privilege phrase starting here, if any. Only unquoted words
  // count, so a role named like a privilege must be quoted ("select").
  std::optional<std::pair<Privilege, size_t>> PeekPrivilege() const {
    if (PeekKeyword("SELECT")) return std::make_pair(Privilege::kSelect, size_t{1});
    if (PeekKeyword("INSERT")) return std::make_pair(Privilege::kInsert, size_t{1});
    if (PeekKeyword("UPDATE")) return std::make_pair(Privilege::kUpdate, size_t{1});
    if (PeekKeyword("DELETE")) return std::make_pair(Privilege::kDelete, size_t{1});
    if (PeekKeyword("ALL")) {
      return std::make_pair(Privilege::kAll, size_t{PeekKeyword("PRIVILEGES", 1) ? 2u : 1u});
    }
    struct Phrase {
      const char* verb;
      const char* object;
      Privilege privilege;
    };
    static const Phrase kPhrases[] = {
        {"CREATE", "TABLE", Privilege::kCreateTable},
        {"CREATE", "VIEW", Privilege::kCreateView},
        {"CREATE", "SCHEMA", Privilege::kCreateSchema},
        {"DROP", "TABLE", Privilege::kDropTable},
        {"DROP", "VIEW", Privilege::kDropView},
        {"DROP", "SCHEMA", Privilege::kDropSchema},
        {"ALTER", "TABLE", Privilege::kAlterTable},
        {"ALTER", "VIEW", Privilege::kAlterView},
    };
    for (const auto& phrase : kPhrases) {
      if (PeekKeyword(phrase.verb) && PeekKeyword(phrase.object, 1)) {
        return std::make_pair(phrase.privilege, size_t{2});
      }
    }
    return std::nullopt;
  }

  // A system privilege phrase starting here: its privilege and length.
  std::optional<std::pair<SystemPrivilege, size_t>> PeekSystemPrivilege() const {
    if (PeekKeyword("CHECKPOINT")) return std::make_pair(SystemPrivilege::kCheckpoint, size_t{1});
    struct Phrase {
      const char* first;
      const char* second;
      SystemPrivilege privilege;
    };
    static const Phrase kPhrases[] = {
        {"MANAGE", "CATALOGS", SystemPrivilege::kManageCatalogs},
        {"MANAGE", "EXTENSIONS", SystemPrivilege::kManageExtensions},
        {"MANAGE", "SECRETS", SystemPrivilege::kManageSecrets},
        {"MANAGE", "CONFIG", SystemPrivilege::kManageConfig},
        {"READ", "FILESYSTEM", SystemPrivilege::kReadFilesystem},
        {"WRITE", "FILESYSTEM", SystemPrivilege::kWriteFilesystem},
        {"EXPORT", "DATABASE", SystemPrivilege::kExportDatabase},
        {"IMPORT", "DATABASE", SystemPrivilege::kImportDatabase},
    };
    for (const auto& phrase : kPhrases) {
      if (PeekKeyword(phrase.first) && PeekKeyword(phrase.second, 1)) {
        return std::make_pair(phrase.privilege, size_t{2});
      }
    }
    return std::nullopt;
  }

  arrow::Result<RbacCommand> ParseSystemPrivileges(bool grant, bool admin_option_for) {
    SystemPrivilegeCommand cmd;
    cmd.grant = grant;
    cmd.with_admin_option = admin_option_for;
    do {
      auto found = PeekSystemPrivilege();
      if (!found) {
        return SyntaxError("expected a system privilege but found '" + Describe(Peek()) + "'");
      }
      cmd.privileges.push_back(found->first);
      pos_ += found->second;
    } while (PeekSymbol(",") && (++pos_, true));
    if (PeekKeyword("ON")) {
      return SyntaxError("system privileges apply to the whole server: no ON clause");
    }
    ARROW_RETURN_NOT_OK(ExpectKeyword(grant ? "TO" : "FROM"));
    ARROW_ASSIGN_OR_RAISE(cmd.grantees, ParseGranteeList());
    if (grant && PeekKeyword("WITH")) {
      ++pos_;
      ARROW_RETURN_NOT_OK(ExpectKeyword("ADMIN"));
      ARROW_RETURN_NOT_OK(ExpectKeyword("OPTION"));
      cmd.with_admin_option = true;
    }
    if (!grant) {
      if (AcceptKeyword("RESTRICT")) {
        cmd.cascade = false;
      } else {
        AcceptKeyword("CASCADE");
      }
    }
    return cmd;
  }

  arrow::Result<std::vector<std::string>> ParseColumnList() {
    std::vector<std::string> columns;
    ARROW_RETURN_NOT_OK(ExpectSymbol("("));
    do {
      ARROW_ASSIGN_OR_RAISE(auto column, ParseIdentifier("a column name"));
      columns.push_back(std::move(column));
    } while (PeekSymbol(",") && (++pos_, true));
    ARROW_RETURN_NOT_OK(ExpectSymbol(")"));
    return columns;
  }

  arrow::Result<RbacCommand> ParseGrantOrRevoke(bool grant) {
    bool option_for = false;
    bool admin_option_for = false;
    if (!grant) {
      // REVOKE GRANT OPTION FOR <privileges> / REVOKE ADMIN OPTION FOR <roles>
      if (PeekKeyword("GRANT") && PeekKeyword("OPTION", 1) && PeekKeyword("FOR", 2)) {
        pos_ += 3;
        option_for = true;
      } else if (PeekKeyword("ADMIN") && PeekKeyword("OPTION", 1) && PeekKeyword("FOR", 2)) {
        pos_ += 3;
        admin_option_for = true;
      }
    }

    if (!option_for && PeekSystemPrivilege()) return ParseSystemPrivileges(grant, admin_option_for);
    if (!admin_option_for && PeekPrivilege()) {
      ObjectPrivilegeCommand cmd;
      cmd.grant = grant;
      cmd.with_grant_option = option_for;  // for REVOKE: "only the grant option"
      do {
        auto found = PeekPrivilege();
        if (!found) {
          return SyntaxError("expected a privilege but found '" + Describe(Peek()) + "'");
        }
        PrivilegeItem item{found->first, {}};
        pos_ += found->second;
        if (PeekSymbol("(")) {
          if (item.privilege != Privilege::kSelect) {
            return SyntaxError("column lists are only supported for SELECT");
          }
          ARROW_ASSIGN_OR_RAISE(item.columns, ParseColumnList());
        }
        cmd.privileges.push_back(std::move(item));
      } while (PeekSymbol(",") && (++pos_, true));

      ARROW_RETURN_NOT_OK(ExpectKeyword("ON"));
      if (AcceptKeyword("SCHEMA")) {
        cmd.scope = GrantScope::kSchema;
        ARROW_ASSIGN_OR_RAISE(cmd.target, ParseQualifiedName("a schema name"));
      } else if (AcceptKeyword("CATALOG") || AcceptKeyword("DATABASE")) {
        cmd.scope = GrantScope::kCatalog;
        ARROW_ASSIGN_OR_RAISE(auto catalog, ParseIdentifier("a catalog name"));
        cmd.target.parts.push_back(std::move(catalog));
      } else {
        // [TABLE | VIEW] name — the keyword is optional, as in Postgres.
        // A following TO / FROM means TABLE or VIEW is itself the object name
        // (GRANT SELECT ON table TO r).
        if (PeekKeyword("TABLE") || PeekKeyword("VIEW")) {
          const auto& next = Peek(1);
          const bool is_name =
              next.type == TokenType::kQuotedIdentifier ||
              (next.type == TokenType::kWord && !KeywordEquals(next.text, "TO") &&
               !KeywordEquals(next.text, "FROM"));
          if (is_name) ++pos_;
        }
        cmd.scope = GrantScope::kObject;
        ARROW_ASSIGN_OR_RAISE(cmd.target, ParseQualifiedName("a table or view name"));
      }

      ARROW_RETURN_NOT_OK(ExpectKeyword(grant ? "TO" : "FROM"));
      ARROW_ASSIGN_OR_RAISE(cmd.grantees, ParseGranteeList());
      if (grant && PeekKeyword("WITH")) {
        ++pos_;
        ARROW_RETURN_NOT_OK(ExpectKeyword("GRANT"));
        ARROW_RETURN_NOT_OK(ExpectKeyword("OPTION"));
        cmd.with_grant_option = true;
      }
      if (!grant) {
        if (AcceptKeyword("RESTRICT")) {
          cmd.cascade = false;
        } else {
          AcceptKeyword("CASCADE");
        }
      }
      ARROW_RETURN_NOT_OK(ValidatePrivilegeScope(cmd));
      return cmd;
    }

    // Role membership: GRANT role, ... TO grantee, ... [WITH ADMIN OPTION]
    RoleMembershipCommand cmd;
    cmd.grant = grant;
    cmd.with_admin_option = admin_option_for;
    do {
      ARROW_ASSIGN_OR_RAISE(auto role, ParsePrincipalName("a role name or privilege"));
      cmd.roles.push_back(std::move(role));
    } while (PeekSymbol(",") && (++pos_, true));
    if (PeekKeyword("ON")) {
      return SyntaxError("'" + cmd.roles.front() +
                         "' is not a privilege. Privileges are SELECT, INSERT, UPDATE, DELETE, "
                         "ALL, CREATE TABLE, CREATE VIEW, CREATE SCHEMA, DROP TABLE, DROP VIEW, "
                         "DROP SCHEMA, ALTER TABLE and ALTER VIEW");
    }
    ARROW_RETURN_NOT_OK(ExpectKeyword(grant ? "TO" : "FROM"));
    ARROW_ASSIGN_OR_RAISE(cmd.grantees, ParseGranteeList());
    if (grant && PeekKeyword("WITH")) {
      ++pos_;
      ARROW_RETURN_NOT_OK(ExpectKeyword("ADMIN"));
      ARROW_RETURN_NOT_OK(ExpectKeyword("OPTION"));
      cmd.with_admin_option = true;
    }
    if (!grant) {
      if (!AcceptKeyword("RESTRICT")) AcceptKeyword("CASCADE");
    }
    return cmd;
  }

  // Which privileges make sense on which kind of target.
  static arrow::Status ValidatePrivilegeScope(const ObjectPrivilegeCommand& cmd) {
    for (const auto& item : cmd.privileges) {
      switch (item.privilege) {
        case Privilege::kCreateTable:
        case Privilege::kCreateView:
        case Privilege::kDropTable:
        case Privilege::kDropView:
        case Privilege::kAlterTable:
        case Privilege::kAlterView:
          if (cmd.scope == GrantScope::kObject) {
            return SyntaxError(PrivilegeName(item.privilege) +
                               " is granted ON SCHEMA or ON CATALOG, not on a single object");
          }
          break;
        case Privilege::kCreateSchema:
        case Privilege::kDropSchema:
          if (cmd.scope != GrantScope::kCatalog) {
            return SyntaxError(PrivilegeName(item.privilege) + " is granted ON CATALOG");
          }
          break;
        default:
          break;
      }
    }
    return arrow::Status::OK();
  }

  std::vector<Token> tokens_;
  std::string sql_;
  size_t pos_ = 0;
};

// The first one or two keywords of `sql`, upper-cased, after comments.
std::vector<std::string> LeadingWords(const std::string& sql, size_t count) {
  std::vector<std::string> words;
  size_t pos = 0;
  while (words.size() < count) {
    if (!SkipSpaceAndComments(sql, pos) || pos >= sql.size()) break;
    if (!IsWordStart(static_cast<unsigned char>(sql[pos]))) break;
    const size_t start = pos;
    while (pos < sql.size() && IsWordChar(static_cast<unsigned char>(sql[pos]))) ++pos;
    std::string word = sql.substr(start, pos - start);
    for (auto& c : word) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    words.push_back(std::move(word));
  }
  return words;
}

}  // namespace

std::string QualifiedName::ToString() const {
  std::string out;
  for (size_t i = 0; i < parts.size(); ++i) {
    if (i) out += ".";
    out += QuoteIdentifierIfNeeded(parts[i]);
  }
  return out;
}

bool LooksLikeRbacStatement(const std::string& sql) {
  const auto words = LeadingWords(sql, 5);
  if (words.empty()) return false;
  const std::string& first = words[0];
  if (first == "GRANT" || first == "REVOKE") return true;
  if (words.size() < 2) return false;
  const std::string& second = words[1];
  auto is_policy = [&](size_t i) {
    return words.size() > i + 1 && (words[i] == "ROW" || words[i] == "MASKING") &&
           words[i + 1] == "POLICY";
  };
  if (first == "CREATE" && is_policy(1)) return true;
  if (first == "CREATE" && second == "OR" && words.size() > 2 && words[2] == "REPLACE" &&
      is_policy(3)) {
    return true;
  }
  if (first == "DROP" && is_policy(1)) return true;
  if ((first == "CREATE" || first == "DROP") && second == "TENANT") return true;
  if (first == "ALTER" && second == "TENANT") return true;
  if (first == "ALTER" && (second == "CATALOG" || second == "DATABASE")) {
    // ALTER CATALOG c SET | UNSET TENANT: ours; anything else is DuckDB's.
    auto tokens = Tokenize(sql);
    if (!tokens.ok()) return false;
    for (const auto& t : *tokens) {
      if (t.type == TokenType::kWord && KeywordEquals(t.text, "TENANT")) return true;
    }
    return false;
  }
  if (first == "SHOW" && words.size() > 2 && (second == "ROW" || second == "MASKING") &&
      words[2] == "POLICIES") {
    return true;
  }
  if (first == "SHOW" && second == "TENANTS" && words.size() == 2) return true;
  if ((first == "SET" || first == "RESET") && second == "ROLE") return true;
  if ((first == "CREATE" || first == "DROP") && second == "GROUP" && words.size() > 2 &&
      words[2] == "MAPPING") {
    return true;
  }
  if (first == "SHOW" && second == "GROUP" && words.size() > 2 && words[2] == "MAPPINGS") return true;
  if ((first == "CREATE" || first == "DROP") && second == "SCOPED" && words.size() > 2 &&
      words[2] == "TOKEN") {
    return true;
  }
  if (first == "SHOW" && second == "SCOPED" && words.size() > 2 && words[2] == "TOKENS") return true;
  if (first == "CREATE" || first == "DROP") return second == "USER" || second == "ROLE";
  if (first == "ALTER") {
    if (second == "USER") return true;
    if (second == "TABLE" || second == "VIEW") {
      // Only ALTER TABLE|VIEW ... OWNER TO is ours; every other ALTER goes to
      // DuckDB. Tokenize to look for OWNER TO after the object name.
      auto tokens = Tokenize(sql);
      if (!tokens.ok()) return false;
      const auto& t = *tokens;
      size_t i = 2;
      // Skip the (possibly dotted) name.
      if (i < t.size() && (t[i].type == TokenType::kWord ||
                           t[i].type == TokenType::kQuotedIdentifier)) {
        ++i;
        while (i + 1 < t.size() && t[i].type == TokenType::kSymbol && t[i].text == "." &&
               (t[i + 1].type == TokenType::kWord ||
                t[i + 1].type == TokenType::kQuotedIdentifier)) {
          i += 2;
        }
      }
      if (i + 1 < t.size() && t[i].type == TokenType::kWord && KeywordEquals(t[i].text, "OWNER") &&
          t[i + 1].type == TokenType::kWord && KeywordEquals(t[i + 1].text, "TO")) {
        return true;
      }
      // ALTER TABLE t MODIFY [COLUMN] c SET | UNSET MASKING POLICY
      if (second == "TABLE" && i < t.size() && t[i].type == TokenType::kWord &&
          KeywordEquals(t[i].text, "MODIFY")) {
        for (size_t j = i + 1; j + 1 < t.size(); ++j) {
          if (t[j].type == TokenType::kWord && KeywordEquals(t[j].text, "MASKING") &&
              t[j + 1].type == TokenType::kWord && KeywordEquals(t[j + 1].text, "POLICY")) {
            return true;
          }
        }
      }
      return false;
    }
    return false;
  }
  if (first == "SHOW") return second == "GRANTS";
  return false;
}

arrow::Result<RbacCommand> ParseRbacStatement(const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto tokens, Tokenize(sql));
  Parser parser(std::move(tokens), sql);
  return parser.ParseStatement();
}

std::string CommandKind(const RbacCommand& command) {
  struct Visitor {
    std::string operator()(const CreateUserCommand&) const { return "CREATE USER"; }
    std::string operator()(const AlterUserCommand&) const { return "ALTER USER"; }
    std::string operator()(const DropUserCommand&) const { return "DROP USER"; }
    std::string operator()(const CreateRoleCommand&) const { return "CREATE ROLE"; }
    std::string operator()(const DropRoleCommand&) const { return "DROP ROLE"; }
    std::string operator()(const RoleMembershipCommand& c) const {
      return c.grant ? "GRANT" : "REVOKE";
    }
    std::string operator()(const ObjectPrivilegeCommand& c) const {
      return c.grant ? "GRANT" : "REVOKE";
    }
    std::string operator()(const SystemPrivilegeCommand& c) const {
      return c.grant ? "GRANT" : "REVOKE";
    }
    std::string operator()(const AlterOwnerCommand& c) const {
      return c.is_view ? "ALTER VIEW OWNER" : "ALTER TABLE OWNER";
    }
    std::string operator()(const ShowGrantsCommand&) const { return "SHOW GRANTS"; }
    std::string operator()(const CreateRowPolicyCommand&) const { return "CREATE ROW POLICY"; }
    std::string operator()(const DropRowPolicyCommand&) const { return "DROP ROW POLICY"; }
    std::string operator()(const CreateMaskingPolicyCommand&) const {
      return "CREATE MASKING POLICY";
    }
    std::string operator()(const DropMaskingPolicyCommand&) const { return "DROP MASKING POLICY"; }
    std::string operator()(const SetColumnMaskCommand& c) const {
      return c.policy ? "SET MASKING POLICY" : "UNSET MASKING POLICY";
    }
    std::string operator()(const TenantCommand& c) const {
      return c.create ? "CREATE TENANT" : "DROP TENANT";
    }
    std::string operator()(const CatalogTenantCommand&) const { return "ALTER CATALOG"; }
    std::string operator()(const TenantDefaultCatalogCommand&) const { return "ALTER TENANT"; }
    std::string operator()(const ShowTenantsCommand&) const { return "SHOW TENANTS"; }
    std::string operator()(const SetRoleCommand&) const { return "SET ROLE"; }
    std::string operator()(const ScopedTokenCommand& c) const {
      return c.kind == ScopedTokenCommand::Kind::kCreate ? "CREATE SCOPED TOKEN"
             : c.kind == ScopedTokenCommand::Kind::kDrop ? "DROP SCOPED TOKEN"
                                                         : "SHOW SCOPED TOKENS";
    }
    std::string operator()(const GroupMappingCommand& c) const {
      return c.kind == GroupMappingCommand::Kind::kCreate ? "CREATE GROUP MAPPING"
             : c.kind == GroupMappingCommand::Kind::kDrop ? "DROP GROUP MAPPING"
                                                          : "SHOW GROUP MAPPINGS";
    }
    std::string operator()(const ShowPoliciesCommand& c) const {
      return c.masking ? "SHOW MASKING POLICIES" : "SHOW ROW POLICIES";
    }
  };
  return std::visit(Visitor{}, command);
}

}  // namespace gizmosql::enterprise::rbac
