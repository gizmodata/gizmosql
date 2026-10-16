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

#include <cctype>

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
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

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
      if (AcceptKeyword("USER")) return ParseCreateUser();
      if (AcceptKeyword("ROLE")) return ParseCreateRole();
      return SyntaxError("expected USER or ROLE after CREATE");
    }
    if (AcceptKeyword("DROP")) {
      const bool is_user = PeekKeyword("USER");
      if (!is_user && !PeekKeyword("ROLE")) return SyntaxError("expected USER or ROLE after DROP");
      ++pos_;
      const bool if_exists = ParseIfExists();
      ARROW_ASSIGN_OR_RAISE(auto name, ParseIdentifier(is_user ? "a user name" : "a role name"));
      if (is_user) return DropUserCommand{std::move(name), if_exists};
      return DropRoleCommand{std::move(name), if_exists};
    }
    if (AcceptKeyword("ALTER")) {
      if (AcceptKeyword("USER")) return ParseAlterUser();
      const bool is_view = PeekKeyword("VIEW");
      if (is_view || PeekKeyword("TABLE")) {
        ++pos_;
        return ParseAlterOwner(is_view);
      }
      return SyntaxError("expected USER, TABLE or VIEW after ALTER");
    }
    if (AcceptKeyword("GRANT")) return ParseGrantOrRevoke(/*grant=*/true);
    if (AcceptKeyword("REVOKE")) return ParseGrantOrRevoke(/*grant=*/false);
    if (AcceptKeyword("SHOW")) {
      ARROW_RETURN_NOT_OK(ExpectKeyword("GRANTS"));
      return ParseShowGrants();
    }
    return SyntaxError("not an access-control statement");
  }

  arrow::Result<RbacCommand> ParseCreateUser() {
    CreateUserCommand cmd;
    cmd.if_not_exists = ParseIfNotExists();
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a user name"));
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
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a role name"));
    return cmd;
  }

  arrow::Result<RbacCommand> ParseAlterUser() {
    AlterUserCommand cmd;
    ARROW_ASSIGN_OR_RAISE(cmd.name, ParseIdentifier("a user name"));
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
    } else {
      return SyntaxError("expected PASSWORD, ENABLE or DISABLE after ALTER USER " + cmd.name);
    }
    return cmd;
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
    ARROW_ASSIGN_OR_RAISE(g.name, ParseIdentifier("a user or role name"));
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
      ARROW_ASSIGN_OR_RAISE(auto role, ParseIdentifier("a role name or privilege"));
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
  const auto words = LeadingWords(sql, 2);
  if (words.empty()) return false;
  const std::string& first = words[0];
  if (first == "GRANT" || first == "REVOKE") return true;
  if (words.size() < 2) return false;
  const std::string& second = words[1];
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
      return i + 1 < t.size() && t[i].type == TokenType::kWord &&
             KeywordEquals(t[i].text, "OWNER") && t[i + 1].type == TokenType::kWord &&
             KeywordEquals(t[i + 1].text, "TO");
    }
    return false;
  }
  if (first == "SHOW") return second == "GRANTS";
  return false;
}

arrow::Result<RbacCommand> ParseRbacStatement(const std::string& sql) {
  ARROW_ASSIGN_OR_RAISE(auto tokens, Tokenize(sql));
  Parser parser(std::move(tokens));
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
    std::string operator()(const AlterOwnerCommand& c) const {
      return c.is_view ? "ALTER VIEW OWNER" : "ALTER TABLE OWNER";
    }
    std::string operator()(const ShowGrantsCommand&) const { return "SHOW GRANTS"; }
  };
  return std::visit(Visitor{}, command);
}

}  // namespace gizmosql::enterprise::rbac
