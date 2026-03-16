// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "catalog_permissions_handler.h"

#include <algorithm>
#include <cctype>

#include <boost/algorithm/string.hpp>

#include "gizmosql_logging.h"
#include "session_context.h"
#include "enterprise/enterprise_features.h"
#include "instrumentation/instrumentation_manager.h"
#include "instrumentation/instrumentation_records.h"

namespace gizmosql::enterprise {

CatalogAccessLevel GetCatalogAccess(
    const std::string& catalog_name,
    const std::string& role,
    const std::vector<CatalogAccessRule>& catalog_access,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  // The instrumentation catalog is special: system-managed, read-only for admins.
  // This protection ALWAYS applies, regardless of licensing or token rules.
  // The catalog name is configurable (e.g., DuckLake catalogs), so we check dynamically.
  if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
    return (role == "admin") ? CatalogAccessLevel::kRead : CatalogAccessLevel::kNone;
  }

  // Runtime check: if catalog permissions feature is not licensed, grant full access
  // to all other catalogs (backward compatible with Core edition behavior)
  if (!EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    return CatalogAccessLevel::kWrite;
  }

  // If no catalog_access rules defined, grant full access (backward compatible)
  if (catalog_access.empty()) {
    return CatalogAccessLevel::kWrite;
  }

  // Check rules in order - first match wins
  for (const auto& rule : catalog_access) {
    if (rule.catalog == catalog_name || rule.catalog == "*") {
      return rule.access;
    }
  }

  // No matching rule - deny access
  return CatalogAccessLevel::kNone;
}

bool HasReadAccess(const ClientSession& client_session, const std::string& catalog_name,
                   const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access,
                                 instrumentation_manager);
  return access >= CatalogAccessLevel::kRead;
}

bool HasWriteAccess(const ClientSession& client_session, const std::string& catalog_name,
                    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access,
                                 instrumentation_manager);
  return access >= CatalogAccessLevel::kWrite;
}

arrow::Status CheckCatalogWriteAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::ModificationInfo>& modified_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  for (const auto& [catalog_name, catalog_identity] : modified_databases) {
    // Block writes to the instrumentation catalog (regardless of other rules)
    // This protects both file-based (_gizmosql_instr) and external catalogs (e.g., DuckLake)
    if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: instrumentation catalog is read-only",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: The instrumentation catalog '" + catalog_name + "' is read-only.";

      // Record the rejected modification attempt
      gizmosql::ddb::StatementInstrumentation(
          instrumentation_manager, statement_id, client_session->session_id,
          logged_sql, flight_method, is_internal, error_msg);

      return arrow::Status::Invalid(error_msg);
    }

    if (!HasWriteAccess(*client_session, catalog_name, instrumentation_manager)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: user lacks write access to catalog",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: You do not have write access to catalog '" + catalog_name + "'.";

      // Record the rejected modification attempt
      if (instrumentation_manager) {
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);
      }

      return arrow::Status::Invalid(error_msg);
    }
  }

  return arrow::Status::OK();
}

arrow::Status CheckCatalogReadAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& read_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  for (const auto& [catalog_name, catalog_identity] : read_databases) {
    // For the instrumentation catalog, only admins can read
    // This protects both file-based (_gizmosql_instr) and external catalogs (e.g., DuckLake)
    if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
      if (client_session->role != "admin") {
        GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                               "Access denied: only admins can read instrumentation catalog",
                               {"kind", "sql"}, {"status", "rejected"},
                               {"catalog", catalog_name}, {"statement_id", statement_id},
                               {"sql", logged_sql});

        std::string error_msg =
            "Access denied: Only administrators can read the instrumentation catalog '" + catalog_name + "'.";

        // Record the rejected read attempt
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);

        return arrow::Status::Invalid(error_msg);
      }
      // Admin can read instrumentation catalog, skip other checks for this catalog
      continue;
    }

    if (!HasReadAccess(*client_session, catalog_name, instrumentation_manager)) {
      GIZMOSQL_LOGKV_SESSION(WARNING, client_session,
                             "Access denied: user lacks read access to catalog",
                             {"kind", "sql"}, {"status", "rejected"},
                             {"catalog", catalog_name}, {"statement_id", statement_id},
                             {"sql", logged_sql});

      std::string error_msg =
          "Access denied: You do not have read access to catalog '" + catalog_name + "'.";

      // Record the rejected read attempt
      if (instrumentation_manager) {
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id,
            logged_sql, flight_method, is_internal, error_msg);
      }

      return arrow::Status::Invalid(error_msg);
    }
  }

  return arrow::Status::OK();
}

arrow::Status EnsureCatalogReadAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::string& catalog_name,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {
  if (instrumentation_manager && catalog_name == instrumentation_manager->GetCatalog()) {
    if (client_session->role != "admin") {
      std::string error_msg =
          "Access denied: Only administrators can read the instrumentation catalog '" +
          catalog_name + "'.";
      if (instrumentation_manager) {
        gizmosql::ddb::StatementInstrumentation(
            instrumentation_manager, statement_id, client_session->session_id, logged_sql,
            flight_method, is_internal, error_msg);
      }
      return arrow::Status::Invalid(error_msg);
    }
    return arrow::Status::OK();
  }

  if (!HasReadAccess(*client_session, catalog_name, instrumentation_manager)) {
    std::string error_msg =
        "Access denied: You do not have read access to catalog '" + catalog_name + "'.";
    if (instrumentation_manager) {
      gizmosql::ddb::StatementInstrumentation(
          instrumentation_manager, statement_id, client_session->session_id, logged_sql,
          flight_method, is_internal, error_msg);
    }
    return arrow::Status::Invalid(error_msg);
  }

  return arrow::Status::OK();
}

// ============================================================================
// Catalog Visibility Filtering
// ============================================================================

std::vector<std::string> GetAllowedCatalogs(
    const ClientSession& client_session,
    duckdb::Connection& connection,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager) {
  // No filtering if feature not licensed or no rules defined
  if (!EnterpriseFeatures::Instance().IsCatalogPermissionsAvailable()) {
    return {};
  }
  if (client_session.catalog_access.empty()) {
    return {};
  }

  // Query actual catalogs from DuckDB
  auto result = connection.Query("SELECT database_name FROM duckdb_databases()");
  if (result->HasError()) {
    return {};
  }

  std::vector<std::string> allowed;
  // Always include DuckDB internal catalogs
  allowed.push_back("system");
  allowed.push_back("temp");

  for (auto& row : *result) {
    std::string db_name = row.GetValue<std::string>(0);
    // Skip system/temp since we already included them
    if (db_name == "system" || db_name == "temp") {
      continue;
    }
    auto access = GetCatalogAccess(db_name, client_session.role,
                                   client_session.catalog_access,
                                   instrumentation_manager);
    if (access >= CatalogAccessLevel::kRead) {
      allowed.push_back(std::move(db_name));
    }
  }

  return allowed;
}

std::string BuildCatalogFilterIN(const std::vector<std::string>& allowed_catalogs) {
  std::string result = "IN (";
  for (size_t i = 0; i < allowed_catalogs.size(); ++i) {
    if (i > 0) result += ',';
    result += '\'';
    // Escape single quotes by doubling
    for (char c : allowed_catalogs[i]) {
      if (c == '\'') result += "''";
      else result += c;
    }
    result += '\'';
  }
  result += ')';
  return result;
}

bool RewriteShowCommand(const std::string& sql, const std::string& filter_in,
                        std::string& rewritten) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Strip trailing semicolon
  if (!trimmed.empty() && trimmed.back() == ';') {
    trimmed.pop_back();
    boost::algorithm::trim_right(trimmed);
  }

  std::string upper = boost::to_upper_copy(trimmed);

  if (upper == "SHOW DATABASES" || upper == "SHOW ALL DATABASES") {
    rewritten = "SELECT database_name FROM duckdb_databases() WHERE database_name " +
                filter_in + " ORDER BY database_name";
    return true;
  }

  if (upper == "SHOW ALL TABLES") {
    rewritten = "SELECT * FROM (SHOW ALL TABLES) WHERE database " + filter_in;
    return true;
  }

  // SHOW SCHEMAS shows schemas across all catalogs (DuckDB v1.5.0+) — needs filtering
  if (upper == "SHOW SCHEMAS") {
    rewritten = "SELECT * FROM (SHOW SCHEMAS) WHERE database_name " + filter_in;
    return true;
  }

  // SHOW TABLES is scoped to current DB — no rewrite needed
  return false;
}

namespace {

// Metadata replacement mapping entry
struct MetadataPattern {
  std::string pattern_lower;  // lowercase pattern to match
  std::string filter_column;  // column name for WHERE clause
  bool is_function;  // true for duckdb_xxx() function calls, false for views/tables
};

// Build the static mapping table
const std::vector<MetadataPattern>& GetMetadataPatterns() {
  static const std::vector<MetadataPattern> patterns = {
      // information_schema views
      {"information_schema.schemata", "catalog_name", false},
      {"information_schema.tables", "table_catalog", false},
      {"information_schema.columns", "table_catalog", false},
      {"information_schema.character_sets", "default_collate_catalog", false},
      {"information_schema.constraint_column_usage", "table_catalog", false},
      {"information_schema.key_column_usage", "table_catalog", false},
      {"information_schema.referential_constraints", "constraint_catalog", false},
      {"information_schema.table_constraints", "table_catalog", false},
      // duckdb_*() function calls
      {"duckdb_databases()", "database_name", true},
      {"duckdb_tables()", "database_name", true},
      {"duckdb_views()", "database_name", true},
      {"duckdb_columns()", "database_name", true},
      {"duckdb_constraints()", "database_name", true},
      {"duckdb_functions()", "database_name", true},
      {"duckdb_indexes()", "database_name", true},
      {"duckdb_schemas()", "database_name", true},
      {"duckdb_sequences()", "database_name", true},
      {"duckdb_types()", "database_name", true},
  };
  return patterns;
}

// Check if character is a valid identifier character
inline bool IsIdentChar(char c) {
  return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

size_t ParseIdentifierTokenEnd(const std::string& sql, size_t start) {
  if (start >= sql.size()) {
    return start;
  }

  if (sql[start] == '"') {
    size_t pos = start + 1;
    while (pos < sql.size()) {
      if (sql[pos] == '"') {
        if (pos + 1 < sql.size() && sql[pos + 1] == '"') {
          pos += 2;
          continue;
        }
        return pos + 1;
      }
      ++pos;
    }
    return start;
  }

  if (!IsIdentChar(sql[start])) {
    return start;
  }

  size_t pos = start;
  while (pos < sql.size() && IsIdentChar(sql[pos])) {
    ++pos;
  }
  return pos;
}

std::string NormalizeQuotedMetadataReferences(const std::string& sql) {
  const auto& patterns = GetMetadataPatterns();
  std::string result;
  result.reserve(sql.size());

  size_t i = 0;
  while (i < sql.size()) {
    if (sql[i] == '\'') {
      result += sql[i++];
      while (i < sql.size()) {
        result += sql[i];
        if (sql[i] == '\'') {
          ++i;
          if (i < sql.size() && sql[i] == '\'') {
            result += sql[i++];
            continue;
          }
          break;
        }
        ++i;
      }
      continue;
    }

    bool replaced = false;
    for (const auto& pat : patterns) {
      std::string quoted_pattern = pat.pattern_lower;
      if (pat.is_function) {
        auto open_paren = quoted_pattern.find("()");
        std::string function_name = quoted_pattern.substr(0, open_paren);
        quoted_pattern = "\"" + function_name + "\"()";
      } else {
        boost::replace_all(quoted_pattern, ".", "\".\"");
        quoted_pattern = "\"" + quoted_pattern + "\"";
      }

      if (i + quoted_pattern.size() > sql.size()) {
        continue;
      }

      std::string candidate = sql.substr(i, quoted_pattern.size());
      if (!boost::iequals(candidate, quoted_pattern)) {
        size_t prefix_end = ParseIdentifierTokenEnd(sql, i);
        if (prefix_end == i || prefix_end >= sql.size() || sql[prefix_end] != '.') {
          continue;
        }
        size_t suffix_start = prefix_end + 1;
        if (suffix_start + quoted_pattern.size() > sql.size()) {
          continue;
        }
        candidate = sql.substr(suffix_start, quoted_pattern.size());
        if (!boost::iequals(candidate, quoted_pattern)) {
          continue;
        }
        result += sql.substr(i, prefix_end - i + 1);
        result += pat.pattern_lower;
        i = suffix_start + quoted_pattern.size();
        replaced = true;
        break;
      }

      result += pat.pattern_lower;
      i += quoted_pattern.size();
      replaced = true;
      break;
    }

    if (!replaced) {
      result += sql[i++];
    }
  }

  return result;
}

}  // namespace

std::string FilterMetadataReferences(const std::string& sql, const std::string& filter_in) {
  std::string normalized_sql = NormalizeQuotedMetadataReferences(sql);
  const auto& patterns = GetMetadataPatterns();
  std::string result;
  result.reserve(normalized_sql.size() * 2);

  size_t i = 0;
  while (i < normalized_sql.size()) {
    // Skip single-quoted strings
    if (normalized_sql[i] == '\'') {
      result += normalized_sql[i++];
      while (i < normalized_sql.size()) {
        if (normalized_sql[i] == '\\' && i + 1 < normalized_sql.size()) {
          result += normalized_sql[i++];
          result += normalized_sql[i++];
          continue;
        }
        if (normalized_sql[i] == '\'') {
          result += normalized_sql[i++];
          // Check for escaped quote ('')
          if (i < normalized_sql.size() && normalized_sql[i] == '\'') {
            result += normalized_sql[i++];
            continue;
          }
          break;
        }
        result += normalized_sql[i++];
      }
      continue;
    }

    // Check boundary: preceding character must not be identifier char
    if (i > 0 && IsIdentChar(normalized_sql[i - 1])) {
      result += normalized_sql[i++];
      continue;
    }

    // Try to match each pattern at current position
    bool matched = false;
    for (const auto& pat : patterns) {
      size_t pat_len = pat.pattern_lower.size();
      if (i + pat_len > normalized_sql.size()) continue;

      // Case-insensitive comparison
      std::string candidate = normalized_sql.substr(i, pat_len);
      std::string candidate_lower = boost::to_lower_copy(candidate);
      if (candidate_lower != pat.pattern_lower) continue;

      // Check trailing boundary
      size_t end_pos = i + pat_len;
      if (pat.is_function) {
        // For function calls like duckdb_tables(), the pattern includes "()"
        // After ")" we accept: whitespace, comma, ), ;, EOF, or SQL keywords
        if (end_pos < normalized_sql.size() && IsIdentChar(normalized_sql[end_pos])) {
          continue;  // Not a boundary — skip
        }
      } else {
        // For views like information_schema.tables
        if (end_pos < normalized_sql.size() && normalized_sql[end_pos] == '.') {
          continue;  // e.g., information_schema.tables.column_name — don't match partial
        }
      }

      // Check for catalog-qualified prefix like "system.information_schema.tables"
      // If preceded by '.', scan backwards to capture the catalog name and remove it
      // from result (it will be included in the subquery's FROM clause instead)
      std::string catalog_prefix;
      if (i > 0 && normalized_sql[i - 1] == '.') {
        // Walk backwards past the '.' to find the catalog identifier
        size_t dot_pos = result.size() - 1;  // position of '.' in result
        size_t ident_start = dot_pos;
        if (dot_pos > 0 && result[dot_pos - 1] == '"') {
          ident_start = dot_pos - 1;
          while (ident_start > 0) {
            --ident_start;
            if (result[ident_start] == '"') {
              break;
            }
          }
          if (result[ident_start] != '"') {
            ident_start = dot_pos;
          }
        } else {
          while (ident_start > 0 && IsIdentChar(result[ident_start - 1])) {
            --ident_start;
          }
        }
        if (ident_start < dot_pos) {
          // Extract "catalog." from result (including the dot)
          catalog_prefix = result.substr(ident_start, dot_pos - ident_start + 1);
          // Remove it from result — it will be part of the subquery
          result.erase(ident_start);
        }
      }

      // Build replacement: (SELECT * FROM [catalog_prefix]pattern WHERE col <filter_in>)
      result += "(SELECT * FROM ";
      result += catalog_prefix;  // e.g. "system." or empty
      result += normalized_sql.substr(i, pat_len);
      result += " WHERE ";
      result += pat.filter_column;
      result += ' ';
      result += filter_in;
      result += ')';

      i = end_pos;
      matched = true;
      break;
    }

    if (!matched) {
      result += normalized_sql[i++];
    }
  }

  return result;
}

}  // namespace gizmosql::enterprise
