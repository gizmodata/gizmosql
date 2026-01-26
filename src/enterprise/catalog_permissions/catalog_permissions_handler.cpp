// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "catalog_permissions_handler.h"

#include "gizmosql_logging.h"
#include "session_context.h"
#include "enterprise/enterprise_features.h"
#include "instrumentation/instrumentation_manager.h"
#include "instrumentation/instrumentation_records.h"

namespace gizmosql::enterprise {

CatalogAccessLevel GetCatalogAccess(
    const std::string& catalog_name,
    const std::string& role,
    const std::vector<CatalogAccessRule>& catalog_access) {
  // The instrumentation database is special: system-managed, read-only for admins
  // This protection ALWAYS applies, regardless of licensing or token rules
  if (catalog_name == "_gizmosql_instr") {
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

bool HasReadAccess(const ClientSession& client_session, const std::string& catalog_name) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access);
  return access >= CatalogAccessLevel::kRead;
}

bool HasWriteAccess(const ClientSession& client_session, const std::string& catalog_name) {
  auto access = GetCatalogAccess(catalog_name, client_session.role, client_session.catalog_access);
  return access >= CatalogAccessLevel::kWrite;
}

arrow::Status CheckCatalogWriteAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& modified_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  for (const auto& [catalog_name, catalog_identity] : modified_databases) {
    if (!HasWriteAccess(*client_session, catalog_name)) {
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
    if (!HasReadAccess(*client_session, catalog_name)) {
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

}  // namespace gizmosql::enterprise
