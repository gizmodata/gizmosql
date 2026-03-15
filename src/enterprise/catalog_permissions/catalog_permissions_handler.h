// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/status.h>
#include <duckdb.hpp>

#include "request_ctx.h"  // For CatalogAccessRule, CatalogAccessLevel

namespace gizmosql {
struct ClientSession;  // forward declare to avoid circular include
}

namespace gizmosql::ddb {
class InstrumentationManager;
}

namespace gizmosql::enterprise {

/// Get the catalog access level for a given catalog name.
/// This evaluates the catalog_access rules from the session's JWT token.
/// Special handling for the instrumentation catalog: only admins can read, no one can write.
///
/// @param catalog_name The catalog to check access for
/// @param role The user's role (e.g., "admin", "readonly")
/// @param catalog_access The catalog access rules from the JWT token
/// @param instrumentation_manager Optional instrumentation manager to resolve the catalog name
/// @return The access level granted for the catalog
CatalogAccessLevel GetCatalogAccess(
    const std::string& catalog_name,
    const std::string& role,
    const std::vector<CatalogAccessRule>& catalog_access,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager = nullptr);

/// Check if the session has read access to a catalog.
/// @param client_session The session to check
/// @param catalog_name The catalog to check access for
/// @param instrumentation_manager Optional instrumentation manager to resolve the catalog name
/// @return true if read access is granted
bool HasReadAccess(const ClientSession& client_session, const std::string& catalog_name,
                   const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager = nullptr);

/// Check if the session has write access to a catalog.
/// @param client_session The session to check
/// @param catalog_name The catalog to check access for
/// @param instrumentation_manager Optional instrumentation manager to resolve the catalog name
/// @return true if write access is granted
bool HasWriteAccess(const ClientSession& client_session, const std::string& catalog_name,
                    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager = nullptr);

/// Check catalog-level write access for all databases a statement will modify.
/// This is an enterprise feature that requires a valid license with catalog_permissions.
///
/// @param client_session The session making the request
/// @param modified_databases Map of catalog names to CatalogIdentity from prepared statement
/// @param instrumentation_manager Optional instrumentation manager for recording rejections
/// @param statement_id The statement ID for instrumentation
/// @param logged_sql The redacted SQL for logging/instrumentation
/// @param flight_method The Flight method name
/// @param is_internal Whether this is an internal request
/// @return Arrow Status - OK if access granted, Invalid if denied
arrow::Status CheckCatalogWriteAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::ModificationInfo>& modified_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal);

/// Check catalog-level read access for all databases a statement will read.
/// This is an enterprise feature that requires a valid license with catalog_permissions.
///
/// @param client_session The session making the request
/// @param read_databases Map of catalog names to CatalogIdentity from prepared statement
/// @param instrumentation_manager Optional instrumentation manager for recording rejections
/// @param statement_id The statement ID for instrumentation
/// @param logged_sql The redacted SQL for logging/instrumentation
/// @param flight_method The Flight method name
/// @param is_internal Whether this is an internal request
/// @return Arrow Status - OK if access granted, Invalid if denied
arrow::Status CheckCatalogReadAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& read_databases,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal);

/// Check read access for a single catalog name.
/// This is used for commands like USE/SetSessionOptions where DuckDB's statement
/// metadata does not reliably capture the target catalog before execution.
arrow::Status EnsureCatalogReadAccess(
    const std::shared_ptr<ClientSession>& client_session,
    const std::string& catalog_name,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id = "",
    const std::string& logged_sql = "",
    const std::string& flight_method = "",
    bool is_internal = false);

// ============================================================================
// Catalog Visibility Filtering (metadata row filtering)
// ============================================================================

/// Get the list of catalogs the session is allowed to see.
/// Returns empty if no filtering should be applied (no rules or not licensed).
/// Always includes "system" and "temp" (DuckDB internals).
///
/// @param client_session The session to check
/// @param connection The DuckDB connection to query for actual catalogs
/// @param instrumentation_manager Optional instrumentation manager
/// @return Vector of allowed catalog names, or empty if no filtering needed
std::vector<std::string> GetAllowedCatalogs(
    const ClientSession& client_session,
    duckdb::Connection& connection,
    const std::shared_ptr<gizmosql::ddb::InstrumentationManager>& instrumentation_manager = nullptr);

/// Build an SQL IN clause for allowed catalogs.
/// Returns e.g. IN ('production','staging','system','temp')
/// Single quotes in catalog names are escaped by doubling.
///
/// @param allowed_catalogs The list of allowed catalog names
/// @return The IN clause string
std::string BuildCatalogFilterIN(const std::vector<std::string>& allowed_catalogs);

/// Rewrite SHOW DATABASES / SHOW ALL TABLES commands to filter by allowed catalogs.
/// Returns true if the SQL was rewritten (output in `rewritten`), false otherwise.
///
/// @param sql The original SQL text
/// @param filter_in The IN clause from BuildCatalogFilterIN
/// @param rewritten Output: the rewritten SQL (only valid if returns true)
/// @return true if sql was a SHOW command that was rewritten
bool RewriteShowCommand(const std::string& sql, const std::string& filter_in,
                        std::string& rewritten);

/// Filter metadata references in SQL by replacing information_schema views
/// and duckdb_*() table functions with subqueries that filter by allowed catalogs.
/// Skips replacements inside quoted strings.
///
/// @param sql The original SQL text
/// @param filter_in The IN clause from BuildCatalogFilterIN
/// @return The filtered SQL (may be unchanged if no metadata references found)
std::string FilterMetadataReferences(const std::string& sql, const std::string& filter_in);

}  // namespace gizmosql::enterprise
