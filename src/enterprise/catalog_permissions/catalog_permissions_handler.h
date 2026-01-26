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
/// Special handling for _gizmosql_instr: only admins can read, no one can write.
///
/// @param catalog_name The catalog to check access for
/// @param role The user's role (e.g., "admin", "readonly")
/// @param catalog_access The catalog access rules from the JWT token
/// @return The access level granted for the catalog
CatalogAccessLevel GetCatalogAccess(
    const std::string& catalog_name,
    const std::string& role,
    const std::vector<CatalogAccessRule>& catalog_access);

/// Check if the session has read access to a catalog.
/// @param client_session The session to check
/// @param catalog_name The catalog to check access for
/// @return true if read access is granted
bool HasReadAccess(const ClientSession& client_session, const std::string& catalog_name);

/// Check if the session has write access to a catalog.
/// @param client_session The session to check
/// @param catalog_name The catalog to check access for
/// @return true if write access is granted
bool HasWriteAccess(const ClientSession& client_session, const std::string& catalog_name);

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
    const std::unordered_map<std::string, duckdb::StatementProperties::CatalogIdentity>& modified_databases,
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

}  // namespace gizmosql::enterprise
