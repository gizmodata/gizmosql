// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

#include <memory>
#include <string>

#include <arrow/result.h>
#include <arrow/status.h>

#include "session_context.h"

namespace gizmosql::ddb {
class DuckDBFlightSqlServer;
class InstrumentationManager;
}

namespace gizmosql::enterprise {

/// Check if a SQL command is a KILL SESSION command
/// @param sql The SQL command to check
/// @param target_session_id Output parameter for the target session ID if matched
/// @return true if this is a KILL SESSION command
bool IsKillSessionCommand(const std::string& sql, std::string& target_session_id);

/// Handle a KILL SESSION command
/// @param client_session The session making the request (must be admin)
/// @param target_session_id The session ID to kill
/// @param server The DuckDB server instance
/// @param instrumentation_manager Optional instrumentation manager for recording
/// @param statement_id The statement ID for instrumentation
/// @param logged_sql The SQL for logging/instrumentation
/// @param flight_method The Flight method name
/// @param is_internal Whether this is an internal request
/// @return Arrow Status indicating success or failure
arrow::Status HandleKillSession(
    const std::shared_ptr<ClientSession>& client_session,
    const std::string& target_session_id,
    gizmosql::ddb::DuckDBFlightSqlServer* server,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal);

}  // namespace gizmosql::enterprise
