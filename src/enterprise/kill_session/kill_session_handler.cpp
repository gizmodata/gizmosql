// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#include "kill_session_handler.h"

#include <regex>

#include <boost/algorithm/string.hpp>

#include "duckdb_server.h"
#include "session_context.h"
#include "gizmosql_logging.h"
#include "instrumentation/instrumentation_manager.h"
#include "instrumentation/instrumentation_records.h"
#include "enterprise/enterprise_features.h"

namespace gizmosql::enterprise {

bool IsKillSessionCommand(const std::string& sql, std::string& target_session_id) {
  std::string trimmed = sql;
  boost::algorithm::trim(trimmed);
  if (trimmed.empty()) return false;

  // Match: KILL SESSION 'uuid' or KILL SESSION "uuid" or KILL SESSION uuid
  std::regex kill_pattern(R"(^\s*KILL\s+SESSION\s+['\"]?([0-9a-fA-F-]+)['\"]?\s*;?\s*$)",
                          std::regex_constants::icase);
  std::smatch match;
  if (std::regex_match(trimmed, match, kill_pattern)) {
    target_session_id = match[1].str();
    return true;
  }
  return false;
}

arrow::Status HandleKillSession(
    const std::shared_ptr<ClientSession>& client_session,
    const std::string& target_session_id,
    gizmosql::ddb::DuckDBFlightSqlServer* server,
    std::shared_ptr<gizmosql::ddb::InstrumentationManager> instrumentation_manager,
    const std::string& statement_id,
    const std::string& logged_sql,
    const std::string& flight_method,
    bool is_internal) {

  // Helper to record KILL SESSION attempts (both successful and failed)
  auto record_kill_session = [&](const std::string& error_msg = "") {
    if (instrumentation_manager) {
      gizmosql::ddb::StatementInstrumentation(instrumentation_manager, statement_id,
                                               client_session->session_id, logged_sql,
                                               flight_method, is_internal, error_msg);
    }
  };

  // Check if kill_session feature is licensed
  if (!EnterpriseFeatures::Instance().IsKillSessionAvailable()) {
    std::string error_msg = EnterpriseFeatures::GetLicenseRequiredError("KILL SESSION");
    GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "Attempted to use unlicensed enterprise feature",
                   {"kind", "sql"}, {"status", "rejected"},
                   {"feature", "kill_session"}, {"target_session_id", target_session_id});
    record_kill_session(error_msg);
    return arrow::Status::Invalid(error_msg);
  }

  // Only admin users can kill sessions
  if (client_session->role != "admin") {
    GIZMOSQL_LOGKV_SESSION(WARNING, client_session, "Non-admin user attempted KILL SESSION",
                   {"kind", "sql"}, {"status", "rejected"},
                   {"target_session_id", target_session_id});
    std::string error_msg = "Only admin users can execute KILL SESSION";
    record_kill_session(error_msg);
    return arrow::Status::Invalid(error_msg);
  }

  // Cannot kill own session
  if (target_session_id == client_session->session_id) {
    std::string error_msg = "Cannot kill your own session";
    record_kill_session(error_msg);
    return arrow::Status::Invalid(error_msg);
  }

  // Find and kill the target session
  if (!server) {
    std::string error_msg = "Unable to get server instance for KILL SESSION";
    record_kill_session(error_msg);
    return arrow::Status::Invalid(error_msg);
  }

  auto target = server->FindSession(target_session_id);
  if (!target) {
    std::string error_msg = "Session not found: " + target_session_id;
    record_kill_session(error_msg);
    return arrow::Status::KeyError(error_msg);
  }

  // Mark session for termination and interrupt its connection
  target->kill_requested = true;
  target->connection->Interrupt();

  // Update instrumentation stop reason
  if (target->instrumentation) {
    target->instrumentation->SetStopReason("killed");
  }

  // Remove from session map and mark as killed to prevent reconnection
  ARROW_RETURN_NOT_OK(server->RemoveSession(target_session_id, /*was_killed=*/true));

  GIZMOSQL_LOGKV_SESSION(INFO, client_session, "Session killed successfully",
                 {"kind", "sql"}, {"status", "success"},
                 {"target_session_id", target_session_id});

  // Record successful kill
  record_kill_session();

  return arrow::Status::OK();
}

}  // namespace gizmosql::enterprise
