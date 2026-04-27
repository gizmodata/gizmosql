// GizmoSQL system catalog: a per-process in-memory DuckDB catalog that hosts
// metadata helper views (e.g. JDBC-shaped index info, view definitions). The
// catalog is read-only for clients — writes are denied at the SQL execution
// layer regardless of role or licensing.
//
// Clients query these views with plain SQL — no Flight SQL protocol
// extensions needed, which keeps us on the stock happy path where DoAction /
// GetFlightInfo are marked `final` in the Arrow base class.

#pragma once

#include <string>

namespace gizmosql {

// The fixed catalog name. Hardcoded (not user-configurable) so write-deny
// enforcement and any future client-side lookups can rely on a stable name.
inline constexpr const char* kSystemCatalogName = "_gizmosql_system";

// Returns the SQL block that ATTACHes the system catalog and creates the
// metadata helper views inside it. Intended to be appended to the server's
// init_sql_commands during startup.
//
// The returned SQL is a single string of semicolon-terminated statements
// suitable for direct execution via DuckDB::Query/Connection::Query.
std::string GetSystemCatalogInitSql();

// True if the given catalog name refers to the GizmoSQL system catalog.
inline bool IsSystemCatalog(const std::string& catalog_name) {
  return catalog_name == kSystemCatalogName;
}

}  // namespace gizmosql
