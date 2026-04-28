import Foundation

/// App-wide constants. Keep in sync with the C++ side
/// (`src/common/include/system_catalog.h::kSystemCatalogName`).
enum GizmoSQLConstants {
    /// The fixed catalog name of GizmoSQL's per-process in-memory metadata
    /// catalog. Hosts server-managed helper views (`gizmosql_index_info`,
    /// `gizmosql_view_definition`); never user data. The Browser screen and
    /// `.tables` / `.catalogs` dot-commands hide it from the UI for the same
    /// reason `information_schema` is hidden.
    static let systemCatalogName = "_gizmosql_system"
}
