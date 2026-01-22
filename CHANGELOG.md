# Changelog

All notable changes to GizmoSQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.15.0] - 2025-01-22

### Highlights

This release introduces **GizmoSQL Enterprise Edition** with commercial features for production deployments, alongside significant updates to core dependencies.

### Added

#### GizmoSQL Enterprise Edition

GizmoSQL is now available in two editions:

| Feature | Core | Enterprise |
|---------|:----:|:----------:|
| DuckDB & SQLite backends | Yes | Yes |
| Arrow Flight SQL protocol | Yes | Yes |
| TLS & mTLS authentication | Yes | Yes |
| JWT token authentication | Yes | Yes |
| Query timeout | Yes | Yes |
| **Session Instrumentation** | No | Yes |
| **KILL SESSION** | No | Yes |

#### Session Instrumentation (Enterprise)
- Track server instances, client sessions, SQL statements, and query executions
- Records stored in a DuckDB database for analysis and auditing
- Views: `active_sessions`, `session_activity`, `session_stats`
- Enable via `--enable-instrumentation` or `GIZMOSQL_ENABLE_INSTRUMENTATION=1`

#### KILL SESSION Command (Enterprise)
- Terminate active client sessions via SQL: `KILL SESSION '<session-id>'`
- Requires admin role
- Useful for terminating runaway queries or rogue connections

#### New SQL Functions
- `GIZMOSQL_CURRENT_SESSION()` - Returns the current session UUID
- `GIZMOSQL_CURRENT_INSTANCE()` - Returns the current server instance UUID
- `GIZMOSQL_VERSION()` - Returns the GizmoSQL version string
- `GIZMOSQL_USER()` - Returns the current username
- `GIZMOSQL_ROLE()` - Returns the current user's role
- `GIZMOSQL_EDITION()` - Returns `'Core'` or `'Enterprise'` based on license status

#### Catalog-Level Permissions (Token Auth)
- Fine-grained access control per catalog/database via JWT `catalog_access` claim
- Supports `read`, `write`, or `none` access levels per catalog
- Example JWT claim:
  ```json
  "catalog_access": [
    {"catalog": "main", "access": "write"},
    {"catalog": "analytics", "access": "read"}
  ]
  ```

#### License Management
- JWT-based license keys signed with RS256
- Configure via `--license-key-file` or `GIZMOSQL_LICENSE_KEY_FILE` environment variable

#### Other Additions
- `GIZMOSQL_PORT` environment variable support
- Configurable health check query (`--health-check-query`)

### Changed

#### Component Updates

| Component | Version |
|-----------|---------|
| Apache Arrow | **23.0.0** |
| DuckDB | v1.4.3 |
| SQLite | 3.51.1 |
| jwt-cpp | v0.7.1 |

#### Improvements
- Graceful shutdown now properly closes instrumentation records
- Instance ID included in JWT tokens and log entries for correlation

### Fixed

- Connection mutex issue that prevented multiple cursors per connection (#105)

### Documentation

- New documentation site: [docs.gizmosql.com](https://docs.gizmosql.com)
- Added [Editions Guide](https://docs.gizmosql.com/#/editions) with feature comparison
- Updated README with editions section and license information

### Ecosystem

- Added [SQLMesh Adapter](https://github.com/gizmodata/sqlmesh-gizmosql)
- Added [PySpark SQLFrame Adapter](https://github.com/gizmodata/sqlframe-gizmosql)

### Enterprise Licensing

Contact **sales@gizmodata.com** to obtain an enterprise license.
