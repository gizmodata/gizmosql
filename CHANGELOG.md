# Changelog

All notable changes to GizmoSQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- SQL functions for instrumentation metadata discovery: `GIZMOSQL_INSTRUMENTATION_ENABLED()`, `GIZMOSQL_INSTRUMENTATION_CATALOG()`, `GIZMOSQL_INSTRUMENTATION_SCHEMA()` — allows clients to dynamically discover instrumentation availability and catalog/schema configuration via standard SQL queries

### Fixed

- Catalog permissions handler no longer hardcodes `_gizmosql_instr` — now uses the actual configured instrumentation catalog name, supporting custom catalogs (e.g., DuckLake)

### Changed

- Docker entrypoint scripts now default to in-memory database when `DATABASE_FILENAME` is not set

### Added

- `GIZMOSQL_EXTRA_ARGS` env var for passing additional CLI flags to `gizmosql_server` in entrypoint scripts
- `MTLS_CA_CERT_FILENAME` and `HEALTH_PORT` env vars in entrypoint scripts
- `:memory:` as an explicit value for `DATABASE_FILENAME` to request in-memory mode
- Comprehensive env var documentation in entrypoint script headers

#### Server-Side OAuth Code Exchange (Enterprise)
- New server-side OAuth authorization code exchange flow — the GizmoSQL server becomes a confidential OAuth client, handling browser redirects, code exchange, ID token validation, and GizmoSQL JWT issuance
- New `GET /oauth/initiate` API endpoint — generates a session UUID and returns the IdP authorization URL as JSON, eliminating the need for clients to know the server's secret key
- New `--oauth-client-id` / `GIZMOSQL_OAUTH_CLIENT_ID` — enables the OAuth HTTP server when set
- New `--oauth-client-secret` / `GIZMOSQL_OAUTH_CLIENT_SECRET` — OAuth client secret (stays on server)
- New `--oauth-scopes` / `GIZMOSQL_OAUTH_SCOPES` — scopes to request (default: `openid profile email`)
- New `--oauth-port` / `GIZMOSQL_OAUTH_PORT` — port for the OAuth HTTP(S) server (default: `31339`)
- New `--oauth-redirect-uri` / `GIZMOSQL_OAUTH_REDIRECT_URI` — override redirect URI for reverse proxy setups
- New `--oauth-disable-tls` / `GIZMOSQL_OAUTH_DISABLE_TLS` — disable TLS on the OAuth callback server for localhost development/testing (WARNING: should never be used on network-exposed servers)
- Clients only need `authType=external` in their connection string — no client-side secrets required
- Security: browser URLs contain a session hash (HMAC-SHA256); the raw UUID is only known to the polling client
- Pending sessions auto-expire after 15 minutes
- Requires `--token-allowed-issuer`, `--token-allowed-audience`, and a valid Enterprise license with `external_auth` feature

#### SSO/OAuth Authentication via JWKS Auto-Discovery (Enterprise)
- New `--token-authorized-emails` CLI flag / `GIZMOSQL_TOKEN_AUTHORIZED_EMAILS` env var to restrict which OIDC-authenticated users can connect
- Supports comma-separated patterns with wildcards (e.g., `*@company.com,admin@partner.com`)
- Default is `*` (all authenticated users allowed) for backward compatibility
- New `--token-jwks-uri` CLI flag / `GIZMOSQL_TOKEN_JWKS_URI` env var to specify a direct JWKS endpoint URL for token signature verification
- Automatic JWKS discovery from `--token-allowed-issuer` via OIDC `.well-known/openid-configuration` when no static cert path is provided
- Thread-safe JWKS key cache with configurable TTL and automatic refresh on key rotation (kid miss)
- Support for RSA (RS256/RS384/RS512) and EC (ES256/ES384/ES512) key types from JWKS
- New `--token-default-role` CLI flag / `GIZMOSQL_TOKEN_DEFAULT_ROLE` env var to assign a default role when IdP tokens lack a `role` claim
- Enterprise gating: JWKS-based external auth requires a valid Enterprise license; static cert path verification remains available in Core edition
- See [Token Authentication docs](https://gizmodata.github.io/gizmosql/#/token_authentication) for details

## [1.16.1] - 2026-02-05

### Added

#### Cross-Instance Token Acceptance
- New `--allow-cross-instance-tokens` CLI flag to relax instance_id validation during bearer token authentication
- New `GIZMOSQL_ALLOW_CROSS_INSTANCE_TOKENS` environment variable (set to `1` or `true` to enable)
- Useful for load-balanced deployments where multiple GizmoSQL server instances share the same secret key
- When enabled, tokens issued by other server instances (with valid signatures) are accepted
- Default behavior unchanged - strict mode rejects tokens from other instances
- See [Token Authentication docs](https://gizmodata.github.io/gizmosql/#/token_authentication) for details

## [1.16.0] - 2026-01-28

### Added

#### DuckLake-Backed Instrumentation (Enterprise)
- Instrumentation can now use a DuckLake catalog instead of a local DuckDB file
- Enables centralized, multi-instance instrumentation with transactional isolation
- New configuration parameters:
  - `--instrumentation-catalog` / `GIZMOSQL_INSTRUMENTATION_CATALOG`: Pre-attached catalog name
  - `--instrumentation-schema` / `GIZMOSQL_INSTRUMENTATION_SCHEMA`: Schema within the catalog (default: `main`)
- Works with persistent or session secrets for DuckLake setup
- Instrumentation catalog is protected as read-only for all clients (admins can read, no one can write)
- See [Session Instrumentation docs](https://gizmodata.github.io/gizmosql/#/session_instrumentation) for setup examples

### Changed

- Instrumentation catalog protection now applies to external catalogs (DuckLake), not just the default `_gizmosql_instr`
- DETACH protection extended to cover external instrumentation catalogs

### Fixed

- Fixed `catalog_access` propagation from token claims to client session

## [1.15.3] - 2026-01-26

### Changed

- Upgraded DuckDB from v1.4.3 to v1.4.4

## [1.15.1] - 2025-01-23

### Added

#### GeoArrow Support for GEOMETRY Types
- DuckDB's SPATIAL extension is now loaded by default at server startup
- GEOMETRY columns export with proper GeoArrow Arrow extension metadata
- Enables seamless integration with GeoArrow-aware clients like GeoPandas
- No manual setup required - spatial functions and GeoArrow export work out of the box
- Example Python workflow with ADBC and GeoPandas:
  ```python
  from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
  import geopandas as gpd

  with gizmosql.connect(uri="grpc+tls://localhost:31337",
                        db_kwargs={"username": "gizmosql_username",
                                   "password": "gizmosql_password",
                                   DatabaseOptions.TLS_SKIP_VERIFY.value: "true"},
                        autocommit=True) as conn:
      with conn.cursor() as cur:
          cur.execute("SELECT ST_Point(1.0, 2.0) AS geom")
          arrow_table = cur.fetch_arrow_table()
          # GeoPandas reads GeoArrow format directly - no WKB conversion needed!
          gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
  ```

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
| DuckDB | v1.4.4 |
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
