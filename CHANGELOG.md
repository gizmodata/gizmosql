# Changelog

All notable changes to GizmoSQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.21.2] - 2026-04-21

### Fixed

- **Bulk ingest inside an open transaction (issue [#155](https://github.com/gizmodata/gizmosql/issues/155))**: `adbc_ingest()` (and any Flight SQL `CommandStatementIngest`) now works when the client has an open transaction (e.g. `autocommit=False`). Previously the server unconditionally opened a nested transaction, which DuckDB rejects, surfacing as `Unexpected error in RPC handling (Unknown; ExecuteIngest)`. The server now honors `CommandStatementIngest.transaction_id` when present, and falls back to detecting `HasActiveTransaction()` on the session's connection to handle clients (e.g. the Go ADBC FlightSQL driver) that don't populate that field. Thanks to @fromm1990 for the detailed report and reproducer.

### Added

- **Active session count API**: New `GetActiveSessionCount()` function in the C library API returns the number of currently active client sessions. Thread-safe and callable while the server is running. Useful for monitoring and embedding (e.g., iOS app).
- **iOS/iPadOS app scaffold**: Initial project structure for a GizmoSQL iOS edition under `ios/`. Includes SwiftUI app with server start/stop controls, log viewer, settings UI, self-signed TLS certificate generation, and WiFi network status display. Cross-compilation support via CMake iOS toolchain.
- **iOS app: SQL client and data browser**: Added Query tab with terminal-style SQL client (syntax highlighting, box-drawing table output, query history, automatic 500-row limit) and Browser tab for navigating catalogs/schemas/tables/columns with data preview and drop-table confirmation. Both connect to the local server via embedded Arrow Flight SQL client over loopback. Includes splash screen with logo, privacy manifest, file sharing for database export, background task handling, and iPad layout support.
- **GizmoSQL on the Apple App Store**: The iOS edition is now available for download on the [Apple App Store](https://apps.apple.com/us/app/gizmosql/id6761951280). README and documentation updated with App Store badges and a new "iOS App" installation option.
- **iOS: DuckDB postgres extension statically linked**: On the iOS build only, the `postgres_scanner` extension is statically linked into the binary. iOS is compiled with `DISABLE_EXTENSION_LOAD` for App Store compliance (Guideline 2.5.2), so extensions must be static to be usable. Enables querying remote PostgreSQL databases from iOS via `ATTACH 'postgresql://...' AS pg`. On Linux/macOS/Windows this remains a normal runtime-loadable extension (`INSTALL postgres_scanner; LOAD postgres_scanner;`).
- **iOS: DuckDB ducklake extension statically linked**: On the iOS build only, the `ducklake` extension is statically linked into the binary (same App Store rationale as postgres_scanner above). Combined with the iOS app, this enables a "lakehouse in your pocket" experience. On Linux/macOS/Windows, `ducklake` remains a runtime-loadable extension.

### Changed

- **Decoupled Boost program_options from library target**: The `gizmosqlserver` static library no longer links `Boost::program_options` (only the CLI executables do). The library uses only header-only Boost components (uuid, algorithm/string). This enables embedding the library in environments where Boost compiled libraries are unavailable (e.g., iOS).

## [1.21.1] - 2026-04-13

### Changed

- **Upgraded DuckDB from v1.5.1 to v1.5.2**

## [1.21.0] - 2026-04-06

### Added

- **[Enterprise] Instance, session, and query tagging for instrumentation**: Users can attach arbitrary JSON metadata to instrumentation records for cost attribution, multi-tenant identification, and observability. Instance tags are set at startup via `--instance-tag` CLI flag or `GIZMOSQL_INSTANCE_TAG` env var. Session tags are set dynamically via `SET gizmosql.session_tag = '<json>'`. Query tags are set via `SET gizmosql.query_tag = '<json>'` and apply to all subsequent SQL statements until changed. All tags are validated as JSON and stored in the instrumentation schema (`instances.instance_tag`, `sessions.session_tag`, `sql_statements.query_tag`). Existing instrumentation databases are automatically migrated with the new columns. Fixes [#152](https://github.com/gizmodata/gizmosql/issues/152).
- **Client tag support**: `gizmosql_client` now supports `--session-tag` and `--query-tag` CLI flags (and `GIZMOSQL_SESSION_TAG` / `GIZMOSQL_QUERY_TAG` env vars) to automatically set tags after connecting. Tags can be overridden during the session via `SET gizmosql.*` commands.

## [1.20.3] - 2026-04-04

### Fixed

- **Client: verify connection before reporting success**: The client now performs a lightweight Flight SQL call (`GetSqlInfo`) after connecting to confirm authentication is valid. Previously, connecting without credentials (e.g., omitting `--username`) would report "Connected" but fail on the first query with a cryptic auth error.

## [1.20.2] - 2026-04-02

### Fixed

- **ArrowSchema leak in FetchResult()**: Added RAII guard to release exported `ArrowSchema` C structs on error and null-chunk paths in `DuckDBStatement::FetchResult()`. Previously, three early-return paths leaked the schema's release callbacks, which under concurrent load caused heap corruption and double-free crashes due to stale callbacks across allocator boundaries (DuckDB jemalloc vs glibc malloc). Fixes [#150](https://github.com/gizmodata/gizmosql/issues/150).

## [1.20.1] - 2026-03-31

### Fixed

- **Pager Page Down on macOS Terminal.app**: Fn+Down was intercepted by Terminal.app's own scroll buffer instead of reaching the pager. Now uses the alternate screen buffer (`ESC[?1049h`) — the same approach used by `less`, `vim`, and `top` — so all pager keys work correctly. Previous terminal content is also restored cleanly when exiting the pager.

## [1.20.0] - 2026-03-31

### Added

- **SQL syntax highlighting**: The GizmoSQL client now highlights SQL input as you type — keywords (green), strings (yellow), numbers (magenta), comments (gray), functions (cyan), and unclosed quotes/brackets (red). Toggle with `.highlight on|off`. Inspired by [DuckDB v1.5](https://duckdb.org/2026/03/09/announcing-duckdb-150).
- **Dynamic prompting**: The prompt now shows the current `catalog.schema>` context in DuckDB's orange color, updating automatically after DDL, `USE`, `ATTACH`/`DETACH`, `CALL`, and `.connect` commands.
- **`.describe TABLE` command**: Quickly inspect a table's column names and types without writing a full DESCRIBE query.
- **Hierarchical `.tables` view**: `.tables` now displays DuckDB-style side-by-side boxes with column names, types, and row counts. Catalog headers in orange, schema headers in blue, box borders and types in gray — matching DuckDB's color palette. Use `.tables --flat` for the previous flat listing.
- **Last result reference (`_`)**: Query results are cached in memory. Reference `_` as a table name in subsequent queries (e.g., `SELECT * FROM _ WHERE col > 5`) — the client uploads the cached result to the server as a temporary table via bulk ingest for full SQL support including joins and aggregations. Use `.last` to re-display and `.export_last [FILE]` to save as Arrow IPC.
- **Built-in pager**: Large result sets (50+ rows by default) are automatically paged with keyboard navigation — Page Up/Down, j/k for line scroll, g/G for home/end, q to quit. Configure with `.pager on|off|N`.
- **`.about` command**: Show client version, GizmoData LLC copyright, and project links.
- **Red error messages**: All error output styled in bold red, matching DuckDB's error styling.
- **Security Guide**: New comprehensive documentation (`docs/security.md`) covering TLS, mTLS, authentication, authorization, and audit logging — written for users unfamiliar with these concepts.
- **CLI doc example tests**: 16 integration tests that run `gizmosql_client` as a subprocess to verify documentation examples work correctly.

### Fixed

- **`_` (last result) works in non-interactive mode**: The underscore table reference now works in `--command`, `--file`, and pipe modes — not just interactive mode.
- **MSVC regex stack overflow**: Replaced `std::regex` usage in init SQL command splitter and underscore detection with simple string scanning to avoid MSVC's `regex_error(error_stack)` crash on Windows.
- **Arrow libtool detection on newer macOS**: Patched Arrow's `BuildUtils.cmake` to handle the `cctools_ld` format reported by newer Xcode versions.

## [1.19.7] - 2026-03-24

### Fixed

- **`query_log_level` now independent of `--log-level`**: `SET gizmosql.query_log_level = DEBUG` (or `--query-log-level debug`) now correctly emits DEBUG query logs even when the global `--log-level` is `info`. Previously, the global logger threshold acted as an additional gate that suppressed query logs below its level.
- **`SET GLOBAL` propagates immediately to existing sessions**: `SET GLOBAL gizmosql.query_log_level` and `SET GLOBAL gizmosql.query_timeout` now take effect immediately for all existing sessions that haven't set a session-level override. Previously, the server's default was baked into each session at creation time, requiring a reconnect to pick up global changes.
- **Auth log level threshold for bearer token validation**: Bearer token validation logs now correctly use `auth_log_level` as the threshold. Previously, repeat token validations could bypass the threshold because `GetTokenLogLevel()` was used as both the threshold and display severity.

### Added

- **`docs/set_commands.md`**: New documentation for `SET gizmosql.*` commands covering `query_log_level` and `query_timeout` with session/global scope, valid values, and admin restrictions.

## [1.19.6] - 2026-03-24

### Fixed

- **Internal query log level filtering**: Internal queries (e.g., `DoGetTables`, `GetFlightInfoStatement`) now correctly use the session/server `query_log_level` as the threshold and the statement's own log level as the display severity. Previously the arguments were effectively swapped, causing internal queries marked as `ARROW_DEBUG` to leak into `INFO`-level logs. With this fix, internal queries are properly suppressed at the default `INFO` threshold and only appear when `query_log_level` is set to `DEBUG`.

### Added

- **`is_internal` and `flight_method` attributes in query logs**: Query log entries now include `is_internal` (`true`/`false`) indicating whether the SQL statement originated from an internal Flight SQL endpoint, and `flight_method` (e.g., `DoGetTables`, `GetFlightInfoStatement`) identifying the originating RPC. These aid in log filtering and debugging.

## [1.19.5] - 2026-03-23

### Changed

- **Upgraded DuckDB from v1.5.0 to v1.5.1**

## [1.19.4] - 2026-03-16

### Added

- **`--oauth-redirect-uri` / `GIZMOSQL_OAUTH_REDIRECT_URI`** *(Enterprise)*: Override the OAuth redirect URI independently of `--oauth-base-url`. When set, takes precedence over the redirect URI derived from the base URL (`<base-url>/oauth/callback`). Useful when the redirect URI differs from the base URL, e.g., separate proxy endpoints. If not set, the redirect URI is derived from `--oauth-base-url` as before (backward compatible).

- **Catalog visibility filtering** *(Enterprise)*: When `catalog_access` rules are present in a JWT token, metadata queries now automatically hide unauthorized catalogs. This applies to `SHOW DATABASES`, `SHOW ALL TABLES`, `information_schema.*` views, `duckdb_*()` table functions, and all Flight SQL metadata RPCs (`GetCatalogs`, `GetDbSchemas`, `GetTables`). The `system` and `temp` catalogs are always visible. Tokens without `catalog_access` rules are unaffected (backward compatible).

### Fixed

- **Multi-statement fallback now preserves SQL rewrites**: The fallback path for statements that cannot be prepared (e.g., `PIVOT`) now uses the rewritten SQL instead of the original, ensuring that `GIZMOSQL_*()` pseudo-function replacements and catalog visibility filters are applied correctly.
- **`--init-sql-commands-file` path validation skipped when passed via CLI**: When the init SQL commands file was provided via the CLI flag (not the `INIT_SQL_COMMANDS_FILE` env var), the path resolution and existence check were accidentally nested inside the env var fallback block and never executed. The file path is now always validated regardless of how it was provided.

## [1.19.3] - 2026-03-11

### Added

- **DuckDB connection tracking metric** (`gizmosql.duckdb.connections.open`): New OpenTelemetry up/down counter that tracks open DuckDB connection objects, including session connections and internal utility/instrumentation connections. Complements the existing `gizmosql.connections.active` metric which tracks GizmoSQL sessions.

### Changed

- **RAII-based DuckDB connection lifecycle**: DuckDB connections are now wrapped in a `TrackedDuckDBConnection` class that automatically increments/decrements the open connection counter. Session cleanup (interrupt in-flight queries, release prepared statements, decrement counters) is now handled by the `ClientSession` destructor instead of being scattered across multiple removal paths.
- **Session-owned prepared statements**: Prepared statements are now owned by their parent `ClientSession` instead of a global server-level map. This establishes a clear ownership hierarchy (Server → Sessions → Statements) with consistent `weak_ptr` back-references at each level, matching the existing `ClientSession` → `DuckDBFlightSqlServer` pattern.

## [1.19.2] - 2026-03-10

### Fixed

- **Query and auth log levels now act as visibility thresholds**: When `--query-log-level` or `--auth-log-level` is set to `error`, routine INFO-level messages (e.g., "SQL command execution succeeded", "Successfully authenticated") are now properly suppressed instead of being promoted to ERROR severity. The component log level controls whether a message appears at all; the display severity always reflects the message's true nature ([#136](https://github.com/gizmodata/gizmosql/issues/136)).

## [1.19.1] - 2026-03-09

### Added

- **Multi-instance OAuth proxy routing** (`--oauth-instance-id` / `GIZMOSQL_OAUTH_INSTANCE_ID`): Optional instance identifier that is embedded in the OAuth `state` parameter as `<instance-id>.<session-hash>`. This allows a shared OAuth callback proxy to extract the instance ID from the state and route the callback to the correct GizmoSQL server, enabling a single registered redirect URI (e.g., `https://oauth.example.com/oauth/callback`) to serve many dynamically provisioned instances. Fully backward-compatible — when unset, behavior is identical to before.

### Fixed

- **Authentication log level misreporting severity**: When `--auth-log-level` was set to a level like `error`, successful authentication messages were logged with ERROR severity instead of INFO. The auth log level now acts as a visibility threshold — success messages always display at INFO severity, and the auth log level controls whether they appear at all ([#136](https://github.com/gizmodata/gizmosql/issues/136)).

## [1.19.0] - 2026-03-09

### Added

- **Python ADBC connectivity guide**: New documentation page (`docs/python_adbc.md`) explaining GizmoSQL's lazy execution model and why `adbc-driver-gizmosql` is the recommended Python driver. Covers automatic DDL/DML handling, OAuth/SSO authentication, bulk ingestion, and Pandas integration. Also documents the pitfall when using `adbc-driver-flightsql` directly, where DDL/DML via `cursor.execute()` silently does nothing without an explicit fetch ([#134](https://github.com/gizmodata/gizmosql/issues/134)).

### Changed

- **Upgraded DuckDB from v1.4.4 to v1.5.0**: Includes native VARIANT data type support and built-in GeoArrow export for GEOMETRY columns (the `register_geoarrow_extensions()` call has been removed as it is no longer needed). Note: DuckDB's Arrow exporter does not yet support VARIANT natively — clients should cast VARIANT columns to VARCHAR or JSON before querying.
- **Upgraded SQLite from 3.51.1 to 3.52.0**
- **Upgraded jwt-cpp from v0.7.1 to v0.7.2**
- **Upgraded cpp-httplib from v0.18.3 to v0.37.0**
- **Upgraded OpenTelemetry C++ from v1.18.0 to v1.25.0**
- **`--otel-enabled` flag changed from string to boolean**: The `--otel-enabled` CLI flag now uses a boolean value (`true`/`false`) instead of string (`on`/`off`), consistent with all other boolean CLI flags. The `GIZMOSQL_OTEL_ENABLED` environment variable accepts `1`/`true` to enable.
- **Python ADBC docs updated for v1.1.0**: All documentation examples now use `cursor.execute()` for DDL/DML statements instead of `cursor.execute_update()`, reflecting the new auto-detection in `adbc-driver-gizmosql` v1.1.0. The `execute_update()` method remains available for when the rows-affected count is needed.
- **Environment variable handling consolidated in library**: All boolean env var fallbacks (`GIZMOSQL_ENABLE_INSTRUMENTATION`, `GIZMOSQL_ALLOW_CROSS_INSTANCE_TOKENS`, `GIZMOSQL_OAUTH_DISABLE_TLS`, `GIZMOSQL_OTEL_ENABLED`) are now resolved in `RunFlightSQLServer()` in the library, ensuring consistent behavior for both the CLI executable and direct C API users. These parameters use `std::optional<bool>` so explicit CLI values (`--flag true` or `--flag false`) always take precedence over environment variables.

### Fixed

- **Duplicate OpenTelemetry initialization**: Removed a duplicate OTel initialization block in `RunFlightSQLServer` that was introduced during the merge of PR #97.
- **OpenTelemetry build protobuf path**: Fixed `ARROW_PROTOBUF_DIR` to point to Arrow's install tree (`third_party/arrow`) instead of the internal build directory (`protobuf_ep-install`), fixing the OTel CMake configure step on CI.

## [1.18.5] - 2026-02-26

### Added

- **Limit row fetching for large result sets** (`gizmosql_client`): In interactive mode, the client now fetches only the rows needed for display (default 40) instead of streaming the entire result set. The real total row count is obtained from `FlightInfo::total_records()` or a `SELECT COUNT(*)` fallback, and displayed in the footer (e.g., "6001215 rows (40 shown)"). Three dot rows (`·`) appear after the data to visually indicate truncation. Non-interactive modes (CSV, JSON, etc.) continue to fetch all rows.

### Fixed

- **TLS system CA certificate loading** (`gizmosql_client`): When TLS is enabled without an explicit `--tls-roots` path, the client now automatically loads system root CA certificates (macOS, Linux, and Windows Certificate Store) so server certificates can be verified without requiring a manual CA bundle.
- **GetTables catalog filter**: The `GetTables` RPC now uses `LIKE` for catalog pattern matching instead of exact equality, allowing wildcard patterns (e.g., `%`) to match across catalogs.

## [1.18.4] - 2026-02-21

### Fixed

- **MSI install directory**: MSI installer now correctly installs to `C:\Program Files\GizmoSQL` instead of `C:\Program Files (x86)\GizmoSQL` by building a 64-bit MSI package (`-arch x64`).

## [1.18.3] - 2026-02-21

### Added

- **Query cancellation** (`gizmosql_client`): Pressing Ctrl+C during a running query now cancels the query on the server via `CancelFlightInfo` RPC and returns to the prompt. Uses a dedicated `sigwait()` thread (same approach as the JVM) for safe gRPC calls, with a separate cancel client connection for thread safety.
- **Clean session disconnect**: The client now sends `CloseSession` RPC on exit (`.quit`, Ctrl+D, SIGTERM) so the server can clean up session state immediately.
- **Windows x64 support**: Native MSVC builds for Windows x64 with full test coverage. Includes DigiCert EV code signing via Azure Key Vault for both executables and the MSI installer.
- **Windows MSI installer**: WiX v4-based MSI installer that installs `gizmosql_server.exe` and `gizmosql_client.exe` to `Program Files\GizmoSQL` and adds the install directory to the system PATH. Includes application icon and Add/Remove Programs integration.
- **Bundled VC++ runtime DLLs**: `vcruntime140.dll`, `vcruntime140_1.dll`, and `msvcp140.dll` are bundled with both the CLI zip and MSI installer, so DuckDB extensions (ICU, Spatial, etc.) load correctly on clean Windows installs without requiring the VC++ Redistributable.
- **Windows version resources**: Executables include VERSIONINFO resources with version, company, and product metadata visible in Windows file properties.
- **Dynamic terminal width** (`gizmosql_client`): Output width now automatically adapts when the terminal window is resized — each query re-reads the terminal dimensions before rendering. Explicit `.maxwidth N` overrides auto-detection; `.maxwidth 0` re-enables it.

### Changed

- **ADBC driver migration**: All Python examples, docs, tests, and CI now use [`adbc-driver-gizmosql`](https://pypi.org/project/adbc-driver-gizmosql/) instead of `adbc-driver-flightsql`. The new driver provides a simplified connection API (no `db_kwargs`/`DatabaseOptions`), `execute_update()` for DDL/DML statements (avoids lazy execution pitfalls), and OAuth/SSO support for Enterprise users.
- **Default username**: Changed the default username from `gizmosql_username` to `gizmosql_user` for brevity. If you were relying on the old default (without explicitly setting `--username` or `GIZMOSQL_USERNAME`), update your client connections accordingly.
- **Cross-platform CMake build**: Library paths, linker flags, and third-party build scripts now use platform-conditional logic for Windows, macOS, and Linux. Arrow patch command replaced with a portable CMake script (no more `sed`).
- **C++20**: Upgraded from C++17 to C++20 for MSVC designated initializer compatibility.

### Fixed

- **Case-insensitive URI boolean parameters**: Connection URI boolean parameters (`useEncryption`, `disableCertificateVerification`) now accept any case (`true`, `True`, `TRUE`, `1`).
- **Unicode box-drawing on Windows**: Client output renderer now correctly displays Unicode box-drawing characters on Windows by setting the MSVC execution charset to UTF-8 and configuring the console output code page.

## [1.18.0] - 2026-02-17

### Added

#### Result Rendering Improvements (`gizmosql_client`)
- **Split display**: When row truncation is active, box/table renderers show the first and last rows with 3 dot indicator rows (`·`) in between (DuckDB-style), e.g. top 20 rows + `···` + bottom 20 rows for a 40-row limit
- **In-table footer**: Row/column counts are rendered inside the box border with a merged footer row (e.g., `│ 60175 rows (40 shown)  16 columns │`), matching DuckDB's output style
- **Row truncation**: Interactive box/table mode shows 40 rows by default; configurable via `.maxrows N` dot command (0 = unlimited)
- **Column truncation**: Box/table output fits to terminal width by capping column widths and omitting rightmost columns that don't fit; configurable via `.maxwidth N` dot command (0 = auto-detect terminal)
- **Column data types**: Box and table renderers show DuckDB-friendly type names (e.g., `bigint`, `varchar`, `double`, `date`) in a centered row below column names
- **Centered column headers**: Column names are now centered in box and table output modes (matching DuckDB style)
- **DuckDB-friendly type names**: Arrow types are displayed as familiar names (`varchar` instead of `string`, `bigint` instead of `int64`, `date` instead of `date32[day]`, `decimal(P,S)` instead of `decimal128(P, S)`, etc.)
- **Right-aligned numbers**: Numeric columns (integer, float, decimal) are right-aligned in box and table output modes
- New `.maxrows [N]` dot command to show or set maximum rows displayed
- New `.maxwidth [N]` dot command to show or set maximum display width

#### Tab Completion (`gizmosql_client`)
- **Context-aware tab completion**: DuckDB-style tab completion using FlightSQL metadata endpoints for schema introspection
- **Table name completion**: `select * from line<TAB>` completes to `lineitem` (uses `GetTables()` RPC)
- **SQL keyword completion**: ~100 common SQL keywords with case preservation (lowercase prefix → lowercase, uppercase → uppercase)
- **Dot command completion**: All dot commands (`.tables`, `.schema`, etc.) with green highlighting
- **Schema-qualified completion**: `main.line<TAB>` completes `main.lineitem`
- **Inline hints**: Single-match completions appear as gray inline hints
- **Lazy schema cache**: First TAB press populates the cache; auto-invalidated after DDL statements (`CREATE`/`DROP`/`ALTER`/`ATTACH`/`DETACH`) and `.connect`
- New `.refresh` dot command to manually refresh the tab-completion schema cache

#### `.show` Improvements (`gizmosql_client`)
- **Sectioned output**: `.show` now organizes information into three sections: **Server** (version, edition, instance, engine, Arrow version), **Session** (connection details, session ID, role, catalog, schema), and **Settings** (client-configurable options)
- **Server metadata via FlightSQL**: Engine version and Arrow version are fetched from the FlightSQL `GetSqlInfo` metadata endpoint (`FLIGHT_SQL_SERVER_VERSION`, `FLIGHT_SQL_SERVER_ARROW_VERSION`)
- **Session info**: Shows session ID, role, current catalog, and current schema from server-side pseudo-functions

#### Interactive Client Shell (`gizmosql_client`)
- New interactive SQL shell replacing the old single-shot client, modeled after psql and DuckDB CLI
- **Line editing**: replxx-based input with persistent history (`~/.gizmosql_history`), cursor navigation, and multi-line SQL accumulation
- **15 output modes**: `box` (default, Unicode), `table` (ASCII), `csv`, `tabs`, `json`, `jsonlines`, `markdown`, `line`, `list`, `html`, `latex`, `insert`, `quote`, `ascii`, `trash` — selectable via `--csv`, `--json`, `--table`, `--box`, `--markdown` CLI flags or `.mode` dot command
- **Dot commands**: `.tables`, `.schema`, `.catalogs`, `.mode`, `.headers`, `.timer`, `.output`, `.once`, `.nullvalue`, `.separator`, `.show`, `.echo`, `.bail`, `.read`, `.shell`, `.cd`, `.prompt`, `.help`, `.quit`
- **Non-interactive modes**: `-c "SQL"` for single commands, `-f FILE` for script files, stdin pipe/heredoc for scripted workflows
- **Init file support**: Automatically loads `~/.gizmosqlrc` on startup (override with `--init FILE`, disable with `--no-init`)
- **Output redirection**: `-o FILE` flag and `.output FILE` / `.once FILE` dot commands
- **OAuth/SSO browser login**: `--auth-type external` initiates browser-based OAuth flow using the server's `/oauth/initiate` and `/oauth/token` endpoints (via cpp-httplib)
- **TLS support**: `--tls`, `--tls-roots`, `--tls-skip-verify`, `--mtls-cert`, `--mtls-key`
- **Environment variables**: `GIZMOSQL_HOST`, `GIZMOSQL_PORT`, `GIZMOSQL_USER`, `GIZMOSQL_PASSWORD`, `GIZMOSQL_TLS`, `GIZMOSQL_TLS_ROOTS`, `GIZMOSQL_OAUTH_PORT`
- **Password prompt**: Secure interactive password entry with `-W` when connected to a terminal (password cannot be passed as a CLI argument, like psql)
- **Timer**: `--timer` flag or `.timer on` to display query execution time
- **Disconnected mode**: Start the client without connection parameters and use `.connect` to connect interactively
- **`.connect` dot command**: Connect (or reconnect) to a server from within the interactive shell, supports both positional args (`.connect HOST PORT USERNAME`) and URI format (`.connect gizmosql://host:port?params`)
- **Connection URI**: `--uri` flag and positional argument support for `gizmosql://HOST:PORT[?username=X&useEncryption=true&authType=external]` connection strings

### Fixed

- **OAuth discovery** (`gizmosql_client`): The client now discovers the OAuth endpoint URL from the server via a discovery handshake (`username="__discover__"`), instead of constructing it from the gRPC connection's TLS setting. This fixes OAuth login when the server's OAuth HTTP port uses a different TLS configuration than the gRPC port (e.g., `--oauth-disable-tls`). Falls back to the previous behavior for older servers that don't support discovery.

### Changed

- **Apache Arrow** updated from 23.0.0 to **23.0.1**
- CLI parsing switched from gflags to Boost.ProgramOptions (with short options: `-h`, `-p`, `-u`, `-c`, `-f`, `-o`, `-q`, `-e`, `-v`)
- **`--password`/`-W` no longer accepts a value** — like `psql`, the flag only forces an interactive prompt. Use the `GIZMOSQL_PASSWORD` environment variable for non-interactive password auth

### Removed

- Old single-shot `gizmosql_client` based on gflags (replaced by the new interactive shell)

## [1.17.4] - 2026-02-13

### Added

- `--oauth-base-url` CLI flag (and `GIZMOSQL_OAUTH_BASE_URL` env var) to override the base URL for the OAuth HTTP server (e.g., `https://my-proxy:443`). Both the redirect URI (`/oauth/callback`) and the discovery URL advertised to clients are derived from this, replacing the previous `--oauth-redirect-uri` flag
- ICU extension is now loaded by default on server startup, enabling `TIMESTAMPTZ` support and timezone-aware timestamp operations
- Arrow type mapping for `LIST`, `LARGE_LIST`, `FIXED_SIZE_LIST`, `STRUCT`, and `MAP` types in both `GetDuckDBTypeFromArrowType` and `GetDataTypeFromDuckDbType`
- Arrow value conversion for `LIST`, `LARGE_LIST`, `STRUCT`, and `MAP` types in `ConvertArrowCellToDuckDBValue`, enabling ADBC bulk ingest of complex/nested types

### Changed

- `--oauth-redirect-uri` / `GIZMOSQL_OAUTH_REDIRECT_URI` replaced by `--oauth-base-url` / `GIZMOSQL_OAUTH_BASE_URL` — the redirect URI is now automatically derived as `<base-url>/oauth/callback`

### Fixed

- `TIMESTAMP_TZ` now correctly maps to `arrow::timestamp(MICRO, "UTC")` instead of `arrow::decimal128(38, 0)` in parameter schemas
- `TIME_TZ` now correctly maps to `arrow::time64(MICRO)` instead of `arrow::decimal128(38, 0)` in parameter schemas
- `HUGEINT` now maps to `arrow::decimal128(38, 0)` in its own case, separated from null/unknown types which now correctly map to `arrow::null()`
- `LARGE_STRING` and `LARGE_BINARY` Arrow types are now handled with their correct array types (`LargeStringArray`, `LargeBinaryArray`) instead of being combined with `STRING`/`BINARY`
- Arrow timestamps with timezone info now correctly map to `TIMESTAMP_TZ` in DuckDB (via timezone detection in `GetDuckDBTypeFromArrowType`)

## [1.17.2] - 2026-02-12

### Fixed

- Docker entrypoint scripts (`start_gizmosql.sh`, `start_gizmosql_slim.sh`) no longer pass `--database-filename=""` when the filename is empty, which caused Boost.ProgramOptions to reject the argument — in-memory mode now works correctly with `DATABASE_FILENAME=":memory:"` or when unset

## [1.17.0] - 2026-02-11

### Added

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

#### Instrumentation Discovery
- SQL functions for instrumentation metadata discovery: `GIZMOSQL_INSTRUMENTATION_ENABLED()`, `GIZMOSQL_INSTRUMENTATION_CATALOG()`, `GIZMOSQL_INSTRUMENTATION_SCHEMA()` — allows clients to dynamically discover instrumentation availability and catalog/schema configuration via standard SQL queries

#### Entrypoint & Configuration
- `GIZMOSQL_EXTRA_ARGS` env var for passing additional CLI flags to `gizmosql_server` in entrypoint scripts
- `MTLS_CA_CERT_FILENAME` and `HEALTH_PORT` env vars in entrypoint scripts
- `:memory:` as an explicit value for `DATABASE_FILENAME` to request in-memory mode
- Comprehensive env var documentation in entrypoint script headers

### Changed

- Docker entrypoint scripts now default to in-memory database when `DATABASE_FILENAME` is not set

### Fixed

- Catalog permissions handler no longer hardcodes `_gizmosql_instr` — now uses the actual configured instrumentation catalog name, supporting custom catalogs (e.g., DuckLake)

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
                        db_kwargs={"username": "gizmosql_user",
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
| Apache Arrow | **23.0.1** |
| DuckDB | v1.5.1 |
| SQLite | 3.52.0 |
| jwt-cpp | v0.7.2 |
| OpenTelemetry C++ | v1.25.0 |

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
