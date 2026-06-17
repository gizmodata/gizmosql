# GizmoSQL LTS Channel

GizmoSQL ships in two parallel **release channels** that share the same GizmoSQL feature set and only differ in which DuckDB release is bundled:

| Channel    | DuckDB tracked                         | When to pick it |
|------------|----------------------------------------|------------------|
| **Stable** | Latest DuckDB minor (e.g. `v1.5.4`)    | You want every new DuckDB feature, type, and performance improvement on its normal cadence. |
| **LTS**    | Most recent DuckDB LTS (e.g. `v1.4.5`) | You need a slower-moving, longer-supported DuckDB engine — production deployments where the underlying database's stability guarantees matter more than new features. |

Both channels get every GizmoSQL fix, feature, and quality-of-life improvement at the same time. Choosing LTS only changes which DuckDB version is statically linked into the binary; the GizmoSQL CLI flags, library API, configuration, authentication, and protocol behavior are identical.

## Which DuckDB versions are LTS?

DuckDB designates LTS releases on its own schedule. The authoritative list (with LTS windows, end-of-support dates, and what counts as "current LTS") lives at:

➡️ **<https://duckdb.org/release_calendar>**

When DuckDB names a new LTS, GizmoSQL bumps `DUCKDB_LTS_VERSION` in `CMakeLists.txt` and the next GizmoSQL release ships the new LTS pin alongside a stable build. Until then, the LTS channel stays on the previous LTS even as the stable channel moves forward.

## Telling the channels apart

The LTS server identifies itself in four ways so it's never ambiguous which build is running:

**1. The `--version` flag on both the server and the client binaries**

```text
$ gizmosql_server_lts --version
GizmoSQL Server CLI: v1.25.0-LTS

$ gizmosql_client_lts --version
GizmoSQL Client v1.25.0-LTS
```

**2. The startup banner**

```text
INFO ... GizmoSQL server version: v1.25.0-LTS - with engine: DuckDB (LTS channel — DuckDB v1.4.5) - will listen on grpc+tcp://0.0.0.0:31337
```

**3. The `GIZMOSQL_VERSION()` SQL function** — query it from any client (JDBC, ADBC, gizmosql_client, etc.) to confirm which channel the server you connected to was built from:

```sql
SELECT GIZMOSQL_VERSION();
-- v1.25.0-LTS    (when connected to an LTS server)
-- v1.25.0        (when connected to a stable server)
```

**4. The `.about` / `.info` dot-commands and the interactive shell banner** in `gizmosql_client_lts` all carry the suffix.

The `-LTS` suffix is also reflected in OpenTelemetry `service.version`, the `gizmosql_version` column on session-instrumentation records, and any other place the server reports its version — so a single string lets you tell at a glance which channel produced any log line, span, or audit row.

## Artifact naming

LTS artifacts are suffixed so they coexist with stable on the same machine, image registry, or download index:

| Type              | Stable                                         | LTS                                              |
|-------------------|------------------------------------------------|--------------------------------------------------|
| Server binary     | `gizmosql_server`                              | `gizmosql_server_lts`                            |
| Client binary     | `gizmosql_client`                              | `gizmosql_client_lts`                            |
| CLI zip           | `gizmosql_cli_<os>_<arch>.zip`                 | `gizmosql_cli_<os>_<arch>_lts.zip`               |
| Windows MSI       | `GizmoSQL-<arch>.msi` *(amd64, arm64)*         | `GizmoSQL-<arch>-lts.msi` *(amd64, arm64)*       |
| Docker (Hub)      | `gizmodata/gizmosql:<ver>` *(+ `-slim`)*       | `gizmodata/gizmosql-lts:<ver>` *(+ `-slim`)*     |
| Docker (GHCR)     | `ghcr.io/gizmodata/gizmosql:<ver>`             | `ghcr.io/gizmodata/gizmosql-lts:<ver>`           |
| Homebrew formula  | `gizmodata/tap` → `gizmosql`                   | `gizmodata/tap` → `gizmosql-lts` *(same tap)*    |

> **iOS:** The iOS edition is currently stable-only. The DuckDB LTS pin is not synced for the iOS extension build matrix.

## Installing GizmoSQL LTS

### Homebrew (macOS / Linux)

The LTS formula lives in the same `gizmodata/tap` as the regular `gizmosql` formula — no separate tap to add:

```bash
brew tap gizmodata/tap         # once (skip if already tapped)
brew install gizmosql-lts      # LTS channel
# or, side-by-side:
brew install gizmosql gizmosql-lts
```

The LTS formula installs `gizmosql_server_lts` / `gizmosql_client_lts`, so it coexists cleanly with the regular `gizmosql` formula's `gizmosql_server` / `gizmosql_client`.

### Docker

```bash
# Full image
docker pull gizmodata/gizmosql-lts:latest

# Slim image (no extras / smaller surface area)
docker pull gizmodata/gizmosql-lts:latest-slim
```

Pin a specific GizmoSQL version with `gizmodata/gizmosql-lts:v1.24.0`.

### Direct download

Download the LTS zip / MSI for your platform from the [GitHub Releases page](https://github.com/gizmodata/gizmosql/releases). The LTS files have an `_lts` (zip) or `-lts` (MSI) suffix in the filename.

## Building from source

The LTS channel is a CMake build option:

```bash
cmake -B build -DGIZMOSQL_DUCKDB_CHANNEL=lts
cmake --build build
```

Override the LTS version pin with `-DDUCKDB_LTS_VERSION=v1.4.4` if you need a specific point release.

The default (`stable`) is unchanged: `cmake -B build` produces the regular `gizmosql_server` / `gizmosql_client` binaries with no `-LTS` markers.

## Switching channels in production

Because the server protocol and on-disk formats are stable across DuckDB minor releases (within DuckDB's own compatibility commitments — see the [release calendar](https://duckdb.org/release_calendar)), you can switch a deployment from stable to LTS by replacing the binary / image and restarting:

1. Stop the running `gizmosql_server`.
2. Replace it with `gizmosql_server_lts` (or pull the `gizmodata/gizmosql-lts` image).
3. Start it the same way — same flags, same env vars, same database files.

GizmoSQL session tokens, instrumentation records, and the database file all carry forward. (Always test on a non-production deployment first, especially across DuckDB *major* versions.)

## License

The LTS channel ships under the same Apache 2.0 / Commercial dual-license model as the stable channel, and the same Enterprise license file works against both — see the [Editions](editions.md) page.
