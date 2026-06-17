# Changelog

All notable changes to GizmoSQL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.30.1] - 2026-06-17

### Changed

- **Upgraded DuckDB to v1.5.4 (stable channel) and v1.4.5 (LTS channel).** Both are upstream bugfix releases shipped on 2026-06-17 (DuckDB v1.5.3 → v1.5.4, v1.4.4 → v1.4.5). The iOS extension pins (`ducklake`, `httpfs`) were re-synced to match DuckDB's own pins for the v1.5.4 release; `postgres_scanner` remains intentionally pinned to the v1.5.2-era commit (it links cleanly against v1.5.4, since these are ABI-compatible patch releases).
- **Documentation site migrated from Docsify to MkDocs Material for SEO/AEO.** [docs.gizmosql.com](https://docs.gizmosql.com) is now pre-rendered static HTML built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/) instead of client-side-rendered Docsify. Every page now has a real URL (`/quickstart/` instead of `/#/quickstart`) with full HTML content, a per-page `<title>`, and a generated `sitemap.xml` — so search engines can index individual pages and AI crawlers (which don't execute JavaScript) can read the docs at all. The raw Markdown sources are also published alongside the HTML (e.g. `/quickstart.md`) and indexed in a new [`llms.txt`](https://docs.gizmosql.com/llms.txt) for answer engines. Legacy `/#/page` links are redirected client-side to the new URLs. The docs CI build now runs `mkdocs build --strict`, which fails on broken internal links and anchors.

## [1.30.0] - 2026-06-12

### Added

- **Windows ARM64 builds** — native `arm64` Windows binaries (for Snapdragon X-class and other Windows-on-Arm machines) now ship alongside the existing `amd64` ones, in both the stable and LTS channels: signed CLI zips (`gizmosql_cli_windows_arm64.zip`, `gizmosql_cli_windows_arm64_lts.zip`) and signed MSI installers (`GizmoSQL-arm64.msi`, `GizmoSQL-arm64-lts.msi`), all with build-provenance attestations. Built natively on GitHub's `windows-11-arm` hosted runners (no cross-compilation), with the integration test suite running on ARM64 in CI. The MSI now packages the VC++ runtime DLLs captured from the build runner's VC redist (matching the binary architecture) instead of the MSI runner's x64 `System32` copies.

### Fixed

- **Linux release binaries now run out of the box on Raspberry Pi OS, Amazon Linux 2023, Ubuntu 20.04+, and Debian 11+.** The Linux CLI binaries (both arches, both channels) are now compiled in a `manylinux_2_28` container (AlmaLinux 8, glibc 2.28 baseline, gcc-toolset-14) instead of directly on the Ubuntu 24.04 runners — previously they required `GLIBC_2.38` / `GLIBCXX_3.4.32`, which made them fail on any distro older than ~mid-2024 (e.g. Raspberry Pi OS bookworm, Amazon Linux 2023) with "GLIBC_2.38 not found". No performance impact: the same gcc 14 code generation is used, and glibc selects its optimized routines at runtime on the target machine (IFUNC). CI now enforces the glibc 2.28 ceiling on every build (symbol-version check in `scripts/build_portable_linux.sh`) and smoke-tests the produced binaries in `debian:bookworm` (the Raspberry Pi OS userland) and `amazonlinux:2023` containers, so a portability regression can never ship silently.

### Changed

- **Quick Start version auto-sync now happens at docs-publish time instead of via a commit to `main`.** The `deploy-docs` workflow runs `scripts/update_docs_version.sh` against the latest release tag right before publishing GitHub Pages, and is triggered after a successful release via `workflow_run`. This replaces the `sync-docs-version` CI job, which tried to push the version bump to `main` and was rejected by the branch-protection ruleset (`github-actions[bot]` is not exempt). The live docs always reflect the newest release with no secrets and no push to the protected branch.

## [1.29.0] - 2026-06-09

### Added

- **`--license-key` / `GIZMOSQL_LICENSE_KEY` — supply the Enterprise license as an inline JWT value.** *[Enterprise]* Operators can now pass the literal license JWT directly instead of a file path, which is convenient for container/secret-manager deployments that inject secrets as environment variables. The existing `--license-key-file` / `GIZMOSQL_LICENSE_KEY_FILE` continues to work unchanged; when both are provided the inline key takes precedence and a warning is logged that the file is ignored. Available via the CLI flag, env var, and the `RunFlightSQLServer()` library API (new `license_key` parameter, defaulted for backward compatibility). Implemented as `LicenseManager::LoadLicenseFromString()` + a second argument to `EnterpriseFeatures::Initialize()`.
- **Quick Start docs now auto-track the latest release.** The example server-startup banner and `GIZMOSQL_VERSION()` output in `docs/quickstart.md` are rewritten to the released version by `scripts/update_docs_version.sh` at docs-publish time. The Quick Start also now notes that starting a second server on the default port `31337` fails with an "address already in use" error, and how to pick a different `--port`.

### Changed

- Updated the Quick Start example version strings from `v1.25.1` to `v1.29.0`.

## [1.28.0] - 2026-06-03

### Added

- **Catalog logging — fork server logs to an attached catalog** [Enterprise]. Server logs can now be written to a `logs` table in an attached catalog (**file-based DuckDB**, **PostgreSQL**, or **DuckLake**) **in addition to** stdout/file — the same log stream, forked. Because logs are append-only, DuckLake is a fine fit here (no concurrent-UPDATE hazard, unlike instrumentation). The `logs` table mirrors the JSON log shape: the popular/queryable fields are promoted to typed columns (`log_time TIMESTAMPTZ`, `level`, `instance_id`, `cluster_id`, `session_id`, `username`, `role`, `peer`, `component`, `trace_id`, `span_id`, `pid`, `tid`, `source_file`, `source_line`, `func`, `message`) and everything else rides along in a `fields` JSON catch-all. There is no primary key (append-only); file-based DuckDB and PostgreSQL also get indexes on `log_time` and the common filter columns so time-range queries and time-based retention deletes stay fast. Writes happen on a dedicated connection + writer thread off the session path (bounded, drop-and-count queue; each batch in an explicit transaction — never auto-commit), so a slow/unreachable log catalog can never block or fail a client query. Like the instrumentation catalog, **the log catalog is system-managed: readable only by admins, never client-writable, and cannot be `DETACH`ed**. Backend is auto-detected (`duckdb_databases().type`). Gated on the `instrumentation` Enterprise license feature.
  - `--enable-catalog-logging` / `GIZMOSQL_ENABLE_CATALOG_LOGGING` — turn it on.
  - `--log-catalog` / `GIZMOSQL_LOG_CATALOG` — name of a pre-attached catalog (attach it via `--init-sql-commands`); empty selects a file-based default catalog (`_gizmosql_logs`) the server attaches itself.
  - `--log-schema` / `GIZMOSQL_LOG_SCHEMA` — schema within the catalog (default `main`).
  - `--log-catalog-db-path` / `GIZMOSQL_LOG_DB_PATH` — file path for the file-based default log database (defaults to `gizmosql_logs.db` beside the main database).
- **Cluster grouping ID** [Enterprise]. A new optional `--cluster-id` / `GIZMOSQL_CLUSTER_ID` (must be a UUID) tags a server as part of a logical cluster. When set, it is recorded on the instrumentation `instances` row (new nullable `cluster_id UUID` column), injected into **every** log entry (stdout/file and the catalog `logs` table), and exposed to SQL via a new **`GIZMOSQL_CURRENT_CLUSTER()`** pseudo-function (returns the cluster UUID, or `NULL` when unset) — the cluster analog of `GIZMOSQL_CURRENT_INSTANCE()`. This lets logs and instrumentation across all instances of a cluster be filtered and correlated together.
- **PostgreSQL-backed instrumentation** [Enterprise]. Instrumentation can now target a plain **PostgreSQL** database as its catalog — the recommended store for multi-instance deployments where several GizmoSQL servers share one instrumentation catalog. Attach a PostgreSQL database in `--init-sql-commands` (e.g. `ATTACH 'host=… dbname=… user=…' AS instr (TYPE postgres)`) and point `--instrumentation-catalog` at it; GizmoSQL **auto-detects** the backend (via `duckdb_databases().type`) and builds a full relational schema. Unlike DuckLake, PostgreSQL has row-level MVCC: each instance writes its own (disjoint) rows, so concurrent instances don't conflict and finalize/stop updates aren't lost to cross-instance contention (the DuckLake failure mode). The schema adds:
  - **Primary keys** on every table and **foreign keys with `ON DELETE CASCADE`**, so time-based retention pruning is a single delete on the parent — `DELETE FROM <catalog>.<schema>.instances WHERE stop_time < <cutoff>` cascades to that instance's sessions, statements, and executions.
  - **`CHECK` constraints** on the status columns (instance/session/execution status).
  - **Indexes** on every foreign-key column and every `TIMESTAMPTZ` lifecycle column (start/stop/created/execution/enqueue times), so time-range queries and time-based deletes stay fast.
  - PostgreSQL objects are created with native SQL via `postgres_execute()` (bypassing DuckDB's DDL translation), and the convenience views (`session_activity`, `active_sessions`, `session_stats`, `execution_details`) use portable SQL. JSON-valued columns (tags, query profile) are stored as `VARCHAR` on PostgreSQL — they hold JSON strings; query them with a `::json` cast.
- **Constraints and indexes restored for file-based instrumentation** [Enterprise]. The default file-based (DuckDB) instrumentation database now also gets primary keys, `CHECK` constraints on status columns, and indexes on foreign-key and `TIMESTAMPTZ` columns — all of which had been removed for DuckLake compatibility. (Foreign keys are added on PostgreSQL but **not** on DuckDB, which implements UPDATE as delete+insert and so rejects updates to a still-referenced parent row, which the instrumentation lifecycle performs.) New constraints apply to newly-created instrumentation databases; existing databases keep working and pick up the indexes.

### Deprecated

- **DuckLake-backed instrumentation** [Enterprise]. Using a DuckLake catalog as the instrumentation store is **deprecated**. DuckLake's table-level optimistic concurrency cannot reliably absorb concurrent UPDATEs from multiple GizmoSQL instances sharing one catalog: a finalize/stop UPDATE that loses a commit conflict is silently dropped, leaving records **permanently stuck** (e.g. an execution stuck at `status='executing'`, never cleaned up in shared-catalog mode). The server now logs a startup **WARNING** when instrumentation resolves to a DuckLake catalog. Use the default **file-based** instrumentation (single instance) or a plain **PostgreSQL** catalog (multiple instances) instead. DuckLake support remains until the upstream concurrent-UPDATE issue is resolved — data loss is not acceptable for observability.

### Fixed

- **Instrumentation writes are now explicitly transactional** [Enterprise]. The dedicated instrumentation writer thread previously executed each queued write under DuckDB's implicit auto-commit. It now wraps each write in an **explicit transaction** with a guaranteed `COMMIT` on success and `ROLLBACK` on error (RAII-guarded), giving a deterministic commit boundary so the writer connection is never left parked inside an open transaction between writes. This matters when instrumentation targets an external **DuckLake catalog backed by PostgreSQL**: a writer connection sitting on an unclosed catalog transaction across its idle wait can surface as a long-lived **`idle in transaction`** session on the PostgreSQL catalog. A failed write is isolated to its own transaction and never affects the writes queued around it.

## [1.27.1] - 2026-06-02

### Changed

- **Statement queuing now hard-errors at startup when configured without a license** [Enterprise]. Previously, setting `--max-concurrent-statements` (or `--max-queued-statements` / `--max-queue-wait`, or their `GIZMOSQL_*` env vars) on a server without the `statement_queue` Enterprise feature was **silently ignored** — the cap was unenforced (fail-open), so an operator who set a concurrency limit *for protection* was unknowingly running with unlimited concurrency, and could be taken down by exactly the overload the limit was meant to prevent. The server now **refuses to start with a clear error** in that case, consistent with the existing instrumentation / `--instance-tag` startup checks and with the runtime `SET GLOBAL gizmosql.max_concurrent_statements` license check (which already rejected unlicensed use). The per-statement admission path still fails open as a defensive backstop, so a license issue never *rejects an in-flight query*. **Upgrade note:** if you were running an unlicensed server with a statement-queue flag set (relying on the old silent fail-open), it will now fail to start — unset those flag(s) to run with unlimited concurrency, or use a license that includes the `statement_queue` feature.

## [1.27.0] - 2026-06-02

### Added

- **Query profile capture** [Enterprise]. Optionally persists DuckDB's per-query profile (operator tree, timings, cardinalities, memory) into the instrumentation `sql_executions.query_profile` column as **DuckDB's native profiling JSON** (verbatim `QueryProfiler::ToJSON()`), suitable for a future graphical query-plan viewer. DuckDB's profiler is per-connection and last-write-wins, but because GizmoSQL runs exactly one statement at a time per connection and never shares a session across users, the profile is harvested synchronously right after execution — before the next statement clobbers it. Opt-in and controllable at server / session / global scope, mirroring `query_log_level`:
  - `--capture-query-profile` / `GIZMOSQL_CAPTURE_QUERY_PROFILE` — server default: `off` (default), `standard` (per-operator profile), or `detailed` (also times each expression). Library parameter `capture_query_profile` on `RunFlightSQLServer()`.
  - `SET [SESSION|GLOBAL] gizmosql.capture_query_profile = 'off'|'standard'|'detailed'` — per-session override (any user) or live server-wide change (`SET GLOBAL`, admin only).
  - Overhead is opt-in: `off` adds nothing; `standard` is negligible on real workloads; `detailed` adds ~15–20% only on expression-heavy queries. Requires instrumentation + a valid Enterprise license (the `instrumentation` feature). The `query_profile` column is added via an additive `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migration and surfaced in the `session_activity` and `execution_details` views. Works on both the stable (DuckDB v1.5) and LTS (v1.4) channels.
- **Statement queuing** [Enterprise]. Caps the number of SQL statements executing concurrently; statements beyond the limit queue (block) until a slot frees. Queued statements are admitted in strict **FIFO** order — when a slot opens it goes to the longest-waiting statement and fresh arrivals never barge ahead, so no statement is starved under sustained load. To JDBC/ADBC/ODBC clients a queued statement is indistinguishable from a slow-executing one, so no client changes are needed. Internal/metadata introspection queries are exempt (they never consume a slot). Enforcement requires a valid Enterprise license with the `statement_queue` feature; without it the limits are unenforced — the feature **fails open**, so a missing or expired license never blocks queries. All knobs are available as CLI flags, env vars (for container deployments), and `RunFlightSQLServer()` library parameters:
  - `--max-concurrent-statements` / `GIZMOSQL_MAX_CONCURRENT_STATEMENTS` — execution slots. `0` = unlimited (default; queue disabled).
  - `--max-queued-statements` / `GIZMOSQL_MAX_QUEUED_STATEMENTS` — waiter bound; statements beyond it are rejected with a retriable Flight `UNAVAILABLE` rather than queued. `-1` (default) auto-sizes to 8× the concurrency limit; `0` = unbounded.
  - `--max-queue-wait` / `GIZMOSQL_MAX_QUEUE_WAIT` — seconds a statement may wait before being rejected. `-1` (default) = 300s; `0` = wait indefinitely.
  - `--admin-bypass-queue-default` / `GIZMOSQL_ADMIN_BYPASS_QUEUE_DEFAULT` (default `true`) — admin-role sessions bypass the queue by default so diagnostics and `KILL SESSION` are never stranded behind a saturated queue. Any session can run `SET SESSION gizmosql.bypass_queue = <bool>` (only admins may enable it).
  - `KILL SESSION` interrupts a **queued** statement immediately: the admission wait is interruptible, so a killed statement abandons the queue at once (without waiting for a slot it will never use) and is recorded as `cancelled`, rather than running to completion once a slot frees.
- **`--memory-limit` / `GIZMOSQL_MEMORY_LIMIT`.** Passthrough to DuckDB's `memory_limit` setting (e.g. `8GB`, `75%`), now a first-class server flag/env var instead of only reachable via `--init-sql-commands`. Empty (the default) leaves DuckDB's built-in default in place (80% of physical RAM). Global/instance-wide; ignored for the SQLite backend. Pairs with statement queuing — set the memory budget, then size concurrency to fit it.
- **gRPC server keepalive.** The Flight gRPC server now sends periodic keepalive pings and is permissive about client-initiated pings, so long-lived or queued streams aren't dropped by a load-balancer/proxy idle timeout (AWS NLB ~350s, Azure ~4min).
- **`SET gizmosql.*` accepts unquoted boolean keywords.** `SET ... = true` / `= false` now work (DuckDB's grammar represents these as a cast expression rather than a constant); previously only quoted/integer constants were accepted.
- **Runtime `SET GLOBAL` for the statement-queue limits.** Admins can tune `gizmosql.max_concurrent_statements`, `gizmosql.max_queued_statements`, and `gizmosql.max_queue_wait` on a live server (no restart) via `SET GLOBAL` — plus `SET SESSION gizmosql.max_queue_wait` for a per-session override. Changes are in-memory and revert to the configured/env value on restart. Internally, all `SET gizmosql.*` handling now dispatches through a single settings registry (one descriptor per setting; centralized license/scope/admin checks).
- **`gizmosql_settings()` table function.** Lists every `gizmosql.*` setting with its current effective value, session/global values, scope, input type, default, env var, enterprise flag, and description — the GizmoSQL analog of DuckDB's `duckdb_settings()`. Composable like any relation (`SELECT … FROM gizmosql_settings() WHERE …`); the row values are passed as bound parameters rather than interpolated into SQL.
- **Statement-queue observability in instrumentation** [Enterprise]. `sql_executions` now records the queued lifecycle: a statement that waits for a slot is written with `status='queued'` and an `enqueue_time`, then transitions to `executing` once admitted. The `execution_details` view exposes `enqueue_time` and a computed `queue_wait_ms`. Statements interrupted by `KILL SESSION` are recorded as `cancelled` (distinct from `error`). The schema change is an additive `ALTER TABLE … ADD COLUMN IF NOT EXISTS` migration, and stale-instance recovery now also resolves orphaned `queued` rows. This is the data backbone for an Oracle-style SQL-monitor view of in-flight / queued / completed queries.
- **Wildcard catalog patterns in catalog-level access control** [Enterprise]. The `catalog` field of each `catalog_access` token rule now supports AWS IAM-style glob matching: `*` matches any sequence of characters (including none) and `?` matches exactly one character. A pattern with no wildcards still matches exactly (case-sensitive), so existing literal-name rules and the bare `*` wildcard are unchanged. This lets a single rule cover a family of catalogs that share a naming convention — e.g. `{"catalog": "prod_*", "access": "write"}` grants write to `prod_sales`, `prod_finance`, etc. Patterns apply everywhere catalog access is evaluated, including metadata visibility filtering (`SHOW DATABASES`, `information_schema.*`, `duckdb_*()` functions, and Flight SQL metadata RPCs). No token-generator or client changes are required — wildcards are ordinary strings in the existing `catalog_access` claim.

### Changed

- **Hardened the Docker images to reduce vulnerability surface (Docker Scout grade).** The full ("fat") image (`Dockerfile.ci`) is now multi-stage and based on `python:3.12-slim-bookworm` instead of the full `python:3.12.11` image. The C/C++ build toolchain that was previously installed at runtime but never used (`build-essential`, `gcc`, `git`, `cmake`, `ninja-build`, `automake`, `libboost-all-dev`, `vim`, etc. — the server/client binaries are prebuilt in CI and copied in) has been removed. The AWS CLI v2, azcopy, and DuckDB CLI convenience utilities are now installed in a throwaway builder stage and copied into the final image, so their download tooling never ships. The slim image (`Dockerfile-slim.ci`) drops the unused `zip` package, adds `ca-certificates`, and uses `--no-install-recommends`. Convenience utilities (Python, AWS CLI, Azure CLI, azcopy, DuckDB CLI, TLS cert generation) remain available in the full image.

### Fixed

- **`KILL SESSION` is now idempotent across Flight's two-phase execution.** A Flight SQL statement is created twice — once when `GetFlightInfo` computes the result schema and again when `DoGet` streams the result — so the `KILL SESSION` side effect ran twice. The first pass removed the target session; the second pass then failed with a spurious `Session not found`, surfacing to any client that *reads* the kill result (`Execute` followed by `DoGet`) as a `KeyError`. The handler now treats an already-killed target as a successful no-op (distinguished from a never-existed session via the killed-session set), so killing a session — including one with a statement waiting in the [statement queue](statement_queuing.md) — completes cleanly. Killing a genuinely unknown session id still returns `Session not found`.
- **`GetSessionOptions` no longer lazily creates a session, so it can be used as a liveness probe.** GizmoSQL sessions are created on demand by whichever Flight SQL RPC arrives first for a given JWT `session_id`; the catalog is only applied when the client sends `SetSessionOptions` (`USE "<catalog>"`), which drivers do exactly once right after the Handshake. If a pooled connection's server-side session disappeared (e.g. the connection re-routed to a replica that never saw the session), the next RPC would silently create a *new* session in the default (`memory`) catalog and every query would fail with a catalog error (`Table ... does not exist` / `expected catalog X but got memory`). `GetSessionOptions` now performs a **non-creating** lookup and returns an `Unauthenticated` Flight error for an evicted/unknown session instead of materialising one in the wrong catalog. This lets a client's `Connection.isValid()` probe the session and recycle a stale pooled connection, forcing a fresh Handshake + `SetSessionOptions` that lands the replacement session in the correct catalog. All data-plane RPCs keep their existing lazy-create behavior, so clients that never send `SetSessionOptions` (e.g. ADBC using the default catalog) are unaffected. Added `tests/integration/test_session_options_probe.cpp` as a regression guard.

### Security

- **Docker images now ship SLSA provenance + SBOM attestations.** The CI image build switched from per-platform tag pushes merged with `Noelware/docker-manifest-action` to the canonical Buildx digest-merge pattern: each platform is built `push-by-digest` with `provenance: mode=max` and `sbom: true`, then the per-platform digests are combined into the multi-arch manifest via `docker buildx imagetools create` (which preserves the attestations as referrers). This satisfies Docker Scout's supply-chain attestation policy for the `latest`/`latest-slim` (and versioned, plus LTS) tags on both Docker Hub and GHCR. (Previously the build set `provenance: false` because the old manifest-merge tool could not carry attestations.)

## [1.26.3] - 2026-05-26

### Changed

- **Instrumentation timing columns are now `TIMESTAMP WITH TIME ZONE` (TIMESTAMPTZ)** instead of naive `TIMESTAMP`. This affects `instances.start_time`/`stop_time`, `sessions.start_time`/`stop_time`, `sql_statements.created_time`, and `sql_executions.execution_start_time`/`execution_end_time`. Filters like `WHERE start_time > now() - INTERVAL '1 hour'` now work without an explicit cast, since DuckDB's `now()` is itself tz-aware. **Migration is automatic** for both file-based DuckDB and DuckLake-backed instrumentation: on startup, GizmoSQL drops the dependent views, stages each legacy table into a TEMP table with timing columns cast `<col> AT TIME ZONE 'UTC'`, drops the legacy tables, lets `InitializeSchema` recreate fresh tz-aware tables and views, and then restores the staged rows via `INSERT INTO ... BY NAME`. Historical rows are preserved and interpreted as UTC, which matches how `now()` produced them when the prior server stripped tz info on insert. (We use stage/drop/restore rather than `ALTER COLUMN SET DATA TYPE ... USING` because DuckLake does not support type changes that use an expression.)

## [1.26.2] - 2026-05-20

### Changed

- **Upgraded DuckDB from v1.5.2 to v1.5.3** (stable channel). DuckDB 1.5.3 is a bugfix release with 120+ fixes for issues discovered after v1.5.2. The iOS extension pins (`ducklake`, `httpfs`, `postgres_scanner`) were re-synced to match DuckDB's own pins for the v1.5.3 release.
- **Upgraded SQLite from 3.52.0 to 3.53.1.** Upstream SQLite 3.52.0 was withdrawn and superseded by 3.53.0 (major release, 2026-04-09) and the follow-up bugfix 3.53.1 (2026-05-05).

## [1.26.0] - 2026-05-09

### Added

- **`--storage-version` / `GIZMOSQL_STORAGE_VERSION`.** New server option that pins the DuckDB storage format version (e.g. `latest`, `v1.4.0`, `v1.2.0`) for newly created database files, mirroring `duckdb -storage_version <ver>`. Maps directly onto DuckDB's `serialization_compatibility` `DBConfig` option (`SerializationCompatibility::FromString`). Useful for forcing a newer on-disk format so newer DuckDB clients can attach at their newest level, or capping at an older version for cross-version compatibility. Available via the CLI flag, the env var (handy for container deployments), and the `RunFlightSQLServer()` library API. Ignored for the SQLite backend. Default: empty (DuckDB's built-in default applies).

## [1.25.2] - 2026-05-08

### Fixed

- **Server now starts in read-only mode (`--readonly` / `-O`).** Previously, startup failed with `Catalog Error: Cannot launch in-memory database in read-only mode!` because the system-catalog `ATTACH ':memory:' AS _gizmosql_system` inherited the parent database's `READ_ONLY` access mode. Both the system-catalog attach and the (Enterprise) instrumentation database attach now specify `(READ_WRITE)` explicitly, so server-managed metadata and instrumentation continue to work even when the user's main database is opened read-only. Added `tests/integration/test_read_only_mode.cpp` as a regression guard.

## [1.25.1] - 2026-05-07

### Fixed

- **`GIZMOSQL_VERSION()` SQL function and `gizmosql_client` `--version` / `.about` / `.info` / interactive shell banner now reflect the `-LTS` suffix on LTS builds.** All four sites still used the raw `PROJECT_VERSION` git tag and silently dropped the channel marker. Switched to `GIZMOSQL_SERVER_VERSION`, which already had the channel-aware suffix logic. Stable builds are unchanged. Updated the LTS Channel docs to enumerate all four reporting surfaces.

## [1.25.0] - 2026-05-07

### Changed

- **Client now also accepts `GIZMOSQL_USERNAME` (the server's env var) as a username fallback.** The client previously only honored `GIZMOSQL_USER` while the server only honored `GIZMOSQL_USERNAME`, forcing operators who export both to set two near-identical names. The client still prefers `GIZMOSQL_USER` when both are defined (preserves prior behavior), and an explicit `--username` still wins over both.

### Added

- **GizmoSQL LTS channel.** New CMake option `GIZMOSQL_DUCKDB_CHANNEL=stable|lts` builds GizmoSQL against either the latest DuckDB release (stable, default — currently `v1.5.2`) or the most recent DuckDB LTS release (currently `v1.4.4`). The LTS channel produces parallel artifacts that coexist with stable: executables `gizmosql_server_lts` / `gizmosql_client_lts`, ZIP/MSI files suffixed `-lts` / `_lts`, Docker image `gizmodata/gizmosql-lts:<ver>` (full + slim, both architectures), and a `gizmosql-lts` Homebrew formula in the existing `gizmodata/tap`. The server's startup banner identifies the channel (`with engine: DuckDB (LTS channel — DuckDB v1.4.4)`), and `GIZMOSQL_SERVER_VERSION` carries a `-LTS` suffix on LTS builds. Stable artifacts and image tags are unchanged. iOS builds remain stable-only. CI exercises the matrix on Linux (amd64/arm64), macOS (arm64), and Windows (x64).

## [1.24.0] - 2026-05-06

### Added

- **`--session-log-level` / `GIZMOSQL_SESSION_LOG_LEVEL`.** New server option that controls the severity threshold for client session lifecycle messages (`session_create`, `session_close`). Mirrors `--auth-log-level`: server-wide, set at startup, defers to the global `--log-level`. Lets operators silence per-session create/close chatter (set to `warn`) without losing other INFO logs, or surface them only when running at `--log-level debug`. Available via the CLI flag, the env var, and the `RunFlightSQLServer()` library API. Default: `info`.

## [1.23.0] - 2026-05-06

### Added

- **`--max-metadata-size` / `GIZMOSQL_MAX_METADATA_SIZE`.** New server option exposing gRPC's `GRPC_ARG_MAX_METADATA_SIZE`. gRPC's default of ~8 KB rejects calls whose total HTTP/2 header list exceeds the limit, surfacing as `Http2Exception$HeaderListSizeException: Header size exceeded max allowed size (8192)` on Apache Arrow Flight SQL JDBC clients that forward unrecognized JDBC URL parameters as gRPC headers. SQL itself is unaffected (it's sent in the request body), so the symptom was misleading. Default is `0` (= keep gRPC's default); set to e.g. `65536` if your clients legitimately ship large per-call metadata (extra JDBC URL parameters, large bearer tokens, accumulated cookies, proxy-injected trace headers).

### Fixed

- **`GetTables(include_schema=true)` now emits `ARROW:FLIGHT:SQL:TYPE_NAME` per column.** ADBC Flight SQL clients (e.g. the Apache `libadbc_driver_flightsql` driver used by Power BI's `Adbc.DataSource()` M function) populate `xdbc_type_name` from this Flight SQL standard metadata key. Previously it was unset, so `adbc_get_objects(depth='columns')` returned `xdbc_type_name=None` for every column, breaking type resolution in Power BI's Power Query connector. The value is taken verbatim from DuckDB's `duckdb_columns().data_type`, with parameter / decoration suffixes stripped (`DECIMAL(18,3)` → `DECIMAL`, `INTEGER[]` → `INTEGER`, `STRUCT(...)` → `STRUCT`).

## [1.22.5] - 2026-05-04

### Changed

- **Client now refuses `--tls-skip-verify` together with `--mtls-cert`/`--mtls-key`.** Arrow Flight's gRPC transport silently drops the client certificate when `disable_server_verification` is set (it routes through `TlsChannelCredentialsOptions`, which doesn't read `cert_chain`/`private_key`), so the server rejected the handshake with a cryptic `peer did not return a certificate`. The client now errors out up-front with an actionable message pointing users at `--tls-roots`.

### Documentation

- **`docs/security.md` and `docs/client.md`**: documented the `--tls-skip-verify` + mTLS incompatibility, including the self-signed-cert workaround (`--tls-roots cert.pem` works because the cert is its own CA).

## [1.22.4] - 2026-04-27

### Fixed

- **iOS app: Browser screen now correctly previews tables in non-default catalogs.** `loadPreview`, `loadColumns`, and `dropTable` in `BrowserView.swift` were building 2-part identifiers `"schema"."table"`, which only resolve against the current/default catalog. DuckDB returned `Catalog Error: Table with name "..." does not exist` for any other attached catalog (e.g. an additional ATTACHed DuckLake or DuckDB file). The three sites now build fully-qualified 3-part `"catalog"."schema"."table"` identifiers with proper double-quote escaping for each part.
- **iOS app: `_gizmosql_system` catalog is now hidden from the catalog/table browser and `.tables` / `.catalogs` dot-commands.** It's a server-managed in-memory catalog with metadata helper views (`gizmosql_index_info`, `gizmosql_view_definition`), not user data — same rationale as hiding `information_schema`. The Browser screen filters it out of `duckdb_databases()`; the Query screen's `.tables` and `.catalogs` add `table_catalog != '_gizmosql_system'` to their `WHERE`.

### Changed

- **Server-side Flight SQL `GetTables` now returns tables from every attached catalog when the caller doesn't specify one.** Previously, an absent `catalog` filter implicitly restricted results to `CURRENT_DATABASE()`, which hid tables in any other ATTACHed catalog — surprising users who came from DuckDB's CLI (whose `.tables` shows all attached catalogs by default). The CLI client's `.tables`, JDBC `DatabaseMetaData.getTables(null, ...)`, and any other Flight SQL client all benefit. `_gizmosql_system` is still excluded server-side regardless, since it's not user data. Explicit catalog filters are unchanged — `LIKE` still scopes to whatever the caller asked for.

## [1.22.3] - 2026-04-27

### Fixed

- **iOS CI now builds against the iOS 26 SDK.** The v1.22.2 TestFlight upload triggered Apple warning ITMS-90725 because it was compiled with Xcode 16.2 / iOS 18.2 SDK; Apple requires the iOS 26 SDK for all App Store Connect uploads starting 2026-04-28. The `build-ios` job now runs on `macos-26` with `xcode-version: latest-stable` (resolves to Xcode 26.x), and the deps cache is bumped to v5 so existing iOS 18.2-compiled artifacts aren't reused. iOS deployment target stays at 17.0 — that's min-supported OS, not SDK version.

### Added

- **`create-release` now also depends on `build-ios`.** A failing iOS build now blocks the GitHub release from being cut, just like the existing macOS / Linux / Windows / MSI gates.

## [1.22.2] - 2026-04-27

### Security

- **`_gizmosql_system` views now respect per-catalog read permissions (Enterprise).** When a token's `catalog_access` rules deny a catalog, queries against `_gizmosql_system.main.gizmosql_index_info` and `_gizmosql_system.main.gizmosql_view_definition` no longer leak rows for that catalog. The visibility filter that already rewrites `information_schema.*` and `duckdb_*()` references now also wraps the system-catalog views with a `WHERE "TABLE_CAT" IN (...)` filter, mirroring the existing JDBC-shaped column name. Users with no `catalog_access` rules (or a wildcard read) are unaffected.
- **`_gizmosql_system` catalog is now write-protected.** The system catalog hosts server-managed metadata helper views (`gizmosql_index_info`, `gizmosql_view_definition`); previously a client could `CREATE`/`DROP`/`ALTER` inside it. A new check in `DuckDBStatement` rejects any statement that would modify `_gizmosql_system` with `Access denied: The GizmoSQL system catalog '_gizmosql_system' is read-only.` Enforced in both Core and Enterprise builds, regardless of role or `catalog_access` token claims. Read access is unchanged — every authenticated user can still query the helper views. Mirrors the existing `_gizmosql_instr` pattern.

### Changed

- **`_gizmosql_system` init SQL extracted to `src/common/system_catalog.cpp`** so it can grow over time without bloating `gizmosql_library.cpp`. Public catalog name and `IsSystemCatalog()` helper now live in `src/common/include/system_catalog.h`.

### Fixed

- **iOS builds now report `os_platform="ios"` and `os_name="iOS X.Y"` in instrumentation/logs.** `uname()` returns `Darwin` as `sysname` on both macOS and iOS (same XNU kernel), so iOS builds were previously identifying themselves as `darwin` / `macOS`. `GetSystemInfo()` now branches on `TARGET_OS_IOS` from `<TargetConditionals.h>` to emit the correct platform string.

### Added

- **CI: iOS build & sign job in `.github/workflows/ci.yml`.** Cross-compiles iOS dependencies via `ios/scripts/build-ios-libs.sh` (with caching keyed on the DuckDB pin, extension pins, and build script), generates the Xcode project with `xcodegen`, signs with an imported Apple Distribution certificate + provisioning profile, and exports an app-store IPA. TestFlight upload via `xcrun altool` is gated on `refs/tags/v*` pushes; the rest of the pipeline currently runs on every push so the signing setup can be exercised between releases.

## [1.22.1] - 2026-04-24

### Added

- **`gizmosql_index_info` catalog view now unions `PRIMARY KEY` + `UNIQUE` constraints.** DuckDB stores PK/UNIQUE constraints in `duckdb_constraints()` but not `duckdb_indexes()`, so the previous version of the view only exposed user-created `CREATE INDEX` indexes. This broke DBeaver's Data Editor — it needs a visible unique index to enable row-level editing — and hid unique constraints from the Indexes folder. The view now `UNION ALL`s both sources with `NON_UNIQUE=false` for PK-backed and UNIQUE-backed indexes.
- **`GetTables` schema now carries real per-column metadata.** The Arrow schema returned via Flight SQL `GetTables` with `include_schema=true` previously came straight from `SELECT * FROM t LIMIT 0`, which marks every field nullable and carries no DDL metadata. `DuckDBTablesWithSchemaBatchReader` now also queries `duckdb_columns()` per table (with proper `?` parameter binding) and applies:
  - `Field.nullable` ← real NOT NULL constraint (fixes DBeaver's column-browser Nullable checkbox).
  - Metadata `ARROW:FLIGHT:SQL:REMARKS` ← column comment (populates JDBC `REMARKS`).
  - Metadata `ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT` ← `"1"` when the default starts with `nextval(`, else `"0"` (populates JDBC `IS_AUTOINCREMENT`).
  - Metadata `GIZMOSQL:COLUMN_DEFAULT` ← the column's default expression, exposed through the GizmoSQL JDBC driver v1.6.1+ as JDBC `COLUMN_DEF`.
  Fully backward-compatible: all three metadata keys are ignored by old clients; the standard Flight SQL JDBC driver picks up `REMARKS` + `IS_AUTO_INCREMENT` for free (they are upstream Apache Arrow Flight SQL spec keys).
- **README: QGIS Plugin (qgizmosql) listed in Extensions & Integrations.**

### Testing

- New `tests/test_v1_22_1_features.py` (wired into CI alongside the existing Python integration suite) covers the two server-level changes end-to-end via the ADBC driver: `gizmosql_index_info` now contains PK + UNIQUE rows; the Flight SQL table schema carries the new nullable/remarks/auto-increment/default metadata.
- JDBC-side regression coverage lives in the gizmosql-jdbc-driver repo (`GizmoSqlIntegrationIT.testGetColumnsEnrichment` and `testDecimalBindWithNonBigDecimalInput`), exercised against `gizmodata/gizmosql:latest` on every JDBC CI run.

## [1.22.0] - 2026-04-24

### Added

- **`_gizmosql_system` system catalog (DuckDB backend).** Server init SQL now `ATTACH ':memory:' AS _gizmosql_system` and creates two JDBC-shaped metadata helper views inside it, so Flight SQL clients can recover metadata that the Flight SQL protocol itself does not expose:
  - `_gizmosql_system.main.gizmosql_index_info` — one row per `(index, column)`, columns exactly match JDBC `DatabaseMetaData.getIndexInfo()` (`TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `NON_UNIQUE`, `INDEX_QUALIFIER`, `INDEX_NAME`, `TYPE`, `ORDINAL_POSITION`, `COLUMN_NAME`, `ASC_OR_DESC`, `CARDINALITY`, `PAGES`, `FILTER_CONDITION`). Wraps DuckDB's `duckdb_indexes()` and unrolls the `expressions` list into one row per column.
  - `_gizmosql_system.main.gizmosql_view_definition` — `(TABLE_CAT, TABLE_SCHEM, TABLE_NAME, VIEW_DEFINITION)` keyed view over `duckdb_views()` for retrieving CREATE VIEW DDL text.
  Works for stock Flight SQL clients (plain SQL over either view), and is the backend for the matching overrides shipped in the GizmoSQL JDBC driver v1.6.0 (`DatabaseMetaData.getIndexInfo()`, `ArrowFlightConnection.getViewDefinition(...)`) and for DBeaver's GizmoSQL MetaModel. SQLite backend is currently unaffected — the views are DuckDB-specific.

### Fixed

- **DECIMAL prepared-statement parameter binding aborts the server process.** Prior to v1.22.0, executing a prepared `INSERT` / `UPDATE` bound to a DuckDB `DECIMAL` column could trigger Arrow's `ValidateDecimalPrecision` check and SIGABRT the `gizmosql_server` process (exit 134). Surface symptom on the client was a raw `UNAVAILABLE: Network closed for unknown reason` / gRPC channel termination — the server process just disappeared mid-query. Three distinct bugs in `duckdb_statement.cpp::GetDataTypeFromDuckDbType` were fixed together: the DECIMAL arm never populated `width`/`scale` from the DuckDB type (hardcoded 0), the `arrow::smallest_decimal(...)` call had its arguments flipped (`(scale, width)` instead of `(width, scale)`), and returning a `Decimal32`/`Decimal64` type is not accepted by the Arrow Java JDBC client (which only supports `Decimal128` / `Decimal256`). The server now emits `Decimal128(width, scale)` for precisions 1–38, `Decimal256(width, scale)` for precision > 38, and falls back to `Decimal256(38, scale)` when DuckDB reports `width == 0` (unresolved parameter placeholders in statements like `INSERT INTO t(decimal_col) VALUES (?)`). Regression coverage lives in the GizmoSQL JDBC driver repo's `GizmoSqlIntegrationIT.testDecimalParameterRoundTrip`, exercised against `gizmodata/gizmosql:latest` on every JDBC CI run.

## [1.21.3] - 2026-04-22

### Fixed

- **Bulk ingest with `temporary=True` (issue [#158](https://github.com/gizmodata/gizmosql/issues/158))**: Repeated `adbc_ingest(..., temporary=True)` calls (or any Flight SQL `CommandStatementIngest` with `temporary=true`) no longer fail with `Catalog Error: Table with name "..." already exists!` (for `create_append`/`replace`) or `Table: "..." does not exist` (for `create` then `append`). The server's table-existence check previously only consulted the current database catalog, so temporary tables — which DuckDB stores in the implicit `temp.main` catalog — were invisible to the lookup. `TableExists()` now scopes to `temp.main` when the ingest is temporary. Thanks again to @fromm1990 for the report and reproducer.

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
