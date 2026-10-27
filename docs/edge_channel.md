# GizmoSQL Edge Channel

> ⚠️ **EXPERIMENTAL — NOT FOR PRODUCTION WORKLOADS.** The edge channel builds
> GizmoSQL against the **newest DuckDB major** (today DuckDB **v2.0.0**). GizmoSQL's
> support for it is new: behavior and extension compatibility can change or
> break from one GizmoSQL release to the next, and the database files it
> creates cannot be opened by the stable or LTS channel. Use the **stable** or
> **LTS** channel for anything you depend on.

The edge channel exists so you can try GizmoSQL on the newest DuckDB major
*before* the stable channel moves to it — to test your workloads, clients and
data against it, and to use new DuckDB features early. It is the third of GizmoSQL's release channels
(see [LTS channel](lts_channel.md) for the overview):

| Channel    | DuckDB tracked                                         | Intended for |
|------------|--------------------------------------------------------|--------------|
| **Stable** | Latest DuckDB minor release (e.g. `v1.5.6`)            | Production |
| **LTS**    | Most recent DuckDB LTS release (e.g. `v1.4.5`)         | Production that favors a slower-moving engine |
| **Edge**   | Newest DuckDB major (`v2.0.0`)                         | **Evaluation and testing only** |

All three channels share the same GizmoSQL code, flags, API and protocol; edge
only changes which DuckDB is linked in.

## What is different on edge

Most of the difference is DuckDB 2.0 itself. Things you will notice through
GizmoSQL:

- **Storage format.** New database files are created in DuckDB 2.0's storage
  format (`v2.0.0+`), which **DuckDB 1.x — and therefore the stable and LTS
  channels — cannot open**. Existing 1.x files open fine and keep their format.
  To create files that 1.x can still read, start the server with
  `--storage-version v1.5.0` (or older).
- **SQL behavior changes** (DuckDB 2.0):

  | SQL | Stable (DuckDB 1.5) | Edge (DuckDB 2.0) |
  |---|---|---|
  | `SELECT 1 // 0`, `SELECT 7 % 0` | `NULL` | error: *Division by zero … Use TRY(...)* |
  | `SELECT 'abc' ~ 'b'` | `false` (full-string match) | `true` (partial match) |
  | Column type of `SELECT NULL` | `INTEGER` | `"NULL"` |

- **VARIANT over Arrow.** `VARIANT` columns travel as Arrow's canonical
  `arrow.parquet.variant` extension type (in results, bulk ingest and
  prepared-statement parameters). On stable they export as opaque binary.
- **Query profiles.** `sql_executions.query_profile` uses DuckDB 2.0's nested
  profile layout — total query time is `$.query.total_time` rather than
  `$.latency` (see [Session instrumentation](session_instrumentation.md)).
- **Extensions** come from DuckDB's repository for that exact version
  (`extensions.duckdb.org/v2.0.0/…`); not every community extension is
  published for it yet.

## Telling an edge build apart

An edge build marks itself everywhere it reports its version:

```text
$ gizmosql_server_edge --version
GizmoSQL Server CLI: v1.42.2-EDGE

$ gizmosql_client_edge --version
GizmoSQL Client v1.42.2-EDGE

$ gizmosql_server_edge --print-duckdb-version
v2.0.0
```

At startup it logs the channel and a warning:

```text
INFO ... GizmoSQL server version: v1.42.2-EDGE - with engine: DuckDB (edge channel — DuckDB v2.0.0) - will listen on grpc+tcp://0.0.0.0:31337
WARN ... This is an EDGE channel build of GizmoSQL, on the newest DuckDB major (v2.0.0). It is EXPERIMENTAL and NOT meant for production workloads: behavior and extensions can change or break between releases, and database files it creates cannot be opened by the stable or LTS channel. Use the stable or LTS channel for production.
```

And from SQL:

```sql
SELECT GIZMOSQL_VERSION();
-- v1.42.2-EDGE
```

## Artifacts

| Type          | Edge                                              |
|---------------|---------------------------------------------------|
| Server binary | `gizmosql_server_edge`                            |
| Client binary | `gizmosql_client_edge`                            |
| CLI zip       | `gizmosql_cli_<os>_<arch>_edge.zip` *(Linux amd64/arm64, macOS arm64, Windows amd64/arm64)* |
| Windows MSI   | `GizmoSQL-<arch>-edge.msi` *(amd64, arm64)* — installs as "GizmoSQL Edge (experimental)" in `C:\Program Files\GizmoSQL Edge`, side by side with stable and LTS |
| Docker (Hub)  | `gizmodata/gizmosql-edge:<ver>` *(+ `-slim`)*     |
| Docker (GHCR) | `ghcr.io/gizmodata/gizmosql-edge:<ver>`           |
| Homebrew      | `gizmodata/tap` → `gizmosql-edge`                 |

The iOS app is stable-only.

## Trying it

### Docker

```bash
docker run --name gizmosql-edge \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull always \
           gizmodata/gizmosql-edge:latest
```

The image's `duckdb` CLI is the matching DuckDB release (v2.0.0), so it can
open the v2.0-format files the server creates.

### Homebrew (macOS / Linux)

```bash
brew tap gizmodata/tap         # once (skip if already tapped)
brew trust gizmodata/tap       # once (Homebrew 6.0+; skip on older versions)
brew install gizmosql-edge     # installs gizmosql_server_edge / gizmosql_client_edge
```

It coexists with the `gizmosql` and `gizmosql-lts` formulas.

### Direct download

Download the `_edge` zip or the `-edge` MSI for your platform from the
[GitHub Releases page](https://github.com/gizmodata/gizmosql/releases).

### Building from source

```bash
cmake -B build -DGIZMOSQL_DUCKDB_CHANNEL=edge
cmake --build build
```

The DuckDB version is `DUCKDB_EDGE_VERSION` (`v2.0.0`), which is also the
string runtime `INSTALL`/`LOAD` looks extensions up by. While edge tracks an
untagged DuckDB pre-release, it is pinned by commit through
`DUCKDB_EDGE_GIT_REF` together with the exact version string DuckDB published
that build as.

## Moving a deployment between edge and stable

Going **to** edge is a binary/image swap, like switching to LTS. Coming **back**
is only that simple if the edge server never created a database file in the
DuckDB 2.0 format: files it created (including the instrumentation and catalog
logging databases) cannot be opened by stable or LTS. Keep edge on copies of
your data, or start it with `--storage-version v1.5.0`.
