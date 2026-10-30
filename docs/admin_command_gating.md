# Admin Command Gating

GizmoSQL restricts a fixed set of dangerous, instance-level SQL commands to
sessions whose role is **`admin`**. This is a **rudimentary, always-on guardrail**
that ships ahead of the full role-based access control (RBAC) model planned for
**GizmoSQL 2.0**. Its job is narrow but important: stop a *non-admin* user from
reaching outside the database or into another tenant's catalog — detaching
catalogs, reading the local filesystem, reaching object storage or the network
with the server's credentials, running SQL directly on an attached database,
attaching databases, installing extensions, or changing global server settings.

!!! info "Interim control, not the full model"
    This is intentionally a small, conservative gate. The complete
    authorization model (object- and column-level grants,
    `GRANT`/`REVOKE` DDL, multi-tenant policies) is the GizmoSQL 2.0 cornerstone.
    The known limitations below are addressed properly there.

!!! tip "Admins are protected from reading host secrets too"
    This gate only applies to non-admin roles. Independently of it, the
    [sensitive-path guard](security.md#sensitive-path-guard) keeps credential and
    system files on the server host (DuckDB secrets, `~/.ssh`, `~/.aws`,
    `/etc/shadow`, the server's TLS key, ...) out of reach of **every** session,
    including admins, and stored secrets stay redacted for everyone.

## Who is affected

The gate keys off the session **role**:

| Authentication | Role | Affected by the gate? |
|----------------|------|-----------------------|
| Username / password (Basic auth) | always `admin` | **No** — basic-auth users are admins |
| Token (JWT) with `role: admin` | `admin` | **No** |
| Token (JWT) with a non-admin `role` (e.g. `analyst`) | that role | **Yes** — gated |
| Token with no `role` claim + `--token-default-role` set to a non-admin role | that role | **Yes** — gated |

In other words: a deployment that only uses username/password auth is unchanged —
every such user is an admin. The gate matters specifically for **token / SSO
deployments that mint non-admin roles**. Internal/system queries that GizmoSQL
runs on your behalf (metadata, instrumentation) are always exempt.

To run a multi-tenant or least-privilege setup, issue tokens whose `role` claim
is something other than `admin` (or set `--token-default-role` to a non-admin
role for IdP tokens that lack the claim).

## What is gated

For **non-admin** sessions, the following are rejected with a Flight
`UNAVAILABLE`/permission error (`"Permission denied: GizmoSQL blocked
<command>, which requires the 'admin' role …"`). The same applies when the
command is wrapped in `EXPLAIN` or `EXPLAIN ANALYZE` (which executes it) or
staged with `PREPARE`:

| Category | Examples |
|----------|----------|
| **ATTACH / DETACH** (any database) | `ATTACH 'lake.db' AS lake`, `DETACH my_ducklake` |
| **SET GLOBAL / RESET GLOBAL** (any setting) | `SET GLOBAL memory_limit='8GB'`, `RESET GLOBAL …` |
| **Dangerous bare `SET`** of a global-only setting | `SET memory_limit='100GB'`, `SET threads=1`, `SET enable_external_access=false`, `SET temp_directory='…'` |
| **INSTALL / LOAD** extensions | `INSTALL httpfs`, `LOAD spatial`, `FORCE INSTALL …` |
| **CHECKPOINT** | `CHECKPOINT`, `FORCE CHECKPOINT` |
| **EXPORT DATABASE / IMPORT DATABASE** (any destination) | `EXPORT DATABASE '/tmp/dump'`, `EXPORT DATABASE 's3://bucket/dump'`, `IMPORT DATABASE '/tmp/dump'` |
| **COPY** to/from a **local** file, or a **remote URL** (see [Local vs. remote](#local-vs-remote)) — including `COPY … TO` as a CTE body ([edge channel](edge_channel.md), DuckDB 2.0) | `COPY t TO '/tmp/x.csv'`, `COPY t FROM '/etc/passwd'`, `COPY t TO 'gs://bucket/x.parquet'`, `WITH c AS (COPY t TO '/tmp/x.csv') SELECT 1` |
| **`read_*` / `glob` / `sniff_csv`** of the **local** filesystem or a **remote URL** (also in `UPDATE … FROM` / `DELETE … USING` / `MERGE … USING` sources on the edge channel) | `SELECT * FROM read_csv('/etc/passwd')`, `read_parquet('/data/x.parquet')`, `read_csv('https://example.com/')`, `read_single_csv_file('/etc/passwd')`, `glob('/home/*')` |
| **Replacement scans** of a **local** path or a **remote URL** | `SELECT * FROM '/etc/passwd'`, `FROM 'data.parquet'`, `FROM 's3://bucket/x.parquet'` |
| **Any other table function given a remote URL** (checked at bind time) | `iceberg_scan('s3://…')`, `delta_scan('gs://…')` |
| **External-database functions** — `postgres_*`, `mysql_*`, `sqlite_*` table functions and `CALL`s | `SELECT * FROM postgres_query('__ducklake_metadata_x', 'select …')`, `CALL postgres_execute('pg', '…')`, `sqlite_scan('/tmp/x.db', 't')` |
| **DuckLake functions that bypass catalog grants** | `ducklake_add_data_files(…)`, `ducklake_commit(…)`, a direct `ducklake_scan('…')` |
| **`CREATE SECRET` / `DROP SECRET`** (all variants) | `CREATE [OR REPLACE] [PERSISTENT\|TEMPORARY] SECRET …`, `DROP SECRET …` |
| **`duckdb_secrets()`** (always) | `SELECT * FROM duckdb_secrets()` |
| **Quack remote protocol** functions (always) | `CALL quack_serve(…)`, `SELECT * FROM quack_query(…)`, `SELECT quack_cancel(…)` |
| **`CONNECT` / `DISCONNECT` / `EXTERNAL RESOURCE`** (edge channel, DuckDB 2.0) — `CONNECT` attaches its target and forwards raw SQL to it | `CONNECT 'other.db'`, `CREATE EXTERNAL RESOURCE …` |

### Local vs. remote

For filesystem reads and `COPY`, the gate distinguishes **local** from
**remote object-storage / HTTP** paths — `s3://`, `s3a://`, `gs://`/`gcs://`,
`r2://`, `az://`/`azure://`/`abfs://`/`abfss://`, `http://`, `https://`, `hf://`.
A path that cannot be *proven* remote (a bare/relative path, `file://`, or a
computed/non-literal path) is treated as local. Local paths are always gated.

**Remote URLs are gated too, by default.** A non-admin cannot create secrets, so
any remote read or write it runs uses the **server's** credentials and network
position. On a server shared by several tenants — for example one DuckLake
catalog per app on a common bucket — that lets one tenant write files into
another tenant's storage prefix (`COPY … TO 'gs://shared-bucket/<other
catalog>/…'`) or use the server as an HTTP egress point. Start the server with
`--allow-non-admin-remote-access` (`GIZMOSQL_ALLOW_NON_ADMIN_REMOTE_ACCESS=true`)
to let non-admins read and write remote URLs again; do this only when a
non-admin may legitimately use everything the server's credentials can reach.

Scans of the tables of an **attached** catalog (DuckLake on S3/GCS, an attached
Postgres or SQLite database, …) are not affected: they are governed by catalog
permissions like any other table.

**`EXPORT DATABASE` and `IMPORT DATABASE` are the exception: they are gated for
non-admins regardless of destination (local *or* remote).** `EXPORT DATABASE`
dumps the **entire** database — every schema and table — to a location, so
unlike `COPY (SELECT …) TO` (bounded by what you can `SELECT`) it is a
full-database egress; `IMPORT DATABASE` runs arbitrary DDL + DML from a dump.
Both are admin-only.

```sql
-- non-admin: rejected by default; allowed with --allow-non-admin-remote-access
SELECT * FROM read_parquet('s3://bucket/data.parquet');
COPY my_table TO 's3://bucket/out.parquet';

-- non-admin: rejected (local filesystem)
SELECT * FROM read_csv('/etc/passwd');
COPY my_table TO '/tmp/out.csv';

-- non-admin: rejected (full-database egress/ingress — local OR remote)
EXPORT DATABASE 's3://bucket/dump';
IMPORT DATABASE '/tmp/dump';
```

### What is *not* gated

### DuckLake and external-database functions

`postgres_query()` / `postgres_execute()` (and the other `postgres_*`,
`mysql_*` and `sqlite_*` table functions) run raw SQL on — or scan — an attached
external database **as the role GizmoSQL attached it with**. A DuckLake catalog
whose metadata lives in Postgres is attached as `__ducklake_metadata_<catalog>`,
so on a shared server these functions would let any session read and rewrite
any other catalog's metadata (tables, data-file lists, snapshots), and list
every database on the metadata server. They are admin-only, whether called
directly, through a view or macro, or with `CALL`.

The `ducklake_*()` table functions look their catalog up by name, so DuckDB does
not report it among the databases a statement reads or writes and catalog
permissions would not otherwise see it. GizmoSQL checks the catalog named by the
first argument against the session's [catalog access](token_authentication.md)
at bind time:

| Function | Requires |
|----------|----------|
| `ducklake_snapshots`, `ducklake_table_info`, `ducklake_list_files`, `ducklake_current_snapshot`, `ducklake_last_committed_snapshot`, `ducklake_options`, `ducklake_settings`, `ducklake_tags`, `ducklake_table_changes`, `ducklake_table_insertions`, `ducklake_table_deletions` | read access to the catalog |
| every other `ducklake_*` function (`ducklake_expire_snapshots`, `ducklake_cleanup_old_files`, `ducklake_merge_adjacent_files`, `ducklake_set_option`, tags, …) | write access to the catalog |
| `ducklake_add_data_files` (registers files at any path into a table), `ducklake_commit`, a direct `ducklake_scan(…)` | admin |

A catalog argument that is not a constant string is refused (fail closed). The
legacy `readonly` role never passes a write check. In the Core edition, which
has no per-catalog grants, only the `readonly` and admin-only rules apply.

### What is *not* gated

Ordinary queries non-admins need keep working: `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, `CREATE TABLE`, `SHOW`/`DESCRIBE`, transactions, non-filesystem table
functions (`range()`, `duckdb_settings()`, …), harmless pragmas
(`PRAGMA table_info(...)`), and **session-scoped** settings — a non-admin can
still tune their own session with `SET SESSION …` or a bare `SET timezone=…` /
`SET search_path=…`.

## How it works

The gate has two layers.

**Parse time.** Each non-admin client statement is parsed with DuckDB's parser
(no execution, no catalog access) and classified by statement type and by
walking the parse tree for table functions at any nesting depth (subqueries,
CTEs, joins, `CREATE TABLE AS SELECT`, `INSERT … SELECT`, `COPY (SELECT …) TO`).
This refuses a statement before DuckDB binds it. Because it parses rather than
string-matches, it is robust to whitespace, comments, case, and nesting:

```sql
-- still gated:
/* comment */ AtTaCh 'x.db' AS x;
WITH t AS (SELECT * FROM read_csv('/etc/passwd')) SELECT * FROM t;
SELECT 1; ATTACH 'x.db' AS x;        -- one gated statement gates the batch
```

The parse-time check runs only for non-admin, non-internal statements; admins
and internal queries skip it entirely.

**Bind time.** Every non-admin session's DuckDB connection also carries a guard
that inspects the **bound plan** of each statement prepared on it — including
statements DuckDB re-binds at execution, as it does for a prepared statement
whose `?` parameters are only known then. There every table function call
appears with its final arguments however the SQL spelled it, so the guard
catches what parsing cannot see: a view or macro that wraps
`postgres_query()` or `read_csv('/etc/passwd')`, and `ducklake_snapshots(?)`
executed with another tenant's catalog name. It enforces the external-database,
DuckLake, file-read and remote-URL rules above.

## Known limitations

- **Replacement scans** of string-literal paths are caught by a path-shape
  heuristic, not by binding, so an unusual real table named like a file path
  could be mis-flagged (rare).
- **Custom parser extensions.** A statement that GizmoSQL's standalone parser
  cannot parse is passed through (DuckDB surfaces the real error at execution).
  The gated statement types here are all core SQL grammar.

- **Extension autoloading.** With DuckDB's `autoinstall_known_extensions` /
  `autoload_known_extensions` on (the default), calling a function from a
  not-yet-loaded core extension installs and loads it while the statement is
  bound, before the bind-time guard runs — even when the call itself is then
  refused. `INSTALL`/`LOAD` are gated, but autoloading is not. On a shared
  server, preload the extensions tenants need in `--init-sql-commands` and turn
  both settings off there.

The **statement-type** gates — ATTACH/DETACH, SET GLOBAL, INSTALL/LOAD,
CHECKPOINT, COPY/EXPORT — are exact.

## Relationship to the C API / library

The gate is part of the server's query path and applies to every client
(JDBC, ADBC, Python, CLI, UI). There is no flag to disable it; it is a baseline
security control. The only related setting is `--allow-non-admin-remote-access`,
which re-opens remote URLs to non-admins. It composes with the Enterprise per-catalog permissions
(`catalog_access`) — both run before a statement executes.
