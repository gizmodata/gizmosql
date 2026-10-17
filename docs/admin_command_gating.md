# Admin Command Gating

GizmoSQL restricts a fixed set of dangerous, instance-level SQL commands to
sessions whose role is **`admin`**. This is a **rudimentary, always-on guardrail**
that ships ahead of the full role-based access control (RBAC) model planned for
**GizmoSQL 2.0**. Its job is narrow but important: stop a *non-admin* user from
reaching outside the database — detaching catalogs, reading the local
filesystem, attaching databases, installing extensions, or changing global
server settings.

!!! info "Interim control, not the full model"
    This is intentionally a small, conservative gate built on SQL parsing. GizmoSQL
    Enterprise also offers [role-based access control](access_control.md): users,
    roles and object- and column-level grants managed with `GRANT` / `REVOKE`, and
    checked on each statement's plan. With it on, this gate is stricter still for
    store users (every file and URL, remote ones included, is refused).

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
| Username / password (Basic auth) of the server's `--username` | `admin` | **No** |
| Username / password of a [role-based access control](access_control.md) store user | `user` | **Yes** — gated |
| Token (JWT) with `role: admin` | `admin` | **No** |
| Token (JWT) with a non-admin `role` (e.g. `analyst`) | that role | **Yes** — gated |
| Token with no `role` claim + `--token-default-role` set to a non-admin role | that role | **Yes** — gated |

In other words: without role-based access control, a deployment that only uses
username/password auth is unchanged — every such user is an admin. The gate matters specifically for **token / SSO
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
| **Dangerous bare `SET`** of a global-only setting | `SET memory_limit='100GB'`, `SET threads=1`, `SET enable_external_access=false`, `SET temp_directory='…'`, and on the edge channel `SET allow_extension_repositories='allowed'`, `SET extension_repository_directory=…`, `SET extension_directories=…`, `SET redact_http_logs=false` |
| **Grammar-switching settings, in any scope** (edge channel, DuckDB 2.0) — a session that changed its own grammar could send SQL the gate cannot parse but DuckDB can | `SET current_dialect=…`, `SET SESSION active_grammar_extensions=[…]`, `SET SESSION enable_legacy_parser=true` (`RESET` is allowed) |
| **INSTALL / LOAD** extensions, and extension repositories (edge channel) — a repository's public keys become trusted extension-signing keys | `INSTALL httpfs`, `LOAD spatial`, `FORCE INSTALL …`, `INSTALL AND LOAD …`, `CREATE EXTENSION REPOSITORY …`, `DROP EXTENSION REPOSITORY …` |
| **CHECKPOINT** | `CHECKPOINT`, `FORCE CHECKPOINT` |
| **EXPORT DATABASE / IMPORT DATABASE** (any destination) | `EXPORT DATABASE '/tmp/dump'`, `EXPORT DATABASE 's3://bucket/dump'`, `IMPORT DATABASE '/tmp/dump'` |
| **COPY** to/from a **local** file — including `COPY … TO` as a CTE body ([edge channel](edge_channel.md), DuckDB 2.0) | `COPY t TO '/tmp/x.csv'`, `COPY t FROM '/etc/passwd'`, `WITH c AS (COPY t TO '/tmp/x.csv') SELECT 1` |
| **`read_*` / `glob` / `sniff_csv`** of the **local** filesystem (also in `UPDATE … FROM` / `DELETE … USING` / `MERGE … USING` sources on the edge channel) | `SELECT * FROM read_csv('/etc/passwd')`, `read_parquet('/data/x.parquet')`, `read_single_csv_file('/etc/passwd')`, `glob('/home/*')` |
| **Replacement scans** of a **local** path | `SELECT * FROM '/etc/passwd'`, `FROM 'data.parquet'` |
| **`CREATE SECRET` / `DROP SECRET`** (all variants) | `CREATE [OR REPLACE] [PERSISTENT\|TEMPORARY] SECRET …`, `DROP SECRET …` |
| **`duckdb_secrets()`** (always) | `SELECT * FROM duckdb_secrets()` |
| **SQL inside `query()` / `query_table()`** — checked like the statement around it; SQL built at run time is refused | `query('SELECT * FROM read_csv(''/tmp/x.csv'')')`, `query('SELECT name FROM duckdb_secrets()')`, `query_table('/tmp/x.csv')` |
| **Macro bodies**, checked at `CREATE MACRO` (a call such as `FROM m()` names no file) | `CREATE TEMP MACRO m() AS TABLE SELECT * FROM read_csv('/tmp/x.csv')`, `CREATE MACRO r(p) AS TABLE SELECT * FROM read_csv(p)` |
| **`json_execute_serialized_sql()`** (always) — runs a serialized statement | `SELECT * FROM json_execute_serialized_sql(json_serialize_sql('…'))` |
| **Quack remote protocol** functions (always) | `CALL quack_serve(…)`, `SELECT * FROM quack_query(…)`, `SELECT quack_cancel(…)` |
| **`CONNECT` / `DISCONNECT` / `EXTERNAL RESOURCE`** (edge channel, DuckDB 2.0) — `CONNECT` attaches its target and forwards raw SQL to it | `CONNECT 'other.db'`, `CREATE EXTERNAL RESOURCE …` |
| **External-resource functions** (edge channel, always) — they run callbacks named by string on an internal connection, outside the checks on the client's statement (`destroy_external_resource('query', '<sql>')` runs `<sql>`) | `SELECT * FROM destroy_external_resource(…)`, `CALL create_external_resource(…)`, `register_external_resource[_type](…)`, `deregister_external_resource(…)`, `duckdb_external_resources()`, `duckdb_external_resource_types()`, `duckdb_extension_repositories()` |

### Local vs. remote

For filesystem reads and `COPY`, the gate distinguishes **local** from
**remote object-storage / HTTP** paths. Non-admins **may** read from and write to
proven-remote URLs — `s3://`, `s3a://`, `gs://`/`gcs://`, `r2://`, `az://`/
`azure://`/`abfs://`/`abfss://`, `http://`, `https://`, `hf://` — but **not** local
paths. A path that cannot be *proven* remote (a bare/relative path, `file://`, or
a computed/non-literal path) is treated as local and **gated** — the gate fails
closed.

**`EXPORT DATABASE` and `IMPORT DATABASE` are the exception: they are gated for
non-admins regardless of destination (local *or* remote).** `EXPORT DATABASE`
dumps the **entire** database — every schema and table — to a location, so
unlike `COPY (SELECT …) TO` (bounded by what you can `SELECT`) it is a
full-database egress; `IMPORT DATABASE` runs arbitrary DDL + DML from a dump.
Both are admin-only.

```sql
-- non-admin: allowed (remote object storage, bounded reads/writes)
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

Ordinary queries non-admins need keep working: `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, `CREATE TABLE`, `SHOW`/`DESCRIBE`, transactions, non-filesystem table
functions (`range()`, `duckdb_settings()`, …), harmless pragmas
(`PRAGMA table_info(...)`), and **session-scoped** settings — a non-admin can
still tune their own session with `SET SESSION …` or a bare `SET timezone=…` /
`SET search_path=…` — except the grammar-switching settings above, which are
gated in every scope.

## How it works

Detection is **parser-based**: each non-admin client statement is parsed with
DuckDB's parser (no execution, no catalog access) and classified by statement
type and, for `read_*`/`duckdb_secrets()`, by walking the parse tree for table
functions at any nesting depth (subqueries, CTEs, joins, `CREATE TABLE AS
SELECT`, `INSERT … SELECT`, `COPY (SELECT …) TO`). Because it parses rather than
string-matches, it is robust to whitespace, comments, case, and nesting:

```sql
-- still gated:
/* comment */ AtTaCh 'x.db' AS x;
WITH t AS (SELECT * FROM read_csv('/etc/passwd')) SELECT * FROM t;
SELECT 1; ATTACH 'x.db' AS x;        -- one gated statement gates the batch
```

The check runs only for non-admin, non-internal statements; admins and internal
queries skip it entirely.

## Known limitations (and the 2.0 fix)

These are inherent to a parse-time gate and are resolved by the binding-level
enforcement in GizmoSQL 2.0:

- **Views / macros.** A view or macro that *wraps* a gated function is not
  inspected — selecting from it is not caught. (Creating such a wrapper still
  requires running the gated function once, which is itself gated.) `PREPARE`
  that stages a gated statement *is* caught (at prepare time), so the
  `PREPARE`/`EXECUTE` indirection is closed.
- **Replacement scans** of string-literal paths are caught by a path-shape
  heuristic, not by binding, so an unusual real table named like a file path
  could be mis-flagged (rare).
- **Custom parser extensions.** A statement that GizmoSQL's standalone parser
  cannot parse is passed through (DuckDB surfaces the real error at execution).
  The gated statement types here are all core SQL grammar. On DuckDB 2.0 a
  session can switch its grammar (`current_dialect`, `active_grammar_extensions`),
  which would let it send SQL the gate cannot parse; non-admins therefore cannot
  change those settings.

The **statement-type** gates — ATTACH/DETACH, SET GLOBAL, INSTALL/LOAD,
CHECKPOINT, COPY/EXPORT — have **no** such gaps; they are exact.

## Relationship to the C API / library

The gate is part of the server's query path and applies to every client
(JDBC, ADBC, Python, CLI, UI). There is no flag to disable it; it is a baseline
security control. It composes with the Enterprise per-catalog permissions
(`catalog_access`) — both run before a statement executes.
