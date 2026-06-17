# Admin Command Gating

GizmoSQL restricts a fixed set of dangerous, instance-level SQL commands to
sessions whose role is **`admin`**. This is a **rudimentary, always-on guardrail**
that ships ahead of the full role-based access control (RBAC) model planned for
**GizmoSQL 2.0**. Its job is narrow but important: stop a *non-admin* user from
reaching outside the database — detaching catalogs, reading the local
filesystem, attaching databases, installing extensions, or changing global
server settings.

!!! info "Interim control, not the full model"
    This is intentionally a small, conservative gate built on SQL parsing. The
    complete, binding-level authorization model (object- and column-level grants,
    `GRANT`/`REVOKE` DDL, multi-tenant policies) is the GizmoSQL 2.0 cornerstone.
    The known limitations below are addressed properly there.

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
`UNAVAILABLE`/permission error (`"Permission denied: <command> requires the
'admin' role …"`):

| Category | Examples |
|----------|----------|
| **ATTACH / DETACH** (any database) | `ATTACH 'lake.db' AS lake`, `DETACH my_ducklake` |
| **SET GLOBAL / RESET GLOBAL** (any setting) | `SET GLOBAL memory_limit='8GB'`, `RESET GLOBAL …` |
| **Dangerous bare `SET`** of a global-only setting | `SET memory_limit='100GB'`, `SET threads=1`, `SET enable_external_access=false`, `SET temp_directory='…'` |
| **INSTALL / LOAD** extensions | `INSTALL httpfs`, `LOAD spatial`, `FORCE INSTALL …` |
| **CHECKPOINT** | `CHECKPOINT`, `FORCE CHECKPOINT` |
| **COPY** to/from a **local** file | `COPY t TO '/tmp/x.csv'`, `COPY t FROM '/etc/passwd'` |
| **EXPORT DATABASE** to a **local** directory | `EXPORT DATABASE '/tmp/dump'` |
| **`read_*` / `glob` / `sniff_csv`** of the **local** filesystem | `SELECT * FROM read_csv('/etc/passwd')`, `read_parquet('/data/x.parquet')`, `glob('/home/*')` |
| **Replacement scans** of a **local** path | `SELECT * FROM '/etc/passwd'`, `FROM 'data.parquet'` |
| **`duckdb_secrets()`** (always) | `SELECT * FROM duckdb_secrets()` |

### Local vs. remote

For filesystem reads and `COPY`/`EXPORT`, the gate distinguishes **local** from
**remote object-storage / HTTP** paths. Non-admins **may** read from and write to
proven-remote URLs — `s3://`, `s3a://`, `gs://`/`gcs://`, `r2://`, `az://`/
`azure://`/`abfs://`/`abfss://`, `http://`, `https://`, `hf://` — but **not** local
paths. A path that cannot be *proven* remote (a bare/relative path, `file://`, or
a computed/non-literal path) is treated as local and **gated** — the gate fails
closed.

```sql
-- non-admin: allowed (remote object storage)
SELECT * FROM read_parquet('s3://bucket/data.parquet');
COPY my_table TO 's3://bucket/out.parquet';

-- non-admin: rejected (local filesystem)
SELECT * FROM read_csv('/etc/passwd');
COPY my_table TO '/tmp/out.csv';
```

### What is *not* gated

Ordinary queries non-admins need keep working: `SELECT`, `INSERT`, `UPDATE`,
`DELETE`, `CREATE TABLE`, `SHOW`/`DESCRIBE`, transactions, non-filesystem table
functions (`range()`, `duckdb_settings()`, …), harmless pragmas
(`PRAGMA table_info(...)`), and **session-scoped** settings — a non-admin can
still tune their own session with `SET SESSION …` or a bare `SET timezone=…` /
`SET search_path=…`.

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
  requires running the gated function once, which is itself gated.)
- **Replacement scans** of string-literal paths are caught by a path-shape
  heuristic, not by binding, so an unusual real table named like a file path
  could be mis-flagged (rare).
- **Custom parser extensions.** A statement that GizmoSQL's standalone parser
  cannot parse is passed through (DuckDB surfaces the real error at execution).
  The gated statement types here are all core SQL grammar.

The **statement-type** gates — ATTACH/DETACH, SET GLOBAL, INSTALL/LOAD,
CHECKPOINT, COPY/EXPORT — have **no** such gaps; they are exact.

## Relationship to the C API / library

The gate is part of the server's query path and applies to every client
(JDBC, ADBC, Python, CLI, UI). There is no flag to disable it; it is a baseline
security control. It composes with the Enterprise per-catalog permissions
(`catalog_access`) — both run before a statement executes.
