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
| **COPY** to/from a **local** file — including `COPY … TO` as a CTE body ([edge channel](edge_channel.md), DuckDB 2.0) | `COPY t TO '/tmp/x.csv'`, `COPY t FROM '/etc/passwd'`, `WITH c AS (COPY t TO '/tmp/x.csv') SELECT 1` |
| **`read_*` / `glob` / `sniff_csv`** of the **local** filesystem (also in `UPDATE … FROM` / `DELETE … USING` / `MERGE … USING` sources on the edge channel) | `SELECT * FROM read_csv('/etc/passwd')`, `read_parquet('/data/x.parquet')`, `read_single_csv_file('/etc/passwd')`, `glob('/home/*')` |
| **Replacement scans** of a **local** path | `SELECT * FROM '/etc/passwd'`, `FROM 'data.parquet'` |
| **`CREATE SECRET` / `DROP SECRET`** (all variants) | `CREATE [OR REPLACE] [PERSISTENT\|TEMPORARY] SECRET …`, `DROP SECRET …` |
| **`duckdb_secrets()`** (always) | `SELECT * FROM duckdb_secrets()` |
| **Quack remote protocol** functions (always) | `CALL quack_serve(…)`, `SELECT * FROM quack_query(…)`, `SELECT quack_cancel(…)` |
| **`postgres_*` / `mysql_*` functions, `sqlite_scan` / `sqlite_attach`** (always; also inside views, CTAS and `CREATE MACRO`) — they reach attached databases (e.g. a DuckLake catalog's metadata Postgres) as that database's own role, outside the token's catalog access | `postgres_query('db', '…')`, `CALL postgres_execute('db', '…')` |
| **Reads and writes of network URLs** (`http://`, `https://`, `hf://`) — outbound HTTP from the server; any table function, `COPY`, replacement scan | `read_csv('https://example.com/')`, `delta_scan('https://…')`, `COPY t TO/FROM 'https://…'`, `FROM 'https://…/x.csv'` |
| **`query('…')` with SQL that is not a string literal**; **`json_execute_serialized_sql`** — they run SQL the checks cannot read | `query(some_column)`, `json_execute_serialized_sql(…)` |
| **A `CREATE VIEW` that wraps a DuckLake function** (Enterprise) — it would run with the access of whoever selects from it | `CREATE VIEW v AS SELECT * FROM ducklake_snapshots('lake')` |
| **`ducklake_add_data_files`** (always) | `CALL ducklake_add_data_files('lake', 't', 'gs://…')` |
| **`CONNECT` / `DISCONNECT` / `EXTERNAL RESOURCE`** (edge channel, DuckDB 2.0) — `CONNECT` attaches its target and forwards raw SQL to it | `CONNECT 'other.db'`, `CREATE EXTERNAL RESOURCE …` |

### Local vs. remote

Non-admins cannot name a **local** path or a **network** URL (`http://`,
`https://`, `hf://`: outbound HTTP from the server's network position), in any
table function, `COPY` or replacement scan, however deeply nested, including
inside a `query('…')` string. A path that cannot be *proven* remote (a
bare/relative path, `file://`, or a computed/non-literal path) is treated as
local and **gated**: the gate fails closed. `query('…')` is checked as the SQL it
runs; a `query()` whose argument is not a string literal is refused.

**Object storage** (`s3://`, `s3a://`, `gs://`/`gcs://`, `r2://`,
`az://`/`azure://`/`abfs://`/`abfss://`) is *not* decided by this text gate:
paths can be built by a macro, `query()` or a computed argument, which text
inspection cannot follow. It is enforced where every path is seen, in the file
system, and only for sessions the token scopes (below).

### Remote storage for catalog-scoped sessions (Enterprise)

Object-storage credentials belong to the server and are shared by every catalog it
hosts, so a token user who could name any `gs://…` path could read another
catalog's data (`read_parquet('gs://bucket/<other catalog>/…')`) or write into it
(`COPY … TO`). For a **catalog-scoped session** (a non-admin whose token carries
`catalog_access` rules, on a server licensed for catalog permissions) GizmoSQL
therefore refuses any remote path that is not under the **`data_path` of a
DuckLake catalog the token may access**: a catalog readable for reads, writable
for writes. It applies to every way of naming a path (`read_*`, `glob`,
`COPY`, replacement scans, macros, `query()`, computed paths)
because it is the file system that refuses, and DuckLake's own reads and writes of
its data files, which go through the same file system, are allowed by the same
rule. The allowed paths are recomputed before each statement, so a catalog an
administrator attaches while the session is open is picked up.

- Sessions that are **not** catalog-scoped keep object storage: admins, and
  non-admin tokens without `catalog_access` rules (a single-tenant server).
- A `role: admin` token is never scoped, even with `catalog_access` rules.
- Extensions that read object storage with their own client rather than through
  DuckDB's file system would escape that check, so a catalog-scoped session may not
  call `delta_*()` or `iceberg_*()` at all (directly, inside `query()`, or in a
  macro that names them). Other sessions are unaffected.
- A catalog whose data path cannot be read is treated as having none, so storage
  under it stays closed to scoped sessions (a warning is logged).

**`EXPORT DATABASE` and `IMPORT DATABASE` are the exception: they are gated for
non-admins regardless of destination (local *or* remote).** `EXPORT DATABASE`
dumps the **entire** database — every schema and table — to a location, so
unlike `COPY (SELECT …) TO` (bounded by what you can `SELECT`) it is a
full-database egress; `IMPORT DATABASE` runs arbitrary DDL + DML from a dump.
Both are admin-only.

```sql
-- non-admin: rejected (network URLs)
SELECT * FROM read_csv('https://example.com/');
SELECT * FROM query('SELECT * FROM read_csv(''https://example.com/'')');

-- non-admin: rejected (local filesystem)
SELECT * FROM read_csv('/etc/passwd');
COPY my_table TO '/tmp/out.csv';

-- non-admin: rejected (full-database egress/ingress — local OR remote)
EXPORT DATABASE 's3://bucket/dump';
IMPORT DATABASE '/tmp/dump';

-- catalog-scoped session (token with catalog_access rules):
SELECT * FROM read_parquet('gs://bucket/other_catalog/x.parquet'); -- rejected
SELECT * FROM my_catalog.my_table;                                  -- allowed
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

### Indirect routes

A gated function is refused however the statement reaches it:

| Route | Example (refused for non-admins) |
|---|---|
| **`CALL`** — it returns the function's rows like `SELECT * FROM f(…)` | `CALL read_csv('/etc/passwd')`, `CALL duckdb_secrets()`, `CALL query('…')` |
| **A macro body** — checked when the macro is created. A body that passes a parameter on as a path or as SQL is refused, because the parameter is not a string literal | `CREATE MACRO m(s) AS TABLE SELECT * FROM query(s)`, `CREATE MACRO r(p) AS TABLE SELECT * FROM read_csv(p)` |
| **SQL or a path handed to any table function** — an existing table macro may pass its argument on to `query()` or `read_csv()` | `SELECT * FROM m('SELECT * FROM duckdb_secrets()')`, `SELECT * FROM r('/etc/passwd')` |
| **`query_table('<name>')`** with a name that is a file, a URL or not a literal | `SELECT * FROM query_table('/data/x.csv')` |
| **`PRAGMA json_execute_serialized_sql`** | `PRAGMA json_execute_serialized_sql('…')` |
| **Statement forms that carry a query** | `SUMMARIZE SELECT * FROM read_csv('/etc/passwd')`, `SET VARIABLE v = (SELECT … FROM read_text('/etc/hosts'))`, `UPDATE t SET x = (SELECT … FROM duckdb_secrets())` |

The last row is a **fail-closed** rule. The parser walk covers the statement forms
listed under *How it works*; SQL has more places a call can sit (`DESCRIBE` /
`SUMMARIZE`, `PIVOT`, `SET VARIABLE`, `CREATE TYPE … AS ENUM (SELECT …)`, and on
DuckDB 1.x `UPDATE` / `DELETE` / `MERGE`). So the gate also counts, from the
statement's tokens, every call of a function it cares about, and refuses the
statement if it names more of them than the walk inspected. Names inside string
constants and comments do not count. One exception keeps object storage usable in
those places: a file-reading function whose every call in the statement reads a
literal object-storage path is allowed, since that is all the walk would have
required (`MERGE INTO t USING read_parquet('s3://bucket/staged/*.parquet') …`,
`DESCRIBE SELECT * FROM read_parquet('s3://bucket/x.parquet')`).

## Known limitations (and the 2.0 fix)

These are inherent to a parse-time gate and are resolved by the binding-level
enforcement in GizmoSQL 2.0:

- **Views / macros that already exist.** A non-admin cannot *create* a view or
  macro around a gated function (see [Indirect routes](#indirect-routes)), but
  the body of one that already exists is not inspected when it is used. That
  covers views and macros an admin created, and any created by a non-admin on a
  release before this check. Only what the caller passes in is checked: a path
  or SQL text given as a string literal. A macro that needs no such argument
  (`CREATE MACRO leak() AS TABLE SELECT * FROM read_csv('/some/file')`) runs for
  whoever may call it. After upgrading, review the macros on a shared server:

  ```sql
  SELECT database_name, schema_name, function_name, macro_definition
  FROM duckdb_functions()
  WHERE function_type IN ('macro', 'table_macro') AND NOT internal;
  ```

  `PREPARE` that stages a gated statement *is* caught (at prepare time), so the
  `PREPARE`/`EXECUTE` indirection is closed.
- **Functions the gate does not know.** A table function outside the lists above
  (an extension's reader such as `read_xlsx` or `st_read`) is refused when a
  string argument plainly names a local file or a network URL, but not when the
  path is computed (`read_xlsx('/data/' || 'book.xlsx')`). The sensitive-path
  guard and, for catalog-scoped sessions, the remote-storage rule still apply to
  whatever file it opens.
- **Names that look like calls.** The fail-closed check reads `name(` in the
  statement text, so a table or alias named after a gated function and followed
  by a column list (`INSERT INTO glob (a, b) …`) is refused in the statement
  forms the gate cannot walk. Renaming the table or alias avoids it.
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

## DuckLake functions and catalog access (Enterprise)

`ducklake_*` table functions and procedures take the catalog as an argument
(`ducklake_snapshots('lake')`, `ducklake_list_files('lake', 't')`) or as a
qualifier (`CALL lake.set_option(…)`, `FROM lake.snapshots()`). For non-admin
sessions with `catalog_access` rules, each is checked against the token: **read**
access for inspection functions (`snapshots`, `table_info`, `list_files`,
`table_insertions`, `table_deletions`, `table_changes`, `settings`, `options`,
`get_option`, `current_snapshot`, `last_committed_snapshot`) and **write** access
for everything else (`expire_snapshots`, `merge_adjacent_files`, `set_option`, …).
A catalog that is not a string literal cannot be verified and is refused.
