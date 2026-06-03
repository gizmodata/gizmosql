# Catalog Logging *(Enterprise)*

Catalog logging **forks** the server's log stream to a `logs` table in an attached
catalog — **in addition to** stdout/file — so logs are queryable with SQL and can
be retained, joined to [instrumentation](session_instrumentation.md), and analyzed
alongside the rest of your data.

> Catalog logging is an Enterprise feature (gated on the `instrumentation` license
> feature). Without a valid license the server refuses to start when it is enabled.

## Overview

- The same records emitted to stdout/file are **also** written to a `logs` table.
- Supported catalogs: **file-based DuckDB**, **PostgreSQL**, and **DuckLake** —
  auto-detected from `duckdb_databases().type`.
- Logs are **append-only**, so DuckLake is a perfectly good target here (unlike
  [instrumentation](session_instrumentation.md), which performs UPDATEs and so
  should not use DuckLake in multi-instance deployments).
- Writes happen on a **dedicated connection and writer thread**, off the client
  session path. The in-memory hand-off queue is **bounded**; if a slow or
  unreachable log catalog can't keep up, the newest records are dropped and
  counted (a throttled `WARNING` reports the running total). **Logging to a
  catalog can never block or fail a client query.**
- Each batch is written in an **explicit transaction** (committed on success,
  rolled back on error) — never relying on auto-commit, so the writer connection
  is never parked `idle in transaction`.
- The log catalog is **system-managed**, exactly like the instrumentation catalog:
  it is **readable only by admins**, **never client-writable**, and **cannot be
  `DETACH`ed**. The sink writes it on its own internal connection; client sessions
  only ever read it.

## Configuration

| Parameter | CLI Flag | Env Var | Default | Description |
|-----------|----------|---------|---------|-------------|
| `enable_catalog_logging` | `--enable-catalog-logging` | `GIZMOSQL_ENABLE_CATALOG_LOGGING` | `false` | Turn catalog logging on. |
| `log_catalog` | `--log-catalog` | `GIZMOSQL_LOG_CATALOG` | (empty) | Name of a pre-attached catalog to write logs into. Empty ⇒ file-based default (`_gizmosql_logs`). |
| `log_schema` | `--log-schema` | `GIZMOSQL_LOG_SCHEMA` | `main` | Schema within the log catalog. |
| `log_catalog_db_path` | `--log-catalog-db-path` | `GIZMOSQL_LOG_DB_PATH` | (beside the main DB) | File path for the file-based default log database (`gizmosql_logs.db`). Ignored when `--log-catalog` is set. |

### File-based default (single instance)

```bash
gizmosql_server \
  --database-filename /data/mydb.db \
  --password mypassword \
  --enable-catalog-logging \
  --license-key-file /path/to/license.txt
```

The server attaches a `gizmosql_logs.db` (catalog `_gizmosql_logs`) beside the main
database and writes the `logs` table there. Override the path with
`--log-catalog-db-path` / `GIZMOSQL_LOG_DB_PATH`.

### PostgreSQL or DuckLake (recommended for multiple instances)

Attach the catalog in `--init-sql-commands`, then point `--log-catalog` at it:

```bash
gizmosql_server \
  --database-filename /data/mydb.db \
  --password mypassword \
  --init-sql-commands "INSTALL postgres; LOAD postgres; ATTACH 'host=pg dbname=logs user=app password=…' AS logpg (TYPE postgres);" \
  --enable-catalog-logging \
  --log-catalog=logpg \
  --log-schema=gizmosql_logs \
  --license-key-file /path/to/license.txt
```

PostgreSQL objects are created with native SQL via `postgres_execute()`. JSON is
stored as `VARCHAR` on PostgreSQL (it holds JSON strings; query with a `::json`
cast). Combine with [`--cluster-id`](#cluster-id) so every instance's rows carry a
shared `cluster_id` and can be filtered together.

## The `logs` table

The schema mirrors the JSON log shape: the popular/queryable fields are promoted
to typed columns, and everything else rides along in a `fields` JSON catch-all.
There is **no primary key** (logs are append-only). File-based DuckDB and
PostgreSQL also get indexes on `log_time` and the common filter columns so
time-range queries and time-based retention deletes stay fast.

| Column | Type | Description |
|--------|------|-------------|
| `log_time` | TIMESTAMPTZ | When the record was emitted (UTC). NOT NULL. |
| `level` | VARCHAR | `DEBUG`/`INFO`/`WARN`/`ERROR`/`FATAL`. NOT NULL. |
| `instance_id` | UUID | Server instance that emitted the record. |
| `cluster_id` | UUID | Cluster grouping UUID (`--cluster-id`), NULL when unset. |
| `session_id` | UUID | Client session (when the record is session-scoped). |
| `username` | VARCHAR | Authenticated user (when present). |
| `role` | VARCHAR | Session role (when present). |
| `peer` | VARCHAR | Client peer address (when present). |
| `component` | VARCHAR | Logging component (when configured). |
| `trace_id` / `span_id` | VARCHAR | OpenTelemetry correlation IDs (when present). |
| `pid` | INTEGER | Process id. |
| `tid` | VARCHAR | Thread id. |
| `source_file` / `source_line` | VARCHAR / INTEGER | Source location. |
| `func` | VARCHAR | Function name (when present). |
| `message` | VARCHAR | The log message. |
| `fields` | JSON | All remaining structured key/value fields. |

Read it as an admin (it is hidden from / denied to non-admins):

```sql
-- File-based default:
SELECT log_time, level, username, message
FROM _gizmosql_logs.logs
WHERE log_time > now() - INTERVAL '1 hour'
ORDER BY log_time DESC;

-- External catalog (PostgreSQL/DuckLake): use the catalog.schema you configured.
SELECT * FROM logpg.gizmosql_logs.logs ORDER BY log_time DESC LIMIT 100;
```

### Retention

Logs grow continuously. Prune them out-of-band on the catalog itself (the catalog
is read-only to GizmoSQL clients). The `log_time` index keeps time-based deletes
fast:

```sql
DELETE FROM gizmosql_logs.logs WHERE log_time < now() - INTERVAL '30 days';
```

## Cluster ID

`--cluster-id` / `GIZMOSQL_CLUSTER_ID` (a UUID) tags a server as part of a logical
cluster. When set, the cluster id is:

- recorded on the instrumentation [`instances`](session_instrumentation.md) row
  (the nullable `cluster_id UUID` column),
- injected into **every** log entry (stdout/file **and** the catalog `logs`
  table), and
- exposed to SQL via the **`GIZMOSQL_CURRENT_CLUSTER()`** pseudo-function (the
  cluster analog of `GIZMOSQL_CURRENT_INSTANCE()`):

```sql
SELECT GIZMOSQL_CURRENT_CLUSTER();   -- the cluster UUID, or NULL when unset
```

This lets logs and instrumentation across all instances of a cluster be filtered
and correlated together — e.g. `WHERE cluster_id = '…'`.
