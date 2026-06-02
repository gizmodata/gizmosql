# SET Commands

GizmoSQL supports server-specific `SET` commands that configure session-level and server-level behavior at runtime. These commands use the `gizmosql.` prefix to distinguish them from DuckDB's built-in `SET` commands.

## Syntax

```sql
-- Session scope (affects only the current connection)
SET gizmosql.<parameter> = <value>;
SET SESSION gizmosql.<parameter> = <value>;

-- Global scope (affects all sessions; requires admin role)
SET GLOBAL gizmosql.<parameter> = <value>;
```

When no scope is specified, the default is **session** scope.

## Parameters

### `gizmosql.query_log_level`

Controls the minimum severity threshold for query log messages.

| Property | Value |
|----------|-------|
| **Type** | String |
| **Valid values** | `debug`, `info`, `warn` (or `warning`), `error`, `fatal` |
| **Default** | `info` |
| **CLI flag** | `--query-log-level` |
| **Environment variable** | `GIZMOSQL_QUERY_LOG_LEVEL` |

When set to `debug`, internal queries (e.g., `DoGetTables`, `GetDbSchemas`) will appear in the logs. At the default `info` level, these internal queries are suppressed.

**Examples:**

```sql
-- Show all query logs including internal/debug queries (session only)
SET gizmosql.query_log_level = debug;

-- Suppress all query logs except errors and above (session only)
SET gizmosql.query_log_level = error;

-- Set the server-wide default to debug (requires admin role)
SET GLOBAL gizmosql.query_log_level = debug;
```

### `gizmosql.query_timeout`

Controls the maximum execution time for queries, in seconds.

| Property | Value |
|----------|-------|
| **Type** | Integer (seconds) |
| **Default** | `0` (unlimited) |
| **CLI flag** | `--query-timeout` |
| **Environment variable** | `GIZMOSQL_QUERY_TIMEOUT` |

A value of `0` means no timeout — queries can run indefinitely.

**Examples:**

```sql
-- Set a 30-second timeout for the current session
SET gizmosql.query_timeout = 30;

-- Remove the timeout (unlimited)
SET gizmosql.query_timeout = 0;

-- Set a server-wide 60-second default (requires admin role)
SET GLOBAL gizmosql.query_timeout = 60;
```

### `gizmosql.capture_query_profile` *(Enterprise)*

Controls whether DuckDB query profiles are captured into the instrumentation `sql_executions.query_profile` column. See [Query profile capture](session_instrumentation.md#query-profile-capture).

| Property | Value |
|----------|-------|
| **Type** | String — `off` \| `standard` \| `detailed` |
| **Default** | `off` (server default via `--capture-query-profile` / `GIZMOSQL_CAPTURE_QUERY_PROFILE`) |
| **Scope** | Session override (any user) or server-global (`SET GLOBAL`, admin only) |

Requires a valid Enterprise license with the `instrumentation` feature. `standard` records the per-operator profile; `detailed` also times each expression (higher overhead).

```sql
-- Capture detailed profiles for this session
SET gizmosql.capture_query_profile = 'detailed';

-- Make 'standard' the live server default for new statements (admin)
SET GLOBAL gizmosql.capture_query_profile = 'standard';
```

### `gizmosql.bypass_queue` *(Enterprise)*

Skips the [statement queue](statement_queuing.md) for the current session.

| Property | Value |
|----------|-------|
| **Type** | Boolean |
| **Default** | `true` for admin sessions (`--admin-bypass-queue-default`), otherwise `false` |
| **Scope** | Session only; only an **admin** may set it to `true` |

```sql
-- Opt a heavy admin query INTO the queue rather than bypassing it
SET SESSION gizmosql.bypass_queue = false;
```

### `gizmosql.max_concurrent_statements` / `max_queued_statements` / `max_queue_wait` *(Enterprise)*

[Statement-queue](statement_queuing.md) limits. `max_concurrent_statements` and `max_queued_statements` are **server-global** (set with `SET GLOBAL`, admin only); `max_queue_wait` is a server default a session may override.

| Parameter | Type | Default | Environment variable |
|-----------|------|---------|----------------------|
| `max_concurrent_statements` | Integer | `0` (unlimited) | `GIZMOSQL_MAX_CONCURRENT_STATEMENTS` |
| `max_queued_statements` | Integer | auto `8 ×` concurrency limit | `GIZMOSQL_MAX_QUEUED_STATEMENTS` |
| `max_queue_wait` | Integer (seconds) | `300` | `GIZMOSQL_MAX_QUEUE_WAIT` |

```sql
-- Tune the live server (admin)
SET GLOBAL gizmosql.max_concurrent_statements = 8;

-- Shorten this session's queue-wait tolerance
SET SESSION gizmosql.max_queue_wait = 30;
```

## Inspecting settings — `gizmosql_settings()`

The `gizmosql_settings()` table function lists every `gizmosql.*` setting with its current effective value, session/global values, scope, default, and environment variable — the GizmoSQL analog of DuckDB's `duckdb_settings()`. It is composable like any relation:

```sql
SELECT name, value, scope, env_var FROM gizmosql_settings();

SELECT name, value FROM gizmosql_settings() WHERE name LIKE 'gizmosql.max%';
```

## Scope and Precedence

| Scope | Who can set | Affects |
|-------|------------|---------|
| **Session** | Any user | Only the current connection |
| **Global** | Admin users only | All new and existing sessions that haven't set a session-level override |

Session-level settings always take precedence over the global server setting. When a session-level value is set, the server-level default is ignored for that session.

## Notes

- `SET GLOBAL` requires the user to have the `admin` role. Non-admin users will receive an error.
- These commands are processed by GizmoSQL directly and are not passed to DuckDB.
- Database GUI tools (e.g., DBeaver, DataGrip) often use separate connections for the SQL editor and the schema/metadata browser. A `SET` command executed in the SQL editor will not affect the metadata browser's connection. Use `SET GLOBAL` or server startup flags to affect all connections.
