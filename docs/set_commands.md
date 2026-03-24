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
