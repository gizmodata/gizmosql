# Session Instrumentation

GizmoSQL provides built-in session instrumentation for tracking server instances, client sessions, and SQL statement execution. This feature enables SOC2/audit compliance, performance analysis, and session management capabilities.

---

## Overview

Session instrumentation automatically tracks:
- **Server instances**: When the server starts and stops
- **Client sessions**: When clients connect and disconnect, including authentication details
- **SQL statements**: Prepared statements created during a session
- **SQL executions**: Every execution of a statement, including bind parameters, execution time, and row counts

All instrumentation data is stored in a separate DuckDB database and exposed as read-only views accessible via SQL queries.

---

## Configuration

### Database Location

By default, the instrumentation database is created in the same directory as the main database file:
- If `--database-filename` is `/path/to/mydb.db`, instrumentation is stored at `/path/to/gizmosql_instrumentation.db`
- If using in-memory mode (`:memory:`), instrumentation is stored in the current working directory

You can override the location using the environment variable:

```bash
export GIZMOSQL_INSTRUMENTATION_DB_PATH=/custom/path/instrumentation.db
```

---

## Schema

### Tables

The instrumentation database contains four core tables:

#### `_gizmosql_instr.instances`

Tracks server instance lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `instance_id` | UUID | Primary key |
| `gizmosql_version` | VARCHAR | GizmoSQL version |
| `duckdb_version` | VARCHAR | DuckDB version |
| `arrow_version` | VARCHAR | Apache Arrow version |
| `hostname` | VARCHAR | Server hostname |
| `port` | INTEGER | Server port |
| `database_path` | VARCHAR | Path to main database |
| `tls_enabled` | BOOLEAN | Whether TLS is enabled |
| `tls_cert_path` | VARCHAR | Path to TLS certificate file (NULL if TLS disabled) |
| `tls_key_path` | VARCHAR | Path to TLS key file (NULL if TLS disabled) |
| `mtls_required` | BOOLEAN | Whether mutual TLS (mTLS) is required |
| `mtls_ca_cert_path` | VARCHAR | Path to mTLS CA certificate file (NULL if mTLS disabled) |
| `readonly` | BOOLEAN | Whether the instance is started in read-only mode |
| `start_time` | TIMESTAMP | When server started |
| `stop_time` | TIMESTAMP | When server stopped (NULL if running) |
| `status` | ENUM | 'running' or 'stopped' |
| `status_text` | VARCHAR | Virtual column with text representation of status |
| `stop_reason` | VARCHAR | Reason for shutdown |

#### `_gizmosql_instr.sessions`

Tracks client session lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | UUID | Primary key |
| `instance_id` | UUID | Reference to instances |
| `username` | VARCHAR | Authenticated username |
| `role` | VARCHAR | User's role (e.g., 'admin', 'user') |
| `auth_method` | VARCHAR | Authentication method (e.g., 'Basic', 'BootstrapToken') |
| `peer` | VARCHAR | Client IP/hostname |
| `start_time` | TIMESTAMP | When session started |
| `stop_time` | TIMESTAMP | When session ended (NULL if active) |
| `status` | ENUM | 'active', 'closed', 'killed', 'timeout', 'error' |
| `status_text` | VARCHAR | Virtual column with text representation of status |
| `stop_reason` | VARCHAR | Reason session ended |

#### `_gizmosql_instr.sql_statements`

Tracks prepared statement definitions (one per prepared statement).

| Column | Type | Description |
|--------|------|-------------|
| `statement_id` | UUID | Primary key (same as used in logs) |
| `session_id` | UUID | Reference to sessions |
| `sql_text` | VARCHAR | The SQL query text |
| `created_time` | TIMESTAMP | When statement was created |

#### `_gizmosql_instr.sql_executions`

Tracks individual executions of statements (one per execution).

| Column | Type | Description |
|--------|------|-------------|
| `execution_id` | UUID | Primary key (same as used in logs) |
| `statement_id` | UUID | Reference to sql_statements |
| `bind_parameters` | VARCHAR | JSON array of bind parameters (NULL if none) |
| `execution_start_time` | TIMESTAMP | When execution started |
| `execution_end_time` | TIMESTAMP | When execution completed |
| `rows_fetched` | BIGINT | Number of rows returned |
| `status` | ENUM | 'executing', 'success', 'error', 'timeout', 'cancelled' |
| `status_text` | VARCHAR | Virtual column with text representation of status |
| `error_message` | VARCHAR | Error message if failed |
| `duration_ms` | BIGINT | Execution duration in milliseconds |

### Views

Several convenience views are provided:

#### `_gizmosql_instr.active_sessions`

Shows currently active sessions.

```sql
SELECT * FROM _gizmosql_instr.active_sessions;
```

Returns: `session_id`, `instance_id`, `username`, `role`, `auth_method`, `peer`, `start_time`, `status`, `hostname`, `port`, `database_path`, `session_duration_seconds`

#### `_gizmosql_instr.session_activity`

Complete view joining instances, sessions, statements, and executions.

```sql
SELECT * FROM _gizmosql_instr.session_activity
WHERE username = 'alice'
ORDER BY execution_start_time DESC
LIMIT 100;
```

#### `_gizmosql_instr.session_stats`

Aggregated statistics per session.

```sql
SELECT
    username,
    total_statements,
    total_executions,
    successful_executions,
    failed_executions,
    total_rows_fetched,
    avg_duration_ms
FROM _gizmosql_instr.session_stats;
```

#### `_gizmosql_instr.execution_details`

Detailed view of executions with statement and session info.

```sql
SELECT
    execution_id,
    statement_id,
    sql_text,
    bind_parameters,
    execution_start_time,
    duration_ms,
    status,
    username
FROM _gizmosql_instr.execution_details
WHERE status = 'error'
LIMIT 20;
```

---

## SQL Functions

GizmoSQL provides several pseudo-functions that are replaced with actual values at query execution time. These functions work in any SQL context (SELECT, WHERE, etc.) and are case-insensitive.

### `GIZMOSQL_VERSION()`

Returns the version string of the GizmoSQL server.

```sql
SELECT GIZMOSQL_VERSION();
-- Returns: 'v1.14.0'
```

Useful for version checking in scripts or diagnostics:

```sql
SELECT
    GIZMOSQL_VERSION() AS gizmosql_version,
    (SELECT version FROM pragma_version()) AS duckdb_version;
```

### `GIZMOSQL_CURRENT_SESSION()`

Returns the UUID of the current session.

```sql
SELECT GIZMOSQL_CURRENT_SESSION();
-- Returns: '550e8400-e29b-41d4-a716-446655440000'
```

Useful for filtering instrumentation data to the current session:

```sql
SELECT * FROM _gizmosql_instr.sql_statements
WHERE session_id = GIZMOSQL_CURRENT_SESSION()
ORDER BY created_time DESC;
```

### `GIZMOSQL_CURRENT_INSTANCE()`

Returns the UUID of the current server instance.

```sql
SELECT GIZMOSQL_CURRENT_INSTANCE();
-- Returns: '7b2a3c4d-5e6f-7890-abcd-ef1234567890'
```

Useful for filtering instrumentation data to the current server instance:

```sql
SELECT * FROM _gizmosql_instr.sessions
WHERE instance_id = GIZMOSQL_CURRENT_INSTANCE()
ORDER BY start_time DESC;
```

---

## Session Management

### KILL SESSION

Administrators can terminate other sessions using the `KILL SESSION` command:

```sql
KILL SESSION '550e8400-e29b-41d4-a716-446655440000';
```

**Requirements:**
- Caller must have the `admin` role
- Cannot kill your own session
- The target session must exist and be active

**Example workflow:**

```sql
-- List active sessions
SELECT session_id, username, peer, session_duration_seconds
FROM _gizmosql_instr.active_sessions;

-- Kill a specific session
KILL SESSION '550e8400-e29b-41d4-a716-446655440000';
```

When a session is killed:
1. Any executing queries are cancelled
2. The session's status is set to 'killed'
3. The client receives an error on their next operation

---

## Security

### Read-Only Access

The instrumentation database is protected from client modifications. Users can query instrumentation data but cannot modify it. Any attempt to INSERT, UPDATE, DELETE, or otherwise modify data in `_gizmosql_instr` will be rejected:

```sql
DELETE FROM _gizmosql_instr.sql_executions;
-- Error: Cannot modify the instrumentation database (_gizmosql_instr).
-- It is read-only for client sessions.
```

### DETACH Prevention

Users cannot detach the instrumentation database:

```sql
DETACH _gizmosql_instr;  -- Error: Cannot DETACH the instrumentation database
```

### Role-Based Access

- All authenticated users can query instrumentation views
- Only users with the `admin` role can execute `KILL SESSION`

---

## Example Queries

### Find slow executions

```sql
SELECT
    sql_text,
    bind_parameters,
    duration_ms,
    rows_fetched,
    execution_start_time
FROM _gizmosql_instr.execution_details
WHERE status = 'success'
ORDER BY duration_ms DESC
LIMIT 10;
```

### Session activity report

```sql
SELECT
    s.username,
    s.peer,
    COUNT(DISTINCT st.statement_id) as statement_count,
    COUNT(e.execution_id) as execution_count,
    SUM(e.rows_fetched) as total_rows,
    AVG(e.duration_ms) as avg_duration_ms
FROM _gizmosql_instr.sessions s
LEFT JOIN _gizmosql_instr.sql_statements st ON s.session_id = st.session_id
LEFT JOIN _gizmosql_instr.sql_executions e ON st.statement_id = e.statement_id
WHERE s.start_time > now() - INTERVAL '1 day'
GROUP BY s.username, s.peer
ORDER BY execution_count DESC;
```

### Find failed executions

```sql
SELECT
    sql_text,
    bind_parameters,
    error_message,
    execution_start_time,
    username
FROM _gizmosql_instr.execution_details
WHERE status = 'error'
ORDER BY execution_start_time DESC
LIMIT 20;
```

### Monitor currently executing queries

```sql
SELECT
    st.sql_text,
    e.bind_parameters,
    s.username,
    s.peer,
    e.execution_start_time,
    EPOCH(now()) - EPOCH(e.execution_start_time) as running_seconds
FROM _gizmosql_instr.sql_executions e
JOIN _gizmosql_instr.sql_statements st ON e.statement_id = st.statement_id
JOIN _gizmosql_instr.sessions s ON st.session_id = s.session_id
WHERE e.status = 'executing'
ORDER BY running_seconds DESC;
```

### Track prepared statement reuse

```sql
SELECT
    st.statement_id,
    st.sql_text,
    COUNT(e.execution_id) as execution_count,
    AVG(e.duration_ms) as avg_duration_ms,
    SUM(e.rows_fetched) as total_rows
FROM _gizmosql_instr.sql_statements st
JOIN _gizmosql_instr.sql_executions e ON st.statement_id = e.statement_id
GROUP BY st.statement_id, st.sql_text
HAVING COUNT(e.execution_id) > 1
ORDER BY execution_count DESC
LIMIT 20;
```

---

## Tracing

The `statement_id` and `execution_id` fields in the instrumentation tables match the IDs used in GizmoSQL logs. This allows you to correlate log entries with instrumentation records for debugging:

```sql
-- Find all executions for a statement_id from the logs
SELECT * FROM _gizmosql_instr.execution_details
WHERE statement_id = '12345678-1234-1234-1234-123456789012';
```

---

## Troubleshooting

### Instrumentation data not appearing

The instrumentation system uses an asynchronous write queue. Data may take a few milliseconds to appear after query execution. If running automated tests, add a small delay before querying instrumentation tables.

### Cannot find my session

Ensure you're querying from the same connection. Each connection has a unique session ID. Use `GIZMOSQL_CURRENT_SESSION()` to verify your session ID.

### Performance impact

Instrumentation writes are asynchronous and have minimal impact on query performance. The instrumentation database is separate from your main database, so instrumentation I/O does not block your queries.

### Unclean shutdown recovery

If the GizmoSQL server terminates unexpectedly (e.g., SIGKILL, crash, power loss), some records may remain in an incomplete state:
- Instances may show `status = 'running'`
- Sessions may show `status = 'active'`
- Executions may show `status = 'executing'`

On the next server startup, GizmoSQL automatically cleans up these stale records:
- Stale instances are marked as `stopped` with `stop_reason = 'unclean_shutdown'`
- Stale sessions are marked as `closed` with `stop_reason = 'unclean_shutdown'`
- Stale executions are marked as `error` with `error_message = 'Server shutdown unexpectedly'`

This cleanup happens before the server accepts new connections, ensuring instrumentation data remains consistent.
