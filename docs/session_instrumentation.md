# Session Instrumentation

GizmoSQL provides built-in session instrumentation for tracking server instances, client sessions, and SQL statement execution. This feature enables SOC2/audit compliance, performance analysis, and session management capabilities.

---

## Overview

Session instrumentation automatically tracks:
- **Server instances**: When the server starts and stops
- **Client sessions**: When clients connect and disconnect, including authentication details
- **SQL statements**: Every SQL query executed, including execution time and row counts

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

The instrumentation database contains three core tables:

#### `_gizmosql_instr.instances`

Tracks server instance lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `instance_id` | UUID | Primary key |
| `gizmosql_version` | VARCHAR | GizmoSQL version |
| `duckdb_version` | VARCHAR | DuckDB version |
| `hostname` | VARCHAR | Server hostname |
| `port` | INTEGER | Server port |
| `database_path` | VARCHAR | Path to main database |
| `start_time` | TIMESTAMPTZ | When server started |
| `stop_time` | TIMESTAMPTZ | When server stopped (NULL if running) |
| `status` | ENUM | 'running' or 'stopped' |
| `stop_reason` | VARCHAR | Reason for shutdown |

#### `_gizmosql_instr.sessions`

Tracks client session lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | UUID | Primary key |
| `instance_id` | UUID | Foreign key to instances |
| `username` | VARCHAR | Authenticated username |
| `role` | VARCHAR | User's role (e.g., 'admin', 'user') |
| `peer` | VARCHAR | Client IP/hostname |
| `start_time` | TIMESTAMPTZ | When session started |
| `stop_time` | TIMESTAMPTZ | When session ended (NULL if active) |
| `status` | ENUM | 'active', 'closed', 'killed', 'timeout', 'error' |
| `stop_reason` | VARCHAR | Reason session ended |

#### `_gizmosql_instr.sql_statements`

Tracks individual SQL statement execution.

| Column | Type | Description |
|--------|------|-------------|
| `statement_id` | UUID | Primary key |
| `session_id` | UUID | Foreign key to sessions |
| `sql_text` | VARCHAR | The SQL query text |
| `statement_handle` | VARCHAR | Internal statement handle |
| `execution_start_time` | TIMESTAMPTZ | When execution started |
| `execution_end_time` | TIMESTAMPTZ | When execution completed |
| `rows_fetched` | BIGINT | Number of rows returned |
| `status` | ENUM | 'executing', 'success', 'error', 'timeout', 'cancelled' |
| `error_message` | VARCHAR | Error message if failed |
| `duration_ms` | BIGINT | Execution duration in milliseconds |

### Views

Several convenience views are provided:

#### `_gizmosql_instr.active_sessions`

Shows currently active sessions.

```sql
SELECT * FROM _gizmosql_instr.active_sessions;
```

Returns: `session_id`, `instance_id`, `username`, `role`, `peer`, `start_time`, `status`, `hostname`, `port`, `database_path`, `session_duration_seconds`

#### `_gizmosql_instr.session_activity`

Complete view joining instances, sessions, and statements.

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
    successful_statements,
    failed_statements,
    total_rows_fetched,
    avg_duration_ms
FROM _gizmosql_instr.session_stats;
```

---

## SQL Functions

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
ORDER BY execution_start_time DESC;
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
DELETE FROM _gizmosql_instr.sql_statements;
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

### Find slow queries

```sql
SELECT
    sql_text,
    duration_ms,
    rows_fetched,
    execution_start_time
FROM _gizmosql_instr.sql_statements
WHERE status = 'success'
ORDER BY duration_ms DESC
LIMIT 10;
```

### Session activity report

```sql
SELECT
    s.username,
    s.peer,
    COUNT(st.statement_id) as query_count,
    SUM(st.rows_fetched) as total_rows,
    AVG(st.duration_ms) as avg_duration_ms
FROM _gizmosql_instr.sessions s
LEFT JOIN _gizmosql_instr.sql_statements st ON s.session_id = st.session_id
WHERE s.start_time > now() - INTERVAL '1 day'
GROUP BY s.username, s.peer
ORDER BY query_count DESC;
```

### Find failed queries

```sql
SELECT
    sql_text,
    error_message,
    execution_start_time,
    username
FROM _gizmosql_instr.sql_statements st
JOIN _gizmosql_instr.sessions s ON st.session_id = s.session_id
WHERE st.status = 'error'
ORDER BY execution_start_time DESC
LIMIT 20;
```

### Monitor currently executing queries

```sql
SELECT
    st.sql_text,
    s.username,
    s.peer,
    st.execution_start_time,
    EPOCH(now()) - EPOCH(st.execution_start_time) as running_seconds
FROM _gizmosql_instr.sql_statements st
JOIN _gizmosql_instr.sessions s ON st.session_id = s.session_id
WHERE st.status = 'executing'
ORDER BY running_seconds DESC;
```

---

## Troubleshooting

### Instrumentation data not appearing

The instrumentation system uses an asynchronous write queue. Data may take a few milliseconds to appear after query execution. If running automated tests, add a small delay before querying instrumentation tables.

### Cannot find my session

Ensure you're querying from the same connection. Each connection has a unique session ID. Use `GIZMOSQL_CURRENT_SESSION()` to verify your session ID.

### Performance impact

Instrumentation writes are asynchronous and have minimal impact on query performance. The instrumentation database is separate from your main database, so instrumentation I/O does not block your queries.
