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

### Database Location (File-Based)

By default, the instrumentation database is created in the same directory as the main database file:
- If `--database-filename` is `/path/to/mydb.db`, instrumentation is stored at `/path/to/gizmosql_instrumentation.db`
- If using in-memory mode (`:memory:`), instrumentation is stored in the current working directory

You can override the location using the environment variable:

```bash
export GIZMOSQL_INSTRUMENTATION_DB_PATH=/custom/path/instrumentation.db
```

### Using DuckLake as Instrumentation Backend

For enterprise deployments, you can store instrumentation data in a DuckLake catalog instead of a local file. This enables:
- Centralized instrumentation across multiple GizmoSQL instances
- Cloud-based storage (S3, Azure Blob, etc.) for instrumentation data
- Transactional consistency with ACID guarantees from DuckLake

#### Configuration Parameters

| Parameter | CLI Flag | Env Var | Default | Description |
|-----------|----------|---------|---------|-------------|
| `instrumentation_catalog` | `--instrumentation-catalog` | `GIZMOSQL_INSTRUMENTATION_CATALOG` | (empty) | Catalog name for instrumentation. If set, uses pre-attached catalog instead of file. |
| `instrumentation_schema` | `--instrumentation-schema` | `GIZMOSQL_INSTRUMENTATION_SCHEMA` | `main` | Schema within the catalog |

When `instrumentation_catalog` is set:
- The catalog must be pre-attached via `--init-sql-commands`
- `instrumentation_db_path` is ignored
- GizmoSQL will create the instrumentation tables in the specified catalog/schema

**Important: Dedicated Catalog Required**

The instrumentation catalog is protected as **read-only** for clients (only administrators can read the data, no one can modify it). This protection applies to the **entire catalog**, not just the instrumentation schema.

**Do NOT** use a shared catalog that contains other application tables. If you do, you will not be able to modify any tables in that catalog. Always use a dedicated catalog for instrumentation.

```
✓ Good: instr_ducklake (dedicated catalog for instrumentation)
✗ Bad:  my_app_ducklake with instrumentation in 'main' schema (entire catalog becomes read-only)
```

#### Example: Using Persistent Secrets (Recommended)

With persistent secrets, you only need to create the secrets once. DuckDB stores them in `~/.duckdb/stored_secrets` and automatically loads them on subsequent startups.

**Initial setup (first time only):**

```bash
GIZMOSQL_PASSWORD="password" gizmosql_server \
  --database-filename mydb.db \
  --enable-instrumentation=true \
  --instrumentation-catalog=instr_ducklake \
  --init-sql-commands="
    INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
    CREATE PERSISTENT SECRET pg_secret (TYPE postgres, HOST 'localhost', PORT 5432, DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'password');
    CREATE PERSISTENT SECRET ducklake_secret (TYPE DUCKLAKE, METADATA_PATH '', DATA_PATH 's3://mybucket/instrumentation/', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_secret'});
    ATTACH 'ducklake:ducklake_secret' AS instr_ducklake;
  "
```

**Subsequent startups (secrets already persisted):**

```bash
GIZMOSQL_PASSWORD="password" gizmosql_server \
  --database-filename mydb.db \
  --enable-instrumentation=true \
  --instrumentation-catalog=instr_ducklake \
  --init-sql-commands="
    LOAD ducklake; LOAD postgres;
    ATTACH 'ducklake:ducklake_secret' AS instr_ducklake;
  "
```

#### Example: Using Session Secrets

If you prefer not to persist secrets, create them each startup:

```bash
GIZMOSQL_PASSWORD="password" gizmosql_server \
  --database-filename mydb.db \
  --enable-instrumentation=true \
  --instrumentation-catalog=instr_ducklake \
  --init-sql-commands="
    INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;
    CREATE OR REPLACE SECRET pg_secret (TYPE postgres, HOST 'localhost', PORT 5432, DATABASE 'ducklake_catalog', USER 'postgres', PASSWORD 'password');
    CREATE OR REPLACE SECRET ducklake_secret (TYPE DUCKLAKE, METADATA_PATH '', DATA_PATH 's3://mybucket/instrumentation/', METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'pg_secret'});
    ATTACH 'ducklake:ducklake_secret' AS instr_ducklake;
  "
```

#### Multiple GizmoSQL Instances

When running multiple GizmoSQL instances with shared DuckLake instrumentation:
- All instances can share the same DuckLake catalog for centralized monitoring
- DuckLake provides transactional isolation between concurrent writers
- Each instance gets a unique `instance_id` for correlation
- Use the `instances` table to track all connected servers

**Note:** Persistent secrets are stored in unencrypted binary format in `~/.duckdb/stored_secrets`. You can customize this location with `SET secret_directory = '/path/to/secrets';` if needed.

### Enabling/Disabling Instrumentation

Session instrumentation is **disabled by default**. To enable it, use the `--enable-instrumentation` flag:

```bash
# Instrumentation disabled (default)
gizmosql_server --database-filename /path/to/mydb.db --password mypassword

# Explicitly enabled
gizmosql_server --database-filename /path/to/mydb.db --password mypassword --enable-instrumentation=true

# Explicitly disabled
gizmosql_server --database-filename /path/to/mydb.db --password mypassword --enable-instrumentation=false
```

When instrumentation is disabled:
- No instrumentation database is created
- The `_gizmosql_instr` schema is not attached to user sessions
- Session, statement, and execution tracking is disabled
- Queries to `_gizmosql_instr.*` tables/views will fail (schema doesn't exist)

Note that `GIZMOSQL_CURRENT_SESSION()`, `GIZMOSQL_CURRENT_INSTANCE()`, `GIZMOSQL_VERSION()`, `GIZMOSQL_USER()`, `GIZMOSQL_ROLE()`, and `KILL SESSION` still work when instrumentation is disabled - they just won't be recorded in the instrumentation tables.

Disabling instrumentation may be useful for:
- Development/testing environments where audit trails are not needed
- Performance-sensitive deployments where the overhead of instrumentation is undesirable
- Environments where the additional database file is not desired

---

## Schema

### Table Location

The location of instrumentation tables depends on your configuration:

| Mode | Table Location | Example Query |
|------|----------------|---------------|
| File-based (default) | `_gizmosql_instr.main.*` | `SELECT * FROM _gizmosql_instr.sessions` |
| DuckLake/External catalog | `{catalog}.{schema}.*` | `SELECT * FROM instr_ducklake.main.sessions` |

When using file-based instrumentation, GizmoSQL automatically attaches the instrumentation database as `_gizmosql_instr` and all examples in this documentation use that name.

When using DuckLake or another external catalog (via `--instrumentation-catalog`), set the schema context with `USE` before running queries. For example, if you configured `--instrumentation-catalog=instr_ducklake --instrumentation-schema=main`:

```sql
-- Set the schema context once
USE instr_ducklake.main;

-- Then query tables/views without prefixes
SELECT * FROM active_sessions;
SELECT * FROM sessions WHERE status = 'active';
```

### Tables

The instrumentation database contains four core tables:

#### `instances`

Tracks server instance lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `instance_id` | UUID | Primary key |
| `gizmosql_version` | VARCHAR | GizmoSQL version |
| `gizmosql_edition` | VARCHAR | GizmoSQL edition ('Core' or 'Enterprise') |
| `duckdb_version` | VARCHAR | DuckDB version |
| `arrow_version` | VARCHAR | Apache Arrow version |
| `hostname` | VARCHAR | Resolved server hostname |
| `hostname_arg` | VARCHAR | Hostname argument passed to server (NULL if not specified) |
| `server_ip` | VARCHAR | Server IP address |
| `port` | INTEGER | Server port |
| `database_path` | VARCHAR | Path to main database |
| `tls_enabled` | BOOLEAN | Whether TLS is enabled |
| `tls_cert_path` | VARCHAR | Path to TLS certificate file (NULL if TLS disabled) |
| `tls_key_path` | VARCHAR | Path to TLS key file (NULL if TLS disabled) |
| `mtls_required` | BOOLEAN | Whether mutual TLS (mTLS) is required |
| `mtls_ca_cert_path` | VARCHAR | Path to mTLS CA certificate file (NULL if mTLS disabled) |
| `readonly` | BOOLEAN | Whether the instance is started in read-only mode |
| `os_platform` | VARCHAR | Operating system platform (e.g., 'darwin', 'linux') |
| `os_name` | VARCHAR | Operating system name (e.g., 'macOS', 'Ubuntu') |
| `os_version` | VARCHAR | Operating system version |
| `cpu_arch` | VARCHAR | CPU architecture (e.g., 'arm64', 'x86_64') |
| `cpu_model` | VARCHAR | CPU model name |
| `cpu_count` | INTEGER | Number of CPU cores |
| `memory_total_bytes` | BIGINT | Total system memory in bytes |
| `start_time` | TIMESTAMP | When server started |
| `stop_time` | TIMESTAMP | When server stopped (NULL if running) |
| `status` | VARCHAR | 'running' or 'stopped' |
| `stop_reason` | VARCHAR | Reason for shutdown |

#### `sessions`

Tracks client session lifecycle.

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | UUID | Primary key |
| `instance_id` | UUID | Reference to instances |
| `username` | VARCHAR | Authenticated username |
| `role` | VARCHAR | User's role (e.g., 'admin', 'user') |
| `auth_method` | VARCHAR | Authentication method (e.g., 'Basic', 'BootstrapToken') |
| `peer` | VARCHAR | Client IP:port (network address) |
| `peer_identity` | VARCHAR | mTLS client certificate identity (NULL if not using mTLS) |
| `user_agent` | VARCHAR | Client user-agent header (e.g., 'ADBC Flight SQL Driver v1.10.0', 'grpc-java-netty/1.65.0') |
| `connection_protocol` | VARCHAR | 'plaintext', 'tls', or 'mtls' |
| `start_time` | TIMESTAMP | When session started |
| `stop_time` | TIMESTAMP | When session ended (NULL if active) |
| `status` | VARCHAR | 'active', 'closed', 'killed', 'timeout', 'error' |
| `stop_reason` | VARCHAR | Reason session ended |

#### `sql_statements`

Tracks prepared statement definitions (one per prepared statement).

| Column | Type | Description |
|--------|------|-------------|
| `statement_id` | UUID | Primary key (same as used in logs) |
| `session_id` | UUID | Reference to sessions |
| `sql_text` | VARCHAR | The SQL query text |
| `flight_method` | VARCHAR | The Flight SQL method that created the statement (e.g., 'DoGetTables', 'CreatePreparedStatement') |
| `is_internal` | BOOLEAN | Whether this is an internal statement (metadata queries, etc.) |
| `prepare_success` | BOOLEAN | Whether statement preparation succeeded |
| `prepare_error` | VARCHAR | Error message if statement preparation failed (NULL if successful) |
| `created_time` | TIMESTAMP | When statement was created |

**Internal vs Client Statements:**
- `is_internal = true`: Statements created by GizmoSQL for metadata queries (DoGetTables, DoGetCatalogs, DoGetPrimaryKeys, etc.)
- `is_internal = false`: Statements explicitly created by the client (CreatePreparedStatement, GetFlightInfoStatement, etc.)

The `flight_method` column tracks which Flight SQL method created the statement for tracing purposes.

**Failed Statements:**
All SQL statements are recorded, including those that fail. The `prepare_success` column indicates whether preparation succeeded, and `prepare_error` contains the error message for failures. This captures:
- Parse/syntax errors
- Unknown table/column references
- Permission violations (readonly user attempting writes, modifying instrumentation database)
- Administrative command failures (KILL SESSION permission denied, session not found)
- Attempts to DETACH the instrumentation database

#### `sql_executions`

Tracks individual executions of statements (one per execution).

| Column | Type | Description |
|--------|------|-------------|
| `execution_id` | UUID | Primary key (same as used in logs) |
| `statement_id` | UUID | Reference to sql_statements |
| `bind_parameters` | VARCHAR | JSON array of bind parameters (NULL if none) |
| `execution_start_time` | TIMESTAMP | When execution started |
| `execution_end_time` | TIMESTAMP | When execution completed |
| `rows_fetched` | BIGINT | Number of rows returned |
| `status` | VARCHAR | 'executing', 'success', 'error', 'timeout', 'cancelled' |
| `error_message` | VARCHAR | Error message if failed |
| `duration_ms` | BIGINT | Execution duration in milliseconds |

### Views

Several convenience views are provided:

#### `active_sessions`

Shows currently active sessions.

```sql
SELECT * FROM active_sessions;
```

Returns: `session_id`, `instance_id`, `username`, `role`, `auth_method`, `peer`, `peer_identity`, `user_agent`, `connection_protocol`, `start_time`, `status`, `hostname`, `hostname_arg`, `server_ip`, `port`, `database_path`, `session_duration_seconds`

#### `session_activity`

Complete view joining instances, sessions, statements, and executions.

```sql
SELECT * FROM session_activity
WHERE username = 'alice'
ORDER BY execution_start_time DESC
LIMIT 100;
```

#### `session_stats`

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
FROM session_stats;
```

#### `execution_details`

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
FROM execution_details
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
    VERSION() AS duckdb_version;
```

### `GIZMOSQL_CURRENT_SESSION()`

Returns the UUID of the current session.

```sql
SELECT GIZMOSQL_CURRENT_SESSION();
-- Returns: '550e8400-e29b-41d4-a716-446655440000'
```

Useful for filtering instrumentation data to the current session:

```sql
SELECT * FROM sql_statements
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
SELECT * FROM sessions
WHERE instance_id = GIZMOSQL_CURRENT_INSTANCE()
ORDER BY start_time DESC;
```

### `GIZMOSQL_USER()`

Returns the username of the current authenticated user.

```sql
SELECT GIZMOSQL_USER();
-- Returns: 'alice'
```

Useful for logging or auditing which user is executing queries:

```sql
SELECT GIZMOSQL_USER() AS current_user,
       GIZMOSQL_ROLE() AS current_role,
       now() AS query_time;
```

### `GIZMOSQL_ROLE()`

Returns the role of the current authenticated user (e.g., `admin`, `user`, `readonly`).

```sql
SELECT GIZMOSQL_ROLE();
-- Returns: 'admin'
```

Useful for conditional logic based on user permissions:

```sql
-- Check if current user has admin privileges
SELECT CASE
    WHEN GIZMOSQL_ROLE() = 'admin' THEN 'Full access'
    WHEN GIZMOSQL_ROLE() = 'readonly' THEN 'Read-only access'
    ELSE 'Standard access'
END AS access_level;
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
FROM active_sessions;

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

- Only users with the `admin` role can query instrumentation tables and views in the `_gizmosql_instr` schema
- Only users with the `admin` role can execute `KILL SESSION`

---

## Example Queries

These examples assume you have set the schema context with `USE`. For file-based instrumentation: `USE _gizmosql_instr.main;`. For DuckLake: `USE {your_catalog}.{your_schema};`.

### Find slow executions

```sql
SELECT
    sql_text,
    bind_parameters,
    duration_ms,
    rows_fetched,
    execution_start_time
FROM execution_details
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
FROM sessions s
LEFT JOIN sql_statements st ON s.session_id = st.session_id
LEFT JOIN sql_executions e ON st.statement_id = e.statement_id
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
FROM execution_details
WHERE status = 'error'
ORDER BY execution_start_time DESC
LIMIT 20;
```

### Find failed statement preparations (parse errors, permission errors)

```sql
SELECT
    st.sql_text,
    st.prepare_error,
    st.created_time,
    s.username,
    s.peer
FROM sql_statements st
JOIN sessions s ON st.session_id = s.session_id
WHERE st.prepare_success = false
ORDER BY st.created_time DESC
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
FROM sql_executions e
JOIN sql_statements st ON e.statement_id = st.statement_id
JOIN sessions s ON st.session_id = s.session_id
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
FROM sql_statements st
JOIN sql_executions e ON st.statement_id = e.statement_id
GROUP BY st.statement_id, st.sql_text
HAVING COUNT(e.execution_id) > 1
ORDER BY execution_count DESC
LIMIT 20;
```

### Filter client-issued vs internal statements

Exclude internal metadata queries to focus on client-issued SQL:

```sql
-- Only client-issued statements
SELECT
    st.sql_text,
    st.flight_method,
    e.duration_ms,
    e.rows_fetched,
    s.username
FROM sql_statements st
JOIN sql_executions e ON st.statement_id = e.statement_id
JOIN sessions s ON st.session_id = s.session_id
WHERE st.is_internal = false
ORDER BY e.execution_start_time DESC
LIMIT 20;

-- Only internal metadata queries
SELECT
    st.flight_method,
    COUNT(*) as query_count,
    AVG(e.duration_ms) as avg_duration_ms
FROM sql_statements st
JOIN sql_executions e ON st.statement_id = e.statement_id
WHERE st.is_internal = true
GROUP BY st.flight_method
ORDER BY query_count DESC;
```

---

## Tracing

The `statement_id` and `execution_id` fields in the instrumentation tables match the IDs used in GizmoSQL logs. This allows you to correlate log entries with instrumentation records for debugging:

```sql
-- Find all executions for a statement_id from the logs
SELECT * FROM execution_details
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

**Note:** When using an external DuckLake catalog for instrumentation (multi-instance deployments), stale record cleanup is disabled. This is because other instances may be legitimately running and should not be marked as stopped. In this scenario, administrators should implement their own cleanup strategy for detecting truly stale instances (e.g., based on heartbeat timeouts or health checks).
