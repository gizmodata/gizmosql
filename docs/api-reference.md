# API Reference

Complete reference for GizmoSQL server and client APIs.

---

## Server API

### Command-Line Options

#### gizmosql_server

Start the GizmoSQL server.

**Syntax:**
```bash
gizmosql_server [OPTIONS]
```

**Options:**

| Option | Short | Type | Description | Default |
|--------|-------|------|-------------|---------|
| `--database-filename` | `-D` | string | Path to database file | `data/TPC-H-small.duckdb` |
| `--backend` | `-B` | string | Backend engine: `duckdb` or `sqlite` | `duckdb` |
| `--host` | `-h` | string | Host address to bind to | `0.0.0.0` |
| `--port` | `-p` | integer | Port number to listen on | `31337` |
| `--print-queries` | - | flag | Print executed queries to stdout | `false` |
| `--query-timeout` | - | integer | Query timeout in seconds (0 = unlimited) | `0` |
| `--tls-cert` | - | string | Path to TLS certificate file | - |
| `--tls-key` | - | string | Path to TLS private key file | - |
| `--tls-ca` | - | string | Path to TLS CA certificate file | - |
| `--disable-tls` | - | flag | Disable TLS encryption | `false` |
| `--help` | - | flag | Display help message | - |
| `--version` | - | flag | Display version information | - |

**Environment Variables:**

| Variable | Description | Overrides |
|----------|-------------|-----------|
| `GIZMOSQL_PASSWORD` | Server password (required) | - |
| `DATABASE_FILENAME` | Database file path | `--database-filename` |
| `GIZMOSQL_HOST` | Host address | `--host` |
| `TLS_ENABLED` | Enable/disable TLS (1/0) | - |
| `TLS_CERT` | TLS certificate path | `--tls-cert` |
| `TLS_KEY` | TLS key path | `--tls-key` |
| `TLS_CA` | TLS CA path | `--tls-ca` |
| `PRINT_QUERIES` | Print queries (1/0) | `--print-queries` |
| `INIT_SQL_COMMANDS` | Initialization SQL commands | - |
| `INIT_SQL_COMMANDS_FILE` | Init SQL commands file path | - |

**Example:**
```bash
GIZMOSQL_PASSWORD="secure_pass" \
  gizmosql_server \
  --database-filename /data/mydb.duckdb \
  --backend duckdb \
  --port 31337 \
  --print-queries \
  --query-timeout 300
```

---

### gizmosql_client

Execute queries from command line.

**Syntax:**
```bash
gizmosql_client [OPTIONS]
```

**Options:**

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `--command` | string | Command to execute (e.g., `Execute`) | - |
| `--host` | string | Server hostname | `localhost` |
| `--port` | integer | Server port | `31337` |
| `--username` | string | Username for authentication | - |
| `--password` | string | Password for authentication | - |
| `--query` | string | SQL query to execute | - |
| `--use-tls` | flag | Enable TLS encryption | `false` |
| `--tls-skip-verify` | flag | Skip TLS certificate verification | `false` |

**Example:**
```bash
gizmosql_client \
  --command Execute \
  --host localhost \
  --port 31337 \
  --username gizmosql_username \
  --password gizmosql_password \
  --query "SELECT * FROM nation LIMIT 5" \
  --use-tls \
  --tls-skip-verify
```

---

## Client APIs

### Python ADBC

#### Connection

**gizmosql.connect()**

Create a connection to GizmoSQL server.

**Signature:**
```python
connect(
    uri: str,
    db_kwargs: dict = None,
    autocommit: bool = True
) -> Connection
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `uri` | str | Connection URI (e.g., `grpc+tls://host:port`) |
| `db_kwargs` | dict | Connection options |
| `autocommit` | bool | Enable autocommit mode |

**db_kwargs Options:**

| Key | Type | Description |
|-----|------|-------------|
| `username` | str | Username for authentication |
| `password` | str | Password for authentication |
| `DatabaseOptions.TLS_SKIP_VERIFY.value` | str | Skip TLS verification ("true"/"false") |
| `DatabaseOptions.TLS_ROOT_CERTS.value` | str | Path to CA certificates |
| `authorization_header` | str | Bearer token (e.g., `Bearer <token>`) |

**Returns:** Connection object

**Example:**
```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password",
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
    },
    autocommit=True
)
```

#### Cursor

**cursor.execute()**

Execute a SQL statement.

**Signature:**
```python
execute(
    query: str,
    parameters: list = None
) -> None
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `query` | str | SQL query to execute |
| `parameters` | list | Query parameters (optional) |

**Example:**
```python
cur.execute(
    "SELECT * FROM nation WHERE n_nationkey = ?",
    parameters=[24]
)
```

**cursor.fetch_arrow_table()**

Fetch results as PyArrow Table.

**Returns:** `pyarrow.Table`

**Example:**
```python
result = cur.fetch_arrow_table()
print(result)
```

**cursor.fetchall()**

Fetch all results as tuples.

**Returns:** `list[tuple]`

**cursor.fetchone()**

Fetch single result as tuple.

**Returns:** `tuple` or `None`

**cursor.adbc_ingest()**

Bulk insert data.

**Signature:**
```python
adbc_ingest(
    table_name: str,
    data: pyarrow.Table,
    mode: str = "append"
) -> None
```

**Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_name` | str | Target table name |
| `data` | pyarrow.Table | Data to insert |
| `mode` | str | Insert mode: "append" or "replace" |

---

### JDBC

#### Connection URL Format

```text
jdbc:arrow-flight-sql://host:port[?options]
```

**Options:**

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `useEncryption` | boolean | Enable TLS | `false` |
| `user` | string | Username | - |
| `password` | string | Password | - |
| `disableCertificateVerification` | boolean | Skip TLS verification | `false` |
| `connectTimeout` | integer | Connection timeout (ms) | `30000` |
| `requestTimeout` | integer | Request timeout (ms) | `0` |

**Example:**
```text
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=gizmosql_password&disableCertificateVerification=true&connectTimeout=10000
```

---

### SQLAlchemy

#### Connection URL Format

```text
gizmosql://username:password@host:port/?[options]
```

**Options:**

| Option | Type | Description | Default |
|--------|------|-------------|---------|
| `tls` | boolean | Enable TLS | `false` |
| `tls_skip_verify` | boolean | Skip TLS verification | `false` |

**Example:**
```python
from sqlalchemy import create_engine

engine = create_engine(
    "gizmosql://user:pass@localhost:31337/?tls=true&tls_skip_verify=true"
)
```

---

### Ibis

#### Connection

**ibis.gizmosql.connect()**

**Signature:**
```python
connect(
    host: str,
    port: int = 31337,
    username: str = None,
    password: str = None,
    use_tls: bool = False,
    tls_skip_verify: bool = False
) -> Backend
```

**Example:**
```python
import ibis

conn = ibis.gizmosql.connect(
    host="localhost",
    port=31337,
    username="gizmosql_username",
    password="gizmosql_password",
    use_tls=True,
    tls_skip_verify=True
)
```

---

## SQL Functions

### DuckDB Functions

GizmoSQL with DuckDB backend supports all [DuckDB SQL functions](https://duckdb.org/docs/sql/functions/overview).

**Common Functions:**

| Category | Functions |
|----------|-----------|
| Aggregate | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV` |
| String | `CONCAT`, `SUBSTRING`, `UPPER`, `LOWER`, `LENGTH` |
| Date/Time | `NOW()`, `DATE_TRUNC`, `EXTRACT`, `INTERVAL` |
| Math | `ROUND`, `CEIL`, `FLOOR`, `ABS`, `SQRT` |
| Array | `ARRAY_AGG`, `UNNEST`, `LIST_CONTAINS` |
| JSON | `JSON_EXTRACT`, `JSON_ARRAY`, `JSON_OBJECT` |

### SQLite Functions

GizmoSQL with SQLite backend supports all [SQLite core functions](https://sqlite.org/lang_corefunc.html).

---

## HTTP API

GizmoSQL uses Apache Arrow Flight SQL protocol over gRPC, not REST HTTP.

For HTTP access, use the [Flight SQL WebSocket Proxy](https://github.com/gizmodata/flight-sql-websocket-proxy).

---

## Data Types

### DuckDB Types

| SQL Type | Python Type | Arrow Type |
|----------|-------------|------------|
| `BOOLEAN` | bool | bool |
| `TINYINT` | int | int8 |
| `SMALLINT` | int | int16 |
| `INTEGER` | int | int32 |
| `BIGINT` | int | int64 |
| `FLOAT` | float | float32 |
| `DOUBLE` | float | float64 |
| `VARCHAR` | str | string |
| `DATE` | datetime.date | date32 |
| `TIMESTAMP` | datetime.datetime | timestamp |
| `BLOB` | bytes | binary |
| `DECIMAL` | decimal.Decimal | decimal128 |

### Type Mapping

When inserting data, types are automatically converted:

```python
import pyarrow as pa

# Create Arrow table
data = pa.table({
    'id': [1, 2, 3],                    # → INTEGER
    'name': ['Alice', 'Bob', 'Charlie'], # → VARCHAR
    'score': [95.5, 87.3, 92.1],        # → DOUBLE
    'active': [True, False, True]       # → BOOLEAN
})
```

---

## Error Codes

| Code | Message | Description | Solution |
|------|---------|-------------|----------|
| `UNAUTHENTICATED` | Invalid bearer token | Token expired or invalid | Re-authenticate |
| `PERMISSION_DENIED` | Insufficient permissions | Authorization failed | Check credentials |
| `NOT_FOUND` | Table not found | Table doesn't exist | Create table or check name |
| `ALREADY_EXISTS` | Table already exists | Duplicate table | Use `IF NOT EXISTS` |
| `INVALID_ARGUMENT` | Invalid SQL syntax | SQL parse error | Fix query syntax |
| `DEADLINE_EXCEEDED` | Query timeout | Query took too long | Optimize query or increase timeout |
| `RESOURCE_EXHAUSTED` | Out of memory | Insufficient memory | Increase memory limit |
| `INTERNAL` | Internal server error | Server malfunction | Check logs, report bug |

---

## Performance Metrics

### Query Profiling

```sql
-- Enable profiling
SET enable_profiling = true;

-- Run query
SELECT * FROM large_table;

-- View metrics
SELECT * FROM pragma_last_profiling_output();
```

**Metrics:**

- Execution time
- Memory usage
- Rows processed
- CPU time
- I/O operations

---

## Version Information

### Check Version

**Server:**
```bash
gizmosql_server --version
```

**SQL:**
```sql
SELECT version();
```

**Python:**
```python
import adbc_driver_flightsql
print(adbc_driver_flightsql.__version__)
```

---

## Limits

| Resource | Limit | Configurable |
|----------|-------|--------------|
| Max connections | Unlimited | No |
| Max query length | 1 MB | No |
| Max result set size | Memory limit | Yes (`memory_limit`) |
| Max table size | Storage limit | No |
| Max database size | Storage limit | No |
| Query timeout | Configurable | Yes (`query_timeout`) |

---

## Next Steps

- [Client connections](clients.md)
- [Configuration](configuration.md)
- [Troubleshooting](troubleshooting.md)

---

## Additional Resources

- [Apache Arrow Flight SQL Spec](https://arrow.apache.org/docs/format/FlightSql.html)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [SQLite SQL Reference](https://sqlite.org/lang.html)
