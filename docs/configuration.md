# Configuration

This guide covers all configuration options for GizmoSQL, including environment variables, backend selection, and runtime settings.

---

## Environment Variables

GizmoSQL can be configured using environment variables, which is especially useful for containerized deployments.

### Core Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GIZMOSQL_PASSWORD` | Password for server authentication | - | ✅ Yes |
| `DATABASE_FILENAME` | Path to database file | `data/TPC-H-small.duckdb` | No |
| `TLS_ENABLED` | Enable/disable TLS encryption | `1` (enabled) | No |
| `PRINT_QUERIES` | Log SQL queries to stdout | `0` (disabled) | No |

### TLS Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `TLS_CERT` | Path to TLS certificate file | `/opt/gizmosql/tls/cert.pem` | If TLS enabled |
| `TLS_KEY` | Path to TLS private key file | `/opt/gizmosql/tls/key.pem` | If TLS enabled |
| `TLS_CA` | Path to TLS CA certificate file | - | Optional |

### Initialization

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `INIT_SQL_COMMANDS` | Semicolon-separated SQL commands to run on startup | - | No |
| `INIT_SQL_COMMANDS_FILE` | Path to file containing initialization SQL commands | - | No |

### Network Configuration

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `GIZMOSQL_HOST` | Host address to bind to | `0.0.0.0` | No |
| `GIZMOSQL_PORT` | Port number to listen on | `31337` | No |

---

## Command-Line Options

When running `gizmosql_server` directly, you can use command-line arguments:

### Basic Options

```bash
gizmosql_server --help
```

| Option | Short | Description |
|--------|-------|-------------|
| `--database-filename` | `-D` | Path to database file |
| `--backend` | `-B` | Backend engine: `duckdb` or `sqlite` |
| `--print-queries` | - | Print executed queries to stdout |
| `--query-timeout` | - | Query timeout in seconds (0 = unlimited) |
| `--port` | `-p` | Port number to listen on |
| `--host` | `-h` | Host address to bind to |

### TLS Options

| Option | Description |
|--------|-------------|
| `--tls-cert` | Path to TLS certificate file |
| `--tls-key` | Path to TLS private key file |
| `--tls-ca` | Path to TLS CA certificate file |
| `--disable-tls` | Disable TLS encryption |

### Example

```bash
GIZMOSQL_PASSWORD="my_password" \
  gizmosql_server \
  --database-filename /data/my_database.duckdb \
  --backend duckdb \
  --print-queries \
  --query-timeout 30 \
  --port 31337
```

---

## Backend Selection

GizmoSQL supports two backend engines: **DuckDB** (default) and **SQLite**.

### DuckDB Backend

DuckDB is optimized for analytical queries and OLAP workloads.

```bash
# Using command-line option
gizmosql_server --backend duckdb --database-filename data/analytics.duckdb

# Or shorthand
gizmosql_server -B duckdb -D data/analytics.duckdb

# Environment variable
export GIZMOSQL_BACKEND="duckdb"
```

**Features:**
- ✅ Analytical query optimization
- ✅ Columnar storage
- ✅ Parallel query execution
- ✅ Advanced analytics functions
- ✅ Auto-install/auto-load extensions

**Default Init Commands:**
```sql
SET autoinstall_known_extensions = true;
SET autoload_known_extensions = true;
```

### SQLite Backend

SQLite is optimized for transactional workloads and embedded use cases.

```bash
# Using command-line option
gizmosql_server --backend sqlite --database-filename data/app.sqlite

# Or shorthand
gizmosql_server -B sqlite -D data/app.sqlite
```

**Features:**
- ✅ ACID compliance
- ✅ Transactional workloads
- ✅ Small footprint
- ✅ Cross-platform compatibility

### Choosing a Backend

| Use Case | Recommended Backend |
|----------|---------------------|
| Analytics & OLAP | DuckDB |
| Transactional & OLTP | SQLite |
| Data warehousing | DuckDB |
| Embedded applications | SQLite |
| Large aggregations | DuckDB |
| High concurrency writes | SQLite |

---

## Initialization Commands

You can run SQL commands automatically when GizmoSQL starts.

### Using Environment Variable

```bash
docker run ... \
  --env INIT_SQL_COMMANDS="SET threads = 4; SET memory_limit = '2GB';"
```

### Using a File

Create an initialization file:

```sql
-- init.sql
SET threads = 4;
SET memory_limit = '2GB';
CREATE SCHEMA IF NOT EXISTS analytics;
LOAD httpfs;
```

Then mount and use it:

```bash
docker run ... \
  --mount type=bind,source=$(pwd)/init.sql,target=/opt/gizmosql/init.sql \
  --env INIT_SQL_COMMANDS_FILE="/opt/gizmosql/init.sql"
```

### DuckDB-Specific Settings

<!-- tabs:start -->

#### **Memory Configuration**

```sql
SET memory_limit = '4GB';
SET temp_directory = '/tmp/duckdb';
```

#### **Thread Configuration**

```sql
SET threads = 8;
SET external_threads = 4;
```

#### **Extensions**

```sql
INSTALL httpfs;
LOAD httpfs;
INSTALL parquet;
LOAD parquet;
```

#### **Query Optimization**

```sql
SET enable_optimizer = true;
SET enable_profiling = true;
SET explain_output = true;
```

<!-- tabs:end -->

!> **Important**: Init commands that return results (SELECT statements) will not display output. Failed commands will cause the server to exit with a non-zero code.

---

## Query Timeout

Set a maximum execution time for queries to prevent runaway queries.

### Command-Line

```bash
gizmosql_server --query-timeout 30
```

### Docker

```bash
docker run ... \
  --env QUERY_TIMEOUT="30"
```

- `0` = Unlimited (default)
- `> 0` = Timeout in seconds

?> **Tip**: Set reasonable timeouts in production to prevent resource exhaustion from long-running queries.

---

## Database Configuration

### DuckDB Settings

Common DuckDB settings you can set via initialization commands:

| Setting | Description | Example |
|---------|-------------|---------|
| `threads` | Number of threads for query execution | `SET threads = 8;` |
| `memory_limit` | Maximum memory usage | `SET memory_limit = '4GB';` |
| `temp_directory` | Directory for temporary files | `SET temp_directory = '/tmp';` |
| `default_order` | Default result ordering | `SET default_order = 'DESC';` |

### SQLite Settings

SQLite PRAGMA statements:

| Setting | Description | Example |
|---------|-------------|---------|
| `journal_mode` | Journal mode | `PRAGMA journal_mode = WAL;` |
| `cache_size` | Cache size in pages | `PRAGMA cache_size = 10000;` |
| `synchronous` | Synchronous mode | `PRAGMA synchronous = NORMAL;` |
| `temp_store` | Temp storage location | `PRAGMA temp_store = MEMORY;` |

---

## Docker Compose Example

Here's a complete `docker-compose.yml` configuration:

```yaml
version: '3.8'

services:
  gizmosql:
    image: gizmodata/gizmosql:latest
    container_name: gizmosql
    restart: unless-stopped
    ports:
      - "31337:31337"
    environment:
      GIZMOSQL_PASSWORD: "${GIZMOSQL_PASSWORD}"
      TLS_ENABLED: "1"
      PRINT_QUERIES: "1"
      DATABASE_FILENAME: "/data/analytics.duckdb"
      INIT_SQL_COMMANDS: "SET threads = 4; SET memory_limit = '2GB';"
    volumes:
      - ./data:/data
      - ./tls:/opt/gizmosql/tls
    healthcheck:
      test: ["CMD", "gizmosql_client", "--command", "Execute", "--query", "SELECT 1"]
      interval: 30s
      timeout: 10s
      retries: 3
```

---

## Kubernetes Configuration

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: gizmosql-config
data:
  init.sql: |
    SET threads = 4;
    SET memory_limit = '2GB';
    CREATE SCHEMA IF NOT EXISTS analytics;
```

### Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gizmosql
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gizmosql
  template:
    metadata:
      labels:
        app: gizmosql
    spec:
      containers:
      - name: gizmosql
        image: gizmodata/gizmosql:latest
        ports:
        - containerPort: 31337
        env:
        - name: GIZMOSQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: gizmosql-secret
              key: password
        - name: TLS_ENABLED
          value: "1"
        - name: PRINT_QUERIES
          value: "1"
        - name: INIT_SQL_COMMANDS_FILE
          value: "/config/init.sql"
        volumeMounts:
        - name: config
          mountPath: /config
        - name: data
          mountPath: /data
      volumes:
      - name: config
        configMap:
          name: gizmosql-config
      - name: data
        persistentVolumeClaim:
          claimName: gizmosql-data
```

---

## Performance Tuning

### Memory Settings

For analytical workloads with large datasets:

```sql
SET memory_limit = '16GB';
SET max_memory = '16GB';
SET temp_directory = '/fast-storage/tmp';
```

### Thread Configuration

Match CPU cores:

```sql
-- For 8-core system
SET threads = 8;
SET external_threads = 4;
```

### Query Optimization

```sql
SET enable_optimizer = true;
SET optimizer_join_order = true;
SET enable_index_scan = true;
```

---

## Logging

### Enable Query Logging

```bash
# Via environment variable
export PRINT_QUERIES="1"

# Via command-line
gizmosql_server --print-queries
```

### Docker Logging

```bash
# View logs
docker logs gizmosql

# Follow logs
docker logs -f gizmosql

# Last 100 lines
docker logs --tail 100 gizmosql
```

---

## Security Best Practices

1. **Always use TLS in production**
   ```bash
   TLS_ENABLED="1"
   ```

2. **Use strong passwords**
   ```bash
   GIZMOSQL_PASSWORD="$(openssl rand -base64 32)"
   ```

3. **Restrict network access**
   ```yaml
   environment:
     GIZMOSQL_HOST: "127.0.0.1"  # Local only
   ```

4. **Use secrets management**
   - Kubernetes Secrets
   - Docker Secrets
   - HashiCorp Vault

5. **Set query timeouts**
   ```bash
   --query-timeout 60
   ```

---

## Configuration Examples

### Development Environment

```bash
docker run --name gizmosql-dev \
  --rm \
  --publish 31337:31337 \
  --env TLS_ENABLED="0" \
  --env GIZMOSQL_PASSWORD="dev_password" \
  --env PRINT_QUERIES="1" \
  gizmodata/gizmosql:latest
```

### Production Environment

```bash
docker run --name gizmosql-prod \
  --detach \
  --restart unless-stopped \
  --publish 31337:31337 \
  --env TLS_ENABLED="1" \
  --env GIZMOSQL_PASSWORD="${SECURE_PASSWORD}" \
  --env QUERY_TIMEOUT="300" \
  --env INIT_SQL_COMMANDS="SET threads = 16; SET memory_limit = '32GB';" \
  --mount type=bind,source=/secure/tls,target=/opt/gizmosql/tls \
  --mount type=bind,source=/data/prod,target=/data \
  gizmodata/gizmosql:latest
```

---

## Troubleshooting

### Check Current Configuration

```sql
-- DuckDB
SELECT * FROM duckdb_settings();

-- SQLite
PRAGMA compile_options;
```

### Verify Backend

```sql
SELECT version();
```

### Test Initialization

```bash
# Check logs for init command execution
docker logs gizmosql | grep "Init SQL"
```

---

## Next Steps

- [Secure your deployment](security.md)
- [Optimize performance](performance.md)
- [Connect clients](clients.md)

---

## Reference

- [DuckDB Configuration](https://duckdb.org/docs/configuration/overview)
- [SQLite PRAGMA Statements](https://sqlite.org/pragma.html)
- [Docker Environment Variables](https://docs.docker.com/compose/environment-variables/)
