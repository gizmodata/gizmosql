# DuckLake Integration

[DuckLake](https://ducklake.select/) is an open table format for DuckDB that provides transactional lakehouse capabilities. GizmoSQL fully supports DuckLake, allowing you to use it as a transactional data lakehouse with Arrow Flight SQL connectivity.

## Overview

DuckLake stores:
- **Metadata** in a relational database (PostgreSQL, MySQL, or DuckDB)
- **Data** as Parquet files in local storage or cloud object storage (S3, GCS, Azure Blob)

This architecture enables ACID transactions, time travel, and schema evolution while leveraging DuckDB's analytical performance.

## Prerequisites

### PostgreSQL for Metadata Storage

DuckLake requires a metadata store. PostgreSQL is recommended for production use. You can start a PostgreSQL instance using Docker:

```bash
docker run --name ducklake-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=your_password \
  -e POSTGRES_DB=ducklake_catalog \
  -p 5432:5432 \
  -d postgres:16-alpine
```

Or use the provided docker-compose file:

```yaml
# docker-compose.yml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: your_password
      POSTGRES_DB: ducklake_catalog
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

Start with:
```bash
docker-compose up -d
```

### GizmoSQL Server

Start the GizmoSQL server:

```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server \
  --database-filename mydb.duckdb \
  --print-queries
```

Or using Docker:

```bash
docker run --name gizmosql \
  --detach \
  --rm \
  --tty \
  --init \
  --publish 31337:31337 \
  --env GIZMOSQL_PASSWORD="gizmosql_password" \
  --env PRINT_QUERIES="1" \
  --network host \
  gizmodata/gizmosql:latest
```

> **Note**: Use `--network host` or ensure the GizmoSQL container can reach PostgreSQL.

## Setting Up DuckLake

Connect to GizmoSQL using any Flight SQL client (JDBC, ADBC, CLI). The following examples use SQL commands.

### Step 1: Install and Load Extensions

```sql
-- Install DuckLake and PostgreSQL extensions
INSTALL ducklake;
INSTALL postgres;

-- Load the extensions
LOAD ducklake;
LOAD postgres;
```

### Step 2: Create PostgreSQL Secret

Create a secret to authenticate with your PostgreSQL metadata store:

```sql
CREATE OR REPLACE SECRET postgres_secret (
  TYPE postgres,
  HOST 'localhost',
  PORT 5432,
  DATABASE 'ducklake_catalog',
  USER 'postgres',
  PASSWORD 'your_password'
);
```

### Step 3: Create DuckLake Secret

Create a DuckLake secret that references the PostgreSQL secret and specifies where to store data:

```sql
CREATE OR REPLACE SECRET ducklake_secret (
  TYPE DUCKLAKE,
  METADATA_PATH '',
  DATA_PATH 'data/my_lakehouse/',
  METADATA_PARAMETERS MAP {
    'TYPE': 'postgres',
    'SECRET': 'postgres_secret'
  }
);
```

**Configuration options:**

| Parameter | Description |
|-----------|-------------|
| `METADATA_PATH` | Schema/namespace in the metadata store (empty for default) |
| `DATA_PATH` | Directory for Parquet files (local path or cloud URI) |
| `METADATA_PARAMETERS` | Connection details for metadata store |

### Step 4: Attach DuckLake

Attach the DuckLake database to your session:

```sql
ATTACH 'ducklake:ducklake_secret' AS my_lakehouse;
```

### Step 5: Use DuckLake

Switch to the DuckLake database:

```sql
USE my_lakehouse;
```

## Working with DuckLake Tables

### Create Tables

```sql
CREATE TABLE customers (
  id INTEGER,
  name VARCHAR,
  email VARCHAR,
  created_at TIMESTAMP
);
```

### Insert Data

```sql
INSERT INTO customers VALUES
  (1, 'Alice Johnson', 'alice@example.com', '2024-01-15 10:30:00'),
  (2, 'Bob Smith', 'bob@example.com', '2024-01-16 14:20:00'),
  (3, 'Carol White', 'carol@example.com', '2024-01-17 09:45:00');
```

### Query Data

```sql
SELECT * FROM customers ORDER BY created_at DESC;
```

### Aggregations

```sql
SELECT
  DATE_TRUNC('day', created_at) as day,
  COUNT(*) as new_customers
FROM customers
GROUP BY 1
ORDER BY 1;
```

## Cloud Storage Integration

DuckLake supports storing data in various cloud object storage providers:

- **Amazon S3**
- **Google Cloud Storage (GCS)**
- **Azure Blob Storage**
- **Cloudflare R2**
- **MinIO** (S3-compatible)
- **Local filesystem** (for development/testing)

Configure the `DATA_PATH` in your DuckLake secret to point to your preferred storage. For comprehensive documentation on all storage options and configuration, see the [official DuckLake documentation](https://ducklake.select/docs/stable/).

Below are examples for common cloud providers:

### Amazon S3

```sql
-- Create S3 credentials secret
CREATE OR REPLACE SECRET s3_secret (
  TYPE S3,
  KEY_ID 'your_access_key',
  SECRET 'your_secret_key',
  REGION 'us-east-1'
);

-- Create DuckLake with S3 storage
CREATE OR REPLACE SECRET ducklake_s3_secret (
  TYPE DUCKLAKE,
  METADATA_PATH '',
  DATA_PATH 's3://my-bucket/lakehouse/',
  METADATA_PARAMETERS MAP {
    'TYPE': 'postgres',
    'SECRET': 'postgres_secret'
  }
);

ATTACH 'ducklake:ducklake_s3_secret' AS s3_lakehouse;
```

### Google Cloud Storage

```sql
-- Create GCS credentials secret
CREATE OR REPLACE SECRET gcs_secret (
  TYPE GCS,
  KEY_ID 'your_key_id',
  SECRET 'your_secret'
);

-- Create DuckLake with GCS storage
CREATE OR REPLACE SECRET ducklake_gcs_secret (
  TYPE DUCKLAKE,
  METADATA_PATH '',
  DATA_PATH 'gs://my-bucket/lakehouse/',
  METADATA_PARAMETERS MAP {
    'TYPE': 'postgres',
    'SECRET': 'postgres_secret'
  }
);

ATTACH 'ducklake:ducklake_gcs_secret' AS gcs_lakehouse;
```

## Python Client Example

Using the ADBC Flight SQL driver:

```python
import os
from adbc_driver_flightsql import dbapi as flight_sql, DatabaseOptions

# Connect to GizmoSQL
conn = flight_sql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
    },
    autocommit=True
)

cursor = conn.cursor()

# Set up DuckLake
cursor.execute("INSTALL ducklake; LOAD ducklake;")
cursor.execute("INSTALL postgres; LOAD postgres;")

cursor.execute("""
    CREATE OR REPLACE SECRET postgres_secret (
        TYPE postgres,
        HOST 'localhost',
        PORT 5432,
        DATABASE 'ducklake_catalog',
        USER 'postgres',
        PASSWORD 'your_password'
    )
""")

cursor.execute("""
    CREATE OR REPLACE SECRET ducklake_secret (
        TYPE DUCKLAKE,
        METADATA_PATH '',
        DATA_PATH 'data/lakehouse/',
        METADATA_PARAMETERS MAP {'TYPE': 'postgres', 'SECRET': 'postgres_secret'}
    )
""")

cursor.execute("ATTACH 'ducklake:ducklake_secret' AS lakehouse")
cursor.execute("USE lakehouse")

# Create and query a table
cursor.execute("""
    CREATE OR REPLACE TABLE events (
        event_id INTEGER,
        event_type VARCHAR,
        event_time TIMESTAMP
    )
""")

cursor.execute("""
    INSERT INTO events VALUES
        (1, 'click', '2024-01-20 10:00:00'),
        (2, 'view', '2024-01-20 10:01:00'),
        (3, 'purchase', '2024-01-20 10:05:00')
""")

cursor.execute("SELECT * FROM events ORDER BY event_time")
result = cursor.fetch_arrow_table()
print(result.to_pandas())

cursor.close()
conn.close()
```

## Session Setup with Init Commands

For production deployments, you can configure GizmoSQL to automatically set up DuckLake on startup using init commands:

```bash
GIZMOSQL_PASSWORD="gizmosql_password" gizmosql_server \
  --database-filename mydb.duckdb \
  --init-sql-commands "INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;"
```

Or via environment variable:

```bash
docker run --name gizmosql \
  -e GIZMOSQL_PASSWORD="gizmosql_password" \
  -e INIT_SQL_COMMANDS="INSTALL ducklake; INSTALL postgres; LOAD ducklake; LOAD postgres;" \
  -p 31337:31337 \
  gizmodata/gizmosql:latest
```

> **Note**: Secrets and ATTACH commands must be run per-session as they are session-scoped.

## Best Practices

1. **Use PostgreSQL for production metadata**: While DuckLake supports other metadata stores, PostgreSQL provides the best reliability and concurrent access.

2. **Separate metadata and data**: Store metadata in PostgreSQL and data in cloud object storage for scalability.

3. **Use secrets management**: Store credentials in secrets rather than hardcoding them in queries.

4. **Monitor with instrumentation**: GizmoSQL Enterprise's session instrumentation captures all DuckLake queries for auditing and performance analysis.

5. **Use transactions**: DuckLake supports ACID transactions - use them for data consistency.

## Troubleshooting

### Connection Issues

If you cannot connect to PostgreSQL:

```sql
-- Test PostgreSQL connectivity directly
INSTALL postgres;
LOAD postgres;
SELECT * FROM postgres_query('postgres_secret', 'SELECT 1');
```

### Extension Not Found

Ensure extensions are installed and loaded:

```sql
-- List installed extensions
SELECT * FROM duckdb_extensions() WHERE installed = true;

-- Force reinstall if needed
FORCE INSTALL ducklake;
LOAD ducklake;
```

### Permission Errors

Verify the PostgreSQL user has permissions to create tables in the metadata database:

```sql
-- In PostgreSQL, grant permissions
GRANT ALL PRIVILEGES ON DATABASE ducklake_catalog TO postgres;
```

## Additional Resources

- [DuckLake Official Documentation](https://ducklake.select/docs/stable/) - Comprehensive guide to DuckLake features, storage options, and configuration
- [DuckLake Website](https://ducklake.select/)
- [DuckDB PostgreSQL Extension](https://duckdb.org/docs/extensions/postgres.html)
- [GizmoSQL Session Instrumentation](session_instrumentation.md) *(Enterprise)*
