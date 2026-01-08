# Client Connections

GizmoSQL supports multiple client connection methods for different use cases and programming languages.

---

## Overview

GizmoSQL can be accessed through:

- 🔌 **JDBC** - Java Database Connectivity for Java/JVM applications
- 🐍 **Python ADBC** - Arrow Database Connectivity for Python
- 💻 **CLI Tool** - Command-line interface for quick queries
- 📊 **Ibis** - Python dataframe library
- 🔗 **SQLAlchemy** - Python SQL toolkit and ORM

---

## JDBC Connection

The Apache Arrow Flight SQL JDBC driver provides connectivity for Java applications and database tools like DBeaver.

### Download the Driver

Download the [Apache Arrow Flight SQL JDBC driver](https://search.maven.org/search?q=a:flight-sql-jdbc-driver) from Maven Central.

### Connection String

```text
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=gizmosql_password&disableCertificateVerification=true
```

### Connection Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `useEncryption` | Enable TLS encryption | `false` |
| `user` | Username for authentication | - |
| `password` | Password for authentication | - |
| `disableCertificateVerification` | Skip TLS certificate verification | `false` |

?> **Tip**: For self-signed certificates, use `disableCertificateVerification=true`.

### DBeaver Setup

For detailed instructions on setting up the JDBC driver in DBeaver:

👉 [Setup Guide for DBeaver](https://github.com/gizmodata/setup-arrow-jdbc-driver-in-dbeaver)

### Example Usage

<!-- tabs:start -->

#### **Java**

```java
import java.sql.*;

public class GizmoSQLExample {
    public static void main(String[] args) throws SQLException {
        String url = "jdbc:arrow-flight-sql://localhost:31337" +
                     "?useEncryption=true" +
                     "&user=gizmosql_username" +
                     "&password=gizmosql_password" +
                     "&disableCertificateVerification=true";
        
        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version()")) {
            
            while (rs.next()) {
                System.out.println("Version: " + rs.getString(1));
            }
        }
    }
}
```

#### **Kotlin**

```kotlin
import java.sql.DriverManager

fun main() {
    val url = "jdbc:arrow-flight-sql://localhost:31337" +
              "?useEncryption=true" +
              "&user=gizmosql_username" +
              "&password=gizmosql_password" +
              "&disableCertificateVerification=true"
    
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery("SELECT version()").use { rs ->
                while (rs.next()) {
                    println("Version: ${rs.getString(1)}")
                }
            }
        }
    }
}
```

<!-- tabs:end -->

!> **Note**: If you restart the container with the same password, you may encounter "Invalid bearer token" errors. Change the `GIZMOSQL_PASSWORD` environment variable to resolve this.

---

## Python ADBC

The Arrow Database Connectivity (ADBC) Python driver offers superior performance by minimizing serialization and keeping data in columnar format.

### Benefits of ADBC

- ⚡ **Performance** - Minimizes serialization/deserialization overhead
- 📊 **Columnar Format** - Data stays in Arrow format throughout
- 🔗 **Native Integration** - Works seamlessly with Pandas and PyArrow

Learn more: [Simplifying Database Connectivity with Arrow Flight SQL and ADBC](https://voltrondata.com/resources/simplifying-database-connectivity-with-arrow-flight-sql-and-adbc)

### Installation

```bash
pip install pandas pyarrow adbc_driver_flightsql
```

### Basic Connection

```python
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
        "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT version()")
        result = cur.fetch_arrow_table()
        print(result)
```

### Query with Parameters

```python
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
        "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
            parameters=[24]
        )
        result = cur.fetch_arrow_table()
        print(result)
```

**Expected Output:**

```text
pyarrow.Table
n_nationkey: int32
n_name: string
----
n_nationkey: [[24]]
n_name: [["UNITED STATES"]]
```

### Convert to Pandas

```python
import pandas as pd

# ... connection code ...
with conn.cursor() as cur:
    cur.execute("SELECT * FROM nation LIMIT 10")
    arrow_table = cur.fetch_arrow_table()
    df = arrow_table.to_pandas()
    print(df)
```

### Bulk Insert Example

```python
import pyarrow as pa

# Create sample data
data = pa.table({
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35]
})

with gizmosql.connect(uri="grpc+tls://localhost:31337", ...) as conn:
    with conn.cursor() as cur:
        # Create table
        cur.execute("""
            CREATE TABLE users (
                id INTEGER,
                name VARCHAR,
                age INTEGER
            )
        """)
        
        # Bulk insert using Arrow table
        cur.adbc_ingest("users", data, mode="append")
```

---

## CLI Tool

The `gizmosql_client` CLI tool provides a convenient way to execute queries from the command line.

### Installation

The CLI tool is included in the Docker image and available as a standalone executable.

### Basic Usage

```bash
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "gizmosql_password" \
  --query "SELECT version()" \
  --use-tls \
  --tls-skip-verify
```

**Output:**

```text
Results from endpoint 1 of 1
Schema:
version(): string

Results:
version():   [
    "v1.4.3"
]

Total: 1
```

### CLI Options

| Option | Description |
|--------|-------------|
| `--command` | Command to execute (e.g., `Execute`) |
| `--host` | Server hostname |
| `--port` | Server port (default: 31337) |
| `--username` | Username for authentication |
| `--password` | Password for authentication |
| `--query` | SQL query to execute |
| `--use-tls` | Enable TLS encryption |
| `--tls-skip-verify` | Skip TLS certificate verification |

### Query Examples

<!-- tabs:start -->

#### **Simple SELECT**

```bash
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "gizmosql_password" \
  --query "SELECT * FROM nation LIMIT 5" \
  --use-tls \
  --tls-skip-verify
```

#### **Aggregate Query**

```bash
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "gizmosql_password" \
  --query "SELECT COUNT(*) as total FROM nation" \
  --use-tls \
  --tls-skip-verify
```

#### **Using Environment Variables**

```bash
export GIZMOSQL_HOST="localhost"
export GIZMOSQL_PORT="31337"
export GIZMOSQL_USERNAME="gizmosql_username"
export GIZMOSQL_PASSWORD="gizmosql_password"

gizmosql_client \
  --command Execute \
  --query "SELECT version()" \
  --use-tls \
  --tls-skip-verify
```

<!-- tabs:end -->

---

## Ibis

Ibis is a Python library for working with dataframes across multiple backends.

### Installation

```bash
pip install ibis-gizmosql
```

### Basic Usage

```python
import ibis
import os

# Connect to GizmoSQL
conn = ibis.gizmosql.connect(
    host="localhost",
    port=31337,
    username=os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
    password=os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
    use_tls=True,
    tls_skip_verify=True
)

# List tables
print(conn.list_tables())

# Query using Ibis expressions
nation = conn.table('nation')
result = nation.filter(nation.n_nationkey == 24).execute()
print(result)
```

### Ibis Example - Aggregations

```python
# Complex query with Ibis
nation = conn.table('nation')
result = (
    nation
    .group_by(nation.n_regionkey)
    .aggregate(count=nation.count())
    .order_by(ibis.desc('count'))
    .execute()
)
print(result)
```

📖 **Full Documentation**: [ibis-gizmosql](https://github.com/gizmodata/ibis-gizmosql)

---

## SQLAlchemy

SQLAlchemy provides a SQL toolkit and Object-Relational Mapping (ORM) for Python.

### Installation

```bash
pip install sqlalchemy-gizmosql-adbc-dialect
```

### Basic Usage

```python
from sqlalchemy import create_engine, text
import os

# Create engine
engine = create_engine(
    "gizmosql://gizmosql_username:gizmosql_password@localhost:31337/"
    "?tls=true&tls_skip_verify=true"
)

# Execute query
with engine.connect() as conn:
    result = conn.execute(text("SELECT version()"))
    for row in result:
        print(row)
```

### Using with Pandas

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("gizmosql://gizmosql_username:gizmosql_password@localhost:31337/")

# Read SQL query into DataFrame
df = pd.read_sql("SELECT * FROM nation", engine)
print(df)
```

### ORM Example

```python
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Nation(Base):
    __tablename__ = 'nation'
    
    n_nationkey = Column(Integer, primary_key=True)
    n_name = Column(String)
    n_regionkey = Column(Integer)
    n_comment = Column(String)

engine = create_engine("gizmosql://gizmosql_username:gizmosql_password@localhost:31337/")
Session = sessionmaker(bind=engine)
session = Session()

# Query using ORM
nations = session.query(Nation).filter(Nation.n_regionkey == 1).all()
for nation in nations:
    print(f"{nation.n_nationkey}: {nation.n_name}")
```

📖 **Full Documentation**: [sqlalchemy-gizmosql-adbc-dialect](https://github.com/gizmodata/sqlalchemy-gizmosql-adbc-dialect)

---

## Token Authentication

GizmoSQL supports JWT token-based authentication in addition to username/password authentication.

### Generate Token

See the [generate-gizmosql-token](https://github.com/gizmodata/generate-gizmosql-token) repository for examples of generating and using authentication tokens.

### Using Token with Python

```python
from adbc_driver_flightsql import dbapi as gizmosql

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "authorization_header": f"Bearer {your_token}"
    },
    autocommit=True
) as conn:
    # Execute queries...
    pass
```

---

## Connection Troubleshooting

### Common Issues

**Issue: "Invalid bearer token provided"**

This typically occurs when the server restarts. Change the `GIZMOSQL_PASSWORD` environment variable to generate a new token.

**Issue: TLS certificate verification failed**

Use one of the following options:
- JDBC: `disableCertificateVerification=true`
- Python ADBC: `DatabaseOptions.TLS_SKIP_VERIFY.value: "true"`
- CLI: `--tls-skip-verify`

**Issue: Connection refused**

- Verify the server is running
- Check the port number (default: 31337)
- Ensure firewall rules allow the connection

### Testing Connection

Test your connection with a simple query:

```bash
gizmosql_client \
  --command Execute \
  --host "localhost" \
  --port 31337 \
  --username "gizmosql_username" \
  --password "gizmosql_password" \
  --query "SELECT 1" \
  --use-tls \
  --tls-skip-verify
```

---

## Next Steps

- [Configure GizmoSQL](configuration.md)
- [Explore integrations](integrations.md)
- [Performance tuning](performance.md)

---

## Additional Resources

- [Apache Arrow Flight SQL Documentation](https://arrow.apache.org/docs/format/FlightSql.html)
- [ADBC Documentation](https://arrow.apache.org/adbc/)
- [GizmoData Website](https://gizmodata.com)
