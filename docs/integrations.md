# Integrations & Extensions

GizmoSQL integrates with a rich ecosystem of data tools and platforms.

---

## Overview

GizmoSQL provides seamless integration with:

- 🐍 Python data stack (Pandas, PyArrow, Polars)
- 📊 BI tools (Apache Superset, Metabase, Grafana)
- ⚙️ Data engineering (dbt, Airflow)
- ☁️ Cloud platforms (Kubernetes, Docker)
- 🔌 Database clients (DBeaver, DataGrip)

---

## SQLAlchemy Dialect

Connect GizmoSQL to any SQLAlchemy-based application.

### Installation

```bash
pip install sqlalchemy-gizmosql-adbc-dialect
```

### Basic Usage

```python
from sqlalchemy import create_engine

engine = create_engine(
    "gizmosql://gizmosql_username:gizmosql_password@localhost:31337/"
    "?tls=true&tls_skip_verify=true"
)

with engine.connect() as conn:
    result = conn.execute("SELECT version()")
    print(result.fetchone())
```

📖 **Repository**: [sqlalchemy-gizmosql-adbc-dialect](https://github.com/gizmodata/sqlalchemy-gizmosql-adbc-dialect)

---

## Apache Superset

Apache Superset is a modern data exploration and visualization platform.

### Installation

```bash
pip install superset-sqlalchemy-gizmosql-adbc-dialect
```

### Add Database Connection

1. Navigate to **Data** → **Databases** → **+ Database**
2. Select **GizmoSQL** from the database list
3. Enter connection details:

```text
SQLAlchemy URI: gizmosql://username:password@host:port/?tls=true
```

### Example Dashboard

Create dashboards with:
- Interactive filters
- Real-time queries
- Multiple visualization types
- Scheduled reports

📖 **Repository**: [superset-sqlalchemy-gizmosql-adbc-dialect](https://github.com/gizmodata/superset-sqlalchemy-gizmosql-adbc-dialect)

---

## Ibis Backend

Ibis is a Python library for portable analytics.

### Installation

```bash
pip install ibis-gizmosql
```

### Usage

```python
import ibis

# Connect
conn = ibis.gizmosql.connect(
    host="localhost",
    port=31337,
    username="gizmosql_username",
    password="gizmosql_password",
    use_tls=True,
    tls_skip_verify=True
)

# Query with Ibis expressions
nation = conn.table('nation')
result = (
    nation
    .filter(nation.n_regionkey == 1)
    .select(['n_nationkey', 'n_name'])
    .execute()
)
print(result)
```

### Benefits

- Portable SQL expressions
- Backend-agnostic code
- Integration with pandas, Polars
- Type-safe operations

📖 **Repository**: [ibis-gizmosql](https://github.com/gizmodata/ibis-gizmosql)

---

## dbt Adapter

dbt (data build tool) enables analytics engineering with SQL.

### Installation

```bash
pip install dbt-gizmosql
```

### Configure Profile

`~/.dbt/profiles.yml`:

```yaml
gizmosql_project:
  outputs:
    dev:
      type: gizmosql
      host: localhost
      port: 31337
      username: gizmosql_username
      password: gizmosql_password
      tls: true
      tls_skip_verify: true
  target: dev
```

### Example dbt Model

`models/sales_summary.sql`:

```sql
{{ config(materialized='table') }}

SELECT
    customer_id,
    SUM(amount) as total_sales,
    COUNT(*) as order_count
FROM {{ ref('orders') }}
GROUP BY customer_id
```

### Run dbt

```bash
dbt run
dbt test
dbt docs generate
dbt docs serve
```

📖 **Repository**: [dbt-gizmosql](https://github.com/gizmodata/dbt-gizmosql)

---

## Metabase Driver

Metabase is an open-source business intelligence tool.

### Installation

Download the Flight SQL driver:

📦 [metabase-flightsql-driver](https://github.com/J0hnG4lt/metabase-flightsql-driver)

### Configuration

1. Copy driver JAR to Metabase plugins directory
2. Add database connection with Flight SQL parameters
3. Create questions and dashboards

---

## Flight SQL WebSocket Proxy

Enable browser-based connections to GizmoSQL.

### Use Case

Connect to GizmoSQL from web applications without backend proxy.

### Installation

```bash
git clone https://github.com/gizmodata/flight-sql-websocket-proxy
cd flight-sql-websocket-proxy
npm install
npm start
```

### Configuration

```javascript
const proxy = new FlightSQLWebSocketProxy({
  flightSqlHost: 'localhost',
  flightSqlPort: 31337,
  websocketPort: 8080
});

proxy.start();
```

📖 **Repository**: [flight-sql-websocket-proxy](https://github.com/gizmodata/flight-sql-websocket-proxy)

---

## PySpark SQLFrame Adapter

Use PySpark API with GizmoSQL backend.

### Installation

```bash
pip install sqlframe[gizmosql]
```

### Usage

```python
from sqlframe.gizmosql import GizmoSQLSession

session = GizmoSQLSession.builder \
    .config("host", "localhost") \
    .config("port", "31337") \
    .config("username", "gizmosql_username") \
    .config("password", "gizmosql_password") \
    .getOrCreate()

# Use PySpark API
df = session.sql("SELECT * FROM nation")
df.show()

# DataFrame operations
df.filter(df.n_regionkey == 1).select("n_name").show()
```

📖 **Repository**: [sqlframe](https://github.com/gizmodata/sqlframe)

---

## ADBC Scanner

Query GizmoSQL directly from DuckDB using ADBC Scanner.

### Usage

```sql
-- Install extension
INSTALL adbc_scanner FROM community;
LOAD adbc_scanner;

-- Query GizmoSQL from DuckDB
SELECT * FROM adbc_scan(
    'adbc_driver_flightsql',
    'grpc+tls://localhost:31337',
    kwargs := {
        'username': 'gizmosql_username',
        'password': 'gizmosql_password',
        'adbc.flight.sql.client_option.tls_skip_verify': 'true'
    }
);
```

### Benefits

- Federation between databases
- Join local and remote data
- Leverage DuckDB's query engine

📖 **Documentation**: [ADBC Scanner Guide](adbc_scanner_duckdb.md)

---

## Kubernetes Operator

Deploy and manage GizmoSQL on Kubernetes with the operator.

### Installation

```bash
kubectl apply -f https://raw.githubusercontent.com/gizmodata/gizmosql-operator/main/deploy/operator.yaml
```

### Deploy GizmoSQL Instance

```yaml
apiVersion: gizmosql.io/v1
kind: GizmoSQL
metadata:
  name: analytics-db
spec:
  version: "latest"
  replicas: 1
  backend: duckdb
  resources:
    requests:
      memory: "8Gi"
      cpu: "4000m"
    limits:
      memory: "16Gi"
      cpu: "8000m"
  tls:
    enabled: true
    secretName: gizmosql-tls
  storage:
    size: "100Gi"
    storageClass: "fast-ssd"
```

```bash
kubectl apply -f gizmosql.yaml
```

📖 **Repository**: [gizmosql-operator](https://github.com/gizmodata/gizmosql-operator)

---

## GizmoSQLLine JDBC CLI

Command-line SQL client for GizmoSQL.

### Installation

```bash
wget https://github.com/gizmodata/gizmosqlline/releases/latest/download/gizmosqlline.jar
```

### Usage

```bash
java -jar gizmosqlline.jar \
  -u "jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&disableCertificateVerification=true" \
  -n gizmosql_username \
  -p gizmosql_password
```

### Features

- Interactive SQL shell
- Command history
- Result formatting
- Script execution
- Auto-completion

📖 **Repository**: [gizmosqlline](https://github.com/gizmodata/gizmosqlline)

---

## Grafana Plugin

Visualize GizmoSQL data in Grafana dashboards.

### Installation

```bash
grafana-cli plugins install gizmodata-gizmosql-datasource
```

### Configure Data Source

1. Navigate to **Configuration** → **Data Sources**
2. Click **Add data source**
3. Select **GizmoSQL**
4. Configure connection:

```yaml
Host: localhost:31337
Username: gizmosql_username
Password: gizmosql_password
TLS: Enabled
```

### Create Dashboard

Use SQL queries in panels:

```sql
SELECT 
    DATE_TRUNC('hour', timestamp) as time,
    AVG(value) as avg_value
FROM metrics
WHERE $__timeFilter(timestamp)
GROUP BY time
ORDER BY time
```

📖 **Repository**: [grafana-gizmosql-datasource](https://github.com/gizmodata/grafana-gizmosql-datasource)

---

## Apache Airflow

Integrate GizmoSQL with Airflow DAGs.

### Installation

```bash
pip install apache-airflow-providers-common-sql
pip install adbc-driver-flightsql
```

### Example DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from adbc_driver_flightsql import dbapi as gizmosql
from datetime import datetime

def query_gizmosql():
    with gizmosql.connect(
        uri="grpc+tls://localhost:31337",
        db_kwargs={
            "username": "gizmosql_username",
            "password": "gizmosql_password"
        }
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders")
            result = cur.fetchone()
            print(f"Total orders: {result[0]}")

with DAG(
    'gizmosql_etl',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    task = PythonOperator(
        task_id='query_orders',
        python_callable=query_gizmosql
    )
```

---

## Jupyter Notebooks

Use GizmoSQL in Jupyter notebooks for interactive analysis.

### Installation

```bash
pip install jupyter adbc-driver-flightsql pandas matplotlib
```

### Example Notebook

```python
import pandas as pd
from adbc_driver_flightsql import dbapi as gizmosql

# Connect
conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    }
)

# Query to DataFrame
df = pd.read_sql("SELECT * FROM sales", conn)

# Visualize
df.plot(x='date', y='amount', kind='line')
```

---

## Database Clients

### DBeaver

Popular open-source database IDE.

**Setup Guide**: [DBeaver Configuration](https://github.com/gizmodata/setup-arrow-jdbc-driver-in-dbeaver)

### DataGrip

JetBrains database IDE.

**Configuration:**
1. Add new datasource
2. Select "Apache Arrow Flight SQL"
3. Enter JDBC URL and credentials

### TablePlus

Modern database GUI.

**Note**: Requires JDBC driver configuration.

---

## Polars Integration

Use Polars DataFrame library with GizmoSQL.

### Example

```python
import polars as pl
from adbc_driver_flightsql import dbapi as gizmosql

# Query to Arrow
with gizmosql.connect(...) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM large_table")
        arrow_table = cur.fetch_arrow_table()

# Convert to Polars
df = pl.from_arrow(arrow_table)

# Use Polars operations
result = (
    df
    .filter(pl.col("amount") > 1000)
    .group_by("category")
    .agg(pl.col("amount").sum())
)
print(result)
```

---

## Custom Integrations

### Build Your Own

GizmoSQL uses standard protocols:

- **Apache Arrow Flight SQL** - Standard SQL over Arrow
- **JDBC** - Java Database Connectivity
- **ADBC** - Arrow Database Connectivity

Any tool supporting these protocols can connect to GizmoSQL.

### Example: Custom Python Client

```python
from adbc_driver_flightsql import dbapi as gizmosql

class GizmoSQLClient:
    def __init__(self, host, port, username, password):
        self.uri = f"grpc+tls://{host}:{port}"
        self.db_kwargs = {
            "username": username,
            "password": password
        }
    
    def query(self, sql):
        with gizmosql.connect(uri=self.uri, db_kwargs=self.db_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetch_arrow_table()
    
    def execute(self, sql):
        with gizmosql.connect(uri=self.uri, db_kwargs=self.db_kwargs) as conn:
            with conn.cursor() as cur:
                cur.execute(sql)

# Usage
client = GizmoSQLClient("localhost", 31337, "user", "pass")
result = client.query("SELECT * FROM nation")
```

---

## Integration Matrix

| Integration | Language | Type | Status |
|-------------|----------|------|--------|
| SQLAlchemy | Python | ORM/Toolkit | ✅ Stable |
| Ibis | Python | DataFrame | ✅ Stable |
| dbt | SQL | Transformation | ✅ Stable |
| Apache Superset | Python/Web | BI Tool | ✅ Stable |
| Metabase | Java/Web | BI Tool | ✅ Community |
| Grafana | Go/Web | Visualization | ✅ Stable |
| Airflow | Python | Orchestration | ✅ Compatible |
| Jupyter | Python | Notebook | ✅ Compatible |
| DBeaver | Java | IDE | ✅ Compatible |
| PySpark SQLFrame | Python | DataFrame | ✅ Stable |
| K8s Operator | Go | Orchestration | ✅ Stable |
| WebSocket Proxy | JavaScript | Connectivity | ✅ Stable |
| ADBC Scanner | C++/SQL | Federation | ✅ Stable |

---

## Community Contributions

Have you built an integration with GizmoSQL?

📧 Let us know: info@gizmodata.com

We'd love to feature your work!

---

## Next Steps

- [Client connections](clients.md)
- [Configuration](configuration.md)
- [Performance tuning](performance.md)

---

## Additional Resources

- [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
- [JDBC Specification](https://docs.oracle.com/javase/tutorial/jdbc/)
- [ADBC Documentation](https://arrow.apache.org/adbc/)
