# Frequently Asked Questions

Common questions and answers about GizmoSQL.

---

## General

### What is GizmoSQL?

GizmoSQL is a high-performance SQL server that uses DuckDB or SQLite as backend engines and exposes them via Apache Arrow Flight SQL protocol. It provides fast, modern connectivity for analytical and transactional workloads.

### Why use GizmoSQL?

- **Fast**: Leverages Arrow columnar format for high-speed data transfer
- **Flexible**: Choose between DuckDB (analytics) or SQLite (transactions)
- **Scalable**: Run in containers, VMs, or Kubernetes
- **Secure**: Built-in TLS, authentication, and JWT tokens
- **Compatible**: Works with JDBC, ADBC, Python, Java, and many BI tools

### Is GizmoSQL free?

Yes! GizmoSQL is open source under the Apache 2.0 license.

### Who maintains GizmoSQL?

GizmoSQL is maintained by [GizmoData™](https://gizmodata.com).

---

## Installation & Setup

### How do I install GizmoSQL?

The easiest way is using Docker:

```bash
docker run --name gizmosql \
  --publish 31337:31337 \
  --env TLS_ENABLED="1" \
  --env GIZMOSQL_PASSWORD="your_password" \
  gizmodata/gizmosql:latest
```

See [Installation Guide](installation.md) for more options.

### What are the system requirements?

- **Minimum**: 2 CPU cores, 4 GB RAM, 10 GB storage
- **Recommended**: 8+ CPU cores, 16+ GB RAM, 100+ GB SSD storage
- **OS**: Linux, macOS, or Windows (via Docker)

### Can I run GizmoSQL on Windows?

Yes, via Docker. Native Windows builds are not currently supported.

### How do I upgrade GizmoSQL?

```bash
docker pull gizmodata/gizmosql:latest
docker stop gizmosql
docker rm gizmosql
docker run ...  # with updated image
```

---

## Configuration

### Which backend should I use?

- **DuckDB**: For analytics, OLAP, large aggregations
- **SQLite**: For transactions, OLTP, embedded use cases

See [Configuration Guide](configuration.md#backend-selection).

### How do I change the default port?

```bash
# Docker
docker run --publish 8080:31337 ...

# CLI
gizmosql_server --port 8080
```

### Can I disable TLS?

Yes, but **not recommended for production**:

```bash
docker run --env TLS_ENABLED="0" ...
```

### How do I use my own TLS certificates?

```bash
docker run \
  --env TLS_CERT="/certs/server.crt" \
  --env TLS_KEY="/certs/server.key" \
  --mount type=bind,source=/path/to/certs,target=/certs \
  ...
```

---

## Connections

### What clients are supported?

- JDBC (Java, DBeaver, DataGrip)
- Python ADBC
- CLI tool
- Ibis
- SQLAlchemy
- dbt
- Apache Superset
- Metabase
- Grafana

See [Client Connections](clients.md).

### How do I connect with Python?

```python
from adbc_driver_flightsql import dbapi as gizmosql

conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "your_password"
    }
)
```

### How do I connect with JDBC?

```text
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=your_password&disableCertificateVerification=true
```

### Why am I getting "Invalid bearer token" errors?

This happens when the server restarts. Change the password:

```bash
docker stop gizmosql
docker run --env GIZMOSQL_PASSWORD="new_password" ...
```

---

## Data & Queries

### What SQL features are supported?

Depends on backend:

- **DuckDB**: Full SQL, analytical functions, window functions, CTEs, etc.
- **SQLite**: Standard SQL with some limitations

See [DuckDB SQL](https://duckdb.org/docs/sql/introduction) and [SQLite SQL](https://sqlite.org/lang.html).

### Can I load CSV/Parquet/JSON files?

Yes with DuckDB backend:

```sql
-- CSV
SELECT * FROM 'data.csv';

-- Parquet
SELECT * FROM 'data.parquet';

-- JSON
SELECT * FROM 'data.json';
```

### How do I import data from S3?

With DuckDB backend:

```sql
INSTALL httpfs;
LOAD httpfs;

SELECT * FROM 's3://bucket/file.parquet';
```

### What's the maximum database size?

Limited only by available storage. DuckDB efficiently handles TB-scale datasets.

### Can I use multiple databases?

Run multiple GizmoSQL instances with different ports and database files.

---

## Performance

### How fast is GizmoSQL?

TPC-H SF 1000 (1 TB) benchmark:
- **Time**: 161 seconds
- **Cost**: ~$0.17 on Azure

See [Performance](performance.md).

### How do I optimize query performance?

1. Use DuckDB for analytics
2. Increase threads: `SET threads = 16;`
3. Increase memory: `SET memory_limit = '32GB';`
4. Create indexes
5. Use fast storage (NVMe SSD)

See [Performance Tuning](performance.md).

### Why is my query slow?

Common causes:
- Not using indexes
- Selecting too many columns (`SELECT *`)
- Insufficient memory/threads
- Slow storage

Run `EXPLAIN ANALYZE` to diagnose.

---

## Security

### Is GizmoSQL secure?

Yes, when configured properly:
- TLS encryption
- Password authentication
- JWT tokens
- Optional mTLS

See [Security Guide](security.md).

### How do I secure my deployment?

1. Enable TLS
2. Use strong passwords
3. Restrict network access
4. Set query timeouts
5. Keep software updated

### Can I use JWT tokens?

Yes! GizmoSQL automatically issues JWT tokens after password authentication.

See [Token Authentication](https://github.com/gizmodata/generate-gizmosql-token).

---

## Deployment

### Can I deploy to Kubernetes?

Yes! Use the [Kubernetes Operator](https://github.com/gizmodata/gizmosql-operator):

```bash
kubectl apply -f https://raw.githubusercontent.com/gizmodata/gizmosql-operator/main/deploy/operator.yaml
```

### How do I deploy to AWS/Azure/GCP?

Deploy as:
- Container on ECS/ACI/Cloud Run
- VM with Docker
- Kubernetes cluster

### Can I run GizmoSQL in production?

Yes! GizmoSQL is production-ready when properly configured:
- Enable TLS
- Use strong authentication
- Monitor resources
- Regular backups

### How do I backup my database?

```bash
# DuckDB
duckdb mydb.duckdb ".backup backup.duckdb"

# SQLite
sqlite3 mydb.sqlite ".backup backup.sqlite"

# Or just copy the file
cp mydb.duckdb backup.duckdb
```

---

## Troubleshooting

### Server won't start

Check:
1. Port not in use: `lsof -i :31337`
2. Valid password set: `GIZMOSQL_PASSWORD`
3. Database file exists
4. Sufficient permissions

See [Troubleshooting](troubleshooting.md).

### Connection refused

Check:
1. Server is running: `docker ps`
2. Port is exposed: `--publish 31337:31337`
3. Firewall allows connection
4. Correct host/port

### Out of memory errors

Solutions:
1. Increase memory limit: `SET memory_limit = '32GB';`
2. Enable spilling: `SET temp_directory = '/tmp';`
3. Reduce query scope
4. Add more RAM

---

## Integrations

### Can I use GizmoSQL with pandas?

Yes!

```python
import pandas as pd
from adbc_driver_flightsql import dbapi as gizmosql

conn = gizmosql.connect(...)
df = pd.read_sql("SELECT * FROM table", conn)
```

### Does GizmoSQL work with Apache Superset?

Yes! Install the [Superset driver](https://github.com/gizmodata/superset-sqlalchemy-gizmosql-adbc-dialect).

### Can I use dbt with GizmoSQL?

Yes! Install the [dbt adapter](https://github.com/gizmodata/dbt-gizmosql).

### Does it work with Grafana?

Yes! Install the [Grafana plugin](https://github.com/gizmodata/grafana-gizmosql-datasource).

---

## Development

### How do I contribute?

See [Contributing Guide](contributing.md).

### Where do I report bugs?

[GitHub Issues](https://github.com/gizmodata/gizmosql/issues)

### Can I add new features?

Yes! Open an issue to discuss, then submit a pull request.

### How do I build from source?

```bash
git clone --recurse-submodules https://github.com/gizmodata/gizmosql.git
cd gizmosql
cmake -S . -B build -G Ninja
cmake --build build
```

---

## Licensing

### What license is GizmoSQL under?

Apache License 2.0

### Can I use GizmoSQL commercially?

Yes! Apache 2.0 allows commercial use.

### Do I need to open source my application?

No. Apache 2.0 is permissive and doesn't require that.

---

## Support

### Where can I get help?

- [Documentation](/)
- [GitHub Issues](https://github.com/gizmodata/gizmosql/issues)
- [Email](mailto:info@gizmodata.com)

### Do you offer commercial support?

Yes! Contact: info@gizmodata.com

### Can you help with custom development?

Yes! Contact us for consulting services.

---

## Comparison

### GizmoSQL vs PostgreSQL?

| Feature | GizmoSQL | PostgreSQL |
|---------|----------|------------|
| Protocol | Flight SQL | PostgreSQL wire |
| Format | Columnar (Arrow) | Row-based |
| Analytics | Excellent (DuckDB) | Good |
| Transactions | Good (SQLite) | Excellent |
| Scale | Single-node | Single/Multi-node |

### GizmoSQL vs Snowflake?

| Feature | GizmoSQL | Snowflake |
|---------|----------|-----------|
| Deployment | Self-hosted | Cloud-only |
| Cost | Very low | Usage-based |
| Scale | Single-node | Multi-node |
| Setup | Minutes | Account required |

### GizmoSQL vs DuckDB directly?

GizmoSQL adds:
- Network access via Flight SQL
- Authentication & TLS
- Multi-client connections
- REST API (via proxy)
- BI tool integrations

---

## Roadmap

### What's next for GizmoSQL?

- Multi-node clustering
- Read replicas
- More backend engines
- Enhanced monitoring
- Cloud-native features

### Can I request features?

Yes! Open a [feature request](https://github.com/gizmodata/gizmosql/issues/new).

---

## Still have questions?

- 📧 Email: info@gizmodata.com
- 💬 [GitHub Discussions](https://github.com/gizmodata/gizmosql/discussions)
- 📚 [Full Documentation](/)

---

> Last updated: January 2026
