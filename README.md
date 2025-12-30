# 🚀 GizmoSQL — High-Performance SQL Server for the Cloud

[![DockerHub](https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker)](https://hub.docker.com/r/gizmodata/gizmosql)
[![GitHub Container](https://img.shields.io/badge/github--package-container--image-green.svg?logo=Docker)](https://github.com/gizmodata/gizmosql/pkgs/container/gizmosql)
[![Documentation](https://img.shields.io/badge/Documentation-dev-yellow.svg)](https://arrow.apache.org/docs/format/FlightSql.html)
[![GitHub](https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql-blue.svg?logo=Github)](https://github.com/gizmodata/gizmosql)
[![JDBC Driver](https://img.shields.io/badge/Arrow%20JDBC%20Driver-download%20artifact-red?logo=Apache%20Maven)](https://search.maven.org/search?q=a:flight-sql-jdbc-driver)
[![ADBC PyPI](https://img.shields.io/badge/PyPI-Arrow%20ADBC%20Flight%20SQL%20driver-blue?logo=PyPI)](https://pypi.org/project/adbc-driver-flightsql/)
[![SQLAlchemy Dialect](https://img.shields.io/badge/PyPI-GizmoSQL%20SQLAlchemy%20Dialect-blue?logo=PyPI)](https://pypi.org/project/sqlalchemy-gizmosql-adbc-dialect/)
[![Ibis Backend](https://img.shields.io/badge/PyPI-GizmoSQL%20Ibis%20Backend-blue?logo=PyPI)](https://pypi.org/project/ibis-gizmosql/)

---

## 🌟 What is GizmoSQL?

**GizmoSQL** is a lightweight, high-performance SQL server built on:

- 🦆 [DuckDB](https://duckdb.org) or 🗃️ [SQLite](https://sqlite.org) for query execution
- 🚀 [Apache Arrow Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) for fast, modern connectivity
- 🔒 Middleware-based auth with optional TLS & JWT

Originally forked from [`sqlflite`](https://github.com/voltrondata/sqlflite) — and now enhanced into a more extensible, production-ready platform under the Apache 2.0 license.

---

## 🧠 Why GizmoSQL?

- 🛰️ **Deploy Anywhere** — Run as a container, native binary, or in Kubernetes
- 📦 **Columnar Fast** — Leverages Arrow columnar format for high-speed transfers
- ⚙️ **Dual Backends** — Switch between DuckDB and SQLite at runtime
- 🔐 **Built-in TLS + Auth** — Password-based login + signed JWT tokens
- 📈 **Super Cheap Analytics** — TPC-H SF 1000 in 161s for ~$0.17 on Azure
- 🧪 **CLI, Python, JDBC, SQLAlchemy, Ibis, WebSocket** — Pick your interface

---

## 📦 Component Versions

| Component                                                                        | Version |
|----------------------------------------------------------------------------------|---------|
| [DuckDB](https://duckdb.org)                                                     | v1.4.3  |
| [SQLite](https://sqlite.org)                                                     | 3.51.1  |
| [Apache Arrow (Flight SQL)](https://arrow.apache.org/docs/format/FlightSql.html) | 22.0.0 |
| [jwt-cpp](https://thalhammer.github.io/jwt-cpp/)                                 | v0.7.1  |
| [nlohmann/json](https://json.nlohmann.me)                                        | v3.12.0 |

## 📚 Documentation

For detailed instructions and configuration information, see our full documentation:

[GizmoSQL Documentation](docs/documentation.md)

---

## 🚀 Quick Start

### Option 1: Run from Docker

```bash
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --pull always \
           gizmodata/gizmosql:latest
```

### Option 2: Mount Your Own DuckDB database file

```bash
duckdb ./tpch_sf1.duckdb << EOF
INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);
EOF

docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --pull always \
           --mount type=bind,source=$(pwd),target=/opt/gizmosql/data \
           --env DATABASE_FILENAME="data/tpch_sf1.duckdb" \
           gizmodata/gizmosql:latest
```

---

## 🧰 Clients and Tools

### 🔗 JDBC

Use with DBeaver or other JDBC clients:

```bash
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=gizmosql_password&disableCertificateVerification=true
```

More info: [Setup guide](https://github.com/gizmodata/setup-arrow-jdbc-driver-in-dbeaver)

---

### 🐍 Python (ADBC)

```python
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions


with gizmosql.connect(uri="grpc+tls://localhost:31337",
                      db_kwargs={"username": os.getenv("GIZMOSQL_USERNAME", "gizmosql_username"),
                                 "password": os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                                 DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Not needed if you use a trusted CA-signed TLS cert
                                 },
                      autocommit=True
                      ) as conn:
  with conn.cursor() as cur:
    cur.execute("SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
                parameters=[24]
                )
    x = cur.fetch_arrow_table()
    print(x)
```

---

### 🔑 Token authentication
See: https://github.com/gizmodata/generate-gizmosql-token for an example of how to generate a token and use it with GizmoSQL.

### 💻 CLI Client

```bash
gizmosql_client   --command Execute   --host localhost --port 31337   --username gizmosql_username   --password gizmosql_password   --query "SELECT version()"   --use-tls --tls-skip-verify
```

---

## 🏗️ Build from Source (Optional)

```bash
git clone https://github.com/gizmodata/gizmosql --recurse-submodules
cd gizmosql
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/usr/local
cmake --build build --target install
```

Then run:

```bash
GIZMOSQL_PASSWORD="..." gizmosql_server --database-filename ./data/your.db --print-queries
```

---

## 🧪 Advanced Features

- ✅ DuckDB + SQLite backend support
- ✅ TLS & optional mTLS
- ✅ JWT-based auth (automatically issued, signed server-side)
- ✅ Server initialization via `INIT_SQL_COMMANDS` or `INIT_SQL_COMMANDS_FILE`
- ✅ Slim Docker image for minimal runtime

---

## 🛠 Backend Selection

```bash
# DuckDB (default)
gizmosql_server -B duckdb --database-filename data/foo.duckdb

# SQLite
gizmosql_server -B sqlite --database-filename data/foo.sqlite
```

> [!TIP]
> You can now use the: `--query-timeout` argument to set a maximum query timeout in seconds for the server.  Queries running longer than the timeout will be killed.  The default value of: `0` means "unlimited".   
> Example: `gizmosql_server (other args...) --query-timeout 10`   
> will set a timeout of 10 seconds for all queries.

---


## 🧩 Extensions & Integrations

- 🔌 [SQLAlchemy dialect](https://github.com/gizmodata/sqlalchemy-gizmosql-adbc-dialect)
- 💿 [Apache Superset compatible SQLAlchemy driver](https://github.com/gizmodata/superset-sqlalchemy-gizmosql-adbc-dialect)
- 🔌 [Ibis adapter](https://github.com/gizmodata/ibis-gizmosql)
- 🌐 [Flight SQL over WebSocket Proxy](https://github.com/gizmodata/flight-sql-websocket-proxy)
- 📈 [Metabase driver](https://github.com/J0hnG4lt/metabase-flightsql-driver)
- ⚙️ [dbt Adapter](https://github.com/gizmodata/dbt-gizmosql)
- ✨ [PySpark SQLFrame adapter](https://github.com/gizmodata/sqlframe) 🚀 **NEW!**
- 🪩 [ADBC Scanner by Query.Farm](docs/adbc_scanner_duckdb.md) 🚀 **NEW!**
- ⚓️ [Kubernetes Operator](https://github.com/gizmodata/gizmosql-operator) 🚀 **NEW!**
- 📺 [GizmoSQLLine JDBC CLI Client](https://github.com/gizmodata/gizmosqlline) **NEW!**
---

## 📊 Performance

💡 On Azure VM `Standard_E64pds_v6` (~$3.74/hr):

- TPC-H SF 1000 benchmark:  
  ⏱️ 161.4 seconds  
  💰 ~$0.17 USD total

> 🏁 Speed for the win. Performance for pennies.

---

## 🔒 License

```
Apache License, Version 2.0
https://www.apache.org/licenses/LICENSE-2.0
```

---

## 📫 Contact

Questions or consulting needs?

📧 info@gizmodata.com  
🌐 [https://gizmodata.com](https://gizmodata.com)

---

> Built with ❤️ by [GizmoData™](https://gizmodata.com)
