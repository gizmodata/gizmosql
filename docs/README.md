# GizmoSQL

> High-Performance SQL Server for the Cloud

[![DockerHub](https://img.shields.io/badge/dockerhub-image-green.svg?logo=Docker)](https://hub.docker.com/r/gizmodata/gizmosql)
[![GitHub Container](https://img.shields.io/badge/github--package-container--image-green.svg?logo=Docker)](https://github.com/gizmodata/gizmosql/pkgs/container/gizmosql)
[![Documentation](https://img.shields.io/badge/Documentation-dev-yellow.svg)](https://arrow.apache.org/docs/format/FlightSql.html)
[![GitHub](https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql-blue.svg?logo=Github)](https://github.com/gizmodata/gizmosql)

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

| Component | Version |
|-----------|---------|
| [DuckDB](https://duckdb.org) | v1.4.3 |
| [SQLite](https://sqlite.org) | 3.51.1 |
| [Apache Arrow (Flight SQL)](https://arrow.apache.org/docs/format/FlightSql.html) | 22.0.0 |
| [jwt-cpp](https://thalhammer.github.io/jwt-cpp/) | v0.7.1 |
| [nlohmann/json](https://json.nlohmann.me) | v3.12.0 |

---

## 🚀 Quick Start

### Run from Docker

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

### Mount Your Own DuckDB Database

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

## 📚 Features

- ✅ DuckDB + SQLite backend support
- ✅ TLS & optional mTLS
- ✅ JWT-based auth (automatically issued, signed server-side)
- ✅ Server initialization via `INIT_SQL_COMMANDS` or `INIT_SQL_COMMANDS_FILE`
- ✅ Query timeout support
- ✅ Slim Docker images for minimal runtime

---

## 🧩 Extensions & Integrations

- 🔌 [SQLAlchemy dialect](https://github.com/gizmodata/sqlalchemy-gizmosql-adbc-dialect)
- 💿 [Apache Superset driver](https://github.com/gizmodata/superset-sqlalchemy-gizmosql-adbc-dialect)
- 🔌 [Ibis adapter](https://github.com/gizmodata/ibis-gizmosql)
- 🌐 [Flight SQL over WebSocket Proxy](https://github.com/gizmodata/flight-sql-websocket-proxy)
- 📈 [Metabase driver](https://github.com/J0hnG4lt/metabase-flightsql-driver)
- ⚙️ [dbt Adapter](https://github.com/gizmodata/dbt-gizmosql)
- ✨ [PySpark SQLFrame adapter](https://github.com/gizmodata/sqlframe)
- 🪩 [ADBC Scanner](adbc_scanner_duckdb.md)
- ⚓️ [Kubernetes Operator](https://github.com/gizmodata/gizmosql-operator)
- 📺 [GizmoSQLLine JDBC CLI Client](https://github.com/gizmodata/gizmosqlline)
- 🔥 [Grafana Plugin](https://github.com/gizmodata/grafana-gizmosql-datasource)

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
