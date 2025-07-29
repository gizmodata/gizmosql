# 🐍 Python Client Demo for GizmoSQL

A quick demo showing how to connect to GizmoSQL using Python ADBC and perform data insertion and querying operations.

---

## 🚀 Quick Start

### 1. Start GizmoSQL Server

Create a DuckDB database with TPC-H data and start the server:

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
           --pull missing \
           --mount type=bind,source=$(pwd),target=/opt/gizmosql/data \
           --env DATABASE_FILENAME="data/tpch_sf1.duckdb" \
           gizmodata/gizmosql:latest
```

### 2. Install Astral UV

Make sure you have [Astral UV](https://docs.astral.sh/uv/) installed:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 3. Run the Python Demo

Navigate to the Python client demo directory and run:

```bash
cd client_demos/python
uv run main.py
```

### 4. Expected Output

You will see something like this:

```
Inserted 1 row(s)
pyarrow.Table
n_nationkey: int32
n_name: string
----
n_nationkey: [[99]]
n_name: [["EXAMPLE_NATION"]]
```

---

## 📦 What the Demo Does

The demo script:
1. 🔌 Connects to GizmoSQL using ADBC Flight SQL driver
2. 📝 Inserts a new record into the `nation` table
3. 🔍 Queries the inserted data back
4. 📊 Returns results as PyArrow Table format

---

## 🔗 Related Links

- 📚 [Main GizmoSQL Documentation](README.md)
- 🐍 [ADBC Flight SQL Driver](https://pypi.org/project/adbc-driver-flightsql/)
- 🚀 [Astral UV Package Manager](https://docs.astral.sh/uv/)
