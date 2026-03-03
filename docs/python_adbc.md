# Python ADBC Connectivity

GizmoSQL supports Python connectivity via [ADBC](https://arrow.apache.org/adbc/) (Arrow Database Connectivity). This guide covers the recommended driver package and explains an important nuance around DDL/DML execution that users of the generic Flight SQL driver may encounter.

---

## Use `adbc-driver-gizmosql` (Recommended)

The [`adbc-driver-gizmosql`](https://pypi.org/project/adbc-driver-gizmosql/) package is the **recommended Python driver** for connecting to GizmoSQL. It is built on top of `adbc-driver-flightsql` and `pyarrow`, so you get all standard Flight SQL capabilities **plus**:

- **Automatic DDL/DML handling** — `cursor.execute()` works for all statement types (SELECT, DDL, DML) without any special handling. The driver detects DDL/DML and routes it correctly so statements are executed immediately on the server.
- **OAuth/SSO authentication** — Built-in support for browser-based OAuth login with identity providers like Google, Okta, etc. Just set `auth_type="external"` and the driver handles the entire flow.
- **Standalone OAuth token retrieval** — A `get_oauth_token()` helper for programmatic token acquisition.

```bash
pip install adbc-driver-gizmosql
```

### SELECT Queries

```python
from adbc_driver_gizmosql import dbapi as gizmosql

with gizmosql.connect(
    "grpc+tls://localhost:31337",
    username="gizmosql_user",
    password="gizmosql_password",
    tls_skip_verify=True,
) as conn:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT n_nationkey, n_name FROM nation WHERE n_nationkey = ?",
            parameters=[24],
        )
        print(cur.fetch_arrow_table())
```

### DDL and DML Statements

`cursor.execute()` handles DDL and DML automatically — no special API needed:

```python
from adbc_driver_gizmosql import dbapi as gizmosql

with gizmosql.connect(
    "grpc+tls://localhost:31337",
    username="gizmosql_user",
    password="gizmosql_password",
    tls_skip_verify=True,
) as conn:
    with conn.cursor() as cur:
        # DDL — just works
        cur.execute("CREATE TABLE demo (id INTEGER, name VARCHAR)")

        # DML — just works
        cur.execute("INSERT INTO demo VALUES (1, 'Alice'), (2, 'Bob')")

        # SELECT — works as usual
        cur.execute("SELECT * FROM demo")
        print(cur.fetch_arrow_table().to_pandas())

        # Cleanup
        cur.execute("DROP TABLE demo")
```

> **Tip:** `cursor.execute_update(query)` is also available and returns the rows-affected count directly — useful when you need to know how many rows were inserted, updated, or deleted: `rows = cur.execute_update("INSERT ...")`

### OAuth/SSO Authentication

For GizmoSQL Enterprise servers configured with [OAuth/SSO](oauth_sso_setup.md), use `auth_type="external"` to authenticate via browser-based login:

```python
from adbc_driver_gizmosql import dbapi as gizmosql

with gizmosql.connect(
    "grpc+tls://gizmosql.example.com:31337",
    auth_type="external",
    tls_skip_verify=True,
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT CURRENT_USER")
        print(cur.fetch_arrow_table())
```

The driver automatically discovers the OAuth endpoint, opens your browser to the identity provider's login page, polls for completion, and exchanges the token — all behind the scenes.

You can also retrieve OAuth tokens programmatically for use in other tools:

```python
from adbc_driver_gizmosql import get_oauth_token

result = get_oauth_token(
    host="gizmosql.example.com",
    port=31339,
    tls_skip_verify=True,
    timeout=300,
)
print(f"Token: {result.token}")
```

### Bulk Data Ingestion

```python
import pyarrow as pa
from adbc_driver_gizmosql import dbapi as gizmosql

table = pa.table({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})

with gizmosql.connect(
    "grpc+tls://localhost:31337",
    username="gizmosql_user",
    password="gizmosql_password",
    tls_skip_verify=True,
) as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("students", table, mode="create")
```

### Pandas Integration

```python
import pandas as pd
from adbc_driver_gizmosql import dbapi as gizmosql

with gizmosql.connect(
    "grpc+tls://localhost:31337",
    username="gizmosql_user",
    password="gizmosql_password",
    tls_skip_verify=True,
) as conn:
    df = pd.read_sql("SELECT * FROM nation", conn)
    print(df)
```

### Connection Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `uri` | *(required)* | Flight SQL URI (e.g., `grpc+tls://host:31337`) |
| `username` | | Username for password authentication |
| `password` | | Password for password authentication |
| `auth_type` | `"password"` | `"password"` or `"external"` (OAuth/SSO) |
| `tls_skip_verify` | `False` | Skip TLS certificate verification |
| `oauth_port` | `31339` | HTTP port for OAuth endpoint |
| `oauth_timeout` | `300` | Seconds to wait for OAuth login |
| `open_browser` | `True` | Automatically open browser for OAuth |

---

## Why Not `adbc-driver-flightsql` Directly?

You _can_ use `adbc-driver-flightsql` to connect to GizmoSQL, but there is a critical caveat: **DDL and DML statements via `cursor.execute()` will appear to succeed but silently do nothing.**

This is due to GizmoSQL's **lazy SQL execution** model. When the Flight SQL client calls `GetFlightInfo` (which is what `cursor.execute()` maps to), GizmoSQL prepares the statement and returns flight information — but does **not** execute it yet. The statement only runs when the client **fetches the results** (calls `DoGet`).

For `SELECT` queries this is transparent, because the natural workflow is `execute()` followed by `fetch_arrow_table()` — the fetch triggers the actual execution. But for DDL (`CREATE TABLE`, `DROP TABLE`, etc.) and DML (`INSERT`, `UPDATE`, `DELETE`), there are no results to fetch. If you just call `execute()` without fetching, the statement never runs on the server.

### What Goes Wrong

```python
from adbc_driver_flightsql import dbapi

with dbapi.connect(
    "grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_user",
        "password": "gizmosql_password",
        "adbc.flight.sql.client_option.tls_skip_verify": "true",
    },
) as conn:
    with conn.cursor() as cur:
        # No error raised, but the table is NOT created!
        cur.execute("CREATE TABLE my_table (id INTEGER, name VARCHAR)")
```

### Workaround (Not Recommended)

If you must use `adbc-driver-flightsql` directly, you can work around this by dropping down to the low-level ADBC statement API:

```python
from adbc_driver_flightsql import dbapi

with dbapi.connect(
    "grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_user",
        "password": "gizmosql_password",
        "adbc.flight.sql.client_option.tls_skip_verify": "true",
    },
) as conn:
    with conn.cursor() as cur:
        # Must use the private _stmt API for DDL/DML
        cur._stmt.set_sql_query(
            "CREATE TABLE my_table (id INTEGER, name VARCHAR)"
        )
        cur._stmt.execute_update()
```

This routes the statement through `CommandStatementUpdate` (the `DoPut` RPC), which executes it immediately. However, this relies on a **private API** (`_stmt`) and is not ergonomic — you should use `adbc-driver-gizmosql` instead, which handles all of this automatically.

---

## Summary

| | `adbc-driver-gizmosql` | `adbc-driver-flightsql` |
|---|---|---|
| **DDL/DML via `cursor.execute()`** | Works automatically | Silently does nothing |
| **OAuth/SSO** | Built-in | Not available |
| **Standalone OAuth token retrieval** | Built-in | Not available |
| **Bulk ingestion** | Supported | Supported |
| **Parameterized queries** | Supported | Supported |
| **Pandas integration** | Supported | Supported |

**We strongly recommend `adbc-driver-gizmosql`** for all Python connectivity to GizmoSQL. It is a drop-in replacement that provides a better experience with no downsides.

```bash
pip install adbc-driver-gizmosql
```

See also: [GitHub](https://github.com/gizmodata/adbc-driver-gizmosql) | [PyPI](https://pypi.org/project/adbc-driver-gizmosql/)
