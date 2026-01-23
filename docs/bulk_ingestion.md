# Bulk Ingestion with ADBC

GizmoSQL supports high-performance bulk data ingestion using the ADBC (Arrow Database Connectivity) `adbc_ingest()` method. This allows you to efficiently load large datasets directly from Arrow RecordBatches or tables.

## Overview

Bulk ingestion with ADBC provides significant performance advantages over row-by-row INSERT statements:

- **Zero serialization overhead** - Data stays in columnar Arrow format
- **Batch processing** - Efficiently processes thousands of rows at a time
- **Direct memory transfer** - Minimizes data copies between client and server

## Prerequisites

Install the required Python packages:

```bash
pip install adbc-driver-flightsql pyarrow duckdb
```

## Quick Example

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
import pyarrow as pa

# Create sample data
data = pa.table({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
    "score": [95.5, 87.3, 92.1, 88.9, 91.7]
})

# Connect to GizmoSQL and bulk ingest
with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cursor:
        # Bulk ingest the data
        rows_loaded = cursor.adbc_ingest(
            table_name="my_table",
            data=data,
            mode="replace"  # or "append" or "create"
        )
        print(f"Loaded {rows_loaded:,} rows")
```

## Ingestion Modes

The `adbc_ingest()` method supports three modes:

| Mode | Description |
|------|-------------|
| `"create"` | Create a new table (fails if table exists) |
| `"append"` | Append to existing table (fails if table doesn't exist) |
| `"replace"` | Drop and recreate the table if it exists |

## Loading Large Datasets

For large datasets, use a RecordBatch reader to stream data in batches:

```python
import duckdb
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

# Generate TPC-H data (1GB scale factor)
duckdb_conn = duckdb.connect()
duckdb_conn.install_extension("tpch")
duckdb_conn.load_extension("tpch")
duckdb_conn.execute("CALL dbgen(sf=1.0)")

# Get Arrow reader with batch size of 10,000 rows
lineitem_reader = duckdb_conn.table("lineitem").fetch_arrow_reader(batch_size=10_000)

# Bulk ingest into GizmoSQL
with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cursor:
        rows_loaded = cursor.adbc_ingest(
            table_name="lineitem",
            data=lineitem_reader,
            mode="replace"
        )
        print(f"Loaded {rows_loaded:,} rows")

        # Verify
        cursor.execute("SELECT COUNT(*) FROM lineitem")
        count = cursor.fetchone()[0]
        print(f"Verified: {count:,} rows in table")
```

## Loading from Parquet Files

You can also bulk ingest directly from Parquet files:

```python
import pyarrow.parquet as pq
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

# Read Parquet file as Arrow table
table = pq.read_table("data.parquet")

with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cursor:
        rows_loaded = cursor.adbc_ingest(
            table_name="parquet_data",
            data=table,
            mode="replace"
        )
        print(f"Loaded {rows_loaded:,} rows from Parquet")
```

## Loading from Pandas DataFrames

Convert Pandas DataFrames to Arrow for bulk ingestion:

```python
import pandas as pd
import pyarrow as pa
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

# Create a Pandas DataFrame
df = pd.DataFrame({
    "date": pd.date_range("2024-01-01", periods=1000),
    "value": range(1000),
    "category": ["A", "B", "C", "D"] * 250
})

# Convert to Arrow table
arrow_table = pa.Table.from_pandas(df)

with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cursor:
        rows_loaded = cursor.adbc_ingest(
            table_name="pandas_data",
            data=arrow_table,
            mode="replace"
        )
        print(f"Loaded {rows_loaded:,} rows from Pandas")
```

## TLS Connections

For TLS-enabled servers:

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password",
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Only for self-signed certs
    },
    autocommit=True
) as conn:
    # ... bulk ingest operations
    pass
```

## Performance Tips

1. **Use appropriate batch sizes** - For `fetch_arrow_reader()`, batch sizes of 10,000-100,000 rows typically work well
2. **Stream large datasets** - Use RecordBatch readers instead of loading entire tables into memory
3. **Use `replace` mode carefully** - It drops and recreates the table, which may be slower for incremental loads
4. **Monitor memory** - Large batch sizes consume more client memory

## Complete Example

Here's a complete example that generates TPC-H data and measures ingestion performance:

```python
import time
import duckdb
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions


def bulk_ingest_tpch(scale_factor=0.1):
    """Generate TPC-H data and bulk ingest into GizmoSQL."""

    # Step 1: Generate TPC-H data
    print(f"Generating TPC-H SF {scale_factor} data...")
    duckdb_conn = duckdb.connect()
    duckdb_conn.install_extension("tpch")
    duckdb_conn.load_extension("tpch")
    duckdb_conn.execute(f"CALL dbgen(sf={scale_factor})")

    row_count = duckdb_conn.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
    print(f"Generated {row_count:,} lineitem rows")

    # Step 2: Get Arrow reader
    reader = duckdb_conn.table("lineitem").fetch_arrow_reader(batch_size=10_000)

    # Step 3: Bulk ingest
    with gizmosql.connect(
        uri="grpc://localhost:31337",
        db_kwargs={
            "username": "gizmosql_username",
            "password": "gizmosql_password"
        },
        autocommit=True
    ) as conn:
        with conn.cursor() as cursor:
            start = time.perf_counter()

            rows_loaded = cursor.adbc_ingest(
                table_name="lineitem",
                data=reader,
                mode="replace"
            )

            elapsed = time.perf_counter() - start
            rows_per_sec = rows_loaded / elapsed

            print(f"Loaded {rows_loaded:,} rows in {elapsed:.2f}s")
            print(f"Throughput: {rows_per_sec:,.0f} rows/sec")

            # Verify
            cursor.execute("SELECT COUNT(*) FROM lineitem")
            verified = cursor.fetchone()[0]
            print(f"Verified: {verified:,} rows")

    duckdb_conn.close()


if __name__ == "__main__":
    bulk_ingest_tpch(scale_factor=0.1)
```

## See Also

- [ADBC Python Documentation](https://arrow.apache.org/adbc/current/python/index.html)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)
- [DuckDB TPC-H Extension](https://duckdb.org/docs/extensions/tpch)
