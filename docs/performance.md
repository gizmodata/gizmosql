# Performance Tuning

Optimize GizmoSQL for your workload with these performance tuning strategies.

---

## Overview

GizmoSQL performance depends on:

- Backend selection (DuckDB vs SQLite)
- Hardware resources (CPU, Memory, Storage)
- Query patterns and optimization
- Configuration settings

---

## Benchmark Results

### TPC-H SF 1000 Performance

On Azure VM `Standard_E64pds_v6` (~$3.74/hr):

| Metric | Value |
|--------|-------|
| Total Time | 161.4 seconds |
| Cost | ~$0.17 USD |
| Scale Factor | 1000 (1TB) |
| Backend | DuckDB v1.4.3 |

> 🏁 **Speed for the win. Performance for pennies.**

See [One Trillion Row Challenge](one_trillion_row_challenge.md) for more benchmarks.

---

## Backend Selection

### DuckDB - Analytics Workloads

**Best for:**
- OLAP queries
- Analytical aggregations
- Large dataset scans
- Columnar operations
- Parallel processing

**Configuration:**

```sql
SET threads = 16;
SET memory_limit = '32GB';
SET temp_directory = '/fast-storage/tmp';
SET enable_optimizer = true;
```

### SQLite - Transactional Workloads

**Best for:**
- OLTP queries
- High concurrency writes
- Small to medium datasets
- Embedded use cases
- ACID compliance

**Configuration:**

```sql
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA cache_size = 10000;
PRAGMA temp_store = MEMORY;
```

---

## CPU Optimization

### Thread Configuration

Match thread count to available CPU cores:

```sql
-- For 16-core system
SET threads = 16;
SET external_threads = 8;
```

**Guidelines:**
- `threads` = Number of physical cores
- `external_threads` = Half of physical cores
- Don't exceed available cores (causes context switching)

### Docker CPU Limits

```bash
docker run --cpus="16" \
  --env INIT_SQL_COMMANDS="SET threads = 16;" \
  gizmodata/gizmosql:latest
```

### Kubernetes Resource Requests

```yaml
resources:
  requests:
    cpu: "8000m"
    memory: "16Gi"
  limits:
    cpu: "16000m"
    memory: "32Gi"
```

---

## Memory Optimization

### DuckDB Memory Settings

```sql
-- Set maximum memory usage
SET memory_limit = '32GB';
SET max_memory = '32GB';

-- Configure temp storage for spilling
SET temp_directory = '/fast-storage/tmp';
```

### Memory Guidelines

| Dataset Size | Recommended Memory |
|--------------|-------------------|
| < 10 GB | 4-8 GB |
| 10-100 GB | 16-32 GB |
| 100-500 GB | 64-128 GB |
| > 500 GB | 256+ GB |

### Docker Memory Limits

```bash
docker run --memory="32g" --memory-swap="32g" \
  --env INIT_SQL_COMMANDS="SET memory_limit = '28GB';" \
  gizmodata/gizmosql:latest
```

?> **Tip**: Set DuckDB memory_limit to ~80% of container memory to leave room for overhead.

---

## Storage Optimization

### Storage Types

| Storage Type | Read Speed | Write Speed | Best For |
|--------------|------------|-------------|----------|
| NVMe SSD | Excellent | Excellent | Production |
| SATA SSD | Good | Good | Development |
| HDD | Poor | Poor | Archives only |

### Temp Directory Configuration

Use fast local storage for temporary files:

```sql
SET temp_directory = '/nvme/tmp';
```

### Docker Volume Performance

```bash
# Local NVMe mount
docker run \
  --mount type=bind,source=/nvme/data,target=/data \
  --env DATABASE_FILENAME="/data/mydb.duckdb" \
  gizmodata/gizmosql:latest
```

---

## Query Optimization

### Use Query Plans

Analyze query execution:

```sql
EXPLAIN SELECT * FROM large_table WHERE id > 1000;
EXPLAIN ANALYZE SELECT * FROM large_table WHERE id > 1000;
```

### Indexes (DuckDB)

Create indexes for frequently filtered columns:

```sql
CREATE INDEX idx_customer_id ON orders(customer_id);
CREATE INDEX idx_date ON orders(order_date);
```

### Partitioning

Partition large tables by common filter columns:

```sql
-- Create partitioned table
CREATE TABLE sales_partitioned AS 
SELECT * FROM sales 
PARTITION BY (YEAR(sale_date), MONTH(sale_date));
```

### Columnar Storage

Use Parquet for large analytical datasets:

```sql
-- Export to Parquet
COPY (SELECT * FROM large_table) TO 'output.parquet' (FORMAT PARQUET);

-- Query Parquet directly
SELECT * FROM 'output.parquet' WHERE category = 'electronics';
```

---

## Connection Pooling

### Python Connection Pool

```python
from queue import Queue
from adbc_driver_flightsql import dbapi as gizmosql
import threading

class ConnectionPool:
    def __init__(self, size=10, uri="grpc+tls://localhost:31337", **kwargs):
        self.pool = Queue(maxsize=size)
        for _ in range(size):
            conn = gizmosql.connect(uri=uri, db_kwargs=kwargs, autocommit=True)
            self.pool.put(conn)
    
    def get_connection(self):
        return self.pool.get()
    
    def return_connection(self, conn):
        self.pool.put(conn)

# Usage
pool = ConnectionPool(size=10, username="user", password="pass")
conn = pool.get_connection()
# ... use connection ...
pool.return_connection(conn)
```

---

## Batch Processing

### Bulk Insert

```python
import pyarrow as pa

# Create Arrow table with 1M rows
data = pa.table({
    'id': range(1000000),
    'value': [f'value_{i}' for i in range(1000000)]
})

# Bulk insert
with conn.cursor() as cur:
    cur.adbc_ingest("my_table", data, mode="append")
```

### Batch Queries

```python
# Process in batches
batch_size = 10000
for offset in range(0, total_rows, batch_size):
    cur.execute(f"""
        SELECT * FROM large_table 
        LIMIT {batch_size} OFFSET {offset}
    """)
    batch = cur.fetch_arrow_table()
    process_batch(batch)
```

---

## Query Timeout

Prevent runaway queries:

```bash
gizmosql_server --query-timeout 300  # 5 minutes
```

Benefits:
- Prevents resource exhaustion
- Improves multi-user experience
- Forces query optimization

---

## Monitoring

### Enable Query Logging

```bash
export PRINT_QUERIES="1"
```

### DuckDB Query Profiling

```sql
SET enable_profiling = true;
SET profiling_output = '/tmp/profile.json';

-- Run your query
SELECT * FROM large_table;

-- View profile
SELECT * FROM 'file:///tmp/profile.json';
```

### Prometheus Metrics (Custom)

Expose metrics for monitoring:

```python
from prometheus_client import Counter, Histogram, start_http_server

query_counter = Counter('gizmosql_queries_total', 'Total queries')
query_duration = Histogram('gizmosql_query_duration_seconds', 'Query duration')

@query_duration.time()
def execute_query(query):
    query_counter.inc()
    # Execute query...
    pass

start_http_server(9090)
```

---

## Caching Strategies

### Result Caching

Cache frequently accessed results:

```python
from functools import lru_cache

@lru_cache(maxsize=100)
def get_report_data(report_id):
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM reports WHERE id = {report_id}")
        return cur.fetch_arrow_table()
```

### Materialized Views

Pre-compute expensive queries:

```sql
-- Create materialized view
CREATE TABLE sales_summary AS
SELECT 
    product_id,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_amount
FROM sales
GROUP BY product_id;

-- Refresh periodically
DROP TABLE IF EXISTS sales_summary;
CREATE TABLE sales_summary AS
SELECT 
    product_id,
    SUM(quantity) as total_quantity,
    SUM(amount) as total_amount
FROM sales
GROUP BY product_id;
```

---

## Compression

### DuckDB Compression

DuckDB automatically compresses data:

```sql
-- Check compression ratios
SELECT 
    database_name,
    schema_name,
    table_name,
    column_name,
    compression,
    pg_size_pretty(data_size) as size
FROM duckdb_columns();
```

### Parquet Compression

```sql
-- Export with compression
COPY (SELECT * FROM large_table) 
TO 'output.parquet' 
(FORMAT PARQUET, COMPRESSION 'ZSTD', COMPRESSION_LEVEL 9);
```

---

## Network Optimization

### Reduce Data Transfer

Use projections to select only needed columns:

```sql
-- ❌ BAD - Transfers all columns
SELECT * FROM large_table;

-- ✅ GOOD - Only needed columns
SELECT id, name, created_at FROM large_table;
```

### Arrow Streaming

Use Arrow's streaming capabilities:

```python
# Stream large results
with conn.cursor() as cur:
    cur.execute("SELECT * FROM huge_table")
    
    # Process in chunks
    while True:
        batch = cur.fetch_record_batch()
        if batch is None:
            break
        process_batch(batch)
```

---

## Parallel Execution

### DuckDB Parallel Query Execution

DuckDB automatically parallelizes queries:

```sql
-- Enable parallel execution
SET enable_parallel_query = true;
SET threads = 16;

-- This query will use all 16 threads
SELECT customer_id, SUM(amount)
FROM sales
GROUP BY customer_id;
```

### Multiple Concurrent Clients

Run multiple client connections:

```python
import concurrent.futures

def execute_query(query_id):
    with gizmosql.connect(...) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM table_{query_id}")
            return cur.fetch_arrow_table()

# Execute 10 queries in parallel
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(execute_query, i) for i in range(10)]
    results = [f.result() for f in futures]
```

---

## Common Performance Issues

### Issue: Slow Aggregations

**Solution:** Increase threads and memory

```sql
SET threads = 16;
SET memory_limit = '32GB';
```

### Issue: Out of Memory

**Solution:** Enable spilling to disk

```sql
SET temp_directory = '/large-storage/tmp';
SET max_temp_directory_size = '100GB';
```

### Issue: Slow Table Scans

**Solution:** Create indexes or partition data

```sql
CREATE INDEX idx_filter_column ON my_table(filter_column);
```

### Issue: Network Bottleneck

**Solution:** Use local deployment or compression

```sql
-- Enable compression
SET arrow_compression = 'zstd';
```

---

## Performance Testing

### Benchmark Your Setup

```bash
# Run TPC-H benchmark
duckdb mydb.duckdb << EOF
INSTALL tpch;
LOAD tpch;
CALL dbgen(sf=10);

.timer on
SELECT * FROM lineitem LIMIT 10;
EOF
```

### Load Testing

Use tools like:
- Apache JMeter
- k6
- Locust
- wrk

Example with Python:

```python
import time
import concurrent.futures

def run_query():
    start = time.time()
    with gizmosql.connect(...) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM large_table")
            cur.fetchall()
    return time.time() - start

# Run 100 concurrent queries
with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    futures = [executor.submit(run_query) for _ in range(1000)]
    times = [f.result() for f in futures]

print(f"Avg: {sum(times)/len(times):.2f}s")
print(f"Min: {min(times):.2f}s")
print(f"Max: {max(times):.2f}s")
```

---

## Performance Checklist

- [ ] Backend matches workload (DuckDB for analytics, SQLite for transactions)
- [ ] Thread count matches CPU cores
- [ ] Memory limit set appropriately
- [ ] Fast storage (NVMe SSD) for data and temp files
- [ ] Queries use projections (not SELECT *)
- [ ] Indexes created for filter columns
- [ ] Query timeout configured
- [ ] Connection pooling implemented
- [ ] Monitoring and logging enabled
- [ ] Batch processing for large operations

---

## Next Steps

- [Configuration options](configuration.md)
- [Security best practices](security.md)
- [Troubleshooting guide](troubleshooting.md)

---

## Additional Resources

- [DuckDB Performance Guide](https://duckdb.org/docs/guides/performance/overview)
- [SQLite Performance Tips](https://www.sqlite.org/performance.html)
- [Apache Arrow Performance](https://arrow.apache.org/docs/format/Columnar.html)
