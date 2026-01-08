# Troubleshooting

Common issues and solutions for GizmoSQL.

---

## Connection Issues

### Cannot Connect to Server

**Symptoms:**
- Connection refused
- Timeout errors
- Network unreachable

**Solutions:**

<!-- tabs:start -->

#### **Check Server Status**

```bash
# Docker
docker ps | grep gizmosql
docker logs gizmosql

# Process
ps aux | grep gizmosql_server
```

#### **Verify Port**

```bash
# Check if port is listening
netstat -an | grep 31337

# Test connection
telnet localhost 31337
nc -zv localhost 31337
```

#### **Check Firewall**

```bash
# UFW (Ubuntu)
sudo ufw status
sudo ufw allow 31337/tcp

# firewalld (RHEL/CentOS)
sudo firewall-cmd --list-ports
sudo firewall-cmd --add-port=31337/tcp --permanent
sudo firewall-cmd --reload
```

<!-- tabs:end -->

---

### Invalid Bearer Token Error

**Error Message:**
```
Invalid bearer token provided. Detail: Unauthenticated
```

**Cause:** Server was restarted, invalidating cached tokens.

**Solution:**

1. Change password and reconnect:
```bash
docker stop gizmosql
docker run ... --env GIZMOSQL_PASSWORD="new_password" ...
```

2. Or clear client-side token cache:
```bash
# Remove cached credentials
rm -rf ~/.adbc/cache/
```

---

### TLS Certificate Verification Failed

**Error Message:**
```
SSL certificate verification failed
```

**Solutions:**

<!-- tabs:start -->

#### **JDBC**

```text
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&disableCertificateVerification=true
```

#### **Python ADBC**

```python
from adbc_driver_flightsql import DatabaseOptions

conn = gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "user",
        "password": "pass",
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
    }
)
```

#### **CLI**

```bash
gizmosql_client --tls-skip-verify ...
```

<!-- tabs:end -->

?> **Production**: Use proper CA-signed certificates instead of skipping verification.

---

## Database Issues

### Database File Not Found

**Error Message:**
```
Error opening database: No such file or directory
```

**Solutions:**

```bash
# Check file exists
ls -lh /path/to/database.duckdb

# Check permissions
chmod 644 /path/to/database.duckdb

# Docker: Verify mount
docker run ... \
  --mount type=bind,source=$(pwd)/data,target=/data \
  --env DATABASE_FILENAME="/data/mydb.duckdb"
```

---

### Database Locked

**Error Message:**
```
database is locked
```

**Cause:** Multiple processes accessing SQLite database.

**Solutions:**

1. Use DuckDB instead (better concurrency)
2. Enable WAL mode for SQLite:
```sql
PRAGMA journal_mode = WAL;
```

3. Close other connections:
```bash
lsof /path/to/database.sqlite
kill <PID>
```

---

### Out of Memory

**Error Message:**
```
Out of Memory Error
```

**Solutions:**

1. Increase memory limit:
```sql
SET memory_limit = '32GB';
```

2. Enable disk spilling:
```sql
SET temp_directory = '/large-storage/tmp';
SET max_temp_directory_size = '100GB';
```

3. Reduce query scope:
```sql
-- Use LIMIT
SELECT * FROM large_table LIMIT 1000;

-- Use filters
SELECT * FROM large_table WHERE date > '2024-01-01';
```

4. Docker memory limit:
```bash
docker run --memory="32g" --memory-swap="32g" ...
```

---

## Query Issues

### Query Timeout

**Error Message:**
```
Query execution timed out
```

**Solutions:**

1. Increase timeout:
```bash
gizmosql_server --query-timeout 600  # 10 minutes
```

2. Optimize query:
```sql
-- Add indexes
CREATE INDEX idx_filter ON table(filter_column);

-- Use projections
SELECT id, name FROM table;  -- Not SELECT *

-- Add filters
WHERE date > CURRENT_DATE - INTERVAL '7 days'
```

3. Check query plan:
```sql
EXPLAIN ANALYZE SELECT ...;
```

---

### Slow Queries

**Symptoms:**
- Queries taking longer than expected
- High CPU usage
- High memory usage

**Diagnostic Steps:**

```sql
-- Enable profiling
SET enable_profiling = true;

-- Run query
SELECT * FROM large_table;

-- View profile
SELECT * FROM pragma_last_profiling_output();
```

**Solutions:**

1. Add indexes:
```sql
CREATE INDEX idx_customer ON orders(customer_id);
```

2. Increase resources:
```sql
SET threads = 16;
SET memory_limit = '32GB';
```

3. Partition data:
```sql
CREATE TABLE orders_partitioned AS
SELECT * FROM orders
PARTITION BY (YEAR(order_date), MONTH(order_date));
```

---

## Docker Issues

### Container Won't Start

**Check Logs:**
```bash
docker logs gizmosql
```

**Common Issues:**

1. **Port already in use:**
```bash
# Find process using port
lsof -i :31337

# Use different port
docker run ... --publish 31338:31337 ...
```

2. **Invalid environment variables:**
```bash
# Check variables
docker inspect gizmosql | jq '.[0].Config.Env'

# Fix and restart
docker stop gizmosql
docker rm gizmosql
docker run ... --env GIZMOSQL_PASSWORD="valid_password" ...
```

3. **Mount permission issues:**
```bash
# Fix permissions
chmod -R 755 /path/to/data
chown -R $(id -u):$(id -g) /path/to/data
```

---

### Container Exits Immediately

**Check Exit Code:**
```bash
docker ps -a | grep gizmosql
docker logs gizmosql
```

**Common Causes:**

1. Missing required environment variables
2. Database file path incorrect
3. Initialization SQL commands failed

**Solution:**
```bash
# Run interactively to see errors
docker run --rm -it gizmodata/gizmosql:latest
```

---

## Performance Issues

### High CPU Usage

**Diagnostic:**

```bash
# Check CPU usage
top -p $(pgrep gizmosql_server)

# Docker
docker stats gizmosql
```

**Solutions:**

1. Limit threads:
```sql
SET threads = 8;
```

2. Kill long-running queries:
```bash
docker exec gizmosql pkill -TERM gizmosql_server
```

3. Add query timeout:
```bash
gizmosql_server --query-timeout 300
```

---

### High Memory Usage

**Diagnostic:**

```bash
# Check memory
free -h
docker stats gizmosql
```

**Solutions:**

1. Reduce memory limit:
```sql
SET memory_limit = '16GB';
```

2. Enable spilling:
```sql
SET temp_directory = '/tmp';
```

3. Restart server:
```bash
docker restart gizmosql
```

---

## Client-Specific Issues

### Python ADBC Issues

**Import Error:**
```python
ImportError: No module named 'adbc_driver_flightsql'
```

**Solution:**
```bash
pip install --upgrade adbc-driver-flightsql
```

**Connection Issues:**
```python
# Verify connection parameters
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

try:
    conn = gizmosql.connect(
        uri="grpc+tls://localhost:31337",
        db_kwargs={
            "username": "gizmosql_username",
            "password": os.getenv("GIZMOSQL_PASSWORD"),
            DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
        },
        autocommit=True
    )
    print("Connection successful!")
except Exception as e:
    print(f"Connection failed: {e}")
```

---

### JDBC Issues

**Driver Not Found:**
```
java.sql.SQLException: No suitable driver found
```

**Solution:**
```java
// Explicitly load driver
Class.forName("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
```

**Connection Timeout:**
```text
jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&connectTimeout=30000
```

---

## Kubernetes Issues

### Pod CrashLoopBackOff

**Check Logs:**
```bash
kubectl logs -f deployment/gizmosql
kubectl describe pod <pod-name>
```

**Common Causes:**

1. Missing secrets
2. PVC not bound
3. Resource limits too low

**Solutions:**

```bash
# Check secrets
kubectl get secrets

# Check PVC
kubectl get pvc

# Increase resources
kubectl edit deployment gizmosql
# Update limits and requests
```

---

### Persistent Volume Issues

**PVC Pending:**
```bash
kubectl get pvc
```

**Solutions:**

1. Check storage class:
```bash
kubectl get storageclass
```

2. Create PV manually:
```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: gizmosql-pv
spec:
  capacity:
    storage: 100Gi
  accessModes:
    - ReadWriteOnce
  hostPath:
    path: /data/gizmosql
```

---

## Debugging Tips

### Enable Debug Logging

```bash
# Docker
docker run ... --env LOG_LEVEL="DEBUG" ...

# CLI
gizmosql_server --log-level debug ...
```

### Query Logging

```bash
export PRINT_QUERIES="1"
```

### Network Debugging

```bash
# Test connectivity
nc -zv localhost 31337

# Monitor traffic
tcpdump -i any port 31337

# Test TLS
openssl s_client -connect localhost:31337
```

### Profiling

```sql
-- DuckDB
SET enable_profiling = true;
SET profiling_mode = 'detailed';

-- Run query
SELECT * FROM large_table;

-- View profile
SELECT * FROM pragma_last_profiling_output();
```

---

## Common Error Messages

### Error: "No such table"

**Cause:** Table doesn't exist in database.

**Solution:**
```sql
-- List tables
SHOW TABLES;

-- Check schema
SELECT * FROM information_schema.tables;

-- Create table if missing
CREATE TABLE my_table (id INTEGER, name VARCHAR);
```

---

### Error: "Permission denied"

**Cause:** Insufficient file permissions.

**Solution:**
```bash
# Check permissions
ls -l /path/to/database.duckdb

# Fix permissions
chmod 644 /path/to/database.duckdb
chown user:group /path/to/database.duckdb
```

---

### Error: "Address already in use"

**Cause:** Port 31337 is in use.

**Solution:**
```bash
# Find process
lsof -i :31337

# Kill process
kill -9 <PID>

# Or use different port
gizmosql_server --port 31338
```

---

## Getting Help

If you can't resolve the issue:

### Gather Information

```bash
# GizmoSQL version
gizmosql_server --version

# Docker info
docker version
docker info

# System info
uname -a
free -h
df -h

# Logs
docker logs gizmosql > gizmosql.log
```

### Report Issue

1. **GitHub Issues**: [gizmosql/issues](https://github.com/gizmodata/gizmosql/issues)
2. **Email Support**: info@gizmodata.com

**Include:**
- GizmoSQL version
- Operating system
- Error message
- Steps to reproduce
- Relevant logs

---

## FAQ

**Q: Can I use SQLite and DuckDB simultaneously?**  
A: No, choose one backend per server instance.

**Q: How do I migrate data between backends?**  
A: Export to Parquet and import to new backend:
```sql
-- Export from DuckDB
COPY (SELECT * FROM table) TO 'data.parquet';

-- Import to SQLite
CREATE TABLE table AS SELECT * FROM 'data.parquet';
```

**Q: What's the maximum database size?**  
A: Limited only by available storage. DuckDB handles TB-scale datasets efficiently.

**Q: Can I run multiple GizmoSQL instances?**  
A: Yes, use different ports and database files for each instance.

**Q: Does GizmoSQL support clustering?**  
A: No, GizmoSQL is designed for single-node deployment. Use query federation for distributed queries.

---

## Best Practices

- ✅ Always use TLS in production
- ✅ Set appropriate query timeouts
- ✅ Monitor resource usage
- ✅ Regular backups
- ✅ Keep GizmoSQL updated
- ✅ Use connection pooling
- ✅ Optimize queries with EXPLAIN
- ✅ Test changes in development first

---

## Next Steps

- [Configuration guide](configuration.md)
- [Security best practices](security.md)
- [Performance tuning](performance.md)

---

## Additional Resources

- [DuckDB Documentation](https://duckdb.org/docs/)
- [SQLite Documentation](https://sqlite.org/docs.html)
- [Apache Arrow Documentation](https://arrow.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
