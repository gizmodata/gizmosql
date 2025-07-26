# Manual PIVOT Testing Guide for GitHub Issue #44

This document provides step-by-step instructions for manually testing the PIVOT multiple statement issue in GizmoSQL using Docker.

## Overview

**Issue**: DuckDB's PIVOT statement gets internally rewritten to multiple SQL statements, but GizmoSQL's prepared statement mechanism only accepts single statements, causing the error:
```
Error: Invalid Input Error: Cannot prepare multiple statements at once! (state=,code=0)
```

**Solution**: Implement fallback from prepared statements to direct query execution for statements that cannot be prepared.

## Quick Overview

This guide covers:
1. **Database Setup** - Create test data with problematic PIVOT queries
2. **Docker Build** - Build custom image with our PIVOT fix
3. **Testing** - Compare original issue (fails) vs our fix (works)
4. **Validation** - Ensure no regressions in existing functionality

---

## Prerequisites

1. **Docker**: Ensure Docker is installed and running
2. **Python Environment** (for ADBC testing):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

---

## Test Database Setup

Before testing, we need to create a DuckDB database with the PIVOT test data from GitHub issue #44.

### Option 1: Create Test Database File
```bash
# Create test data using DuckDB CLI
duckdb data/pivot_test.duckdb << 'EOF'
-- Create the test table
CREATE TABLE IF NOT EXISTS pivottest (
    period Date,
    category String,
    league String,
    pnl_amount DECIMAL(38, 2)
);

-- Insert test data from GitHub issue #44
INSERT INTO pivottest (period, category, league, pnl_amount) VALUES
    ('2024-01-01', 'Other Sales Revenue', 'C', 16304900),
    ('2024-02-01', 'Discount', 'M', 17918200),
    ('2024-03-01', 'Discount', 'C', 18693200),
    ('2024-04-01', 'Other Sales Revenue', 'N', 7374843),
    ('2024-05-01', 'Discount', 'M', 17918200);

-- Verify data
SELECT * FROM pivottest ORDER BY period;
EOF
```

### Option 2: Use Built-in Database (Docker Only)
If using Docker, you can create the test data directly in the running container's built-in database (see Step 3 below).

---

## Build Custom Docker Image with PIVOT Fix

### Step 1: Build Docker Image with Our Changes
```bash
# Build a custom Docker image with the PIVOT fix
docker build -t gizmosql-pivot-fix:latest .
```

This will build the GizmoSQL server with our PIVOT multiple statement fix included.

### Step 2: Start GizmoSQL Server with Our Custom Image

#### Option A: Using Local Database File (if created above)
```bash
# Start server mounting the local test database
docker run --name gizmosql-pivot-test \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="test123" \
           --env PRINT_QUERIES="1" \
           --env DATABASE_FILENAME="data/pivot_test.duckdb" \
           --mount type=bind,source=$(pwd)/data,target=/opt/gizmosql/data \
           gizmosql-pivot-fix:latest
```

#### Option B: Using Built-in Database
```bash
# Start server with built-in database (will add test data in Step 3)
docker run --name gizmosql-pivot-test \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_PASSWORD="test123" \
           --env PRINT_QUERIES="1" \
           gizmosql-pivot-fix:latest
```

### Step 3: Set Up PIVOT Test Data (Option B Only)

**Skip this step if you used Option A** (local database file) - your test data is already loaded.

**For Option B** (built-in database), add the test data:
```bash
# Connect to the server and create test data in built-in database
docker exec -it gizmosql-pivot-test duckdb /opt/gizmosql/data/TPC-H-small.duckdb << 'EOF'
-- Create the test table
CREATE TABLE IF NOT EXISTS pivottest (
    period Date,
    category String,
    league String,
    pnl_amount DECIMAL(38, 2)
);

-- Insert test data from GitHub issue #44
INSERT INTO pivottest (period, category, league, pnl_amount) VALUES
    ('2024-01-01', 'Other Sales Revenue', 'C', 16304900),
    ('2024-02-01', 'Discount', 'M', 17918200),
    ('2024-03-01', 'Discount', 'C', 18693200),
    ('2024-04-01', 'Other Sales Revenue', 'N', 7374843),
    ('2024-05-01', 'Discount', 'M', 17918200);

-- Verify data
SELECT * FROM pivottest ORDER BY period;
EOF
```

---

## Manual Testing Methods

### Method 1: Docker CLI Client Testing

#### Test 1: Regular Query (Should Work)
```bash
docker exec gizmosql-pivot-test gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT COUNT(*) FROM pivottest" \
    --use-tls \
    --tls-skip-verify
```

**Expected Result**: Returns count of 5 rows

#### Test 2: Standard PIVOT Syntax (Should Work)
```bash
docker exec gizmosql-pivot-test gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT * FROM pivottest PIVOT (SUM(pnl_amount) FOR league IN ('M', 'C', 'N') GROUP BY category)" \
    --use-tls \
    --tls-skip-verify
```

**Expected Result**: Returns pivoted data with columns for each league

#### Test 3: Problematic PIVOT Syntax (Will Fail Before Fix)
```bash
docker exec gizmosql-pivot-test gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "PIVOT (select * from pivottest where (league in ('M'))) ON league USING sum(pnl_amount) GROUP BY category ORDER BY category LIMIT 100 OFFSET 0" \
    --use-tls \
    --tls-skip-verify
```

**Before Fix**: Should fail with "Cannot prepare multiple statements at once!"  
**After Fix**: Should return pivoted data successfully

### Method 2: Python ADBC Testing

Create a test script `manual_pivot_test.py`:

```python
#!/usr/bin/env python3
import os
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

def test_pivot_issue():
    connection_params = {
        "uri": "grpc+tls://localhost:31337",
        "db_kwargs": {
            "username": "gizmosql_username",
            "password": "test123",
            DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
        }
    }
    
    with gizmosql.connect(**connection_params) as conn:
        with conn.cursor() as cur:
            print("=== Test 1: Regular Query ===")
            cur.execute("SELECT COUNT(*) as count FROM pivottest")
            result = cur.fetch_arrow_table()
            print(f"Row count: {result}")
            
            print("\n=== Test 2: Standard PIVOT Syntax ===")
            try:
                cur.execute("""
                    SELECT * FROM pivottest 
                    PIVOT (SUM(pnl_amount) FOR league IN ('M', 'C', 'N') GROUP BY category)
                """)
                result = cur.fetch_arrow_table()
                print(f"Standard PIVOT result: {result}")
            except Exception as e:
                print(f"Standard PIVOT failed: {e}")
            
            print("\n=== Test 3: Problematic PIVOT Syntax ===")
            try:
                cur.execute("""
                    PIVOT (select * from pivottest where (league in ('M'))) 
                    ON league USING sum(pnl_amount) 
                    GROUP BY category ORDER BY category LIMIT 100 OFFSET 0
                """)
                result = cur.fetch_arrow_table()
                print(f"Problematic PIVOT result: {result}")
                print("✅ SUCCESS: Problematic PIVOT now works!")
            except Exception as e:
                if "Cannot prepare multiple statements at once" in str(e):
                    print(f"❌ EXPECTED FAILURE (before fix): {e}")
                else:
                    print(f"❌ UNEXPECTED ERROR: {e}")

if __name__ == "__main__":
    test_pivot_issue()
```

Run the test:
```bash
python manual_pivot_test.py
```

### Method 3: JDBC Testing

For JDBC testing with DBeaver or other tools:

1. **JDBC Connection String:**
   ```
   jdbc:arrow-flight-sql://localhost:31337?useEncryption=true&user=gizmosql_username&password=test123&disableCertificateVerification=true
   ```

2. **Test Queries:** Execute the same queries as in the CLI tests above

---

## Expected Results

### Before Implementing the Fix

| Test Case | Expected Result |
|-----------|----------------|
| Regular Query | ✅ Works (returns 5 rows) |
| Standard PIVOT | ✅ Works (returns pivoted data) |
| Problematic PIVOT | ❌ Fails with "Cannot prepare multiple statements at once!" |

### After Implementing the Fix

| Test Case | Expected Result |
|-----------|----------------|
| Regular Query | ✅ Works (returns 5 rows) |
| Standard PIVOT | ✅ Works (returns pivoted data) |
| Problematic PIVOT | ✅ Works (returns pivoted data) |

---

## Docker Cleanup

### Stop and Clean Up
```bash
# Stop the test container
docker stop gizmosql-pivot-test

# The container will be automatically removed due to --rm flag
# If you need to force removal:
# docker rm -f gizmosql-pivot-test
```

---

## Troubleshooting

### Docker Issues
```bash
# Check if container is running
docker ps | grep gizmosql-pivot-test

# View container logs
docker logs gizmosql-pivot-test

# Restart container if needed
docker restart gizmosql-pivot-test
```

### Connection Issues
```bash
# Test connection with simple query
docker exec gizmosql-pivot-test gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT 1" \
    --use-tls \
    --tls-skip-verify
```

### Test Data Issues
```bash
# Verify test data exists
docker exec gizmosql-pivot-test gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT * FROM pivottest LIMIT 5" \
    --use-tls \
    --tls-skip-verify
```

---

## Implementation Verification

After implementing the fix, you should see:

1. **Test 3 (Problematic PIVOT) succeeds** instead of failing with "Cannot prepare multiple statements at once!"

2. **Server logs show queries being executed** (with `PRINT_QUERIES="1"`)

3. **All other tests continue to pass** (no regressions)

4. **Performance maintained for regular queries** (they still use prepared statements)

## Quick Comparison: Before vs After Fix

### Test the Original Issue (Should Fail)
```bash
# Test with original GizmoSQL image (should fail with "Cannot prepare multiple statements")
docker run --rm -d --name gizmosql-original -p 31337:31337 -e TLS_ENABLED=1 -e GIZMOSQL_PASSWORD=test123 gizmodata/gizmosql:latest && sleep 10 && docker exec gizmosql-original duckdb /opt/gizmosql/data/TPC-H-small.duckdb -c "CREATE TABLE pivottest (period Date, category String, league String, pnl_amount DECIMAL(38, 2)); INSERT INTO pivottest VALUES ('2024-01-01', 'Other Sales Revenue', 'C', 16304900), ('2024-02-01', 'Discount', 'M', 17918200);" && docker exec gizmosql-original gizmosql_client --command Execute --host localhost --port 31337 --username gizmosql_username --password test123 --query "PIVOT (select * from pivottest where (league in ('M'))) ON league USING sum(pnl_amount) GROUP BY category" --use-tls --tls-skip-verify; docker stop gizmosql-original
```

**Expected Result**: ❌ Fails with "Cannot prepare multiple statements at once!"

### Test with Our Fix (Should Work)
```bash
# First build our custom image with the fix
docker build -t gizmosql-pivot-fix:latest .

# Test with our fixed image (should work)
docker run --rm -d --name gizmosql-fixed -p 31337:31337 -e TLS_ENABLED=1 -e GIZMOSQL_PASSWORD=test123 gizmosql-pivot-fix:latest && sleep 10 && docker exec gizmosql-fixed duckdb /opt/gizmosql/data/TPC-H-small.duckdb -c "CREATE TABLE pivottest (period Date, category String, league String, pnl_amount DECIMAL(38, 2)); INSERT INTO pivottest VALUES ('2024-01-01', 'Other Sales Revenue', 'C', 16304900), ('2024-02-01', 'Discount', 'M', 17918200);" && docker exec gizmosql-fixed gizmosql_client --command Execute --host localhost --port 31337 --username gizmosql_username --password test123 --query "PIVOT (select * from pivottest where (league in ('M'))) ON league USING sum(pnl_amount) GROUP BY category" --use-tls --tls-skip-verify; docker stop gizmosql-fixed
```

**Expected Result**: ✅ Works and returns pivoted data!

## Alternative: Test Without Docker Build

If you want to test the current behavior without building, you can:

1. **Reproduce the issue** with the existing Docker image:
   ```bash
   docker run --rm -d --name gizmosql-issue-demo -p 31337:31337 -e TLS_ENABLED=1 -e GIZMOSQL_PASSWORD=test123 gizmodata/gizmosql:latest
   # ... add test data and run problematic PIVOT query (will fail)
   ```

2. **Build and test locally** (for development):
   ```bash
   # Build the project locally with our changes
   cmake -S . -B build && cmake --build build --target gizmosql_server
   
   # Run the server locally
   GIZMOSQL_PASSWORD="test123" ./build/gizmosql_server --database-filename data/test.duckdb --print-queries
   ```

The Docker approach provides the most realistic testing environment and ensures the fix works in the same containerized setup that users will actually use.