# Manual PIVOT Testing Guide for GitHub Issue #44

This document provides step-by-step instructions for manually testing the PIVOT multiple statement issue in GizmoSQL.

## Overview

**Issue**: DuckDB's PIVOT statement gets internally rewritten to multiple SQL statements, but GizmoSQL's prepared statement mechanism only accepts single statements, causing the error:
```
Error: Invalid Input Error: Cannot prepare multiple statements at once! (state=,code=0)
```

**Solution**: Implement fallback from prepared statements to direct query execution for statements that cannot be prepared.

---

## Prerequisites

**Build Tools and Dependencies**: 
```bash
# macOS
brew install cmake ninja boost gflags

# Ubuntu/Debian
sudo apt-get install cmake ninja-build libboost-all-dev libgflags-dev
```

---

## Step 1: Build

```bash
# Configure and build
cmake -S . -B build \
  -DCMAKE_PREFIX_PATH="$(brew --prefix gflags);$(brew --prefix boost)" \
  -DCMAKE_INCLUDE_PATH="$(brew --prefix boost)/include" \
  -DCMAKE_LIBRARY_PATH="$(brew --prefix boost)/lib"

cmake --build build --target gizmosql_server gizmosql_client
```

---

## Step 2: Create Test Database

```bash
# Create data directory and test database
mkdir -p data

duckdb data/pivot_test.duckdb << 'EOF'
CREATE TABLE pivottest (
    period Date,
    category String,
    league String,
    pnl_amount DECIMAL(38, 2)
);

INSERT INTO pivottest VALUES
    ('2024-01-01', 'Other Sales Revenue', 'C', 16304900),
    ('2024-02-01', 'Discount', 'M', 17918200),
    ('2024-03-01', 'Discount', 'C', 18693200),
    ('2024-04-01', 'Other Sales Revenue', 'N', 7374843),
    ('2024-05-01', 'Discount', 'M', 17918200);
EOF
```

---

## Step 3: Start Server

```bash
# Start server (keep this terminal open)
export GIZMOSQL_PASSWORD="test123"
./build/gizmosql_server --database-filename data/pivot_test.duckdb --port 31337 --print-queries
```

---

## Step 4: Run Tests

In a new terminal (keep server running):

### Test 1: Regular Query
```bash
./build/gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT COUNT(*) FROM pivottest"
```
**Expected**: Returns count of 5 rows

### Test 2: Standard PIVOT
```bash
./build/gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "SELECT * FROM pivottest PIVOT (SUM(pnl_amount) FOR league IN ('M', 'C', 'N') GROUP BY category)"
```
**Expected**: Returns pivoted data

### Test 3: Problematic PIVOT (The Fix)
```bash
./build/gizmosql_client \
    --command Execute \
    --host localhost \
    --port 31337 \
    --username gizmosql_username \
    --password test123 \
    --query "PIVOT (select * from pivottest where (league in ('M'))) ON league USING sum(pnl_amount) GROUP BY category ORDER BY category LIMIT 100 OFFSET 0"
```
**Before Fix**: Fails with "Cannot prepare multiple statements at once!"  
**After Fix**: Returns pivoted data successfully

---

## Results

**Before Fix:**
- Test 1: ✅ Works 
- Test 2: ✅ Works
- Test 3: ❌ Fails with "Cannot prepare multiple statements at once!"

**After Fix:**
- Test 1: ✅ Works 
- Test 2: ✅ Works
- Test 3: ✅ Works (returns pivoted data)

---

## Cleanup

Stop the server with `Ctrl+C` in the server terminal.