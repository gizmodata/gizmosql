#!/usr/bin/env python3
"""
Test script for PIVOT multiple statement issue in GizmoSQL.
Tests the specific issue described in GitHub issue #44.

This test should FAIL initially (before the fix) and PASS after implementing
the fallback from prepared statements to direct query execution.
"""

import os
import sys
import time
from typing import Optional
import pyarrow
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions


def setup_test_data(connection_params: dict) -> bool:
    """Create test table and data for PIVOT testing."""
    try:
        with gizmosql.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Create test table
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS pivottest (
                        period Date,
                        category String,
                        league String,
                        pnl_amount DECIMAL(38, 2)
                    )
                """)
                
                # Clear any existing data
                cur.execute("DELETE FROM pivottest")
                
                # Insert test data from the GitHub issue
                cur.execute("""
                    INSERT INTO pivottest (period, category, league, pnl_amount) VALUES
                        ('2024-01-01', 'Other Sales Revenue', 'C', 16304900),
                        ('2024-02-01', 'Discount', 'M', 17918200),
                        ('2024-03-01', 'Discount', 'C', 18693200),
                        ('2024-04-01', 'Other Sales Revenue', 'N', 7374843),
                        ('2024-05-01', 'Discount', 'M', 17918200)
                """)
                
                print("✅ Test data setup completed")
                return True
                
    except Exception as e:
        print(f"❌ Failed to setup test data: {e}")
        return False


def test_standard_pivot_syntax(connection_params: dict) -> bool:
    """Test standard SQL PIVOT syntax (should work)."""
    try:
        with gizmosql.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # This should work with both prepared statements and direct queries
                query = """
                    SELECT *
                    FROM pivottest
                    PIVOT (
                        SUM(pnl_amount)
                        FOR league IN ('M', 'C', 'N')
                        GROUP BY category
                    )
                """
                cur.execute(query)
                result = cur.fetch_arrow_table()
                
                print(f"✅ Standard PIVOT syntax works: {result.num_rows} rows returned")
                return True
                
    except Exception as e:
        print(f"❌ Standard PIVOT syntax failed: {e}")
        return False


def test_problematic_pivot_syntax(connection_params: dict) -> bool:
    """Test the problematic PIVOT syntax that fails with 'Cannot prepare multiple statements at once!'"""
    try:
        with gizmosql.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # This is the failing query from GitHub issue #44
                query = """
                    PIVOT (select * from pivottest where (league in ('M'))) 
                    ON league 
                    USING sum(pnl_amount) 
                    GROUP BY category 
                    ORDER BY category 
                    LIMIT 100 OFFSET 0
                """
                cur.execute(query)
                result = cur.fetch_arrow_table()
                
                print(f"✅ Problematic PIVOT syntax now works: {result.num_rows} rows returned")
                return True
                
    except Exception as e:
        error_msg = str(e)
        if "Cannot prepare multiple statements at once" in error_msg:
            print(f"❌ Expected failure: {error_msg}")
            print("   This test will pass after implementing the fix")
            return False
        else:
            print(f"❌ Unexpected error in problematic PIVOT: {e}")
            return False


def test_regular_queries_still_work(connection_params: dict) -> bool:
    """Ensure regular queries still work (should use prepared statements)."""
    try:
        with gizmosql.connect(**connection_params) as conn:
            with conn.cursor() as cur:
                # Simple SELECT query
                cur.execute("SELECT COUNT(*) as row_count FROM pivottest")
                result = cur.fetch_arrow_table()
                
                row_count = result.column('row_count')[0].as_py()
                if row_count == 5:
                    print(f"✅ Regular queries work: {row_count} rows in test table")
                    return True
                else:
                    print(f"❌ Unexpected row count: {row_count}, expected 5")
                    return False
                    
    except Exception as e:
        print(f"❌ Regular query failed: {e}")
        return False


def main():
    """Main test runner."""
    print("🧪 Testing PIVOT Multiple Statement Issue (GitHub #44)")
    print("=" * 60)
    
    # Setup connection parameters
    gizmosql_password = os.getenv("GIZMOSQL_PASSWORD", "test123")
    connection_params = {
        "uri": "grpc+tls://localhost:31337",
        "db_kwargs": {
            "username": "gizmosql_username",
            "password": gizmosql_password,
            DatabaseOptions.TLS_SKIP_VERIFY.value: "true"
        }
    }
    
    # Wait for server to be ready
    print("⏳ Waiting for GizmoSQL server...")
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            with gizmosql.connect(**connection_params) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetch_arrow_table()
                    break
        except Exception as e:
            if attempt == max_attempts - 1:
                print(f"❌ Could not connect to server after {max_attempts} attempts: {e}")
                sys.exit(1)
            time.sleep(1)
    
    print("✅ Connected to GizmoSQL server")
    
    # Run tests
    tests_passed = 0
    total_tests = 4
    
    print("\n📋 Running Tests:")
    print("-" * 40)
    
    if setup_test_data(connection_params):
        tests_passed += 1
    
    if test_regular_queries_still_work(connection_params):
        tests_passed += 1
        
    if test_standard_pivot_syntax(connection_params):
        tests_passed += 1
        
    # This test is expected to fail initially
    problematic_test_passed = test_problematic_pivot_syntax(connection_params)
    if problematic_test_passed:
        tests_passed += 1
    
    # Summary
    print("\n" + "=" * 60)
    print(f"📊 Test Results: {tests_passed}/{total_tests} tests passed")
    
    if tests_passed == total_tests:
        print("🎉 All tests passed! PIVOT issue has been fixed.")
        sys.exit(0)
    elif tests_passed == total_tests - 1 and not problematic_test_passed:
        print("⚠️  Expected state: Problematic PIVOT test failed (needs fix implementation)")
        print("   Implement the fallback logic in DuckDBStatement::Create() to make this pass")
        sys.exit(1)  # Exit with error code to indicate test failure
    else:
        print("💥 Unexpected test failures! Check server setup and configuration.")
        sys.exit(1)


if __name__ == "__main__":
    main()