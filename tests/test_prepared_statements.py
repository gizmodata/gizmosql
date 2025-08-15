#!/usr/bin/env python3
"""
Test prepared statement functionality in GizmoSQL.
"""

from adbc_driver_flightsql import dbapi as flightsql
import sys

username = "gizmosql_username"
password = "test123"

def test_prepared_statements():
    """Test prepared statements with parameters."""
    print("\nTesting prepared statements")
    print("-" * 40)
    
    conn = flightsql.connect(
        uri="grpc://localhost:31337",
        db_kwargs={
            "username": username,
            "password": password,
        },
    )
    
    with conn.cursor() as cur:
        # Test prepared statement with single parameter
        print("Testing single parameter query...")
        cur.execute("SELECT * FROM nation WHERE n_nationkey = ?", [1])
        result = cur.fetchall()
        print(f"Nation with key 1: {result}")
        assert len(result) == 1, "Should find exactly one nation"
        
        # Test multiple executions with different parameters
        print("\nTesting multiple executions...")
        for key in [0, 1, 2]:
            cur.execute("SELECT n_name FROM nation WHERE n_nationkey = ?", [key])
            name = cur.fetchone()
            if name:
                print(f"Nation {key}: {name[0]}")
            else:
                print(f"Nation {key}: Not found")
        
        # Test with multiple parameters
        print("\nTesting multiple parameters...")
        cur.execute(
            "SELECT * FROM nation WHERE n_nationkey > ? AND n_nationkey < ?", 
            [5, 10]
        )
        results = cur.fetchall()
        print(f"Nations between 5 and 10: Found {len(results)} nations")
        
        # Test prepared statement reuse
        print("\nTesting statement reuse...")
        query = "SELECT COUNT(*) FROM nation WHERE n_regionkey = ?"
        for region in [0, 1, 2, 3, 4]:
            cur.execute(query, [region])
            count = cur.fetchone()[0]
            print(f"Region {region}: {count} nations")
    
    conn.close()
    print("\n✓ All prepared statement tests passed")

def main():
    """Run all prepared statement tests."""
    try:
        test_prepared_statements()
        print("\n✅ Prepared statement tests completed successfully!")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())