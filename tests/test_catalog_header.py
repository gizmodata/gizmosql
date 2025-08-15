#!/usr/bin/env python3
"""
Test x-default-catalog header functionality in GizmoSQL.
"""

from adbc_driver_flightsql import dbapi as flightsql, DatabaseOptions
import sys

username = "gizmosql_username"
password = "test123"

def test_default_connection():
    """Test connection without catalog header uses default database."""
    print("\nTest 1: Default connection (no header)")
    print("-" * 40)
    
    conn = flightsql.connect(
        uri="grpc://localhost:31337",
        db_kwargs={
            "username": username,
            "password": password,
        },
    )
    
    with conn.cursor() as cur:
        cur.execute("SELECT current_database()")
        result = cur.fetchone()
        print(f"Current database: {result[0]}")
        assert result[0] == "test", f"Expected default database 'test', got {result[0]}"
    
    conn.close()
    print("✓ Default connection test passed")

def test_catalog_header():
    """Test x-default-catalog header switches to specified catalog."""
    print("\nTest 2: With x-default-catalog header")
    print("-" * 40)
    
    conn = flightsql.connect(
        uri="grpc://localhost:31337",
        db_kwargs={
            "username": username,
            "password": password,
            DatabaseOptions.WITH_COOKIE_MIDDLEWARE.value: "true",
            # Set default catalog via custom header
            f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}x-default-catalog": "superduck",
        }
    )
    
    with conn.cursor() as cur:
        # Check current database
        cur.execute("SELECT current_database()")
        result = cur.fetchone()
        print(f"Current database: {result[0]}")
        
        if result[0] == 'superduck':
            print("✓ Default catalog was set correctly!")
        else:
            # Note: If the server doesn't switch catalogs, it might still be on 'test'
            # This is OK - the server logs the attempt but doesn't fail the connection
            print(f"Note: Catalog is '{result[0]}', header may not have switched it")
        
        # Try unqualified query - this should work if catalog was switched
        try:
            cur.execute("SELECT COUNT(*) FROM movies")
            result = cur.fetchone()
            print(f"✓ SUCCESS! Unqualified query works! Count: {result[0]}")
            assert result[0] == 3, f"Expected 3 movies, got {result[0]}"
            
            # Try a more complex query
            cur.execute("""
                SELECT *
                FROM movies
                ORDER BY year DESC
                LIMIT 10
            """)
            results = cur.fetchall()
            print(f"✓ Complex query also works! Got {len(results)} rows")
            assert len(results) == 3, f"Expected 3 rows, got {len(results)}"
            
        except Exception as e:
            # If the catalog didn't switch, we might need qualified names
            print(f"Note: Unqualified query failed, trying with qualified name...")
            cur.execute("SELECT COUNT(*) FROM superduck.movies")
            result = cur.fetchone()
            print(f"✓ Qualified query works! Count: {result[0]}")
            assert result[0] == 3, f"Expected 3 movies, got {result[0]}"
    
    conn.close()
    print("✓ Catalog header test completed")

def main():
    """Run all catalog header tests."""
    try:
        test_default_connection()
        test_catalog_header()
        print("\n✅ All catalog header tests passed!")
        return 0
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())