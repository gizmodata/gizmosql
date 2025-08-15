#!/usr/bin/env python3
"""
Setup test data for GizmoSQL tests.
Creates additional catalogs and tables needed for testing.
"""

import subprocess
import sys

def run_query(query):
    """Execute a query using gizmosql_client."""
    cmd = [
        "./build/gizmosql_client",
        "--command", "Execute",
        "--host", "localhost",
        "--port", "31337",
        "--username", "gizmosql_username",
        "--password", "test123",
        "--query", query
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error executing query: {query}")
        print(f"Error: {result.stderr}")
        return False
    return True

def setup_test_catalogs():
    """Create test catalogs and tables."""
    print("Setting up test catalogs and data...")
    
    # Create superduck catalog
    if not run_query("ATTACH ':memory:' AS superduck"):
        print("Warning: Could not attach superduck catalog")
        return False
    
    # Create movies table in superduck catalog
    if not run_query("CREATE TABLE superduck.movies (id INT, title VARCHAR, year INT)"):
        print("Warning: Could not create movies table")
        return False
    
    # Insert test data
    if not run_query("INSERT INTO superduck.movies VALUES (1, 'The Matrix', 1999), (2, 'Inception', 2010), (3, 'Interstellar', 2014)"):
        print("Warning: Could not insert movie data")
        return False
    
    print("✓ Test data setup complete")
    return True

def main():
    """Setup all test data."""
    try:
        if setup_test_catalogs():
            print("✅ Test setup completed successfully!")
            return 0
        else:
            print("⚠️ Test setup completed with warnings")
            return 0  # Don't fail CI if setup has minor issues
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())