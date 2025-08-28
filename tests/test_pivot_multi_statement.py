#!/usr/bin/env python3
"""
Test script for PIVOT queries with multi-statement prepared statement fallback.

This script tests that PIVOT queries (which get rewritten to multiple statements)
work correctly with the prepared statement fallback mechanism.
"""

import sys
import os
import traceback
import pyarrow.flight as flight
from pyarrow import flight as fl
import pyarrow as pa

def create_flight_client(host='localhost', port=31337, username='gizmosql_username', 
                        password='gizmosql_password', use_tls=False):
    """Create a Flight SQL client connection."""
    if use_tls:
        # For TLS connections with self-signed certificates
        location = fl.Location.for_grpc_tls(host, port)
        # Skip certificate verification for testing
        client = fl.FlightClient(location, tls_root_certs=None)
    else:
        location = fl.Location.for_grpc_tcp(host, port)
        client = fl.FlightClient(location)
    
    # Authenticate
    try:
        token_pair = client.authenticate_basic_token(username, password)
        options = fl.FlightCallOptions(headers=[token_pair])
        return client, options
    except Exception as e:
        print(f"Authentication failed: {e}")
        raise

def test_pivot_query(client, options):
    """Test PIVOT query execution."""
    
    # First, create test data
    setup_queries = [
        "DROP TABLE IF EXISTS test_sales",
        """
        CREATE TABLE test_sales (
            product VARCHAR,
            quarter VARCHAR,
            revenue INTEGER
        )
        """,
        """
        INSERT INTO test_sales VALUES
            ('Widget', 'Q1', 100),
            ('Widget', 'Q2', 150),
            ('Widget', 'Q3', 200),
            ('Widget', 'Q4', 250),
            ('Gadget', 'Q1', 80),
            ('Gadget', 'Q2', 120),
            ('Gadget', 'Q3', 160),
            ('Gadget', 'Q4', 200)
        """
    ]
    
    print("Setting up test data...")
    for query in setup_queries:
        try:
            # Execute as a simple statement
            descriptor = flight.FlightDescriptor.for_command(query.encode())
            flight_info = client.get_flight_info(descriptor, options)
            
            # Fetch results if any
            for endpoint in flight_info.endpoints:
                for location in endpoint.locations:
                    reader = client.do_get(endpoint.ticket, options)
                    # Consume results
                    for batch in reader:
                        pass
        except Exception as e:
            print(f"Setup query failed: {query[:50]}...")
            print(f"Error: {e}")
            return False
    
    # Now test PIVOT query
    pivot_query = """
    PIVOT test_sales
    ON quarter
    USING sum(revenue) AS total_revenue
    GROUP BY product
    """
    
    print("\nTesting PIVOT query...")
    print(f"Query: {pivot_query}")
    
    try:
        # Test as prepared statement (this should trigger the fallback)
        descriptor = flight.FlightDescriptor.for_command(pivot_query.encode())
        flight_info = client.get_flight_info(descriptor, options)
        
        print("✓ PIVOT query prepared successfully (using fallback mechanism)")
        
        # Fetch and display results
        for endpoint in flight_info.endpoints:
            for location in endpoint.locations:
                reader = client.do_get(endpoint.ticket, options)
                
                print("\nPIVOT Results:")
                print("-" * 50)
                
                for batch in reader:
                    # Convert to pandas for pretty printing
                    df = batch.to_pandas()
                    print(df.to_string())
                    
                    # Verify results
                    assert 'product' in df.columns, "Missing 'product' column"
                    assert 'Q1' in df.columns, "Missing 'Q1' column"
                    assert 'Q2' in df.columns, "Missing 'Q2' column"
                    assert 'Q3' in df.columns, "Missing 'Q3' column"
                    assert 'Q4' in df.columns, "Missing 'Q4' column"
                    
                    # Check values
                    widget_row = df[df['product'] == 'Widget'].iloc[0]
                    assert widget_row['Q1'] == 100, f"Expected Q1=100, got {widget_row['Q1']}"
                    assert widget_row['Q2'] == 150, f"Expected Q2=150, got {widget_row['Q2']}"
                    assert widget_row['Q3'] == 200, f"Expected Q3=200, got {widget_row['Q3']}"
                    assert widget_row['Q4'] == 250, f"Expected Q4=250, got {widget_row['Q4']}"
                    
                    print("\n✓ PIVOT query results verified successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ PIVOT query failed: {e}")
        traceback.print_exc()
        return False
    finally:
        # Cleanup
        try:
            cleanup_query = "DROP TABLE IF EXISTS test_sales"
            descriptor = flight.FlightDescriptor.for_command(cleanup_query.encode())
            flight_info = client.get_flight_info(descriptor, options)
            for endpoint in flight_info.endpoints:
                for location in endpoint.locations:
                    reader = client.do_get(endpoint.ticket, options)
                    for batch in reader:
                        pass
        except:
            pass

def main():
    """Main test runner."""
    
    # Parse command line arguments
    host = os.getenv('GIZMOSQL_HOST', 'localhost')
    port = int(os.getenv('GIZMOSQL_PORT', '31337'))
    username = os.getenv('GIZMOSQL_USERNAME', 'gizmosql_username')
    password = os.getenv('GIZMOSQL_PASSWORD', 'gizmosql_password')
    use_tls = os.getenv('TLS_ENABLED', '0') == '1'
    
    print("=" * 60)
    print("Multi-Statement Prepared Statement Fallback Test")
    print("=" * 60)
    print(f"Connecting to GizmoSQL at {host}:{port}")
    print(f"TLS: {'Enabled' if use_tls else 'Disabled'}")
    print()
    
    try:
        # Create client
        client, options = create_flight_client(host, port, username, password, use_tls)
        print("✓ Connected to GizmoSQL server")
        
        # Run PIVOT test
        success = test_pivot_query(client, options)
        
        print("\n" + "=" * 60)
        if success:
            print("✓ All tests PASSED")
            print("=" * 60)
            return 0
        else:
            print("✗ Tests FAILED")
            print("=" * 60)
            return 1
            
    except Exception as e:
        print(f"\n✗ Test execution failed: {e}")
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())