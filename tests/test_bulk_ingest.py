#!/usr/bin/env python3
"""
Bulk Ingestion Test for GizmoSQL

Tests ADBC bulk ingestion functionality using adbc_ingest() method.
Generates TPC-H data in DuckDB and bulk loads it into GizmoSQL.

Requirements:
    pip install adbc-driver-flightsql duckdb pyarrow

Usage:
    # Start GizmoSQL server first, then:
    python tests/test_bulk_ingest.py

    # Or with custom connection settings:
    GIZMOSQL_HOST=localhost GIZMOSQL_PORT=31337 python tests/test_bulk_ingest.py
"""

import os
import sys
import time
import traceback


def test_bulk_ingest():
    """Test bulk ingestion of TPC-H lineitem data into GizmoSQL."""
    import duckdb
    from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

    host = os.getenv("GIZMOSQL_HOST", "localhost")
    port = os.getenv("GIZMOSQL_PORT", "31337")
    username = os.getenv("GIZMOSQL_USERNAME", "gizmosql_username")
    password = os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password")
    use_tls = os.getenv("TLS_ENABLED", "0") == "1"

    uri = f"grpc+tls://{host}:{port}" if use_tls else f"grpc://{host}:{port}"

    # Use a smaller scale factor for CI tests (SF 0.01 = ~60K rows in lineitem)
    scale_factor = float(os.getenv("TPCH_SCALE_FACTOR", "0.01"))

    print(f"Connecting to GizmoSQL at {uri}")
    print(f"TPC-H Scale Factor: {scale_factor}")

    # Step 1: Generate TPC-H data in DuckDB
    print("\nStep 1: Generating TPC-H data in DuckDB...")
    gen_start = time.perf_counter()

    duckdb_conn = duckdb.connect()
    duckdb_conn.install_extension("tpch")
    duckdb_conn.load_extension("tpch")
    duckdb_conn.execute(f"CALL dbgen(sf={scale_factor})")

    gen_seconds = time.perf_counter() - gen_start
    print(f"  TPC-H data generated in {gen_seconds:.2f}s")

    # Get row count
    row_count = duckdb_conn.execute("SELECT COUNT(*) FROM lineitem").fetchone()[0]
    print(f"  Lineitem table has {row_count:,} rows")

    # Step 2: Get Arrow reader for the lineitem table
    print("\nStep 2: Creating Arrow RecordBatch reader...")
    reader_start = time.perf_counter()

    lineitem_arrow_reader = duckdb_conn.table("lineitem").fetch_arrow_reader(batch_size=10_000)

    reader_seconds = time.perf_counter() - reader_start
    print(f"  Arrow reader created in {reader_seconds:.4f}s")

    # Step 3: Bulk ingest into GizmoSQL
    print("\nStep 3: Bulk ingesting into GizmoSQL...")

    db_kwargs = {
        "username": username,
        "password": password,
    }
    if use_tls:
        db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"

    with gizmosql.connect(uri=uri, db_kwargs=db_kwargs, autocommit=True) as conn:
        with conn.cursor() as cursor:
            ingest_start = time.perf_counter()

            rows_loaded = cursor.adbc_ingest(
                table_name="bulk_ingest_lineitem",
                data=lineitem_arrow_reader,
                mode="replace"
            )

            ingest_seconds = time.perf_counter() - ingest_start

            rows_per_sec = rows_loaded / ingest_seconds if ingest_seconds > 0 else float("inf")
            print(f"  Loaded rows: {rows_loaded:,}")
            print(f"  Ingest time: {ingest_seconds:.4f}s")
            print(f"  Throughput: {rows_per_sec:,.0f} rows/sec")

            # Step 4: Verify the row count
            print("\nStep 4: Verifying row count...")
            cursor.execute("SELECT COUNT(*) FROM bulk_ingest_lineitem")
            result = cursor.fetchone()[0]
            print(f"  Row count verification: {result:,}")

            assert result == row_count, f"Expected {row_count:,} rows, got {result:,}"
            print("  Row count matches!")

            # Step 5: Verify data integrity with a sample query
            print("\nStep 5: Verifying data integrity...")
            cursor.execute("""
                SELECT
                    COUNT(*) as cnt,
                    SUM(l_quantity) as total_qty,
                    AVG(l_extendedprice) as avg_price
                FROM bulk_ingest_lineitem
            """)
            stats = cursor.fetchone()
            print(f"  Count: {stats[0]:,}")
            print(f"  Total Quantity: {stats[1]:,.2f}")
            print(f"  Avg Extended Price: {stats[2]:,.2f}")

            # Step 6: Clean up
            print("\nStep 6: Cleaning up...")
            cursor.execute("DROP TABLE IF EXISTS bulk_ingest_lineitem")
            print("  Table dropped")

    duckdb_conn.close()
    return True


def main():
    """Main test runner."""
    print("=" * 60)
    print("Bulk Ingestion Test for GizmoSQL")
    print("=" * 60)

    try:
        success = test_bulk_ingest()

        print("\n" + "=" * 60)
        if success:
            print("All bulk ingestion tests PASSED")
            print("=" * 60)
            return 0
        else:
            print("Bulk ingestion tests FAILED")
            print("=" * 60)
            return 1

    except ImportError as e:
        print(f"\nMissing required package: {e}")
        print("\nInstall requirements with:")
        print("  pip install adbc-driver-flightsql duckdb pyarrow")
        return 1

    except Exception as e:
        print(f"\nTest execution failed: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
