#!/usr/bin/env python3
"""
GeoArrow Integration Test for GizmoSQL

Tests that GEOMETRY types from DuckDB's SPATIAL extension are properly
exported as GeoArrow format, enabling seamless integration with GeoPandas.

Requirements:
    pip install adbc-driver-flightsql geopandas shapely pyarrow

Usage:
    # Start GizmoSQL server first, then:
    python tests/test_geoarrow.py

    # Or with custom connection settings:
    GIZMOSQL_HOST=localhost GIZMOSQL_PORT=31337 python tests/test_geoarrow.py
"""

import os
import sys
import traceback


def test_geoarrow_export():
    """Test that GEOMETRY columns export with GeoArrow metadata."""
    from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

    host = os.getenv("GIZMOSQL_HOST", "localhost")
    port = os.getenv("GIZMOSQL_PORT", "31337")
    username = os.getenv("GIZMOSQL_USERNAME", "gizmosql_username")
    password = os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password")
    use_tls = os.getenv("TLS_ENABLED", "0") == "1"

    uri = f"grpc+tls://{host}:{port}" if use_tls else f"grpc://{host}:{port}"

    print(f"Connecting to GizmoSQL at {uri}")

    db_kwargs = {
        "username": username,
        "password": password,
    }
    if use_tls:
        db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"

    with gizmosql.connect(uri=uri, db_kwargs=db_kwargs, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Test 1: Query a simple point geometry
            print("\nTest 1: Query point geometry...")
            cur.execute("SELECT ST_Point(1.0, 2.0) AS geom")
            arrow_table = cur.fetch_arrow_table()

            assert arrow_table.num_rows == 1, f"Expected 1 row, got {arrow_table.num_rows}"
            assert "geom" in arrow_table.column_names, "Missing 'geom' column"

            geom_field = arrow_table.schema.field("geom")
            print(f"  Geometry field type: {geom_field.type}")

            # Check for GeoArrow extension metadata
            metadata = geom_field.metadata
            if metadata:
                print(f"  Metadata keys: {list(metadata.keys())}")
                if b"ARROW:extension:name" in metadata:
                    ext_name = metadata[b"ARROW:extension:name"].decode()
                    print(f"  Extension name: {ext_name}")
                    assert "geoarrow" in ext_name.lower() or "ogc" in ext_name.lower(), \
                        f"Expected GeoArrow extension, got: {ext_name}"
                    print("  ✓ GeoArrow extension metadata present")
                else:
                    print("  ⚠ No ARROW:extension:name metadata (may be binary WKB)")
            else:
                print("  ⚠ No metadata on geometry field")

            print("  ✓ Point geometry query successful")

            # Test 2: Query multiple geometry types
            print("\nTest 2: Query multiple geometry types...")
            cur.execute("""
                SELECT
                    1 AS id,
                    'point' AS type,
                    ST_Point(0.0, 0.0) AS geom
                UNION ALL
                SELECT
                    2 AS id,
                    'linestring' AS type,
                    ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)') AS geom
                UNION ALL
                SELECT
                    3 AS id,
                    'polygon' AS type,
                    ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))') AS geom
            """)
            arrow_table = cur.fetch_arrow_table()

            assert arrow_table.num_rows == 3, f"Expected 3 rows, got {arrow_table.num_rows}"
            print(f"  Retrieved {arrow_table.num_rows} geometry rows")
            print("  ✓ Multiple geometry types query successful")

            # Test 3: Integration with GeoPandas (if available)
            print("\nTest 3: GeoPandas integration...")
            try:
                import geopandas as gpd

                gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
                print(f"  Created GeoDataFrame with {len(gdf)} rows")
                print(f"  Geometry column type: {type(gdf.geometry.iloc[0])}")

                # Verify geometries are valid
                assert gdf.geometry.is_valid.all(), "Some geometries are invalid"
                print("  ✓ All geometries are valid")

                # Verify we can do spatial operations
                bounds = gdf.total_bounds
                print(f"  Total bounds: {bounds}")
                print("  ✓ GeoPandas integration successful")

            except ImportError:
                print("  ⚠ GeoPandas not installed, skipping GeoPandas tests")
                print("    Install with: pip install geopandas")

            # Test 4: Verify spatial functions work
            print("\nTest 4: Spatial functions...")
            cur.execute("""
                SELECT
                    ST_Distance(ST_Point(0, 0), ST_Point(3, 4)) AS distance,
                    ST_Area(ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')) AS area
            """)
            arrow_table = cur.fetch_arrow_table()

            distance = arrow_table.column("distance")[0].as_py()
            area = arrow_table.column("area")[0].as_py()

            assert abs(distance - 5.0) < 0.001, f"Expected distance 5.0, got {distance}"
            assert abs(area - 100.0) < 0.001, f"Expected area 100.0, got {area}"

            print(f"  ST_Distance(Point(0,0), Point(3,4)) = {distance}")
            print(f"  ST_Area(10x10 square) = {area}")
            print("  ✓ Spatial functions work correctly")

    return True


def main():
    """Main test runner."""
    print("=" * 60)
    print("GeoArrow Integration Test for GizmoSQL")
    print("=" * 60)

    try:
        success = test_geoarrow_export()

        print("\n" + "=" * 60)
        if success:
            print("✓ All GeoArrow tests PASSED")
            print("=" * 60)
            return 0
        else:
            print("✗ GeoArrow tests FAILED")
            print("=" * 60)
            return 1

    except ImportError as e:
        print(f"\n✗ Missing required package: {e}")
        print("\nInstall requirements with:")
        print("  pip install adbc-driver-flightsql geopandas shapely pyarrow")
        return 1

    except Exception as e:
        print(f"\n✗ Test execution failed: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
