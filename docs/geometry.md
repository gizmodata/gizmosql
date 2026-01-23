# Geometry & Spatial Support

GizmoSQL includes built-in support for geospatial data through DuckDB's SPATIAL extension. The spatial extension is automatically loaded at server startup, enabling spatial functions and seamless GeoArrow export.

## Overview

GizmoSQL provides:

- **GEOMETRY type support** - Store and query spatial data
- **100+ spatial functions** - ST_Point, ST_Distance, ST_Area, ST_Contains, etc.
- **GeoArrow export** - GEOMETRY columns export with proper GeoArrow metadata
- **GeoPandas integration** - Read geometry data directly without WKB conversion

## Quick Example

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
import geopandas as gpd

with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cur:
        # Create a table with geometry
        cur.execute("""
            CREATE TABLE locations (
                id INTEGER,
                name VARCHAR,
                geom GEOMETRY
            )
        """)

        # Insert some points
        cur.execute("""
            INSERT INTO locations VALUES
                (1, 'New York', ST_Point(-74.006, 40.7128)),
                (2, 'Los Angeles', ST_Point(-118.2437, 34.0522)),
                (3, 'Chicago', ST_Point(-87.6298, 41.8781))
        """)

        # Query with spatial functions
        cur.execute("""
            SELECT name, ST_X(geom) as lon, ST_Y(geom) as lat
            FROM locations
        """)
        print(cur.fetch_arrow_table().to_pandas())

        # Get as GeoDataFrame - geometry is read directly!
        cur.execute("SELECT * FROM locations")
        arrow_table = cur.fetch_arrow_table()
        gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
        print(gdf)
```

## Supported Geometry Types

| Type | Description | Example |
|------|-------------|---------|
| POINT | Single point | `ST_Point(1.0, 2.0)` |
| LINESTRING | Connected line segments | `ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)')` |
| POLYGON | Closed shape | `ST_GeomFromText('POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))')` |
| MULTIPOINT | Collection of points | `ST_GeomFromText('MULTIPOINT(0 0, 1 1)')` |
| MULTILINESTRING | Collection of linestrings | `ST_GeomFromText('MULTILINESTRING((0 0, 1 1), (2 2, 3 3))')` |
| MULTIPOLYGON | Collection of polygons | `ST_GeomFromText('MULTIPOLYGON(((0 0, 1 0, 1 1, 0 0)))')` |

## Common Spatial Functions

### Geometry Creation

```sql
-- Create a point
SELECT ST_Point(-122.4194, 37.7749) AS san_francisco;

-- Create from WKT (Well-Known Text)
SELECT ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))') AS square;

-- Create a line between two points
SELECT ST_MakeLine(ST_Point(0, 0), ST_Point(10, 10)) AS diagonal;
```

### Measurements

```sql
-- Distance between two points
SELECT ST_Distance(
    ST_Point(-74.006, 40.7128),   -- New York
    ST_Point(-118.2437, 34.0522)  -- Los Angeles
) AS distance;

-- Area of a polygon
SELECT ST_Area(
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
) AS area;

-- Length of a linestring
SELECT ST_Length(
    ST_GeomFromText('LINESTRING(0 0, 3 4)')
) AS length;
```

### Spatial Relationships

```sql
-- Check if point is within polygon
SELECT ST_Contains(
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'),
    ST_Point(5, 5)
) AS is_inside;

-- Check if geometries intersect
SELECT ST_Intersects(
    ST_GeomFromText('LINESTRING(0 0, 10 10)'),
    ST_GeomFromText('LINESTRING(0 10, 10 0)')
) AS intersects;
```

### Geometry Operations

```sql
-- Get bounding box
SELECT ST_Envelope(geom) FROM locations;

-- Get centroid
SELECT ST_Centroid(geom) FROM polygons;

-- Buffer around geometry
SELECT ST_Buffer(ST_Point(0, 0), 10) AS circle;

-- Union of geometries
SELECT ST_Union(geom1, geom2) FROM shapes;
```

## GeoArrow Export

GizmoSQL automatically exports GEOMETRY columns with GeoArrow extension metadata. This enables zero-copy integration with GeoArrow-aware tools like GeoPandas.

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
import geopandas as gpd

with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM my_spatial_table")
        arrow_table = cur.fetch_arrow_table()

        # Check GeoArrow metadata
        geom_field = arrow_table.schema.field("geom")
        print(f"Extension: {geom_field.metadata[b'ARROW:extension:name'].decode()}")
        # Output: Extension: geoarrow.wkb

        # GeoPandas reads it directly - no conversion needed!
        gdf = gpd.GeoDataFrame.from_arrow(arrow_table)
        print(gdf.geometry.geom_type.value_counts())
```

## Loading Spatial Data

### From GeoJSON

```python
import geopandas as gpd
import pyarrow as pa
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

# Read GeoJSON file
gdf = gpd.read_file("data.geojson")

# Convert to Arrow
arrow_table = gdf.to_arrow()

with gizmosql.connect(
    uri="grpc://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password"
    },
    autocommit=True
) as conn:
    with conn.cursor() as cursor:
        cursor.adbc_ingest(
            table_name="geojson_data",
            data=arrow_table,
            mode="replace"
        )
```

### From Shapefile

```python
import geopandas as gpd

gdf = gpd.read_file("data.shp")
arrow_table = gdf.to_arrow()

# Bulk ingest into GizmoSQL
# ... same as above
```

### From GeoParquet

```python
import geopandas as gpd

gdf = gpd.read_parquet("data.parquet")
arrow_table = gdf.to_arrow()

# Bulk ingest into GizmoSQL
# ... same as above
```

## Spatial Joins

```sql
-- Find all points within polygons
SELECT
    points.name AS point_name,
    regions.name AS region_name
FROM points
JOIN regions ON ST_Contains(regions.geom, points.geom);

-- Find nearest neighbors
SELECT
    a.name,
    b.name AS nearest,
    ST_Distance(a.geom, b.geom) AS distance
FROM locations a
CROSS JOIN locations b
WHERE a.id != b.id
ORDER BY a.id, distance
LIMIT 10;
```

## TLS Connections

For TLS-enabled servers:

```python
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions

with gizmosql.connect(
    uri="grpc+tls://localhost:31337",
    db_kwargs={
        "username": "gizmosql_username",
        "password": "gizmosql_password",
        DatabaseOptions.TLS_SKIP_VERIFY.value: "true"  # Only for self-signed certs
    },
    autocommit=True
) as conn:
    # ... spatial operations
    pass
```

## See Also

- [DuckDB Spatial Extension](https://duckdb.org/docs/extensions/spatial/overview)
- [GeoArrow Specification](https://geoarrow.org/)
- [GeoPandas Documentation](https://geopandas.org/)
- [Bulk Ingestion Guide](bulk_ingestion.md)
