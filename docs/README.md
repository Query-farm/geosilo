# GeoSilo by [Query.Farm](https://query.farm)

A DuckDB extension for compact geometry encoding. Delta-encodes coordinates as scaled integers instead of float64 pairs, achieving **3–4x smaller** geometry on disk and over the wire compared to standard WKB. Points compress to just **9 bytes** (vs 21 bytes WKB).

GeoSilo provides a first-class `GEOSILO` column type with transparent interop — standard `ST_*` spatial functions work directly on GEOSILO columns, with high-performance native implementations for common operations that skip the WKB decode step entirely.

```sql
INSTALL spatial; LOAD spatial;
INSTALL geosilo FROM community; LOAD geosilo;

-- Create a table with GEOSILO type
CREATE TABLE parcels (
    id INTEGER,
    geom GEOSILO('EPSG:4326') USING COMPRESSION zstd
);

-- Insert from any GEOMETRY source (auto-encodes)
INSERT INTO parcels SELECT id, geom FROM raw_data;

-- ST_* functions work directly — no decode needed for many operations
SELECT id, ST_Area(geom), ST_X(ST_Centroid(geom)) FROM parcels;
```

## Why

Geometry data in DuckDB is stored and transferred as [WKB](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry#Well-known_binary) — raw IEEE 754 float64 coordinate pairs. This is simple and lossless, but wastes space because adjacent vertices in a polygon share nearly identical coordinate values that differ only in the last few decimal places.

GeoSilo exploits this spatial adjacency:

1. Scale coordinates to integers (e.g., -75.509491 × 1e7 = -755094910)
2. Store the first coordinate per ring as absolute int32
3. Store subsequent coordinates as **int16 deltas** from the previous value
4. ~90% of deltas fit in 2 bytes; the remaining ~10% use a 6-byte escape
5. **Points** use a 1-byte compact header (vs 12-byte full header), totaling just 9 bytes
6. **Polygon rings** omit the closing vertex (reconstructed on decode)
7. **MULTIPOINT** delta-encodes between sub-points

The result is ~70% smaller raw blobs that compress 3–4x better with ZSTD because small integer deltas have highly repetitive byte patterns.

## GEOSILO type

The `GEOSILO` type is a first-class DuckDB column type with optional CRS parameterization:

```sql
-- With CRS (scale auto-detected)
CREATE TABLE t (geom GEOSILO('EPSG:4326') USING COMPRESSION zstd);

-- Without CRS (default scale 1e7)
CREATE TABLE t (geom GEOSILO USING COMPRESSION zstd);
```

**Transparent casts** — GEOSILO implicitly casts to/from GEOMETRY, so all existing spatial functions work:

```sql
-- Insert from GEOMETRY (auto-encodes)
INSERT INTO t SELECT ST_GeomFromText('POINT(1 2)');

-- ST_* functions work transparently
SELECT ST_Buffer(geom, 0.01) FROM t;  -- auto-decodes to GEOMETRY
```

**Direct ST_\* overloads** — common functions have native GEOSILO implementations that skip the WKB decode, running 2–3x faster:

| Function | Implementation | Avoids decode? |
|---|---|---|
| `ST_GeometryType(GEOSILO)` | Read 1 byte from header | Yes |
| `ST_IsEmpty(GEOSILO)` | Check sentinel/count fields | Yes |
| `ST_NPoints(GEOSILO)` | Walk structure count fields | Yes |
| `ST_X(GEOSILO)`, `ST_Y(GEOSILO)` | Read int32, divide by scale | Yes |
| `ST_XMin/XMax/YMin/YMax(GEOSILO)` | Delta walk with min/max tracking | Yes |
| `ST_Area(GEOSILO)` | Shoelace formula on int64 accumulators | Yes |
| `ST_Length(GEOSILO)` | Euclidean distance during delta walk | Yes |
| `ST_Perimeter(GEOSILO)` | Same as length for polygon rings | Yes |

Functions without a native overload (ST_Buffer, ST_Union, ST_AsText, etc.) automatically fall back to the implicit cast — users write the same SQL regardless.

### Storage recommendation

For best on-disk compression, use `USING COMPRESSION zstd` on GEOSILO columns:

```sql
CREATE TABLE t (
    id INTEGER,
    geom GEOSILO('EPSG:4326') USING COMPRESSION zstd
);
```

On-disk size comparison (TIGER/Line 2025, geom + centroid columns, 4 tables, default row groups):

| Format | Size | vs GEOMETRY |
|---|---|---|
| GEOMETRY (shredded + ALP) | 183 MB | 1.00x |
| GEOSILO (no ZSTD) | 230 MB | 1.26x |
| **GEOSILO + ZSTD** | **144 MB** | **0.79x** |

Without ZSTD, DuckDB stores GEOSILO blobs uncompressed — larger than native GEOMETRY with shredding. With ZSTD, GEOSILO columns are ~21% smaller because delta-encoded small integers compress extremely well.

## Functions

| Function | Input | Output | Description |
|---|---|---|---|
| `geosilo_encode(geom)` | GEOMETRY | BLOB | Encode with auto-detected or default scale |
| `geosilo_encode(geom, scale)` | GEOMETRY, BIGINT | BLOB | Encode with explicit scale |
| `geosilo_decode(blob)` | BLOB/GEOSILO | GEOMETRY | Decode back to GEOMETRY |
| `geosilo_metadata(blob)` | BLOB/GEOSILO | STRUCT | Read header without decoding |

All geometry types are supported: Point, LineString, Polygon, Multi\*, GeometryCollection, and empty geometries.

## Scale and precision

The scale factor controls coordinate precision. It is stored in each blob header, so decoding never needs configuration.

| CRS | Units | Scale | Precision |
|---|---|---|---|
| WGS84 / NAD83 (default) | degrees | 10,000,000 | ~1cm |
| UTM / Web Mercator | meters | 100 | 1cm |

When the GEOMETRY column carries a CRS (e.g., `GEOMETRY(EPSG:4326)`), the scale is auto-detected. An explicit scale parameter always overrides:

```sql
SELECT geosilo_encode(geom, 100) FROM my_utm_table;
```

## Benchmarks

### Compression

Geometry column compression vs standard WKB on US Census TIGER/Line 2025 (WGS84):

| Table | Rows | WKB Size | GeoSilo | GeoSilo + ZSTD |
|---|---|---|---|---|
| block_group | 242,748 | 209 MB | 0.31x | 0.27x |
| zcta5 | 33,791 | 180 MB | 0.29x | 0.26x |
| tract | 85,529 | 125 MB | 0.30x | 0.27x |
| county | 3,235 | 25 MB | 0.30x | 0.27x |
| urban_area | 2,644 | 23 MB | 0.29x | 0.26x |

Reproduce with: `./build/release/duckdb -f scripts/benchmark.sql` (requires `tiger.duckdb` in the working directory).

### Performance

Direct ST_* functions on GEOSILO vs implicit-cast path (decode + spatial), in-memory, TIGER/Line 2025:

| Function | Data | Direct | Via Cast | Speedup |
|---|---|---|---|---|
| ST_X | 242K points | 1 ms | 3 ms | 3x |
| ST_Area | 3,235 counties | 3 ms | 9 ms | 3x |
| ST_XMin | 3,235 counties | 4 ms | 11 ms | 2.75x |

ST_Area results match the spatial extension within float64 epsilon (max diff 5.7e-13) across all 365K TIGER geometries.

Encode/decode throughput (single-threaded, Apple M-series):

| Table | Rows | Encode | Decode |
|---|---|---|---|
| block_group | 242,748 | 75 ms (2,789 MB/s) | 71 ms (2,946 MB/s) |
| zcta5 | 33,791 | 49 ms (3,673 MB/s) | 47 ms (3,830 MB/s) |
| tract | 85,529 | 41 ms (3,039 MB/s) | 39 ms (3,195 MB/s) |
| county | 3,235 | 7 ms (3,514 MB/s) | 7 ms (3,514 MB/s) |

## Usage

### Using the GEOSILO type (recommended)

```sql
INSTALL spatial; LOAD spatial;
INSTALL geosilo FROM community; LOAD geosilo;

-- Create table with GEOSILO column
CREATE TABLE parcels (
    id INTEGER,
    geom GEOSILO('EPSG:4326') USING COMPRESSION zstd
);

-- Insert from any GEOMETRY source — auto-encodes
INSERT INTO parcels
SELECT id, ST_GeomFromText(wkt) FROM raw_data;

-- Standard spatial functions work directly
SELECT id, ST_Area(geom) FROM parcels;             -- native (no decode)
SELECT id, ST_X(geom), ST_Y(geom) FROM parcels;    -- native (no decode)
SELECT id, ST_Buffer(geom, 0.01) FROM parcels;     -- auto-decodes to GEOMETRY

-- Spatial filters using bounding box (no decode)
SELECT * FROM parcels
WHERE ST_XMin(geom) > -78 AND ST_XMax(geom) < -76;
```

### Manual encode/decode (BLOB workflow)

For cases where you want explicit control over encoding:

```sql
-- Encode to BLOB
SELECT geosilo_encode(geom) FROM source_table;

-- Decode back to GEOMETRY
SELECT ST_Area(geosilo_decode(silo_blob)) FROM compact_table;

-- Roundtrip
SELECT ST_AsText(geosilo_decode(geosilo_encode(
    ST_GeomFromText('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))')
)));
-- POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))
```

### Arrow IPC transport

GeoSilo registers an [Arrow extension type](https://arrow.apache.org/docs/format/Columnar.html#extension-types) named `queryfarm.geosilo`. When an Arrow column carries this metadata, DuckDB (with geosilo loaded) automatically decodes silo blobs to GEOMETRY on read and encodes GEOMETRY to silo blobs on write — no explicit `geosilo_decode` needed.

CRS is preserved through the Arrow metadata. The extension metadata JSON carries `{"crs": "EPSG:4326"}` (or similar), which is attached to the GEOMETRY type on decode and used to auto-detect scale on encode.

**Producer (Python/PyArrow):**

```python
# Without CRS
geom_field = pa.field("geom", pa.binary(), metadata={
    b"ARROW:extension:name": b"queryfarm.geosilo",
    b"ARROW:extension:metadata": b"",
})

# With CRS
geom_field = pa.field("geom", pa.binary(), metadata={
    b"ARROW:extension:name": b"queryfarm.geosilo",
    b"ARROW:extension:metadata": b'{"crs":"EPSG:4326"}',
})
```

**Consumer:** DuckDB with geosilo loaded receives `GEOMETRY` (or `GEOMETRY(EPSG:4326)` if CRS is present) automatically.

## Binary format

Each blob uses either a **compact header** (1 byte) or a **full header** (12 bytes):

```
Compact header (1 byte, 0x50–0x67):
  A single magic byte encodes geometry type, vertex type, and scale.
  Each geometry type gets 4 consecutive values:
    base + 0 = XY,  scale 1e7
    base + 1 = XYZ, scale 1e7
    base + 2 = XY,  scale 100
    base + 3 = XYZ, scale 100
  POINT: 0x50–0x53, LINESTRING: 0x54–0x57, POLYGON: 0x58–0x5B,
  MULTIPOINT: 0x5C–0x5F, MULTILINESTRING: 0x60–0x63, MULTIPOLYGON: 0x64–0x67

Full header (12 bytes, magic 0x47):
  magic            1 byte   0x47 ('G')
  version          1 byte   (0x01–0x03)
  geometry_type    1 byte   (1=Point .. 7=GeometryCollection)
  vertex_type      1 byte   (0=XY, 1=XYZ, 2=XYM, 3=XYZM)
  scale            8 bytes  int64 LE

Coordinates:
  First per ring:  int32 absolute (4 bytes per dimension)
  Subsequent:      int16 delta (2 bytes), or sentinel -32768 + int32 (6 bytes)
  Empty (NaN):     INT32_MAX sentinel per dimension

Body (recursive):
  Point:           D absolute int32 values (9 bytes total with compact header)
  LineString:      uint32 num_points + delta-encoded coordinates
  Polygon:         uint32 num_rings, each: uint32 num_points + delta coords
                   (v3/compact: closing vertex omitted, reconstructed on decode)
  MultiPoint:      uint32 num_parts + delta-encoded points
                   (v2+/compact: first absolute, rest delta-encoded)
  Multi*:          uint32 num_parts, each part recursively
  Collection:      uint32 num_parts, each with type byte + recursive body

Version history:
  v1: Original encoding
  v2: MULTIPOINT delta encoding between sub-points
  v3: Polygon ring closure elimination
  Compact headers always use the latest features for their geometry type.
```


---

Copyright 2026 [Query.Farm LLC](https://query.farm)
