# GeoSilo

A DuckDB extension for compact geometry encoding. Reduces geometry storage and wire transfer size by delta-encoding coordinates as scaled int32 values instead of float64 pairs. Combined with DuckDB's ZSTD compression, achieves up to 3.8x reduction in geometry storage compared to default GEOMETRY.

## Functions

### `silo_encode(geom)` / `silo_encode(geom, scale)`

Encodes a GEOMETRY column into a compact binary blob using delta-encoded coordinates.

- **geom** (GEOMETRY) — input geometry (any type: Point, LineString, Polygon, Multi*, GeometryCollection)
- **scale** (BIGINT, optional) — coordinate scale factor. Default `10000000` (1e7), which gives ~1cm precision for WGS84 (EPSG:4326). Use `100` for projected CRS in meters.
- **Returns** BLOB

```sql
-- Default scale (WGS84)
SELECT silo_encode(geom) FROM my_table;

-- Custom scale for UTM coordinates (meters, 1cm precision)
SELECT silo_encode(geom, 100) FROM my_table;
```

### `silo_decode(blob)`

Decodes a GeoSilo blob back into a GEOMETRY value.

- **blob** (BLOB) — GeoSilo-encoded geometry
- **Returns** GEOMETRY

```sql
SELECT ST_AsText(silo_decode(encoded_geom)) FROM my_table;
```

### Roundtrip

```sql
SELECT ST_AsText(silo_decode(silo_encode(
    ST_GeomFromText('POLYGON((-75.5 39.7, -75.4 39.8, -75.3 39.7, -75.5 39.7))')
)));
-- POLYGON ((-75.5 39.7, -75.4 39.8, -75.3 39.7, -75.5 39.7))
```

## How it works

GeoSilo uses the same structure as WKB (geometry type, part counts, ring counts, coordinate sequences) but replaces float64 coordinate pairs with delta-encoded int32 values:

1. Coordinates are scaled to integers (e.g., -75.509491 * 1e7 = -755094910)
2. The first coordinate in each ring/linestring is stored as an absolute int32
3. Subsequent coordinates are stored as deltas from the previous value (typically small numbers)
4. The scale factor is stored in the blob header, so decoding needs no configuration

The delta values are small because adjacent vertices in a polygon are spatially close. General-purpose compressors like ZSTD achieve ~3.5x compression on the delta-encoded data vs ~1.1x on raw WKB, because the small integer deltas have highly repetitive byte patterns.

### Why delta encoding beats other approaches

| Encoding | Arrow IPC + ZSTD | Ratio |
|---|---|---|
| Raw float64 (WKB) | 15.2 MB | 1.00x |
| Byte-stream split (Parquet-style) | 11.2 MB | 0.65x |
| XOR delta (Gorilla) | 12.9 MB | 0.75x |
| **Delta int32 (GeoSilo)** | **5.5 MB** | **0.36x** |

Adjacent coordinates in a polygon share similar values, but IEEE 754 double encoding obscures this at the byte level. Delta encoding captures the spatial adjacency directly — the difference between -75.509491 and -75.509089 is just 402 in 1e-7 degree units, which ZSTD compresses trivially.

### Binary format

```
Header (14 bytes):
  magic            1 byte   0x47 ('G')
  version          1 byte   0x01
  geometry_type    1 byte   (1=Point, 2=LineString, 3=Polygon, 4-6=Multi*, 7=Collection)
  vertex_type      1 byte   (0=XY, 1=XYZ, 2=XYM, 3=XYZM)
  scale            8 bytes  int64 LE

Coordinate encoding:
  First per ring/line: int32 absolute (4 bytes per dimension)
  Subsequent deltas:   int16 (2 bytes) if value fits [-32767, 32767]
                       otherwise: int16 sentinel -32768, then int32 (6 bytes)
  Empty point (NaN):   INT32_MAX sentinel per dimension

Body (recursive):
  Point:           D int32 values (absolute, D = vertex dimension)
  LineString:      uint32 num_points, then delta-encoded coordinates
  Polygon:         uint32 num_rings, per ring: uint32 num_points + delta coords
  Multi*:          uint32 num_parts, each part encoded recursively
  Collection:      uint32 num_parts, each with type byte + recursive encoding
```

~90% of coordinate deltas fit in int16, using 2 bytes instead of the full 8-byte float64. The remaining ~10% use the 6-byte escape path. Combined with the int32 first coordinate (vs 8-byte double), this achieves ~70% raw size reduction before any external compression.

## Benchmarks

Tested on US Census TIGER/Line 2025 boundary data (WGS84, 2048-row row groups). All ratios relative to 1.00x = default GEOMETRY size. Reproduce with:

```sh
cd /path/to/directory/with/tiger.duckdb
/path/to/geosilo/build/release/duckdb -f /path/to/geosilo/scripts/benchmark.sql
```

### On-disk storage (geom column only, 256 KB blocks)

| Table | Default | Shredded+ALP | Silo | Silo+ZSTD |
|---|---|---|---|---|
| block_group | 294 MB (1.00x) | 182 MB (0.62x) | 75 MB (0.25x) | 71 MB (0.24x) |
| zcta5 | 189 MB (1.00x) | 76 MB (0.40x) | 71 MB (0.38x) | 71 MB (0.38x) |
| county | 25 MB (1.00x) | 11 MB (0.44x) | 8 MB (0.34x) | 8 MB (0.34x) |
| urban_area | 24 MB (1.00x) | 10 MB (0.42x) | 9 MB (0.37x) | 9 MB (0.37x) |

### Wire transfer (geometry column serialized size)

| Table | Rows | WKB (1.00x) | Silo | WKB+ZSTD | Silo+ZSTD |
|---|---|---|---|---|---|
| block_group | 242,748 | 209.2 MB | 68.4 MB (0.33x) | 129.1 MB (0.62x) | 57.5 MB (0.28x) |
| zcta5 | 33,791 | 180.0 MB | 52.9 MB (0.29x) | 134.9 MB (0.75x) | 47.6 MB (0.26x) |
| tract | 85,529 | 124.6 MB | 39.5 MB (0.32x) | 75.6 MB (0.61x) | 34.1 MB (0.27x) |
| county | 3,235 | 24.6 MB | 7.5 MB (0.31x) | 19.7 MB (0.80x) | 6.7 MB (0.27x) |
| urban_area | 2,644 | 23.2 MB | 6.9 MB (0.29x) | 18.3 MB (0.79x) | 6.1 MB (0.26x) |
| coastline | 4,240 | 7.2 MB | 2.1 MB (0.29x) | 5.7 MB (0.80x) | 1.9 MB (0.26x) |
| state | 56 | 3.2 MB | 1.1 MB (0.35x) | 2.4 MB (0.76x) | 0.9 MB (0.29x) |

## Enabling ZSTD compression for stored blobs

DuckDB does not apply ZSTD to BLOB columns by default. Two requirements:

1. **Set `storage_compatibility_version` to `latest`** — ZSTD compression requires serialization version 4+ (v1.5.0+). Without this, DuckDB silently falls back to uncompressed storage.

2. **Specify `USING COMPRESSION zstd` on the column** — DuckDB's auto-compression won't select ZSTD for BLOBs. You must declare it in the CREATE TABLE.

```sql
-- Both are required for ZSTD-compressed silo blobs
PRAGMA storage_compatibility_version='latest';

CREATE TABLE my_table (
    id INTEGER,
    name VARCHAR,
    geom BLOB USING COMPRESSION zstd   -- explicit ZSTD on the silo column
);

INSERT INTO my_table
SELECT id, name, silo_encode(geom) FROM source_table;
```

Without `storage_compatibility_version='latest'`, `USING COMPRESSION zstd` is silently ignored and blobs are stored uncompressed.

GeoSilo achieves **0.24-0.38x** on-disk storage vs default GEOMETRY, and **0.26-0.29x** wire transfer size vs WKB when both are ZSTD-compressed. It outperforms DuckDB's native shredded ALP compression by ~1.5-2.5x because delta-encoded int16 values compress far better than ALP-encoded float64 arrays.

## Scale and precision

At the default scale of 1e7, coordinates are quantized to 1e-7 degrees (~1cm at the equator). The scale factor is stored in the blob header, so decoding never needs configuration.

### Automatic scale detection

When the GEOMETRY column carries a CRS (e.g., `GEOMETRY(EPSG:4326)`), `silo_encode` automatically selects the appropriate scale:

| CRS | Units | Auto Scale | Precision |
|---|---|---|---|
| EPSG:4326 (WGS84) | degrees | 10,000,000 | ~1cm |
| EPSG:4269 (NAD83) | degrees | 10,000,000 | ~1cm |
| EPSG:326xx (UTM) | meters | 100 | 1cm |
| EPSG:3857 (Web Mercator) | meters | 100 | 1cm |
| No CRS | — | 10,000,000 (default) | ~1cm for degrees |

An explicit scale parameter always overrides auto-detection:

```sql
-- Force scale for a projected CRS in meters
SELECT silo_encode(geom, 100) FROM my_utm_table;
```

## Building

Requires DuckDB v1.5.0+ (GEOMETRY type is in core).

```sh
git clone --recurse-submodules https://github.com/Query-farm/geosilo.git
cd geosilo
GEN=ninja make release
```

The built extension is at `build/release/extension/geosilo/geosilo.duckdb_extension`.

## Usage with the spatial extension

GeoSilo works alongside the spatial extension — use spatial functions to create and manipulate geometry, and GeoSilo for compact encoding:

```sql
INSTALL spatial;
LOAD spatial;
LOAD geosilo;

-- Encode for compact storage or transfer
PRAGMA storage_compatibility_version='latest';

CREATE TABLE compact (
    id INTEGER,
    geom BLOB USING COMPRESSION zstd
);

INSERT INTO compact
SELECT id, silo_encode(geom) FROM source_table;

-- Decode back for spatial operations
SELECT ST_Area(silo_decode(geom)) FROM compact;
```
