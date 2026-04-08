-- GeoSilo compression benchmark
--
-- Compares geometry encoding across storage and wire transfer:
--   1. Default GEOMETRY (uncompressed WKB on disk)
--   2. Shredded GEOMETRY + ALP (DuckDB v1.5+ with homogeneous multi-types)
--   3. GeoSilo blob (delta-encoded int16/32)
--   4. GeoSilo blob + ZSTD (on disk)
--   5. Wire transfer: WKB vs Silo, raw and zstd-compressed
--
-- Outputs markdown tables suitable for pasting into README.md.
-- All ratios are relative to 1.00x = default GEOMETRY size.
--
-- Usage:
--   cd /path/to/directory/with/tiger.duckdb
--   /path/to/geosilo/build/release/duckdb -f /path/to/geosilo/scripts/benchmark.sql
--
-- Requires: spatial extension installed, tiger.duckdb in current directory.
-- The source tiger.duckdb should use homogeneous multi-types (ST_Multi)
-- and storage_compatibility_version='latest' for shredded storage.

INSTALL spatial;
LOAD spatial;
LOAD geosilo;

SET enable_progress_bar = false;

ATTACH 'tiger.duckdb' AS src (READ_ONLY);

.mode list
.separator ""
.headers off

-- ---------------------------------------------------------------------------
-- Table 1: On-disk storage comparison
-- ---------------------------------------------------------------------------
-- Build test databases for each format and measure geom column block sizes.

-- Default GEOMETRY (old compat, no shredding)
ATTACH '/tmp/_gs_default.duckdb' AS d_default;
CREATE TABLE d_default.bg AS SELECT * FROM src.block_group;
CREATE TABLE d_default.zcta5 AS SELECT * FROM src.zcta5;
CREATE TABLE d_default.county AS SELECT * FROM src.county;
CREATE TABLE d_default.urban_area AS SELECT * FROM src.urban_area;
CHECKPOINT d_default;

-- Shredded GEOMETRY + ALP (latest storage, small row groups)
PRAGMA storage_compatibility_version='latest';
SET geometry_minimum_shredding_size = 0;

ATTACH '/tmp/_gs_shredded.duckdb' AS d_shredded (ROW_GROUP_SIZE 2048);
CREATE TABLE d_shredded.bg AS SELECT * FROM src.block_group;
CREATE TABLE d_shredded.zcta5 AS SELECT * FROM src.zcta5;
CREATE TABLE d_shredded.county AS SELECT * FROM src.county;
CREATE TABLE d_shredded.urban_area AS SELECT * FROM src.urban_area;
CHECKPOINT d_shredded;

-- Silo blob (uncompressed)
SET geometry_minimum_shredding_size = -1;

ATTACH '/tmp/_gs_silo_store.duckdb' AS d_silo (ROW_GROUP_SIZE 2048);
CREATE TABLE d_silo.bg (statefp VARCHAR, countyfp VARCHAR, tractce VARCHAR, blkgrpce VARCHAR, geoid VARCHAR, geoidfq VARCHAR, namelsad VARCHAR, mtfcc VARCHAR, funcstat VARCHAR, aland BIGINT, awater BIGINT, intptlat DOUBLE, intptlon DOUBLE, geom BLOB, bbox_xmin DOUBLE, bbox_ymin DOUBLE, bbox_xmax DOUBLE, bbox_ymax DOUBLE, centroid BLOB);
INSERT INTO d_silo.bg SELECT statefp, countyfp, tractce, blkgrpce, geoid, geoidfq, namelsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, geosilo_encode(geom), bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, geosilo_encode(centroid) FROM src.block_group;

CREATE TABLE d_silo.zcta5 AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.zcta5;
CREATE TABLE d_silo.county AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.county;
CREATE TABLE d_silo.urban_area AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.urban_area;
CHECKPOINT d_silo;

-- Silo blob + ZSTD
ATTACH '/tmp/_gs_silo_zstd.duckdb' AS d_silo_z (ROW_GROUP_SIZE 2048);
CREATE TABLE d_silo_z.bg (statefp VARCHAR, countyfp VARCHAR, tractce VARCHAR, blkgrpce VARCHAR, geoid VARCHAR, geoidfq VARCHAR, namelsad VARCHAR, mtfcc VARCHAR, funcstat VARCHAR, aland BIGINT, awater BIGINT, intptlat DOUBLE, intptlon DOUBLE, geom BLOB USING COMPRESSION zstd, bbox_xmin DOUBLE, bbox_ymin DOUBLE, bbox_xmax DOUBLE, bbox_ymax DOUBLE, centroid BLOB USING COMPRESSION zstd);
INSERT INTO d_silo_z.bg SELECT statefp, countyfp, tractce, blkgrpce, geoid, geoidfq, namelsad, mtfcc, funcstat, aland, awater, intptlat, intptlon, geosilo_encode(geom), bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax, geosilo_encode(centroid) FROM src.block_group;

CREATE TABLE d_silo_z.zcta5 AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.zcta5;
CREATE TABLE d_silo_z.county AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.county;
CREATE TABLE d_silo_z.urban_area AS SELECT * EXCLUDE (geom, centroid), geosilo_encode(geom) AS geom, geosilo_encode(centroid) AS centroid FROM src.urban_area;
CHECKPOINT d_silo_z;

-- Collect geom column block counts (each block = 256 KB max)
CREATE TEMP TABLE disk_sizes AS
WITH block_counts AS (
    SELECT 'default' AS fmt, 'block_group' AS t, sum(len(additional_block_ids) + 1) AS blocks FROM pragma_storage_info('d_default.bg') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'default', 'zcta5', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_default.zcta5') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'default', 'county', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_default.county') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'default', 'urban_area', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_default.urban_area') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0

    UNION ALL SELECT 'shredded', 'block_group', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_shredded.bg') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'shredded', 'zcta5', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_shredded.zcta5') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'shredded', 'county', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_shredded.county') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'shredded', 'urban_area', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_shredded.urban_area') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0

    UNION ALL SELECT 'silo', 'block_group', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo.bg') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo', 'zcta5', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo.zcta5') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo', 'county', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo.county') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo', 'urban_area', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo.urban_area') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0

    UNION ALL SELECT 'silo_zstd', 'block_group', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo_z.bg') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo_zstd', 'zcta5', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo_z.zcta5') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo_zstd', 'county', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo_z.county') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
    UNION ALL SELECT 'silo_zstd', 'urban_area', sum(len(additional_block_ids) + 1) FROM pragma_storage_info('d_silo_z.urban_area') WHERE column_name = 'geom' AND segment_type NOT IN ('VALIDITY') AND block_id >= 0
)
SELECT * FROM block_counts;

DETACH d_default;
DETACH d_shredded;
DETACH d_silo;
DETACH d_silo_z;

-- Print storage comparison
SELECT '';
SELECT '### On-disk storage (geom column only, 256 KB blocks)';
SELECT '';
SELECT '| Table | Default | Shredded+ALP | Silo | Silo+ZSTD |';
SELECT '|---|---|---|---|---|';

SELECT '| ' || d.t
    || ' | ' || printf('%.0f MB (1.00x)', d.blocks * 0.25)
    || ' | ' || printf('%.0f MB (%.2fx)', s.blocks * 0.25, 1.0 * s.blocks / d.blocks)
    || ' | ' || printf('%.0f MB (%.2fx)', si.blocks * 0.25, 1.0 * si.blocks / d.blocks)
    || ' | ' || printf('%.0f MB (%.2fx)', sz.blocks * 0.25, 1.0 * sz.blocks / d.blocks)
    || ' |'
FROM disk_sizes d
JOIN disk_sizes s ON d.t = s.t AND s.fmt = 'shredded'
JOIN disk_sizes si ON d.t = si.t AND si.fmt = 'silo'
JOIN disk_sizes sz ON d.t = sz.t AND sz.fmt = 'silo_zstd'
WHERE d.fmt = 'default'
ORDER BY d.blocks DESC;

-- ---------------------------------------------------------------------------
-- Table 2: Wire transfer comparison
-- ---------------------------------------------------------------------------

CREATE TEMP TABLE raw_sizes AS
SELECT 'block_group' AS t, count(*) AS n, sum(octet_length(ST_AsWKB(geom))) AS wkb, sum(octet_length(geosilo_encode(geom))) AS silo FROM src.block_group
UNION ALL SELECT 'zcta5', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.zcta5
UNION ALL SELECT 'tract', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.tract
UNION ALL SELECT 'county', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.county
UNION ALL SELECT 'urban_area', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.urban_area
UNION ALL SELECT 'state', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.state
UNION ALL SELECT 'coastline', count(*), sum(octet_length(ST_AsWKB(geom))), sum(octet_length(geosilo_encode(geom))) FROM src.coastline;

CREATE TEMP TABLE zstd_sizes (t VARCHAR, wkb_z BIGINT, silo_z BIGINT);

COPY (SELECT ST_AsWKB(geom) AS g FROM src.block_group) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.block_group) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('block_group', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.zcta5) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.zcta5) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('zcta5', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.tract) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.tract) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('tract', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.county) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.county) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('county', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.urban_area) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.urban_area) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('urban_area', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.state) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.state) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('state', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

COPY (SELECT ST_AsWKB(geom) AS g FROM src.coastline) TO '/tmp/_gs_wkb.parquet' (COMPRESSION ZSTD);
COPY (SELECT geosilo_encode(geom) AS g FROM src.coastline) TO '/tmp/_gs_silo.parquet' (COMPRESSION ZSTD);
INSERT INTO zstd_sizes VALUES ('coastline', (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_wkb.parquet')), (SELECT sum(total_compressed_size) FROM parquet_metadata('/tmp/_gs_silo.parquet')));

SELECT '';
SELECT '### Wire transfer (geometry column serialized size)';
SELECT '';
SELECT '| Table | Rows | WKB (1.00x) | Silo | WKB+ZSTD | Silo+ZSTD |';
SELECT '|---|---|---|---|---|---|';

SELECT '| ' || r.t
    || ' | ' || format('{:,}', r.n)
    || ' | ' || printf('%.1f MB', r.wkb / 1e6)
    || ' | ' || printf('%.1f MB (%.2fx)', r.silo / 1e6, 1.0 * r.silo / r.wkb)
    || ' | ' || printf('%.1f MB (%.2fx)', z.wkb_z / 1e6, 1.0 * z.wkb_z / r.wkb)
    || ' | ' || printf('%.1f MB (%.2fx)', z.silo_z / 1e6, 1.0 * z.silo_z / r.wkb)
    || ' |'
FROM raw_sizes r
JOIN zstd_sizes z ON r.t = z.t
ORDER BY r.wkb DESC;
