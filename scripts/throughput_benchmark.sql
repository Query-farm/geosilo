-- GeoSilo encode/decode throughput benchmark
--
-- Usage:
--   cd ~/Development/geosilo
--   ./build/release/duckdb -f scripts/throughput_benchmark.sql

INSTALL spatial;
LOAD spatial;
LOAD geosilo;

SET enable_progress_bar = false;
SET threads = 1;

ATTACH '~/Development/vgi-tiger/tiger.duckdb' AS src (READ_ONLY);

-- ---------------------------------------------------------------------------
-- Prepare: materialize test data into memory
-- ---------------------------------------------------------------------------

CREATE TEMP TABLE test_bg AS SELECT geom FROM src.block_group;
CREATE TEMP TABLE test_county AS SELECT geom FROM src.county;
CREATE TEMP TABLE test_zcta5 AS SELECT geom FROM src.zcta5;
CREATE TEMP TABLE test_tract AS SELECT geom FROM src.tract;

CREATE TEMP TABLE encoded_bg AS SELECT geosilo_encode(geom) AS silo FROM test_bg;
CREATE TEMP TABLE encoded_county AS SELECT geosilo_encode(geom) AS silo FROM test_county;
CREATE TEMP TABLE encoded_zcta5 AS SELECT geosilo_encode(geom) AS silo FROM test_zcta5;
CREATE TEMP TABLE encoded_tract AS SELECT geosilo_encode(geom) AS silo FROM test_tract;

CREATE TEMP TABLE wkb_bg AS SELECT ST_AsWKB(geom) AS wkb FROM test_bg;

-- Sizes
.mode list
.separator ""
.headers off

CREATE TEMP TABLE sizes AS
SELECT 'block_group' AS t, count(*) AS n,
       sum(octet_length(ST_AsWKB(geom)))::BIGINT AS wkb_bytes,
       sum(octet_length(geosilo_encode(geom)))::BIGINT AS silo_bytes
FROM test_bg
UNION ALL
SELECT 'county', count(*),
       sum(octet_length(ST_AsWKB(geom)))::BIGINT,
       sum(octet_length(geosilo_encode(geom)))::BIGINT
FROM test_county
UNION ALL
SELECT 'zcta5', count(*),
       sum(octet_length(ST_AsWKB(geom)))::BIGINT,
       sum(octet_length(geosilo_encode(geom)))::BIGINT
FROM test_zcta5
UNION ALL
SELECT 'tract', count(*),
       sum(octet_length(ST_AsWKB(geom)))::BIGINT,
       sum(octet_length(geosilo_encode(geom)))::BIGINT
FROM test_tract;

SELECT '';
SELECT '### Dataset sizes';
SELECT '';
SELECT '| Table | Rows | WKB Size | Silo Size | Ratio |';
SELECT '|---|---|---|---|---|';
SELECT '| ' || t
    || ' | ' || format('{:,}', n)
    || ' | ' || printf('%.1f MB', wkb_bytes / 1e6)
    || ' | ' || printf('%.1f MB', silo_bytes / 1e6)
    || ' | ' || printf('%.2fx', 1.0 * silo_bytes / wkb_bytes)
    || ' |'
FROM sizes ORDER BY wkb_bytes DESC;

-- ---------------------------------------------------------------------------
-- Warmup runs (force full materialization via sum of output length)
-- ---------------------------------------------------------------------------
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_bg;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_bg;
SELECT sum(octet_length(ST_AsWKB(geom))) FROM test_bg;
SELECT sum(ST_NPoints(ST_GeomFromWKB(wkb))) FROM wkb_bg;

-- ---------------------------------------------------------------------------
-- Timed runs — .timer on prints wall clock per query
-- Use sum(octet_length(...)) to force materialization of every row.
-- ---------------------------------------------------------------------------

SELECT '';
SELECT '### Encode: GEOMETRY → GeoSilo (single-threaded, 3 runs)';
SELECT '';

.timer on

SELECT '-- block_group encode (209 MB WKB input, 242k rows)';
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_bg;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_bg;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_bg;

SELECT '-- zcta5 encode (180 MB WKB input, 34k rows)';
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_zcta5;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_zcta5;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_zcta5;

SELECT '-- tract encode (125 MB WKB input, 86k rows)';
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_tract;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_tract;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_tract;

SELECT '-- county encode (25 MB WKB input, 3k rows)';
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_county;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_county;
SELECT sum(octet_length(geosilo_encode(geom))) FROM test_county;

SELECT '';
SELECT '### Decode: GeoSilo → GEOMETRY (single-threaded, 3 runs)';
SELECT '';

SELECT '-- block_group decode (68 MB silo → 209 MB WKB, 242k rows)';
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_bg;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_bg;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_bg;

SELECT '-- zcta5 decode (53 MB silo → 180 MB WKB, 34k rows)';
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_zcta5;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_zcta5;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_zcta5;

SELECT '-- tract decode (40 MB silo → 125 MB WKB, 86k rows)';
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_tract;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_tract;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_tract;

SELECT '-- county decode (8 MB silo → 25 MB WKB, 3k rows)';
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_county;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_county;
SELECT sum(ST_NPoints(geosilo_decode(silo))) FROM encoded_county;

SELECT '';
SELECT '### Baseline: WKB serialize/deserialize (block_group, single-threaded, 3 runs)';
SELECT '';

SELECT '-- GEOMETRY → WKB (209 MB, 242k rows)';
SELECT sum(octet_length(ST_AsWKB(geom))) FROM test_bg;
SELECT sum(octet_length(ST_AsWKB(geom))) FROM test_bg;
SELECT sum(octet_length(ST_AsWKB(geom))) FROM test_bg;

SELECT '-- WKB → GEOMETRY (209 MB, 242k rows)';
SELECT sum(ST_NPoints(ST_GeomFromWKB(wkb))) FROM wkb_bg;
SELECT sum(ST_NPoints(ST_GeomFromWKB(wkb))) FROM wkb_bg;
SELECT sum(ST_NPoints(ST_GeomFromWKB(wkb))) FROM wkb_bg;

SELECT '';
SELECT '### Metadata extraction (block_group, 242k rows, single-threaded, 3 runs)';
SELECT '';

SELECT '-- geosilo_metadata (header-only read, no decode)';
SELECT count(geosilo_metadata(silo).scale) FROM encoded_bg;
SELECT count(geosilo_metadata(silo).scale) FROM encoded_bg;
SELECT count(geosilo_metadata(silo).scale) FROM encoded_bg;

SELECT '';
SELECT '### End-to-end: ST_Area raw vs decode+ST_Area (single-threaded, 3 runs)';
SELECT '';

SELECT '-- block_group: raw ST_Area (242k rows)';
SELECT sum(ST_Area(geom)) FROM test_bg;
SELECT sum(ST_Area(geom)) FROM test_bg;
SELECT sum(ST_Area(geom)) FROM test_bg;

SELECT '-- block_group: decode+ST_Area (242k rows)';
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_bg;
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_bg;
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_bg;

SELECT '-- county: raw ST_Area (3k rows)';
SELECT sum(ST_Area(geom)) FROM test_county;
SELECT sum(ST_Area(geom)) FROM test_county;
SELECT sum(ST_Area(geom)) FROM test_county;

SELECT '-- county: decode+ST_Area (3k rows)';
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_county;
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_county;
SELECT sum(ST_Area(geosilo_decode(silo))) FROM encoded_county;

.timer off

SELECT '';

DETACH src;
