<p align="center">
  <a href="https://query.farm">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="https://query.farm/media-kit/logo/wordmark-dark.svg">
      <img alt="Query.Farm" src="https://query.farm/media-kit/logo/wordmark-light.svg" height="64">
    </picture>
  </a>
</p>

# GeoSilo by [Query.Farm](https://query.farm)

[![DuckDB](https://img.shields.io/badge/DuckDB-community_extension-fdf1e0?logo=duckdb&logoColor=fff000)](https://duckdb.org/community_extensions/extensions/geosilo.html)
[![v1.5 build](https://github.com/Query-farm/geosilo/actions/workflows/MainDistributionPipeline.yml/badge.svg?branch=v1.5)](https://github.com/Query-farm/geosilo/actions/workflows/MainDistributionPipeline.yml?query=branch%3Av1.5)

A DuckDB extension for compact geometry encoding. Delta-encodes coordinates as scaled integers instead of float64 pairs, achieving **3–4x smaller** geometry on disk and over the wire compared to standard WKB. Points compress to just **9 bytes** (vs 21 bytes WKB).

## Documentation

Full documentation, including installation, usage, the function reference, and cookbook examples, is available at:

**[https://query.farm/products/extensions/geosilo](https://query.farm/products/extensions/geosilo)**

## Installation

```sql
INSTALL geosilo FROM community;
LOAD geosilo;
```
