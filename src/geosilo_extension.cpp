#define DUCKDB_EXTENSION_MAIN

#include "geosilo_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_type_extension.hpp"
#include "duckdb/common/arrow/schema_metadata.hpp"
#include "duckdb/function/table/arrow/arrow_duck_schema.hpp"
#include "duckdb/function/table/arrow/arrow_type_info.hpp"
#include "duckdb/main/config.hpp"
#include "yyjson.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

#include "query_farm_telemetry.hpp"

#include <cmath>
#include <cstring>
#include <limits>

namespace duckdb {

// ---------------------------------------------------------------------------
// GeoSilo binary format
// ---------------------------------------------------------------------------
//
// Full header (12 bytes, magic 0x47):
//   magic            1 byte   0x47 ('G')
//   version          1 byte   0x01
//   geometry_type    1 byte   GeometryType enum
//   vertex_type      1 byte   VertexType enum (XY=0, XYZ=1, XYM=2, XYZM=3)
//   scale            8 bytes  int64 LE
//
// Compact point header (1 byte, magic 0x50–0x53):
//   A single magic byte encodes geometry_type=POINT, vertex_type, and scale.
//   0x50 = POINT XY  scale 1e7    (9 bytes total)
//   0x51 = POINT XYZ scale 1e7    (13 bytes total)
//   0x52 = POINT XY  scale 100    (9 bytes total)
//   0x53 = POINT XYZ scale 100    (13 bytes total)
//   Body: raw int32 coordinates (no count prefix).
//
// Coordinate encoding:
//   First coordinate per ring/linestring: int32 absolute.
//   Subsequent coordinates: int16 delta from previous.
//     If delta doesn't fit int16, write INT16_MIN as sentinel, then int32 delta.
//   Empty point (NaN): INT32_MAX sentinel per dimension.
//
// Version history:
//   v1 (0x01): MULTIPOINT stores each sub-point with absolute int32 coords.
//   v2 (0x02): MULTIPOINT uses delta encoding between sub-points
//              (first absolute, rest delta — same as linestring sequences).
//              All other geometry types are identical to v1.
//   The reader accepts both v1 and v2. The writer always emits v2 for
//   MULTIPOINT and v1 for all other types using the full header.

static constexpr uint8_t GEOSILO_MAGIC      = 0x47; // 'G' — full header
static constexpr uint8_t GEOSILO_VERSION_1   = 0x01; // v1: MULTIPOINT uses absolute coords per point
static constexpr uint8_t GEOSILO_VERSION_2   = 0x02; // v2: MULTIPOINT uses delta encoding between points
static constexpr int64_t DEFAULT_SCALE       = 10000000; // 1e7
static constexpr int16_t DELTA_ESCAPE        = std::numeric_limits<int16_t>::min(); // -32768
static constexpr int32_t EMPTY_COORD         = std::numeric_limits<int32_t>::max();

// Compact point magic bytes — the single byte implies geometry_type, vertex_type, and scale.
// Body is just the raw int32 coordinates (8 bytes for XY, 12 bytes for XYZ).
static constexpr uint8_t COMPACT_POINT_XY_1E7   = 0x50; // 'P' — POINT XY, scale 1e7
static constexpr uint8_t COMPACT_POINT_XYZ_1E7  = 0x51; // 'Q' — POINT XYZ, scale 1e7
static constexpr uint8_t COMPACT_POINT_XY_100   = 0x52; // 'R' — POINT XY, scale 100
static constexpr uint8_t COMPACT_POINT_XYZ_100  = 0x53; // 'S' — POINT XYZ, scale 100

// ---------------------------------------------------------------------------
// Scale detection from CRS
// ---------------------------------------------------------------------------

static int64_t ScaleFromCRS(const CoordinateReferenceSystem &crs) {
	auto id = crs.GetIdentifier();
	int epsg = 0;
	if (id.rfind("EPSG:", 0) == 0) {
		try { epsg = std::stoi(id.substr(5)); } catch (...) { return DEFAULT_SCALE; }
	} else {
		try { epsg = std::stoi(id); } catch (...) { return DEFAULT_SCALE; }
	}

	// Geographic CRS (degrees) → 1e7
	if (epsg == 4326 || epsg == 4269 || epsg == 4267 || epsg == 4258 || epsg == 4674) {
		return 10000000;
	}
	// UTM zones (meters)
	if ((epsg >= 32601 && epsg <= 32660) || (epsg >= 32701 && epsg <= 32760) ||
	    (epsg >= 26901 && epsg <= 26999) || (epsg >= 26701 && epsg <= 26799)) {
		return 100;
	}
	// Web Mercator
	if (epsg == 3857) { return 100; }
	// State Plane meters
	if (epsg >= 6300 && epsg <= 6600) { return 100; }
	// State Plane US feet
	if (epsg >= 2200 && epsg <= 2300) { return 30; }

	return DEFAULT_SCALE;
}

// ---------------------------------------------------------------------------
// Bind data
// ---------------------------------------------------------------------------

struct SiloEncodeBindData : public FunctionData {
	int64_t scale;
	explicit SiloEncodeBindData(int64_t scale) : scale(scale) {}
	unique_ptr<FunctionData> Copy() const override { return make_uniq<SiloEncodeBindData>(scale); }
	bool Equals(const FunctionData &other_p) const override {
		return scale == other_p.Cast<SiloEncodeBindData>().scale;
	}
};

// ---------------------------------------------------------------------------
// Writer: GEOMETRY (WKB bytes) → GeoSilo v2 blob
// ---------------------------------------------------------------------------
// Uses raw pointer writes to a pre-sized buffer. The worst-case silo output
// size is bounded by the WKB input size (every coordinate shrinks or stays
// the same), so we allocate WKB_size/2 + 14 upfront and never resize.

class SiloWriter {
public:
	explicit SiloWriter(int64_t scale) : scale_(scale), dbl_scale_(static_cast<double>(scale)) {}

	void SetScale(int64_t scale) { scale_ = scale; dbl_scale_ = static_cast<double>(scale); }

	string_t Encode(const uint8_t *data, idx_t size, Vector &result) {
		rp_ = data;

		auto byte_order = RByte();
		(void)byte_order;
		auto wkb_type = RU32();
		auto geom_type = static_cast<GeometryType>(wkb_type & 0xFF);
		auto has_z = (wkb_type & 0x80000000) != 0;
		auto has_m = (wkb_type & 0x40000000) != 0;
		VertexType vert_type = VertexType::XY;
		if (has_z && has_m)      vert_type = VertexType::XYZM;
		else if (has_z)          vert_type = VertexType::XYZ;
		else if (has_m)          vert_type = VertexType::XYM;
		vw_ = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

		// Pre-size buffer. Silo is always <= WKB: worst case is all escape
		// deltas (6 bytes vs 8 bytes per coord = 75%) plus 14-byte header.
		buf_.resize(size + 14);
		wp_ = buf_.data();

		// Try compact point encoding: single magic byte replaces the full header.
		uint8_t compact_magic = TryCompactPointMagic(geom_type, vert_type);
		if (compact_magic) {
			WByte(compact_magic);
		} else {
			// Full header — use v2 for MULTIPOINT (delta encoding)
			uint8_t version = (geom_type == GeometryType::MULTIPOINT) ? GEOSILO_VERSION_2 : GEOSILO_VERSION_1;
			WByte(GEOSILO_MAGIC);
			WByte(version);
			WByte(static_cast<uint8_t>(geom_type));
			WByte(static_cast<uint8_t>(vert_type));
			WI64(scale_);
		}

		EncodeGeometry(geom_type);

		auto actual_size = static_cast<idx_t>(wp_ - buf_.data());
		return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(buf_.data()), actual_size);
	}

private:
	// --- WKB reading (unchecked in inner loop — validated by overall size) ---
	uint8_t RByte()  { return *rp_++; }
	uint32_t RU32()  { uint32_t v; memcpy(&v, rp_, 4); rp_ += 4; return v; }
	double RDbl()    { double v; memcpy(&v, rp_, 8); rp_ += 8; return v; }

	// --- Output writing (raw pointer, no resize) ---
	void WByte(uint8_t b)    { *wp_++ = b; }
	void WU32(uint32_t v)    { memcpy(wp_, &v, 4); wp_ += 4; }
	void WI16(int16_t v)     { memcpy(wp_, &v, 2); wp_ += 2; }
	void WI32(int32_t v)     { memcpy(wp_, &v, 4); wp_ += 4; }
	void WI64(int64_t v)     { memcpy(wp_, &v, 8); wp_ += 8; }

	// Returns a compact magic byte if this is a POINT with a well-known scale, else 0.
	uint8_t TryCompactPointMagic(GeometryType type, VertexType vert_type) {
		if (type != GeometryType::POINT || vert_type == VertexType::XYM || vert_type == VertexType::XYZM) {
			return 0;
		}
		bool is_xy = (vert_type == VertexType::XY);
		if (scale_ == 10000000) {
			return is_xy ? COMPACT_POINT_XY_1E7 : COMPACT_POINT_XYZ_1E7;
		}
		if (scale_ == 100) {
			return is_xy ? COMPACT_POINT_XY_100 : COMPACT_POINT_XYZ_100;
		}
		return 0;
	}

	int32_t ScaleCoord(double val) {
		if (std::isnan(val)) return EMPTY_COORD;
		auto scaled = static_cast<int64_t>(std::round(val * dbl_scale_));
		if (scaled > std::numeric_limits<int32_t>::max() || scaled < std::numeric_limits<int32_t>::min()) {
			throw InvalidInputException(
			    "GeoSilo: coordinate %.7f overflows int32 at scale %lld. Use a smaller scale.",
			    val, static_cast<long long>(scale_));
		}
		return static_cast<int32_t>(scaled);
	}

	void WriteDelta(int32_t delta) {
		if (delta > std::numeric_limits<int16_t>::max() || delta <= std::numeric_limits<int16_t>::min()) {
			WI16(DELTA_ESCAPE);
			WI32(delta);
		} else {
			WI16(static_cast<int16_t>(delta));
		}
	}

	void EncodeCoordinateSequence(uint32_t num_points) {
		WU32(num_points);
		if (num_points == 0) return;

		// First vertex: absolute
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t d = 0; d < vw_; d++) {
			prev[d] = ScaleCoord(RDbl());
			WI32(prev[d]);
		}
		// Remaining vertices: delta
		for (uint32_t i = 1; i < num_points; i++) {
			for (uint32_t d = 0; d < vw_; d++) {
				int32_t scaled = ScaleCoord(RDbl());
				WriteDelta(scaled - prev[d]);
				prev[d] = scaled;
			}
		}
	}

	// Delta-encode MULTIPOINT: like a coordinate sequence but skipping WKB sub-headers.
	void EncodeMultiPointDelta() {
		uint32_t num_parts = RU32();
		WU32(num_parts);
		if (num_parts == 0) return;

		int32_t prev[4] = {0, 0, 0, 0};
		// First point: absolute
		RByte(); RU32(); // skip WKB sub-header
		for (uint32_t d = 0; d < vw_; d++) {
			prev[d] = ScaleCoord(RDbl());
			WI32(prev[d]);
		}
		// Remaining points: delta
		for (uint32_t p = 1; p < num_parts; p++) {
			RByte(); RU32(); // skip WKB sub-header
			for (uint32_t d = 0; d < vw_; d++) {
				int32_t scaled = ScaleCoord(RDbl());
				WriteDelta(scaled - prev[d]);
				prev[d] = scaled;
			}
		}
	}

	void EncodeGeometry(GeometryType type) {
		switch (type) {
		case GeometryType::POINT: {
			for (uint32_t d = 0; d < vw_; d++) {
				WI32(ScaleCoord(RDbl()));
			}
			break;
		}
		case GeometryType::LINESTRING: {
			EncodeCoordinateSequence(RU32());
			break;
		}
		case GeometryType::POLYGON: {
			uint32_t num_rings = RU32();
			WU32(num_rings);
			for (uint32_t r = 0; r < num_rings; r++) {
				EncodeCoordinateSequence(RU32());
			}
			break;
		}
		case GeometryType::MULTIPOINT: {
			EncodeMultiPointDelta();
			break;
		}
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			uint32_t num_parts = RU32();
			WU32(num_parts);
			auto inner = (type == GeometryType::MULTILINESTRING) ? GeometryType::LINESTRING
			                                                     : GeometryType::POLYGON;
			for (uint32_t p = 0; p < num_parts; p++) {
				RByte();   // byte order
				RU32();    // type (skip)
				EncodeGeometry(inner);
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			uint32_t num_parts = RU32();
			WU32(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				RByte(); // byte order
				auto wt = RU32();
				auto child_type = static_cast<GeometryType>(wt & 0xFF);
				WByte(static_cast<uint8_t>(child_type));
				EncodeGeometry(child_type);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	int64_t scale_;
	double dbl_scale_;
	uint32_t vw_ = 2;
	const uint8_t *rp_ = nullptr;
	uint8_t *wp_ = nullptr;
	vector<uint8_t> buf_;
};

// ---------------------------------------------------------------------------
// Compact point magic resolution
// ---------------------------------------------------------------------------

static void ResolveCompactMagic(uint8_t magic, GeometryType &geom_type, VertexType &vert_type, double &inv_scale) {
	geom_type = GeometryType::POINT;
	switch (magic) {
	case COMPACT_POINT_XY_1E7:
		vert_type = VertexType::XY;
		inv_scale = 1.0 / 10000000.0;
		break;
	case COMPACT_POINT_XYZ_1E7:
		vert_type = VertexType::XYZ;
		inv_scale = 1.0 / 10000000.0;
		break;
	case COMPACT_POINT_XY_100:
		vert_type = VertexType::XY;
		inv_scale = 1.0 / 100.0;
		break;
	case COMPACT_POINT_XYZ_100:
		vert_type = VertexType::XYZ;
		inv_scale = 1.0 / 100.0;
		break;
	default:
		throw InvalidInputException("GeoSilo: invalid compact magic 0x%02x", magic);
	}
}

// Map compact magic → scale (for metadata)
static int64_t CompactMagicScale(uint8_t magic) {
	switch (magic) {
	case COMPACT_POINT_XY_1E7:
	case COMPACT_POINT_XYZ_1E7:
		return 10000000;
	case COMPACT_POINT_XY_100:
	case COMPACT_POINT_XYZ_100:
		return 100;
	default:
		return 0;
	}
}

// ---------------------------------------------------------------------------
// Reader: GeoSilo blob → WKB bytes
// ---------------------------------------------------------------------------
// Uses raw pointer reads/writes. Output is allocated directly in StringVector
// via EmptyString — no intermediate buffer, no copy. WKB output size is
// bounded by silo_size * 4 (worst case: every 2-byte delta expands to 8-byte double).

class SiloReader {
public:
	string_t Decode(const uint8_t *data, idx_t size, Vector &result) {
		rp_ = data;

		if (size < 1) throw InvalidInputException("GeoSilo: blob too small");
		uint8_t magic = RByte();

		GeometryType geom_type;
		VertexType vert_type;

		if (magic >= COMPACT_POINT_XY_1E7 && magic <= COMPACT_POINT_XYZ_100) {
			// Compact point format — magic byte encodes everything.
			ResolveCompactMagic(magic, geom_type, vert_type, inv_scale_);
			version_ = GEOSILO_VERSION_2;
		} else if (magic == GEOSILO_MAGIC) {
			// Full header
			if (size < 12) throw InvalidInputException("GeoSilo: blob too small");
			version_ = RByte();
			if (version_ != GEOSILO_VERSION_1 && version_ != GEOSILO_VERSION_2) {
				throw InvalidInputException("GeoSilo: unsupported version %d", version_);
			}
			geom_type = static_cast<GeometryType>(RByte());
			vert_type = static_cast<VertexType>(RByte());
			auto scale = RI64();
			inv_scale_ = 1.0 / static_cast<double>(scale);
		} else {
			throw InvalidInputException("GeoSilo: invalid magic byte 0x%02x", magic);
		}

		bool has_z = (vert_type == VertexType::XYZ || vert_type == VertexType::XYZM);
		bool has_m = (vert_type == VertexType::XYM || vert_type == VertexType::XYZM);
		vw_ = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

		// Pre-size buffer. Worst case: each 2-byte delta → 8-byte double (4x),
		// plus 5-byte WKB part headers for Multi* types.
		buf_.resize(size * 5 + 64);
		wp_ = buf_.data();

		// WKB header
		WByte(0x01); // little-endian
		uint32_t wkb_type = static_cast<uint32_t>(geom_type);
		if (has_z) wkb_type |= 0x80000000;
		if (has_m) wkb_type |= 0x40000000;
		WU32(wkb_type);

		DecodeGeometry(geom_type);

		auto actual_size = static_cast<idx_t>(wp_ - buf_.data());
		return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(buf_.data()), actual_size);
	}

private:
	// --- Silo reading (raw pointer, no per-read bounds check) ---
	uint8_t RByte()  { return *rp_++; }
	uint32_t RU32()  { uint32_t v; memcpy(&v, rp_, 4); rp_ += 4; return v; }
	int16_t RI16()   { int16_t v; memcpy(&v, rp_, 2); rp_ += 2; return v; }
	int32_t RI32()   { int32_t v; memcpy(&v, rp_, 4); rp_ += 4; return v; }
	int64_t RI64()   { int64_t v; memcpy(&v, rp_, 8); rp_ += 8; return v; }

	int32_t RDelta() {
		int16_t d = RI16();
		if (d == DELTA_ESCAPE) return RI32();
		return static_cast<int32_t>(d);
	}

	// --- WKB writing (raw pointer, no resize) ---
	void WByte(uint8_t b)    { *wp_++ = b; }
	void WU32(uint32_t v)    { memcpy(wp_, &v, 4); wp_ += 4; }
	void WDbl(double v)      { memcpy(wp_, &v, 8); wp_ += 8; }

	double Unscale(int32_t val) {
		if (val == EMPTY_COORD) return std::numeric_limits<double>::quiet_NaN();
		return static_cast<double>(val) * inv_scale_;
	}

	// Delta-decode MULTIPOINT: reverse of EncodeMultiPointDelta.
	void DecodeMultiPointDelta() {
		uint32_t num_parts = RU32();
		WU32(num_parts);
		if (num_parts == 0) return;

		uint32_t wkb_type = static_cast<uint32_t>(GeometryType::POINT);
		if (vw_ >= 3) wkb_type |= 0x80000000;
		if (vw_ == 4) wkb_type |= 0x40000000;

		int32_t prev[4] = {0, 0, 0, 0};
		// First point: absolute
		WByte(0x01);
		WU32(wkb_type);
		for (uint32_t d = 0; d < vw_; d++) {
			prev[d] = RI32();
			WDbl(Unscale(prev[d]));
		}
		// Remaining points: delta
		for (uint32_t p = 1; p < num_parts; p++) {
			WByte(0x01);
			WU32(wkb_type);
			for (uint32_t d = 0; d < vw_; d++) {
				prev[d] += RDelta();
				WDbl(Unscale(prev[d]));
			}
		}
	}

	void DecodeCoordinateSequence() {
		uint32_t num_points = RU32();
		WU32(num_points);
		if (num_points == 0) return;

		// First vertex: absolute
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t d = 0; d < vw_; d++) {
			prev[d] = RI32();
			WDbl(Unscale(prev[d]));
		}
		// Remaining vertices: delta (no branch in inner loop)
		for (uint32_t i = 1; i < num_points; i++) {
			for (uint32_t d = 0; d < vw_; d++) {
				prev[d] += RDelta();
				WDbl(Unscale(prev[d]));
			}
		}
	}

	void DecodeGeometry(GeometryType type) {
		switch (type) {
		case GeometryType::POINT: {
			for (uint32_t d = 0; d < vw_; d++) {
				WDbl(Unscale(RI32()));
			}
			break;
		}
		case GeometryType::LINESTRING: {
			DecodeCoordinateSequence();
			break;
		}
		case GeometryType::POLYGON: {
			uint32_t num_rings = RU32();
			WU32(num_rings);
			for (uint32_t r = 0; r < num_rings; r++) {
				DecodeCoordinateSequence();
			}
			break;
		}
		case GeometryType::MULTIPOINT: {
			if (version_ >= GEOSILO_VERSION_2) {
				DecodeMultiPointDelta();
			} else {
				// v1: absolute coords per point
				uint32_t num_parts = RU32();
				WU32(num_parts);
				uint32_t wkb_type = static_cast<uint32_t>(GeometryType::POINT);
				if (vw_ >= 3) wkb_type |= 0x80000000;
				if (vw_ == 4) wkb_type |= 0x40000000;
				for (uint32_t p = 0; p < num_parts; p++) {
					WByte(0x01);
					WU32(wkb_type);
					DecodeGeometry(GeometryType::POINT);
				}
			}
			break;
		}
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			uint32_t num_parts = RU32();
			WU32(num_parts);
			auto inner = (type == GeometryType::MULTILINESTRING) ? GeometryType::LINESTRING
			                                                     : GeometryType::POLYGON;
			uint32_t wkb_type = static_cast<uint32_t>(inner);
			if (vw_ >= 3) wkb_type |= 0x80000000;
			if (vw_ == 4) wkb_type |= 0x40000000;
			for (uint32_t p = 0; p < num_parts; p++) {
				WByte(0x01);
				WU32(wkb_type);
				DecodeGeometry(inner);
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			uint32_t num_parts = RU32();
			WU32(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				auto child_type = static_cast<GeometryType>(RByte());
				WByte(0x01);
				uint32_t wkb_type = static_cast<uint32_t>(child_type);
				if (vw_ >= 3) wkb_type |= 0x80000000;
				if (vw_ == 4) wkb_type |= 0x40000000;
				WU32(wkb_type);
				DecodeGeometry(child_type);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	double inv_scale_ = 1.0 / static_cast<double>(DEFAULT_SCALE);
	uint8_t version_ = GEOSILO_VERSION_1;
	uint32_t vw_ = 2;
	const uint8_t *rp_ = nullptr;
	uint8_t *wp_ = nullptr;
	vector<uint8_t> buf_;
};

// ---------------------------------------------------------------------------
// Bind functions
// ---------------------------------------------------------------------------

static unique_ptr<FunctionData> SiloEncodeBind(ClientContext &context, ScalarFunction &bound_function,
                                               vector<unique_ptr<Expression>> &arguments) {
	int64_t scale = DEFAULT_SCALE;
	auto &geom_type = arguments[0]->return_type;
	if (geom_type.id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(geom_type)) {
		scale = ScaleFromCRS(GeoType::GetCRS(geom_type));
	}
	return make_uniq<SiloEncodeBindData>(scale);
}

static unique_ptr<FunctionData> SiloEncodeWithScaleBind(ClientContext &context, ScalarFunction &bound_function,
                                                        vector<unique_ptr<Expression>> &arguments) {
	return make_uniq<SiloEncodeBindData>(0); // sentinel — scale comes from runtime arg
}

// ---------------------------------------------------------------------------
// Scalar functions
// ---------------------------------------------------------------------------

// #2: encode reads GEOMETRY string_t data directly as WKB — no intermediate Vector
// DuckDB's internal GEOMETRY format IS WKB (ToBinary is a no-op Reinterpret),
// so we read the GEOMETRY bytes directly without conversion.
static void SiloEncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<SiloEncodeBindData>();
	int64_t scale = bind_data.scale;

	auto &geom_vec = args.data[0];

	SiloWriter writer(scale);
	UnaryExecutor::Execute<string_t, string_t>(geom_vec, result, args.size(), [&](string_t geom) {
		return writer.Encode(reinterpret_cast<const uint8_t *>(geom.GetData()),
		                     static_cast<idx_t>(geom.GetSize()), result);
	});
}

static void SiloEncodeWithScaleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom_vec = args.data[0];
	auto &scale_vec = args.data[1];

	SiloWriter writer(0);
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    geom_vec, scale_vec, result, args.size(), [&](string_t geom, int64_t scale) {
		    writer.SetScale(scale);
		    return writer.Encode(reinterpret_cast<const uint8_t *>(geom.GetData()),
		                         static_cast<idx_t>(geom.GetSize()), result);
	    });
}

// Decode writes WKB directly into the GEOMETRY result vector — no intermediate.
// DuckDB's internal GEOMETRY format IS WKB, so the bytes SiloReader produces
// are already valid GEOMETRY data.
static void SiloDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &blob_vec = args.data[0];

	SiloReader reader;
	UnaryExecutor::Execute<string_t, string_t>(blob_vec, result, args.size(), [&](string_t blob) {
		return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
		                     static_cast<idx_t>(blob.GetSize()), result);
	});
}

// silo_metadata(blob) → STRUCT{geometry_type VARCHAR, vertex_type VARCHAR, scale BIGINT}
static void SiloMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	static const char *geom_names[] = {"INVALID", "POINT", "LINESTRING", "POLYGON",
	                                   "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON",
	                                   "GEOMETRYCOLLECTION"};
	static const char *vert_names[] = {"XY", "XYZ", "XYM", "XYZM"};

	auto &blob_vec = args.data[0];
	auto &entries = StructVector::GetEntries(result);
	auto &geom_type_vec = *entries[0];
	auto &vert_type_vec = *entries[1];
	auto &scale_vec = *entries[2];

	UnifiedVectorFormat blob_format;
	blob_vec.ToUnifiedFormat(args.size(), blob_format);
	auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);

	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto blob_idx = blob_format.sel->get_index(i);
		if (!blob_format.validity.RowIsValid(blob_idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto &blob = blob_data[blob_idx];
		auto data = reinterpret_cast<const uint8_t *>(blob.GetData());
		auto size = static_cast<idx_t>(blob.GetSize());
		uint8_t gt, vt;
		int64_t scale;
		if (data[0] >= COMPACT_POINT_XY_1E7 && data[0] <= COMPACT_POINT_XYZ_100) {
			gt = static_cast<uint8_t>(GeometryType::POINT);
			bool is_xyz = (data[0] == COMPACT_POINT_XYZ_1E7 || data[0] == COMPACT_POINT_XYZ_100);
			vt = is_xyz ? static_cast<uint8_t>(VertexType::XYZ) : static_cast<uint8_t>(VertexType::XY);
			scale = CompactMagicScale(data[0]);
		} else if (data[0] == GEOSILO_MAGIC && size >= 12) {
			gt = data[2];
			vt = data[3];
			memcpy(&scale, data + 4, 8);
		} else {
			throw InvalidInputException("GeoSilo: invalid blob");
		}

		FlatVector::GetData<string_t>(geom_type_vec)[i] =
		    StringVector::AddString(geom_type_vec, (gt <= 7) ? geom_names[gt] : "UNKNOWN");
		FlatVector::GetData<string_t>(vert_type_vec)[i] =
		    StringVector::AddString(vert_type_vec, (vt <= 3) ? vert_names[vt] : "UNKNOWN");
		FlatVector::GetData<int64_t>(scale_vec)[i] = scale;
	}
}

// ---------------------------------------------------------------------------
// Arrow extension type: "queryfarm.geosilo"
// ---------------------------------------------------------------------------
// CRS is carried in the Arrow extension metadata as {"crs": "EPSG:4326"}.
// On read (ArrowToDuck), the CRS is parsed and attached to the GEOMETRY type.
// On write (DuckToArrow/PopulateSchema), the CRS from the GEOMETRY type is
// written to the metadata and used to auto-detect scale.

struct ArrowGeoSilo {
	// Parse Arrow metadata → GEOMETRY type (with optional CRS)
	static unique_ptr<ArrowType> GetType(ClientContext &context, const ArrowSchema &schema,
	                                     const ArrowSchemaMetadata &schema_metadata) {
		const auto extension_metadata = schema_metadata.GetOption(ArrowSchemaMetadata::ARROW_METADATA_KEY);

		unique_ptr<CoordinateReferenceSystem> duckdb_crs;
		if (!extension_metadata.empty()) {
			unique_ptr<duckdb_yyjson::yyjson_doc, void (*)(duckdb_yyjson::yyjson_doc *)> doc(
			    duckdb_yyjson::yyjson_read(extension_metadata.data(), extension_metadata.size(),
			                               duckdb_yyjson::YYJSON_READ_NOFLAG),
			    duckdb_yyjson::yyjson_doc_free);
			if (doc) {
				auto *root = yyjson_doc_get_root(doc.get());
				if (yyjson_is_obj(root)) {
					auto *crs_val = yyjson_obj_get(root, "crs");
					if (crs_val && yyjson_is_str(crs_val)) {
						duckdb_crs = CoordinateReferenceSystem::TryIdentify(context, yyjson_get_str(crs_val));
					}
				}
			}
		}

		auto geo_type = duckdb_crs ? LogicalType::GEOMETRY(*duckdb_crs) : LogicalType::GEOMETRY();
		const auto format = string(schema.format);
		if (format == "z") {
			return make_uniq<ArrowType>(std::move(geo_type), make_uniq<ArrowStringInfo>(ArrowVariableSizeType::NORMAL));
		}
		if (format == "Z") {
			return make_uniq<ArrowType>(std::move(geo_type),
			                            make_uniq<ArrowStringInfo>(ArrowVariableSizeType::SUPER_SIZE));
		}
		throw InvalidInputException("Arrow format \"%s\" not supported for queryfarm.geosilo", format.c_str());
	}

	// GEOMETRY type → Arrow schema metadata (with optional CRS)
	static void PopulateSchema(DuckDBArrowSchemaHolder &root_holder, ArrowSchema &schema, const LogicalType &type,
	                           ClientContext &context, const ArrowTypeExtension &extension) {
		ArrowSchemaMetadata schema_metadata;
		schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_EXTENSION_NAME, "queryfarm.geosilo");

		if (GeoType::HasCRS(type)) {
			auto &crs = GeoType::GetCRS(type);
			auto crs_id = crs.GetIdentifier();
			auto json_str = "{\"crs\":\"" + crs_id + "\"}";
			schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, json_str);
		} else {
			schema_metadata.AddOption(ArrowSchemaMetadata::ARROW_METADATA_KEY, "{}");
		}

		root_holder.metadata_info.emplace_back(schema_metadata.SerializeMetadata());
		schema.metadata = root_holder.metadata_info.back().get();

		const auto options = context.GetClientProperties();
		schema.format = (options.arrow_offset_size == ArrowOffsetSize::LARGE) ? "Z" : "z";
	}

	// Arrow → DuckDB: silo blob → GEOMETRY (direct, no WKB intermediate)
	static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		SiloReader reader;
		UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t blob) {
			return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
			                     static_cast<idx_t>(blob.GetSize()), result);
		});
	}

	// DuckDB → Arrow: GEOMETRY → silo blob (direct, no WKB intermediate)
	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		int64_t scale = DEFAULT_SCALE;
		if (source.GetType().id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(source.GetType())) {
			scale = ScaleFromCRS(GeoType::GetCRS(source.GetType()));
		}

		SiloWriter writer(scale);
		UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t geom) {
			return writer.Encode(reinterpret_cast<const uint8_t *>(geom.GetData()),
			                     static_cast<idx_t>(geom.GetSize()), result);
		});
	}
};

// ---------------------------------------------------------------------------
// Extension loading
// ---------------------------------------------------------------------------

static void LoadInternal(ExtensionLoader &loader) {
	QueryFarmSendTelemetry(loader, "geosilo", GeosiloExtension().Version());

	// silo_encode(GEOMETRY) → BLOB
	auto encode_fn = ScalarFunction("geosilo_encode", {LogicalType::GEOMETRY()}, LogicalType::BLOB,
	                                SiloEncodeFunction, SiloEncodeBind);
	loader.RegisterFunction(encode_fn);

	// silo_encode(GEOMETRY, BIGINT) → BLOB
	auto encode_scale_fn = ScalarFunction("geosilo_encode", {LogicalType::GEOMETRY(), LogicalType::BIGINT},
	                                      LogicalType::BLOB, SiloEncodeWithScaleFunction, SiloEncodeWithScaleBind);
	loader.RegisterFunction(encode_scale_fn);

	// silo_decode(BLOB) → GEOMETRY
	auto decode_fn = ScalarFunction("geosilo_decode", {LogicalType::BLOB}, LogicalType::GEOMETRY(), SiloDecodeFunction);
	loader.RegisterFunction(decode_fn);

	// silo_metadata(BLOB) → STRUCT{geometry_type VARCHAR, vertex_type VARCHAR, scale BIGINT}
	auto meta_type = LogicalType::STRUCT({
	    {"geometry_type", LogicalType::VARCHAR},
	    {"vertex_type", LogicalType::VARCHAR},
	    {"scale", LogicalType::BIGINT},
	});
	auto meta_fn = ScalarFunction("geosilo_metadata", {LogicalType::BLOB}, meta_type, SiloMetadataFunction);
	loader.RegisterFunction(meta_fn);

	// Register "queryfarm.geosilo" Arrow extension type
	// When Arrow IPC data arrives with ARROW:extension:name = "queryfarm.geosilo",
	// the BLOB data is automatically decoded to GEOMETRY.
	// When DuckDB exports GEOMETRY to Arrow with this extension, it encodes to silo blobs.
	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	try {
		config.RegisterArrowExtension(
		    {"queryfarm.geosilo", ArrowGeoSilo::PopulateSchema, ArrowGeoSilo::GetType,
		     make_shared_ptr<ArrowTypeExtensionData>(LogicalType::GEOMETRY(), LogicalType::BLOB,
		                                             ArrowGeoSilo::ArrowToDuck, ArrowGeoSilo::DuckToArrow)});
	} catch (...) {
		// Already registered (e.g., multiple LOAD calls)
	}
}

void GeosiloExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string GeosiloExtension::Name() {
	return "geosilo";
}

std::string GeosiloExtension::Version() const {
#ifdef EXT_VERSION_GEOSILO
	return EXT_VERSION_GEOSILO;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(geosilo, loader) {
	duckdb::LoadInternal(loader);
}
}
