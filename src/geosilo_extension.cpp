#define DUCKDB_EXTENSION_MAIN

#include "geosilo_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/geometry.hpp"
#include "duckdb/common/types/geometry_crs.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/string_vector.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/unified_vector_format.hpp"
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
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/cast/default_casts.hpp"
#include "duckdb/common/operator/cast_operators.hpp"

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
// Version history (full header only — compact headers always use latest):
//   v1 (0x01): Original encoding.
//   v2 (0x02): MULTIPOINT uses delta encoding between sub-points.
//   v3 (0x03): Polygon rings omit the closing vertex (reconstructed on decode).
//   The reader accepts v1, v2, and v3. Feature checks:
//     MULTIPOINT delta: version >= 2, or nested in a GEOMETRYCOLLECTION (always written delta-encoded)
//     Ring closure elimination: version >= 3
//   GEOMETRYCOLLECTIONs are written with a v2 header.
//
// Readers bounds-check every access: truncated or corrupt blobs raise an error, element counts are checked
// against the remaining bytes before use, and nesting is limited to Geometry::MAX_RECURSION_DEPTH.

static constexpr uint8_t GEOSILO_MAGIC      = 0x47; // 'G' — full header
static constexpr uint8_t GEOSILO_VERSION_1   = 0x01;
static constexpr uint8_t GEOSILO_VERSION_2   = 0x02; // + MULTIPOINT delta
static constexpr uint8_t GEOSILO_VERSION_3   = 0x03; // + ring closure elimination
static constexpr int64_t DEFAULT_SCALE       = 10000000; // 1e7
static constexpr int16_t DELTA_ESCAPE        = std::numeric_limits<int16_t>::min(); // -32768
static constexpr int32_t EMPTY_COORD         = std::numeric_limits<int32_t>::max();

// Single source for Version() and telemetry.
static constexpr const char *GEOSILO_EXTENSION_VERSION = "2026110701";

// ---------------------------------------------------------------------------
// Compact magic bytes (0x50 – 0x67)
// ---------------------------------------------------------------------------
// A single byte replaces the 12-byte full header by encoding geometry type,
// vertex type, and scale. Each geometry type gets 4 consecutive values:
//   base + 0 = XY,  scale 1e7
//   base + 1 = XYZ, scale 1e7
//   base + 2 = XY,  scale 100
//   base + 3 = XYZ, scale 100
//
// Compact headers always imply the latest encoding features for that type
// (MULTIPOINT delta, ring closure elimination, etc.).

static constexpr uint8_t COMPACT_MAGIC_BASE  = 0x50;
static constexpr uint8_t COMPACT_MAGIC_MAX   = 0x67;
// Per-type bases (for readability):
static constexpr uint8_t COMPACT_POINT_BASE          = 0x50; // 0x50–0x53
static constexpr uint8_t COMPACT_LINESTRING_BASE     = 0x54; // 0x54–0x57
static constexpr uint8_t COMPACT_POLYGON_BASE        = 0x58; // 0x58–0x5B
static constexpr uint8_t COMPACT_MULTIPOINT_BASE     = 0x5C; // 0x5C–0x5F
static constexpr uint8_t COMPACT_MULTILINESTRING_BASE = 0x60; // 0x60–0x63
static constexpr uint8_t COMPACT_MULTIPOLYGON_BASE   = 0x64; // 0x64–0x67

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
// GEOSILO type (BLOB alias with CRS + scale in ExtensionTypeInfo)
// ---------------------------------------------------------------------------

struct GeoSiloType {
	// Bind function: GEOSILO or GEOSILO('EPSG:4326')
	static LogicalType Bind(BindLogicalTypeInput &input) {
		auto &modifiers = input.modifiers;
		if (modifiers.empty()) {
			return GetDefault();
		}
		if (modifiers.size() != 1) {
			throw BinderException("GEOSILO type accepts 0 or 1 arguments (CRS identifier)");
		}
		if (modifiers[0].GetType() != LogicalType::VARCHAR) {
			throw BinderException("GEOSILO CRS argument must be a string (e.g., 'EPSG:4326')");
		}
		if (!modifiers[0].IsNotNull()) {
			throw BinderException("GEOSILO CRS argument cannot be NULL");
		}
		auto crs_string = modifiers[0].GetValue().GetValue<string>();
		// Validate CRS and detect scale
		if (input.context) {
			auto crs = CoordinateReferenceSystem::TryIdentify(*input.context, crs_string);
			if (crs) {
				return Get(crs_string, ScaleFromCRS(*crs));
			}
		}
		// Fall back: parse EPSG code directly for scale detection
		int64_t scale = DEFAULT_SCALE;
		int epsg = 0;
		if (crs_string.rfind("EPSG:", 0) == 0) {
			try { epsg = std::stoi(crs_string.substr(5)); } catch (...) {}
		}
		if (epsg == 4326 || epsg == 4269 || epsg == 4267) scale = 10000000;
		else if ((epsg >= 32601 && epsg <= 32660) || (epsg >= 32701 && epsg <= 32760)) scale = 100;
		else if (epsg == 3857) scale = 100;
		return Get(crs_string, scale);
	}

	static LogicalType Get(const string &crs, int64_t scale) {
		auto info = make_uniq<ExtensionTypeInfo>();
		info->modifiers.emplace_back(Value(crs));
		info->modifiers.emplace_back(Value::BIGINT(scale));
		return LogicalType(LogicalTypeId::BLOB).WithAlias("GEOSILO").WithExtensionInfo(std::move(info));
	}

	static LogicalType GetDefault() {
		return LogicalType(LogicalTypeId::BLOB).WithAlias("GEOSILO");
	}

	static bool IsGeoSilo(const LogicalType &type) {
		return type.HasAlias() && type.GetAlias() == "GEOSILO";
	}

	static string GetCRS(const LogicalType &type) {
		if (!type.HasExtensionInfo()) return "";
		auto &mods = type.GetExtensionInfo()->modifiers;
		if (mods.empty() || mods[0].value.IsNull()) return "";
		return mods[0].value.GetValue<string>();
	}

	static int64_t GetScale(const LogicalType &type) {
		if (!type.HasExtensionInfo()) return DEFAULT_SCALE;
		auto &mods = type.GetExtensionInfo()->modifiers;
		if (mods.size() < 2 || mods[1].value.IsNull()) return DEFAULT_SCALE;
		return mods[1].value.GetValue<int64_t>();
	}
};

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
// Shared helpers
// ---------------------------------------------------------------------------

// GEOSILO stores coordinates as int32 and deltas between consecutive coordinates. A delta can exceed the
// int32 range (e.g. -179 -> 179 degrees at scale 1e7), so deltas are computed and applied with wrapping
// (modulo 2^32) arithmetic. This keeps the round trip exact and avoids signed-overflow UB; for deltas that
// fit in int32 the encoding is byte-for-byte identical to the original format.
static inline int32_t WrapSub(int32_t a, int32_t b) {
	uint32_t r = static_cast<uint32_t>(a) - static_cast<uint32_t>(b);
	int32_t out;
	memcpy(&out, &r, sizeof(out));
	return out;
}

static inline int32_t WrapAdd(int32_t a, int32_t b) {
	uint32_t r = static_cast<uint32_t>(a) + static_cast<uint32_t>(b);
	int32_t out;
	memcpy(&out, &r, sizeof(out));
	return out;
}

// DuckDB's GEOMETRY readers refuse nesting deeper than this, so GEOSILO enforces the same bound on both
// encode and decode. It also caps recursion when walking untrusted blobs.
static constexpr idx_t GEOSILO_MAX_DEPTH = Geometry::MAX_RECURSION_DEPTH;

// Multi-geometry parts sit one level below their parent in WKB, so they count against the limit too.
static inline bool ExceedsDepth(GeometryType type, idx_t depth) {
	bool has_part_level = type == GeometryType::MULTIPOINT || type == GeometryType::MULTILINESTRING ||
	                      type == GeometryType::MULTIPOLYGON;
	return depth >= GEOSILO_MAX_DEPTH || (has_part_level && depth + 1 >= GEOSILO_MAX_DEPTH);
}

// MULTIPOINTs are delta-encoded from v2 on. Nested inside a GEOMETRYCOLLECTION they have always been written
// delta-encoded, even though collections carried a v1 header until 2026110701, so nested MULTIPOINTs are
// always read as delta-encoded (this also fixes decoding of collections written by earlier releases).
static inline bool MultiPointIsDelta(uint8_t version, idx_t depth) {
	return version >= GEOSILO_VERSION_2 || depth > 0;
}

static inline uint32_t VertexWidth(VertexType vert_type) {
	return 2 + ((vert_type == VertexType::XYZ || vert_type == VertexType::XYZM) ? 1 : 0) +
	       ((vert_type == VertexType::XYM || vert_type == VertexType::XYZM) ? 1 : 0);
}

static inline bool IsValidGeometryType(uint32_t type) {
	return type >= static_cast<uint32_t>(GeometryType::POINT) &&
	       type <= static_cast<uint32_t>(GeometryType::GEOMETRYCOLLECTION);
}

// ISO WKB type code (DuckDB's internal GEOMETRY representation): type + 1000 (Z) + 2000 (M).
static inline uint32_t IsoWkbType(GeometryType type, uint32_t vw, bool has_m) {
	bool has_z = (vw == 4) || (vw == 3 && !has_m);
	return static_cast<uint32_t>(type) + (has_z ? 1000 : 0) + (has_m ? 2000 : 0);
}

// Growable output buffer used by both the encoder and the decoder.
class ByteBuffer {
public:
	void Reset(idx_t reserve) {
		if (buf_.size() < reserve) {
			buf_.resize(reserve);
		}
		pos_ = 0;
	}
	void Ensure(idx_t n) {
		if (pos_ + n > buf_.size()) {
			buf_.resize(MaxValue<idx_t>(buf_.size() * 2, pos_ + n));
		}
	}
	void Byte(uint8_t b) {
		Ensure(1);
		buf_[pos_++] = b;
	}
	template <class T>
	void Put(T v) {
		Ensure(sizeof(T));
		memcpy(buf_.data() + pos_, &v, sizeof(T));
		pos_ += sizeof(T);
	}
	// Unchecked variant, only after Ensure() reserved the space.
	template <class T>
	void PutUnchecked(T v) {
		memcpy(buf_.data() + pos_, &v, sizeof(T));
		pos_ += sizeof(T);
	}
	const char *Data() const {
		return reinterpret_cast<const char *>(buf_.data());
	}
	idx_t Size() const {
		return pos_;
	}

private:
	vector<uint8_t> buf_;
	idx_t pos_ = 0;
};

// Bounds-checked little-endian reader over an untrusted byte range.
class ByteCursor {
public:
	ByteCursor(const uint8_t *data, idx_t size, const char *what) : ptr_(data), end_(data + size), what_(what) {
	}
	idx_t Remaining() const {
		return static_cast<idx_t>(end_ - ptr_);
	}
	void Need(idx_t n) const {
		if (Remaining() < n) {
			throw InvalidInputException("%s: input is truncated or malformed", what_);
		}
	}
	uint8_t U8() {
		Need(1);
		return *ptr_++;
	}
	template <class T>
	T Get() {
		Need(sizeof(T));
		T v;
		memcpy(&v, ptr_, sizeof(T));
		ptr_ += sizeof(T);
		return v;
	}
	void Skip(idx_t n) {
		Need(n);
		ptr_ += n;
	}
	// Reads an element count and rejects counts that cannot possibly fit in the remaining bytes, so a
	// corrupt count can never drive a huge allocation or a long loop.
	uint32_t Count(idx_t min_bytes_per_element) {
		auto n = Get<uint32_t>();
		if (min_bytes_per_element > 0 && static_cast<idx_t>(n) > Remaining() / min_bytes_per_element) {
			throw InvalidInputException("%s: element count %u exceeds the remaining %llu bytes", what_, n,
			                            static_cast<unsigned long long>(Remaining()));
		}
		return n;
	}

private:
	const uint8_t *ptr_;
	const uint8_t *end_;
	const char *what_;
};

// ---------------------------------------------------------------------------
// Header parsing (shared by every reader of GEOSILO bytes)
// ---------------------------------------------------------------------------

struct SiloHeader {
	GeometryType geom_type = GeometryType::INVALID;
	VertexType vert_type = VertexType::XY;
	int64_t scale = DEFAULT_SCALE;
	double dbl_scale = static_cast<double>(DEFAULT_SCALE); // coordinates are stored as round(value * scale)
	uint8_t version = GEOSILO_VERSION_3;
	idx_t body_offset = 0;
	bool compact = false;
};

// Decode a compact magic byte into geometry type, vertex type, and scale.
static void ResolveCompactMagic(uint8_t magic, SiloHeader &header) {
	static constexpr GeometryType type_table[] = {GeometryType::POINT,      GeometryType::LINESTRING,
	                                              GeometryType::POLYGON,    GeometryType::MULTIPOINT,
	                                              GeometryType::MULTILINESTRING, GeometryType::MULTIPOLYGON};
	uint8_t offset = magic - COMPACT_MAGIC_BASE;
	uint8_t variant = offset % 4; // 0=XY/1e7, 1=XYZ/1e7, 2=XY/100, 3=XYZ/100
	header.geom_type = type_table[offset / 4];
	header.vert_type = (variant & 1) ? VertexType::XYZ : VertexType::XY;
	header.scale = (variant >= 2) ? 100 : 10000000;
	header.dbl_scale = static_cast<double>(header.scale);
	header.version = GEOSILO_VERSION_3;
	header.body_offset = 1;
	header.compact = true;
}

// Parses and validates the GEOSILO header. Returns an empty string on success, otherwise the error.
static string TryParseSiloHeader(const uint8_t *data, idx_t size, SiloHeader &header) {
	if (size < 1) {
		return "GeoSilo: blob is empty";
	}
	uint8_t magic = data[0];
	if (magic >= COMPACT_MAGIC_BASE && magic <= COMPACT_MAGIC_MAX) {
		ResolveCompactMagic(magic, header);
		return string();
	}
	if (magic != GEOSILO_MAGIC) {
		return StringUtil::Format("GeoSilo: invalid magic byte 0x%02x", magic);
	}
	if (size < 12) {
		return "GeoSilo: blob too small for a GEOSILO header";
	}
	uint8_t version = data[1];
	if (version < GEOSILO_VERSION_1 || version > GEOSILO_VERSION_3) {
		return StringUtil::Format("GeoSilo: unsupported version %d", static_cast<int>(version));
	}
	uint8_t geom_type = data[2];
	if (!IsValidGeometryType(geom_type)) {
		return StringUtil::Format("GeoSilo: invalid geometry type %d", static_cast<int>(geom_type));
	}
	uint8_t vert_type = data[3];
	if (vert_type > static_cast<uint8_t>(VertexType::XYZM)) {
		return StringUtil::Format("GeoSilo: invalid vertex type %d", static_cast<int>(vert_type));
	}
	int64_t scale;
	memcpy(&scale, data + 4, 8);
	if (scale <= 0) {
		return "GeoSilo: invalid scale in header";
	}
	header.version = version;
	header.geom_type = static_cast<GeometryType>(geom_type);
	header.vert_type = static_cast<VertexType>(vert_type);
	header.scale = scale;
	header.dbl_scale = static_cast<double>(scale);
	header.body_offset = 12;
	header.compact = false;
	return string();
}

static SiloHeader ParseSiloHeader(const uint8_t *data, idx_t size) {
	SiloHeader header;
	auto error = TryParseSiloHeader(data, size, header);
	if (!error.empty()) {
		throw InvalidInputException(error);
	}
	return header;
}

// ---------------------------------------------------------------------------
// Writer: GEOMETRY (DuckDB's internal ISO WKB) → GeoSilo blob
// ---------------------------------------------------------------------------

class SiloWriter {
public:
	explicit SiloWriter(int64_t scale) {
		SetScale(scale);
	}

	void SetScale(int64_t scale) {
		if (scale <= 0) {
			throw InvalidInputException("geosilo_encode: scale must be a positive integer, got %lld",
			                            static_cast<long long>(scale));
		}
		scale_ = scale;
		dbl_scale_ = static_cast<double>(scale);
	}

	string_t Encode(const uint8_t *data, idx_t size, Vector &result) {
		ByteCursor in(data, size, "geosilo_encode");
		in_ = &in;

		uint32_t vw;
		bool has_m;
		auto geom_type = ReadWkbHeader(vw, has_m);
		vw_ = vw;
		VertexType vert_type = VertexType::XY;
		if (vw == 4) {
			vert_type = VertexType::XYZM;
		} else if (vw == 3) {
			vert_type = has_m ? VertexType::XYM : VertexType::XYZ;
		}

		out_.Reset(size + 16);

		// Try compact encoding: single magic byte replaces the 12-byte full header.
		uint8_t compact_magic = TryCompactMagic(geom_type, vert_type);
		if (compact_magic) {
			out_.Byte(compact_magic);
			ring_closure_ = (geom_type == GeometryType::POLYGON || geom_type == GeometryType::MULTIPOLYGON);
		} else {
			// Full header — version selects encoding features
			uint8_t version = GEOSILO_VERSION_1;
			// v2 marks delta-encoded MULTIPOINTs, including those nested in a GEOMETRYCOLLECTION.
			if (geom_type == GeometryType::MULTIPOINT || geom_type == GeometryType::GEOMETRYCOLLECTION) {
				version = GEOSILO_VERSION_2;
			}
			if (geom_type == GeometryType::POLYGON || geom_type == GeometryType::MULTIPOLYGON) {
				version = GEOSILO_VERSION_3;
			}
			ring_closure_ = (version >= GEOSILO_VERSION_3);
			out_.Byte(GEOSILO_MAGIC);
			out_.Byte(version);
			out_.Byte(static_cast<uint8_t>(geom_type));
			out_.Byte(static_cast<uint8_t>(vert_type));
			out_.Put<int64_t>(scale_);
		}

		EncodeGeometry(geom_type, 0);
		in_ = nullptr;
		return StringVector::AddStringOrBlob(result, out_.Data(), out_.Size());
	}

private:
	// Reads a WKB geometry header (byte order + type). Accepts ISO (1000/2000/3000) and EWKB flag encodings.
	GeometryType ReadWkbHeader(uint32_t &vw, bool &has_m) {
		auto byte_order = in_->U8();
		if (byte_order != 1) {
			throw InvalidInputException("geosilo_encode: only little-endian WKB is supported");
		}
		auto meta = in_->Get<uint32_t>();
		auto type_id = (meta & 0x0000FFFF) % 1000;
		auto flag_id = (meta & 0x0000FFFF) / 1000;
		bool has_z = (flag_id & 0x01) || (meta & 0x80000000);
		has_m = (flag_id & 0x02) || (meta & 0x40000000);
		if (!IsValidGeometryType(type_id) || flag_id > 3) {
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(meta & 0xFFFF));
		}
		vw = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);
		return static_cast<GeometryType>(type_id);
	}

	void SkipChildHeader() {
		uint32_t child_vw;
		bool child_m;
		ReadWkbHeader(child_vw, child_m);
	}

	// Returns a compact magic byte for well-known type/vertex/scale combos, else 0.
	uint8_t TryCompactMagic(GeometryType type, VertexType vert_type) {
		if (vert_type == VertexType::XYM || vert_type == VertexType::XYZM) {
			return 0;
		}
		uint8_t type_base;
		switch (type) {
		case GeometryType::POINT:
			type_base = COMPACT_POINT_BASE;
			break;
		case GeometryType::LINESTRING:
			type_base = COMPACT_LINESTRING_BASE;
			break;
		case GeometryType::POLYGON:
			type_base = COMPACT_POLYGON_BASE;
			break;
		case GeometryType::MULTIPOINT:
			type_base = COMPACT_MULTIPOINT_BASE;
			break;
		case GeometryType::MULTILINESTRING:
			type_base = COMPACT_MULTILINESTRING_BASE;
			break;
		case GeometryType::MULTIPOLYGON:
			type_base = COMPACT_MULTIPOLYGON_BASE;
			break;
		default:
			return 0;
		}
		uint8_t offset = (vert_type == VertexType::XYZ) ? 1 : 0;
		if (scale_ == 10000000) {
			return type_base + offset;
		}
		if (scale_ == 100) {
			return type_base + 2 + offset;
		}
		return 0;
	}

	int32_t ScaleCoord(double val) {
		if (std::isnan(val)) {
			return EMPTY_COORD;
		}
		double scaled = std::round(val * dbl_scale_);
		// INT32_MAX is reserved as the empty-coordinate sentinel.
		if (!(scaled >= static_cast<double>(std::numeric_limits<int32_t>::min()) &&
		      scaled < static_cast<double>(std::numeric_limits<int32_t>::max()))) {
			throw InvalidInputException(
			    "GeoSilo: coordinate %.7f overflows int32 at scale %lld. Use a smaller scale.", val,
			    static_cast<long long>(scale_));
		}
		return static_cast<int32_t>(scaled);
	}

	void WriteDelta(int32_t delta) {
		if (delta > std::numeric_limits<int16_t>::max() || delta <= std::numeric_limits<int16_t>::min()) {
			out_.Put<int16_t>(DELTA_ESCAPE);
			out_.Put<int32_t>(delta);
		} else {
			out_.Put<int16_t>(static_cast<int16_t>(delta));
		}
	}

	// Writes `count` vertices from the input: the first absolute, the rest as deltas.
	void EncodeVertices(uint32_t count) {
		if (count == 0) {
			return;
		}
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t d = 0; d < vw_; d++) {
			prev[d] = ScaleCoord(in_->Get<double>());
			out_.Put<int32_t>(prev[d]);
		}
		for (uint32_t i = 1; i < count; i++) {
			for (uint32_t d = 0; d < vw_; d++) {
				int32_t scaled = ScaleCoord(in_->Get<double>());
				WriteDelta(WrapSub(scaled, prev[d]));
				prev[d] = scaled;
			}
		}
	}

	void EncodeCoordinateSequence() {
		auto num_points = in_->Count(8 * vw_);
		out_.Put<uint32_t>(num_points);
		EncodeVertices(num_points);
	}

	// Encode a polygon ring, omitting the closing vertex (which equals the first).
	// The WKB ring has num_points including closure; we store num_points - 1.
	void EncodeRing() {
		auto num_points = in_->Count(8 * vw_);
		if (num_points <= 1) {
			// Degenerate ring: store it as-is (a 1-point ring decodes as that point plus its closure).
			out_.Put<uint32_t>(num_points);
			EncodeVertices(num_points);
			return;
		}
		out_.Put<uint32_t>(num_points - 1);
		EncodeVertices(num_points - 1);
		in_->Skip(8 * vw_); // the closing vertex equals the first
	}

	void EncodePolygon() {
		auto num_rings = in_->Count(4);
		out_.Put<uint32_t>(num_rings);
		for (uint32_t r = 0; r < num_rings; r++) {
			if (ring_closure_) {
				EncodeRing();
			} else {
				EncodeCoordinateSequence();
			}
		}
	}

	// Delta-encode MULTIPOINT: like a coordinate sequence but skipping WKB sub-headers.
	void EncodeMultiPointDelta() {
		auto num_parts = in_->Count(5 + 8 * vw_);
		out_.Put<uint32_t>(num_parts);
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t p = 0; p < num_parts; p++) {
			SkipChildHeader();
			for (uint32_t d = 0; d < vw_; d++) {
				int32_t scaled = ScaleCoord(in_->Get<double>());
				if (p == 0) {
					out_.Put<int32_t>(scaled);
				} else {
					WriteDelta(WrapSub(scaled, prev[d]));
				}
				prev[d] = scaled;
			}
		}
	}

	void EncodeGeometry(GeometryType type, idx_t depth) {
		if (depth >= GEOSILO_MAX_DEPTH) {
			throw InvalidInputException("GeoSilo: geometry exceeds the maximum nesting depth of %llu",
			                            static_cast<unsigned long long>(GEOSILO_MAX_DEPTH));
		}
		switch (type) {
		case GeometryType::POINT:
			for (uint32_t d = 0; d < vw_; d++) {
				out_.Put<int32_t>(ScaleCoord(in_->Get<double>()));
			}
			break;
		case GeometryType::LINESTRING:
			EncodeCoordinateSequence();
			break;
		case GeometryType::POLYGON:
			EncodePolygon();
			break;
		case GeometryType::MULTIPOINT:
			EncodeMultiPointDelta();
			break;
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			auto num_parts = in_->Count(9);
			out_.Put<uint32_t>(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				SkipChildHeader();
				if (type == GeometryType::MULTILINESTRING) {
					EncodeCoordinateSequence();
				} else {
					EncodePolygon();
				}
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			auto num_parts = in_->Count(9);
			out_.Put<uint32_t>(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				uint32_t child_vw;
				bool child_m;
				auto child_type = ReadWkbHeader(child_vw, child_m);
				if (child_vw != vw_) {
					throw InvalidInputException("GeoSilo: geometry has inconsistent Z/M dimensions");
				}
				out_.Byte(static_cast<uint8_t>(child_type));
				EncodeGeometry(child_type, depth + 1);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	int64_t scale_ = DEFAULT_SCALE;
	double dbl_scale_ = static_cast<double>(DEFAULT_SCALE);
	bool ring_closure_ = false; // true = omit closing vertex in polygon rings
	uint32_t vw_ = 2;
	ByteCursor *in_ = nullptr;
	ByteBuffer out_;
};

// ---------------------------------------------------------------------------
// SiloWalker: walk an (untrusted) silo blob calling a visitor per XY vertex
// ---------------------------------------------------------------------------
// No WKB output. Used for bbox, area, length, point counts and validation. Every read is bounds-checked,
// counts are validated against the remaining bytes and nesting depth is capped. The Visitor provides:
//   void OnPartStart(SiloPart kind)          — start of a point / linestring / polygon ring
//   void OnVertex(int32_t x, int32_t y)      — every vertex (implicit ring closures are visited too)
//   void OnPartEnd()                         — end of the part

enum class SiloPart : uint8_t { POINTS, LINE, EXTERIOR_RING, INTERIOR_RING };

template <typename Visitor>
class SiloWalker {
public:
	SiloWalker(const uint8_t *data, idx_t size, Visitor &visitor)
	    : header_(ParseSiloHeader(data, size)),
	      cur_(data + header_.body_offset, size - header_.body_offset, "GeoSilo"), visitor_(visitor) {
		vw_ = VertexWidth(header_.vert_type);
		ring_closure_ = header_.version >= GEOSILO_VERSION_3;
	}

	void Walk() {
		WalkGeometry(header_.geom_type, 0);
	}

	const SiloHeader &header() const {
		return header_;
	}
	double dbl_scale() const {
		return header_.dbl_scale;
	}
	idx_t Remaining() const {
		return cur_.Remaining();
	}

private:
	int32_t Delta() {
		auto d = cur_.Get<int16_t>();
		if (d == DELTA_ESCAPE) {
			return cur_.Get<int32_t>();
		}
		return static_cast<int32_t>(d);
	}

	void WalkSequence(SiloPart kind, bool implicit_closure) {
		auto n = cur_.Count(2 * vw_);
		if (n == 0) {
			return;
		}
		visitor_.OnPartStart(kind);
		int32_t first_x = cur_.Get<int32_t>();
		int32_t first_y = cur_.Get<int32_t>();
		cur_.Skip(4 * (vw_ - 2));
		int32_t x = first_x, y = first_y;
		if (kind != SiloPart::POINTS || x != EMPTY_COORD) {
			visitor_.OnVertex(x, y);
		}
		for (uint32_t i = 1; i < n; i++) {
			x = WrapAdd(x, Delta());
			y = WrapAdd(y, Delta());
			for (uint32_t d = 2; d < vw_; d++) {
				Delta();
			}
			if (kind != SiloPart::POINTS || x != EMPTY_COORD) {
				visitor_.OnVertex(x, y);
			}
		}
		if (implicit_closure) {
			visitor_.OnVertex(first_x, first_y);
		}
		visitor_.OnPartEnd();
	}

	void WalkPolygon() {
		auto num_rings = cur_.Count(4);
		for (uint32_t r = 0; r < num_rings; r++) {
			WalkSequence(r == 0 ? SiloPart::EXTERIOR_RING : SiloPart::INTERIOR_RING, ring_closure_);
		}
	}

	void WalkPoint() {
		int32_t x = cur_.Get<int32_t>();
		int32_t y = cur_.Get<int32_t>();
		cur_.Skip(4 * (vw_ - 2));
		if (x != EMPTY_COORD) {
			visitor_.OnPartStart(SiloPart::POINTS);
			visitor_.OnVertex(x, y);
			visitor_.OnPartEnd();
		}
	}

	void WalkGeometry(GeometryType type, idx_t depth) {
		if (ExceedsDepth(type, depth)) {
			throw InvalidInputException("GeoSilo: geometry exceeds the maximum nesting depth of %llu",
			                            static_cast<unsigned long long>(GEOSILO_MAX_DEPTH));
		}
		switch (type) {
		case GeometryType::POINT:
			WalkPoint();
			break;
		case GeometryType::LINESTRING:
			WalkSequence(SiloPart::LINE, false);
			break;
		case GeometryType::POLYGON:
			WalkPolygon();
			break;
		case GeometryType::MULTIPOINT:
			if (MultiPointIsDelta(header_.version, depth)) {
				WalkSequence(SiloPart::POINTS, false);
			} else {
				auto n = cur_.Count(4 * vw_);
				for (uint32_t p = 0; p < n; p++) {
					WalkPoint();
				}
			}
			break;
		case GeometryType::MULTILINESTRING: {
			auto n = cur_.Count(4);
			for (uint32_t p = 0; p < n; p++) {
				WalkSequence(SiloPart::LINE, false);
			}
			break;
		}
		case GeometryType::MULTIPOLYGON: {
			auto n = cur_.Count(4);
			for (uint32_t p = 0; p < n; p++) {
				WalkPolygon();
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			auto n = cur_.Count(5);
			for (uint32_t p = 0; p < n; p++) {
				auto child_type = cur_.U8();
				if (!IsValidGeometryType(child_type)) {
					throw InvalidInputException("GeoSilo: invalid geometry type %d in collection",
					                            static_cast<int>(child_type));
				}
				WalkGeometry(static_cast<GeometryType>(child_type), depth + 1);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	SiloHeader header_;
	ByteCursor cur_;
	Visitor &visitor_;
	uint32_t vw_ = 2;
	bool ring_closure_ = false;
};

// --- Visitor implementations ---

struct NullVisitor {
	void OnPartStart(SiloPart) {
	}
	void OnVertex(int32_t, int32_t) {
	}
	void OnPartEnd() {
	}
};

struct CountVisitor {
	int64_t count = 0;
	void OnPartStart(SiloPart) {
	}
	void OnVertex(int32_t, int32_t) {
		count++;
	}
	void OnPartEnd() {
	}
};

struct BBoxVisitor {
	int32_t xmin = std::numeric_limits<int32_t>::max();
	int32_t xmax = std::numeric_limits<int32_t>::min();
	int32_t ymin = std::numeric_limits<int32_t>::max();
	int32_t ymax = std::numeric_limits<int32_t>::min();
	bool has_points = false;

	void OnPartStart(SiloPart) {
	}
	void OnVertex(int32_t x, int32_t y) {
		xmin = MinValue(xmin, x);
		xmax = MaxValue(xmax, x);
		ymin = MinValue(ymin, y);
		ymax = MaxValue(ymax, y);
		has_points = true;
	}
	void OnPartEnd() {
	}
};

// Planar area of the polygonal parts: |exterior| - sum(|holes|) per polygon, summed over all polygons.
// Points and linestrings contribute 0 (matching the spatial extension). The shoelace sum is computed on
// coordinates relative to the ring's first vertex in double precision, so it cannot overflow.
struct AreaVisitor {
	double total = 0.0; // in scaled units squared, times 2
	double ring = 0.0;
	int64_t origin_x = 0, origin_y = 0;
	double prev_dx = 0.0, prev_dy = 0.0;
	bool active = false;
	bool first = false;
	bool hole = false;

	void OnPartStart(SiloPart kind) {
		active = (kind == SiloPart::EXTERIOR_RING || kind == SiloPart::INTERIOR_RING);
		hole = (kind == SiloPart::INTERIOR_RING);
		ring = 0.0;
		first = true;
	}
	void OnVertex(int32_t x, int32_t y) {
		if (!active) {
			return;
		}
		if (first) {
			origin_x = x;
			origin_y = y;
			prev_dx = prev_dy = 0.0;
			first = false;
			return;
		}
		double dx = static_cast<double>(static_cast<int64_t>(x) - origin_x);
		double dy = static_cast<double>(static_cast<int64_t>(y) - origin_y);
		ring += prev_dx * dy - dx * prev_dy;
		prev_dx = dx;
		prev_dy = dy;
	}
	void OnPartEnd() {
		if (!active) {
			return;
		}
		// Closing edge back to the origin contributes prev x 0 - 0 x prev = 0.
		total += hole ? -std::abs(ring) : std::abs(ring);
		active = false;
	}
};

// Sum of segment lengths over the parts selected by `Polygonal` (rings for perimeter, linestrings for length).
template <bool POLYGONAL>
struct LengthVisitor {
	double total_length = 0.0;
	int32_t prev_x = 0, prev_y = 0;
	bool has_prev = false;
	bool active = false;

	void OnPartStart(SiloPart kind) {
		bool ring = (kind == SiloPart::EXTERIOR_RING || kind == SiloPart::INTERIOR_RING);
		active = POLYGONAL ? ring : (kind == SiloPart::LINE);
		has_prev = false;
	}
	void OnVertex(int32_t x, int32_t y) {
		if (!active) {
			return;
		}
		if (has_prev) {
			double dx = static_cast<double>(x) - static_cast<double>(prev_x);
			double dy = static_cast<double>(y) - static_cast<double>(prev_y);
			total_length += std::sqrt(dx * dx + dy * dy);
		}
		prev_x = x;
		prev_y = y;
		has_prev = true;
	}
	void OnPartEnd() {
		has_prev = false;
		active = false;
	}
};

// Fully validates a GEOSILO blob (header, structure, counts, nesting, no trailing bytes).
// Returns an empty string when valid, otherwise the error message.
static string ValidateSilo(const uint8_t *data, idx_t size) {
	SiloHeader header;
	auto error = TryParseSiloHeader(data, size, header);
	if (!error.empty()) {
		return error;
	}
	try {
		NullVisitor vis;
		SiloWalker<NullVisitor> walker(data, size, vis);
		walker.Walk();
		if (walker.Remaining() != 0) {
			return StringUtil::Format("GeoSilo: %llu unexpected trailing bytes",
			                          static_cast<unsigned long long>(walker.Remaining()));
		}
	} catch (std::exception &ex) {
		ErrorData err(ex);
		return err.RawMessage();
	}
	return string();
}

// ---------------------------------------------------------------------------
// Reader: GeoSilo blob → GEOMETRY (DuckDB's internal ISO WKB)
// ---------------------------------------------------------------------------

class SiloReader {
public:
	string_t Decode(const uint8_t *data, idx_t size, Vector &result) {
		auto header = ParseSiloHeader(data, size);
		ByteCursor in(data + header.body_offset, size - header.body_offset, "GeoSilo");
		in_ = &in;
		dbl_scale_ = header.dbl_scale;
		version_ = header.version;
		has_m_ = (header.vert_type == VertexType::XYM || header.vert_type == VertexType::XYZM);
		vw_ = VertexWidth(header.vert_type);

		out_.Reset(MinValue<idx_t>(size * 5 + 64, 1 << 20));
		WriteWkbHeader(header.geom_type);
		DecodeGeometry(header.geom_type, 0);
		in_ = nullptr;
		return StringVector::AddStringOrBlob(result, out_.Data(), out_.Size());
	}

private:
	int32_t Delta() {
		auto d = in_->Get<int16_t>();
		if (d == DELTA_ESCAPE) {
			return in_->Get<int32_t>();
		}
		return static_cast<int32_t>(d);
	}

	double Unscale(int32_t val) const {
		if (val == EMPTY_COORD) {
			return std::numeric_limits<double>::quiet_NaN();
		}
		// Divide (rather than multiply by 1/scale) so values with <= log10(scale) decimals round-trip exactly.
		return static_cast<double>(val) / dbl_scale_;
	}

	void WriteWkbHeader(GeometryType type) {
		out_.Byte(0x01); // little-endian
		out_.Put<uint32_t>(IsoWkbType(type, vw_, has_m_));
	}

	// Decodes `count` vertices (first absolute, the rest deltas); optionally re-emits the first as closure.
	void DecodeVertices(uint32_t count, bool emit_closure) {
		if (count == 0) {
			return;
		}
		out_.Ensure((static_cast<idx_t>(count) + 1) * 8 * vw_);
		int32_t first[4] = {0, 0, 0, 0};
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t d = 0; d < vw_; d++) {
			first[d] = prev[d] = in_->Get<int32_t>();
			out_.PutUnchecked<double>(Unscale(prev[d]));
		}
		for (uint32_t i = 1; i < count; i++) {
			for (uint32_t d = 0; d < vw_; d++) {
				prev[d] = WrapAdd(prev[d], Delta());
				out_.PutUnchecked<double>(Unscale(prev[d]));
			}
		}
		if (emit_closure) {
			for (uint32_t d = 0; d < vw_; d++) {
				out_.PutUnchecked<double>(Unscale(first[d]));
			}
		}
	}

	void DecodeCoordinateSequence() {
		auto num_points = in_->Count(2 * vw_);
		out_.Put<uint32_t>(num_points);
		DecodeVertices(num_points, false);
	}

	// Decode a polygon ring where the closing vertex was omitted: stored N-1, emit N (closure = first).
	void DecodeRing() {
		auto stored_count = in_->Count(2 * vw_);
		out_.Put<uint32_t>(stored_count == 0 ? 0 : stored_count + 1);
		DecodeVertices(stored_count, true);
	}

	void DecodePolygon() {
		auto num_rings = in_->Count(4);
		out_.Put<uint32_t>(num_rings);
		for (uint32_t r = 0; r < num_rings; r++) {
			if (version_ >= GEOSILO_VERSION_3) {
				DecodeRing();
			} else {
				DecodeCoordinateSequence();
			}
		}
	}

	void DecodePoint() {
		out_.Ensure(8 * vw_);
		for (uint32_t d = 0; d < vw_; d++) {
			out_.PutUnchecked<double>(Unscale(in_->Get<int32_t>()));
		}
	}

	void DecodeGeometry(GeometryType type, idx_t depth) {
		if (ExceedsDepth(type, depth)) {
			throw InvalidInputException("GeoSilo: geometry exceeds the maximum nesting depth of %llu",
			                            static_cast<unsigned long long>(GEOSILO_MAX_DEPTH));
		}
		switch (type) {
		case GeometryType::POINT:
			DecodePoint();
			break;
		case GeometryType::LINESTRING:
			DecodeCoordinateSequence();
			break;
		case GeometryType::POLYGON:
			DecodePolygon();
			break;
		case GeometryType::MULTIPOINT: {
			if (MultiPointIsDelta(version_, depth)) {
				// Delta-encoded points: reverse of EncodeMultiPointDelta.
				auto num_parts = in_->Count(2 * vw_);
				out_.Put<uint32_t>(num_parts);
				int32_t prev[4] = {0, 0, 0, 0};
				for (uint32_t p = 0; p < num_parts; p++) {
					WriteWkbHeader(GeometryType::POINT);
					out_.Ensure(8 * vw_);
					for (uint32_t d = 0; d < vw_; d++) {
						prev[d] = (p == 0) ? in_->Get<int32_t>() : WrapAdd(prev[d], Delta());
						out_.PutUnchecked<double>(Unscale(prev[d]));
					}
				}
			} else {
				// v1: absolute coords per point
				auto num_parts = in_->Count(4 * vw_);
				out_.Put<uint32_t>(num_parts);
				for (uint32_t p = 0; p < num_parts; p++) {
					WriteWkbHeader(GeometryType::POINT);
					DecodePoint();
				}
			}
			break;
		}
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			auto num_parts = in_->Count(4);
			out_.Put<uint32_t>(num_parts);
			auto inner =
			    (type == GeometryType::MULTILINESTRING) ? GeometryType::LINESTRING : GeometryType::POLYGON;
			for (uint32_t p = 0; p < num_parts; p++) {
				WriteWkbHeader(inner);
				if (inner == GeometryType::LINESTRING) {
					DecodeCoordinateSequence();
				} else {
					DecodePolygon();
				}
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			auto num_parts = in_->Count(5);
			out_.Put<uint32_t>(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				auto child_type = in_->U8();
				if (!IsValidGeometryType(child_type)) {
					throw InvalidInputException("GeoSilo: invalid geometry type %d in collection",
					                            static_cast<int>(child_type));
				}
				WriteWkbHeader(static_cast<GeometryType>(child_type));
				DecodeGeometry(static_cast<GeometryType>(child_type), depth + 1);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	double dbl_scale_ = static_cast<double>(DEFAULT_SCALE);
	uint8_t version_ = GEOSILO_VERSION_1;
	uint32_t vw_ = 2;
	bool has_m_ = false;
	ByteCursor *in_ = nullptr;
	ByteBuffer out_;
};

// ---------------------------------------------------------------------------
// Bind functions
// ---------------------------------------------------------------------------

static unique_ptr<FunctionData> SiloEncodeBind(BindScalarFunctionInput &input) {
	int64_t scale = DEFAULT_SCALE;
	auto &geom_type = input.GetArguments()[0]->GetReturnType();
	if (geom_type.id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(geom_type)) {
		scale = ScaleFromCRS(GeoType::GetCRS(geom_type));
	}
	return make_uniq<SiloEncodeBindData>(scale);
}

// DuckDB 2.0 casts the arguments to the parameter types (plain GEOMETRY, without CRS) before the bind
// callback runs. Propagate the argument's own GEOMETRY type into the bound function so the CRS survives
// and SiloEncodeBind can derive the scale from it.
static void SiloEncodeResolveTypes(ResolveScalarFunctionTypesInput &input) {
	auto &argument_type = input.GetArgumentType(0);
	if (argument_type.id() != LogicalTypeId::GEOMETRY) {
		return;
	}
	input.GetBoundFunction().GetArguments()[0] = argument_type;
}

static unique_ptr<FunctionData> SiloEncodeWithScaleBind(BindScalarFunctionInput &input) {
	return make_uniq<SiloEncodeBindData>(0); // sentinel — scale comes from runtime arg
}

// ---------------------------------------------------------------------------
// Scalar functions
// ---------------------------------------------------------------------------

// #2: encode reads GEOMETRY string_t data directly as WKB — no intermediate Vector
// DuckDB's internal GEOMETRY format IS WKB (ToBinary is a no-op Reinterpret),
// so we read the GEOMETRY bytes directly without conversion.
static void SiloEncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<SiloEncodeBindData>();
	int64_t scale = bind_data.scale;

	auto &geom_vec = args.data[0];

	SiloWriter writer(scale);
	UnaryExecutor::Execute<string_t, string_t>(geom_vec, result, [&](string_t geom) {
		return writer.Encode(reinterpret_cast<const uint8_t *>(geom.GetData()),
		                     static_cast<idx_t>(geom.GetSize()), result);
	});
}

static void SiloEncodeWithScaleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom_vec = args.data[0];
	auto &scale_vec = args.data[1];

	SiloWriter writer(DEFAULT_SCALE);
	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    geom_vec, scale_vec, result, [&](string_t geom, int64_t scale) {
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
	UnaryExecutor::Execute<string_t, string_t>(blob_vec, result, [&](string_t blob) {
		return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
		                     static_cast<idx_t>(blob.GetSize()), result);
	});
}


static const char *const GEOMETRY_TYPE_NAMES[] = {"INVALID",    "POINT",           "LINESTRING",  "POLYGON",
                                                  "MULTIPOINT", "MULTILINESTRING", "MULTIPOLYGON", "GEOMETRYCOLLECTION"};
static const char *const VERTEX_TYPE_NAMES[] = {"XY", "XYZ", "XYM", "XYZM"};

// geosilo_metadata(blob) → STRUCT{geometry_type VARCHAR, vertex_type VARCHAR, scale BIGINT}
static void SiloMetadataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &blob_vec = args.data[0];
	auto &entries = StructVector::GetEntries(result);
	auto &geom_type_vec = entries[0];
	auto &vert_type_vec = entries[1];
	auto &scale_vec = entries[2];

	UnifiedVectorFormat blob_format;
	blob_vec.ToUnifiedFormat(blob_format);
	auto blob_data = UnifiedVectorFormat::GetData<string_t>(blob_format);

	auto &result_validity = FlatVector::ValidityMutable(result);

	for (idx_t i = 0; i < args.size(); i++) {
		auto blob_idx = blob_format.sel->get_index(i);
		if (!blob_format.validity.RowIsValid(blob_idx)) {
			// Mark the children NULL too, so struct field access on a NULL row yields NULL
			result_validity.SetInvalid(i);
			for (auto &entry : entries) {
				FlatVector::ValidityMutable(entry).SetInvalid(i);
			}
			continue;
		}
		auto &blob = blob_data[blob_idx];
		auto header =
		    ParseSiloHeader(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()));
		FlatVector::GetDataMutable<string_t>(geom_type_vec)[i] =
		    StringVector::AddString(geom_type_vec, GEOMETRY_TYPE_NAMES[static_cast<uint8_t>(header.geom_type)]);
		FlatVector::GetDataMutable<string_t>(vert_type_vec)[i] =
		    StringVector::AddString(vert_type_vec, VERTEX_TYPE_NAMES[static_cast<uint8_t>(header.vert_type)]);
		FlatVector::GetDataMutable<int64_t>(scale_vec)[i] = header.scale;
	}
}

// geosilo_is_valid(blob) → BOOLEAN: true when the blob is a structurally valid GEOSILO value
static void SiloIsValidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t blob) {
		return ValidateSilo(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()))
		    .empty();
	});
}

// geosilo_validate(blob) → VARCHAR: NULL when valid, otherwise a description of the problem
static void SiloValidateFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(), [&](string_t blob) -> optional<string_t> {
		    auto error =
		        ValidateSilo(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()));
		    if (error.empty()) {
			    return nullopt;
		    }
		    return StringVector::AddString(result, error);
	    });
}

// try_geosilo_decode(blob) → GEOMETRY, NULL when the blob is malformed
static void SiloTryDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	SiloReader reader;
	UnaryExecutor::Execute<string_t, string_t>(
	    args.data[0], result, args.size(), [&](string_t blob) -> optional<string_t> {
		    try {
			    return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
			                         static_cast<idx_t>(blob.GetSize()), result);
		    } catch (InvalidInputException &) {
			    return nullopt;
		    }
	    });
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
	static void DuckToArrow(ClientContext &context, const Vector &source, Vector &result, idx_t count) {
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
// Cast functions
// ---------------------------------------------------------------------------

// GEOSILO → GEOMETRY (decode silo binary to WKB)
static bool GeoSiloToGeometryCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	SiloReader reader;
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t blob) {
		return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
		                     static_cast<idx_t>(blob.GetSize()), result);
	});
	return true;
}

// GEOMETRY → GEOSILO bind: extract scale from target type at bind time
struct GeoSiloCastData : public BoundCastData {
	int64_t scale;
	explicit GeoSiloCastData(int64_t scale) : scale(scale) {}
	unique_ptr<BoundCastData> Copy() const override { return make_uniq<GeoSiloCastData>(scale); }
};

static bool GeometryToGeoSiloCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &cast_data = parameters.cast_data->Cast<GeoSiloCastData>();
	SiloWriter writer(cast_data.scale);
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t geom) {
		return writer.Encode(reinterpret_cast<const uint8_t *>(geom.GetData()),
		                     static_cast<idx_t>(geom.GetSize()), result);
	});
	return true;
}

static BoundCastInfo BindGeometryToGeoSiloCast(BindCastInput &input, const LogicalType &source,
                                                const LogicalType &target) {
	int64_t scale = GeoSiloType::GetScale(target);
	// If target has no explicit scale but source GEOMETRY has CRS, use that
	if (scale == DEFAULT_SCALE && source.id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(source)) {
		scale = ScaleFromCRS(GeoType::GetCRS(source));
	}
	return BoundCastInfo(GeometryToGeoSiloCast, make_uniq<GeoSiloCastData>(scale));
}


// BLOB → GEOSILO (explicit only): accepts the raw output of geosilo_encode after validating the whole blob
// (header, structure, counts). The GEOSILO binary is self-describing, so the bytes are copied unchanged.
static bool BlobToGeoSiloCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	bool all_converted = true;
	UnaryExecutor::Execute<string_t, string_t>(source, result, count, [&](string_t blob) -> optional<string_t> {
		auto error = ValidateSilo(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()));
		if (!error.empty()) {
			// Throws for CAST; records the error and yields NULL for TRY_CAST
			HandleCastError::AssignError(error, parameters);
			all_converted = false;
			return nullopt;
		}
		return StringVector::AddStringOrBlob(result, blob);
	});
	return all_converted;
}

// GEOSILO → VARCHAR (decode to WKB, then to WKT for display)
static bool GeoSiloToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	Vector geom_vec(LogicalType::GEOMETRY(), count);
	SiloReader reader;
	UnaryExecutor::Execute<string_t, string_t>(source, geom_vec, count, [&](string_t blob) {
		return reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()),
		                     geom_vec);
	});
	UnaryExecutor::Execute<string_t, string_t>(geom_vec, result, count, [&](string_t geom) {
		return Geometry::ToString(StringVector::GetStringHeap(result), geom);
	});
	return true;
}

// ---------------------------------------------------------------------------
// Direct ST_* function implementations on GEOSILO binary
// ---------------------------------------------------------------------------
// Semantics follow the spatial extension's GEOMETRY overloads: NULL for an empty geometry where spatial
// returns NULL, 0 for area/length/perimeter of geometries without that dimension.

static inline const uint8_t *BlobData(const string_t &blob) {
	return reinterpret_cast<const uint8_t *>(blob.GetData());
}
static inline idx_t BlobSize(const string_t &blob) {
	return static_cast<idx_t>(blob.GetSize());
}

// ST_GeometryType(GEOSILO) → VARCHAR
static void GeoSiloSTGeometryType(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t blob) {
		auto header = ParseSiloHeader(BlobData(blob), BlobSize(blob));
		return StringVector::AddString(result, GEOMETRY_TYPE_NAMES[static_cast<uint8_t>(header.geom_type)]);
	});
}

// ST_IsEmpty(GEOSILO) → BOOL: true when the geometry has no vertices
static void GeoSiloSTIsEmpty(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t blob) {
		auto data = BlobData(blob);
		auto size = BlobSize(blob);
		auto header = ParseSiloHeader(data, size);
		ByteCursor cur(data + header.body_offset, size - header.body_offset, "GeoSilo");
		if (header.geom_type == GeometryType::POINT) {
			return cur.Get<int32_t>() == EMPTY_COORD;
		}
		auto top_count = cur.Get<uint32_t>();
		if (top_count == 0) {
			return true;
		}
		if (header.geom_type == GeometryType::LINESTRING) {
			return false;
		}
		// Polygons, multi-geometries and collections may consist solely of empty parts.
		CountVisitor vis;
		SiloWalker<CountVisitor> walker(data, size, vis);
		walker.Walk();
		return vis.count == 0;
	});
}

// ST_NPoints(GEOSILO) → INTEGER: total vertex count, counting ring closures like the WKB representation
static void GeoSiloSTNPoints(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, int32_t>(args.data[0], result, args.size(), [&](string_t blob) {
		CountVisitor vis;
		SiloWalker<CountVisitor> walker(BlobData(blob), BlobSize(blob), vis);
		walker.Walk();
		if (vis.count > std::numeric_limits<int32_t>::max()) {
			throw InvalidInputException("ST_NPoints: vertex count overflows INTEGER");
		}
		return static_cast<int32_t>(vis.count);
	});
}

// ST_X / ST_Y (GEOSILO) → DOUBLE: coordinate of a POINT; NULL for an empty POINT, error for other types
template <idx_t AXIS>
static void GeoSiloSTCoord(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, double>(
	    args.data[0], result, args.size(), [&](string_t blob) -> optional<double> {
		    auto data = BlobData(blob);
		    auto size = BlobSize(blob);
		    auto header = ParseSiloHeader(data, size);
		    if (header.geom_type != GeometryType::POINT) {
			    throw InvalidInputException("%s: geometry is not a POINT (got %s)", AXIS == 0 ? "ST_X" : "ST_Y",
			                                GEOMETRY_TYPE_NAMES[static_cast<uint8_t>(header.geom_type)]);
		    }
		    ByteCursor cur(data + header.body_offset, size - header.body_offset, "GeoSilo");
		    auto x = cur.Get<int32_t>();
		    auto y = cur.Get<int32_t>();
		    auto v = AXIS == 0 ? x : y;
		    if (x == EMPTY_COORD || v == EMPTY_COORD) {
			    return nullopt;
		    }
		    return static_cast<double>(v) / header.dbl_scale;
	    });
}

// Bounding box: NULL for empty geometries
enum class BBoxField : uint8_t { XMIN, XMAX, YMIN, YMAX };

template <BBoxField FIELD>
static void GeoSiloSTBBox(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, double>(
	    args.data[0], result, args.size(), [&](string_t blob) -> optional<double> {
		    BBoxVisitor vis;
		    SiloWalker<BBoxVisitor> walker(BlobData(blob), BlobSize(blob), vis);
		    walker.Walk();
		    if (!vis.has_points) {
			    return nullopt;
		    }
		    int32_t v;
		    switch (FIELD) {
		    case BBoxField::XMIN:
			    v = vis.xmin;
			    break;
		    case BBoxField::XMAX:
			    v = vis.xmax;
			    break;
		    case BBoxField::YMIN:
			    v = vis.ymin;
			    break;
		    default:
			    v = vis.ymax;
			    break;
		    }
		    return static_cast<double>(v) / walker.dbl_scale();
	    });
}

static void GeoSiloSTArea(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(), [&](string_t blob) {
		AreaVisitor vis;
		SiloWalker<AreaVisitor> walker(BlobData(blob), BlobSize(blob), vis);
		walker.Walk();
		// The shoelace sum is twice the area in scaled units squared.
		double sc = walker.dbl_scale();
		return vis.total * 0.5 / (sc * sc);
	});
}

template <bool POLYGONAL>
static void GeoSiloSTLengthImpl(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, double>(args.data[0], result, args.size(), [&](string_t blob) {
		LengthVisitor<POLYGONAL> vis;
		SiloWalker<LengthVisitor<POLYGONAL>> walker(BlobData(blob), BlobSize(blob), vis);
		walker.Walk();
		return vis.total_length / walker.dbl_scale();
	});
}

// ---------------------------------------------------------------------------
// Extension loading
// ---------------------------------------------------------------------------

// Declares a positional-only parameter with a real name, so duckdb_functions() reports it instead of col<N>.
static FunctionParameter Positional(const char *name, LogicalType type) {
	return FunctionParameter(name, std::move(type), {}, FunctionParameterKind::POSITIONAL_ONLY);
}

static void LoadInternal(ExtensionLoader &loader) {
	QueryFarmSendTelemetry(loader, "geosilo", GeosiloExtension().Version());

	// --- GEOSILO type registration ---
	auto geosilo_type = GeoSiloType::GetDefault();
	loader.RegisterType("GEOSILO", geosilo_type, GeoSiloType::Bind);

	// --- Cast functions ---
	// GEOSILO → GEOMETRY (implicit, cost 100)
	loader.RegisterCastFunction(geosilo_type, LogicalType::GEOMETRY(),
	                            BoundCastInfo(GeoSiloToGeometryCast), 100);
	// GEOMETRY → GEOSILO (implicit, cost 200 — for INSERT INTO)
	loader.RegisterCastFunction(LogicalType::GEOMETRY(), geosilo_type,
	                            BindGeometryToGeoSiloCast, 200);
	// GEOSILO → VARCHAR (implicit, high cost — for display)
	loader.RegisterCastFunction(geosilo_type, LogicalType::VARCHAR,
	                            BoundCastInfo(GeoSiloToVarcharCast), 10000);
	// BLOB → GEOSILO (explicit only — e.g. geosilo_encode(...)::GEOSILO); validates the whole blob
	loader.RegisterCastFunction(LogicalType::BLOB, geosilo_type, BoundCastInfo(BlobToGeoSiloCast));

	// --- geosilo_encode (overloaded: with and without scale) ---
	ScalarFunctionSet encode_set("geosilo_encode");
	ScalarFunction encode_fn("geosilo_encode", {Positional("geometry", LogicalType::GEOMETRY())}, LogicalType::BLOB,
	                         SiloEncodeFunction, SiloEncodeBind);
	encode_fn.SetResolveTypesCallback(SiloEncodeResolveTypes);
	encode_set.AddFunction(encode_fn);
	encode_set.AddFunction(ScalarFunction(
	    "geosilo_encode", {Positional("geometry", LogicalType::GEOMETRY()), Positional("scale", LogicalType::BIGINT)},
	    LogicalType::BLOB, SiloEncodeWithScaleFunction, SiloEncodeWithScaleBind));
	// Can throw on invalid input (bad blob, coordinate overflow, unsupported geometry type)
	encode_set.SetFallible();
	CreateScalarFunctionInfo encode_info(std::move(encode_set));
	encode_info.descriptions.push_back({
	    {LogicalType::GEOMETRY()},
	    {"geometry"},
	    "Encodes a GEOMETRY value into the compact GEOSILO binary format. "
	    "When the GEOMETRY carries CRS metadata, an appropriate integer scale is derived from the CRS (e.g. degree-based CRSes get higher precision than projected meter-based ones); otherwise a default scale of 10,000,000 (1e7) is used. "
	    "Returns a BLOB; cast it to GEOSILO (`geosilo_encode(...)::GEOSILO`, which validates the header) to use it with the GEOSILO ST_* overloads. "
	    "geosilo_decode and geosilo_metadata also accept the BLOB directly.",
	    {"geosilo_encode(ST_Point(1, 2))",
	     "geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))"},
	    {"geosilo", "geometry", "encoding"}
	});
	encode_info.descriptions.push_back({
	    {LogicalType::GEOMETRY(), LogicalType::BIGINT},
	    {"geometry", "scale"},
	    "Encodes a GEOMETRY into GEOSILO using a caller-supplied integer scale. Each coordinate is multiplied by `scale` and rounded to a 32-bit integer before delta-encoding, "
	    "so a scale of 1e6 keeps 6 decimal digits of precision, 1e7 keeps 7, etc. "
	    "Larger scales preserve more precision; `scale` must be positive, and encoding fails if any scaled coordinate falls outside the 32-bit integer range "
	    "(about ±214 at scale 1e7, ±21,474,836 at scale 100).",
	    {"geosilo_encode(ST_Point(1.234, 5.678), 1000000)",
	     "geosilo_encode(ST_GeomFromText('LINESTRING(500000.25 4649776.75, 500010.5 4649780)'), 100)"},
	    {"geosilo", "geometry", "encoding"}
	});
	loader.RegisterFunction(encode_info);

	// --- geosilo_decode (accepts GEOSILO or BLOB) ---
	ScalarFunctionSet decode_set("geosilo_decode");
	decode_set.AddFunction(ScalarFunction("geosilo_decode", {Positional("geom", geosilo_type)}, LogicalType::GEOMETRY(),
	                                      SiloDecodeFunction));
	decode_set.AddFunction(ScalarFunction("geosilo_decode", {Positional("blob", LogicalType::BLOB)},
	                                      LogicalType::GEOMETRY(), SiloDecodeFunction));
	// Can throw on invalid input (bad blob, coordinate overflow, unsupported geometry type)
	decode_set.SetFallible();
	CreateScalarFunctionInfo decode_info(std::move(decode_set));
	decode_info.descriptions.push_back({
	    {geosilo_type},
	    {"geom"},
	    "Decodes a GEOSILO value back into a DuckDB GEOMETRY. "
	    "Round-trips losslessly when the original was encoded at sufficient scale.",
	    {"geosilo_decode(geosilo_encode(ST_Point(1, 2))::GEOSILO)",
	     "SELECT geosilo_decode(silo_col) FROM (SELECT ST_Point(1, 2)::GEOSILO AS silo_col) t"},
	    {"geosilo", "geometry", "encoding"}
	});
	decode_info.descriptions.push_back({
	    {LogicalType::BLOB},
	    {"blob"},
	    "Decodes a raw GEOSILO-formatted BLOB into a GEOMETRY. "
	    "Provided for backward compatibility with values stored as BLOB rather than the GEOSILO type.",
	    {"SELECT geosilo_decode(blob_col) FROM (SELECT geosilo_encode(ST_Point(1, 2)) AS blob_col) t"},
	    {"geosilo", "geometry", "encoding"}
	});
	loader.RegisterFunction(decode_info);

	// --- geosilo_metadata (accepts GEOSILO or BLOB) ---
	auto meta_type = LogicalType::STRUCT({
	    {"geometry_type", LogicalType::VARCHAR},
	    {"vertex_type", LogicalType::VARCHAR},
	    {"scale", LogicalType::BIGINT},
	});
	ScalarFunctionSet metadata_set("geosilo_metadata");
	metadata_set.AddFunction(
	    ScalarFunction("geosilo_metadata", {Positional("geom", geosilo_type)}, meta_type, SiloMetadataFunction));
	metadata_set.AddFunction(
	    ScalarFunction("geosilo_metadata", {Positional("blob", LogicalType::BLOB)}, meta_type, SiloMetadataFunction));
	// Can throw on invalid input (bad blob, coordinate overflow, unsupported geometry type)
	metadata_set.SetFallible();
	CreateScalarFunctionInfo metadata_info(std::move(metadata_set));
	const char *metadata_desc =
	    "Reads the GEOSILO header without decoding the geometry. "
	    "Returns a STRUCT with geometry_type (POINT, LINESTRING, ...), "
	    "vertex_type (XY, XYZ, XYM, XYZM), and scale (the integer scale factor used at encode time).";
	metadata_info.descriptions.push_back({
	    {geosilo_type},
	    {"geom"},
	    metadata_desc,
	    {"geosilo_metadata(geosilo_encode(ST_Point(1, 2))::GEOSILO)"},
	    {"geosilo", "metadata"}
	});
	metadata_info.descriptions.push_back({
	    {LogicalType::BLOB},
	    {"blob"},
	    metadata_desc,
	    {"geosilo_metadata(geosilo_encode(ST_Point(1, 2)))"},
	    {"geosilo", "metadata"}
	});
	loader.RegisterFunction(metadata_info);

	// --- geosilo_is_valid / geosilo_validate / try_geosilo_decode (accept GEOSILO or BLOB) ---
	auto register_blob_fn = [&](const char *name, scalar_function_t fn, const LogicalType &return_type,
	                            const char *desc, vector<string> examples, bool fallible) {
		ScalarFunctionSet set(name);
		set.AddFunction(ScalarFunction(name, {Positional("geom", geosilo_type)}, return_type, fn));
		set.AddFunction(ScalarFunction(name, {Positional("blob", LogicalType::BLOB)}, return_type, fn));
		if (fallible) {
			set.SetFallible();
		}
		CreateScalarFunctionInfo info(std::move(set));
		info.descriptions.push_back({{geosilo_type}, {"geom"}, desc, examples, {"geosilo", "validation"}});
		info.descriptions.push_back({{LogicalType::BLOB}, {"blob"}, desc, examples, {"geosilo", "validation"}});
		loader.RegisterFunction(info);
	};
	register_blob_fn("geosilo_is_valid", SiloIsValidFunction, LogicalType::BOOLEAN,
	                 "Returns true when the value is a structurally valid GEOSILO blob: a known header, element "
	                 "counts consistent with the data, supported nesting depth and no trailing bytes. Never raises "
	                 "an error for malformed input, so it can be used to filter untrusted BLOBs before decoding.",
	                 {"geosilo_is_valid(geosilo_encode(ST_Point(1, 2)))", "geosilo_is_valid('\\x00\\x01'::BLOB)"},
	                 false);
	register_blob_fn("geosilo_validate", SiloValidateFunction, LogicalType::VARCHAR,
	                 "Validates a GEOSILO blob and returns NULL when it is valid, otherwise a message describing the "
	                 "first problem found (truncated data, bad magic byte, unsupported version, invalid count, ...).",
	                 {"geosilo_validate(geosilo_encode(ST_Point(1, 2)))", "geosilo_validate('\\x47\\x01'::BLOB)"},
	                 false);
	register_blob_fn("try_geosilo_decode", SiloTryDecodeFunction, LogicalType::GEOMETRY(),
	                 "Decodes a GEOSILO blob into a GEOMETRY like geosilo_decode, but returns NULL instead of raising "
	                 "an error when the blob is malformed.",
	                 {"try_geosilo_decode(geosilo_encode(ST_Point(1, 2)))", "try_geosilo_decode('\\x54\\xff'::BLOB)"},
	                 false);

	// --- ST_* function overloads (direct on GEOSILO, no decode) ---
	auto register_st = [&](const char *name, ScalarFunction fn, const char *param_name,
	                       const char *desc, vector<string> examples, vector<string> categories) {
		// All GEOSILO ST_* overloads throw on an invalid blob
		fn.SetFallible();
		CreateScalarFunctionInfo info(std::move(fn));
		// Add the GEOSILO overload without replacing Spatial's existing overloads.
		// The CreateScalarFunctionInfo overload does not set this policy for us.
		info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
		info.descriptions.push_back({
		    {geosilo_type},
		    {param_name},
		    desc,
		    std::move(examples),
		    std::move(categories)
		});
		loader.RegisterFunction(info);
	};

	// Phase 1: Metadata
	register_st("ST_GeometryType",
	    ScalarFunction("ST_GeometryType", {Positional("geom", geosilo_type)}, LogicalType::VARCHAR, GeoSiloSTGeometryType),
	    "geom",
	    "Returns the geometry type name (POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION) "
	    "by reading the GEOSILO header — no full decode is performed.",
	    {"ST_GeometryType(geosilo_encode(ST_Point(1, 2))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	register_st("ST_IsEmpty",
	    ScalarFunction("ST_IsEmpty", {Positional("geom", geosilo_type)}, LogicalType::BOOLEAN, GeoSiloSTIsEmpty),
	    "geom",
	    "Returns true if the encoded geometry contains no coordinates. "
	    "Reads only the header and the top-level vertex/part count.",
	    {"ST_IsEmpty(geosilo_encode(ST_GeomFromText('POINT EMPTY'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	register_st("ST_NPoints",
	    ScalarFunction("ST_NPoints", {Positional("geom", geosilo_type)}, LogicalType::INTEGER, GeoSiloSTNPoints),
	    "geom",
	    "Returns the total number of vertices in the encoded geometry, including all rings and sub-geometries "
	    "(polygon rings count their closing vertex, empty points count as 0).",
	    {"ST_NPoints(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 1 1, 2 2)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	// Phase 2: POINT coordinate access
	register_st("ST_X",
	    ScalarFunction("ST_X", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTCoord<0>),
	    "geom",
	    "Returns the X coordinate of a POINT geometry. "
	    "Returns NULL for an empty POINT and raises an error for non-POINT geometries.",
	    {"ST_X(geosilo_encode(ST_Point(1.5, 2.5))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	register_st("ST_Y",
	    ScalarFunction("ST_Y", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTCoord<1>),
	    "geom",
	    "Returns the Y coordinate of a POINT geometry. "
	    "Returns NULL for an empty POINT and raises an error for non-POINT geometries.",
	    {"ST_Y(geosilo_encode(ST_Point(1.5, 2.5))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	// Phase 3: Bounding box
	register_st("ST_XMin",
	    ScalarFunction("ST_XMin", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTBBox<BBoxField::XMIN>),
	    "geom",
	    "Returns the minimum X coordinate of the geometry's bounding box, or NULL for an empty geometry.",
	    {"ST_XMin(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 5 5)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial", "bbox"});

	register_st("ST_XMax",
	    ScalarFunction("ST_XMax", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTBBox<BBoxField::XMAX>),
	    "geom",
	    "Returns the maximum X coordinate of the geometry's bounding box, or NULL for an empty geometry.",
	    {"ST_XMax(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 5 5)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial", "bbox"});

	register_st("ST_YMin",
	    ScalarFunction("ST_YMin", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTBBox<BBoxField::YMIN>),
	    "geom",
	    "Returns the minimum Y coordinate of the geometry's bounding box, or NULL for an empty geometry.",
	    {"ST_YMin(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 5 5)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial", "bbox"});

	register_st("ST_YMax",
	    ScalarFunction("ST_YMax", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTBBox<BBoxField::YMAX>),
	    "geom",
	    "Returns the maximum Y coordinate of the geometry's bounding box, or NULL for an empty geometry.",
	    {"ST_YMax(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 5 5)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial", "bbox"});

	// Phase 4: Computational
	register_st("ST_Area",
	    ScalarFunction("ST_Area", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTArea),
	    "geom",
	    "Returns the planar area of the polygonal parts of the geometry in the units of its coordinate system "
	    "(exterior ring area minus hole area, summed over all polygons, including those inside a GEOMETRYCOLLECTION). "
	    "Returns 0 for points and linestrings.",
	    {"ST_Area(geosilo_encode(ST_GeomFromText('POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	register_st("ST_Length",
	    ScalarFunction("ST_Length", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTLengthImpl<false>),
	    "geom",
	    "Returns the planar length of the linear parts of the geometry (LINESTRING, MULTILINESTRING and linestrings "
	    "inside a GEOMETRYCOLLECTION) in the units of its coordinate system. Returns 0 for points and polygons "
	    "(use ST_Perimeter for polygon boundaries).",
	    {"ST_Length(geosilo_encode(ST_GeomFromText('LINESTRING(0 0, 3 4)'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	register_st("ST_Perimeter",
	    ScalarFunction("ST_Perimeter", {Positional("geom", geosilo_type)}, LogicalType::DOUBLE, GeoSiloSTLengthImpl<true>),
	    "geom",
	    "Returns the planar perimeter of the polygonal parts of the geometry, summed across all rings (holes included). "
	    "Returns 0 for points and linestrings.",
	    {"ST_Perimeter(geosilo_encode(ST_GeomFromText('POLYGON((0 0, 4 0, 4 3, 0 3, 0 0))'))::GEOSILO)"},
	    {"geosilo", "geometry", "spatial"});

	// Register "queryfarm.geosilo" Arrow extension type
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
	return GEOSILO_EXTENSION_VERSION;
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(geosilo, loader) {
	duckdb::LoadInternal(loader);
}
}
