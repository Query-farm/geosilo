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
// GeoSilo binary format (v2)
// ---------------------------------------------------------------------------
//
// Header (14 bytes):
//   magic            1 byte   0x47 ('G')
//   version          1 byte   0x02
//   geometry_type    1 byte   GeometryType enum
//   vertex_type      1 byte   VertexType enum (XY=0, XYZ=1, XYM=2, XYZM=3)
//   scale            8 bytes  int64 LE
//
// Coordinate encoding:
//   First coordinate per ring/linestring: int32 absolute.
//   Subsequent coordinates: int16 delta from previous.
//     If delta doesn't fit int16, write INT16_MIN as sentinel, then int32 delta.
//   Empty point (NaN): INT32_MAX sentinel per dimension.
//
// v1 blobs are still decodable by the reader.

static constexpr uint8_t GEOSILO_MAGIC      = 0x47; // 'G'
static constexpr uint8_t GEOSILO_VERSION     = 0x01;
static constexpr int64_t DEFAULT_SCALE       = 10000000; // 1e7
static constexpr int16_t DELTA_ESCAPE        = std::numeric_limits<int16_t>::min(); // -32768
static constexpr int32_t EMPTY_COORD         = std::numeric_limits<int32_t>::max();

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

class SiloWriter {
public:
	explicit SiloWriter(int64_t scale) : scale_(scale), dbl_scale_(static_cast<double>(scale)) {}

	void EncodeWKB(const uint8_t *data, idx_t size) {
		wkb_ = data;
		wkb_size_ = size;
		pos_ = 0;
		buf_.clear();
		buf_.reserve(size / 2 + 14); // #6: pre-allocate — silo is always smaller than WKB

		auto byte_order = ReadWKBByte();
		(void)byte_order;
		auto wkb_type = ReadWKBUint32();
		auto geom_type = static_cast<GeometryType>(wkb_type & 0xFF);
		auto has_z = (wkb_type & 0x80000000) != 0;
		auto has_m = (wkb_type & 0x40000000) != 0;
		VertexType vert_type = VertexType::XY;
		if (has_z && has_m)      vert_type = VertexType::XYZM;
		else if (has_z)          vert_type = VertexType::XYZ;
		else if (has_m)          vert_type = VertexType::XYM;
		vertex_width_ = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

		WriteHeader(geom_type, vert_type);
		EncodeGeometry(geom_type);
	}

	string_t Finish(Vector &result) {
		return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(buf_.data()), buf_.size());
	}

private:
	void WriteHeader(GeometryType geom_type, VertexType vert_type) {
		WriteByte(GEOSILO_MAGIC);
		WriteByte(GEOSILO_VERSION);
		WriteByte(static_cast<uint8_t>(geom_type));
		WriteByte(static_cast<uint8_t>(vert_type));
		WriteInt64(scale_);
	}

	// --- WKB reading ---
	uint8_t ReadWKBByte() {
		if (pos_ >= wkb_size_) throw InvalidInputException("GeoSilo: unexpected end of WKB");
		return wkb_[pos_++];
	}
	uint32_t ReadWKBUint32() {
		if (pos_ + 4 > wkb_size_) throw InvalidInputException("GeoSilo: unexpected end of WKB");
		uint32_t val; memcpy(&val, wkb_ + pos_, 4); pos_ += 4; return val;
	}
	double ReadWKBDouble() {
		if (pos_ + 8 > wkb_size_) throw InvalidInputException("GeoSilo: unexpected end of WKB");
		double val; memcpy(&val, wkb_ + pos_, 8); pos_ += 8; return val;
	}

	// --- Output writing ---
	void WriteByte(uint8_t b)    { buf_.push_back(b); }
	void WriteUint32(uint32_t v) { auto off = buf_.size(); buf_.resize(off + 4); memcpy(buf_.data() + off, &v, 4); }
	void WriteInt16(int16_t v)   { auto off = buf_.size(); buf_.resize(off + 2); memcpy(buf_.data() + off, &v, 2); }
	void WriteInt32(int32_t v)   { auto off = buf_.size(); buf_.resize(off + 4); memcpy(buf_.data() + off, &v, 4); }
	void WriteInt64(int64_t v)   { auto off = buf_.size(); buf_.resize(off + 8); memcpy(buf_.data() + off, &v, 8); }

	// #1: overflow-safe scaling — clamp to int32 range
	int32_t ScaleCoord(double val) {
		if (std::isnan(val)) return EMPTY_COORD; // #5: empty geometry
		auto scaled = static_cast<int64_t>(std::round(val * dbl_scale_));
		if (scaled > std::numeric_limits<int32_t>::max() || scaled < std::numeric_limits<int32_t>::min()) {
			throw InvalidInputException(
			    "GeoSilo: coordinate %.7f overflows int32 at scale %lld. Use a smaller scale.",
			    val, static_cast<long long>(scale_));
		}
		return static_cast<int32_t>(scaled);
	}

	// #3: write delta as int16, escaping to int32 when needed
	void WriteDelta(int32_t delta) {
		if (delta > std::numeric_limits<int16_t>::max() || delta <= std::numeric_limits<int16_t>::min()) {
			// Delta doesn't fit int16 — write escape sentinel + int32
			WriteInt16(DELTA_ESCAPE);
			WriteInt32(delta);
		} else {
			WriteInt16(static_cast<int16_t>(delta));
		}
	}

	void EncodeCoordinateSequence(uint32_t num_points) {
		WriteUint32(num_points);
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t i = 0; i < num_points; i++) {
			for (uint32_t d = 0; d < vertex_width_; d++) {
				int32_t scaled = ScaleCoord(ReadWKBDouble());
				if (i == 0) {
					WriteInt32(scaled); // first point: absolute
				} else {
					WriteDelta(scaled - prev[d]); // #3: variable-width delta
				}
				prev[d] = scaled;
			}
		}
	}

	void EncodeGeometry(GeometryType type) {
		switch (type) {
		case GeometryType::POINT: {
			for (uint32_t d = 0; d < vertex_width_; d++) {
				WriteInt32(ScaleCoord(ReadWKBDouble()));
			}
			break;
		}
		case GeometryType::LINESTRING: {
			EncodeCoordinateSequence(ReadWKBUint32());
			break;
		}
		case GeometryType::POLYGON: {
			uint32_t num_rings = ReadWKBUint32();
			WriteUint32(num_rings);
			for (uint32_t r = 0; r < num_rings; r++) {
				EncodeCoordinateSequence(ReadWKBUint32());
			}
			break;
		}
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			uint32_t num_parts = ReadWKBUint32();
			WriteUint32(num_parts);
			auto inner = (type == GeometryType::MULTIPOINT)       ? GeometryType::POINT
			             : (type == GeometryType::MULTILINESTRING) ? GeometryType::LINESTRING
			                                                      : GeometryType::POLYGON;
			for (uint32_t p = 0; p < num_parts; p++) {
				ReadWKBByte();    // byte order
				ReadWKBUint32();  // type (skip)
				EncodeGeometry(inner);
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			uint32_t num_parts = ReadWKBUint32();
			WriteUint32(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				ReadWKBByte(); // byte order
				auto wt = ReadWKBUint32();
				auto child_type = static_cast<GeometryType>(wt & 0xFF);
				WriteByte(static_cast<uint8_t>(child_type));
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
	uint32_t vertex_width_ = 2;
	const uint8_t *wkb_ = nullptr;
	idx_t wkb_size_ = 0;
	idx_t pos_ = 0;
	vector<uint8_t> buf_;
};

// ---------------------------------------------------------------------------
// Reader: GeoSilo blob → WKB bytes
// Supports both v1 (all int32 deltas) and v2 (int16 deltas with escape)
// ---------------------------------------------------------------------------

class SiloReader {
public:
	void Decode(const uint8_t *data, idx_t size) {
		silo_ = data;
		silo_size_ = size;
		pos_ = 0;

		uint8_t magic = ReadByte();
		if (magic != GEOSILO_MAGIC) {
			throw InvalidInputException("GeoSilo: invalid magic byte 0x%02x", magic);
		}
		auto version = ReadByte();
		if (version != GEOSILO_VERSION) {
			throw InvalidInputException("GeoSilo: unsupported version %d", version);
		}

		auto geom_type = static_cast<GeometryType>(ReadByte());
		auto vert_type = static_cast<VertexType>(ReadByte());
		scale_ = ReadInt64();
		inv_scale_ = 1.0 / static_cast<double>(scale_);

		bool has_z = (vert_type == VertexType::XYZ || vert_type == VertexType::XYZM);
		bool has_m = (vert_type == VertexType::XYM || vert_type == VertexType::XYZM);
		vertex_width_ = 2 + (has_z ? 1 : 0) + (has_m ? 1 : 0);

		buf_.clear();
		buf_.reserve(silo_size_ * 2); // #6: pre-allocate — WKB is ~2x silo

		// WKB header
		WriteByte(0x01); // little-endian
		uint32_t wkb_type = static_cast<uint32_t>(geom_type);
		if (has_z) wkb_type |= 0x80000000;
		if (has_m) wkb_type |= 0x40000000;
		WriteUint32(wkb_type);

		DecodeGeometry(geom_type);
	}

	string_t Finish(Vector &result) {
		return StringVector::AddStringOrBlob(result, reinterpret_cast<const char *>(buf_.data()), buf_.size());
	}

private:
	uint8_t ReadByte() {
		if (pos_ >= silo_size_) throw InvalidInputException("GeoSilo: unexpected end of blob");
		return silo_[pos_++];
	}
	uint32_t ReadUint32() {
		if (pos_ + 4 > silo_size_) throw InvalidInputException("GeoSilo: unexpected end of blob");
		uint32_t v; memcpy(&v, silo_ + pos_, 4); pos_ += 4; return v;
	}
	int16_t ReadInt16() {
		if (pos_ + 2 > silo_size_) throw InvalidInputException("GeoSilo: unexpected end of blob");
		int16_t v; memcpy(&v, silo_ + pos_, 2); pos_ += 2; return v;
	}
	int32_t ReadInt32() {
		if (pos_ + 4 > silo_size_) throw InvalidInputException("GeoSilo: unexpected end of blob");
		int32_t v; memcpy(&v, silo_ + pos_, 4); pos_ += 4; return v;
	}
	int64_t ReadInt64() {
		if (pos_ + 8 > silo_size_) throw InvalidInputException("GeoSilo: unexpected end of blob");
		int64_t v; memcpy(&v, silo_ + pos_, 8); pos_ += 8; return v;
	}

	int32_t ReadDelta() {
		int16_t d = ReadInt16();
		if (d == DELTA_ESCAPE) {
			return ReadInt32();
		}
		return static_cast<int32_t>(d);
	}

	void WriteByte(uint8_t b)    { buf_.push_back(b); }
	void WriteUint32(uint32_t v) { auto off = buf_.size(); buf_.resize(off + 4); memcpy(buf_.data() + off, &v, 4); }
	void WriteDouble(double v)   { auto off = buf_.size(); buf_.resize(off + 8); memcpy(buf_.data() + off, &v, 8); }

	double UnscaleCoord(int32_t val) {
		if (val == EMPTY_COORD) return std::numeric_limits<double>::quiet_NaN(); // #5: empty geometry
		return static_cast<double>(val) * inv_scale_;
	}

	void DecodeCoordinateSequence() {
		uint32_t num_points = ReadUint32();
		WriteUint32(num_points);
		int32_t prev[4] = {0, 0, 0, 0};
		for (uint32_t i = 0; i < num_points; i++) {
			for (uint32_t d = 0; d < vertex_width_; d++) {
				if (i == 0) {
					prev[d] = ReadInt32(); // absolute
				} else {
					prev[d] += ReadDelta(); // accumulate
				}
				WriteDouble(UnscaleCoord(prev[d]));
			}
		}
	}

	void DecodeGeometry(GeometryType type) {
		switch (type) {
		case GeometryType::POINT: {
			for (uint32_t d = 0; d < vertex_width_; d++) {
				WriteDouble(UnscaleCoord(ReadInt32()));
			}
			break;
		}
		case GeometryType::LINESTRING: {
			DecodeCoordinateSequence();
			break;
		}
		case GeometryType::POLYGON: {
			uint32_t num_rings = ReadUint32();
			WriteUint32(num_rings);
			for (uint32_t r = 0; r < num_rings; r++) {
				DecodeCoordinateSequence();
			}
			break;
		}
		case GeometryType::MULTIPOINT:
		case GeometryType::MULTILINESTRING:
		case GeometryType::MULTIPOLYGON: {
			uint32_t num_parts = ReadUint32();
			WriteUint32(num_parts);
			auto inner = (type == GeometryType::MULTIPOINT)       ? GeometryType::POINT
			             : (type == GeometryType::MULTILINESTRING) ? GeometryType::LINESTRING
			                                                      : GeometryType::POLYGON;
			for (uint32_t p = 0; p < num_parts; p++) {
				WriteByte(0x01);
				uint32_t wkb_type = static_cast<uint32_t>(inner);
				if (vertex_width_ >= 3) wkb_type |= 0x80000000;
				if (vertex_width_ == 4) wkb_type |= 0x40000000;
				WriteUint32(wkb_type);
				DecodeGeometry(inner);
			}
			break;
		}
		case GeometryType::GEOMETRYCOLLECTION: {
			uint32_t num_parts = ReadUint32();
			WriteUint32(num_parts);
			for (uint32_t p = 0; p < num_parts; p++) {
				auto child_type = static_cast<GeometryType>(ReadByte());
				WriteByte(0x01);
				uint32_t wkb_type = static_cast<uint32_t>(child_type);
				if (vertex_width_ >= 3) wkb_type |= 0x80000000;
				if (vertex_width_ == 4) wkb_type |= 0x40000000;
				WriteUint32(wkb_type);
				DecodeGeometry(child_type);
			}
			break;
		}
		default:
			throw InvalidInputException("GeoSilo: unsupported geometry type %d", static_cast<int>(type));
		}
	}

	int64_t scale_ = DEFAULT_SCALE;
	double inv_scale_ = 1.0 / static_cast<double>(DEFAULT_SCALE);
	uint32_t vertex_width_ = 2;
	const uint8_t *silo_ = nullptr;
	idx_t silo_size_ = 0;
	idx_t pos_ = 0;
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
static void SiloEncodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &bind_data = state.expr.Cast<BoundFunctionExpression>().bind_info->Cast<SiloEncodeBindData>();
	int64_t scale = bind_data.scale;

	auto &geom_vec = args.data[0];

	// Convert GEOMETRY → WKB (needed because internal format differs from WKB)
	Vector wkb_vec(LogicalType::BLOB, args.size());
	Geometry::ToBinary(geom_vec, wkb_vec, args.size());

	UnaryExecutor::Execute<string_t, string_t>(wkb_vec, result, args.size(), [&](string_t wkb) {
		SiloWriter writer(scale);
		writer.EncodeWKB(reinterpret_cast<const uint8_t *>(wkb.GetData()), static_cast<idx_t>(wkb.GetSize()));
		return writer.Finish(result);
	});
}

static void SiloEncodeWithScaleFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom_vec = args.data[0];
	auto &scale_vec = args.data[1];

	Vector wkb_vec(LogicalType::BLOB, args.size());
	Geometry::ToBinary(geom_vec, wkb_vec, args.size());

	BinaryExecutor::Execute<string_t, int64_t, string_t>(
	    wkb_vec, scale_vec, result, args.size(), [&](string_t wkb, int64_t scale) {
		    SiloWriter writer(scale);
		    writer.EncodeWKB(reinterpret_cast<const uint8_t *>(wkb.GetData()),
		                     static_cast<idx_t>(wkb.GetSize()));
		    return writer.Finish(result);
	    });
}

// #2: decode writes WKB directly into the result GEOMETRY vector via per-value FromBinary
static void SiloDecodeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &blob_vec = args.data[0];

	// We need a temporary vector for per-value WKB output, then convert to GEOMETRY
	Vector wkb_vec(LogicalType::BLOB, args.size());

	UnaryExecutor::Execute<string_t, string_t>(blob_vec, wkb_vec, args.size(), [&](string_t blob) {
		SiloReader reader;
		reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()), static_cast<idx_t>(blob.GetSize()));
		return reader.Finish(wkb_vec);
	});

	// Batch convert WKB → GEOMETRY
	Geometry::FromBinary(wkb_vec, result, args.size(), true);
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
		if (size < 14 || data[0] != GEOSILO_MAGIC) {
			throw InvalidInputException("GeoSilo: invalid blob");
		}

		auto gt = data[2];
		auto vt = data[3];
		int64_t scale;
		memcpy(&scale, data + 4, 8);

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

	// Arrow → DuckDB: silo blob → GEOMETRY
	static void ArrowToDuck(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		Vector wkb_vec(LogicalType::BLOB, count);

		UnaryExecutor::Execute<string_t, string_t>(source, wkb_vec, count, [&](string_t blob) {
			SiloReader reader;
			reader.Decode(reinterpret_cast<const uint8_t *>(blob.GetData()),
			              static_cast<idx_t>(blob.GetSize()));
			return reader.Finish(wkb_vec);
		});

		Geometry::FromBinary(wkb_vec, result, count, true);
	}

	// DuckDB → Arrow: GEOMETRY → silo blob
	static void DuckToArrow(ClientContext &context, Vector &source, Vector &result, idx_t count) {
		int64_t scale = DEFAULT_SCALE;
		if (source.GetType().id() == LogicalTypeId::GEOMETRY && GeoType::HasCRS(source.GetType())) {
			scale = ScaleFromCRS(GeoType::GetCRS(source.GetType()));
		}

		Vector wkb_vec(LogicalType::BLOB, count);
		Geometry::ToBinary(source, wkb_vec, count);

		UnaryExecutor::Execute<string_t, string_t>(wkb_vec, result, count, [&](string_t wkb) {
			SiloWriter writer(scale);
			writer.EncodeWKB(reinterpret_cast<const uint8_t *>(wkb.GetData()),
			                 static_cast<idx_t>(wkb.GetSize()));
			return writer.Finish(result);
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
