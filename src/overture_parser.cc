#include "overture_parser.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <valhalla/baldr/graphconstants.h>
#include <valhalla/midgard/sequence.h>
#include <valhalla/mjolnir/osmaccess.h>
#include <valhalla/mjolnir/osmnode.h>
#include <valhalla/mjolnir/osmway.h>

#include <cstring>
#include <fstream>
#include <iostream>
#include <unordered_map>

using namespace valhalla::baldr;
using namespace valhalla::mjolnir;
using valhalla::midgard::sequence;

namespace {

uint64_t HashId(const std::string& id) {
  uint64_t hash = 14695981039346656037ULL;  // FNV-1a offset basis
  for (char c : id) {
    hash ^= static_cast<uint64_t>(c);
    hash *= 1099511628211ULL;
  }
  return hash == 0 ? 1 : hash;  // valhalla treats 0 as invalid
}

std::pair<double, double> ParseWKBPoint(const uint8_t* data, int64_t len) {
  if (len < 21) throw std::runtime_error("WKB Point too short");
  double lng, lat;
  std::memcpy(&lng, data + 5, 8);
  std::memcpy(&lat, data + 13, 8);
  return {lng, lat};
}

std::vector<std::pair<double, double>> ParseWKBLineString(const uint8_t* data, int64_t len) {
  if (len < 9) throw std::runtime_error("WKB LineString too short");
  uint32_t num_points;
  std::memcpy(&num_points, data + 5, 4);

  std::vector<std::pair<double, double>> points;
  points.reserve(num_points);
  const uint8_t* ptr = data + 9;
  for (uint32_t i = 0; i < num_points; i++) {
    double lng, lat;
    std::memcpy(&lng, ptr, 8);
    std::memcpy(&lat, ptr + 8, 8);
    points.emplace_back(lng, lat);
    ptr += 16;
  }
  return points;
}

Use ClassToUse(const std::string& cls) {
  static const std::unordered_map<std::string, Use> map = {
      {"motorway", Use::kRoad},       {"trunk", Use::kRoad},
      {"primary", Use::kRoad},        {"secondary", Use::kRoad},
      {"tertiary", Use::kRoad},       {"residential", Use::kRoad},
      {"unclassified", Use::kRoad},   {"living_street", Use::kLivingStreet},
      {"service", Use::kServiceRoad}, {"pedestrian", Use::kPedestrian},
      {"footway", Use::kFootway},     {"path", Use::kPath},
      {"cycleway", Use::kCycleway},   {"steps", Use::kSteps},
      {"track", Use::kTrack},         {"bridleway", Use::kBridleway},
  };
  auto it = map.find(cls);
  return it != map.end() ? it->second : Use::kRoad;
}

RoadClass ClassToRoadClass(const std::string& cls) {
  static const std::unordered_map<std::string, RoadClass> map = {
      {"motorway", RoadClass::kMotorway},       {"trunk", RoadClass::kTrunk},
      {"primary", RoadClass::kPrimary},         {"secondary", RoadClass::kSecondary},
      {"tertiary", RoadClass::kTertiary},       {"unclassified", RoadClass::kUnclassified},
      {"residential", RoadClass::kResidential}, {"living_street", RoadClass::kResidential},
      {"service", RoadClass::kServiceOther},    {"pedestrian", RoadClass::kServiceOther},
      {"footway", RoadClass::kServiceOther},    {"path", RoadClass::kServiceOther},
      {"cycleway", RoadClass::kServiceOther},   {"steps", RoadClass::kServiceOther},
      {"track", RoadClass::kServiceOther},      {"bridleway", RoadClass::kServiceOther},
  };
  auto it = map.find(cls);
  return it != map.end() ? it->second : RoadClass::kServiceOther;
}

Surface SurfaceFromString(const std::string& s) {
  static const std::unordered_map<std::string, Surface> map = {
      {"asphalt", Surface::kPavedSmooth},
      {"concrete", Surface::kPavedSmooth},
      {"paved", Surface::kPaved},
      {"paving_stones", Surface::kPaved},
      {"cobblestone", Surface::kPavedRough},
      {"sett", Surface::kPavedRough},
      {"gravel", Surface::kGravel},
      {"dirt", Surface::kDirt},
      {"sand", Surface::kDirt},
      {"grass", Surface::kPath},
      {"unpaved", Surface::kGravel},
      {"ground", Surface::kDirt},
      {"compacted", Surface::kCompacted},
      {"fine_gravel", Surface::kCompacted},
      {"metal", Surface::kPaved},
      {"wood", Surface::kPaved},
  };
  auto it = map.find(s);
  return it != map.end() ? it->second : Surface::kPaved;
}

bool IsHighwayClass(const std::string& cls) { return cls == "motorway" || cls == "trunk"; }

std::shared_ptr<arrow::Table> ReadParquet(const std::string& path) {
  auto infile = arrow::io::ReadableFile::Open(path).ValueOrDie();
  auto reader = parquet::arrow::OpenFile(infile, arrow::default_memory_pool()).ValueOrDie();
  std::shared_ptr<arrow::Table> table;
  auto status = reader->ReadTable(&table);
  if (!status.ok()) throw std::runtime_error("Cannot read table " + path + ": " + status.message());
  return table;
}

struct ConnectorInfo {
  uint64_t node_id;
  double lng;
  double lat;
};

void SetAllAccess(OSMWay& way, bool forward, bool backward) {
  way.set_auto_forward(forward);
  way.set_auto_backward(backward);
  way.set_bus_forward(forward);
  way.set_bus_backward(backward);
  way.set_taxi_forward(forward);
  way.set_taxi_backward(backward);
  way.set_truck_forward(forward);
  way.set_truck_backward(backward);
  way.set_moped_forward(forward);
  way.set_moped_backward(backward);
  way.set_motorcycle_forward(forward);
  way.set_motorcycle_backward(backward);
  way.set_emergency_forward(forward);
  way.set_emergency_backward(backward);
  way.set_hov_forward(forward);
  way.set_hov_backward(backward);
}

}  // namespace

namespace overture {

OSMData Parse(const boost::property_tree::ptree& config, const std::string& connectors_path,
              const std::string& segments_path, const std::string& ways_file,
              const std::string& way_nodes_file, const std::string& access_file,
              const std::string& cr_from_file, const std::string& cr_to_file,
              const std::string& linguistic_node_file, bool pedestrian_only) {
  OSMData data;
  data.initialized = true;

  uint64_t synthetic_node_id = 0xFFFF000000000000ULL;

  // Read connectors
  std::cout << "Reading connectors from " << connectors_path << "..." << std::endl;
  auto conn_table = ReadParquet(connectors_path);

  std::unordered_map<std::string, ConnectorInfo> connector_map;
  connector_map.reserve(conn_table->num_rows());

  auto conn_id_col = conn_table->GetColumnByName("id");
  auto conn_geom_col = conn_table->GetColumnByName("geometry");

  for (int chunk_idx = 0; chunk_idx < conn_id_col->num_chunks(); chunk_idx++) {
    auto id_arr = std::static_pointer_cast<arrow::StringArray>(conn_id_col->chunk(chunk_idx));
    auto geom_arr = std::static_pointer_cast<arrow::BinaryArray>(conn_geom_col->chunk(chunk_idx));

    for (int64_t i = 0; i < id_arr->length(); i++) {
      if (id_arr->IsNull(i) || geom_arr->IsNull(i)) continue;

      std::string id = id_arr->GetString(i);
      int32_t geom_len;
      const uint8_t* geom_data = geom_arr->GetValue(i, &geom_len);
      auto [lng, lat] = ParseWKBPoint(geom_data, geom_len);

      connector_map[id] = {HashId(id), lng, lat};
    }
  }
  std::cout << "Loaded " << connector_map.size() << " connectors" << std::endl;
  data.osm_node_count = connector_map.size();

  // Read segments, write ways and way_nodes
  std::cout << "Reading segments from " << segments_path << "..." << std::endl;
  auto seg_table = ReadParquet(segments_path);

  sequence<OSMWay> ways(ways_file, true);
  sequence<OSMWayNode> way_nodes(way_nodes_file, true);

  auto seg_id_col = seg_table->GetColumnByName("id");
  auto seg_class_col = seg_table->GetColumnByName("class");
  auto seg_names_col = seg_table->GetColumnByName("names");
  auto seg_connectors_col = seg_table->GetColumnByName("connectors");
  auto seg_surface_col = seg_table->GetColumnByName("road_surface");
  auto seg_speed_col = seg_table->GetColumnByName("speed_limits");
  auto seg_geom_col = seg_table->GetColumnByName("geometry");

  uint32_t way_index = 0;
  uint64_t total_way_nodes = 0;
  uint64_t skipped_segments = 0;

  for (int chunk_idx = 0; chunk_idx < seg_id_col->num_chunks(); chunk_idx++) {
    auto id_arr = std::static_pointer_cast<arrow::StringArray>(seg_id_col->chunk(chunk_idx));
    auto class_arr = std::static_pointer_cast<arrow::StringArray>(seg_class_col->chunk(chunk_idx));
    auto geom_arr = std::static_pointer_cast<arrow::BinaryArray>(seg_geom_col->chunk(chunk_idx));

    auto names_arr = std::static_pointer_cast<arrow::StructArray>(seg_names_col->chunk(chunk_idx));
    std::shared_ptr<arrow::StringArray> name_primary_arr;
    if (names_arr && names_arr->GetFieldByName("primary")) {
      name_primary_arr =
          std::static_pointer_cast<arrow::StringArray>(names_arr->GetFieldByName("primary"));
    }

    auto connectors_list =
        std::static_pointer_cast<arrow::ListArray>(seg_connectors_col->chunk(chunk_idx));
    auto connectors_values =
        std::static_pointer_cast<arrow::StructArray>(connectors_list->values());
    auto conn_id_arr = std::static_pointer_cast<arrow::StringArray>(
        connectors_values->GetFieldByName("connector_id"));
    auto conn_at_arr =
        std::static_pointer_cast<arrow::DoubleArray>(connectors_values->GetFieldByName("at"));

    std::shared_ptr<arrow::ListArray> surface_list;
    std::shared_ptr<arrow::StringArray> surface_value_arr;
    if (seg_surface_col) {
      surface_list = std::static_pointer_cast<arrow::ListArray>(seg_surface_col->chunk(chunk_idx));
      auto surface_values = std::static_pointer_cast<arrow::StructArray>(surface_list->values());
      if (surface_values->GetFieldByName("value")) {
        surface_value_arr =
            std::static_pointer_cast<arrow::StringArray>(surface_values->GetFieldByName("value"));
      }
    }

    std::shared_ptr<arrow::ListArray> speed_list;
    std::shared_ptr<arrow::StructArray> speed_max_arr;
    std::shared_ptr<arrow::Int32Array> speed_val_arr;
    if (seg_speed_col) {
      speed_list = std::static_pointer_cast<arrow::ListArray>(seg_speed_col->chunk(chunk_idx));
      auto speed_values = std::static_pointer_cast<arrow::StructArray>(speed_list->values());
      if (speed_values->GetFieldByName("max_speed")) {
        speed_max_arr =
            std::static_pointer_cast<arrow::StructArray>(speed_values->GetFieldByName("max_speed"));
        if (speed_max_arr->GetFieldByName("value")) {
          speed_val_arr =
              std::static_pointer_cast<arrow::Int32Array>(speed_max_arr->GetFieldByName("value"));
        }
      }
    }

    for (int64_t i = 0; i < id_arr->length(); i++) {
      if (id_arr->IsNull(i) || geom_arr->IsNull(i)) continue;
      if (connectors_list->IsNull(i) || connectors_list->value_length(i) < 2) continue;

      std::string road_class;
      if (!class_arr->IsNull(i)) road_class = class_arr->GetString(i);

      if (pedestrian_only && IsHighwayClass(road_class)) {
        skipped_segments++;
        continue;
      }

      int32_t geom_len;
      const uint8_t* geom_data = geom_arr->GetValue(i, &geom_len);
      auto shape = ParseWKBLineString(geom_data, geom_len);
      if (shape.size() < 2) continue;

      // Map connector "at" positions (0.0=start, 1.0=end) to shape point
      // indices
      int32_t conn_start = connectors_list->value_offset(i);
      int32_t conn_end = connectors_list->value_offset(i + 1);

      std::unordered_map<uint32_t, std::string> shape_to_connector;
      for (int32_t c = conn_start; c < conn_end; c++) {
        if (conn_id_arr->IsNull(c)) continue;
        std::string cid = conn_id_arr->GetString(c);
        double at_val = conn_at_arr->Value(c);

        uint32_t shape_idx;
        if (at_val <= 0.0)
          shape_idx = 0;
        else if (at_val >= 1.0)
          shape_idx = static_cast<uint32_t>(shape.size() - 1);
        else
          shape_idx = static_cast<uint32_t>(std::round(at_val * (shape.size() - 1)));

        shape_to_connector[shape_idx] = cid;
      }

      std::string seg_id = id_arr->GetString(i);
      OSMWay way(HashId(seg_id));
      way.set_node_count(static_cast<uint32_t>(shape.size()));
      way.set_road_class(ClassToRoadClass(road_class));
      way.set_use(ClassToUse(road_class));

      if (surface_list && !surface_list->IsNull(i) && surface_list->value_length(i) > 0) {
        int32_t surf_start = surface_list->value_offset(i);
        if (surface_value_arr && !surface_value_arr->IsNull(surf_start))
          way.set_surface(SurfaceFromString(surface_value_arr->GetString(surf_start)));
      }

      if (name_primary_arr && !name_primary_arr->IsNull(i)) {
        std::string name = name_primary_arr->GetString(i);
        if (!name.empty()) way.set_name_index(data.name_offset_map.index(name));
      }

      if (speed_list && !speed_list->IsNull(i) && speed_list->value_length(i) > 0) {
        int32_t spd_start = speed_list->value_offset(i);
        if (speed_val_arr && !speed_max_arr->IsNull(spd_start) &&
            !speed_val_arr->IsNull(spd_start)) {
          int32_t speed_kph = speed_val_arr->Value(spd_start);
          if (speed_kph > 0) {
            way.set_speed(static_cast<float>(speed_kph));
            way.set_speed_limit(static_cast<float>(speed_kph));
          }
        }
      }

      // Default: all modes in both directions
      SetAllAccess(way, true, true);
      way.set_pedestrian_forward(true);
      way.set_pedestrian_backward(true);
      way.set_bike_forward(true);
      way.set_bike_backward(true);

      // Restrict motor vehicles on foot-only ways
      if (road_class == "footway" || road_class == "pedestrian" || road_class == "steps" ||
          road_class == "path") {
        SetAllAccess(way, false, false);
        way.set_pedestrian_forward(true);
        way.set_pedestrian_backward(true);
        way.set_bike_forward(true);
        way.set_bike_backward(true);
      }

      if (road_class == "cycleway") {
        SetAllAccess(way, false, false);
        way.set_pedestrian_forward(true);
        way.set_pedestrian_backward(true);
        way.set_bike_forward(true);
        way.set_bike_backward(true);
        way.set_moped_forward(true);
        way.set_moped_backward(true);
      }

      way.set_drive_on_right(true);

      ways.push_back(way);

      for (uint32_t s = 0; s < shape.size(); s++) {
        OSMWayNode wn;
        wn.way_index = way_index;
        wn.way_shape_node_index = s;

        auto it = shape_to_connector.find(s);
        if (it != shape_to_connector.end()) {
          auto cit = connector_map.find(it->second);
          if (cit != connector_map.end()) {
            wn.node.set_id(cit->second.node_id);
            wn.node.set_latlng(cit->second.lng, cit->second.lat);
          } else {
            wn.node.set_id(synthetic_node_id++);
            wn.node.set_latlng(shape[s].first, shape[s].second);
          }
          wn.node.set_intersection(true);
          wn.node.set_access(kAllAccess);
        } else {
          wn.node.set_id(synthetic_node_id++);
          wn.node.set_latlng(shape[s].first, shape[s].second);
        }

        way_nodes.push_back(wn);
        total_way_nodes++;
      }

      way_index++;
    }
  }

  ways.flush();
  way_nodes.flush();

  data.osm_way_count = way_index;
  data.osm_way_node_count = total_way_nodes;
  data.node_count = connector_map.size();
  data.edge_count = way_index;
  data.max_changeset_id_ = 1;

  std::cout << "Parsed " << way_index << " ways, " << total_way_nodes << " way nodes" << std::endl;
  if (skipped_segments > 0)
    std::cout << "Skipped " << skipped_segments << " highway segments (pedestrian-only mode)"
              << std::endl;

  // Empty placeholder files for pipeline stages we don't populate
  {
    sequence<OSMAccess> access(access_file, true);
  }
  std::ofstream(cr_from_file, std::ios::binary | std::ios::trunc);
  std::ofstream(cr_to_file, std::ios::binary | std::ios::trunc);
  std::ofstream(linguistic_node_file, std::ios::binary | std::ios::trunc);

  return data;
}

}  // namespace overture
