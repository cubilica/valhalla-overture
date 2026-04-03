#include <valhalla/baldr/tilehierarchy.h>
#include <valhalla/mjolnir/graphbuilder.h>
#include <valhalla/mjolnir/graphenhancer.h>
#include <valhalla/mjolnir/graphfilter.h>
#include <valhalla/mjolnir/graphvalidator.h>
#include <valhalla/mjolnir/hierarchybuilder.h>
#include <valhalla/mjolnir/shortcutbuilder.h>

#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <filesystem>
#include <iostream>
#include <string>

#include "overture_parser.h"

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: overture_build_tiles <valhalla.json> <connectors.parquet> "
                 "<segments.parquet> [--pedestrian-only]\n";
    return 1;
  }

  std::string config_path = argv[1];
  std::string connectors_path = argv[2];
  std::string segments_path = argv[3];
  bool pedestrian_only = false;
  for (int i = 4; i < argc; i++) {
    if (std::string(argv[i]) == "--pedestrian-only") pedestrian_only = true;
  }

  boost::property_tree::ptree config;
  boost::property_tree::read_json(config_path, config);

  std::string tile_dir = config.get<std::string>("mjolnir.tile_dir");
  if (tile_dir.back() != '/') tile_dir.push_back('/');

  for (const auto& level : valhalla::baldr::TileHierarchy::levels()) {
    auto level_dir = tile_dir + std::to_string(level.level);
    if (std::filesystem::exists(level_dir) && !std::filesystem::is_empty(level_dir))
      std::filesystem::remove_all(level_dir);
  }
  std::filesystem::create_directories(tile_dir);

  // Intermediate files (same names valhalla uses internally)
  std::string ways_bin = tile_dir + "ways.bin";
  std::string way_nodes_bin = tile_dir + "way_nodes.bin";
  std::string nodes_bin = tile_dir + "nodes.bin";
  std::string edges_bin = tile_dir + "edges.bin";
  std::string access_bin = tile_dir + "access.bin";
  std::string cr_from_bin = tile_dir + "complex_from_restrictions.bin";
  std::string cr_to_bin = tile_dir + "complex_to_restrictions.bin";
  std::string linguistic_node_bin = tile_dir + "linguistics_node.bin";
  std::string new_to_old_bin = tile_dir + "new_nodes_to_old_nodes.bin";
  std::string old_to_new_bin = tile_dir + "old_nodes_to_new_nodes.bin";

  std::cout << "Parsing Overture data..." << std::endl;
  auto osm_data =
      overture::Parse(config, connectors_path, segments_path, ways_bin, way_nodes_bin, access_bin,
                      cr_from_bin, cr_to_bin, linguistic_node_bin, pedestrian_only);

  std::cout << "Constructing edges..." << std::endl;
  auto tiles = valhalla::mjolnir::GraphBuilder::BuildEdges(config, ways_bin, way_nodes_bin,
                                                           nodes_bin, edges_bin);

  std::cout << "Building graph tiles..." << std::endl;
  valhalla::mjolnir::GraphBuilder::Build(config, osm_data, ways_bin, way_nodes_bin, nodes_bin,
                                         edges_bin, cr_from_bin, cr_to_bin, linguistic_node_bin,
                                         tiles);

  std::cout << "Enhancing graph..." << std::endl;
  valhalla::mjolnir::GraphEnhancer::Enhance(config, osm_data, access_bin);

  std::cout << "Building hierarchy..." << std::endl;
  valhalla::mjolnir::HierarchyBuilder::Build(config, new_to_old_bin, old_to_new_bin);

  std::cout << "Building shortcuts..." << std::endl;
  valhalla::mjolnir::ShortcutBuilder::Build(config);

  std::cout << "Filtering graph..." << std::endl;
  valhalla::mjolnir::GraphFilter::Filter(config);

  std::cout << "Validating and binning edges..." << std::endl;
  valhalla::mjolnir::GraphValidator::Validate(config);

  std::cout << "Cleaning up..." << std::endl;
  auto remove_file = [](const std::string& f) {
    if (std::filesystem::exists(f)) std::filesystem::remove(f);
  };
  remove_file(ways_bin);
  remove_file(way_nodes_bin);
  remove_file(nodes_bin);
  remove_file(edges_bin);
  remove_file(access_bin);
  remove_file(cr_from_bin);
  remove_file(cr_to_bin);
  remove_file(linguistic_node_bin);
  remove_file(new_to_old_bin);
  remove_file(old_to_new_bin);

  std::cout << "Done! Tiles written to " << tile_dir << std::endl;
  return 0;
}
