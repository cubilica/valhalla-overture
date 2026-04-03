#pragma once

#include <valhalla/mjolnir/osmdata.h>

#include <boost/property_tree/ptree.hpp>
#include <string>

namespace overture {

valhalla::mjolnir::OSMData Parse(const boost::property_tree::ptree& config,
                                 const std::string& connectors_path,
                                 const std::string& segments_path, const std::string& ways_file,
                                 const std::string& way_nodes_file, const std::string& access_file,
                                 const std::string& cr_from_file, const std::string& cr_to_file,
                                 const std::string& linguistic_node_file,
                                 bool pedestrian_only = false);

}  // namespace overture
