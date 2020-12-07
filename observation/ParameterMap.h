#pragma once

#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <vector>

#include "RoadAndForeignIds.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ParameterMap
{
 public:
  ParameterMap() {}
  ParameterMap(const ParameterMap&) = delete;

  using StationParameters = std::map<std::string, std::string>;
  using NameToStationParameterMap = std::map<std::string, StationParameters>;

  std::string getParameter(const std::string& name, const std::string& stationtype) const;
  std::string getParameterName(const std::string& id, const std::string& stationtype) const;

  void addStationParameterMap(const std::string& name,
                              std::map<std::string, std::string> stationparams);
  const StationParameters& at(const std::string& name) const;
  NameToStationParameterMap::const_iterator find(const std::string& name) const;

  NameToStationParameterMap::const_iterator begin() const { return params.begin(); }
  NameToStationParameterMap::const_iterator end() const { return params.end(); }
  const RoadAndForeignIds& getRoadAndForeignIds() const { return road_foregn_ids; }

 private:
  NameToStationParameterMap params;
  NameToStationParameterMap params_id_map;
  StationParameters emptymap;
  RoadAndForeignIds road_foregn_ids;
};

using ParameterMapPtr = std::shared_ptr<const ParameterMap>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
