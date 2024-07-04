#pragma once

#include <memory>
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
static const std::string DEFAULT_STATIONTYPE = "default";
static const std::string DATA_SOURCE = "data_source";
static const std::string MAIN_MEASURAND_ID = "main_measurand_id";

class ParameterMap
{
 public:
  ~ParameterMap() = default;
  ParameterMap() = default;
  ParameterMap(const ParameterMap&) = delete;
  ParameterMap(ParameterMap&&) = delete;
  ParameterMap& operator=(const ParameterMap&) = delete;
  ParameterMap& operator=(ParameterMap&&) = delete;

  using StationParameters = std::map<std::string, std::string>;
  using NameToStationParameterMap = std::map<std::string, StationParameters>;

  std::string getParameter(const std::string& name, const std::string& stationtype) const;
  std::string getParameterName(const std::string& id, const std::string& stationtype) const;

  void addStationParameterMap(const std::string& name,
                              const std::map<std::string, std::string>& stationparams);
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
