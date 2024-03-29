#include "ParameterMap.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
std::string ParameterMap::getParameter(const std::string& name,
                                       const std::string& stationtype) const
{
  if (params.find(name) == params.end())
    return {};

  const StationParameters& stationparams = params.at(name);

  if (stationparams.find(stationtype) != stationparams.end())
    return stationparams.at(stationtype);

  if (stationtype != MAIN_MEASURAND_ID &&
      stationparams.find(DEFAULT_STATIONTYPE) != stationparams.end())
    return stationparams.at(DEFAULT_STATIONTYPE);

  return {};
}

// params_id_map
std::string ParameterMap::getParameterName(const std::string& id,
                                           const std::string& stationtype) const
{
  if (params_id_map.find(stationtype) != params_id_map.end())
  {
    const StationParameters& stationparams = params_id_map.at(stationtype);
    if (stationparams.find(id) != stationparams.end())
      return stationparams.at(id);
  }
  return {};
}

// stationtype -> id -> name
void ParameterMap::addStationParameterMap(const std::string& name,
                                          const std::map<std::string, std::string>& stationparams)
{
  params.insert(make_pair(name, stationparams));

  // Add item to params_id_map
  for (const auto& item : stationparams)
    params_id_map[item.first][item.second] = name;
}

const ParameterMap::StationParameters& ParameterMap::at(const std::string& name) const
{
  if (params.find(name) != params.end())
    return params.at(name);

  return emptymap;
}

ParameterMap::NameToStationParameterMap::const_iterator ParameterMap::find(
    const std::string& name) const
{
  return params.find(name);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
