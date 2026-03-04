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
  const auto params_it = params.find(name);
  if (params_it == params.end())
    return {};

  const StationParameters& stationparams = params_it->second;

  const auto stationtype_it = stationparams.find(stationtype);
  if (stationtype_it != stationparams.end())
    return stationtype_it->second;

  if (stationtype != MAIN_MEASURAND_ID)
  {
    const auto default_it = stationparams.find(DEFAULT_STATIONTYPE);
    if (default_it != stationparams.end())
      return default_it->second;
  }

  return {};
}

// params_id_map
std::string ParameterMap::getParameterName(const std::string& id,
                                           const std::string& stationtype) const
{
  const auto params_id_it = params_id_map.find(stationtype);
  if (params_id_it == params_id_map.end())
    return {};

  const auto stationstype_it = params_id_it->second.find(id);
  if (stationstype_it != params_id_it->second.end())
  {
    const StationParameters& stationparams = params_id_it->second;
    if (stationstype_it != stationparams.end())
      return stationstype_it->second;
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
  const auto params_it = params.find(name);
  if (params_it != params.end())
    return params_it->second;

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
