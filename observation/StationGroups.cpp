#include "StationGroups.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
void StationGroups::addGroupPeriod(int station_id,
                                   const std::string& group_name,
                                   const Fmi::DateTime& starttime,
                                   const Fmi::DateTime& endtime)
{
  itsGroupPeriods[station_id][group_name].push_back(StationGroupPeriod(starttime, endtime));
}

std::set<std::string> StationGroups::getStationGroups(int station_id) const
{
  std::set<std::string> ret;

  if (itsGroupPeriods.find(station_id) != itsGroupPeriods.end())
  {
    const auto& group_periods = itsGroupPeriods.at(station_id);
    for (const auto& item : group_periods)
      ret.insert(item.first);
  }

  return ret;
}

bool StationGroups::belongsToGroup(int station_id,
                                   const Fmi::DateTime& starttime,
                                   const Fmi::DateTime& endtime) const
{
  if (itsGroupPeriods.find(station_id) == itsGroupPeriods.end())
    return false;

  // If at least one period overlaps
  StationGroupPeriod gp(starttime, endtime);
  const auto& group_periods = itsGroupPeriods.at(station_id);
  for (const auto& group_period_map : group_periods)
  {
    for (const auto& group_period : group_period_map.second)
    {
      if (group_period.intersects(gp))
        return true;
    }
  }

  return false;
}

std::set<int> StationGroups::getStations() const
{
  std::set<int> ret;

  for (const auto& item : itsGroupPeriods)
    ret.insert(item.first);

  return ret;
}

bool StationGroups::groupOK(int station_id, const std::string& station_type) const
{
  if (itsGroupPeriods.find(station_id) == itsGroupPeriods.end())
    return false;

  const auto& group_periods = itsGroupPeriods.at(station_id);

  return (group_periods.find(station_type) != group_periods.end());
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
