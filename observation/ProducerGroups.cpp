#include "ProducerGroups.h"
#include <macgyver/DateTime.h>
#include <macgyver/StringConversion.h>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
void ProducerGroups::addGroupPeriod(const std::string& group_name,
                                    unsigned int producer_id,
                                    const Fmi::DateTime& starttime,
                                    const Fmi::DateTime& endtime)
{
  itsGroupPeriods[group_name][producer_id].push_back(ProducerGroupPeriod(starttime, endtime));
}

std::set<unsigned int> ProducerGroups::getProducerIds(const std::string& group_name,
                                                      const Fmi::DateTime& starttime,
                                                      const Fmi::DateTime& endtime) const
{
  std::set<unsigned int> ret;

  if (itsGroupPeriods.find(group_name) != itsGroupPeriods.end())
  {
    ProducerGroupPeriod gp(starttime, endtime);
    const auto& group_periods = itsGroupPeriods.at(group_name);
    for (const auto& group_period_map : group_periods)
    {
      for (const auto& group_period : group_period_map.second)
      {
        if (group_period.intersects(gp))
        {
          ret.insert(group_period_map.first);
          break;
        }
      }
    }
  }

  return ret;
}

std::set<std::string> ProducerGroups::getProducerIdsString(const std::string& group_name,
                                                           const Fmi::DateTime& starttime,
                                                           const Fmi::DateTime& endtime) const
{
  std::set<std::string> ret;

  if (itsGroupPeriods.find(group_name) != itsGroupPeriods.end())
  {
    ProducerGroupPeriod gp(starttime, endtime);
    const auto& group_periods = itsGroupPeriods.at(group_name);
    for (const auto& group_period_map : group_periods)
    {
      for (const auto& group_period : group_period_map.second)
      {
        if (group_period.intersects(gp))
        {
          ret.insert(Fmi::to_string(group_period_map.first));
          break;
        }
      }
    }
  }

  return ret;
}

std::set<std::string> ProducerGroups::getProducerGroups() const
{
  std::set<std::string> ret;

  for (const auto& producer_group_periods : itsGroupPeriods)
    ret.insert(producer_group_periods.first);

  return ret;
}

void ProducerGroups::replaceProducerIds(const std::string& group_name_from,
                                        const std::string& group_name_to)
{
  if (itsGroupPeriods.find(group_name_from) != itsGroupPeriods.end())
    itsGroupPeriods[group_name_to] = itsGroupPeriods.at(group_name_from);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
