#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{

using StationGroupPeriod = boost::posix_time::time_period; // group period
using StationGroupPeriodVector = std::vector<StationGroupPeriod>; // group periods
using StationGroupPeriodMap = std::map<std::string, StationGroupPeriodVector>; // group_name -> group periods
using StationGroupPeriods = std::map<int, StationGroupPeriodMap>; // station_id -> group periods
  
class StationGroups
{
public:  
  void addGroupPeriod(int station_id,
					  const std::string& group_name, 
					  const boost::posix_time::ptime& starttime,
					  const boost::posix_time::ptime& endtime);
  std::set<std::string> getStationGroups(int station_id) const;
  std::set<int> getStations() const;
  bool belongsToGroup(int station_id,
					  const boost::posix_time::ptime& starttime,
					  const boost::posix_time::ptime& endtime) const;
  bool groupOK(int station_id, const std::string& station_type) const;

private:
  StationGroupPeriods itsGroupPeriods;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
