#include "StationLocation.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
static StationLocation emptyLocation = StationLocation();
static StationLocationVector emptyLocationVector = StationLocationVector();

const StationLocation& StationLocations::getLocation(int fmisid,
                                                     const boost::posix_time::ptime& t) const
{
  const StationLocationVector& allLocations = getAllLocations(fmisid);

  for (const auto& loc : allLocations)
  {
    if (t >= loc.location_start && t <= loc.location_end)
      return loc;
  }

  return emptyLocation;
}

const StationLocation& StationLocations::getCurrentLocation(int fmisid) const
{
  const StationLocationVector& allLocations = getAllLocations(fmisid);
  boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();

  for (const auto& loc : allLocations)
  {
    if (now >= loc.location_start && now <= loc.location_end)
      return loc;
  }

  return emptyLocation;
}

const StationLocationVector& StationLocations::getAllLocations(int fmisid) const
{
  if (find(fmisid) == end())
    return emptyLocationVector;

  return at(fmisid);
}

unsigned int StationLocations::getNumberOfLocations(int fmisid) const
{
  if (find(fmisid) == end())
    return 0;

  return at(fmisid).size();
}

bool StationLocations::isCurrentlyActive(int fmisid) const
{
  return (getCurrentLocation(fmisid).fmisid == -1);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::StationLocations& locations)
{
  for (const auto& item : locations)
  {
    out << item.first << std::endl;
    for (const auto& loc : item.second)
      out << "  " << loc.location_start << "..." << loc.location_end << ", " << loc.longitude
          << ", " << loc.latitude << ", " << loc.elevation << ", " << loc.x << ", " << loc.y << ", "
          << loc.country_id << ", " << loc.time_zone_name << std::endl;
  }
  return out;
}
