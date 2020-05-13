#pragma once

#include <boost/date_time/posix_time/posix_time.hpp>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class StationLocation
{
 public:
  int location_id;
  int fmisid;
  int country_id;
  boost::posix_time::ptime location_start;
  boost::posix_time::ptime location_end;
  double longitude;
  double latitude;
  double x;
  double y;
  double elevation;
  std::string time_zone_name;
  std::string time_zone_abbrev;

  StationLocation()
      : location_id(-1),
        fmisid(-1),
        location_start(boost::posix_time::not_a_date_time),
        location_end(boost::posix_time::not_a_date_time),
        longitude(-1.0),
        latitude(-1.0),
        x(-1.0),
        y(-1.0),
        elevation(-1.0),
        time_zone_name(""),
        time_zone_abbrev("")
  {
  }
};

// Vector of all locations
using StationLocationVector = std::vector<StationLocation>;

// FMISID -> StationLocations
using StatationLocationMap = std::map<int, StationLocationVector>;

class StationLocations : public StatationLocationMap
{
 public:
  const StationLocation& getLocation(int fmisid, const boost::posix_time::ptime& t) const;
  const StationLocation& getCurrentLocation(int fmisid) const;
  const StationLocationVector& getAllLocations(int fmisid) const;
  unsigned int getNumberOfLocations(int fmisid) const;
  bool isCurrentlyActive(int fmisid) const;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::StationLocations& locations);
