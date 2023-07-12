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
  int location_id = -1;
  int fmisid = -1;
  int country_id = -1;
  boost::posix_time::ptime location_start{boost::posix_time::not_a_date_time};
  boost::posix_time::ptime location_end{boost::posix_time::not_a_date_time};
  double longitude = -1;
  double latitude = -1;
  double x = -1;
  double y = -1;
  double elevation = -1;
  std::string time_zone_name;
  std::string time_zone_abbrev;

  StationLocation() = default;
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
