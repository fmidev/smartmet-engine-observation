#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>

namespace pt = boost::posix_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class LocationItem
{
 public:
  int location_id;
  int fmisid;
  int country_id;
  pt::ptime location_start;
  pt::ptime location_end;
  double longitude;
  double latitude;
  double x;
  double y;
  double elevation;
  std::string time_zone_name;
  std::string time_zone_abbrev;

 private:
};

using LocationItems = std::vector<LocationItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
