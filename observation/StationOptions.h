#pragma once

#include <set>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/optional.hpp>
#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct StationOptions
{
  std::set<int> fmisid;
  std::set<int> lpnn;
  std::set<int> wmo;
  std::set<int> rwsid;
  std::string type;
  std::string name;
  std::string iso2;
  std::string region;
  std::string timeformat;
  boost::posix_time::ptime start_time;
  boost::posix_time::ptime end_time;
  boost::optional<Spine::BoundingBox> bbox;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
