#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class WeatherDataQCItem
{
 public:
  int fmisid;
  boost::posix_time::ptime obstime;
  std::string parameter;
  int sensor_no;
  double value;
  int flag;

  std::size_t hash_value() const;
};

using WeatherDataQCItems = std::vector<WeatherDataQCItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
