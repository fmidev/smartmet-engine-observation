#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <string>

namespace pt = boost::posix_time;

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
  pt::ptime obstime;
  std::string parameter;
  int sensor_no;
  double value;
  int flag;

 private:
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
