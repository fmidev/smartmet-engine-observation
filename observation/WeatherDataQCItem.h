#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <macgyver/DateTime.h>
#include <boost/optional.hpp>
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
  Fmi::DateTime obstime;
  Fmi::DateTime modified_last{Fmi::DateTime::NOT_A_DATE_TIME};
  std::string parameter;
  boost::optional<double> value;
  int fmisid;
  int sensor_no;
  int flag;

  std::size_t hash_value() const;
};

using WeatherDataQCItems = std::vector<WeatherDataQCItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
