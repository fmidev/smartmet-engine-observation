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
  Fmi::DateTime modified_last{boost::posix_time::not_a_date_time};
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
