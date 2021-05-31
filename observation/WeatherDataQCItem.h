#pragma once

#include <boost/date_time/gregorian/formatters.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
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
  boost::posix_time::ptime obstime;
  boost::posix_time::ptime modified_last{boost::posix_time::not_a_date_time};
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
