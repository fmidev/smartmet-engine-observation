#pragma once

#include <macgyver/DateTime.h>
#include <optional>
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
  std::optional<double> value;
  int fmisid;
  int sensor_no;
  int flag;

  std::size_t hash_value() const;
};

using WeatherDataQCItems = std::vector<WeatherDataQCItem>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
