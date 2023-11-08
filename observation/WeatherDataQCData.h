#pragma once

#include <macgyver/DateTime.h>
#include <boost/optional.hpp>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct WeatherDataQCData
{
  std::vector<boost::optional<int>> fmisidsAll;
  std::vector<Fmi::DateTime> obstimesAll;
  std::vector<boost::optional<double>> longitudesAll;
  std::vector<boost::optional<double>> latitudesAll;
  std::vector<boost::optional<double>> elevationsAll;
  std::vector<boost::optional<std::string>> stationtypesAll;
  std::vector<boost::optional<int>> parametersAll;
  std::vector<boost::optional<double>> data_valuesAll;
  std::vector<boost::optional<int>> sensor_nosAll;
  std::vector<boost::optional<int>> data_qualityAll;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
