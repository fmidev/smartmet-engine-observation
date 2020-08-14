#pragma once

#include <boost/date_time/posix_time/ptime.hpp>
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
  std::vector<boost::posix_time::ptime> obstimesAll;
  std::vector<boost::optional<double>> longitudesAll;
  std::vector<boost::optional<double>> latitudesAll;
  std::vector<boost::optional<double>> elevationsAll;
  std::vector<boost::optional<std::string>> parametersAll;
  std::vector<boost::optional<double>> data_valuesAll;
  std::vector<boost::optional<int>> sensor_nosAll;
  std::vector<boost::optional<int>> data_qualityAll;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
