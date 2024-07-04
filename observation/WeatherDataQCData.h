#pragma once

#include <macgyver/DateTime.h>
#include <optional>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct WeatherDataQCData
{
  std::vector<std::optional<int>> fmisidsAll;
  std::vector<Fmi::DateTime> obstimesAll;
  std::vector<std::optional<double>> longitudesAll;
  std::vector<std::optional<double>> latitudesAll;
  std::vector<std::optional<double>> elevationsAll;
  std::vector<std::optional<std::string>> stationtypesAll;
  std::vector<std::optional<int>> parametersAll;
  std::vector<std::optional<double>> data_valuesAll;
  std::vector<std::optional<int>> sensor_nosAll;
  std::vector<std::optional<int>> data_qualityAll;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
