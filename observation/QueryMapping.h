#pragma once

#include <map>
#include <string>
#include <vector>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct QueryMapping
{
  std::map<int, int> timeseriesPositions;
  std::map<std::string, int> timeseriesPositionsString;
  std::map<std::string, std::string> parameterNameMap;
  std::vector<int> paramVector;
  std::map<std::string, int> specialPositions;
  std::vector<int> measurandIds;  // all needed measurand ids
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
