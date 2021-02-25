#pragma once

#include <map>
#include <set>
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
  std::map<std::string, int> timeseriesPositionsString;
  std::map<std::string, std::string> parameterNameMap;
  std::map<std::string, int> parameterNameIdMap;
  std::vector<int> paramVector;
  std::map<std::string, int> specialPositions;
  std::vector<int> measurandIds;                            // all needed measurand ids
  std::map<int, std::set<int>> sensorNumberToMeasurandIds;  // sensor number -> measurand ids

  bool isDefaultSensor(int sensor_no, int measurand_id) const
  {
    if (sensorNumberToMeasurandIds.empty())
      return false;

    bool ret = true;

    if (sensorNumberToMeasurandIds.find(sensor_no) != sensorNumberToMeasurandIds.end())
    {
      const std::set<int> &mids = sensorNumberToMeasurandIds.at(sensor_no);
      if (mids.find(measurand_id) != mids.end())
        ret = false;
    }

    return ret;
  }
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
