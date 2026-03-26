#pragma once

#include <timeseries/TimeSeriesInclude.h>
#include <map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct DataWithQuality
{
  DataWithQuality(const TS::Value& val,
                  const TS::Value& q,
                  const TS::Value& ds,
                  bool default_sensor_data)
      : value(val), data_quality(q), data_source(ds), is_default_sensor_data(default_sensor_data)
  {
  }
  DataWithQuality() = default;
  TS::Value value;
  TS::Value data_quality;
  TS::Value data_source;
  bool is_default_sensor_data{false};
};

// sensor_no -> value
using SensorData = std::map<int, DataWithQuality>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
