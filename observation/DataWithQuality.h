#pragma once

#include <boost/date_time/local_time/local_time.hpp>
#include <timeseries/TimeSeriesInclude.h>
#include <map>
#include <string>

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

// sensor ->
using SensorData = std::map<int, DataWithQuality>;
// measurand ->
using MeasurandData = std::map<int, SensorData>;
// observation time ->
using TimedMeasurandData = std::map<boost::local_time::local_date_time, MeasurandData>;
// fmisd ->
using StationTimedMeasurandData = std::map<int, TimedMeasurandData>;

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::StationTimedMeasurandData& data);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
