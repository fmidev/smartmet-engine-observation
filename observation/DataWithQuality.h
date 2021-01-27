#pragma once

#include <boost/date_time/local_time/local_time.hpp>
#include <spine/TimeSeries.h>
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
  DataWithQuality(const Spine::TimeSeries::Value& val, const Spine::TimeSeries::Value& q, const Spine::TimeSeries::Value& ds, bool default_sensor_data)
	: value(val), data_quality(q), data_source(ds), is_default_sensor_data(default_sensor_data) {}
  DataWithQuality() {}
  Spine::TimeSeries::Value value;
  Spine::TimeSeries::Value data_quality;
  Spine::TimeSeries::Value data_source;
  bool is_default_sensor_data{false};
};

// sensor ->
using SensorData = std::map<int, DataWithQuality>;
// measurand ->
using MeasurandData = std::map<int,SensorData>;
// observation time ->
using TimedMeasurandData = std::map<boost::local_time::local_date_time, MeasurandData>;
// fmisd ->
using StationTimedMeasurandData = std::map<int, TimedMeasurandData>;

std::ostream& operator<<(std::ostream& out, const SmartMet::Engine::Observation::StationTimedMeasurandData& data);

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

