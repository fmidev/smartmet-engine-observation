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
// A data structure for converting narrow table data structure to timeseries vectors

struct ObservationsMap
{
  // fmisid -> obstime -> measurand_id -> sensor_no -> value
  std::map<int,
           std::map<boost::local_time::local_date_time,
                    std::map<int, std::map<int, SmartMet::Spine::TimeSeries::Value>>>>
      data;
  std::map<
      int,
      std::map<boost::local_time::local_date_time,
               std::map<std::string, std::map<std::string, SmartMet::Spine::TimeSeries::Value>>>>
      dataWithStringParameterId;
  std::map<
      int,
      std::map<boost::local_time::local_date_time,
               std::map<std::string, std::map<std::string, SmartMet::Spine::TimeSeries::Value>>>>
      dataSourceWithStringParameterId;
  std::map<
      int,
      std::map<boost::local_time::local_date_time,
               std::map<std::string, std::map<std::string, SmartMet::Spine::TimeSeries::Value>>>>
      dataQualityWithStringParameterId;
  const std::map<int, std::map<int, int>> *default_sensors{
      nullptr};  // fmisid -> measurand_id -> sensor_no
};
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
