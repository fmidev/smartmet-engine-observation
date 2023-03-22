#pragma once

#include "DataWithQuality.h"
#include "LocationDataItem.h"
#include "ParameterMap.h"
#include "QueryMapping.h"
#include "Settings.h"
#include "StationInfo.h"
#include <macgyver/TimeZones.h>
#include <timeseries/TimeSeriesInclude.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using StationMap = std::map<int, Spine::Station>;
using TimestepsByFMISID = std::map<int, std::set<boost::local_time::local_date_time>>;
enum class AdditionalTimestepOption
{
  JustRequestedTimesteps,    // wms,wfs
  RequestedAndDataTimesteps  // timeseries
};

class DBQueryUtils
{
 public:
  virtual ~DBQueryUtils() = default;
  DBQueryUtils(const ParameterMapPtr &pm) : itsParameterMap(pm) {}

  // If timesteps requested, timeseries must have all requested and data timesteps (because of
  // aggregation) but wms, wfs must have only requested timesteps
  void setAdditionalTimestepOption(AdditionalTimestepOption opt);
  const ParameterMapPtr &getParameterMap() const { return itsParameterMap; }
  void setDebug(bool state) { itsDebug = state; }
  bool getDebug() const { return itsDebug; }

 protected:
  virtual QueryMapping buildQueryMapping(const Settings &settings,
                                         const std::string &stationtype,
                                         bool isWeatherDataQCTable) const;

  virtual StationTimedMeasurandData buildStationTimedMeasurandData(
      const LocationDataItems &observations,
      const Settings &settings,
      const Fmi::TimeZones &timezones,
      const StationMap &fmisid_to_station) const;

  virtual TS::TimeSeriesVectorPtr buildTimeseries(
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
      const StationTimedMeasurandData &station_data,
      const QueryMapping &qmap,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;

  virtual TimestepsByFMISID getValidTimeSteps(
      const Settings &settings,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones,
      std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results) const;

  virtual StationMap mapQueryStations(const Spine::Stations &stations,
                                      const std::set<int> &observed_fmisids) const;

  // Build fmisid1,fmisid2,... list
  virtual std::string buildSqlStationList(const Spine::Stations &stations,
                                          const std::set<std::string> &stationgroup_codes,
                                          const StationInfo &stationInfo,
                                          const TS::RequestLimits &requestLimits) const;

  std::string getSensorQueryCondition(
      const std::map<int, std::set<int>> &sensorNumberToMeasurandIds) const;

  const ParameterMapPtr &itsParameterMap;
  bool itsDebug{false};

 private:
  AdditionalTimestepOption itsGetRequestedAndDataTimesteps{
      AdditionalTimestepOption::RequestedAndDataTimesteps};
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
