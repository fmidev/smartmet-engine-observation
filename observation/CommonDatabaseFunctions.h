#pragma once

#include "LocationDataItem.h"
#include "DataWithQuality.h"
#include "ParameterMap.h"
#include "QueryMapping.h"
#include "Settings.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include "Utils.h"
#include "WeatherDataQCData.h"
#include <boost/atomic.hpp>
#include <macgyver/TimeZones.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using StationMap = std::map<int, Spine::Station>;

class CommonDatabaseFunctions
{
 public:
  CommonDatabaseFunctions(const StationtypeConfig &stc, const ParameterMapPtr &pm)
      : itsStationtypeConfig(stc), itsParameterMap(pm)
  {
  }

  virtual ~CommonDatabaseFunctions() {}

  virtual std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                     const std::string &params,
                                                     const std::string &station_ids) const = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Fmi::TimeZones &timezones);

  virtual Spine::TimeSeries::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                                              const Fmi::TimeZones &timezones) = 0;
  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const SmartMet::Spine::TaggedLocationList &locations) = 0;

  Spine::TimeSeries::TimeSeriesVectorPtr getWeatherDataQCData(const Spine::Stations &stations,
                                                              const Settings &settings,
                                                              const StationInfo &stationInfo,
                                                              const Fmi::TimeZones &timezones);

  Spine::TimeSeries::TimeSeriesVectorPtr getWeatherDataQCData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  virtual void fetchWeatherDataQCData(const std::string &sqlStmt,
                                      const StationInfo &stationInfo,
                                      const std::set<std::string> &stationgroup_codes,
                                      const QueryMapping &qmap,
                                      WeatherDataQCData &weatherDataQCData) = 0;

  const StationtypeConfig &getStationtypeConfig() const { return itsStationtypeConfig; }
  const ParameterMapPtr &getParameterMap() const { return itsParameterMap; }
  void setDebug(bool state) { itsDebug = state; }
  bool getDebug() const { return itsDebug; }
  virtual std::string getWeatherDataQCParams(const std::set<std::string> &param_set) const;

 protected:
  QueryMapping buildQueryMapping(const Spine::Stations &stations,
                                 const Settings &settings,
                                 const ParameterMapPtr &parameterMap,
                                 const std::string &stationtype,
                                 bool isWeatherDataQCTable) const;
  std::string getSensorQueryCondition(
      const std::map<int, std::set<int>> &sensorNumberToMeasurandIds) const;
  bool isDataSourceField(const std::string &fieldname) const;
  bool isDataQualityField(const std::string &fieldname) const;
  bool isDataSourceOrDataQualityField(const std::string &fieldname) const;
  void solveMeasurandIds(const std::vector<std::string> &parameters,
                         const ParameterMapPtr &parameterMap,
                         const std::string &stationType,
                         std::multimap<int, std::string> &parameterIDs) const;
  StationMap mapQueryStations(const Spine::Stations &stations,
                              const std::set<int> &observed_fmisids) const;
  std::string buildSqlStationList(const Spine::Stations &stations,
                                  const std::set<std::string> &stationgroup_codes,
                                  const StationInfo &stationInfo) const;
  StationTimedMeasurandData buildStationTimedMeasurandData(const LocationDataItems &observations,
														   const Settings &settings,
														   const Fmi::TimeZones &timezones,
														   const StationMap &fmisid_to_station) const;
  Spine::TimeSeries::TimeSeriesVectorPtr buildTimeseries(
      const Spine::Stations &stations,
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
	  const StationTimedMeasurandData& station_data,
      const QueryMapping &qmap,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;

  void addSpecialFieldsToTimeSeries(const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
									int fmisid,
									const TimedMeasurandData& data,
									const std::set<boost::local_time::local_date_time>& valid_timesteps,
									const std::map<std::string, int> &specialPositions,
									const std::map<std::string, std::string> &parameterNameMap,
									bool addDataSourceField) const;   

  void addParameterToTimeSeries(
								const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
								const std::pair<boost::local_time::local_date_time, MeasurandData> &dataItem,
								int fmisid,
								const std::map<std::string, int> &specialPositions,
								const std::map<std::string, int> &parameterNameIdMap,
								const std::map<std::string, int> &timeseriesPositions,
								const std::string &stationtype,
								const Spine::Station &station,
								const Settings &settings) const;

  void addSpecialParameterToTimeSeries(
      const std::string &paramname,
      const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const Spine::Station &station,
      const int pos,
      const std::string &stationtype,
      const boost::local_time::local_date_time &obstime,
      const std::string &timezone) const;

  const StationtypeConfig &itsStationtypeConfig;
  const ParameterMapPtr &itsParameterMap;
  bool itsDebug{false};

 private:
};  // namespace Observation

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
