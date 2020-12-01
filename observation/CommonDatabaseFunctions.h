#pragma once

#include "LocationDataItem.h"
#include "ObservationsMap.h"
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
                                      std::map<int, std::map<int, int>> &default_sensors,
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
  Spine::TimeSeries::Value getDefaultSensorValue(
      const std::map<int, std::map<int, int>> *defaultSensors,
      const std::map<std::string, Spine::TimeSeries::Value> &sensorValues,
      int measurandId,
      int fmisid) const;
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
  ObservationsMap buildObservationsMap(const LocationDataItems &observations,
                                       const Settings &settings,
                                       const Fmi::TimeZones &timezones,
                                       const StationMap &fmisid_to_station) const;
  Spine::TimeSeries::TimeSeriesVectorPtr buildTimeseries(
      const Spine::Stations &stations,
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
      const LocationDataItems &observations,
      ObservationsMap &obsmap,
      const QueryMapping &qmap,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;
  Spine::TimeSeries::TimeSeriesVectorPtr buildTimeseriesAllTimeSteps(
      const Spine::Stations &stations,
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
      ObservationsMap &obsmap,
      const QueryMapping &qmap) const;
  Spine::TimeSeries::TimeSeriesVectorPtr buildTimeseriesLatestTimeStep(
      const Spine::Stations &stations,
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
      const LocationDataItems &observations,
      ObservationsMap &obsmap,
      const QueryMapping &qmap,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;
  Spine::TimeSeries::TimeSeriesVectorPtr buildTimeseriesListedTimeSteps(
      const Spine::Stations &stations,
      const Settings &settings,
      const std::string &stationtype,
      const StationMap &fmisid_to_station,
      const LocationDataItems &observations,
      ObservationsMap &obsmap,
      const QueryMapping &qmap,
      const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) const;
  void appendWeatherParameters(const Spine::Station &s,
                               const boost::local_time::local_date_time &t,
                               const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
                               const std::string &stationtype,
                               const StationMap &fmisid_to_station,
                               const LocationDataItems &observations,
                               ObservationsMap &obsmap,
                               const QueryMapping &qmap,
                               const Settings &settings) const;

  void addSpecialFieldsToTimeSeries(
      const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const std::map<boost::local_time::local_date_time,
                     std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
          &stationData,
      int fmisid,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<int, std::map<int, int>> *defaultSensors,
      const std::list<boost::local_time::local_date_time> &tlist,
      bool addDataSourceField) const;
  void addParameterToTimeSeries(
      const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
      const std::pair<boost::local_time::local_date_time,
                      std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
          &dataItem,
      int fmisid,
      const std::map<std::string, int> &specialPositions,
      const std::map<std::string, std::string> &parameterNameMap,
      const std::map<std::string, int> &timeseriesPositions,
      const std::map<int, std::set<int>> &sensorNumberToMeasurandIds,
      const std::map<int, std::map<int, int>> *defaultSensors,  // measurand_id -> sensor_no
      const std::string &stationtype,
      const Spine::Station &station,
      const Settings &settings) const;
  void addEmptyValuesToTimeSeries(Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
                                  const boost::local_time::local_date_time &obstime,
                                  const std::map<std::string, int> &specialPositions,
                                  const std::map<std::string, std::string> &parameterNameMap,
                                  const std::map<std::string, int> &timeseriesPositions,
                                  const std::string &stationtype,
                                  const Spine::Station &station,
                                  const std::string &timezone) const;
  void addSmartSymbolToTimeSeries(
      const int pos,
      const Spine::Station &s,
      const boost::local_time::local_date_time &time,
      const std::string &stationtype,
      const std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> &stationData,
      const std::map<int,
                     std::map<boost::local_time::local_date_time,
                              std::map<int, std::map<int, Spine::TimeSeries::Value>>>> &data,
      int fmisid,
      const std::map<int, std::map<int, int>> *defaultSensors,
      const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns) const;
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
