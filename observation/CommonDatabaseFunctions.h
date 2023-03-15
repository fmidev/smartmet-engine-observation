#pragma once

#include "DBQueryUtils.h"
#include "StationtypeConfig.h"
#include "Utils.h"
#include "WeatherDataQCData.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationMemoryCache;

class CommonDatabaseFunctions : public DBQueryUtils
{
 public:
  CommonDatabaseFunctions(const StationtypeConfig &stc, const ParameterMapPtr &pm)
      : DBQueryUtils(pm), itsStationtypeConfig(stc)
  {
  }

  virtual ~CommonDatabaseFunctions() = default;

  CommonDatabaseFunctions() = delete;
  CommonDatabaseFunctions(const CommonDatabaseFunctions &other) = delete;
  CommonDatabaseFunctions(CommonDatabaseFunctions &&other) = delete;
  CommonDatabaseFunctions &operator=(const CommonDatabaseFunctions &other) = delete;
  CommonDatabaseFunctions &operator=(CommonDatabaseFunctions &&other) = delete;

  virtual std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                     const std::string &params,
                                                     const std::string &station_ids) const = 0;

  virtual TS::TimeSeriesVectorPtr getObservationDataForMovingStations(
      const Settings &settings,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) = 0;

  virtual TS::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Fmi::TimeZones &timezones,
      const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache);

  virtual TS::TimeSeriesVectorPtr getObservationData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones,
      const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache) = 0;

  virtual TS::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                               const Fmi::TimeZones &timezones) = 0;
  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) = 0;

  TS::TimeSeriesVectorPtr getWeatherDataQCData(const Spine::Stations &stations,
                                               const Settings &settings,
                                               const StationInfo &stationInfo,
                                               const Fmi::TimeZones &timezones);

  TS::TimeSeriesVectorPtr getWeatherDataQCData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones);

  virtual void fetchWeatherDataQCData(const std::string &sqlStmt,
                                      const StationInfo &stationInfo,
                                      const std::set<std::string> &stationgroup_codes,
                                      const TS::RequestLimits &requestLimits,
                                      WeatherDataQCData &weatherDataQCData) = 0;

  const StationtypeConfig &getStationtypeConfig() const { return itsStationtypeConfig; }

  virtual std::string getWeatherDataQCParams(const std::set<std::string> &param_set) const;
  virtual TS::TimeSeriesVectorPtr getMagnetometerData(const Spine::Stations &stations,
                                                      const Settings &settings,
                                                      const StationInfo &stationInfo,
                                                      const Fmi::TimeZones &timezones);
  virtual TS::TimeSeriesVectorPtr getMagnetometerData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) = 0;

 protected:
  const StationtypeConfig &itsStationtypeConfig;
};  // namespace Observation

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
