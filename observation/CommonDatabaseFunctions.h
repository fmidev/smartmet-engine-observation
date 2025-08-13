#pragma once

#include "DBQueryUtils.h"
#include "LocationDataItem.h"
#include "MeasurandInfo.h"
#include "StationtypeConfig.h"
#include "Utils.h"

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

  ~CommonDatabaseFunctions() override = default;

  CommonDatabaseFunctions() = delete;
  CommonDatabaseFunctions(const CommonDatabaseFunctions &other) = delete;
  CommonDatabaseFunctions(CommonDatabaseFunctions &&other) = delete;
  CommonDatabaseFunctions &operator=(const CommonDatabaseFunctions &other) = delete;
  CommonDatabaseFunctions &operator=(CommonDatabaseFunctions &&other) = delete;

  const StationtypeConfig &getStationtypeConfig() const { return itsStationtypeConfig; }

  virtual TS::TimeSeriesVectorPtr getFlashData(const Settings &settings,
                                               const Fmi::TimeZones &timezones) = 0;
  virtual FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime,
                                    const Spine::TaggedLocationList &locations) = 0;

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

  virtual TS::TimeSeriesVectorPtr getWeatherDataQCData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const Fmi::TimeZones &timezones,
      const std::unique_ptr<ObservationMemoryCache> &extMemoryCache);

  virtual TS::TimeSeriesVectorPtr getWeatherDataQCData(
      const Spine::Stations &stations,
      const Settings &settings,
      const StationInfo &stationInfo,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones,
      const std::unique_ptr<ObservationMemoryCache> &extMemoryCache);

  virtual TS::TimeSeriesVectorPtr getObservationDataForMovingStations(
      const Settings &settings,
      const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
      const Fmi::TimeZones &timezones) = 0;

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

  virtual std::string getWeatherDataQCParams(const std::set<std::string> &param_set) const;

  virtual void fetchWeatherDataQCData(const std::string &sqlStmt,
                                      const StationInfo &stationInfo,
                                      const std::set<std::string> &stationgroup_codes,
                                      const TS::RequestLimits &requestLimits,
                                      LocationDataItems &weatherDataQCData) = 0;
  virtual std::string sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                     const std::string &params,
                                                     const std::string &station_ids) const = 0;

 protected:
  const StationtypeConfig &itsStationtypeConfig;
};  // namespace Observation

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
