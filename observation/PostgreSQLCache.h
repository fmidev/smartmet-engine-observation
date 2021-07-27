#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "PostgreSQLCacheConnectionPool.h"
#include "PostgreSQLCacheParameters.h"
#include "Settings.h"
#include "StationtypeConfig.h"

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class PostgreSQLCache : public ObservationCache
{
 public:
  PostgreSQLCache(const std::string &name,
                  const EngineParametersPtr &p,
                  const Spine::ConfigBase &cfg);
  ~PostgreSQLCache() override;

  void initializeConnectionPool() override;
  void initializeCaches(int finCacheDuration,
                        int finMemoryCacheDuration,
                        int extCacheDuration,
                        int flashCacheDuration,
                        int flashMemoryCacheDuration) override;

  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(Settings &settings) override;
  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  bool dataAvailableInCache(const Settings &settings) const override;
  bool flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                             const boost::posix_time::ptime &endtime) const override;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const override;
  boost::posix_time::ptime getLatestFlashModifiedTime() const override;
  boost::posix_time::ptime getLatestFlashTime() const override;
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const override;
  void cleanFlashDataCache(
      const boost::posix_time::time_duration &timetokeep,
      const boost::posix_time::time_duration &timetokeep_memory) const override;

  boost::posix_time::ptime getLatestObservationModifiedTime() const override;
  boost::posix_time::ptime getLatestObservationTime() const override;
  std::size_t fillDataCache(const DataItems &cacheData) const override;
  void cleanDataCache(const boost::posix_time::time_duration &timetokeep,
                      const boost::posix_time::time_duration &timetokeep_memory) const override;
  boost::posix_time::ptime getLatestWeatherDataQCTime() const override;
  boost::posix_time::ptime getLatestWeatherDataQCModifiedTime() const override;
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const override;
  void cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const override;

  // RoadCloud
  bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestRoadCloudDataTime() const override;
  boost::posix_time::ptime getLatestRoadCloudCreatedTime() const override;
  std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const override;

  // NetAtmo
  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const override;
  boost::posix_time::ptime getLatestNetAtmoCreatedTime() const override;
  std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const override;

  // FmiIoT
  bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                              const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestFmiIoTDataTime() const override;
  boost::posix_time::ptime getLatestFmiIoTCreatedTime() const override;
  std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const override;

  void shutdown() override;

 private:
  std::unique_ptr<PostgreSQLCacheConnectionPool> itsConnectionPool;

  Fmi::TimeZones itsTimeZones;

  void readConfig(const Spine::ConfigBase &cfg);
  bool timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime) const;
  bool timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const;
  Spine::TimeSeries::TimeSeriesVectorPtr flashValuesFromPostgreSQL(Settings &settings) const;
  Spine::TimeSeries::TimeSeriesVectorPtr roadCloudValuesFromPostgreSQL(Settings &settings) const;
  Spine::TimeSeries::TimeSeriesVectorPtr netAtmoValuesFromPostgreSQL(Settings &settings) const;
  Spine::TimeSeries::TimeSeriesVectorPtr fmiIoTValuesFromPostgreSQL(Settings &settings) const;

  PostgreSQLCacheParameters itsParameters;

  // Cache available time interval to avoid unnecessary database requests. The interval
  // needs to be updated once at initialization, after a write, and before cleaning
  mutable Spine::MutexType itsTimeIntervalMutex;
  mutable boost::posix_time::ptime itsTimeIntervalStart;
  mutable boost::posix_time::ptime itsTimeIntervalEnd;

  mutable Spine::MutexType itsWeatherDataQCTimeIntervalMutex;
  mutable boost::posix_time::ptime itsWeatherDataQCTimeIntervalStart;
  mutable boost::posix_time::ptime itsWeatherDataQCTimeIntervalEnd;

  mutable Spine::MutexType itsFlashTimeIntervalMutex;
  mutable boost::posix_time::ptime itsFlashTimeIntervalStart;
  mutable boost::posix_time::ptime itsFlashTimeIntervalEnd;

  mutable Spine::MutexType itsRoadCloudTimeIntervalMutex;
  mutable boost::posix_time::ptime itsRoadCloudTimeIntervalStart;
  mutable boost::posix_time::ptime itsRoadCloudTimeIntervalEnd;

  mutable Spine::MutexType itsNetAtmoTimeIntervalMutex;
  mutable boost::posix_time::ptime itsNetAtmoTimeIntervalStart;
  mutable boost::posix_time::ptime itsNetAtmoTimeIntervalEnd;

  mutable Spine::MutexType itsFmiIoTTimeIntervalMutex;
  mutable boost::posix_time::ptime itsFmiIoTTimeIntervalStart;
  mutable boost::posix_time::ptime itsFmiIoTTimeIntervalEnd;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
