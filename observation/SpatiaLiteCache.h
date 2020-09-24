#pragma once

#include "EngineParameters.h"
#include "FlashMemoryCache.h"
#include "InsertStatus.h"
#include "ObservationCache.h"
#include "Settings.h"
#include "SpatiaLiteCacheParameters.h"
#include "SpatiaLiteConnectionPool.h"
#include "StationtypeConfig.h"

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class SpatiaLiteCache : public ObservationCache
{
 public:
  SpatiaLiteCache(const std::string &name, const EngineParametersPtr &p, Spine::ConfigBase &cfg);
  ~SpatiaLiteCache();

  void initializeConnectionPool();
  void initializeCaches(int finCacheDuration,
                        int finMemoryCacheDuration,
                        int extCacheDuration,
                        int flashCacheDuration,
                        int flashMemoryCacheDuration);

  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(Settings &settings);
  Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions);

  bool dataAvailableInCache(const Settings &settings) const;
  bool flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                             const boost::posix_time::ptime &endtime) const;
  FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime,
                            const Spine::TaggedLocationList &locations) const;
  boost::posix_time::ptime getLatestFlashModifiedTime() const;
  boost::posix_time::ptime getLatestFlashTime() const;
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const;
  void cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep,
                           const boost::posix_time::time_duration &timetokeep_memory) const;
  boost::posix_time::ptime getLatestObservationModifiedTime() const;
  boost::posix_time::ptime getLatestObservationTime() const;
  std::size_t fillDataCache(const DataItems &cacheData) const;
  void cleanDataCache(const boost::posix_time::time_duration &timetokeep,
                      const boost::posix_time::time_duration &timetokeep_memory) const;
  boost::posix_time::ptime getLatestWeatherDataQCTime() const;
  boost::posix_time::ptime getLatestWeatherDataQCModifiedTime() const;
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const;
  void cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const;

  // RoadCloud
  bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestRoadCloudDataTime() const;
  boost::posix_time::ptime getLatestRoadCloudCreatedTime() const;
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const;
  Spine::TimeSeries::TimeSeriesVectorPtr roadCloudValuesFromSpatiaLite(Settings &settings) const;

  // NetAtmo
  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const;
  boost::posix_time::ptime getLatestNetAtmoCreatedTime() const;
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const;
  Spine::TimeSeries::TimeSeriesVectorPtr netAtmoValuesFromSpatiaLite(Settings &settings) const;

  // FmiIoT
  bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                              const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestFmiIoTDataTime() const;
  boost::posix_time::ptime getLatestFmiIoTCreatedTime() const;
  std::size_t fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const;
  Spine::TimeSeries::TimeSeriesVectorPtr fmiIoTValuesFromSpatiaLite(Settings &settings) const;

  void shutdown();

 private:
  Spine::Stations getStationsFromSpatiaLite(Settings &settings,
                                            boost::shared_ptr<SpatiaLite> spatialitedb);
  bool timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime) const;
  bool timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const;
  Spine::TimeSeries::TimeSeriesVectorPtr flashValuesFromSpatiaLite(Settings &settings) const;
  void readConfig(Spine::ConfigBase &cfg);

  SpatiaLiteConnectionPool *itsConnectionPool = nullptr;
  Fmi::TimeZones itsTimeZones;

  SpatiaLiteCacheParameters itsParameters;

  // Cache available time interval to avoid unnecessary sqlite requests. The interval
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

  // Caches for last inserted rows to avoid duplicate inserts
  mutable InsertStatus itsDataInsertCache;
  mutable InsertStatus itsWeatherQCInsertCache;
  mutable InsertStatus itsFlashInsertCache;
  mutable InsertStatus itsRoadCloudInsertCache;
  mutable InsertStatus itsNetAtmoInsertCache;
  mutable InsertStatus itsFmiIoTInsertCache;

  // Memory caches smaller than the spatialite cache itself
  std::unique_ptr<FlashMemoryCache> itsFlashMemoryCache;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
