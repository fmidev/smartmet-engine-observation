#pragma once

#include "EngineParameters.h"
#include "InsertStatus.h"
#include "ObservationCache.h"
#include "Settings.h"
#include "SpatiaLiteCacheParameters.h"
#include "SpatiaLiteConnectionPool.h"
#include "StationtypeConfig.h"
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationMemoryCache;
class FlashMemoryCache;

class SpatiaLiteCache : public ObservationCache
{
 public:
  SpatiaLiteCache(const std::string &name,
                  const EngineParametersPtr &p,
                  const Spine::ConfigBase &cfg);
  ~SpatiaLiteCache() override;

  SpatiaLiteCache() = delete;
  SpatiaLiteCache(const SpatiaLiteCache &other) = delete;
  SpatiaLiteCache(SpatiaLiteCache &&other) = delete;
  SpatiaLiteCache &operator=(const SpatiaLiteCache &other) = delete;
  SpatiaLiteCache &operator=(SpatiaLiteCache &&other) = delete;

  void initializeConnectionPool() override;
  void initializeCaches(int finCacheDuration,
                        int finMemoryCacheDuration,
                        int extCacheDuration,
                        int flashCacheDuration,
                        int flashMemoryCacheDuration) override;

  TS::TimeSeriesVectorPtr valuesFromCache(Settings &settings) override;
  TS::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

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
  std::size_t fillMovingLocationsCache(const MovingLocationItems &cacheData) const override;
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
  TS::TimeSeriesVectorPtr roadCloudValuesFromSpatiaLite(const Settings &settings) const;

  // NetAtmo
  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const override;
  boost::posix_time::ptime getLatestNetAtmoCreatedTime() const override;
  std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const override;
  TS::TimeSeriesVectorPtr netAtmoValuesFromSpatiaLite(const Settings &settings) const;

  // BKHydrometa
  bool bkHydrometaIntervalIsCached(const boost::posix_time::ptime &starttime,
                                   const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestBKHydrometaDataTime() const override;
  boost::posix_time::ptime getLatestBKHydrometaCreatedTime() const override;
  std::size_t fillBKHydrometaCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanBKHydrometaCache(const boost::posix_time::time_duration &timetokeep) const override;
  TS::TimeSeriesVectorPtr bkHydrometaValuesFromSpatiaLite(const Settings &settings) const;

  // FmiIoT
  bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                              const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestFmiIoTDataTime() const override;
  boost::posix_time::ptime getLatestFmiIoTCreatedTime() const override;
  std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const override;
  TS::TimeSeriesVectorPtr fmiIoTValuesFromSpatiaLite(const Settings &settings) const;

  // Magnetometer
  bool magnetometerIntervalIsCached(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestMagnetometerDataTime() const override;
  boost::posix_time::ptime getLatestMagnetometerModifiedTime() const override;
  std::size_t fillMagnetometerCache(
      const MagnetometerDataItems &magnetometerCacheData) const override;
  void cleanMagnetometerCache(const boost::posix_time::time_duration &timetokeep) const override;
  TS::TimeSeriesVectorPtr magnetometerValuesFromSpatiaLite(Settings &settings) const;

  void shutdown() final;

  // This has been added for flash emulator
  int getMaxFlashId() const override;

  /**
   * \brief Init the internal memory cache from SpatiaLite
   * \param starttime Start time for the update
   */

  void cleanMemoryDataCache(const boost::posix_time::ptime &newstarttime) const;

  Fmi::Cache::CacheStatistics getCacheStats() const override;

 private:
  Spine::Stations getStationsFromSpatiaLite(Settings &settings,
                                            std::shared_ptr<SpatiaLite> spatialitedb);
  bool timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                            const boost::posix_time::ptime &endtime) const;
  bool timeIntervalWeatherDataQCIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const;
  TS::TimeSeriesVectorPtr flashValuesFromSpatiaLite(const Settings &settings) const;
  void readConfig(const Spine::ConfigBase &cfg);

  void getMovingStations(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override;

  std::unique_ptr<SpatiaLiteConnectionPool> itsConnectionPool;
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

  mutable Spine::MutexType itsBKHydrometaTimeIntervalMutex;
  mutable boost::posix_time::ptime itsBKHydrometaTimeIntervalStart;
  mutable boost::posix_time::ptime itsBKHydrometaTimeIntervalEnd;

  mutable Spine::MutexType itsFmiIoTTimeIntervalMutex;
  mutable boost::posix_time::ptime itsFmiIoTTimeIntervalStart;
  mutable boost::posix_time::ptime itsFmiIoTTimeIntervalEnd;

  mutable Spine::MutexType itsMagnetometerTimeIntervalMutex;
  mutable boost::posix_time::ptime itsMagnetometerTimeIntervalStart;
  mutable boost::posix_time::ptime itsMagnetometerTimeIntervalEnd;

  // Caches for last inserted rows to avoid duplicate inserts
  mutable InsertStatus itsDataInsertCache;
  mutable InsertStatus itsMovingLocationsInsertCache;
  mutable InsertStatus itsWeatherQCInsertCache;
  mutable InsertStatus itsFlashInsertCache;
  mutable InsertStatus itsRoadCloudInsertCache;
  mutable InsertStatus itsNetAtmoInsertCache;
  mutable InsertStatus itsBKHydrometaInsertCache;
  mutable InsertStatus itsFmiIoTInsertCache;
  mutable InsertStatus itsMagnetometerInsertCache;

  // Memory caches smaller than the spatialite cache itself
  std::unique_ptr<ObservationMemoryCache> itsObservationMemoryCache;
  std::unique_ptr<FlashMemoryCache> itsFlashMemoryCache;

  // Cache statistics
  void hit(const std::string &name) const;
  void miss(const std::string &name) const;
  mutable Spine::MutexType itsCacheStatisticsMutex;
  mutable Fmi::Cache::CacheStatistics itsCacheStatistics;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
