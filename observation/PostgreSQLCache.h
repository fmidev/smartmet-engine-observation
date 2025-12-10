#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "PostgreSQLCacheDB.h"
#include "PostgreSQLCacheParameters.h"
#include "Settings.h"
#include "StationtypeConfig.h"

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationMemoryCache;

class PostgreSQLCache : public ObservationCache
{
 public:
  PostgreSQLCache(const std::string &name,
                  const EngineParametersPtr &p,
                  const Spine::ConfigBase &cfg);
  ~PostgreSQLCache() override;

  PostgreSQLCache() = delete;
  PostgreSQLCache(const PostgreSQLCache &other) = delete;
  PostgreSQLCache(PostgreSQLCache &&other) = delete;
  PostgreSQLCache &operator=(const PostgreSQLCache &other) = delete;
  PostgreSQLCache &operator=(PostgreSQLCache &&other) = delete;

  void initializeConnectionPool() override;
  void initializeCaches(int finCacheDuration,
                        int finMemoryCacheDuration,
                        int extCacheDuration,
                        int extMemoryCacheDuration,
                        int flashCacheDuration,
                        int flashMemoryCacheDuration) override;

  TS::TimeSeriesVectorPtr valuesFromCache(Settings &settings) override;
  TS::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) override;

  bool dataAvailableInCache(const Settings &settings) const override;
  bool flashIntervalIsCached(const Fmi::DateTime &starttime,
                             const Fmi::DateTime &endtime) const override;
  FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                            const Fmi::DateTime &endtime,
                            const Spine::TaggedLocationList &locations) const override;
  Fmi::DateTime getLatestFlashModifiedTime() const override;
  Fmi::DateTime getLatestFlashTime() const override;
  std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const override;
  void cleanFlashDataCache(const Fmi::TimeDuration &timetokeep,
                           const Fmi::TimeDuration &timetokeep_memory) const override;

  Fmi::DateTime getLatestObservationModifiedTime() const override;
  Fmi::DateTime getLatestObservationTime() const override;
  std::size_t fillDataCache(const DataItems &cacheData) const override;
  std::size_t fillMovingLocationsCache(const MovingLocationItems &cacheData) const override;
  void cleanDataCache(const Fmi::TimeDuration &timetokeep,
                      const Fmi::TimeDuration &timetokeep_memory) const override;
  Fmi::DateTime getLatestWeatherDataQCTime() const override;
  Fmi::DateTime getLatestWeatherDataQCModifiedTime() const override;
  std::size_t fillWeatherDataQCCache(const DataItems &cacheData) const override;
  void cleanWeatherDataQCCache(const Fmi::TimeDuration &timetokeep,
                               const Fmi::TimeDuration &timetokeep_memory) const override;

  // RoadCloud
  bool roadCloudIntervalIsCached(const Fmi::DateTime &starttime,
                                 const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestRoadCloudDataTime() const override;
  Fmi::DateTime getLatestRoadCloudCreatedTime() const override;
  std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanRoadCloudCache(const Fmi::TimeDuration &timetokeep) const override;

  // NetAtmo
  bool netAtmoIntervalIsCached(const Fmi::DateTime &starttime,
                               const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestNetAtmoDataTime() const override;
  Fmi::DateTime getLatestNetAtmoCreatedTime() const override;
  std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanNetAtmoCache(const Fmi::TimeDuration &timetokeep) const override;

  // FmiIoT
  bool fmiIoTIntervalIsCached(const Fmi::DateTime &starttime,
                              const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestFmiIoTDataTime() const override;
  Fmi::DateTime getLatestFmiIoTCreatedTime() const override;
  std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanFmiIoTCache(const Fmi::TimeDuration &timetokeep) const override;

  // TapsiQc
  bool tapsiQcIntervalIsCached(const Fmi::DateTime &starttime,
                               const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestTapsiQcDataTime() const override;
  Fmi::DateTime getLatestTapsiQcCreatedTime() const override;
  std::size_t fillTapsiQcCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanTapsiQcCache(const Fmi::TimeDuration &timetokeep) const override;

  // Magnetometer
  bool magnetometerIntervalIsCached(const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestMagnetometerDataTime() const override;
  Fmi::DateTime getLatestMagnetometerModifiedTime() const override;
  std::size_t fillMagnetometerCache(
      const MagnetometerDataItems &magnetometerCacheData) const override;
  void cleanMagnetometerCache(const Fmi::TimeDuration &timetokeep) const override;

  void shutdown() final;

  void getMovingStations(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override;

 private:
  using PoolType =
      Fmi::Pool<Fmi::PoolInitType::Parallel, PostgreSQLCacheDB, PostgreSQLCacheParameters>;

  std::unique_ptr<PoolType> itsConnectionPool;

  Fmi::TimeZones itsTimeZones;

  void readConfig(const Spine::ConfigBase &cfg);
  bool timeIntervalIsCached(const Fmi::DateTime &starttime, const Fmi::DateTime &endtime) const;
  bool timeIntervalWeatherDataQCIsCached(const Fmi::DateTime &starttime,
                                         const Fmi::DateTime &endtime) const;
  TS::TimeSeriesVectorPtr flashValuesFromPostgreSQL(const Settings &settings) const;
  TS::TimeSeriesVectorPtr roadCloudValuesFromPostgreSQL(const Settings &settings) const;
  TS::TimeSeriesVectorPtr netAtmoValuesFromPostgreSQL(const Settings &settings) const;
  TS::TimeSeriesVectorPtr fmiIoTValuesFromPostgreSQL(const Settings &settings) const;
  TS::TimeSeriesVectorPtr tapsiQcValuesFromPostgreSQL(const Settings &settings) const;

  PostgreSQLCacheParameters itsParameters;

  // Cache available time interval to avoid unnecessary database requests. The interval
  // needs to be updated once at initialization, after a write, and before cleaning
  mutable Spine::MutexType itsTimeIntervalMutex;
  mutable Fmi::DateTime itsTimeIntervalStart;
  mutable Fmi::DateTime itsTimeIntervalEnd;

  mutable Spine::MutexType itsWeatherDataQCTimeIntervalMutex;
  mutable Fmi::DateTime itsWeatherDataQCTimeIntervalStart;
  mutable Fmi::DateTime itsWeatherDataQCTimeIntervalEnd;

  mutable Spine::MutexType itsFlashTimeIntervalMutex;
  mutable Fmi::DateTime itsFlashTimeIntervalStart;
  mutable Fmi::DateTime itsFlashTimeIntervalEnd;

  mutable Spine::MutexType itsRoadCloudTimeIntervalMutex;
  mutable Fmi::DateTime itsRoadCloudTimeIntervalStart;
  mutable Fmi::DateTime itsRoadCloudTimeIntervalEnd;

  mutable Spine::MutexType itsNetAtmoTimeIntervalMutex;
  mutable Fmi::DateTime itsNetAtmoTimeIntervalStart;
  mutable Fmi::DateTime itsNetAtmoTimeIntervalEnd;

  mutable Spine::MutexType itsFmiIoTTimeIntervalMutex;
  mutable Fmi::DateTime itsFmiIoTTimeIntervalStart;
  mutable Fmi::DateTime itsFmiIoTTimeIntervalEnd;

  mutable Spine::MutexType itsTapsiQcTimeIntervalMutex;
  mutable Fmi::DateTime itsTapsiQcTimeIntervalStart;
  mutable Fmi::DateTime itsTapsiQcTimeIntervalEnd;

  std::unique_ptr<ObservationMemoryCache> itsObservationMemoryCache;
  std::unique_ptr<ObservationMemoryCache> itsExtMemoryCache;

  // Cache statistics
  void hit(const std::string &name) const;
  void miss(const std::string &name) const;
  mutable Spine::MutexType itsCacheStatisticsMutex;
  mutable Fmi::Cache::CacheStatistics itsCacheStatistics;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
