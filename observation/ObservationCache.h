#pragma once

#include "CacheInfoItem.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "Keywords.h"
#include "MagnetometerDataItem.h"
#include "MobileExternalDataItem.h"
#include "MovingLocationItem.h"
#include "Settings.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <macgyver/CacheStats.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCache
{
 public:
  virtual ~ObservationCache();

  ObservationCache() = delete;  // because there is a const reference data member
  ObservationCache(const ObservationCache &other) = delete;
  ObservationCache(ObservationCache &&other) = delete;
  ObservationCache &operator=(const ObservationCache &other) = delete;
  ObservationCache &operator=(ObservationCache &&other) = delete;

  virtual void initializeConnectionPool() = 0;
  virtual void initializeCaches(int finCacheDuration,
                                int finMemoryCacheDuration,
                                int extCacheDuration,
                                int flashCacheDuration,
                                int flashMemoryCacheDuration) = 0;

  virtual TS::TimeSeriesVectorPtr valuesFromCache(Settings &settings) = 0;

  virtual TS::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;

  virtual bool dataAvailableInCache(const Settings &settings) const = 0;

  virtual bool flashIntervalIsCached(const Fmi::DateTime &starttime,
                                     const Fmi::DateTime &endtime) const = 0;

  virtual FlashCounts getFlashCount(const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime,
                                    const Spine::TaggedLocationList &locations) const = 0;

  virtual Fmi::DateTime getLatestFlashModifiedTime() const = 0;
  virtual Fmi::DateTime getLatestFlashTime() const = 0;
  virtual std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const = 0;
  virtual void cleanFlashDataCache(const Fmi::TimeDuration &timetokeep,
                                   const Fmi::TimeDuration &timetokeep_memory) const = 0;

  virtual Fmi::DateTime getLatestObservationModifiedTime() const = 0;
  virtual Fmi::DateTime getLatestObservationTime() const = 0;
  virtual std::size_t fillDataCache(const DataItems &cacheData) const = 0;
  virtual std::size_t fillMovingLocationsCache(
      const MovingLocationItems &cacheDataMovingLocations) const = 0;

  virtual void cleanDataCache(const Fmi::TimeDuration &timetokeep,
                              const Fmi::TimeDuration &timetokeep_memory) const = 0;

  virtual Fmi::DateTime getLatestWeatherDataQCTime() const = 0;
  virtual Fmi::DateTime getLatestWeatherDataQCModifiedTime() const = 0;
  virtual std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const = 0;
  virtual void cleanWeatherDataQCCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual bool roadCloudIntervalIsCached(const Fmi::DateTime &starttime,
                                         const Fmi::DateTime &endtime) const = 0;
  virtual Fmi::DateTime getLatestRoadCloudDataTime() const = 0;
  virtual Fmi::DateTime getLatestRoadCloudCreatedTime() const = 0;
  virtual std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanRoadCloudCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual bool netAtmoIntervalIsCached(const Fmi::DateTime &starttime,
                                       const Fmi::DateTime &endtime) const = 0;
  virtual Fmi::DateTime getLatestNetAtmoDataTime() const = 0;
  virtual Fmi::DateTime getLatestNetAtmoCreatedTime() const = 0;
  virtual std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanNetAtmoCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual bool fmiIoTIntervalIsCached(const Fmi::DateTime &starttime,
                                      const Fmi::DateTime &endtime) const = 0;
  virtual Fmi::DateTime getLatestFmiIoTDataTime() const = 0;
  virtual Fmi::DateTime getLatestFmiIoTCreatedTime() const = 0;
  virtual std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanFmiIoTCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual bool tapsiQcIntervalIsCached(const Fmi::DateTime &starttime,
                                       const Fmi::DateTime &endtime) const = 0;
  virtual Fmi::DateTime getLatestTapsiQcDataTime() const = 0;
  virtual Fmi::DateTime getLatestTapsiQcCreatedTime() const = 0;
  virtual std::size_t fillTapsiQcCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanTapsiQcCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual bool magnetometerIntervalIsCached(const Fmi::DateTime &starttime,
                                            const Fmi::DateTime &endtime) const = 0;
  virtual Fmi::DateTime getLatestMagnetometerDataTime() const = 0;
  virtual Fmi::DateTime getLatestMagnetometerModifiedTime() const = 0;
  virtual std::size_t fillMagnetometerCache(
      const MagnetometerDataItems &magnetometerCacheData) const = 0;
  virtual void cleanMagnetometerCache(const Fmi::TimeDuration &timetokeep) const = 0;

  virtual Fmi::Cache::CacheStatistics getCacheStats() const { return {}; }

  virtual void shutdown() = 0;

  // This has been added for flash emulator
  virtual int getMaxFlashId() const { return 0; }

  const std::string &name() const;

  bool isFakeCache(const std::string &tablename) const;
  std::vector<std::map<std::string, std::string>> getFakeCacheSettings(
      const std::string &tablename) const;

  virtual void getMovingStations(Spine::Stations &stations,
                                 const Settings &settings,
                                 const std::string &wkt) const = 0;
  virtual Fmi::DateTime getLatestDataUpdateTime(const std::string &tablename,
                                                const Fmi::DateTime &starttime,
                                                const std::string &producer_ids,
                                                const std::string &measurand_ids) const;

 protected:
  ObservationCache(const CacheInfoItem &ci);

  const CacheInfoItem &itsCacheInfo;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
