#pragma once

#include "CacheInfoItem.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "MobileExternalDataItem.h"
#include "Settings.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <macgyver/CacheStats.h>
#include <spine/Station.h>
#include <timeseries/TimeSeriesInclude.h>
#include <macgyver/CacheStats.h>
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

  virtual bool flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                     const boost::posix_time::ptime &endtime) const = 0;

  virtual FlashCounts getFlashCount(const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime,
                                    const Spine::TaggedLocationList &locations) const = 0;

  virtual boost::posix_time::ptime getLatestFlashModifiedTime() const = 0;
  virtual boost::posix_time::ptime getLatestFlashTime() const = 0;
  virtual std::size_t fillFlashDataCache(const FlashDataItems &flashCacheData) const = 0;
  virtual void cleanFlashDataCache(
      const boost::posix_time::time_duration &timetokeep,
      const boost::posix_time::time_duration &timetokeep_memory) const = 0;

  virtual boost::posix_time::ptime getLatestObservationModifiedTime() const = 0;
  virtual boost::posix_time::ptime getLatestObservationTime() const = 0;
  virtual std::size_t fillDataCache(const DataItems &cacheData) const = 0;
  virtual void cleanDataCache(const boost::posix_time::time_duration &timetokeep,
                              const boost::posix_time::time_duration &timetokeep_memory) const = 0;

  virtual boost::posix_time::ptime getLatestWeatherDataQCTime() const = 0;
  virtual boost::posix_time::ptime getLatestWeatherDataQCModifiedTime() const = 0;
  virtual std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const = 0;
  virtual void cleanWeatherDataQCCache(
      const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                         const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestRoadCloudDataTime() const = 0;
  virtual boost::posix_time::ptime getLatestRoadCloudCreatedTime() const = 0;
  virtual std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                       const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestNetAtmoDataTime() const = 0;
  virtual boost::posix_time::ptime getLatestNetAtmoCreatedTime() const = 0;
  virtual std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool bkHydrometaIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestBKHydrometaDataTime() const = 0;
  virtual boost::posix_time::ptime getLatestBKHydrometaCreatedTime() const = 0;
  virtual std::size_t fillBKHydrometaCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanBKHydrometaCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestFmiIoTDataTime() const = 0;
  virtual boost::posix_time::ptime getLatestFmiIoTCreatedTime() const = 0;
  virtual std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const = 0;
  virtual Fmi::Cache::CacheStatistics getCacheStats() const
  {
    return Fmi::Cache::CacheStatistics();
  }

  virtual void shutdown() = 0;

  // This has been added for flash emulator
  virtual int getMaxFlashId() const { return 0; }

  const std::string &name() const;

  bool isFakeCache(const std::string &tablename) const;
  std::vector<std::map<std::string, std::string>> getFakeCacheSettings(
      const std::string &tablename) const;

 protected:
  ObservationCache(const CacheInfoItem &ci);

  const CacheInfoItem &itsCacheInfo;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
