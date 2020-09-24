#pragma once

#include "CacheInfoItem.h"
#include "DataItem.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "MobileExternalDataItem.h"
#include "Settings.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"

#include <macgyver/Cache.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

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

  virtual Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(Settings &settings) = 0;

  virtual Spine::TimeSeries::TimeSeriesVectorPtr valuesFromCache(
      Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions) = 0;

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

  virtual bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime) const = 0;
  virtual boost::posix_time::ptime getLatestFmiIoTDataTime() const = 0;
  virtual boost::posix_time::ptime getLatestFmiIoTCreatedTime() const = 0;
  virtual std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const = 0;
  virtual void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const = 0;

  virtual void shutdown() = 0;

  const std::string &name() const { return itsCacheName; }

 protected:
  ObservationCache(const std::string &name);

  std::string itsCacheName;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
