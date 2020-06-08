#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "Settings.h"

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservableProperty;

class DummyCache : public ObservationCache
{
 public:
  DummyCache(const std::string &name, const EngineParametersPtr &p);

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
  std::size_t fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const;
  void cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep) const;

  bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestRoadCloudDataTime() const;
  boost::posix_time::ptime getLatestRoadCloudCreatedTime() const;
  std::size_t fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const;

  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const;
  boost::posix_time::ptime getLatestNetAtmoCreatedTime() const;
  std::size_t fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData) const;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const;

  boost::shared_ptr<std::vector<ObservableProperty> > observablePropertyQuery(
      std::vector<std::string> &parameters, const std::string language) const;
  void shutdown();

 private:
  EngineParametersPtr itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
