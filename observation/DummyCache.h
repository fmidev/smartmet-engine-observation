#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "Settings.h"

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DummyCache : public ObservationCache
{
 public:
  DummyCache(const std::string &name, const EngineParametersPtr &p);

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

  bool roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                 const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestRoadCloudDataTime() const override;
  boost::posix_time::ptime getLatestRoadCloudCreatedTime() const override;
  std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const override;

  bool netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                               const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestNetAtmoDataTime() const override;
  boost::posix_time::ptime getLatestNetAtmoCreatedTime() const override;
  std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const override;

  bool bkHydrometaIntervalIsCached(const boost::posix_time::ptime &starttime,
								   const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestBKHydrometaDataTime() const override;
  boost::posix_time::ptime getLatestBKHydrometaCreatedTime() const override;
  std::size_t fillBKHydrometaCache(
								   const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanBKHydrometaCache(const boost::posix_time::time_duration &timetokeep) const override;

  bool fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                              const boost::posix_time::ptime &endtime) const override;
  boost::posix_time::ptime getLatestFmiIoTDataTime() const override;
  boost::posix_time::ptime getLatestFmiIoTCreatedTime() const override;
  std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const override;

  void shutdown() override;

 private:
  EngineParametersPtr itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
