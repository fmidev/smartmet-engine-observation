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

  bool roadCloudIntervalIsCached(const Fmi::DateTime &starttime,
                                 const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestRoadCloudDataTime() const override;
  Fmi::DateTime getLatestRoadCloudCreatedTime() const override;
  std::size_t fillRoadCloudCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanRoadCloudCache(const Fmi::TimeDuration &timetokeep) const override;

  bool netAtmoIntervalIsCached(const Fmi::DateTime &starttime,
                               const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestNetAtmoDataTime() const override;
  Fmi::DateTime getLatestNetAtmoCreatedTime() const override;
  std::size_t fillNetAtmoCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanNetAtmoCache(const Fmi::TimeDuration &timetokeep) const override;

  bool fmiIoTIntervalIsCached(const Fmi::DateTime &starttime,
                              const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestFmiIoTDataTime() const override;
  Fmi::DateTime getLatestFmiIoTCreatedTime() const override;
  std::size_t fillFmiIoTCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanFmiIoTCache(const Fmi::TimeDuration &timetokeep) const override;

  bool tapsiQcIntervalIsCached(const Fmi::DateTime &starttime,
                               const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestTapsiQcDataTime() const override;
  Fmi::DateTime getLatestTapsiQcCreatedTime() const override;
  std::size_t fillTapsiQcCache(
      const MobileExternalDataItems &mobileExternalCacheData) const override;
  void cleanTapsiQcCache(const Fmi::TimeDuration &timetokeep) const override;

  bool magnetometerIntervalIsCached(const Fmi::DateTime &starttime,
                                    const Fmi::DateTime &endtime) const override;
  Fmi::DateTime getLatestMagnetometerDataTime() const override;
  Fmi::DateTime getLatestMagnetometerModifiedTime() const override;
  std::size_t fillMagnetometerCache(
      const MagnetometerDataItems &magnetometerCacheData) const override;
  void cleanMagnetometerCache(const Fmi::TimeDuration &timetokeep) const override;
  void getMovingStations(Spine::Stations &stations,
                         const Settings &settings,
                         const std::string &wkt) const override;

  void shutdown() override;

 private:
  EngineParametersPtr itsParameters;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
