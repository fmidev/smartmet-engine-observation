#include "DummyCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
DummyCache::DummyCache(const std::string &name, const EngineParametersPtr &p)
    : ObservationCache(p->databaseDriverInfo.getAggregateCacheInfo(name)), itsParameters(p)
{
}

void DummyCache::initializeConnectionPool()
{
  Utils::logMessage("[Observation Engine] Dummy cache initialized!", itsParameters->quiet);
}

void DummyCache::initializeCaches(int /* finCacheDuration */,
                                  int /* finMemoryCacheDuration */,
                                  int /* extCacheDuration */,
                                  int /* flashCacheDuration */,
                                  int /* flashMemoryCacheDuration */)
{
}

TS::TimeSeriesVectorPtr DummyCache::valuesFromCache(Settings & /* settings */)
{
  return {};
}

TS::TimeSeriesVectorPtr DummyCache::valuesFromCache(
    Settings & /* settings */, const TS::TimeSeriesGeneratorOptions & /* timeSeriesOptions */)
{
  return {};
}

bool DummyCache::dataAvailableInCache(const Settings & /* settings */) const
{
  return false;
}

bool DummyCache::flashIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                       const Fmi::DateTime & /* endtime */) const
{
  return false;
}

FlashCounts DummyCache::getFlashCount(const Fmi::DateTime & /* starttime */,
                                      const Fmi::DateTime & /* endtime */,
                                      const Spine::TaggedLocationList & /* locations */) const
{
  return {};
}

Fmi::DateTime DummyCache::getLatestFlashModifiedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestFlashTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillFlashDataCache(const FlashDataItems & /* flashCacheData */) const
{
  return 0;
}

void DummyCache::cleanFlashDataCache(const Fmi::TimeDuration & /* timetokeep */,
                                     const Fmi::TimeDuration & /*timetokeep_memory */) const
{
}

Fmi::DateTime DummyCache::getLatestObservationModifiedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestObservationTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillDataCache(const DataItems & /* cacheData */) const
{
  return 0;
}

std::size_t DummyCache::fillMovingLocationsCache(const MovingLocationItems & /* cacheData */) const
{
  return 0;
}

void DummyCache::cleanDataCache(const Fmi::TimeDuration & /* timetokeep */,
                                const Fmi::TimeDuration & /* timetokeep_memory */) const
{
}

Fmi::DateTime DummyCache::getLatestWeatherDataQCTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestWeatherDataQCModifiedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillWeatherDataQCCache(const WeatherDataQCItems & /* cacheData */) const
{
  return 0;
}

void DummyCache::cleanWeatherDataQCCache(const Fmi::TimeDuration & /* timetokeep */) const {}

bool DummyCache::roadCloudIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                           const Fmi::DateTime & /* endtime */) const
{
  return false;
}

Fmi::DateTime DummyCache::getLatestRoadCloudDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestRoadCloudCreatedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillRoadCloudCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanRoadCloudCache(const Fmi::TimeDuration & /* timetokeep */) const {}

bool DummyCache::netAtmoIntervalIsCached(const Fmi::DateTime & /*starttime */,
                                         const Fmi::DateTime & /*endtime */) const
{
  return false;
}

Fmi::DateTime DummyCache::getLatestNetAtmoDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestNetAtmoCreatedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillNetAtmoCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanNetAtmoCache(const Fmi::TimeDuration & /* timetokeep */) const {}

bool DummyCache::fmiIoTIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                        const Fmi::DateTime & /* endtime */) const
{
  return false;
}

Fmi::DateTime DummyCache::getLatestFmiIoTDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestFmiIoTCreatedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillFmiIoTCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanFmiIoTCache(const Fmi::TimeDuration & /* timetokeep */) const {}

bool DummyCache::tapsiQcIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                         const Fmi::DateTime & /* endtime */) const
{
  return false;
}

Fmi::DateTime DummyCache::getLatestTapsiQcDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestTapsiQcCreatedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillTapsiQcCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanTapsiQcCache(const Fmi::TimeDuration & /* timetokeep */) const {}

bool DummyCache::magnetometerIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                              const Fmi::DateTime & /* endtime */) const
{
  return false;
}

Fmi::DateTime DummyCache::getLatestMagnetometerDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime DummyCache::getLatestMagnetometerModifiedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t DummyCache::fillMagnetometerCache(
    const MagnetometerDataItems & /* magnetometerCacheData */) const
{
  return 0;
}

void DummyCache::cleanMagnetometerCache(const Fmi::TimeDuration & /* timetokeep */) const {}

void DummyCache::getMovingStations(Spine::Stations &stations,
                                   const Settings &settings,
                                   const std::string &wkt) const
{
}

void DummyCache::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
