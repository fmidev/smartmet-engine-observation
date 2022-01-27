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

Spine::TimeSeries::TimeSeriesVectorPtr DummyCache::valuesFromCache(Settings & /* settings */)
{
  return Spine::TimeSeries::TimeSeriesVectorPtr();
}

Spine::TimeSeries::TimeSeriesVectorPtr DummyCache::valuesFromCache(
    Settings & /* settings */, const Spine::TimeSeriesGeneratorOptions & /* timeSeriesOptions */)
{
  return Spine::TimeSeries::TimeSeriesVectorPtr();
}

bool DummyCache::dataAvailableInCache(const Settings &settings) const
{
  return false;
}

bool DummyCache::flashIntervalIsCached(const boost::posix_time::ptime & /* starttime */,
                                       const boost::posix_time::ptime & /* endtime */) const
{
  return false;
}

FlashCounts DummyCache::getFlashCount(const boost::posix_time::ptime & /* starttime */,
                                      const boost::posix_time::ptime & /* endtime */,
                                      const Spine::TaggedLocationList & /* locations */) const
{
  return FlashCounts();
}

boost::posix_time::ptime DummyCache::getLatestFlashModifiedTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestFlashTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillFlashDataCache(const FlashDataItems & /* flashCacheData */) const
{
  return 0;
}

void DummyCache::cleanFlashDataCache(
    const boost::posix_time::time_duration & /* timetokeep */,
    const boost::posix_time::time_duration & /*timetokeep_memory */) const
{
}

boost::posix_time::ptime DummyCache::getLatestObservationModifiedTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestObservationTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillDataCache(const DataItems &cacheData) const
{
  return 0;
}

void DummyCache::cleanDataCache(
    const boost::posix_time::time_duration & /* timetokeep */,
    const boost::posix_time::time_duration & /* timetokeep_memory */) const
{
}

boost::posix_time::ptime DummyCache::getLatestWeatherDataQCTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestWeatherDataQCModifiedTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillWeatherDataQCCache(const WeatherDataQCItems & /* cacheData */) const
{
  return 0;
}

void DummyCache::cleanWeatherDataQCCache(
    const boost::posix_time::time_duration & /* timetokeep */) const
{
}

bool DummyCache::roadCloudIntervalIsCached(const boost::posix_time::ptime & /* starttime */,
                                           const boost::posix_time::ptime & /* endtime */) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestRoadCloudDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestRoadCloudCreatedTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillRoadCloudCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanRoadCloudCache(
    const boost::posix_time::time_duration & /* timetokeep */) const
{
}

bool DummyCache::netAtmoIntervalIsCached(const boost::posix_time::ptime & /*starttime */,
                                         const boost::posix_time::ptime & /*endtime */) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestNetAtmoDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestNetAtmoCreatedTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillNetAtmoCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanNetAtmoCache(const boost::posix_time::time_duration & /* timetokeep */) const
{
}

bool DummyCache::bkHydrometaIntervalIsCached(const boost::posix_time::ptime & /*starttime */,
                                             const boost::posix_time::ptime & /*endtime */) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestBKHydrometaDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestBKHydrometaCreatedTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillBKHydrometaCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanBKHydrometaCache(
    const boost::posix_time::time_duration & /* timetokeep */) const
{
}

bool DummyCache::fmiIoTIntervalIsCached(const boost::posix_time::ptime & /* starttime */,
                                        const boost::posix_time::ptime & /* endtime */) const
{
  return false;
}

boost::posix_time::ptime DummyCache::getLatestFmiIoTDataTime() const
{
  return boost::posix_time::not_a_date_time;
}

boost::posix_time::ptime DummyCache::getLatestFmiIoTCreatedTime() const
{
  return boost::posix_time::not_a_date_time;
}

std::size_t DummyCache::fillFmiIoTCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */) const
{
  return 0;
}

void DummyCache::cleanFmiIoTCache(const boost::posix_time::time_duration & /* timetokeep */) const
{
}

void DummyCache::shutdown() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
