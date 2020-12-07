#include "SpatiaLiteCache.h"

#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/StringConversion.h>
#include <atomic>

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
// Round down to HH:MM:00. Deleting an entire hour at once takes too long, and causes a major
// increase in response times. This should perhaps be made configurable.

boost::posix_time::ptime round_down_to_cache_clean_interval(const boost::posix_time::ptime &t)
{
  auto secs = (t.time_of_day().total_seconds() / 60) * 60;
  return boost::posix_time::ptime(t.date(), boost::posix_time::seconds(secs));
}

}  // namespace

void SpatiaLiteCache::initializeConnectionPool()
{
  try
  {
    // Check if already initialized (one cache can be shared by multiple database drivers)
    if (itsConnectionPool)
    {
      // Cache already initialized
      return;
    }

    logMessage("[Observation Engine] Initializing SpatiaLite cache connection pool...",
               itsParameters.quiet);

    itsConnectionPool.reset(new SpatiaLiteConnectionPool(itsParameters));

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    const std::set<std::string> &cacheTables = itsCacheInfo.tables;

    db->createTables(cacheTables);

    // Observation data
    if (cacheTables.find(OBSERVATION_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestObservationTime();
      auto end = db->getLatestObservationTime();
      itsTimeIntervalStart = start;
      itsTimeIntervalEnd = end;
    }

    // WeatherDataQC
    if (cacheTables.find(WEATHER_DATA_QC_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestWeatherDataQCTime();
      auto end = db->getLatestWeatherDataQCTime();
      itsWeatherDataQCTimeIntervalStart = start;
      itsWeatherDataQCTimeIntervalEnd = end;
    }

    // Flash
    if (cacheTables.find(FLASH_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestFlashTime();
      auto end = db->getLatestFlashTime();
      itsFlashTimeIntervalStart = start;
      itsFlashTimeIntervalEnd = end;
    }

    // Road cloud
    if (cacheTables.find(ROADCLOUD_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestRoadCloudDataTime();
      auto end = db->getLatestRoadCloudDataTime();
      itsRoadCloudTimeIntervalStart = start;
      itsRoadCloudTimeIntervalEnd = end;
    }

    // NetAtmo
    if (cacheTables.find(NETATMO_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestNetAtmoDataTime();
      auto end = db->getLatestNetAtmoDataTime();
      itsNetAtmoTimeIntervalStart = start;
      itsNetAtmoTimeIntervalEnd = end;
    }

    // FmiIoT
    if (cacheTables.find(FMI_IOT_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestFmiIoTDataTime();
      auto end = db->getLatestFmiIoTDataTime();
      itsFmiIoTTimeIntervalStart = start;
      itsFmiIoTTimeIntervalEnd = end;
    }

    logMessage("[Observation Engine] SpatiaLite connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Initializing connection pool failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}  // namespace Observation

void SpatiaLiteCache::initializeCaches(int finCacheDuration,
                                       int finMemoryCacheDuration,
                                       int extCacheDuration,
                                       int flashCacheDuration,
                                       int flashMemoryCacheDuration)
{
  try
  {
    auto now = boost::posix_time::second_clock::universal_time();

    const std::set<std::string> &cacheTables = itsCacheInfo.tables;

    if (cacheTables.find(FLASH_DATA_TABLE) != cacheTables.end() && flashMemoryCacheDuration > 0)
    {
      logMessage("[Observation Engine] Initializing SpatiaLite flash memory cache",
                 itsParameters.quiet);
      itsFlashMemoryCache.reset(new FlashMemoryCache);
      auto timetokeep_memory = boost::posix_time::hours(flashMemoryCacheDuration);
      auto flashdata =
          itsConnectionPool->getConnection()->readFlashCacheData(now - timetokeep_memory);
      itsFlashMemoryCache->fill(flashdata);
    }
    if (cacheTables.find(OBSERVATION_DATA_TABLE) != cacheTables.end() && finMemoryCacheDuration > 0)
    {
      logMessage("[Observation Engine] Initializing SpatiaLite observation memory cache",
                 itsParameters.quiet);
      auto timetokeep_memory = boost::posix_time::hours(finMemoryCacheDuration);
      itsConnectionPool->getConnection()->initObservationMemoryCache(now - timetokeep_memory);
    }

    logMessage("[Observation Engine] SpatiaLite memory cache ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cache initialization failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromSpatiaLite(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      std::shared_ptr<CommonDatabaseFunctions> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, itsTimeZones);
      }
      else
      {
        ret = db->getObservationData(stations, settings, *sinfo, itsTimeZones);
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "Getting values from cache for stationtype '" + settings.stationtype + "' failed!");
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromSpatiaLite(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        ret = db->getObservationData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "Getting values from cache for stationtype '" + settings.stationtype + "' failed!");
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::flashValuesFromSpatiaLite(Settings &settings) const
{
  try
  {
    // Use memory cache if possible. t is not set if the cache is not ready yet
    if (itsFlashMemoryCache)
    {
      auto t = itsFlashMemoryCache->getStartTime();

      if (!t.is_not_a_date_time() && settings.starttime >= t)
        return itsFlashMemoryCache->getData(settings, itsParameters.parameterMap, itsTimeZones);
    }

    // Must use disk cache instead
    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    return db->getFlashData(settings, itsTimeZones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash values from cache failed!");
  }
}

bool SpatiaLiteCache::timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const
{
  try
  {
    Spine::ReadLock lock(itsTimeIntervalMutex);

    if (itsTimeIntervalStart.is_not_a_date_time() || itsTimeIntervalEnd.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsTimeIntervalStart && starttime < itsTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if time interval is cached failed!");
  }
}

bool SpatiaLiteCache::flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime) const
{
  try
  {
    // No need to check memory cache here, it is always supposed to be shorted than the disk cache

    Spine::ReadLock lock(itsFlashTimeIntervalMutex);

    if (itsFlashTimeIntervalStart.is_not_a_date_time() ||
        itsFlashTimeIntervalEnd.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsFlashTimeIntervalStart && starttime < itsFlashTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if flash interval is cached failed!");
  }
}

bool SpatiaLiteCache::timeIntervalWeatherDataQCIsCached(
    const boost::posix_time::ptime &starttime, const boost::posix_time::ptime &endtime) const
{
  try
  {
    Spine::ReadLock lock(itsWeatherDataQCTimeIntervalMutex);
    if (itsWeatherDataQCTimeIntervalStart.is_not_a_date_time() ||
        itsWeatherDataQCTimeIntervalEnd.is_not_a_date_time())
      return false;

    // We ignore end time intentionally
    return (starttime >= itsWeatherDataQCTimeIntervalStart &&
            starttime < itsWeatherDataQCTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if weather data QC is cached failed!");
  }
}

bool SpatiaLiteCache::dataAvailableInCache(const Settings &settings) const
{
  try
  {
    // If stationtype is cached and if we have requested time interval in
    // SpatiaLite, get all data from there
    if (settings.useCommonQueryMethod)
      return timeIntervalIsCached(settings.starttime, settings.endtime);

    const auto &s = settings.stationtype;

    if (s == "road" || s == "foreign" || s == "observations_fmi_extaws")
      return timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime);

    if (s == "flash")
      return flashIntervalIsCached(settings.starttime, settings.endtime);

    if (s == ROADCLOUD_PRODUCER)
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    if (s == NETATMO_PRODUCER)
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

    if (s == FMI_IOT_PRODUCER)
      return fmiIoTIntervalIsCached(settings.starttime, settings.endtime);

    // Either the stationtype is not cached or the requested time interval is
    // not cached
    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if data is available in cache failed!");
  }
}

FlashCounts SpatiaLiteCache::getFlashCount(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime,
                                           const Spine::TaggedLocationList &locations) const
{
  return itsConnectionPool->getConnection()->getFlashCount(starttime, endtime, locations);
}

boost::posix_time::ptime SpatiaLiteCache::getLatestFlashModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashModifiedTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestFlashTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashTime();
}

std::size_t SpatiaLiteCache::fillFlashDataCache(const FlashDataItems &flashCacheData) const
{
  try
  {
    // Memory cache first
    if (itsFlashMemoryCache)
      itsFlashMemoryCache->fill(flashCacheData);

    // Then disk cache
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillFlashDataCache(flashCacheData, itsFlashInsertCache);

    // Update info on what is in the database
    auto start = conn->getOldestFlashTime();
    auto end = conn->getLatestFlashTime();
    Spine::WriteLock lock(itsFlashTimeIntervalMutex);
    itsFlashTimeIntervalStart = start;
    itsFlashTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling flash data cache failed!");
  }
}

void SpatiaLiteCache::cleanFlashDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(FLASH_DATA_TABLE))
      return;

    auto now = boost::posix_time::second_clock::universal_time();

    // Clean memory cache first:

    if (itsFlashMemoryCache)
      itsFlashMemoryCache->clean(now - timetokeep_memory);

    // How old observations to keep in the disk cache:
    auto t = round_down_to_cache_clean_interval(now - timetokeep);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsFlashTimeIntervalMutex);
      itsFlashTimeIntervalStart = t;
    }
    conn->cleanFlashDataCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestFlashTime();
    auto end = conn->getLatestFlashTime();
    Spine::WriteLock lock(itsFlashTimeIntervalMutex);
    itsFlashTimeIntervalStart = start;
    itsFlashTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning flash data cache failed!");
  }
}

bool SpatiaLiteCache::roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &) const
{
  try
  {
    Spine::ReadLock lock(itsRoadCloudTimeIntervalMutex);
    if (itsRoadCloudTimeIntervalStart.is_not_a_date_time() &&
        itsRoadCloudTimeIntervalEnd.is_not_a_date_time())
      return false;

    // We ignore end time intentionally
    return (starttime >= itsRoadCloudTimeIntervalStart && starttime < itsRoadCloudTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if road cloud interval is cached failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestRoadCloudDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudDataTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestRoadCloudCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudCreatedTime();
}

std::size_t SpatiaLiteCache::fillRoadCloudCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillRoadCloudCache(mobileExternalCacheData, itsRoadCloudInsertCache);

    // Update what really now really is in the database
    auto start = conn->getOldestRoadCloudDataTime();
    auto end = conn->getLatestRoadCloudDataTime();
    Spine::WriteLock lock(itsRoadCloudTimeIntervalMutex);
    itsRoadCloudTimeIntervalStart = start;
    itsRoadCloudTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling road cloud cached failed!");
  }
}

void SpatiaLiteCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(ROADCLOUD_DATA_TABLE))
      return;

    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_cache_clean_interval(t);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsRoadCloudTimeIntervalMutex);
      itsRoadCloudTimeIntervalStart = t;
    }
    conn->cleanRoadCloudCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestRoadCloudDataTime();
    auto end = conn->getLatestRoadCloudDataTime();
    Spine::WriteLock lock(itsRoadCloudTimeIntervalMutex);
    itsRoadCloudTimeIntervalStart = start;
    itsRoadCloudTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning road cloud cache failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::roadCloudValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ret = db->getRoadCloudData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting road cloud values from cache failed!");
  }
}

bool SpatiaLiteCache::netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                              const boost::posix_time::ptime &) const
{
  try
  {
    Spine::ReadLock lock(itsNetAtmoTimeIntervalMutex);
    if (itsNetAtmoTimeIntervalStart.is_not_a_date_time() ||
        itsNetAtmoTimeIntervalEnd.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsNetAtmoTimeIntervalStart || starttime < itsNetAtmoTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if NetAtmo interval is cached failed!");
  }
}

std::size_t SpatiaLiteCache::fillNetAtmoCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillNetAtmoCache(mobileExternalCacheData, itsNetAtmoInsertCache);

    // Update what really now really is in the database
    auto start = conn->getOldestNetAtmoDataTime();
    auto end = conn->getLatestNetAtmoDataTime();
    Spine::WriteLock lock(itsNetAtmoTimeIntervalMutex);
    itsNetAtmoTimeIntervalStart = start;
    itsNetAtmoTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling NetAtmo cache failed!");
  }
}

void SpatiaLiteCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(NETATMO_DATA_TABLE))
      return;

    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_cache_clean_interval(t);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsNetAtmoTimeIntervalMutex);
      itsNetAtmoTimeIntervalStart = t;
    }
    conn->cleanNetAtmoCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestNetAtmoDataTime();
    auto end = conn->getLatestNetAtmoDataTime();
    Spine::WriteLock lock(itsNetAtmoTimeIntervalMutex);
    itsNetAtmoTimeIntervalStart = start;
    itsNetAtmoTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning NetAtmo cache failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::netAtmoValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ret = db->getNetAtmoData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestNetAtmoDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoDataTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestNetAtmoCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoCreatedTime();
}

bool SpatiaLiteCache::fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
                                             const boost::posix_time::ptime &) const
{
  try
  {
    Spine::ReadLock lock(itsFmiIoTTimeIntervalMutex);
    if (itsFmiIoTTimeIntervalStart.is_not_a_date_time() ||
        itsFmiIoTTimeIntervalEnd.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsFmiIoTTimeIntervalStart || starttime < itsFmiIoTTimeIntervalEnd);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if FmiIoT interval is cached failed!");
  }
}

std::size_t SpatiaLiteCache::fillFmiIoTCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillFmiIoTCache(mobileExternalCacheData, itsFmiIoTInsertCache);

    // Update what really now really is in the database
    auto start = conn->getOldestFmiIoTDataTime();
    auto end = conn->getLatestFmiIoTDataTime();
    Spine::WriteLock lock(itsFmiIoTTimeIntervalMutex);
    itsFmiIoTTimeIntervalStart = start;
    itsFmiIoTTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling FmiIoT cache failed!");
  }
}

void SpatiaLiteCache::cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(FMI_IOT_DATA_TABLE))
      return;

    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_cache_clean_interval(t);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsFmiIoTTimeIntervalMutex);
      itsFmiIoTTimeIntervalStart = t;
    }
    conn->cleanFmiIoTCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestFmiIoTDataTime();
    auto end = conn->getLatestFmiIoTDataTime();
    Spine::WriteLock lock(itsFmiIoTTimeIntervalMutex);
    itsFmiIoTTimeIntervalStart = start;
    itsFmiIoTTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning FmiIoT cache failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::fmiIoTValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ret = db->getFmiIoTData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting FmiIoT values from cache failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestFmiIoTDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTDataTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestFmiIoTCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTCreatedTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestObservationModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationModifiedTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestObservationTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationTime();
}

std::size_t SpatiaLiteCache::fillDataCache(const DataItems &cacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillDataCache(cacheData, itsDataInsertCache);

    // Update what really now really is in the database
    auto start = conn->getOldestObservationTime();
    auto end = conn->getLatestObservationTime();
    Spine::WriteLock lock(itsTimeIntervalMutex);
    itsTimeIntervalStart = start;
    itsTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling data cache failed!");
  }
}

void SpatiaLiteCache::cleanDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(OBSERVATION_DATA_TABLE))
      return;

    auto now = boost::posix_time::second_clock::universal_time();

    auto time1 = round_down_to_cache_clean_interval(now - timetokeep);
    auto time2 = round_down_to_cache_clean_interval(now - timetokeep_memory);

    auto conn = itsConnectionPool->getConnection();
    conn->cleanMemoryDataCache(time2);

    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsTimeIntervalMutex);
      itsTimeIntervalStart = time1;
    }
    conn->cleanDataCache(time1);

    // Update what really remains in the database
    auto start = conn->getOldestObservationTime();
    auto end = conn->getLatestObservationTime();
    Spine::WriteLock lock(itsTimeIntervalMutex);
    itsTimeIntervalStart = start;
    itsTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning data cache failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

boost::posix_time::ptime SpatiaLiteCache::getLatestWeatherDataQCModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCModifiedTime();
}

std::size_t SpatiaLiteCache::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillWeatherDataQCCache(cacheData, itsWeatherQCInsertCache);
    // Update what really now really is in the database
    auto start = conn->getOldestWeatherDataQCTime();
    auto end = conn->getLatestWeatherDataQCTime();
    Spine::WriteLock lock(itsTimeIntervalMutex);
    itsWeatherDataQCTimeIntervalStart = start;
    itsWeatherDataQCTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void SpatiaLiteCache::cleanWeatherDataQCCache(
    const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(WEATHER_DATA_QC_TABLE))
      return;

    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_cache_clean_interval(t);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsWeatherDataQCTimeIntervalMutex);
      itsWeatherDataQCTimeIntervalStart = t;
    }
    conn->cleanWeatherDataQCCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestWeatherDataQCTime();
    auto end = conn->getLatestWeatherDataQCTime();
    Spine::WriteLock lock(itsTimeIntervalMutex);
    itsWeatherDataQCTimeIntervalStart = start;
    itsWeatherDataQCTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning weather data QC cache failed!");
  }
}

void SpatiaLiteCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

SpatiaLiteCache::SpatiaLiteCache(const std::string &name,
                                 const EngineParametersPtr &p,
                                 const Spine::ConfigBase &cfg)
    : ObservationCache(p->databaseDriverInfo.getAggregateCacheInfo(name)), itsParameters(p)
{
  try
  {
    readConfig(cfg);

    // Verify multithreading is possible
    if (!sqlite3_threadsafe())
      throw Fmi::Exception(BCP, "Installed sqlite is not thread safe");

    // Switch from serialized to multithreaded access

    int err = 0;

    if (itsParameters.sqlite.threading_mode == "MULTITHREAD")
      err = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
    else if (itsParameters.sqlite.threading_mode == "SERIALIZED")
      err = sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    else
      throw Fmi::Exception(BCP,
                           "Unknown sqlite threading mode: " + itsParameters.sqlite.threading_mode);

    if (err != 0)
      throw Fmi::Exception(BCP,
                           "Failed to set sqlite3 multithread mode to " +
                               itsParameters.sqlite.threading_mode +
                               ", exit code = " + Fmi::to_string(err));

    // Enable or disable memory statistics
    err = sqlite3_config(SQLITE_CONFIG_MEMSTATUS, itsParameters.sqlite.memstatus);
    if (err != 0)
      throw Fmi::Exception(
          BCP, "Failed to initialize sqlite3 memstatus mode, exit code " + Fmi::to_string(err));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creating SpatiaLite cache failed!");
  }
}

void SpatiaLiteCache::readConfig(const Spine::ConfigBase &cfg)
{
  try
  {
    itsParameters.connectionPoolSize = Fmi::stoi(itsCacheInfo.params.at("poolSize"));
    itsParameters.cacheFile = itsCacheInfo.params.at("spatialiteFile");
    itsParameters.maxInsertSize = Fmi::stoi(itsCacheInfo.params.at("maxInsertSize"));

    itsDataInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("dataInsertCacheSize")));
    itsWeatherQCInsertCache.resize(
        Fmi::stoi(itsCacheInfo.params.at("weatherDataQCInsertCacheSize")));
    itsFlashInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("flashInsertCacheSize")));
    itsRoadCloudInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("roadCloudInsertCacheSize")));
    itsNetAtmoInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("netAtmoInsertCacheSize")));
    itsFmiIoTInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("fmiIoTInsertCacheSize")));

    itsParameters.sqlite.cache_size = Fmi::stoi(itsCacheInfo.params.at("cache_size"));
    itsParameters.sqlite.threads = Fmi::stoi(itsCacheInfo.params.at("threads"));
    itsParameters.sqlite.threading_mode = itsCacheInfo.params.at("threading_mode");
    itsParameters.sqlite.timeout = Fmi::stoi(itsCacheInfo.params.at("timeout"));
    itsParameters.sqlite.shared_cache = (Fmi::stoi(itsCacheInfo.params.at("shared_cache")) == 1);
    itsParameters.sqlite.read_uncommitted =
        (Fmi::stoi(itsCacheInfo.params.at("read_uncommitted")) == 1);
    itsParameters.sqlite.memstatus = (Fmi::stoi(itsCacheInfo.params.at("memstatus")) == 1);
    itsParameters.sqlite.synchronous = itsCacheInfo.params.at("synchronous");
    itsParameters.sqlite.journal_mode = itsCacheInfo.params.at("journal_mode");
    itsParameters.sqlite.temp_store = itsCacheInfo.params.at("temp_store");
    itsParameters.sqlite.auto_vacuum = itsCacheInfo.params.at("auto_vacuum");
    itsParameters.sqlite.mmap_size = Fmi::stoi(itsCacheInfo.params.at("mmap_size"));
    itsParameters.sqlite.wal_autocheckpoint =
        Fmi::stoi(itsCacheInfo.params.at("wal_autocheckpoint"));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading SpatiaLite settings from configuration file failed!");
  }
}

SpatiaLiteCache::~SpatiaLiteCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
