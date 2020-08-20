#include "PostgreSQLCache.h"

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

void PostgreSQLCache::initializeConnectionPool()
{
  try
  {
    // Check if already initialized (one cache can be shared by multiple database drivers)
    if (itsConnectionPool)
      return;

    logMessage("[Observation Engine] Initializing PostgreSQL cache connection pool...",
               itsParameters.quiet);

    itsConnectionPool = new PostgreSQLConnectionPool(itsParameters);

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    const std::set<std::string> &cacheTables =
        itsParameters.databaseDriverInfo.getAggregateCacheInfo(itsCacheName).tables;

    db->createTables(cacheTables);

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

    logMessage("[Observation Engine] PostgreSQL connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Initializing connection pool failed!");
  }
}

void PostgreSQLCache::initializeCaches(int finCacheDuration,
                                       int finMemoryCacheDuration,
                                       int extCacheDuration,
                                       int flashCacheDuration,
                                       int flashMemoryCacheDuration)
{
  // Nothing to do
}

ts::TimeSeriesVectorPtr PostgreSQLCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromPostgreSQL(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromPostgreSQL(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, itsTimeZones);
        return ret;
      }
      ret = db->getObservationData(stations, settings, *sinfo, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cache initialization failed!");
  }
}

ts::TimeSeriesVectorPtr PostgreSQLCache::valuesFromCache(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromPostgreSQL(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromPostgreSQL(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        ret = db->getData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
    }
    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting values from cache failed!");
  }
}

ts::TimeSeriesVectorPtr PostgreSQLCache::flashValuesFromPostgreSQL(Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    ret = db->getFlashData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting flash values from cache failed!");
  }
}
ts::TimeSeriesVectorPtr PostgreSQLCache::roadCloudValuesFromPostgreSQL(Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    ret = db->getRoadCloudData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting road cloud values from cache failed!");
  }
}

ts::TimeSeriesVectorPtr PostgreSQLCache::netAtmoValuesFromPostgreSQL(Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    ret = db->getNetAtmoData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
  }
}

ts::TimeSeriesVectorPtr PostgreSQLCache::fmiIoTValuesFromPostgreSQL(Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    ret = db->getFmiIoTData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting FmiIoT values from cache failed!");
  }
}

bool PostgreSQLCache::timeIntervalIsCached(const boost::posix_time::ptime &starttime,
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
    throw Spine::Exception::Trace(BCP, "Checking if time interval is cached failed!");
  }
}

bool PostgreSQLCache::flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime) const
{
  try
  {
    Spine::ReadLock lock(itsFlashTimeIntervalMutex);
    if (itsFlashTimeIntervalStart.is_not_a_date_time() ||
        itsFlashTimeIntervalEnd.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsFlashTimeIntervalStart && starttime < itsFlashTimeIntervalEnd);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Checking if flash interval is cached failed!");
  }
}

bool PostgreSQLCache::timeIntervalWeatherDataQCIsCached(
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
    throw Spine::Exception::Trace(BCP, "Checking if weather data QC is cached failed!");
  }
}

bool PostgreSQLCache::dataAvailableInCache(const Settings &settings) const
{
  try
  {
    // If stationtype is cached and if we have requested time interval in
    // PostgreSQL, get all data
    // from there
    if (settings.stationtype == "opendata" || settings.stationtype == "fmi" ||
        settings.stationtype == "opendata_mareograph" || settings.stationtype == "opendata_buoy" ||
        settings.stationtype == "research" || settings.stationtype == "syke")
    {
      return timeIntervalIsCached(settings.starttime, settings.endtime);
    }
    else if ((settings.stationtype == "road" || settings.stationtype == "foreign"))
      return timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime);

    else if (settings.stationtype == "flash")
      return flashIntervalIsCached(settings.starttime, settings.endtime);

    else if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    else if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

    else if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTIntervalIsCached(settings.starttime, settings.endtime);

    // Either the stationtype is not cached or the requested time interval is
    // not cached

    return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Checking if data is available in cache for stationtype '" +
                                      settings.stationtype + "' failed!");
  }
}

FlashCounts PostgreSQLCache::getFlashCount(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime,
                                           const Spine::TaggedLocationList &locations) const
{
  return itsConnectionPool->getConnection()->getFlashCount(starttime, endtime, locations);
}

boost::posix_time::ptime PostgreSQLCache::getLatestFlashTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashTime();
}

std::size_t PostgreSQLCache::fillFlashDataCache(const FlashDataItems &flashCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillFlashDataCache(flashCacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling flash data cache failed!");
  }
}

void PostgreSQLCache::cleanFlashDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration & /* timetokeep_memory */) const
{
  try
  {
    auto now = boost::posix_time::second_clock::universal_time();

    // How old observations to keep in the disk cache:
    auto t = round_down_to_cache_clean_interval(now - timetokeep);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsFlashTimeIntervalMutex);
      itsFlashTimeIntervalStart = t;
    }

    // Clean database
    conn->cleanFlashDataCache(t);

    // Update info on what is in the database
    auto start = conn->getOldestFlashTime();
    auto end = conn->getLatestFlashTime();
    Spine::WriteLock lock(itsFlashTimeIntervalMutex);
    itsFlashTimeIntervalStart = start;
    itsFlashTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning flash data cache failed!");
  }
}

boost::posix_time::ptime PostgreSQLCache::getLatestObservationModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationModifiedTime();
}

boost::posix_time::ptime PostgreSQLCache::getLatestObservationTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationTime();
}

std::size_t PostgreSQLCache::fillDataCache(const DataItems &cacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillDataCache(cacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling data cache failed!");
  }
}

void PostgreSQLCache::cleanDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  try
  {
    auto now = boost::posix_time::second_clock::universal_time();

    auto t = round_down_to_cache_clean_interval(now - timetokeep);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsTimeIntervalMutex);
      itsTimeIntervalStart = t;
    }
    conn->cleanDataCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestObservationTime();
    auto end = conn->getLatestObservationTime();
    Spine::WriteLock lock(itsTimeIntervalMutex);
    itsTimeIntervalStart = start;
    itsTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning data cache failed!");
  }
}

boost::posix_time::ptime PostgreSQLCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

std::size_t PostgreSQLCache::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillWeatherDataQCCache(cacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void PostgreSQLCache::cleanWeatherDataQCCache(
    const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning weather data QC cache failed!");
  }
}

bool PostgreSQLCache::roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
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
    throw Spine::Exception::Trace(BCP, "Checking if road cloud interval is cached failed!");
  }
}

boost::posix_time::ptime PostgreSQLCache::getLatestRoadCloudDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudDataTime();
}

boost::posix_time::ptime PostgreSQLCache::getLatestRoadCloudCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudCreatedTime();
}

std::size_t PostgreSQLCache::fillRoadCloudCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillRoadCloudCache(mobileExternalCacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling road cloud cached failed!");
  }
}

void PostgreSQLCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning road cloud cache failed!");
  }
}

bool PostgreSQLCache::netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
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
    throw Spine::Exception::Trace(BCP, "Checking if NetAtmo interval is cached failed!");
  }
}

boost::posix_time::ptime PostgreSQLCache::getLatestNetAtmoDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoDataTime();
}

boost::posix_time::ptime PostgreSQLCache::getLatestNetAtmoCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoCreatedTime();
}

std::size_t PostgreSQLCache::fillNetAtmoCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillNetAtmoCache(mobileExternalCacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling NetAtmo cache failed!");
  }
}

void PostgreSQLCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning NetAtmo cache failed!");
  }
}

bool PostgreSQLCache::fmiIoTIntervalIsCached(const boost::posix_time::ptime &starttime,
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
    throw Spine::Exception::Trace(BCP, "Checking if FmiIoT interval is cached failed!");
  }
}

boost::posix_time::ptime PostgreSQLCache::getLatestFmiIoTDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTDataTime();
}

boost::posix_time::ptime PostgreSQLCache::getLatestFmiIoTCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTCreatedTime();
}

std::size_t PostgreSQLCache::fillFmiIoTCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillFmiIoTCache(mobileExternalCacheData);

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
    throw Spine::Exception::Trace(BCP, "Filling FmiIoT cache failed!");
  }
}

void PostgreSQLCache::cleanFmiIoTCache(const boost::posix_time::time_duration &timetokeep) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning FmiIoT cache failed!");
  }
}

void PostgreSQLCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

PostgreSQLCache::PostgreSQLCache(const std::string &name,
                                 const EngineParametersPtr &p,
                                 Spine::ConfigBase &cfg)
    : ObservationCache(name), itsParameters(p)
{
  try
  {
    readConfig(cfg);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Observation-engine initialization failed");
  }
}

void PostgreSQLCache::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    const Engine::Observation::CacheInfoItem &cacheInfo =
        itsParameters.databaseDriverInfo.getAggregateCacheInfo(itsCacheName);

    itsParameters.postgresql.host = cacheInfo.params.at("host");
    itsParameters.postgresql.port = Fmi::stoi(cacheInfo.params.at("port"));
    itsParameters.postgresql.database = cacheInfo.params.at("database");
    itsParameters.postgresql.username = cacheInfo.params.at("username");
    itsParameters.postgresql.password = cacheInfo.params.at("password");
    itsParameters.postgresql.encoding = cacheInfo.params.at("encoding");
    itsParameters.postgresql.connect_timeout = Fmi::stoi(cacheInfo.params.at("connect_timeout"));
    itsParameters.connectionPoolSize = Fmi::stoi(cacheInfo.params.at("poolSize"));
    itsParameters.maxInsertSize = Fmi::stoi(cacheInfo.params.at("maxInsertSize"));
    itsParameters.dataInsertCacheSize = Fmi::stoi(cacheInfo.params.at("dataInsertCacheSize"));
    itsParameters.weatherDataQCInsertCacheSize =
        Fmi::stoi(cacheInfo.params.at("weatherDataQCInsertCacheSize"));
    itsParameters.flashInsertCacheSize = Fmi::stoi(cacheInfo.params.at("flashInsertCacheSize"));
    itsParameters.roadCloudInsertCacheSize =
        Fmi::stoi(cacheInfo.params.at("roadCloudInsertCacheSize"));
    itsParameters.netAtmoInsertCacheSize = Fmi::stoi(cacheInfo.params.at("netAtmoInsertCacheSize"));
    itsParameters.fmiIoTInsertCacheSize = Fmi::stoi(cacheInfo.params.at("fmiIoTInsertCacheSize"));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Reading PostgreSQL settings from configuration file failed");
  }
}

PostgreSQLCache::~PostgreSQLCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
