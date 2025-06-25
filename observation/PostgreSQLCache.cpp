#include "PostgreSQLCache.h"
#include "ObservationMemoryCache.h"
#include <boost/make_shared.hpp>
#include <macgyver/StringConversion.h>

#include <atomic>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;
namespace
{
// Round down to HH:MM:00. Deleting an entire hour at once takes too long, and causes a major
// increase in response times. This should perhaps be made configurable.

Fmi::DateTime round_down_to_cache_clean_interval(const Fmi::DateTime &t)
{
  auto secs = (t.time_of_day().total_seconds() / 60) * 60;
  return {t.date(), Fmi::Seconds(secs)};
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

    itsConnectionPool.reset(new PostgreSQLCacheConnectionPool(itsParameters));

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    const std::set<std::string> &cacheTables = itsCacheInfo.tables;

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

    // TapsiQc
    if (cacheTables.find(TAPSI_QC_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestTapsiQcDataTime();
      auto end = db->getLatestTapsiQcDataTime();
      itsTapsiQcTimeIntervalStart = start;
      itsTapsiQcTimeIntervalEnd = end;
    }

    logMessage("[Observation Engine] PostgreSQL connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Initializing connection pool failed!");
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

TS::TimeSeriesVectorPtr PostgreSQLCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromPostgreSQL(settings);

    if (settings.stationtype == TAPSI_QC_PRODUCER)
      return tapsiQcValuesFromPostgreSQL(settings);

    if (settings.stationtype == FLASH_PRODUCER)
      return flashValuesFromPostgreSQL(settings);

    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    auto sinfo = itsParameters.stationInfo->load();

    Spine::Stations stations = sinfo->findFmisidStations(
        settings.taggedFMISIDs, settings.stationgroups, settings.starttime, settings.endtime);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      std::shared_ptr<CommonDatabaseFunctions> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);
      db->setAdditionalTimestepOption(AdditionalTimestepOption::JustRequestedTimesteps);

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ++itsCacheStatistics.at(WEATHER_DATA_QC_TABLE).hits;
        ret = db->getLocationDataItems(stations, settings, *sinfo, itsTimeZones);
      }
      else
      {
        ++itsCacheStatistics.at(OBSERVATION_DATA_TABLE).hits;

        ret = db->getObservationData(
            stations, settings, *sinfo, itsTimeZones, itsObservationMemoryCache);
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cache initialization failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLCache::valuesFromCache(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromPostgreSQL(settings);

    if (settings.stationtype == TAPSI_QC_PRODUCER)
      return tapsiQcValuesFromPostgreSQL(settings);

    if (settings.stationtype == FLASH_PRODUCER)
      return flashValuesFromPostgreSQL(settings);

    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    auto sinfo = itsParameters.stationInfo->load();

    Spine::Stations stations = sinfo->findFmisidStations(
        settings.taggedFMISIDs, settings.stationgroups, settings.starttime, settings.endtime);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);
      db->setAdditionalTimestepOption(AdditionalTimestepOption::RequestedAndDataTimesteps);

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ++itsCacheStatistics.at(WEATHER_DATA_QC_TABLE).hits;
        ret = db->getLocationDataItems(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        ++itsCacheStatistics.at(OBSERVATION_DATA_TABLE).hits;
        ret = db->getObservationData(
            stations, settings, *sinfo, timeSeriesOptions, itsTimeZones, itsObservationMemoryCache);
      }
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting values from cache failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLCache::flashValuesFromPostgreSQL(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ++itsCacheStatistics.at(FLASH_DATA_TABLE).hits;
    ret = db->getFlashData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash values from cache failed!");
  }
}
TS::TimeSeriesVectorPtr PostgreSQLCache::roadCloudValuesFromPostgreSQL(
    const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ++itsCacheStatistics.at(ROADCLOUD_DATA_TABLE).hits;
    ret = db->getRoadCloudData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting road cloud values from cache failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLCache::netAtmoValuesFromPostgreSQL(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ++itsCacheStatistics.at(NETATMO_DATA_TABLE).hits;
    ret = db->getNetAtmoData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLCache::fmiIoTValuesFromPostgreSQL(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ++itsCacheStatistics.at(FMI_IOT_DATA_TABLE).hits;
    ret = db->getFmiIoTData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting FmiIoT values from cache failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLCache::tapsiQcValuesFromPostgreSQL(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<PostgreSQLCacheDB> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    ++itsCacheStatistics.at(TAPSI_QC_DATA_TABLE).hits;
    ret = db->getTapsiQcData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting TapsiQc values from cache failed!");
  }
}

bool PostgreSQLCache::timeIntervalIsCached(const Fmi::DateTime &starttime,
                                           const Fmi::DateTime & /* endtime */) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsTimeIntervalMutex);
      // We ignore end time intentionally
      ok = (!itsTimeIntervalStart.is_not_a_date_time() &&
            !itsTimeIntervalEnd.is_not_a_date_time() && starttime >= itsTimeIntervalStart);
    }

    if (ok)
      hit(OBSERVATION_DATA_TABLE);
    else
      miss(OBSERVATION_DATA_TABLE);

    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if time interval is cached failed!");
  }
}

bool PostgreSQLCache::flashIntervalIsCached(const Fmi::DateTime &starttime,
                                            const Fmi::DateTime & /* endtime */) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsFlashTimeIntervalMutex);
      // We ignore end time intentionally
      ok =
          (!itsFlashTimeIntervalStart.is_not_a_date_time() &&
           !itsFlashTimeIntervalEnd.is_not_a_date_time() && starttime >= itsFlashTimeIntervalStart);
    }

    if (ok)
      hit(FLASH_DATA_TABLE);
    else
      miss(FLASH_DATA_TABLE);
    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if flash interval is cached failed!");
  }
}

bool PostgreSQLCache::timeIntervalWeatherDataQCIsCached(const Fmi::DateTime &starttime,
                                                        const Fmi::DateTime & /* endtime */) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsWeatherDataQCTimeIntervalMutex);
      ok = (!itsWeatherDataQCTimeIntervalStart.is_not_a_date_time() &&
            !itsWeatherDataQCTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsWeatherDataQCTimeIntervalStart);
    }

    if (ok)
      hit(WEATHER_DATA_QC_TABLE);
    else
      miss(WEATHER_DATA_QC_TABLE);
    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if weather data QC is cached failed!");
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
    if ((settings.stationtype == "road" || settings.stationtype == "foreign"))
      return timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime);

    if (settings.stationtype == FLASH_PRODUCER)
      return flashIntervalIsCached(settings.starttime, settings.endtime);

    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTIntervalIsCached(settings.starttime, settings.endtime);

    if (settings.stationtype == TAPSI_QC_PRODUCER)
      return tapsiQcIntervalIsCached(settings.starttime, settings.endtime);

    // Either the stationtype is not cached or the requested time interval is
    // not cached

    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Checking if data is available in cache for stationtype '" +
                                    settings.stationtype + "' failed!");
  }
}

FlashCounts PostgreSQLCache::getFlashCount(const Fmi::DateTime &starttime,
                                           const Fmi::DateTime &endtime,
                                           const Spine::TaggedLocationList &locations) const
{
  return itsConnectionPool->getConnection()->getFlashCount(starttime, endtime, locations);
}

Fmi::DateTime PostgreSQLCache::getLatestFlashModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashModifiedTime();
}

Fmi::DateTime PostgreSQLCache::getLatestFlashTime() const
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
    throw Fmi::Exception::Trace(BCP, "Filling flash data cache failed!");
  }
}

void PostgreSQLCache::cleanFlashDataCache(const Fmi::TimeDuration &timetokeep,
                                          const Fmi::TimeDuration & /* timetokeep_memory */) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(FLASH_DATA_TABLE))
      return;

    auto now = Fmi::SecondClock::universal_time();

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
    throw Fmi::Exception::Trace(BCP, "Cleaning flash data cache failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestObservationModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationModifiedTime();
}

Fmi::DateTime PostgreSQLCache::getLatestObservationTime() const
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
    throw Fmi::Exception::Trace(BCP, "Filling data cache failed!");
  }
}

std::size_t PostgreSQLCache::fillMovingLocationsCache(const MovingLocationItems &cacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillMovingLocationsCache(cacheData);

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

void PostgreSQLCache::cleanDataCache(const Fmi::TimeDuration &timetokeep,
                                     const Fmi::TimeDuration & /* timetokeep_memory */) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(OBSERVATION_DATA_TABLE))
      return;

    auto now = Fmi::SecondClock::universal_time();

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
    throw Fmi::Exception::Trace(BCP, "Cleaning data cache failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

Fmi::DateTime PostgreSQLCache::getLatestWeatherDataQCModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCModifiedTime();
}

std::size_t PostgreSQLCache::fillWeatherDataQCCache(const DataItems &cacheData) const
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
    throw Fmi::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void PostgreSQLCache::cleanWeatherDataQCCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(WEATHER_DATA_QC_TABLE))
      return;

    Fmi::DateTime t = Fmi::SecondClock::universal_time() - timetokeep;
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

bool PostgreSQLCache::roadCloudIntervalIsCached(const Fmi::DateTime &starttime,
                                                const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;

    {
      Spine::ReadLock lock(itsRoadCloudTimeIntervalMutex);
      // We ignore end time intentionally
      ok = (!itsRoadCloudTimeIntervalStart.is_not_a_date_time() &&
            !itsRoadCloudTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsRoadCloudTimeIntervalStart);
    }

    if (ok)
      hit(ROADCLOUD_DATA_TABLE);
    else
      miss(ROADCLOUD_DATA_TABLE);

    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if road cloud interval is cached failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestRoadCloudDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudDataTime();
}

Fmi::DateTime PostgreSQLCache::getLatestRoadCloudCreatedTime() const
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
    throw Fmi::Exception::Trace(BCP, "Filling road cloud cached failed!");
  }
}

void PostgreSQLCache::cleanRoadCloudCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(ROADCLOUD_DATA_TABLE))
      return;

    Fmi::DateTime t = Fmi::SecondClock::universal_time() - timetokeep;
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

bool PostgreSQLCache::netAtmoIntervalIsCached(const Fmi::DateTime &starttime,
                                              const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;

    {
      Spine::ReadLock lock(itsNetAtmoTimeIntervalMutex);
      // We ignore end time intentionally
      ok = (!itsNetAtmoTimeIntervalStart.is_not_a_date_time() &&
            !itsNetAtmoTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsNetAtmoTimeIntervalStart);
    }

    if (ok)
      hit(NETATMO_DATA_TABLE);
    else
      miss(NETATMO_DATA_TABLE);

    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if NetAtmo interval is cached failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestNetAtmoDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoDataTime();
}

Fmi::DateTime PostgreSQLCache::getLatestNetAtmoCreatedTime() const
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
    throw Fmi::Exception::Trace(BCP, "Filling NetAtmo cache failed!");
  }
}

void PostgreSQLCache::cleanNetAtmoCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(NETATMO_DATA_TABLE))
      return;

    Fmi::DateTime t = Fmi::SecondClock::universal_time() - timetokeep;
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

bool PostgreSQLCache::fmiIoTIntervalIsCached(const Fmi::DateTime &starttime,
                                             const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsFmiIoTTimeIntervalMutex);
      // We ignore end time intentionally
      ok = (!itsFmiIoTTimeIntervalStart.is_not_a_date_time() &&
            !itsFmiIoTTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsFmiIoTTimeIntervalStart);
    }

    if (ok)
      hit(FMI_IOT_DATA_TABLE);
    else
      miss(FMI_IOT_DATA_TABLE);
    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if FmiIoT interval is cached failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestFmiIoTDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTDataTime();
}

Fmi::DateTime PostgreSQLCache::getLatestFmiIoTCreatedTime() const
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
    throw Fmi::Exception::Trace(BCP, "Filling FmiIoT cache failed!");
  }
}

void PostgreSQLCache::cleanFmiIoTCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(FMI_IOT_DATA_TABLE))
      return;

    Fmi::DateTime t = Fmi::SecondClock::universal_time() - timetokeep;
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

bool PostgreSQLCache::tapsiQcIntervalIsCached(const Fmi::DateTime &starttime,
                                              const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsTapsiQcTimeIntervalMutex);
      // We ignore end time intentionally
      ok = (!itsTapsiQcTimeIntervalStart.is_not_a_date_time() &&
            !itsTapsiQcTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsTapsiQcTimeIntervalStart);
    }

    if (ok)
      hit(TAPSI_QC_DATA_TABLE);
    else
      miss(TAPSI_QC_DATA_TABLE);
    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if TapsiQc interval is cached failed!");
  }
}

Fmi::DateTime PostgreSQLCache::getLatestTapsiQcDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestTapsiQcDataTime();
}

Fmi::DateTime PostgreSQLCache::getLatestTapsiQcCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestTapsiQcCreatedTime();
}

std::size_t PostgreSQLCache::fillTapsiQcCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillTapsiQcCache(mobileExternalCacheData);

    // Update what really now really is in the database
    auto start = conn->getOldestTapsiQcDataTime();
    auto end = conn->getLatestTapsiQcDataTime();
    Spine::WriteLock lock(itsTapsiQcTimeIntervalMutex);
    itsTapsiQcTimeIntervalStart = start;
    itsTapsiQcTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling TapsiQc cache failed!");
  }
}

void PostgreSQLCache::cleanTapsiQcCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(TAPSI_QC_DATA_TABLE))
      return;

    Fmi::DateTime t = Fmi::SecondClock::universal_time() - timetokeep;
    t = round_down_to_cache_clean_interval(t);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsTapsiQcTimeIntervalMutex);
      itsTapsiQcTimeIntervalStart = t;
    }
    conn->cleanTapsiQcCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestTapsiQcDataTime();
    auto end = conn->getLatestTapsiQcDataTime();
    Spine::WriteLock lock(itsTapsiQcTimeIntervalMutex);
    itsTapsiQcTimeIntervalStart = start;
    itsTapsiQcTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning TapsiQc cache failed!");
  }
}

bool PostgreSQLCache::magnetometerIntervalIsCached(const Fmi::DateTime & /* starttime */,
                                                   const Fmi::DateTime & /* endtime */) const
{
  return false;
}

Fmi::DateTime PostgreSQLCache::getLatestMagnetometerDataTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

Fmi::DateTime PostgreSQLCache::getLatestMagnetometerModifiedTime() const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::size_t PostgreSQLCache::fillMagnetometerCache(
    const MagnetometerDataItems & /* magnetometerCacheData */) const
{
  return 0;
}

void PostgreSQLCache::cleanMagnetometerCache(const Fmi::TimeDuration &timetokeep) const {}

void PostgreSQLCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

PostgreSQLCache::PostgreSQLCache(const std::string &name,
                                 const EngineParametersPtr &p,
                                 const Spine::ConfigBase &cfg)
    : ObservationCache(p->databaseDriverInfo.getAggregateCacheInfo(name)), itsParameters(p)
{
  try
  {
    readConfig(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Observation-engine initialization failed");
  }
}

void PostgreSQLCache::readConfig(const Spine::ConfigBase & /* cfg */)
{
  try
  {
    itsParameters.postgresql.host = itsCacheInfo.params.at("host");
    itsParameters.postgresql.port = Fmi::stoi(itsCacheInfo.params.at("port"));
    itsParameters.postgresql.database = itsCacheInfo.params.at("database");
    itsParameters.postgresql.username = itsCacheInfo.params.at("username");
    itsParameters.postgresql.password = itsCacheInfo.params.at("password");
    itsParameters.postgresql.encoding = itsCacheInfo.params.at("encoding");
    itsParameters.postgresql.connect_timeout = Fmi::stoi(itsCacheInfo.params.at("connect_timeout"));

    auto pos = itsCacheInfo.params.find("slow_query_limit");
    if (pos != itsCacheInfo.params.end())
      itsParameters.postgresql.slow_query_limit = Fmi::stoi(pos->second);

    itsParameters.connectionPoolSize = Fmi::stoi(itsCacheInfo.params.at("poolSize"));
    itsParameters.maxInsertSize = Fmi::stoi(itsCacheInfo.params.at("maxInsertSize"));
    itsParameters.dataInsertCacheSize = Fmi::stoi(itsCacheInfo.params.at("dataInsertCacheSize"));
    itsParameters.weatherDataQCInsertCacheSize =
        Fmi::stoi(itsCacheInfo.params.at("weatherDataQCInsertCacheSize"));
    itsParameters.flashInsertCacheSize = Fmi::stoi(itsCacheInfo.params.at("flashInsertCacheSize"));
    itsParameters.roadCloudInsertCacheSize =
        Fmi::stoi(itsCacheInfo.params.at("roadCloudInsertCacheSize"));
    itsParameters.netAtmoInsertCacheSize =
        Fmi::stoi(itsCacheInfo.params.at("netAtmoInsertCacheSize"));
    itsParameters.fmiIoTInsertCacheSize =
        Fmi::stoi(itsCacheInfo.params.at("fmiIoTInsertCacheSize"));
    itsParameters.tapsiQcInsertCacheSize =
        Fmi::stoi(itsCacheInfo.params.at("tapsiQcInsertCacheSize"));
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading PostgreSQL settings from configuration file failed");
  }
}

PostgreSQLCache::~PostgreSQLCache()
{
  shutdown();
}

void PostgreSQLCache::getMovingStations(Spine::Stations & /*stations*/,
                                        const Settings & /*settings*/,
                                        const std::string & /*wkt*/) const
{
}

void PostgreSQLCache::hit(const std::string &name) const
{
  Spine::WriteLock lock(itsCacheStatisticsMutex);
  ++itsCacheStatistics.at(name).hits;
}

void PostgreSQLCache::miss(const std::string &name) const
{
  Spine::WriteLock lock(itsCacheStatisticsMutex);
  ++itsCacheStatistics.at(name).misses;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
