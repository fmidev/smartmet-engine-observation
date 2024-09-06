#include "SpatiaLiteCache.h"
#include "FlashMemoryCache.h"
#include "ObservationMemoryCache.h"
#include <boost/algorithm/string/join.hpp>
#include <boost/make_shared.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Convenience.h>
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

    // TapsiQc
    if (cacheTables.find(TAPSI_QC_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestTapsiQcDataTime();
      auto end = db->getLatestTapsiQcDataTime();
      itsTapsiQcTimeIntervalStart = start;
      itsTapsiQcTimeIntervalEnd = end;
    }

    // Magnetometer
    if (cacheTables.find(MAGNETOMETER_DATA_TABLE) != cacheTables.end())
    {
      auto start = db->getOldestMagnetometerDataTime();
      auto end = db->getLatestMagnetometerDataTime();
      itsMagnetometerTimeIntervalStart = start;
      itsMagnetometerTimeIntervalEnd = end;
    }

    logMessage("[Observation Engine] SpatiaLite connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Initializing connection pool failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}  // namespace Observation

void SpatiaLiteCache::initializeCaches(int /* finCacheDuration */,
                                       int finMemoryCacheDuration,
                                       int /* extCacheDuration */,
                                       int /* flashCacheDuration */,
                                       int flashMemoryCacheDuration)
{
  try
  {
    auto now = Fmi::SecondClock::universal_time();

    const std::set<std::string> &cacheTables = itsCacheInfo.tables;

    if (cacheTables.find(FLASH_DATA_TABLE) != cacheTables.end() && flashMemoryCacheDuration > 0)
    {
      logMessage("[Observation Engine] Initializing SpatiaLite flash memory cache",
                 itsParameters.quiet);
      itsFlashMemoryCache.reset(new FlashMemoryCache);
      auto timetokeep_memory = Fmi::Hours(flashMemoryCacheDuration);
      auto flashdata =
          itsConnectionPool->getConnection()->readFlashCacheData(now - timetokeep_memory);
      itsFlashMemoryCache->fill(flashdata);
    }
    if (cacheTables.find(OBSERVATION_DATA_TABLE) != cacheTables.end() && finMemoryCacheDuration > 0)
    {
      logMessage("[Observation Engine] Initializing SpatiaLite observation memory cache",
                 itsParameters.quiet);
      itsObservationMemoryCache.reset(new ObservationMemoryCache);
      auto timetokeep_memory = Fmi::Hours(finMemoryCacheDuration);
      itsConnectionPool->getConnection()->initObservationMemoryCache(now - timetokeep_memory,
                                                                     itsObservationMemoryCache);
    }

    logMessage("[Observation Engine] SpatiaLite memory cache ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cache initialization failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}

TS::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromSpatiaLite(settings);

    if (settings.stationtype == TAPSI_QC_PRODUCER)
      return tapsiQcValuesFromSpatiaLite(settings);

    if (settings.stationtype == FLASH_PRODUCER)
      return flashValuesFromSpatiaLite(settings);

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
        hit(WEATHER_DATA_QC_TABLE);
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, itsTimeZones);
      }
      else if (settings.stationtype == MAGNETO_PRODUCER &&
               magnetometerIntervalIsCached(settings.starttime, settings.endtime))
      {
        hit(MAGNETOMETER_DATA_TABLE);
        ret = db->getMagnetometerData(stations, settings, *sinfo, itsTimeZones);
      }
      else if (settings.stationtype == ICEBUOY_PRODUCER ||
               settings.stationtype == COPERNICUS_PRODUCER)
      {
        TS::TimeSeriesGeneratorOptions timeSeriesOptions;
        timeSeriesOptions.startTime = settings.starttime;
        timeSeriesOptions.endTime = settings.endtime;
        return db->getObservationDataForMovingStations(settings, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        if (itsObservationMemoryCache)
        {
          // We know that spatialite cache hits will include memory cache hits, but do not
          // bother to substract the memory cache hits from spatialite hits

          auto cache_start_time = itsObservationMemoryCache->getStartTime();
          bool use_memory_cache =
              (!cache_start_time.is_not_a_date_time() && cache_start_time <= settings.starttime);
          if (use_memory_cache)
            hit("observation_memory");
          else
            miss("observation_memory");
        }

        ret = db->getObservationData(
            stations, settings, *sinfo, itsTimeZones, itsObservationMemoryCache);
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == ROADCLOUD_PRODUCER)
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == NETATMO_PRODUCER)
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == FMI_IOT_PRODUCER)
      return fmiIoTValuesFromSpatiaLite(settings);

    if (settings.stationtype == TAPSI_QC_PRODUCER)
      return tapsiQcValuesFromSpatiaLite(settings);

    if (settings.stationtype == FLASH_PRODUCER)
      return flashValuesFromSpatiaLite(settings);

    if (settings.stationtype == ICEBUOY_PRODUCER || settings.stationtype == COPERNICUS_PRODUCER)
    {
      std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);
      return db->getObservationDataForMovingStations(settings, timeSeriesOptions, itsTimeZones);
    }

    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    auto sinfo = itsParameters.stationInfo->load();

    Spine::Stations stations = sinfo->findFmisidStations(
        settings.taggedFMISIDs, settings.stationgroups, settings.starttime, settings.endtime);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
      db->setDebug(settings.debug_options);
      db->setAdditionalTimestepOption(AdditionalTimestepOption::RequestedAndDataTimesteps);

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        hit(WEATHER_DATA_QC_TABLE);
        ret = db->getWeatherDataQCData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
      else if (settings.stationtype == MAGNETO_PRODUCER &&
               magnetometerIntervalIsCached(settings.starttime, settings.endtime))
      {
        hit(MAGNETOMETER_DATA_TABLE);
        ret = db->getMagnetometerData(stations, settings, *sinfo, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        if (itsObservationMemoryCache)
        {
          // We know that spatialite cache hits will include memory cache hits, but do not
          // bother to substract the memory cache hits from spatialite hits

          auto cache_start_time = itsObservationMemoryCache->getStartTime();
          bool use_memory_cache =
              (!cache_start_time.is_not_a_date_time() && cache_start_time <= settings.starttime);

          if (use_memory_cache)
            hit("observation_memory");
          else
            miss("observation_memory");
        }

        ret = db->getObservationData(
            stations, settings, *sinfo, timeSeriesOptions, itsTimeZones, itsObservationMemoryCache);
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::flashValuesFromSpatiaLite(const Settings &settings) const
{
  try
  {
    // Use memory cache if possible. t is not set if the cache is not ready yet
    if (itsFlashMemoryCache)
    {
      auto t = itsFlashMemoryCache->getStartTime();

      if (!t.is_not_a_date_time() && settings.starttime >= t)
      {
        hit("flash_memory");
        return itsFlashMemoryCache->getData(settings, itsParameters.parameterMap, itsTimeZones);
      }
      miss("flash_memory");
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

bool SpatiaLiteCache::timeIntervalIsCached(const Fmi::DateTime &starttime,
                                           const Fmi::DateTime & /* endtime */) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsTimeIntervalMutex);
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

bool SpatiaLiteCache::flashIntervalIsCached(const Fmi::DateTime &starttime,
                                            const Fmi::DateTime & /* endtime */) const
{
  try
  {
    // No need to check memory cache here, it is always supposed to be shorted than the disk cache
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

bool SpatiaLiteCache::timeIntervalWeatherDataQCIsCached(const Fmi::DateTime &starttime,
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

    if (s == FLASH_PRODUCER)
      return flashIntervalIsCached(settings.starttime, settings.endtime);

    if (s == ROADCLOUD_PRODUCER)
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    if (s == NETATMO_PRODUCER)
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

    if (s == FMI_IOT_PRODUCER)
      return fmiIoTIntervalIsCached(settings.starttime, settings.endtime);

    if (s == TAPSI_QC_PRODUCER)
      return tapsiQcIntervalIsCached(settings.starttime, settings.endtime);

    if (s == MAGNETO_PRODUCER)
      return magnetometerIntervalIsCached(settings.starttime, settings.endtime);

    // Either the stationtype is not cached or the requested time interval is
    // not cached
    return false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if data is available in cache failed!");
  }
}

FlashCounts SpatiaLiteCache::getFlashCount(const Fmi::DateTime &starttime,
                                           const Fmi::DateTime &endtime,
                                           const Spine::TaggedLocationList &locations) const
{
  try
  {
    // Use memory cache if possible. t is not set if the cache is not ready yet
    if (itsFlashMemoryCache)
    {
      auto t = itsFlashMemoryCache->getStartTime();

      if (!t.is_not_a_date_time() && starttime >= t)
      {
        hit("flash_memory");
        return itsFlashMemoryCache->getFlashCount(starttime, endtime, locations);
      }
      miss("flash_memory");
    }

    // Must use disk cache instead
    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(false);

    return db->getFlashCount(starttime, endtime, locations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash count from cache failed!");
  }
}

Fmi::DateTime SpatiaLiteCache::getLatestFlashModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashModifiedTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestFlashTime() const
{
  return itsConnectionPool->getConnection()->getLatestFlashTime();
}

void SpatiaLiteCache::cleanMemoryDataCache(const Fmi::DateTime &newstarttime) const
{
  try
  {
    if (itsObservationMemoryCache)
      itsObservationMemoryCache->clean(newstarttime);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of memory data cache failed!");
  }
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

void SpatiaLiteCache::cleanFlashDataCache(const Fmi::TimeDuration &timetokeep,
                                          const Fmi::TimeDuration &timetokeep_memory) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(FLASH_DATA_TABLE))
      return;

    auto now = Fmi::SecondClock::universal_time();

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

bool SpatiaLiteCache::roadCloudIntervalIsCached(const Fmi::DateTime &starttime,
                                                const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;

    {
      Spine::ReadLock lock(itsRoadCloudTimeIntervalMutex);
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

Fmi::DateTime SpatiaLiteCache::getLatestRoadCloudDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudDataTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestRoadCloudCreatedTime() const
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

void SpatiaLiteCache::cleanRoadCloudCache(const Fmi::TimeDuration &timetokeep) const
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::roadCloudValuesFromSpatiaLite(
    const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    hit(ROADCLOUD_DATA_TABLE);
    ret = db->getRoadCloudData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting road cloud values from cache failed!");
  }
}

bool SpatiaLiteCache::netAtmoIntervalIsCached(const Fmi::DateTime &starttime,
                                              const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsNetAtmoTimeIntervalMutex);
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

void SpatiaLiteCache::cleanNetAtmoCache(const Fmi::TimeDuration &timetokeep) const
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::netAtmoValuesFromSpatiaLite(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    hit(NETATMO_DATA_TABLE);
    ret = db->getNetAtmoData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
  }
}

Fmi::DateTime SpatiaLiteCache::getLatestNetAtmoDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoDataTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestNetAtmoCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoCreatedTime();
}

bool SpatiaLiteCache::fmiIoTIntervalIsCached(const Fmi::DateTime &starttime,
                                             const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;

    {
      Spine::ReadLock lock(itsFmiIoTTimeIntervalMutex);
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

void SpatiaLiteCache::cleanFmiIoTCache(const Fmi::TimeDuration &timetokeep) const
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::fmiIoTValuesFromSpatiaLite(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    hit(FMI_IOT_DATA_TABLE);
    ret = db->getFmiIoTData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting FmiIoT values from cache failed!");
  }
}

Fmi::DateTime SpatiaLiteCache::getLatestFmiIoTDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTDataTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestFmiIoTCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestFmiIoTCreatedTime();
}

bool SpatiaLiteCache::tapsiQcIntervalIsCached(const Fmi::DateTime &starttime,
                                              const Fmi::DateTime &) const
{
  try
  {
    bool ok = false;

    {
      Spine::ReadLock lock(itsTapsiQcTimeIntervalMutex);
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

std::size_t SpatiaLiteCache::fillTapsiQcCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillTapsiQcCache(mobileExternalCacheData, itsTapsiQcInsertCache);

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

void SpatiaLiteCache::cleanTapsiQcCache(const Fmi::TimeDuration &timetokeep) const
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

TS::TimeSeriesVectorPtr SpatiaLiteCache::tapsiQcValuesFromSpatiaLite(const Settings &settings) const
{
  try
  {
    TS::TimeSeriesVectorPtr ret(new TS::TimeSeriesVector);

    std::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    db->setDebug(settings.debug_options);
    hit(TAPSI_QC_DATA_TABLE);
    ret = db->getTapsiQcData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting TapsiQc values from cache failed!");
  }
}

Fmi::DateTime SpatiaLiteCache::getLatestTapsiQcDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestTapsiQcDataTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestTapsiQcCreatedTime() const
{
  return itsConnectionPool->getConnection()->getLatestTapsiQcCreatedTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestObservationModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationModifiedTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestObservationTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationTime();
}

std::size_t SpatiaLiteCache::fillDataCache(const DataItems &cacheData) const
{
  try
  {
    // Update memory cache first

    if (itsObservationMemoryCache)
      itsObservationMemoryCache->fill(cacheData);

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

std::size_t SpatiaLiteCache::fillMovingLocationsCache(const MovingLocationItems &cacheData) const
{
  try
  {
    // Update memory cache first
    /*
if (itsObservationMemoryCache)
  itsObservationMemoryCache->fill(cacheData);
    */

    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillMovingLocationsCache(cacheData, itsMovingLocationsInsertCache);
    // itsTimeIntervalStart, itsTimeIntervalEnd are updated in fillDataCache()
    /*
// Update what really now really is in the database
auto start = conn->getOldestObservationTime();
auto end = conn->getLatestObservationTime();
Spine::WriteLock lock(itsTimeIntervalMutex);
itsTimeIntervalStart = start;
itsTimeIntervalEnd = end;
    */
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling data cache failed!");
  }
}

void SpatiaLiteCache::cleanDataCache(const Fmi::TimeDuration &timetokeep,
                                     const Fmi::TimeDuration &timetokeep_memory) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(OBSERVATION_DATA_TABLE))
      return;

    auto now = Fmi::SecondClock::universal_time();

    auto time1 = round_down_to_cache_clean_interval(now - timetokeep);
    auto time2 = round_down_to_cache_clean_interval(now - timetokeep_memory);

    cleanMemoryDataCache(time2);

    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsTimeIntervalMutex);
      itsTimeIntervalStart = time1;
    }
    auto conn = itsConnectionPool->getConnection();
    conn->cleanMovingLocationsCache(time1);
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

Fmi::DateTime SpatiaLiteCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestWeatherDataQCModifiedTime() const
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
    Spine::WriteLock lock(itsWeatherDataQCTimeIntervalMutex);
    itsWeatherDataQCTimeIntervalStart = start;
    itsWeatherDataQCTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void SpatiaLiteCache::cleanWeatherDataQCCache(const Fmi::TimeDuration &timetokeep) const
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

bool SpatiaLiteCache::magnetometerIntervalIsCached(const Fmi::DateTime &starttime,
                                                   const Fmi::DateTime & /* endtime */) const
{
  try
  {
    bool ok = false;
    {
      Spine::ReadLock lock(itsMagnetometerTimeIntervalMutex);
      ok = (!itsMagnetometerTimeIntervalStart.is_not_a_date_time() &&
            !itsMagnetometerTimeIntervalEnd.is_not_a_date_time() &&
            starttime >= itsMagnetometerTimeIntervalStart);
    }

    if (ok)
      hit(MAGNETOMETER_DATA_TABLE);
    else
      miss(MAGNETOMETER_DATA_TABLE);
    return ok;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Checking if Magnetometer interval is cached failed!");
  }
}

Fmi::DateTime SpatiaLiteCache::getLatestMagnetometerDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestMagnetometerDataTime();
}

Fmi::DateTime SpatiaLiteCache::getLatestMagnetometerModifiedTime() const
{
  return itsConnectionPool->getConnection()->getLatestMagnetometerModifiedTime();
}

std::size_t SpatiaLiteCache::fillMagnetometerCache(
    const MagnetometerDataItems &magnetometerCacheData) const
{
  try
  {
    auto conn = itsConnectionPool->getConnection();
    auto sz = conn->fillMagnetometerDataCache(magnetometerCacheData, itsMagnetometerInsertCache);
    // Update what really now really is in the database
    auto start = conn->getOldestMagnetometerDataTime();
    auto end = conn->getLatestMagnetometerDataTime();
    Spine::WriteLock lock(itsMagnetometerTimeIntervalMutex);
    itsMagnetometerTimeIntervalStart = start;
    itsMagnetometerTimeIntervalEnd = end;
    return sz;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void SpatiaLiteCache::cleanMagnetometerCache(const Fmi::TimeDuration &timetokeep) const
{
  try
  {
    // Dont clean fake cache
    if (isFakeCache(MAGNETOMETER_DATA_TABLE))
      return;

    auto now = Fmi::SecondClock::universal_time();
    auto t = round_down_to_cache_clean_interval(now - timetokeep);

    auto conn = itsConnectionPool->getConnection();
    {
      // We know the cache will not contain anything before this after the update
      Spine::WriteLock lock(itsMagnetometerTimeIntervalMutex);
      itsMagnetometerTimeIntervalStart = t;
    }

    conn->cleanMagnetometerCache(t);

    // Update what really remains in the database
    auto start = conn->getOldestMagnetometerDataTime();
    auto end = conn->getLatestMagnetometerDataTime();
    Spine::WriteLock lock(itsMagnetometerTimeIntervalMutex);
    itsMagnetometerTimeIntervalStart = start;
    itsMagnetometerTimeIntervalEnd = end;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning data cache failed!");
  }
}

void SpatiaLiteCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

// This has been added for flash emulator
int SpatiaLiteCache::getMaxFlashId() const
{
  try
  {
    return itsConnectionPool->getConnection()->getMaxFlashId();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting maximum flash id from cache failed!");
  }
}

SpatiaLiteCache::SpatiaLiteCache(const std::string &name,
                                 const EngineParametersPtr &p,
                                 const Spine::ConfigBase &cfg)
    : ObservationCache(p->databaseDriverInfo.getAggregateCacheInfo(name)), itsParameters(p)
{
  try
  {
    // Create cache statistics objecs for each table
    for (const auto &tablename : itsCacheInfo.tables)
    {
      itsCacheStatistics.insert(std::make_pair(tablename, Fmi::Cache::CacheStats()));

      if (tablename == OBSERVATION_DATA_TABLE)
      {
        itsCacheStatistics.insert(std::make_pair("observation_memory", Fmi::Cache::CacheStats()));
      }
      else if (tablename == FLASH_DATA_TABLE)
      {
        itsCacheStatistics.insert(std::make_pair("flash_memory", Fmi::Cache::CacheStats()));
      }
    }

    readConfig(cfg);

    // Verify multithreading is possible
    if (!sqlite3_threadsafe())
      throw Fmi::Exception(BCP, "Installed sqlite is not thread safe");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creating SpatiaLite cache failed!");
  }
}

void SpatiaLiteCache::readConfig(const Spine::ConfigBase & /* cfg */)
{
  try
  {
    itsParameters.connectionPoolSize = Fmi::stoi(itsCacheInfo.params.at("poolSize"));
    itsParameters.cacheFile = itsCacheInfo.params.at("spatialiteFile");
    itsParameters.maxInsertSize = Fmi::stoi(itsCacheInfo.params.at("maxInsertSize"));

    itsDataInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("dataInsertCacheSize")));
    itsMovingLocationsInsertCache.resize(
        Fmi::stoi(itsCacheInfo.params.at("movingLocationsInsertCacheSize")));
    itsWeatherQCInsertCache.resize(
        Fmi::stoi(itsCacheInfo.params.at("weatherDataQCInsertCacheSize")));
    itsFlashInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("flashInsertCacheSize")));
    itsRoadCloudInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("roadCloudInsertCacheSize")));
    itsNetAtmoInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("netAtmoInsertCacheSize")));
    itsFmiIoTInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("fmiIoTInsertCacheSize")));
    itsTapsiQcInsertCache.resize(Fmi::stoi(itsCacheInfo.params.at("tapsiQcInsertCacheSize")));
    itsMagnetometerInsertCache.resize(
        Fmi::stoi(itsCacheInfo.params.at("magnetometerInsertCacheSize")));

    itsParameters.sqlite.cache_size = Fmi::stoi(itsCacheInfo.params.at("cache_size"));
    itsParameters.sqlite.threads = Fmi::stoi(itsCacheInfo.params.at("threads"));
    itsParameters.sqlite.timeout = Fmi::stoi(itsCacheInfo.params.at("timeout"));
    itsParameters.sqlite.shared_cache = (Fmi::stoi(itsCacheInfo.params.at("shared_cache")) == 1);
    itsParameters.sqlite.read_uncommitted =
        (Fmi::stoi(itsCacheInfo.params.at("read_uncommitted")) == 1);
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

Fmi::Cache::CacheStatistics SpatiaLiteCache::getCacheStats() const
{
  return itsCacheStatistics;
}

SpatiaLiteCache::~SpatiaLiteCache()
{
  shutdown();
}

void SpatiaLiteCache::getMovingStations(Spine::Stations &stations,
                                        const Settings &settings,
                                        const std::string &wkt) const
{
  itsConnectionPool->getConnection()->getMovingStations(stations, settings, wkt);
}

Fmi::DateTime SpatiaLiteCache::getLatestDataUpdateTime(const std::string &tablename,
                                                       const Fmi::DateTime &starttime,
                                                       const std::string &producer_ids,
                                                       const std::string &measurand_ids) const
{
  return itsConnectionPool->getConnection()->getLatestDataUpdateTime(
      tablename, starttime, producer_ids, measurand_ids);
}

void SpatiaLiteCache::hit(const std::string &name) const
{
  Spine::WriteLock lock(itsCacheStatisticsMutex);
  ++itsCacheStatistics.at(name).hits;
}

void SpatiaLiteCache::miss(const std::string &name) const
{
  Spine::WriteLock lock(itsCacheStatisticsMutex);
  ++itsCacheStatistics.at(name).misses;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
