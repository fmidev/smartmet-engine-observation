#include "SpatiaLiteCache.h"

#include "ObservableProperty.h"

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
    logMessage("[Observation Engine] Initializing SpatiaLite cache connection pool...",
               itsParameters.quiet);

    itsConnectionPool = new SpatiaLiteConnectionPool(itsParameters);

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    spatialitedb->createTables();

    for (int i = 0; i < itsParameters.connectionPoolSize; i++)
    {
      boost::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
      if (i == 0)
      {
        // Observation data
        auto start = db->getOldestObservationTime();
        auto end = db->getLatestObservationTime();
        itsTimeIntervalStart = start;
        itsTimeIntervalEnd = end;

        // WeatherDataQC
        start = db->getOldestWeatherDataQCTime();
        end = db->getLatestWeatherDataQCTime();
        itsWeatherDataQCTimeIntervalStart = start;
        itsWeatherDataQCTimeIntervalEnd = end;

        // Flash
        start = db->getOldestFlashTime();
        end = db->getLatestFlashTime();
        itsFlashTimeIntervalStart = start;
        itsFlashTimeIntervalEnd = end;

        // Road cloud
        start = db->getOldestRoadCloudDataTime();
        end = db->getLatestRoadCloudDataTime();
        itsRoadCloudTimeIntervalStart = start;
        itsRoadCloudTimeIntervalEnd = end;

        // NetAtmo
        start = db->getOldestNetAtmoDataTime();
        end = db->getLatestNetAtmoDataTime();
        itsNetAtmoTimeIntervalStart = start;
        itsNetAtmoTimeIntervalEnd = end;
      }
    }

    logMessage("[Observation Engine] SpatiaLite connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Initializing connection pool failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}

void SpatiaLiteCache::initializeCaches(int finCacheDuration,
                                       int finMemoryCacheDuration,
                                       int extCacheDuration,
                                       int flashCacheDuration,
                                       int flashMemoryCacheDuration)
{
  try
  {
    auto now = boost::posix_time::second_clock::universal_time();

    if (flashMemoryCacheDuration > 0)
    {
      logMessage("[Observation Engine] Initializing SpatiaLite flash memory cache",
                 itsParameters.quiet);
      itsFlashMemoryCache.reset(new FlashMemoryCache);
      auto timetokeep_memory = boost::posix_time::hours(flashMemoryCacheDuration);
      auto flashdata =
          itsConnectionPool->getConnection()->readFlashCacheData(now - timetokeep_memory);
      itsFlashMemoryCache->fill(flashdata);
    }
    if (finMemoryCacheDuration > 0)
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
    throw Spine::Exception::Trace(BCP, "Cache initialization failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == "roadcloud")
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == "netatmo")
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    SmartMet::Spine::Stations stations =
        itsParameters.stationInfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, *itsParameters.stationInfo, itsTimeZones);
        return ret;
      }

      ret =
          spatialitedb->getCachedData(stations, settings, *itsParameters.stationInfo, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(
        BCP, "Getting values from cache for stationtype '" + settings.stationtype + "' failed!");
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == "roadcloud")
      return roadCloudValuesFromSpatiaLite(settings);

    if (settings.stationtype == "netatmo")
      return netAtmoValuesFromSpatiaLite(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    SmartMet::Spine::Stations stations =
        itsParameters.stationInfo->findFmisidStations(settings.taggedFMISIDs);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();

      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, *itsParameters.stationInfo, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        ret = spatialitedb->getCachedData(
            stations, settings, *itsParameters.stationInfo, timeSeriesOptions, itsTimeZones);
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(
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
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    return spatialitedb->getCachedFlashData(settings, itsTimeZones);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting flash values from cache failed!");
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
    throw Spine::Exception::Trace(BCP, "Checking if time interval is cached failed!");
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
    throw Spine::Exception::Trace(BCP, "Checking if flash interval is cached failed!");
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
    throw Spine::Exception::Trace(BCP, "Checking if weather data QC is cached failed!");
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

    if (s == "roadcloud")
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    if (s == "netatmo")
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

    // Either the stationtype is not cached or the requested time interval is
    // not cached
    return false;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Checking if data is available in cache failed!");
  }
}

FlashCounts SpatiaLiteCache::getFlashCount(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime,
                                           const Spine::TaggedLocationList &locations) const
{
  return itsConnectionPool->getConnection()->getFlashCount(starttime, endtime, locations);
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
    throw Spine::Exception::Trace(BCP, "Filling flash data cache failed!");
  }
}

void SpatiaLiteCache::cleanFlashDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning flash data cache failed!");
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
    throw Spine::Exception::Trace(BCP, "Checking if road cloud interval is cached failed!");
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
    throw Spine::Exception::Trace(BCP, "Filling road cloud cached failed!");
  }
}

void SpatiaLiteCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const
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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::roadCloudValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    ret = spatialitedb->getCachedRoadCloudData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting road cloud values from cache failed!");
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
    throw Spine::Exception::Trace(BCP, "Checking if NetAtmo interval is cached failed!");
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
    throw Spine::Exception::Trace(BCP, "Filling NetAtmo cache failed!");
  }
}

void SpatiaLiteCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const
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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::netAtmoValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    ret = spatialitedb->getCachedNetAtmoData(settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
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
    throw Spine::Exception::Trace(BCP, "Filling data cache failed!");
  }
}

void SpatiaLiteCache::cleanDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  try
  {
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
    throw Spine::Exception::Trace(BCP, "Cleaning data cache failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
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
    throw Spine::Exception::Trace(BCP, "Filling weather data QC cache failed!");
  }
}

void SpatiaLiteCache::cleanWeatherDataQCCache(
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

void SpatiaLiteCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

SpatiaLiteCache::SpatiaLiteCache(const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : itsParameters(p)
{
  try
  {
    readConfig(cfg);

    // Verify multithreading is possible
    if (!sqlite3_threadsafe())
      throw Spine::Exception(BCP, "Installed sqlite is not thread safe");

    // Switch from serialized to multithreaded access

    int err;

    if (itsParameters.sqlite.threading_mode == "MULTITHREAD")
      err = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
    else if (itsParameters.sqlite.threading_mode == "SERIALIZED")
      err = sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    else
      throw Spine::Exception(
          BCP, "Unknown sqlite threading mode: " + itsParameters.sqlite.threading_mode);

    if (err != 0)
      throw Spine::Exception(BCP,
                             "Failed to set sqlite3 multithread mode to " +
                                 itsParameters.sqlite.threading_mode +
                                 ", exit code = " + Fmi::to_string(err));

    // Enable or disable memory statistics
    err = sqlite3_config(SQLITE_CONFIG_MEMSTATUS, itsParameters.sqlite.memstatus);
    if (err != 0)
      throw Spine::Exception(
          BCP, "Failed to initialize sqlite3 memstatus mode, exit code " + Fmi::to_string(err));
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creating SpatiaLite cache failed!");
  }
}

boost::shared_ptr<std::vector<ObservableProperty> > SpatiaLiteCache::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language) const
{
  boost::shared_ptr<std::vector<ObservableProperty> > data(new std::vector<ObservableProperty>());
  try
  {
    std::string stationType("metadata");
    data = itsConnectionPool->getConnection()->getObservableProperties(
        parameters, language, stationType);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "observablePropertyQuery failed!");
  }

  return data;
}

void SpatiaLiteCache::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    itsParameters.connectionPoolSize = cfg.get_mandatory_config_param<int>("cache.poolSize");

    itsParameters.cacheFile = cfg.get_mandatory_path("spatialiteFile");

    itsParameters.maxInsertSize = cfg.get_optional_config_param<std::size_t>(
        "cache.maxInsertSize", 99999999);  // default = all at once

    itsDataInsertCache.resize(
        cfg.get_optional_config_param<std::size_t>("cache.dataInsertCacheSize", 1000000));
    itsWeatherQCInsertCache.resize(
        cfg.get_optional_config_param<std::size_t>("cache.weatherDataQCInsertCacheSize", 1000000));
    itsFlashInsertCache.resize(
        cfg.get_optional_config_param<std::size_t>("cache.flashInsertCacheSize", 100000));
    itsRoadCloudInsertCache.resize(
        cfg.get_optional_config_param<std::size_t>("cache.roadCloudInsertCacheSize", 50000));
    itsNetAtmoInsertCache.resize(
        cfg.get_optional_config_param<std::size_t>("cache.netAtmoInsertCacheSize", 50000));

    itsParameters.sqlite.cache_size =
        cfg.get_optional_config_param<long>("sqlite.cache_size", 0);  // zero = use default value

    itsParameters.sqlite.threads =
        cfg.get_optional_config_param<int>("sqlite.threads", 0);  // zero = no helper threads

    itsParameters.sqlite.threading_mode =
        cfg.get_optional_config_param<std::string>("sqlite.threading_mode", "SERIALIZED");

    itsParameters.sqlite.timeout = cfg.get_optional_config_param<size_t>("sqlite.timeout", 30000);

    itsParameters.sqlite.shared_cache =
        cfg.get_optional_config_param<bool>("sqlite.shared_cache", false);

    itsParameters.sqlite.read_uncommitted =
        cfg.get_optional_config_param<bool>("sqlite.read_uncommitted", false);

    itsParameters.sqlite.memstatus = cfg.get_optional_config_param<bool>("sqlite.memstatus", false);

    itsParameters.sqlite.synchronous =
        cfg.get_optional_config_param<std::string>("sqlite.synchronous", "NORMAL");

    itsParameters.sqlite.journal_mode =
        cfg.get_optional_config_param<std::string>("sqlite.journal_mode", "WAL");

    itsParameters.sqlite.temp_store =
        cfg.get_optional_config_param<std::string>("sqlite.temp_store", "DEFAULT");

    itsParameters.sqlite.auto_vacuum =
        cfg.get_optional_config_param<std::string>("sqlite.auto_vacuum", "NONE");

    itsParameters.sqlite.mmap_size = cfg.get_optional_config_param<long>("sqlite.mmap_size", 0);

    itsParameters.sqlite.wal_autocheckpoint =
        cfg.get_optional_config_param<int>("sqlite.wal_autocheckpoint", 1000);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Reading SpatiaLite settings from configuration file failed!");
  }
}

SpatiaLiteCache::~SpatiaLiteCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
