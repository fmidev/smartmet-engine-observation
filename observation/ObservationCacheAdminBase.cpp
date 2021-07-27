#include "ObservationCacheAdminBase.h"
#include "Utils.h"
#include <macgyver/AnsiEscapeCodes.h>
#include <spine/Convenience.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCacheAdminBase::ObservationCacheAdminBase(const DatabaseDriverParameters& parameters,
                                                     Engine::Geonames::Engine* geonames,
                                                     std::atomic<bool>& conn_ok,
                                                     bool timer)
    : itsParameters(parameters),
      itsCacheProxy(parameters.params->observationCacheProxy),
      itsGeonames(geonames),
      itsShutdownRequested(false),
      itsConnectionsOK(conn_ok),
      itsTimer(timer),
      itsBackgroundTasks(new Fmi::AsyncTaskGroup)
{
  itsBackgroundTasks->on_task_error(
      [](const std::string& task_name)
      {
        auto err = Fmi::Exception::Trace(BCP, "Operation failed");
        err.addParameter("Task", task_name);
        throw err;
      });
}

// FIXME: catch condition when destroying without calling shutdown first as it
//        could lead to pure virtual method called
ObservationCacheAdminBase::~ObservationCacheAdminBase() = default;

void ObservationCacheAdminBase::shutdown()
{
  itsShutdownRequested = true;
  itsBackgroundTasks->stop();
  itsCacheProxy->shutdown();
}

void ObservationCacheAdminBase::init()
{
  try
  {
    const DatabaseDriverInfoItem& ddi =
        itsParameters.params->databaseDriverInfo.getDatabaseDriverInfo(itsParameters.driverName);
    // Cache names
    std::set<std::string> cachenames = ddi.caches;
    std::map<std::string, std::string> cache_tables;  // For debug purposes

    // Table names
    std::set<std::string> tablenames;
    for (const auto& cachename : cachenames)
    {
      if (cache_tables.find(cachename) == cache_tables.end())
        cache_tables[cachename] = "";

      const CacheInfoItem& cii = ddi.getCacheInfo(cachename);
      for (const auto& t : cii.tables)
      {
        tablenames.insert(t);
        if (!cache_tables.at(cachename).empty())
          cache_tables[cachename] += ", ";
        cache_tables[cachename] += t;
      }
    }

    if (cache_tables.size() > 0)
    {
      for (const auto& item : cache_tables)
        logMessage(" Table '" + item.second + "' is cached in '" + item.first + "'...",
                   itsParameters.quiet);
    }

    std::shared_ptr<ObservationCache> observationCache;
    std::shared_ptr<ObservationCache> weatherDataQCCache;
    std::shared_ptr<ObservationCache> flashCache;
    std::shared_ptr<ObservationCache> netatmoCache;
    std::shared_ptr<ObservationCache> roadcloudCache;
    std::shared_ptr<ObservationCache> fmiIoTCache;
    std::set<ObservationCache*> cache_set;

    for (const auto& tablename : tablenames)
    {
      if (tablename == OBSERVATION_DATA_TABLE)
      {
        observationCache = getCache(OBSERVATION_DATA_TABLE);
        cache_set.insert(observationCache.get());
      }
      else if (tablename == WEATHER_DATA_QC_TABLE)
      {
        weatherDataQCCache = getCache(WEATHER_DATA_QC_TABLE);
        cache_set.insert(weatherDataQCCache.get());
      }
      else if (tablename == FLASH_DATA_TABLE)
      {
        flashCache = getCache(FLASH_DATA_TABLE);
        cache_set.insert(flashCache.get());
      }
      else if (tablename == NETATMO_DATA_TABLE)
      {
        netatmoCache = getCache(NETATMO_DATA_TABLE);
        cache_set.insert(netatmoCache.get());
      }
      else if (tablename == ROADCLOUD_DATA_TABLE)
      {
        roadcloudCache = getCache(ROADCLOUD_DATA_TABLE);
        cache_set.insert(roadcloudCache.get());
      }
      else if (tablename == FMI_IOT_DATA_TABLE)
      {
        fmiIoTCache = getCache(FMI_IOT_DATA_TABLE);
        cache_set.insert(fmiIoTCache.get());
      }
    }

    for (auto* cache : cache_set)
    {
      cache->initializeConnectionPool();
      cache->initializeCaches(itsParameters.finCacheDuration,
                              itsParameters.finMemoryCacheDuration,
                              itsParameters.extCacheDuration,
                              itsParameters.flashCacheDuration,
                              itsParameters.flashMemoryCacheDuration);
    }

    // Update all caches once in parallel
    if (!itsParameters.disableAllCacheUpdates)
    {
      if (observationCache)
      {
        // First clean all caches once. If the server has been down for a long
        // time, the sqlite file will increase in size significantly if this
        // is not done first. We will not start threads for these since sqlite
        // would do them serially anyway.

        observationCache->cleanDataCache(
            boost::posix_time::hours(itsParameters.finCacheDuration),
            boost::posix_time::hours(itsParameters.finMemoryCacheDuration));

        // Oracle reads can be parallelized. The writes will be done
        // in practise serially, even though the threads will give
        // each other some timeslices.
        if (itsParameters.finCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init observation cache", [this]() { updateObservationCache(); });
        }
      }

      if (weatherDataQCCache)
      {
        weatherDataQCCache->cleanWeatherDataQCCache(
            boost::posix_time::hours(itsParameters.extCacheDuration));

        if (itsParameters.extCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init weather data QC cache",
                                  [this]() { updateWeatherDataQCCache(); });
        }
      }

      if (flashCache)
      {
        flashCache->cleanFlashDataCache(
            boost::posix_time::hours(itsParameters.flashCacheDuration),
            boost::posix_time::hours(itsParameters.flashMemoryCacheDuration));

        if (itsParameters.flashCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init flash cache", [this]() { updateFlashCache(); });
        }
      }
      if (netatmoCache)
      {
        netatmoCache->cleanNetAtmoCache(
            boost::posix_time::hours(itsParameters.netAtmoCacheDuration));

        if (itsParameters.netAtmoCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init Netatmo cache", [this]() { updateNetAtmoCache(); });
        }
      }

      if (roadcloudCache)
      {
        roadcloudCache->cleanRoadCloudCache(
            boost::posix_time::hours(itsParameters.roadCloudCacheDuration));

        if (itsParameters.roadCloudCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init roadcloud cache", [this]() { updateRoadCloudCache(); });
        }
      }

      if (fmiIoTCache)
      {
        fmiIoTCache->cleanFmiIoTCache(boost::posix_time::hours(itsParameters.fmiIoTCacheDuration));

        if (itsParameters.fmiIoTCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init fmi_iot cache", [this]() { updateFmiIoTCache(); });
        }
      }
    }

    // If stations info does not exist (stations.txt file  missing), load info from database
    if (itsParameters.loadStations && itsParameters.params->stationInfo->stations.size() == 0)
    {
      std::cout << Spine::log_time_str() << driverName()
                << " Stations info missing, loading from database! " << std::endl;
      itsBackgroundTasks->add("Load station data", [this]() { loadStations(); });
    }

    itsBackgroundTasks->wait();

    startCacheUpdateThreads(tablenames);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::startCacheUpdateThreads(const std::set<std::string>& tables)
{
  try
  {
    if (itsShutdownRequested || (tables.empty() && !itsParameters.loadStations))
      return;

    if (itsParameters.loadStations)
    {
      itsBackgroundTasks->add("station cache update loop", [this]() { updateStationsCacheLoop(); });
    }

    // Updates are disabled for example in regression tests and sometimes when
    // profiling
    if (itsParameters.disableAllCacheUpdates)
    {
      std::cout << Spine::log_time_str() << ANSI_FG_GREEN
                << " Note! Observation cache updates disabled for tables "
                << boost::algorithm::join(tables, ", ") << "! " << ANSI_FG_DEFAULT << std::endl;
      return;
    }

    for (const auto& tablename : tables)
    {
      // Dont start update loop for fake cache
      if (getCache(tablename)->isFakeCache(tablename))
        return;
    }

    if (tables.find(OBSERVATION_DATA_TABLE) != tables.end() &&
        itsParameters.finCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("observation cache update loop",
                              [this]() { updateObservationCacheLoop(); });
    }

    if (tables.find(WEATHER_DATA_QC_TABLE) != tables.end() &&
        itsParameters.extCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("weather data QC cache update loop",
                              [this]() { updateWeatherDataQCCacheLoop(); });
    }

    if (tables.find(FLASH_DATA_TABLE) != tables.end() && itsParameters.flashCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("flash data cache update loop", [this]() { updateFlashCacheLoop(); });
    }

    if (tables.find(NETATMO_DATA_TABLE) != tables.end() &&
        itsParameters.netAtmoCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("netatmo cache update loop", [this]() { updateNetAtmoCacheLoop(); });
    }

    if (tables.find(ROADCLOUD_DATA_TABLE) != tables.end() &&
        itsParameters.roadCloudCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("road cloud cache update loop",
                              [this]() { updateRoadCloudCacheLoop(); });
    }

    if (tables.find(FMI_IOT_DATA_TABLE) != tables.end() &&
        itsParameters.fmiIoTCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("fmi_iot cache update loop", [this]() { updateFmiIoTCacheLoop(); });
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::updateFlashFakeCache(std::shared_ptr<ObservationCache>& cache) const
{
  std::vector<std::map<std::string, std::string>> settings =
      cache->getFakeCacheSettings(FLASH_DATA_TABLE);

  for (const auto& setting : settings)
  {
    std::vector<FlashDataItem> cacheData;
    boost::posix_time::ptime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    boost::posix_time::ptime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    boost::posix_time::time_period dataPeriod(starttime, endtime);

    auto begin1 = std::chrono::high_resolution_clock::now();
    readFlashCacheData(cacheData, dataPeriod, itsTimeZones);
    auto end1 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver read "
              << cacheData.size() << " FLASH observations between " << starttime << "..." << endtime
              << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end1 - begin1).count()
              << " ms" << std::endl;

    auto begin2 = std::chrono::high_resolution_clock::now();
    auto count = cache->fillFlashDataCache(cacheData);
    auto end2 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
              << " FLASH observations between " << starttime << "..." << endtime << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end2 - begin2).count()
              << " ms" << std::endl;
  }
}

void ObservationCacheAdminBase::updateFlashCache() const
{
  try
  {
    if (itsParameters.disableAllCacheUpdates)
      return;

    std::shared_ptr<ObservationCache> flashCache = getCache(FLASH_DATA_TABLE);

    if (flashCache->isFakeCache(FLASH_DATA_TABLE))
      return updateFlashFakeCache(flashCache);

    std::vector<FlashDataItem> flashCacheData;

    std::map<std::string, boost::posix_time::ptime> last_times = getLatestFlashTime(flashCache);

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readFlashCacheData(flashCacheData,
                         last_times.at("start_time"),
                         last_times.at("last_stroke_time"),
                         last_times.at("last_modified_time"),
                         itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << flashCacheData.size() << " FLASH observations starting from "
                  << last_times.at("start_time")
                  << " when stroke_time >= " << last_times.at("last_stroke_time")
                  << " and last_modified >= " << last_times.at("last_modified_time")
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = flashCache->fillFlashDataCache(flashCacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << " FLASH observations starting from " << last_times.at("start_time")
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
    if (itsShutdownRequested)
      return;

    // Delete too old flashes from the Cache database
    {
      auto begin = std::chrono::high_resolution_clock::now();
      flashCache->cleanFlashDataCache(
          boost::posix_time::hours(itsParameters.flashCacheDuration),
          boost::posix_time::hours(itsParameters.flashMemoryCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName()
                  << " database driver FLASH cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Unserialization failed!");
  }
}

void ObservationCacheAdminBase::updateObservationFakeCache(
    std::shared_ptr<ObservationCache>& cache) const
{
  std::vector<std::map<std::string, std::string>> settings =
      cache->getFakeCacheSettings(OBSERVATION_DATA_TABLE);

  for (const auto& setting : settings)
  {
    std::vector<DataItem> cacheData;
    boost::posix_time::ptime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    boost::posix_time::ptime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    boost::posix_time::time_period dataPeriod(starttime, endtime);

    auto begin1 = std::chrono::high_resolution_clock::now();
    readObservationCacheData(
        cacheData, dataPeriod, setting.at("fmisid"), setting.at("measurand_id"), itsTimeZones);
    auto end1 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver read "
              << cacheData.size() << " FIN observations between " << starttime << "..." << endtime
              << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end1 - begin1).count()
              << " ms" << std::endl;

    auto begin2 = std::chrono::high_resolution_clock::now();
    auto count = cache->fillDataCache(cacheData);
    auto end2 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
              << " FIN observations between " << starttime << "..." << endtime << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end2 - begin2).count()
              << " ms" << std::endl;
  }
}

void ObservationCacheAdminBase::updateObservationCache() const
{
  try
  {
    if (itsShutdownRequested || itsParameters.disableAllCacheUpdates)
      return;

    // The time of the last observation in the cache
    std::shared_ptr<ObservationCache> observationCache = getCache(OBSERVATION_DATA_TABLE);

    if (observationCache->isFakeCache(OBSERVATION_DATA_TABLE))
      return updateObservationFakeCache(observationCache);

    std::vector<DataItem> cacheData;

    std::pair<boost::posix_time::ptime, boost::posix_time::ptime> last_time_pair =
        getLatestObservationTime(observationCache);

    // Extra safety margin since the view contains 3 tables with different max(modified_last) values
    if (!last_time_pair.first.is_not_a_date_time())
      last_time_pair.first -= boost::posix_time::seconds(itsParameters.updateExtraInterval);

    // Making sure that we do not request more data than we actually store into
    // the cache.

    {
      auto begin = std::chrono::high_resolution_clock::now();
      readObservationCacheData(
          cacheData, last_time_pair.first, last_time_pair.second, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << " FIN observations starting from " << last_time_pair.first
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = observationCache->fillDataCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << " FIN observations starting from " << last_time_pair.first << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the Cache database
    auto begin = std::chrono::high_resolution_clock::now();
    observationCache->cleanDataCache(
        boost::posix_time::hours(itsParameters.finCacheDuration),
        boost::posix_time::hours(itsParameters.finMemoryCacheDuration));
    auto end = std::chrono::high_resolution_clock::now();

    if (itsTimer)
      std::cout << Spine::log_time_str() << driverName()
                << " database driver FIN cache cleaner finished in "
                << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                << " ms" << std::endl;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Unserialization failed!");
  }
}

void ObservationCacheAdminBase::updateWeatherDataQCFakeCache(
    std::shared_ptr<ObservationCache>& cache) const
{
  std::vector<std::map<std::string, std::string>> settings =
      cache->getFakeCacheSettings(WEATHER_DATA_QC_TABLE);

  for (const auto& setting : settings)
  {
    std::vector<WeatherDataQCItem> cacheData;
    boost::posix_time::ptime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    boost::posix_time::ptime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    boost::posix_time::time_period dataPeriod(starttime, endtime);

    auto begin1 = std::chrono::high_resolution_clock::now();
    readWeatherDataQCCacheData(
        cacheData, dataPeriod, setting.at("fmisid"), setting.at("measurand_id"), itsTimeZones);
    auto end1 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver read "
              << cacheData.size() << " EXT observations between " << starttime << "..." << endtime
              << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end1 - begin1).count()
              << " ms" << std::endl;

    auto begin2 = std::chrono::high_resolution_clock::now();
    auto count = cache->fillWeatherDataQCCache(cacheData);
    auto end2 = std::chrono::high_resolution_clock::now();
    std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
              << " EXT observations between " << starttime << "..." << endtime << " finished in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(end2 - begin2).count()
              << " ms" << std::endl;
  }
}

void ObservationCacheAdminBase::updateWeatherDataQCCache() const
{
  try
  {
    if (itsShutdownRequested || itsParameters.disableAllCacheUpdates)
      return;

    std::shared_ptr<ObservationCache> weatherDataQCCache = getCache(WEATHER_DATA_QC_TABLE);

    if (weatherDataQCCache->isFakeCache(WEATHER_DATA_QC_TABLE))
      return updateWeatherDataQCFakeCache(weatherDataQCCache);

    std::vector<WeatherDataQCItem> cacheData;

    std::pair<boost::posix_time::ptime, boost::posix_time::ptime> last_time_pair =
        getLatestWeatherDataQCTime(weatherDataQCCache);

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readWeatherDataQCCacheData(
          cacheData, last_time_pair.first, last_time_pair.second, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << " EXT observations starting from " << last_time_pair.first
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = weatherDataQCCache->fillWeatherDataQCCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << " EXT observations starting from " << last_time_pair.first << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the Cache database
    {
      auto begin = std::chrono::high_resolution_clock::now();
      weatherDataQCCache->cleanWeatherDataQCCache(
          boost::posix_time::hours(itsParameters.extCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName()
                  << " database driver EXT cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Unserialization failed!");
  }
}

void ObservationCacheAdminBase::updateNetAtmoCache() const
{
  try
  {
    if (itsShutdownRequested)
      return;

    std::shared_ptr<ObservationCache> netatmoCache = getCache(NETATMO_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    boost::posix_time::ptime last_time = netatmoCache->getLatestNetAtmoDataTime();
    boost::posix_time::ptime last_created_time = netatmoCache->getLatestNetAtmoCreatedTime();

    // Make sure the time is not in the future
    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    boost::posix_time::ptime min_last_time =
        boost::posix_time::second_clock::universal_time() -
        boost::posix_time::hours(itsParameters.netAtmoCacheDuration);

    static int update_count = 0;

    if (!last_time.is_not_a_date_time() &&
        last_time < min_last_time)  // do not read too old observations
    {
      last_time = min_last_time;
    }

    // Note: observations are always delayed. Do not make the latter update interval
    // too short! Experimentally 3 minutes was too short at FMI.

    // Big update every 10 updates to get delayed observations.
    bool long_update = (++update_count % 10 == 0);

    if (!last_time.is_not_a_date_time() && update_count > 0)
    {
      if (long_update)
        last_time -= boost::posix_time::hours(3);
      else
        last_time -= boost::posix_time::minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time = boost::posix_time::second_clock::universal_time() -
                  boost::posix_time::hours(itsParameters.netAtmoCacheDuration);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readMobileCacheData(NETATMO_PRODUCER, cacheData, last_time, last_created_time, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << NETATMO_PRODUCER << " observations starting from "
                  << last_time << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = netatmoCache->fillNetAtmoCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << NETATMO_PRODUCER << " observations starting from " << last_time
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      netatmoCache->cleanNetAtmoCache(
          boost::posix_time::hours(itsParameters.roadCloudCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver cleaner "
                  << NETATMO_PRODUCER << " cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                ("Updating " + std::string(NETATMO_PRODUCER) + " cache failed!"));
  }
}

void ObservationCacheAdminBase::updateRoadCloudCache() const
{
  try
  {
    if (itsShutdownRequested)
      return;

    std::shared_ptr<ObservationCache> roadcloudCache = getCache(ROADCLOUD_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    boost::posix_time::ptime last_time = roadcloudCache->getLatestRoadCloudDataTime();
    boost::posix_time::ptime last_created_time = roadcloudCache->getLatestRoadCloudCreatedTime();

    // Make sure the time is not in the future
    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    boost::posix_time::ptime min_last_time =
        boost::posix_time::second_clock::universal_time() -
        boost::posix_time::hours(itsParameters.roadCloudCacheDuration);

    static int update_count = 0;

    if (!last_time.is_not_a_date_time() &&
        last_time < min_last_time)  // do not read too old observations
    {
      last_time = min_last_time;
    }

    // Note: observations are always delayed. Do not make the latter update interval
    // too short! Experimentally 3 minutes was too short at FMI.

    // Big update every 10 updates to get delayed observations.
    bool long_update = (++update_count % 10 == 0);

    if (!last_time.is_not_a_date_time() && update_count > 0)
    {
      if (long_update)
        last_time -= boost::posix_time::hours(3);
      else
        last_time -= boost::posix_time::minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time = boost::posix_time::second_clock::universal_time() -
                  boost::posix_time::hours(itsParameters.roadCloudCacheDuration);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readMobileCacheData(
          ROADCLOUD_PRODUCER, cacheData, last_time, last_created_time, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << ROADCLOUD_PRODUCER << " observations starting from "
                  << last_time << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = roadcloudCache->fillRoadCloudCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << ROADCLOUD_PRODUCER << " observations starting from " << last_time
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      roadcloudCache->cleanRoadCloudCache(
          boost::posix_time::hours(itsParameters.roadCloudCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver "
                  << ROADCLOUD_PRODUCER << " cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                ("Updating " + std::string(ROADCLOUD_PRODUCER) + " cache failed!"));
  }
}

void ObservationCacheAdminBase::updateFmiIoTCache() const
{
  try
  {
    if (itsShutdownRequested)
      return;

    std::shared_ptr<ObservationCache> fmiIoTCache = getCache(FMI_IOT_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    boost::posix_time::ptime last_time = fmiIoTCache->getLatestFmiIoTDataTime();
    boost::posix_time::ptime last_created_time = fmiIoTCache->getLatestFmiIoTCreatedTime();

    // Make sure the time is not in the future
    boost::posix_time::ptime now = boost::posix_time::second_clock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    boost::posix_time::ptime min_last_time =
        boost::posix_time::second_clock::universal_time() -
        boost::posix_time::hours(itsParameters.fmiIoTCacheDuration);

    static int update_count = 0;

    if (!last_time.is_not_a_date_time() &&
        last_time < min_last_time)  // do not read too old observations
    {
      last_time = min_last_time;
    }

    // Note: observations are always delayed. Do not make the latter update interval
    // too short! Experimentally 3 minutes was too short at FMI.

    // Big update every 10 updates to get delayed observations.
    bool long_update = (++update_count % 10 == 0);

    if (!last_time.is_not_a_date_time() && update_count > 0)
    {
      if (long_update)
        last_time -= boost::posix_time::hours(3);
      else
        last_time -= boost::posix_time::minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time = boost::posix_time::second_clock::universal_time() -
                  boost::posix_time::hours(itsParameters.fmiIoTCacheDuration);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readMobileCacheData(FMI_IOT_PRODUCER, cacheData, last_time, last_created_time, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << FMI_IOT_PRODUCER << " observations starting from "
                  << last_time << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = fmiIoTCache->fillFmiIoTCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << FMI_IOT_PRODUCER << " observations starting from " << last_time
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (itsShutdownRequested)
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      fmiIoTCache->cleanFmiIoTCache(boost::posix_time::hours(itsParameters.fmiIoTCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver "
                  << FMI_IOT_PRODUCER << " cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                ("Updating " + std::string(FMI_IOT_PRODUCER) + " cache failed!"));
  }
}

void ObservationCacheAdminBase::updateObservationCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateObservationCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateObservationCacheLoop(): ") + err.what(),
                   itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateObservationCacheLoop(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      int wait_duration = itsParameters.finCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::updateFlashCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateFlashCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(driverName() + ": updateFlashCache(): ") + err.what(),
                   itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateFlashCache(): unknown error", itsParameters.quiet);
      }

      int wait_duration = itsParameters.flashCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::updateWeatherDataQCCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateWeatherDataQCCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateWeatherDataQCCache(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateWeatherDataQCCache(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      int wait_duration = itsParameters.extCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::updateNetAtmoCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateNetAtmoCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateNetAtmoCache(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateNetAtmoCache(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.netAtmoCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in updateNetAtmoCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::updateRoadCloudCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateRoadCloudCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateRoadCloudCache(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateRoadCloudCache(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.roadCloudCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in updateRoadCloudCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::updateFmiIoTCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateFmiIoTCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateFmiIoTCache(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateFmiIoTCache(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.fmiIoTCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in fmiIoTCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::updateStationsCacheLoop()
{
  try
  {
    while (!itsShutdownRequested)
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        loadStations();

        // Update only once if an interval has not been set
        if (itsParameters.stationsCacheUpdateInterval == 0)
          return;
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": loadStations(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": loadStations(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.stationsCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in updateRoadCloudCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::addInfoToStations(SmartMet::Spine::Stations& stations,
                                                  const std::string& language) const
{
  Locus::QueryOptions opts;
  opts.SetLanguage(language);
  opts.SetResultLimit(50000);
  opts.SetCountries("all");
  opts.SetFullCountrySearch(true);
  opts.SetFeatures("SYNOP,FINAVIA,STUK");
  opts.SetSearchVariants(true);

  SmartMet::Spine::LocationList locationList;

  // Stations from center of Finland with 2000 km radius
  locationList = itsGeonames->latlonSearch(opts, 64.96, 27.59, 2000);

  // Get synop_foreign stations
  SmartMet::Spine::LocationList locationList2 = itsGeonames->keywordSearch(opts, "synop_foreign");

  locationList.splice(locationList.end(), locationList2);

  std::set<int> processed_stations;

  std::map<int, SmartMet::Spine::LocationPtr> locations;

  for (const auto& loc : locationList)
    if (loc->fmisid)
      locations[*loc->fmisid] = loc;

  for (Spine::Station& station : stations)
  {
    if (locations.find(station.fmisid) != locations.end())
    {
      const SmartMet::Spine::LocationPtr& place = locations.at(station.fmisid);
      station.country = place->country;
      station.iso2 = place->iso2;
      station.geoid = place->geoid;
      station.requestedLat = place->latitude;
      station.requestedLon = place->longitude;
      station.requestedName = place->name;
      station.timezone = place->timezone;
      station.region = place->area;
      station.station_elevation = place->elevation;
      processed_stations.insert(station.fmisid);
    }
  }

  // Update info of the remainig stations
  for (Spine::Station& station : stations)
    if (processed_stations.find(station.fmisid) == processed_stations.end())
      addInfoToStation(station, language);
}

void ObservationCacheAdminBase::reloadStations()
{
  if (itsParameters.stationsCacheUpdateInterval > 0)
  {
    std::cout << Spine::log_time_str() << ANSI_FG_GREEN
              << " Stations update loop is running! Reload request ignored!" << ANSI_FG_DEFAULT
              << std::endl;
    return;
  }

  if (itsParameters.loadStations)
  {
    // FIXME: tun in background
    boost::thread stationsReloadThread([this]() { loadStations(); });
    stationsReloadThread.join();
  }
}

void ObservationCacheAdminBase::loadStations()
{
  try
  {
    if (itsStationsCurrentlyLoading)
    {
      std::cout << Spine::log_time_str() << ANSI_FG_GREEN
                << " Stations are being loaded currently! Reload request ignored!"
                << ANSI_FG_DEFAULT << std::endl;
      return;
    }

    itsStationsCurrentlyLoading = true;
    loadStations(itsParameters.params->serializedStationsFile);
    itsStationsCurrentlyLoading = false;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}  // namespace Delfoi

void ObservationCacheAdminBase::calculateStationDirection(SmartMet::Spine::Station& station) const
{
  try
  {
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude_out);
    double lat2 = deg2rad(station.latitude_out);

    double dlon = lon2 - lon1;

    double direction = rad2deg(
        atan2(sin(dlon) * cos(lat2), cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)));

    if (direction < 0)
      direction += 360.0;

    station.stationDirection = std::round(10.0 * direction) / 10.0;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void ObservationCacheAdminBase::addInfoToStation(SmartMet::Spine::Station& station,
                                                 const std::string& language) const
{
  try
  {
    const std::string lang = (language.empty() ? "fi" : language);

    Locus::QueryOptions opts;
    opts.SetLanguage("fmisid");
    opts.SetResultLimit(1);
    opts.SetCountries("");
    opts.SetFullCountrySearch(true);
    opts.SetFeatures("SYNOP");
    opts.SetSearchVariants(true);

    SmartMet::Spine::LocationList places;

    try
    {
      // Search by fmisid.
      std::string fmisid_s = Fmi::to_string(station.fmisid);
      SmartMet::Spine::LocationList suggest = itsGeonames->nameSearch(opts, fmisid_s);

      opts.SetLanguage(lang);

      if (not suggest.empty())
      {
        // When language is "fmisid" the name is the fmisid.
        if (suggest.front()->name == fmisid_s)
          places = itsGeonames->idSearch(opts, suggest.front()->geoid);
      }

      // Trying to find a location of station by assuming the geoid is
      // negative value of fmisid.
      if (places.empty())
      {
        places = itsGeonames->idSearch(opts, -station.fmisid);
      }

      // Next looking for a nearest station inside 50 meter radius.
      // There might be multiple stations at the same positon so the possibility
      // to get
      // a wrong geoid is big.
      if (places.empty())
      {
        places = itsGeonames->latlonSearch(opts,
                                           boost::numeric_cast<float>(station.latitude_out),
                                           boost::numeric_cast<float>(station.longitude_out),
                                           0.05);
      }

      // As a fallback we will try to find neasert populated place.
      // There is some places this will also fail e.g. South Pole (0.0, -90).
      if (places.empty())
      {
        opts.SetFeatures("PPL");
        places = itsGeonames->latlonSearch(opts,
                                           boost::numeric_cast<float>(station.latitude_out),
                                           boost::numeric_cast<float>(station.longitude_out));
      }
    }
    catch (...)
    {
      return;
    }

    for (const auto& place : places)
    {
      station.country = place->country;
      station.geoid = place->geoid;
      station.iso2 = place->iso2;
      station.requestedLat = place->latitude;
      station.requestedLon = place->longitude;
      station.requestedName = place->name;
      station.timezone = place->timezone;
      station.region = place->area;
      station.station_elevation = place->elevation;
    }

    calculateStationDirection(station);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::shared_ptr<ObservationCache> ObservationCacheAdminBase::getCache(
    const std::string& tablename) const
{
  return itsCacheProxy->getCacheByTableName(tablename);
}

std::string ObservationCacheAdminBase::driverName() const
{
  return (" [" + itsParameters.driverName + "]");
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
