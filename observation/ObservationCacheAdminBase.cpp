#include "ObservationCacheAdminBase.h"
#include "Utils.h"
#include <macgyver/AnsiEscapeCodes.h>
#include <macgyver/Join.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

ObservationCacheAdminBase::ObservationCacheAdminBase(const DatabaseDriverParameters& parameters,
                                                     Engine::Geonames::Engine* geonames,
                                                     std::atomic<bool>& conn_ok,
                                                     bool timer)
    : itsParameters(parameters),
      itsCacheProxy(parameters.params->observationCacheProxy),
      itsGeonames(geonames),
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
  itsBackgroundTasks->stop();
  try
  {
    itsBackgroundTasks->wait();
  }
  catch (...)
  {
    // We are not interested about possible exceptions when shutting down
  }
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

    if (!cache_tables.empty())
    {
      for (const auto& item : cache_tables)
        logMessage("Table '" + item.second + "' is cached in '" + item.first + "'...",
                   itsParameters.quiet);
    }

    std::shared_ptr<ObservationCache> observationCache;
    std::shared_ptr<ObservationCache> weatherDataQCCache;
    std::shared_ptr<ObservationCache> flashCache;
    std::shared_ptr<ObservationCache> netatmoCache;
    std::shared_ptr<ObservationCache> roadcloudCache;
    std::shared_ptr<ObservationCache> fmiIoTCache;
    std::shared_ptr<ObservationCache> tapsiQcCache;
    std::shared_ptr<ObservationCache> magnetometerCache;
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
      else if (tablename == TAPSI_QC_DATA_TABLE)
      {
        tapsiQcCache = getCache(TAPSI_QC_DATA_TABLE);
        cache_set.insert(tapsiQcCache.get());
      }
      else if (tablename == MAGNETOMETER_DATA_TABLE)
      {
        magnetometerCache = getCache(MAGNETOMETER_DATA_TABLE);
        cache_set.insert(magnetometerCache.get());
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

        observationCache->cleanDataCache(Fmi::Hours(itsParameters.finCacheDuration),
                                         Fmi::Hours(itsParameters.finMemoryCacheDuration));

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
        weatherDataQCCache->cleanWeatherDataQCCache(Fmi::Hours(itsParameters.extCacheDuration));

        if (itsParameters.extCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init weather data QC cache",
                                  [this]() { updateWeatherDataQCCache(); });
        }
      }

      if (flashCache)
      {
        flashCache->cleanFlashDataCache(Fmi::Hours(itsParameters.flashCacheDuration),
                                        Fmi::Hours(itsParameters.flashMemoryCacheDuration));

        if (itsParameters.flashCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init flash cache", [this]() { updateFlashCache(); });
        }
      }
      if (netatmoCache)
      {
        netatmoCache->cleanNetAtmoCache(Fmi::Hours(itsParameters.netAtmoCacheDuration));

        if (itsParameters.netAtmoCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init Netatmo cache", [this]() { updateNetAtmoCache(); });
        }
      }

      if (roadcloudCache)
      {
        roadcloudCache->cleanRoadCloudCache(Fmi::Hours(itsParameters.roadCloudCacheDuration));

        if (itsParameters.roadCloudCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init roadcloud cache", [this]() { updateRoadCloudCache(); });
        }
      }

      if (fmiIoTCache)
      {
        fmiIoTCache->cleanFmiIoTCache(Fmi::Hours(itsParameters.fmiIoTCacheDuration));

        if (itsParameters.fmiIoTCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init fmi_iot cache", [this]() { updateFmiIoTCache(); });
        }
      }

      if (tapsiQcCache)
      {
        tapsiQcCache->cleanTapsiQcCache(Fmi::Hours(itsParameters.tapsiQcCacheDuration));

        if (itsParameters.tapsiQcCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init tapsi_qc cache", [this]() { updateTapsiQcCache(); });
        }
      }

      if (magnetometerCache)
      {
        magnetometerCache->cleanMagnetometerCache(
            Fmi::Hours(itsParameters.magnetometerCacheDuration));

        if (itsParameters.magnetometerCacheUpdateInterval > 0)
        {
          itsBackgroundTasks->add("Init magnetometer cache",
                                  [this]() { updateMagnetometerCache(); });
        }
      }
    }

    // If stations info does not exist (stations.txt file  missing), load info from database
    if (itsParameters.loadStations)
    {
      auto sinfo = itsParameters.params->stationInfo.load();
      if (sinfo->stations.empty())
      {
        std::cout << Spine::log_time_str() << driverName()
                  << " Stations info missing, loading from database! " << std::endl;
        itsBackgroundTasks->add("Load station data", [this]() { loadStations(); });
      }
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
    if (Spine::Reactor::isShuttingDown() || (tables.empty() && !itsParameters.loadStations))
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
                << Fmi::join(tables, ", ") << "! " << ANSI_FG_DEFAULT << std::endl;
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

    if (tables.find(TAPSI_QC_DATA_TABLE) != tables.end() &&
        itsParameters.tapsiQcCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("tapsi_qc cache update loop", [this]() { updateTapsiQcCacheLoop(); });
    }

    if (tables.find(MAGNETOMETER_DATA_TABLE) != tables.end() &&
        itsParameters.magnetometerCacheUpdateInterval > 0)
    {
      itsBackgroundTasks->add("magnetometer cache update loop",
                              [this]() { updateMagnetometerCacheLoop(); });
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
    Fmi::DateTime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    Fmi::DateTime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    Fmi::TimePeriod dataPeriod(starttime, endtime);

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

int random_integer(int min, int max)
{
  int range = max - min;
  return ((rand() % range) + min);
}

void ObservationCacheAdminBase::emulateFlashCacheUpdate(
    std::shared_ptr<ObservationCache>& cache) const
{
  auto function_starttime = std::chrono::high_resolution_clock::now();
  std::map<std::string, Fmi::DateTime> last_times = getLatestFlashTime(cache);

  auto starttime = last_times.at("last_stroke_time");
  auto endtime = Fmi::SecondClock::universal_time();
  // Start emulation from next second
  starttime += Fmi::Seconds(1);

  std::vector<FlashDataItem> cacheData;

  std::srand(std::time(nullptr));

  auto total_count = 0;
  auto time_iter = starttime;
  auto flash_id = cache->getMaxFlashId() + 1;
  std::cout << "Emulating flash cache database update, id start from: " << flash_id
            << ", time from: " << time_iter << std::endl;
  while (time_iter < endtime)
  {
    auto number_of_seconds = std::min(static_cast<int>((endtime - time_iter).total_seconds()), 60);
    auto number_of_flashes =
        itsParameters.flashEmulator.strokes_per_minute * (number_of_seconds / 60.0);

    for (unsigned int i = 0; i < number_of_flashes; i++)
    {
      FlashDataItem item;
      item.flash_id = flash_id++;
      item.longitude = (random_integer(itsParameters.flashEmulator.bbox.xMin * 1000,
                                       itsParameters.flashEmulator.bbox.xMax * 1000) /
                        1000.0);
      item.latitude = (random_integer(itsParameters.flashEmulator.bbox.yMin * 1000,
                                      itsParameters.flashEmulator.bbox.yMax * 1000) /
                       1000.0);
      item.stroke_time = time_iter + Fmi::Seconds(random_integer(0, number_of_seconds));
      item.stroke_time_fraction = random_integer(0, 1000);  // milliseconds
      item.created = endtime;
      item.modified_last = endtime;
      item.ellipse_angle = 1.0;
      item.ellipse_major = 1.0;
      item.ellipse_minor = 1.0;
      item.chi_square = 1.0;
      item.rise_time = 1.0;
      item.ptz_time = 1.0;
      item.multiplicity = 1;
      item.peak_current = 1;
      item.sensors = 1;
      item.freedom_degree = 1;
      item.cloud_indicator = 1;
      item.angle_indicator = 1;
      item.signal_indicator = 1;
      item.timing_indicator = 1;
      item.stroke_status = 1;
      item.data_source = -1;
      item.modified_by = 1;
      cacheData.push_back(item);

      // Write 10000 flashes at a time
      if ((cacheData.size() % 10000) == 0)
      {
        total_count += cache->fillFlashDataCache(cacheData);
        std::cout << "Added 10000 flashes to database, total number of flashes #" << total_count
                  << std::endl;
        cacheData.clear();
      }
      if (Spine::Reactor::isShuttingDown())
        return;
    }
    time_iter += Fmi::Seconds(number_of_seconds);
  }

  if (Spine::Reactor::isShuttingDown())
    return;

  if (!cacheData.empty())
  {
    total_count += cache->fillFlashDataCache(cacheData);
    std::cout << "Added " << cacheData.size() << " flashes to database, total number of flashes #"
              << total_count << std::endl;
  }

  auto function_endtime = std::chrono::high_resolution_clock::now();
  std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << total_count
            << " emulated flash observations between " << starttime << "..." << endtime
            << " finished in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(function_endtime -
                                                                     function_starttime)
                   .count()
            << " ms" << std::endl;
}

void ObservationCacheAdminBase::updateFlashCache() const
{
  try
  {
    if (itsParameters.disableAllCacheUpdates)
      return;

    std::shared_ptr<ObservationCache> flashCache = getCache(FLASH_DATA_TABLE);

    if (itsParameters.flashEmulator.active)
      return emulateFlashCacheUpdate(flashCache);

    if (flashCache->isFakeCache(FLASH_DATA_TABLE))
      return updateFlashFakeCache(flashCache);

    std::vector<FlashDataItem> flashCacheData;

    std::map<std::string, Fmi::DateTime> last_times = getLatestFlashTime(flashCache);

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

    if (Spine::Reactor::isShuttingDown())
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
    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old flashes from the Cache database
    {
      auto begin = std::chrono::high_resolution_clock::now();
      flashCache->cleanFlashDataCache(Fmi::Hours(itsParameters.flashCacheDuration),
                                      Fmi::Hours(itsParameters.flashMemoryCacheDuration));
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
    Fmi::DateTime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    Fmi::DateTime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    Fmi::TimePeriod dataPeriod(starttime, endtime);

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
    //	std::cout << "ObservationCacheAdminBase::updateObservationCache";
    if (Spine::Reactor::isShuttingDown() || itsParameters.disableAllCacheUpdates)
      return;

    // The time of the last observation in the cache
    std::shared_ptr<ObservationCache> observationCache = getCache(OBSERVATION_DATA_TABLE);

    if (observationCache->isFakeCache(OBSERVATION_DATA_TABLE))
      return updateObservationFakeCache(observationCache);

    std::vector<DataItem> cacheData;
    std::vector<MovingLocationItem> cacheDataMovingLocations;

    // pair of data_time, modified_last
    auto last_time_pair = getLatestObservationTime(observationCache);

    // Extra safety margin since the view contains 3 tables with different max(modified_last) values
    if (!last_time_pair.second.is_not_a_date_time())
      last_time_pair.second -= Fmi::Seconds(itsParameters.updateExtraInterval);

    // Making sure that we do not request more data than we actually store into
    // the cache.

    {
      auto begin = std::chrono::high_resolution_clock::now();

      // Read in bloks of finCacheUpdateSize to reduce database load

      const auto now = Utils::utc_second_clock();

      const auto length = itsParameters.finCacheUpdateSize;
      if (length == 0 || now - last_time_pair.second < Fmi::Hours(length))
      {
        // Small update, use a modified_last search
        readObservationCacheData(
            cacheData, last_time_pair.first, last_time_pair.second, itsTimeZones);
      }
      else
      {
        // Large update, use a data_time interval search

        std::string fmisid;       // all by default
        std::string measurandId;  // all by default

        auto t1 = last_time_pair.first;  // latest data_time in cache

        while (t1 < now)
        {
          auto t2 = t1 + Fmi::Hours(length);
          Fmi::TimePeriod period(t1, t2);
          std::cout << "Reading FIN period " << period << "\n";
          readObservationCacheData(cacheData, period, fmisid, measurandId, itsTimeZones);
          t1 = t2;
        }
      }

      readMovingStationsCacheData(
          cacheDataMovingLocations, last_time_pair.first, last_time_pair.second, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << " FIN observations starting from " << last_time_pair.first
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count_moving_locations =
          observationCache->fillMovingLocationsCache(cacheDataMovingLocations);
      auto count = observationCache->fillDataCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << " FIN observations and " << count_moving_locations
                  << " moving locations, starting from " << last_time_pair.first << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database
    auto begin = std::chrono::high_resolution_clock::now();
    observationCache->cleanDataCache(Fmi::Hours(itsParameters.finCacheDuration),
                                     Fmi::Hours(itsParameters.finMemoryCacheDuration));
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
    Fmi::DateTime starttime = Fmi::TimeParser::parse(setting.at("starttime"));
    Fmi::DateTime endtime = Fmi::TimeParser::parse(setting.at("endtime"));
    Fmi::TimePeriod dataPeriod(starttime, endtime);

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
    if (Spine::Reactor::isShuttingDown() || itsParameters.disableAllCacheUpdates)
      return;

    std::shared_ptr<ObservationCache> weatherDataQCCache = getCache(WEATHER_DATA_QC_TABLE);

    if (weatherDataQCCache->isFakeCache(WEATHER_DATA_QC_TABLE))
      return updateWeatherDataQCFakeCache(weatherDataQCCache);

    std::vector<WeatherDataQCItem> cacheData;

    std::pair<Fmi::DateTime, Fmi::DateTime> last_time_pair =
        getLatestWeatherDataQCTime(weatherDataQCCache);

    {
      auto begin = std::chrono::high_resolution_clock::now();

      // Read in blocks of extCacheUpdateSize to reduce database load

      const auto now = Utils::utc_second_clock();

      const auto length = itsParameters.extCacheUpdateSize;
      if (length == 0 || now - last_time_pair.second < Fmi::Hours(length))
      {
        // Small update, use a modified_last search
        readWeatherDataQCCacheData(
            cacheData, last_time_pair.first, last_time_pair.second, itsTimeZones);
      }
      else
      {
        // Large update, use a data_time interval search

        std::string fmisid;       // all by default
        std::string measurandId;  // all by default

        auto t1 = last_time_pair.first;  // latest data_time in cache

        while (t1 < now)
        {
          auto t2 = t1 + Fmi::Hours(length);
          Fmi::TimePeriod period(t1, t2);
          readWeatherDataQCCacheData(cacheData, period, fmisid, measurandId, itsTimeZones);
          t1 = t2;
        }
      }

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << " EXT observations starting from " << last_time_pair.first
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
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

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database
    {
      auto begin = std::chrono::high_resolution_clock::now();
      weatherDataQCCache->cleanWeatherDataQCCache(Fmi::Hours(itsParameters.extCacheDuration));
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
    if (Spine::Reactor::isShuttingDown())
      return;

    std::shared_ptr<ObservationCache> netatmoCache = getCache(NETATMO_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    Fmi::DateTime last_time = netatmoCache->getLatestNetAtmoDataTime();
    Fmi::DateTime last_created_time = netatmoCache->getLatestNetAtmoCreatedTime();

    // Make sure the time is not in the future
    Fmi::DateTime now = Fmi::SecondClock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    Fmi::DateTime min_last_time =
        Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.netAtmoCacheDuration);

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
        last_time -= Fmi::Hours(3);
      else
        last_time -= Fmi::Minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time =
          Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.netAtmoCacheDuration);
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

    if (Spine::Reactor::isShuttingDown())
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

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      netatmoCache->cleanNetAtmoCache(Fmi::Hours(itsParameters.netAtmoCacheDuration));
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
    if (Spine::Reactor::isShuttingDown())
      return;

    std::shared_ptr<ObservationCache> roadcloudCache = getCache(ROADCLOUD_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    Fmi::DateTime last_time = roadcloudCache->getLatestRoadCloudDataTime();
    Fmi::DateTime last_created_time = roadcloudCache->getLatestRoadCloudCreatedTime();

    // Make sure the time is not in the future
    Fmi::DateTime now = Fmi::SecondClock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    Fmi::DateTime min_last_time =
        Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.roadCloudCacheDuration);

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
        last_time -= Fmi::Hours(3);
      else
        last_time -= Fmi::Minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time =
          Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.roadCloudCacheDuration);
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

    if (Spine::Reactor::isShuttingDown())
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

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      roadcloudCache->cleanRoadCloudCache(Fmi::Hours(itsParameters.roadCloudCacheDuration));
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
    if (Spine::Reactor::isShuttingDown())
      return;

    std::shared_ptr<ObservationCache> fmiIoTCache = getCache(FMI_IOT_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    Fmi::DateTime last_time = fmiIoTCache->getLatestFmiIoTDataTime();
    Fmi::DateTime last_created_time = fmiIoTCache->getLatestFmiIoTCreatedTime();

    // Make sure the time is not in the future
    Fmi::DateTime now = Fmi::SecondClock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    Fmi::DateTime min_last_time =
        Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.fmiIoTCacheDuration);

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
        last_time -= Fmi::Hours(3);
      else
        last_time -= Fmi::Minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time =
          Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.fmiIoTCacheDuration);
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

    if (Spine::Reactor::isShuttingDown())
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

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      fmiIoTCache->cleanFmiIoTCache(Fmi::Hours(itsParameters.fmiIoTCacheDuration));
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

void ObservationCacheAdminBase::updateTapsiQcCache() const
{
  try
  {
    if (Spine::Reactor::isShuttingDown())
      return;

    std::shared_ptr<ObservationCache> tapsiQcCache = getCache(TAPSI_QC_DATA_TABLE);

    std::vector<MobileExternalDataItem> cacheData;

    Fmi::DateTime last_time = tapsiQcCache->getLatestTapsiQcDataTime();
    Fmi::DateTime last_created_time = tapsiQcCache->getLatestTapsiQcCreatedTime();

    // Make sure the time is not in the future
    Fmi::DateTime now = Fmi::SecondClock::universal_time();
    if (!last_time.is_not_a_date_time() && last_time > now)
      last_time = now;

    // Making sure that we do not request more data than we actually store into
    // the cache.
    Fmi::DateTime min_last_time =
        Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.tapsiQcCacheDuration);

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
        last_time -= Fmi::Hours(3);
      else
        last_time -= Fmi::Minutes(15);
    }

    if (last_time.is_not_a_date_time())
    {
      last_time =
          Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.tapsiQcCacheDuration);
    }

    {
      auto begin = std::chrono::high_resolution_clock::now();

      readMobileCacheData(TAPSI_QC_PRODUCER, cacheData, last_time, last_created_time, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << TAPSI_QC_PRODUCER << " observations starting from "
                  << last_time << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = tapsiQcCache->fillTapsiQcCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << TAPSI_QC_PRODUCER << " observations starting from " << last_time
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database

    {
      auto begin = std::chrono::high_resolution_clock::now();
      tapsiQcCache->cleanTapsiQcCache(Fmi::Hours(itsParameters.tapsiQcCacheDuration));
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver "
                  << TAPSI_QC_PRODUCER << " cache cleaner finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                ("Updating " + std::string(TAPSI_QC_PRODUCER) + " cache failed!"));
  }
}

void ObservationCacheAdminBase::updateMagnetometerCache() const
{
  try
  {
    if (Spine::Reactor::isShuttingDown() || itsParameters.disableAllCacheUpdates)
      return;

    // The time of the last observation in the cache
    std::shared_ptr<ObservationCache> magnetometerCache = getCache(MAGNETOMETER_DATA_TABLE);

    /*
        if (magnetometerCache->isFakeCache(MAGNETOMETER_DATA_TABLE))
          return updateMagnetometerFakeCache(magnetometerCache);
    */
    std::vector<MagnetometerDataItem> cacheData;

    // pair of data_time, modified_last
    auto min_last_time =
        Fmi::SecondClock::universal_time() - Fmi::Hours(itsParameters.magnetometerCacheDuration);

    auto last_time = magnetometerCache->getLatestMagnetometerDataTime();
    auto last_modified_time = magnetometerCache->getLatestMagnetometerModifiedTime();

    if (last_time.is_not_a_date_time())
      last_time = min_last_time;

    if (last_modified_time.is_not_a_date_time())
      last_modified_time = last_time;

    auto last_time_pair = std::pair<Fmi::DateTime, Fmi::DateTime>(last_time, last_modified_time);

    // Extra safety margin since the view contains 3 tables with different max(modified_last) values
    if (!last_time_pair.second.is_not_a_date_time())
      last_time_pair.second -= Fmi::Seconds(itsParameters.updateExtraInterval);

    // Making sure that we do not request more data than we actually store into
    // the cache.
    {
      auto begin = std::chrono::high_resolution_clock::now();

      readMagnetometerCacheData(
          cacheData, last_time_pair.first, last_time_pair.second, itsTimeZones);

      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver read "
                  << cacheData.size() << " Magnetometer observations starting from "
                  << last_time_pair.first << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    {
      auto begin = std::chrono::high_resolution_clock::now();
      auto count = magnetometerCache->fillMagnetometerCache(cacheData);
      auto end = std::chrono::high_resolution_clock::now();

      if (itsTimer)
        std::cout << Spine::log_time_str() << driverName() << " database driver wrote " << count
                  << " Magnetometer observations starting from " << last_time_pair.first
                  << " finished in "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                  << " ms" << std::endl;
    }

    if (Spine::Reactor::isShuttingDown())
      return;

    // Delete too old observations from the Cache database
    auto begin = std::chrono::high_resolution_clock::now();
    magnetometerCache->cleanMagnetometerCache(Fmi::Hours(itsParameters.magnetometerCacheDuration));
    auto end = std::chrono::high_resolution_clock::now();

    if (itsTimer)
      std::cout << Spine::log_time_str() << driverName()
                << " database driver Magnetometer cache cleaner finished in "
                << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
                << " ms" << std::endl;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "UPdating magnetometer cache failed failed!");
  }
}

void ObservationCacheAdminBase::updateObservationCacheLoop()
{
  try
  {
    while (!Spine::Reactor::isShuttingDown())
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
    while (!Spine::Reactor::isShuttingDown())
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
    while (!Spine::Reactor::isShuttingDown())
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
    while (!Spine::Reactor::isShuttingDown())
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
    while (!Spine::Reactor::isShuttingDown())
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
    while (!Spine::Reactor::isShuttingDown())
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

void ObservationCacheAdminBase::updateTapsiQcCacheLoop()
{
  try
  {
    while (!Spine::Reactor::isShuttingDown())
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateTapsiQcCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateTapsiQcCache(): ") + err.what(), itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateTapsiQcCache(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.tapsiQcCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in tapsiQcCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::updateMagnetometerCacheLoop()
{
  try
  {
    while (!Spine::Reactor::isShuttingDown())
    {
      Fmi::AsyncTask::interruption_point();
      try
      {
        updateMagnetometerCache();
      }
      catch (std::exception& err)
      {
        logMessage(std::string(": updateMagnetometerCacheLoop(): ") + err.what(),
                   itsParameters.quiet);
      }
      catch (...)
      {
        logMessage(": updateMagnetometerCacheLoop(): unknown error", itsParameters.quiet);
      }

      // Use absolute time to wait, not duration since there may be spurious wakeups.
      std::size_t wait_duration = itsParameters.magnetometerCacheUpdateInterval;
      boost::this_thread::sleep_until(boost::chrono::system_clock::now() +
                                      boost::chrono::seconds(wait_duration));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failure in updateMagnetometerCacheLoop-function!");
  }
}

void ObservationCacheAdminBase::updateStationsCacheLoop()
{
  try
  {
    while (!Spine::Reactor::isShuttingDown())
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

void ObservationCacheAdminBase::addInfoToStations(Spine::Stations& stations,
                                                  const std::string& language) const
{
  Locus::QueryOptions opts;
  opts.SetLanguage(language);
  opts.SetResultLimit(50000);
  opts.SetCountries("all");
  opts.SetFullCountrySearch(true);
  opts.SetFeatures("SYNOP,FINAVIA,STUK");
  opts.SetSearchVariants(true);

  Spine::LocationList locationList;

  // Stations from center of Finland with 2000 km radius
  locationList = itsGeonames->latlonSearch(opts, 64.96, 27.59, 2000);

  // Get synop_foreign stations
  Spine::LocationList locationList2 = itsGeonames->keywordSearch(opts, "synop_foreign");

  locationList.splice(locationList.end(), locationList2);

  std::set<int> processed_stations;

  std::map<int, Spine::LocationPtr> locations;

  for (const auto& loc : locationList)
    if (loc->fmisid)
      locations[*loc->fmisid] = loc;

  for (Spine::Station& station : stations)
  {
    if (Spine::Reactor::isShuttingDown())
      return;

    if (locations.find(station.fmisid) != locations.end())
    {
      const Spine::LocationPtr& place = locations.at(station.fmisid);
      station.country = place->country;
      station.iso2 = place->iso2;
      station.geoid = place->geoid;
      station.requestedLat = place->latitude;
      station.requestedLon = place->longitude;
      station.requestedName = place->name;
      station.timezone = place->timezone;
      station.region = place->area;
      station.elevation = place->elevation;
      processed_stations.insert(station.fmisid);
    }
  }

  // Update info of the remainig stations
  for (Spine::Station& station : stations)
  {
    if (Spine::Reactor::isShuttingDown())
      throw Fmi::Exception(BCP,
                           "[ObservationCacheAdminBase] Station updates aborted due to shutdown")
          .disableLogging();
    if (processed_stations.find(station.fmisid) == processed_stations.end())
      addInfoToStation(station, language);
  }
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

void ObservationCacheAdminBase::calculateStationDirection(Spine::Station& station) const
{
  try
  {
    double lon1 = deg2rad(station.requestedLon);
    double lat1 = deg2rad(station.requestedLat);
    double lon2 = deg2rad(station.longitude);
    double lat2 = deg2rad(station.latitude);

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

void ObservationCacheAdminBase::addInfoToStation(Spine::Station& station,
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

    Spine::LocationList places;

    try
    {
      // Search by fmisid.
      std::string fmisid_s = Fmi::to_string(station.fmisid);
      Spine::LocationList suggest = itsGeonames->nameSearch(opts, fmisid_s);

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
                                           boost::numeric_cast<float>(station.latitude),
                                           boost::numeric_cast<float>(station.longitude),
                                           0.05);
      }

      // As a fallback we will try to find neasert populated place.
      // There is some places this will also fail e.g. South Pole (0.0, -90).
      if (places.empty())
      {
        opts.SetFeatures("PPL");
        places = itsGeonames->latlonSearch(opts,
                                           boost::numeric_cast<float>(station.latitude),
                                           boost::numeric_cast<float>(station.longitude));
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
      station.elevation = place->elevation;
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
