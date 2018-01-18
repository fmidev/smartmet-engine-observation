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
/*!
 * \brief Find stations close to the given coordinate with filtering
 */

Spine::Stations findNearestStations(const StationInfo &info,
                                    double longitude,
                                    double latitude,
                                    double maxdistance,
                                    int numberofstations,
                                    const std::set<std::string> &stationgroup_codes,
                                    const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime)
{
  return info.findNearestStations(
      longitude, latitude, maxdistance, numberofstations, stationgroup_codes, starttime, endtime);
}

/*!
 * \brief Find stations close to the given location with filtering
 */

Spine::Stations findNearestStations(const StationInfo &info,
                                    const Spine::LocationPtr &location,
                                    double maxdistance,
                                    int numberofstations,
                                    const std::set<std::string> &stationgroup_codes,
                                    const boost::posix_time::ptime &starttime,
                                    const boost::posix_time::ptime &endtime)
{
  return findNearestStations(info,
                             location->longitude,
                             location->latitude,
                             maxdistance,
                             numberofstations,
                             stationgroup_codes,
                             starttime,
                             endtime);
}

}  // namespace

void SpatiaLiteCache::initializeConnectionPool(int finCacheDuration)
{
  try
  {
    logMessage("[Observation Engine] Initializing SpatiaLite cache connection pool...",
               itsParameters.quiet);

    itsConnectionPool = new SpatiaLiteConnectionPool(itsParameters.connectionPoolSize,
                                                     itsParameters.cacheFile,
                                                     itsParameters.maxInsertSize,
                                                     itsParameters.options);

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    spatialitedb->createTables();

    boost::posix_time::ptime last_time(spatialitedb->getLatestObservationTime());

    // Check first if we already have stations in SpatiaLite db so that we know
    // if we can use it
    // before loading station info
    size_t stationCount = spatialitedb->getStationCount();
    if (stationCount > 1)  // Arbitrary number because we cannot know how many
                           // stations there must be
    {
      itsParameters.cacheHasStations = true;
    }

    for (int i = 0; i < itsParameters.connectionPoolSize; i++)
    {
      boost::shared_ptr<SpatiaLite> db = itsConnectionPool->getConnection();
    }

    logMessage("[Observation Engine] SpatiaLite connection pool ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(Settings &settings)
{
  try
  {
    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    Spine::Stations stations = getStationsFromSpatiaLite(settings, spatialitedb);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, itsParameters.parameterMap, itsTimeZones);
        return ret;
      }

      ret =
          spatialitedb->getCachedData(stations, settings, itsParameters.parameterMap, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::valuesFromCache(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    if (settings.stationtype == "flash")
      return flashValuesFromSpatiaLite(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();

    Spine::Stations stations = getStationsFromSpatiaLite(settings, spatialitedb);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = spatialitedb->getCachedWeatherDataQCData(
            stations, settings, itsParameters.parameterMap, timeSeriesOptions, itsTimeZones);
        return ret;
      }

      ret = spatialitedb->getCachedData(
          stations, settings, itsParameters.parameterMap, timeSeriesOptions, itsTimeZones);
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::flashValuesFromSpatiaLite(Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    ret = spatialitedb->getCachedFlashData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Spine::Stations SpatiaLiteCache::getStationsFromSpatiaLite(
    Settings &settings, boost::shared_ptr<SpatiaLite> spatialitedb)
{
  try
  {
    auto stationstarttime = day_start(settings.starttime);
    auto stationendtime = day_end(settings.endtime);

    Spine::Stations stations;

    try
    {
      auto stationgroupCodeSet =
          itsParameters.stationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
      settings.stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());
    }
    catch (...)
    {
      return stations;
    }

    auto info = boost::atomic_load(&itsParameters.stationInfo);

    if (settings.allplaces)
    {
      Spine::Stations allStationsFromGroups = spatialitedb->findAllStationsFromGroups(
          settings.stationgroup_codes, *info, settings.starttime, settings.starttime);
      return allStationsFromGroups;
    }

    Spine::Stations tmpIdStations;

    auto taggedStations = getStationsByTaggedLocations(settings.taggedLocations,
                                                       settings.numberofstations,
                                                       settings.stationtype,
                                                       settings.maxdistance,
                                                       settings.stationgroup_codes,
                                                       settings.starttime,
                                                       settings.endtime);
    for (const auto &s : taggedStations)
      stations.push_back(s);

    for (const Spine::LocationPtr &location : settings.locations)
    {
      std::string locationCacheKey =
          getLocationCacheKey(location->geoid,
                              settings.numberofstations,
                              settings.stationtype,
                              boost::numeric_cast<int>(settings.maxdistance),
                              stationstarttime,
                              stationendtime);
      auto cachedStations = itsLocationCache.find(locationCacheKey);
      if (cachedStations)
      {
        for (const Spine::Station &cachedStation : *cachedStations)
          stations.push_back(cachedStation);
      }
      else
      {
        auto newStations = findNearestStations(*info,
                                               location,
                                               settings.maxdistance,
                                               settings.numberofstations,
                                               settings.stationgroup_codes,
                                               stationstarttime,
                                               stationendtime);

        if (!newStations.empty())
        {
          for (const Spine::Station &newStation : newStations)
            stations.push_back(newStation);

          itsLocationCache.insert(locationCacheKey, newStations);
        }
      }
    }

    // Find station data by using fmisid
    for (int fmisid : settings.fmisids)
    {
      Spine::Station s;
      if (not spatialitedb->getStationById(s, fmisid, settings.stationgroup_codes))
        continue;

      tmpIdStations.push_back(s);
    }

    for (const auto &coordinate : settings.coordinates)
    {
      auto newStations = findNearestStations(*info,
                                             coordinate.at("lon"),
                                             coordinate.at("lat"),
                                             settings.maxdistance,
                                             settings.numberofstations,
                                             settings.stationgroup_codes,
                                             stationstarttime,
                                             stationendtime);

      for (const Spine::Station &newStation : newStations)
        stations.push_back(newStation);
    }

    if (!settings.wmos.empty())
    {
      Spine::Stations tmpStations = spatialitedb->findStationsByWMO(settings, *info);
      for (const Spine::Station &s : tmpStations)
        tmpIdStations.push_back(s);
    }

    if (!settings.lpnns.empty())
    {
      Spine::Stations tmpStations = spatialitedb->findStationsByLPNN(settings, *info);
      for (const Spine::Station &s : tmpStations)
        tmpIdStations.push_back(s);
    }

    if (!settings.boundingBox.empty())
    {
      getStationsByBoundingBox(stations, settings);
    }

    for (const Spine::Station &s : tmpIdStations)
    {
      stations.push_back(s);
      if (settings.numberofstations > 1)
      {
        auto newStations = findNearestStations(*info,
                                               s.longitude_out,
                                               s.latitude_out,
                                               settings.maxdistance,
                                               settings.numberofstations,
                                               settings.stationgroup_codes,
                                               stationstarttime,
                                               stationendtime);

        for (const Spine::Station &nstation : newStations)
          stations.push_back(nstation);
      }
    }

    return stations;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLiteCache::timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const
{
  try
  {
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    auto oldest_time = spatialitedb->getOldestObservationTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLiteCache::flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime) const
{
  try
  {
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    auto oldest_time = spatialitedb->getOldestFlashTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLiteCache::timeIntervalWeatherDataQCIsCached(
    const boost::posix_time::ptime &starttime, const boost::posix_time::ptime &endtime) const
{
  try
  {
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    auto oldest_time = spatialitedb->getOldestWeatherDataQCTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Spine::Stations SpatiaLiteCache::getStationsByTaggedLocations(
    const Spine::TaggedLocationList &taggedLocations,
    const int numberofstations,
    const std::string &stationtype,
    const int maxdistance,
    const std::set<std::string> &stationgroup_codes,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime)
{
  try
  {
    Spine::Stations stations;

    auto stationstarttime = day_start(starttime);
    auto stationendtime = day_end(endtime);

    for (const Spine::TaggedLocation &tloc : taggedLocations)
    {
      // BUG? Why is maxdistance int?
      std::string locationCacheKey = getLocationCacheKey(tloc.loc->geoid,
                                                         numberofstations,
                                                         stationtype,
                                                         maxdistance,
                                                         stationstarttime,
                                                         stationendtime);
      auto cachedStations = itsLocationCache.find(locationCacheKey);

      if (cachedStations)
      {
        for (Spine::Station &cachedStation : *cachedStations)
        {
          cachedStation.tag = tloc.tag;
          stations.push_back(cachedStation);
        }
      }
      else
      {
        auto info = boost::atomic_load(&itsParameters.stationInfo);

        auto newStations = findNearestStations(*info,
                                               tloc.loc,
                                               maxdistance,
                                               numberofstations,
                                               stationgroup_codes,
                                               stationstarttime,
                                               stationendtime);

        if (!newStations.empty())
        {
          for (Spine::Station &s : newStations)
          {
            s.tag = tloc.tag;
            stations.push_back(s);
          }
          itsLocationCache.insert(locationCacheKey, newStations);
        }
      }
    }
    return stations;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLiteCache::getStationsByBoundingBox(Spine::Stations &stations,
                                               const Settings &settings) const
{
  try
  {
    Settings tempSettings = settings;
    try
    {
      auto stationgroupCodeSet =
          itsParameters.stationtypeConfig.getGroupCodeSetByStationtype(tempSettings.stationtype);
      tempSettings.stationgroup_codes.insert(stationgroupCodeSet->begin(),
                                             stationgroupCodeSet->end());
    }
    catch (...)
    {
      return;
    }

    auto info = boost::atomic_load(&itsParameters.stationInfo);

    try
    {
#if 1
      auto stationList = info->findStationsInsideBox(tempSettings.boundingBox.at("minx"),
                                                     tempSettings.boundingBox.at("miny"),
                                                     tempSettings.boundingBox.at("maxx"),
                                                     tempSettings.boundingBox.at("maxy"),
                                                     tempSettings.stationgroup_codes,
                                                     tempSettings.starttime,
                                                     tempSettings.endtime);
#else
      boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
      auto stationList = spatialitedb->findStationsInsideBox(tempSettings, *info);
#endif
      for (const auto &station : stationList)
        stations.push_back(station);
    }
    catch (...)
    {
      throw Spine::Exception::Trace(BCP, "Operation failed!");
    }
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLiteCache::dataAvailableInCache(const Settings &settings) const
{
  try
  {
    // If stationtype is cached and if we have requested time interval in
    // SpatiaLite, get all data
    // from there
    if (settings.stationtype == "opendata" || settings.stationtype == "fmi" ||
        settings.stationtype == "opendata_mareograph" || settings.stationtype == "opendata_buoy" ||
        settings.stationtype == "research" || settings.stationtype == "syke")
    {
      if (timeIntervalIsCached(settings.starttime, settings.endtime))
      {
        return true;
      }
    }
    else if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
             timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
    {
      return true;
    }
    else if (settings.stationtype == "flash" &&
             flashIntervalIsCached(settings.starttime, settings.endtime))
      return true;

    // Either the stationtype is not cached or the requested time interval is
    // not cached
    return false;
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLiteCache::updateStationsAndGroups(const StationInfo &info) const
{
  logMessage("Updating stations to SpatiaLite databases...", itsParameters.quiet);
  boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
  spatialitedb->updateStationsAndGroups(info);
}

Spine::Stations SpatiaLiteCache::findAllStationsFromGroups(
    const std::set<std::string> stationgroup_codes,
    const StationInfo &info,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime) const
{
  return itsConnectionPool->getConnection()->findAllStationsFromGroups(
      stationgroup_codes, info, starttime, endtime);
}

bool SpatiaLiteCache::getStationById(Spine::Station &station,
                                     int station_id,
                                     const std::set<std::string> &stationgroup_codes) const
{
  return itsConnectionPool->getConnection()->getStationById(
      station, station_id, stationgroup_codes);
}

Spine::Stations SpatiaLiteCache::findStationsInsideArea(const Settings &settings,
                                                        const std::string &areaWkt,
                                                        const StationInfo &info) const
{
  return itsConnectionPool->getConnection()->findStationsInsideArea(settings, areaWkt, info);
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

void SpatiaLiteCache::fillFlashDataCache(const std::vector<FlashDataItem> &flashCacheData) const
{
  return itsConnectionPool->getConnection()->fillFlashDataCache(flashCacheData);
}

void SpatiaLiteCache::cleanFlashDataCache(const boost::posix_time::ptime &timetokeep) const
{
  return itsConnectionPool->getConnection()->cleanFlashDataCache(timetokeep);
}

boost::posix_time::ptime SpatiaLiteCache::getLatestObservationTime() const
{
  return itsConnectionPool->getConnection()->getLatestObservationTime();
}

void SpatiaLiteCache::fillDataCache(const std::vector<DataItem> &cacheData) const
{
  return itsConnectionPool->getConnection()->fillDataCache(cacheData);
}

void SpatiaLiteCache::cleanDataCache(const boost::posix_time::ptime &last_time) const
{
  return itsConnectionPool->getConnection()->cleanDataCache(last_time);
}

boost::posix_time::ptime SpatiaLiteCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

void SpatiaLiteCache::fillWeatherDataQCCache(const std::vector<WeatherDataQCItem> &cacheData) const
{
  return itsConnectionPool->getConnection()->fillWeatherDataQCCache(cacheData);
}

void SpatiaLiteCache::cleanWeatherDataQCCache(const boost::posix_time::ptime &last_time) const
{
  return itsConnectionPool->getConnection()->cleanWeatherDataQCCache(last_time);
}

void SpatiaLiteCache::fillLocationCache(const std::vector<LocationItem> &locations) const
{
  return itsConnectionPool->getConnection()->fillLocationCache(locations);
}

void SpatiaLiteCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

SpatiaLiteCache::SpatiaLiteCache(boost::shared_ptr<EngineParameters> p, Spine::ConfigBase &cfg)
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

    if (itsParameters.options.threading_mode == "MULTITHREAD")
      err = sqlite3_config(SQLITE_CONFIG_MULTITHREAD);
    else if (itsParameters.options.threading_mode == "SERIALIZED")
      err = sqlite3_config(SQLITE_CONFIG_SERIALIZED);
    else
      throw Spine::Exception(
          BCP, "Unknown sqlite threading mode: " + itsParameters.options.threading_mode);

    if (err != 0)
      throw Spine::Exception(BCP,
                             "Failed to set sqlite3 multithread mode to " +
                                 itsParameters.options.threading_mode +
                                 ", exit code = " + Fmi::to_string(err));

    // Enable or disable memory statistics

    err = sqlite3_config(SQLITE_CONFIG_MEMSTATUS, itsParameters.options.memstatus);
    if (err != 0)
      throw Spine::Exception(
          BCP, "Failed to initialize sqlite3 memstatus mode, exit code " + Fmi::to_string(err));

    // Make one connection so that SOCI can initialize its once-types without
    // worrying about race conditions. This prevents a "database table is
    // locked"
    // warning during startup if the engine is simultaneously bombed with
    // requests.

    SpatiaLite connection(
        itsParameters.cacheFile, itsParameters.maxInsertSize, itsParameters.options);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Observation-engine initialization failed", NULL);
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
        parameters, language, itsParameters.parameterMap, stationType);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "SpatiaLiteCache::observablePropertyQuery failed", NULL);
  }

  return data;
}

void SpatiaLiteCache::readConfig(Spine::ConfigBase &cfg)
{
  itsParameters.connectionPoolSize = cfg.get_mandatory_config_param<int>("cache.poolSize");

  itsParameters.cacheFile = cfg.get_mandatory_path("spatialiteFile");

  itsParameters.maxInsertSize = cfg.get_optional_config_param<std::size_t>(
      "cache.max_insert_size", 9999999999);  // default = all at once

  itsParameters.options.cache_size = cfg.get_optional_config_param<std::size_t>(
      "sqlite.cache_size", 0);  // zero = use default value

  itsParameters.options.threading_mode =
      cfg.get_optional_config_param<std::string>("sqlite.threading_mode", "SERIALIZED");

  itsParameters.options.timeout = cfg.get_optional_config_param<size_t>("sqlite.timeout", 30000);

  itsParameters.options.shared_cache =
      cfg.get_optional_config_param<bool>("sqlite.shared_cache", false);

  itsParameters.options.memstatus = cfg.get_optional_config_param<bool>("sqlite.memstatus", false);

  itsParameters.options.synchronous =
      cfg.get_optional_config_param<std::string>("sqlite.synchronous", "NORMAL");

  itsParameters.options.journal_mode =
      cfg.get_optional_config_param<std::string>("sqlite.journal_mode", "WAL");

  itsParameters.options.auto_vacuum =
      cfg.get_optional_config_param<std::string>("sqlite.auto_vacuum", "NONE");

  itsParameters.options.mmap_size = cfg.get_optional_config_param<long>("sqlite.mmap_size", 0);
}

bool SpatiaLiteCache::cacheHasStations() const
{
  return itsParameters.cacheHasStations;
}

SpatiaLiteCache::~SpatiaLiteCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
