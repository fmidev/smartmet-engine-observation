#include "PostgreSQLCache.h"

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

void PostgreSQLCache::initializeConnectionPool()
{
  try
  {
    logMessage("[Observation Engine] Initializing PostgreSQL cache connection pool...",
               itsParameters.quiet);

    itsConnectionPool = new PostgreSQLConnectionPool(itsParameters);

    // Ensure that necessary tables exists:
    // 1) stations
    // 2) locations
    // 3) observation_data
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    db->createTables();

    // Check first if we already have stations in PostgreSQL db so that we know
    // if we can use it
    // before loading station info
    size_t stationCount = db->getStationCount();
    if (stationCount > 1)  // Arbitrary number because we cannot know how many
                           // stations there must be
    {
      itsParameters.cacheHasStations = true;
    }

    for (int i = 0; i < itsParameters.connectionPoolSize; i++)
    {
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
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
    if (settings.stationtype == "roadcloud")
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == "netatmo")
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromPostgreSQL(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    Spine::Stations stations = getStationsFromPostgreSQL(settings, db);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getCachedWeatherDataQCData(
            stations, settings, itsParameters.parameterMap, itsTimeZones);
        return ret;
      }

      ret = db->getCachedData(stations, settings, itsParameters.parameterMap, itsTimeZones);
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
    if (settings.stationtype == "roadcloud")
      return roadCloudValuesFromPostgreSQL(settings);

    if (settings.stationtype == "netatmo")
      return netAtmoValuesFromPostgreSQL(settings);

    if (settings.stationtype == "flash")
      return flashValuesFromPostgreSQL(settings);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    // Get stations
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();

    Spine::Stations stations = getStationsFromPostgreSQL(settings, db);
    stations = removeDuplicateStations(stations);

    // Get data if we have stations
    if (!stations.empty())
    {
      if ((settings.stationtype == "road" || settings.stationtype == "foreign") &&
          timeIntervalWeatherDataQCIsCached(settings.starttime, settings.endtime))
      {
        ret = db->getCachedWeatherDataQCData(
            stations, settings, itsParameters.parameterMap, timeSeriesOptions, itsTimeZones);
      }
      else
      {
        ret = db->getCachedData(
            stations, settings, itsParameters.parameterMap, timeSeriesOptions, itsTimeZones);
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
    ret = db->getCachedFlashData(settings, itsParameters.parameterMap, itsTimeZones);

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
    ret = db->getCachedRoadCloudData(settings, itsParameters.parameterMap, itsTimeZones);

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
    ret = db->getCachedNetAtmoData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting NetAtmo values from cache failed!");
  }
}

Spine::Stations PostgreSQLCache::getStationsFromPostgreSQL(Settings &settings,
                                                           boost::shared_ptr<PostgreSQL> db)
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
      Spine::Stations allStationsFromGroups = db->findAllStationsFromGroups(
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
      if (not db->getStationById(
              s, fmisid, settings.stationgroup_codes, settings.starttime, settings.endtime))
        continue;

      tmpIdStations.push_back(s);
    }

    // Find station data by using geoid
    for (int geoid : settings.geoids)
    {
      Spine::Station s;
      if (not db->getStationByGeoid(
              s, geoid, settings.stationgroup_codes, settings.starttime, settings.endtime))
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
      Spine::Stations tmpStations = db->findStationsByWMO(settings, *info);
      for (const Spine::Station &s : tmpStations)
        tmpIdStations.push_back(s);
    }

    if (!settings.lpnns.empty())
    {
      Spine::Stations tmpStations = db->findStationsByLPNN(settings, *info);
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
    throw Spine::Exception::Trace(BCP, "Getting stations from cache failed!");
  }
}

bool PostgreSQLCache::timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const
{
  try
  {
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    auto oldest_time = db->getOldestObservationTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
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
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    auto oldest_time = db->getOldestFlashTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
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
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    auto oldest_time = db->getOldestWeatherDataQCTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Checking if weather data QC is cached  failed!");
  }
}

Spine::Stations PostgreSQLCache::getStationsByTaggedLocations(
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
    throw Spine::Exception::Trace(BCP, "Getting stations by tagged locations failed!");
  }
}

void PostgreSQLCache::getStationsByBoundingBox(Spine::Stations &stations,
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
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
      auto stationList = db->findStationsInsideBox(tempSettings, *info);
#endif
      for (const auto &station : stationList)
        stations.push_back(station);
    }
    catch (...)
    {
      throw Spine::Exception::Trace(BCP, "Getting stations by bounding box failed!");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting stations by bounding box failed!");
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

    else if (settings.stationtype == "roadcloud")
      return roadCloudIntervalIsCached(settings.starttime, settings.endtime);

    else if (settings.stationtype == "netatmo")
      return netAtmoIntervalIsCached(settings.starttime, settings.endtime);

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

void PostgreSQLCache::updateStationsAndGroups(const StationInfo &info) const
{
  logMessage("Updating stations to PostgreSQL database...", itsParameters.quiet);
  itsConnectionPool->getConnection()->updateStationsAndGroups(info);
}

Spine::Stations PostgreSQLCache::findAllStationsFromGroups(
    const std::set<std::string> stationgroup_codes,
    const StationInfo &info,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime) const
{
  return itsConnectionPool->getConnection()->findAllStationsFromGroups(
      stationgroup_codes, info, starttime, endtime);
}

bool PostgreSQLCache::getStationById(Spine::Station &station,
                                     int station_id,
                                     const std::set<std::string> &stationgroup_codes,
                                     const boost::posix_time::ptime &starttime,
                                     const boost::posix_time::ptime &endtime) const
{
  return itsConnectionPool->getConnection()->getStationById(
      station, station_id, stationgroup_codes, starttime, endtime);
}

Spine::Stations PostgreSQLCache::findStationsInsideArea(const Settings &settings,
                                                        const std::string &areaWkt,
                                                        const StationInfo &info) const
{
  return itsConnectionPool->getConnection()->findStationsInsideArea(settings, areaWkt, info);
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
  return itsConnectionPool->getConnection()->fillFlashDataCache(flashCacheData);
}

void PostgreSQLCache::cleanFlashDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration & /* timetokeep_memory */) const
{
  return itsConnectionPool->getConnection()->cleanFlashDataCache(timetokeep);
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
  return itsConnectionPool->getConnection()->fillDataCache(cacheData);
}

void PostgreSQLCache::cleanDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  return itsConnectionPool->getConnection()->cleanDataCache(timetokeep);
}

boost::posix_time::ptime PostgreSQLCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

std::size_t PostgreSQLCache::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData) const
{
  return itsConnectionPool->getConnection()->fillWeatherDataQCCache(cacheData);
}

void PostgreSQLCache::cleanWeatherDataQCCache(
    const boost::posix_time::time_duration &timetokeep) const
{
  return itsConnectionPool->getConnection()->cleanWeatherDataQCCache(timetokeep);
}

bool PostgreSQLCache::roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &) const
{
  try
  {
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    auto oldest_time = db->getOldestRoadCloudDataTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
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
  return itsConnectionPool->getConnection()->fillRoadCloudCache(mobileExternalCacheData);
}

void PostgreSQLCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const
{
  return itsConnectionPool->getConnection()->cleanRoadCloudCache(timetokeep);
}

bool PostgreSQLCache::netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                              const boost::posix_time::ptime &) const
{
  try
  {
    boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();
    auto oldest_time = db->getOldestNetAtmoDataTime();

    if (oldest_time.is_not_a_date_time())
      return false;

    // we need only the beginning though
    return (starttime >= oldest_time);
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
  return itsConnectionPool->getConnection()->fillNetAtmoCache(mobileExternalCacheData);
}

void PostgreSQLCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const
{
  return itsConnectionPool->getConnection()->cleanNetAtmoCache(timetokeep);
}

void PostgreSQLCache::fillLocationCache(const LocationItems &locations) const
{
  return itsConnectionPool->getConnection()->fillLocationCache(locations);
}

void PostgreSQLCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

PostgreSQLCache::PostgreSQLCache(const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : itsParameters(p)
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

boost::shared_ptr<std::vector<ObservableProperty> > PostgreSQLCache::observablePropertyQuery(
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
    throw Spine::Exception::Trace(BCP, "PostgreSQLCache::observablePropertyQuery failed");
  }

  return data;
}

void PostgreSQLCache::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    itsParameters.postgresql.host = cfg.get_mandatory_config_param<std::string>("postgresql.host");
    itsParameters.postgresql.port = cfg.get_mandatory_config_param<unsigned int>("postgresql.port");
    itsParameters.postgresql.database =
        cfg.get_mandatory_config_param<std::string>("postgresql.database");
    itsParameters.postgresql.username =
        cfg.get_mandatory_config_param<std::string>("postgresql.username");
    itsParameters.postgresql.password =
        cfg.get_mandatory_config_param<std::string>("postgresql.password");
    itsParameters.postgresql.encoding =
        cfg.get_optional_config_param<std::string>("postgresql.encoding", "UTF8");
    itsParameters.postgresql.connect_timeout =
        cfg.get_optional_config_param<unsigned int>("postgresql.connect_timeout", 60);

    itsParameters.connectionPoolSize = cfg.get_mandatory_config_param<int>("cache.poolSize");

    itsParameters.maxInsertSize = cfg.get_optional_config_param<std::size_t>(
        "cache.maxInsertSize", 99999999);  // default = all at once

    itsParameters.dataInsertCacheSize =
        cfg.get_optional_config_param<std::size_t>("cache.dataInsertCacheSize", 100000);
    itsParameters.weatherDataQCInsertCacheSize =
        cfg.get_optional_config_param<std::size_t>("cache.weatherDataQCInsertCacheSize", 100000);
    itsParameters.flashInsertCacheSize =
        cfg.get_optional_config_param<std::size_t>("cache.flashInsertCacheSize", 10000);
    itsParameters.roadCloudInsertCacheSize =
        cfg.get_optional_config_param<std::size_t>("cache.roadCloudInsertCacheSize", 10000);
    itsParameters.netAtmoInsertCacheSize =
        cfg.get_optional_config_param<std::size_t>("cache.netAtmoInsertCacheSize", 10000);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Reading PostgreSQL settings from configuration file failed");
  }
}

bool PostgreSQLCache::cacheHasStations() const
{
  return itsParameters.cacheHasStations;
}

PostgreSQLCache::~PostgreSQLCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
