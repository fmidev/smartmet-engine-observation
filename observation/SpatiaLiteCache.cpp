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
    throw Spine::Exception::Trace(BCP, "Operation failed!")
        .addParameter("filename", itsParameters.cacheFile);
  }
}

void SpatiaLiteCache::initializeCaches(int finCacheDuration,
                                       int extCacheDuration,
                                       int flashCacheDuration,
                                       int flashMemoryCacheDuration)
{
  try
  {
    logMessage("[Observation Engine] Initializing SpatiaLite memory cache", itsParameters.quiet);

    auto now = boost::posix_time::second_clock::universal_time();
    auto timetokeep_memory = boost::posix_time::hours(flashMemoryCacheDuration);
    auto flashdata =
        itsConnectionPool->getConnection()->readFlashCacheData(now - timetokeep_memory);
    itsFlashMemoryCache.fill(flashdata);
    logMessage("[Observation Engine] SpatiaLite memory cache ready.", itsParameters.quiet);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!")
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
      }
      else
      {
        ret = spatialitedb->getCachedData(
            stations, settings, itsParameters.parameterMap, timeSeriesOptions, itsTimeZones);
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

ts::TimeSeriesVectorPtr SpatiaLiteCache::flashValuesFromSpatiaLite(Settings &settings) const
{
  try
  {
    // Use memory cache if possible. t is not set if the cache is not ready yet
    auto t = itsFlashMemoryCache.getStartTime();

    if (!t.is_not_a_date_time() && settings.starttime >= t)
      return itsFlashMemoryCache.getData(settings, itsParameters.parameterMap, itsTimeZones);

    // Must use disk cache instead
    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    return spatialitedb->getCachedFlashData(settings, itsParameters.parameterMap, itsTimeZones);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
      if (not spatialitedb->getStationById(
              s, fmisid, settings.stationgroup_codes, settings.starttime, settings.endtime))
        continue;

      tmpIdStations.push_back(s);
    }

    // Find station data by using geoid
    for (int geoid : settings.geoids)
    {
      Spine::Station s;
      if (not spatialitedb->getStationByGeoid(
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLiteCache::timeIntervalIsCached(const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime) const
{
  try
  {
    Spine::ReadLock lock(itsTimeIntervalMutex);
    if (itsTimeIntervalStart.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsTimeIntervalStart);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLiteCache::flashIntervalIsCached(const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime) const
{
  try
  {
    // No need to check memory cache here, it is always supposed to be shorted than the disk cache

    Spine::ReadLock lock(itsFlashTimeIntervalMutex);
    if (itsFlashTimeIntervalStart.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsFlashTimeIntervalStart);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLiteCache::timeIntervalWeatherDataQCIsCached(
    const boost::posix_time::ptime &starttime, const boost::posix_time::ptime &endtime) const
{
  try
  {
    Spine::ReadLock lock(itsWeatherDataQCTimeIntervalMutex);
    if (itsWeatherDataQCTimeIntervalStart.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsWeatherDataQCTimeIntervalStart);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
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
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLiteCache::updateStationsAndGroups(const StationInfo &info) const
{
  logMessage("Updating stations to SpatiaLite databases...", itsParameters.quiet);
  boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
  spatialitedb->updateStationsAndGroups(info);

  // Clear all cached search results, read new info from sqlite
  itsStationIdCache.clear();
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

// Station request are cached into memory since asking the data from sqlite
// wastes a member from the connection pool. In the current sqlite version
// there can be max 64 spatialite connections, which is not enough during
// high peak times. Note that updating the Stations table causes the
// cache to be cleared (itsStationIdCache.clear()).

bool SpatiaLiteCache::getStationById(Spine::Station &station,
                                     int station_id,
                                     const std::set<std::string> &stationgroup_codes,
                                     const boost::posix_time::ptime &starttime,
                                     const boost::posix_time::ptime &endtime) const
{
  // Cache key
  auto key = boost::hash_value(station_id);
  boost::hash_combine(key, boost::hash_value(stationgroup_codes));

  // Return cached value if it exists
  auto cached = itsStationIdCache.find(key);
  if (cached)
  {
    station = *cached;
    return true;
  }

  // Search the database
  bool ok = itsConnectionPool->getConnection()->getStationById(
      station, station_id, stationgroup_codes, starttime, endtime);
  if (!ok)
    return false;

  // Cache the result for next searches
  itsStationIdCache.insert(key, station);
  return true;
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

std::size_t SpatiaLiteCache::fillFlashDataCache(
    const FlashDataItems &flashCacheData) const
{
  // Memory cache first
  itsFlashMemoryCache.fill(flashCacheData);

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

void SpatiaLiteCache::cleanFlashDataCache(
    const boost::posix_time::time_duration &timetokeep,
    const boost::posix_time::time_duration &timetokeep_memory) const
{
  auto now = boost::posix_time::second_clock::universal_time();

  // Clean memory cache first:

  itsFlashMemoryCache.clean(now - timetokeep_memory);

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

bool SpatiaLiteCache::roadCloudIntervalIsCached(const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &) const
{
  try
  {
    Spine::ReadLock lock(itsRoadCloudTimeIntervalMutex);
    if (itsRoadCloudTimeIntervalStart.is_not_a_date_time())
      return false;

    // We ignore end time intentionally
    return (starttime >= itsRoadCloudTimeIntervalStart);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestRoadCloudDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestRoadCloudDataTime();
}

std::size_t SpatiaLiteCache::fillRoadCloudCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
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

void SpatiaLiteCache::cleanRoadCloudCache(const boost::posix_time::time_duration &timetokeep) const
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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::roadCloudValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    ret = spatialitedb->getCachedRoadCloudData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLiteCache::netAtmoIntervalIsCached(const boost::posix_time::ptime &starttime,
                                              const boost::posix_time::ptime &) const
{
  try
  {
    Spine::ReadLock lock(itsNetAtmoTimeIntervalMutex);
    if (itsNetAtmoTimeIntervalStart.is_not_a_date_time())
      return false;
    // We ignore end time intentionally
    return (starttime >= itsNetAtmoTimeIntervalStart);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

std::size_t SpatiaLiteCache::fillNetAtmoCache(
    const MobileExternalDataItems &mobileExternalCacheData) const
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

void SpatiaLiteCache::cleanNetAtmoCache(const boost::posix_time::time_duration &timetokeep) const
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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLiteCache::netAtmoValuesFromSpatiaLite(
    Settings &settings) const
{
  try
  {
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    boost::shared_ptr<SpatiaLite> spatialitedb = itsConnectionPool->getConnection();
    ret = spatialitedb->getCachedNetAtmoData(settings, itsParameters.parameterMap, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

boost::posix_time::ptime SpatiaLiteCache::getLatestNetAtmoDataTime() const
{
  return itsConnectionPool->getConnection()->getLatestNetAtmoDataTime();
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

void SpatiaLiteCache::cleanDataCache(const boost::posix_time::time_duration &timetokeep) const
{
  boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
  t = round_down_to_cache_clean_interval(t);

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

boost::posix_time::ptime SpatiaLiteCache::getLatestWeatherDataQCTime() const
{
  return itsConnectionPool->getConnection()->getLatestWeatherDataQCTime();
}

std::size_t SpatiaLiteCache::fillWeatherDataQCCache(
    const WeatherDataQCItems &cacheData) const
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

void SpatiaLiteCache::cleanWeatherDataQCCache(
    const boost::posix_time::time_duration &timetokeep) const
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

void SpatiaLiteCache::fillLocationCache(const LocationItems &locations) const
{
  return itsConnectionPool->getConnection()->fillLocationCache(locations);
}

void SpatiaLiteCache::shutdown()
{
  if (itsConnectionPool)
    itsConnectionPool->shutdown();
  itsConnectionPool = nullptr;
}

SpatiaLiteCache::SpatiaLiteCache(const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : itsParameters(p), itsStationIdCache(p->stationIdCacheSize)
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
    throw Spine::Exception::Trace(BCP, "Observation-engine initialization failed");
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
    throw Spine::Exception::Trace(BCP, "SpatiaLiteCache::observablePropertyQuery failed");
  }

  return data;
}

void SpatiaLiteCache::readConfig(Spine::ConfigBase &cfg)
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
