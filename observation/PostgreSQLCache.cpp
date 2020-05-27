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

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();

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

    auto sinfo = boost::atomic_load(itsParameters.stationInfo);

    SmartMet::Spine::Stations stations = sinfo->findFmisidStations(settings.taggedFMISIDs);

    // Get data if we have stations
    if (!stations.empty())
    {
      boost::shared_ptr<PostgreSQL> db = itsConnectionPool->getConnection();

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

PostgreSQLCache::~PostgreSQLCache()
{
  shutdown();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
