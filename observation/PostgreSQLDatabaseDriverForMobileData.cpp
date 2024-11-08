#include "PostgreSQLDatabaseDriverForMobileData.h"
#include "ObservationCache.h"
#include "QueryExternalAndMobileData.h"
#include "QueryObservableProperty.h"
#include "QueryResult.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include <boost/make_shared.hpp>
#include <macgyver/DateTime.h>
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <atomic>
#include <chrono>
#include <clocale>
#include <numeric>

// #define MYDEBUG 1

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
void setSettings(Settings &settings, PostgreSQLObsDB &db)
{
  try
  {
    db.timeZone = settings.timezone;
    db.stationType = settings.stationtype;
    db.maxDistance = settings.maxdistance;
    db.allPlaces = settings.allplaces;
    db.wantedTime = settings.wantedtime;

    Fmi::DateTime startTime = Fmi::SecondClock::universal_time() - Fmi::Hours(24);
    Fmi::DateTime endTime = Fmi::SecondClock::universal_time();
    int timeStep = 1;
    if (!settings.starttime.is_not_a_date_time())
      startTime = settings.starttime;

    if (!settings.endtime.is_not_a_date_time())
      endTime = settings.endtime;

    if (settings.timestep >= 0)
      timeStep = settings.timestep;

    // PostgreSQL SQL is wrong to use timeStep>60 around DST changes
    if (timeStep > 60)
    {
      if (timeStep % 60 == 0)  // try to use 1h steps
        timeStep = 60;
      else
        timeStep = 1;  // strange timestep, use 1 minute to be sure
    }

    db.setTimeInterval(startTime, endTime, timeStep);

    if (!settings.timeformat.empty())
      db.resetTimeFormatter(settings.timeformat);
    else
      db.resetTimeFormatter(db.timeFormat);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace

PostgreSQLDatabaseDriverForMobileData::PostgreSQLDatabaseDriverForMobileData(
    const std::string &name, const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : PostgreSQLDatabaseDriver(name, p, cfg)
{
  if (setlocale(LC_NUMERIC, "en_US.utf8") == nullptr)
    throw Fmi::Exception(
        BCP, "PostgreSQL database driver for mobile data failed to set locale to en_US.utf8");

  readConfig(cfg);

  for (const auto &item : p->externalAndMobileProducerConfig)
    itsSupportedProducers.insert(item.first);
}

void PostgreSQLDatabaseDriverForMobileData::init(Engine *obsengine)
{
  try
  {
    PostgreSQLDatabaseDriver::init(obsengine);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLDatabaseDriverForMobileData::makeQuery(QueryBase *qb)
{
  try
  {
    if (Spine::Reactor::isShuttingDown())
      return;

    if (qb == nullptr)
    {
      std::ostringstream msg;
      msg << "PostgreSQLDatabaseDriverForMobileData::makeQuery : Implementation of '"
          << typeid(qb).name() << "' class is missing.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");

      exception.addDetail(msg.str());
      throw exception;
    }

    const std::string sqlStatement = qb->getSQLStatement();

    if (sqlStatement.empty())
    {
      std::ostringstream msg;
      msg << "PostgreSQLDatabaseDriverForMobileData::makeQuery : SQL statement of '"
          << typeid(*qb).name() << "' class is empty.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }

    std::shared_ptr<QueryResultBase> result = qb->getQueryResultContainer();

    // Try cache first
    std::optional<std::shared_ptr<QueryResultBase> > cacheResult =
        itsParameters.params->queryResultBaseCache.find(sqlStatement);
    if (cacheResult)
    {
      if (result->set(*cacheResult))
        return;
    }

    if (result == nullptr)
    {
      std::ostringstream msg;
      msg << "PostgreSQLDatabaseDriverForMobileData::makeQuery : Result container of '"
          << typeid(*qb).name() << "' class not found.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }

    std::shared_ptr<PostgreSQLObsDB> db;

    // Select an active connection in a very rude way.
    // If connection is not connected, reconnec it.
    // If a connection is not reconnected in here the ConnectionPool
    // will return the same faulty connection again and again.
    int poolSize = std::accumulate(
        itsParameters.connectionPoolSize.begin(), itsParameters.connectionPoolSize.end(), 0);
    for (int counter = 1; counter <= poolSize; ++counter)
    {
      db = itsPostgreSQLConnectionPool->getConnection(false);

      if (!db)
        break;

      if (db->isConnected())
        break;

      // ConnectionPool should do this
      db->reConnect();

      if (db->isConnected())
        break;

      if (counter == poolSize)
      {
        Fmi::Exception exception(BCP, "Missing database connection!");
        exception.addDetail("Can not get a database connection.");
        throw exception;
      }
    }

    try
    {
      db->get(sqlStatement, result, itsTimeZones);

      if (not cacheResult)
      {
        itsParameters.params->queryResultBaseCache.insert(sqlStatement, result);
      }
    }
    catch (...)
    {
      Fmi::Exception exception(BCP, "Database query failed!");
      throw exception;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForMobileData::values(Settings &settings)
{
  if (Spine::Reactor::isShuttingDown())
    return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);

  // Try first from cache and on failure (Fmi::Exception::) get from
  // database.
  try
  {
    // Get all data from Cache database if all requirements below apply:
    // 1) stationtype is cached
    // 2) we have the requested time interval in cache
    // 3) stations are available in Cache
    // However, if PostgreSQL connection pool is full, use Cache even if we have
    // no recent data in there
    if (settings.useDataCache)
    {
      auto cache = resolveCache(settings.stationtype, itsParameters.params);
      if (cache && cache->dataAvailableInCache(settings))
        return cache->valuesFromCache(settings);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }

  /*  FROM THIS POINT ONWARDS DATA IS REQUESTED FROM ORIGINAL DATABASE */

  try
  {
    // Database query prevented
    if (settings.preventDatabaseQuery)
      return std::make_shared<TS::TimeSeriesVector>();

    if (!itsConnectionsOK)
    {
      std::cerr << "[PostgreSQLDatabaseDriverForMobileData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      return std::make_shared<TS::TimeSeriesVector>();
    }

    std::shared_ptr<PostgreSQLObsDB> db =
        itsPostgreSQLConnectionPool->getConnection(settings.debug_options);
    setSettings(settings, *db);

    QueryExternalAndMobileData extdata(itsParameters.externalAndMobileProducerConfig,
                                       itsParameters.fmiIoTStations);

    return extdata.values(*db, settings, itsTimeZones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from database failed!!");
  }
}

/*
 * \brief Read values for given times only.
 */

TS::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForMobileData::values(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  if (Spine::Reactor::isShuttingDown())
    return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);

  // Try first from cache and on failure (Fmi::Exception::) get from
  // database.
  try
  {
    // Get all data from Cache database if all requirements below apply:
    // 1) stationtype is cached
    // 2) we have the requested time interval in cache
    // 3) stations are available in Cache
    // However, if PostgreSQL connection pool is full, use Cache even if we have
    // no recent data in there
    if (settings.useDataCache)
    {
      auto cache = resolveCache(settings.stationtype, itsParameters.params);
      if (cache && cache->dataAvailableInCache(settings))
        return cache->valuesFromCache(settings, timeSeriesOptions);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }

  /*  FROM THIS POINT ONWARDS DATA IS REQUESTED FROM ORIGINAL DATABASE */

  try
  {
    // Database query prevented
    if (settings.preventDatabaseQuery)
      return std::make_shared<TS::TimeSeriesVector>();

    if (!itsConnectionsOK)
    {
      std::cerr << "[PostgreSQLDatabaseDriverForMobileData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      return std::make_shared<TS::TimeSeriesVector>();
    }

    std::shared_ptr<PostgreSQLObsDB> db =
        itsPostgreSQLConnectionPool->getConnection(settings.debug_options);
    setSettings(settings, *db);

    QueryExternalAndMobileData extdata(itsParameters.externalAndMobileProducerConfig,
                                       itsParameters.fmiIoTStations);

    return extdata.values(*db, settings, timeSeriesOptions, itsTimeZones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from database failed!");
  }
}

void PostgreSQLDatabaseDriverForMobileData::getStations(Spine::Stations &stations,
                                                        const Settings &settings) const
{
}

void PostgreSQLDatabaseDriverForMobileData::getMovingStationsByArea(
    Spine::Stations & /*stations*/,
    const Settings & /*settings*/,
    const std::string & /*wkt*/) const
{
}

void PostgreSQLDatabaseDriverForMobileData::getStationsByArea(Spine::Stations & /*stations*/,
                                                              const Settings & /*settings*/,
                                                              const std::string & /*wkt*/) const
{
}
void PostgreSQLDatabaseDriverForMobileData::getStationsByBoundingBox(Spine::Stations &stations,
                                                                     const Settings &settings) const
{
}

std::shared_ptr<std::vector<ObservableProperty> >
PostgreSQLDatabaseDriverForMobileData::observablePropertyQuery(
    std::vector<std::string> & /* parameters */, const std::string & /* language */)
{
  try
  {
    std::shared_ptr<std::vector<ObservableProperty> > data;

    return data;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLDatabaseDriverForMobileData::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    const DatabaseDriverInfoItem &driverInfo =
        itsParameters.params->databaseDriverInfo.getDatabaseDriverInfo(itsDriverName);

    itsParameters.loadFmiIoTStations = (driverInfo.params.at("loadFmiIoTStations") != "0");
    itsParameters.netAtmoCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("netAtmoCacheUpdateInterval"));
    itsParameters.roadCloudCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("roadCloudCacheUpdateInterval"));
    itsParameters.fmiIoTCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("fmiIoTCacheUpdateInterval"));
    itsParameters.tapsiQcCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("tapsiQcCacheUpdateInterval"));

    if (!itsParameters.disableAllCacheUpdates)
    {
      itsParameters.netAtmoCacheDuration = Fmi::stoi(driverInfo.params.at("netAtmoCacheDuration"));
      itsParameters.roadCloudCacheDuration =
          Fmi::stoi(driverInfo.params.at("roadCloudCacheDuration"));
      itsParameters.fmiIoTCacheDuration = Fmi::stoi(driverInfo.params.at("fmiIoTCacheDuration"));
      itsParameters.tapsiQcCacheDuration = Fmi::stoi(driverInfo.params.at("tapsiQcCacheDuration"));
    }

    // Read part of config in base class
    PostgreSQLDatabaseDriver::readConfig(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading PostgreSQL configuration failed!");
  }
}

Fmi::DateTime PostgreSQLDatabaseDriverForMobileData::getLatestDataUpdateTime(
    const std::string & /*producer*/,
    const Fmi::DateTime & /*from*/,
    const MeasurandInfo & /*measurand_info*/) const
{
  return Fmi::DateTime::NOT_A_DATE_TIME;
}

std::string PostgreSQLDatabaseDriverForMobileData::id() const
{
  return "postgresql_mobile";
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
