#include "PostgreSQLDatabaseDriverForMobileData.h"
#include "ObservationCache.h"
#include "QueryExternalAndMobileData.h"
#include "QueryObservableProperty.h"
#include "QueryResult.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include <boost/date_time/posix_time/posix_time.hpp>  //include all types plus i/o
#include <boost/date_time/time_duration.hpp>
#include <boost/make_shared.hpp>
#include <spine/Convenience.h>
#include <atomic>
#include <chrono>
#include <clocale>
#include <numeric>

// #define MYDEBUG 1

namespace ts = SmartMet::Spine::TimeSeries;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
PostgreSQLDatabaseDriverForMobileData::PostgreSQLDatabaseDriverForMobileData(
    const std::string &name, const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : PostgreSQLDatabaseDriver(name, p, cfg)
{
  setlocale(LC_NUMERIC, "en_US.utf8");

  readConfig(cfg);

  for (const auto &item : p->externalAndMobileProducerConfig)
    itsSupportedProducers.insert(item.first);
}

void PostgreSQLDatabaseDriverForMobileData::setSettings(Settings &settings, PostgreSQLObsDB &db)
{
  try
  {
    db.timeZone = settings.timezone;
    db.stationType = settings.stationtype;
    db.maxDistance = settings.maxdistance;
    db.allPlaces = settings.allplaces;
    db.latest = settings.latest;

    boost::posix_time::ptime startTime =
        boost::posix_time::second_clock::universal_time() - boost::posix_time::hours(24);
    boost::posix_time::ptime endTime = boost::posix_time::second_clock::universal_time();
    int timeStep = 1;
    if (!settings.starttime.is_not_a_date_time())
    {
      startTime = settings.starttime;
    }
    if (!settings.endtime.is_not_a_date_time())
    {
      endTime = settings.endtime;
    }
    if (settings.timestep >= 0)
    {
      timeStep = settings.timestep;
    }

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
    {
      db.resetTimeFormatter(settings.timeformat);
    }
    else
    {
      db.resetTimeFormatter(db.timeFormat);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
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
    if (itsShutdownRequested)
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

    boost::shared_ptr<QueryResultBase> result = qb->getQueryResultContainer();

    // Try cache first
    boost::optional<boost::shared_ptr<QueryResultBase> > cacheResult =
        itsParameters.params->queryResultBaseCache.find(sqlStatement);
    if (cacheResult)
    {
      if (result->set(cacheResult.get()))
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

    boost::shared_ptr<PostgreSQLObsDB> db;

    // Select an active connection in a very rude way.
    // If connection is not connected, reconnec it.
    // If a connection is not reconnected in here the ConnectionPool
    // will return the same faulty connection again and again.
    int poolSize = std::accumulate(
        itsParameters.connectionPoolSize.begin(), itsParameters.connectionPoolSize.end(), 0);
    for (int counter = 1; counter <= poolSize; ++counter)
    {
      db = itsPostgreSQLConnectionPool->getConnection();

      if (db and db->isConnected())
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
        itsParameters.params->queryResultBaseCache.insert(sqlStatement, std::move(result));
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

ts::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForMobileData::values(Settings &settings)
{
  if (itsShutdownRequested)
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
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    if (!itsConnectionsOK)
    {
      std::cerr << "[PostgreSQLDatabaseDriverForMobileData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      return ret;
    }

    boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
    setSettings(settings, *db);

    QueryExternalAndMobileData extdata(itsParameters.externalAndMobileProducerConfig,
                                       itsParameters.fmiIoTStations);

    ret = extdata.values(*db, settings, itsTimeZones);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from database failed!!");
  }
}

/*
 * \brief Read values for given times only.
 */

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForMobileData::values(
    Settings &settings, const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  if (itsShutdownRequested)
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
    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    if (!itsConnectionsOK)
    {
      std::cerr << "[PostgreSQLDatabaseDriverForMobileData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      return ret;
    }

    boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
    setSettings(settings, *db);

    QueryExternalAndMobileData extdata(itsParameters.externalAndMobileProducerConfig,
                                       itsParameters.fmiIoTStations);

    ret = extdata.values(*db, settings, timeSeriesOptions, itsTimeZones);

    return ret;
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
void PostgreSQLDatabaseDriverForMobileData::getStationsByArea(
    Spine::Stations &stations,
    const std::string &stationtype,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &wkt) const
{
}
void PostgreSQLDatabaseDriverForMobileData::getStationsByBoundingBox(Spine::Stations &stations,
                                                                     const Settings &settings) const
{
}

boost::shared_ptr<std::vector<ObservableProperty> >
PostgreSQLDatabaseDriverForMobileData::observablePropertyQuery(std::vector<std::string> &parameters,
                                                               const std::string language)
{
  try
  {
    boost::shared_ptr<std::vector<ObservableProperty> > data;

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

    itsParameters.netAtmoCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("netAtmoCacheUpdateInterval"));
    itsParameters.roadCloudCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("roadCloudCacheUpdateInterval"));
    itsParameters.fmiIoTCacheUpdateInterval =
        Fmi::stoi(driverInfo.params.at("fmiIoTCacheUpdateInterval"));

    if (!itsParameters.disableAllCacheUpdates)
    {
      itsParameters.netAtmoCacheDuration = Fmi::stoi(driverInfo.params.at("netAtmoCacheDuration"));
      itsParameters.roadCloudCacheDuration =
          Fmi::stoi(driverInfo.params.at("roadCloudCacheDuration"));
      itsParameters.fmiIoTCacheDuration = Fmi::stoi(driverInfo.params.at("fmiIoTCacheDuration"));
    }

    // Read part of config in base class
    PostgreSQLDatabaseDriver::readConfig(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading PostgreSQL configuration failed!");
  }
}

std::string PostgreSQLDatabaseDriverForMobileData::id() const
{
  return "postgresql_mobile";
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
