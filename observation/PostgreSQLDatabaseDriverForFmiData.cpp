#include "PostgreSQLDatabaseDriverForFmiData.h"
#include "QueryObservablePropertyPostgreSQL.h"
#include <boost/date_time/posix_time/posix_time.hpp>  //include all types plus i/o
#include <boost/date_time/time_duration.hpp>
#include <boost/make_shared.hpp>
#include "ObservationCache.h"
#include "QueryResult.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
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

PostgreSQLDatabaseDriverForFmiData::PostgreSQLDatabaseDriverForFmiData(
    const std::string &name,
    const EngineParametersPtr &p,
    Spine::ConfigBase &cfg)
    : PostgreSQLDatabaseDriver(name, p, cfg)
{
  setlocale(LC_NUMERIC, "en_US.utf8");

  readConfig(cfg);
}

void PostgreSQLDatabaseDriverForFmiData::setSettings(Settings &settings,
                                                     PostgreSQLObsDB &db)
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

void PostgreSQLDatabaseDriverForFmiData::init(Engine *obsengine)
{
  try
  {
    PostgreSQLDatabaseDriver::init(obsengine);
	/*
    itsDatabaseStations.reset(
        new DatabaseStations(itsParameters.params, itsObsEngine->getGeonames()));
	*/
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLDatabaseDriverForFmiData::makeQuery(QueryBase *qb)
{
  try
  {
    if (itsShutdownRequested) return;

    if (qb == nullptr)
    {
      std::ostringstream msg;
      msg << "PostgreSQLDatabaseDriverForFmiData::makeQuery : Implementation of '"
          << typeid(qb).name() << "' class is missing.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");

      exception.addDetail(msg.str());
      throw exception;
    }

    const std::string sqlStatement = qb->getSQLStatement("postgresql");

    if (sqlStatement.empty())
    {
      std::ostringstream msg;
      msg << "PostgreSQLDatabaseDriverForFmiData::makeQuery : SQL statement of '"
          << typeid(*qb).name() << "' class is empty.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }

    std::shared_ptr<QueryResultBase> result = qb->getQueryResultContainer();

    // Try cache first
    boost::optional<std::shared_ptr<QueryResultBase>> cacheResult =
        itsParameters.params->queryResultBaseCache.find(sqlStatement);
    if (cacheResult)
    {
      if (result->set(cacheResult.get())) return;
    }

    if (result == nullptr)
    {
      std::ostringstream msg;
      msg << "OPostgreSQLDatabaseDriverForFmiData::makeQuery : Result container of '"
          << typeid(*qb).name() << "' class not found.\n";

      Fmi::Exception exception(BCP, "Invalid parameter value!");
      exception.addDetail(msg.str());
      throw exception;
    }

    try
    {
      boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
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

ts::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForFmiData::values(
    Settings &settings)
{
  if (itsShutdownRequested) return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);
  updateProducers(itsParameters.params, settings);
  settings.useCommonQueryMethod =
      itsParameters.params->stationtypeConfig.getUseCommonQueryMethod(settings.stationtype);

  if (!settings.sqlDataFilter.exist("data_quality"))
    settings.sqlDataFilter.setDataFilter(
        "data_quality", itsParameters.params->dataQualityFilters.at(settings.stationtype));

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
      {
        return cache->valuesFromCache(settings);
      }
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
      std::cerr << "[PostgreSQLDatabaseDriverForFmiData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      return ret;
    }

    Spine::Stations stations;
    itsDatabaseStations->getStations(stations, settings);
    Spine::TimeSeriesGeneratorOptions timeSeriesOptions;
    timeSeriesOptions.startTime = settings.starttime;
    timeSeriesOptions.endTime = settings.endtime;
    timeSeriesOptions.timeStep = settings.timestep;
    timeSeriesOptions.startTimeUTC = false;
    timeSeriesOptions.endTimeUTC = false;

    boost::shared_ptr<PostgreSQLObsDB> db =
        itsPostgreSQLConnectionPool->getConnection(settings.debug_options);
    setSettings(settings, *db);

    std::string tablename = DatabaseDriverBase::resolveDatabaseTableName(
        settings.stationtype, itsParameters.params->stationtypeConfig);

    auto info = boost::atomic_load(&itsParameters.params->stationInfo);

    if (tablename == OBSERVATION_DATA_TABLE)
      return db->getObservationData(stations, settings, *info, timeSeriesOptions, itsTimeZones);
    else if (tablename == WEATHER_DATA_QC_TABLE)
      return db->getWeatherDataQCData(stations, settings, *info, timeSeriesOptions, itsTimeZones);
    else if (tablename == FLASH_DATA_TABLE)
      return db->getFlashData(settings, itsTimeZones);

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

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLDatabaseDriverForFmiData::values(
    Settings &settings,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  if (itsShutdownRequested) return nullptr;

  parameterSanityCheck(
      settings.stationtype, settings.parameters, *itsParameters.params->parameterMap);
  updateProducers(itsParameters.params, settings);
  settings.useCommonQueryMethod =
      itsParameters.params->stationtypeConfig.getUseCommonQueryMethod(settings.stationtype);

  if (!settings.sqlDataFilter.exist("data_quality"))
    settings.sqlDataFilter.setDataFilter(
        "data_quality", itsParameters.params->dataQualityFilters.at(settings.stationtype));

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
      {
        return cache->valuesFromCache(settings, timeSeriesOptions);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from cache failed!");
  }

  /*  FROM THIS POINT ONWARDS DATA IS REQUESTED FROM ORIGINAL DATABASE */

  try
  {
    if (!itsConnectionsOK)
    {
      std::cerr << "[PostgreSQLDatabaseDriverForFmiData] values(): No connections to PostgreSQL "
                   "database!"
                << std::endl;
      ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);
      return ret;
    }

    Spine::Stations stations;
    getStations(stations, settings);
    boost::shared_ptr<PostgreSQLObsDB> db =
        itsPostgreSQLConnectionPool->getConnection(settings.debug_options);
    setSettings(settings, *db);

    std::string tablename = DatabaseDriverBase::resolveDatabaseTableName(
        settings.stationtype, itsParameters.params->stationtypeConfig);

    auto info = boost::atomic_load(&itsParameters.params->stationInfo);

    if (tablename == OBSERVATION_DATA_TABLE)
      return db->getObservationData(stations, settings, *info, timeSeriesOptions, itsTimeZones);
    else if (tablename == WEATHER_DATA_QC_TABLE)
      return db->getWeatherDataQCData(stations, settings, *info, timeSeriesOptions, itsTimeZones);
    else if (tablename == FLASH_DATA_TABLE)
      return db->getFlashData(settings, itsTimeZones);

    ts::TimeSeriesVectorPtr ret(new ts::TimeSeriesVector);

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data from database failed!");
  }
}

void PostgreSQLDatabaseDriverForFmiData::getStations(
    Spine::Stations &stations, const Settings &settings) const
{
  itsDatabaseStations->getStations(stations, settings);
}
void PostgreSQLDatabaseDriverForFmiData::getStationsByArea(
    Spine::Stations &stations,
    const std::string &stationtype,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &wkt) const
{
  itsDatabaseStations->getStationsByArea(stations, stationtype, starttime, endtime, wkt);
}

void PostgreSQLDatabaseDriverForFmiData::getStationsByBoundingBox(
    Spine::Stations &stations, const Settings &settings) const
{
  itsDatabaseStations->getStationsByBoundingBox(stations, settings);
}

FlashCounts PostgreSQLDatabaseDriverForFmiData::getFlashCount(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const Spine::TaggedLocationList &locations) const
{
  try
  {
    Settings settings;
    settings.stationtype = "flash";

    auto cache = resolveCache(settings.stationtype, itsParameters.params);
    if (cache && cache->flashIntervalIsCached(starttime, endtime))
    {
      return cache->getFlashCount(starttime, endtime, locations);
    }
    else
    {
      boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();

      return db->getFlashCount(starttime, endtime, locations);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash count failed!");
  }
}

boost::shared_ptr<std::vector<ObservableProperty>>
PostgreSQLDatabaseDriverForFmiData::observablePropertyQuery(std::vector<std::string> &parameters,
                                                            const std::string language)
{
  try
  {
    QueryObservablePropertyPostgreSQL qop;

    boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();

    return qop.executeQuery(
        *db, "metadata", parameters, itsParameters.params->parameterMap, language);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLDatabaseDriverForFmiData::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    // Read config in base class
    PostgreSQLDatabaseDriver::readConfig(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading PostgreSQL configuration failed!");
  }
}

std::string PostgreSQLDatabaseDriverForFmiData::id() const { return "postgresql_fmi"; }

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
