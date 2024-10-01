#include "PostgreSQLDatabaseDriver.h"
#include "DatabaseDriverInfo.h"
#include <macgyver/TimeParser.h>
#include <spine/Reactor.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

PostgreSQLDatabaseDriver::PostgreSQLDatabaseDriver(const std::string &name,
                                                   const EngineParametersPtr &p,
                                                   Spine::ConfigBase & /* cfg */)
    : DatabaseDriverBase(name), itsParameters(name, p)
{
}

void PostgreSQLDatabaseDriver::initializeConnectionPool()
{
  try
  {
    itsPostgreSQLConnectionPool.reset(new PostgreSQLObsDBConnectionPool(this));

    for (uint i = 0; i < itsParameters.connectionOptions.size(); ++i)
    {
      itsPostgreSQLConnectionPool->addService(itsParameters.connectionOptions[i],
                                              itsParameters.connectionPoolSize[i]);
    }
    itsPostgreSQLConnectionPool->setGetConnectionTimeOutSeconds(
        itsParameters.connectionTimeoutSeconds);

    if (itsPostgreSQLConnectionPool->initializePool(itsParameters.params->stationtypeConfig,
                                                    itsParameters.params->parameterMap))
    {
      itsConnectionsOK = true;
      logMessage(
          "[PostgreSQLDatabaseDriver] PostgreSQL connection pool initialization successful for " +
              itsDriverName,
          itsParameters.quiet);
    }
    else
    {
      logMessage(
          "[PostgreSQLDatabaseDriver] PostgreSQL connection pool initialization unsuccessful for " +
              itsDriverName,
          itsParameters.quiet);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "PostgreSQL connection pool initialization failed for " + itsDriverName);
  }
}

void PostgreSQLDatabaseDriver::readConfig(Spine::ConfigBase &cfg)
{
  try
  {
    const DatabaseDriverInfoItem &driverInfo =
        itsParameters.params->databaseDriverInfo.getDatabaseDriverInfo(itsDriverName);

    Fmi::Database::PostgreSQLConnectionOptions connectionOptions;
    connectionOptions.host = driverInfo.params.at("host");
    connectionOptions.port = Fmi::stoi(driverInfo.params.at("port"));
    connectionOptions.database = driverInfo.params.at("database");
    connectionOptions.username = driverInfo.params.at("username");
    connectionOptions.password = driverInfo.params.at("password");
    connectionOptions.encoding = driverInfo.params.at("encoding");
    connectionOptions.connect_timeout = Fmi::stoi(driverInfo.params.at("connect_timeout"));

    auto pos = driverInfo.params.find("slow_query_limit");
    if (pos != driverInfo.params.end())
      connectionOptions.slow_query_limit = Fmi::stoi(pos->second);

    itsParameters.connectionOptions.push_back(connectionOptions);
    itsParameters.connectionPoolSize.push_back(Fmi::stoi(driverInfo.params.at("poolSize")));

    DatabaseDriverBase::readConfig(cfg, itsParameters);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading PostgreSQL configuration failed!");
  }
}

void PostgreSQLDatabaseDriver::shutdown()
{
  try
  {
    // Shutting down cache connections
    auto cache_admin = itsObservationCacheAdmin.load();
    if (cache_admin)
      cache_admin->shutdown();

    // Shutting down PostgreSQL connections
    if (itsPostgreSQLConnectionPool != nullptr)
      itsPostgreSQLConnectionPool->shutdown();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Shutting down PostgreSQLDatabaseDriver failed !");
  }
}

void PostgreSQLDatabaseDriver::init(Engine *obsengine)
{
  try
  {
    logMessage(
        "[PostgreSQLDatabaseDriver] Initializing connection pool for " + itsDriverName + "...",
        itsParameters.quiet);
    itsObsEngine = obsengine;
    initializeConnectionPool();

    // Caches
    std::shared_ptr<ObservationCacheAdminPostgreSQL> cacheAdmin(new ObservationCacheAdminPostgreSQL(
        itsParameters, itsPostgreSQLConnectionPool, getGeonames(), itsConnectionsOK, itsTimer));

    if (!Spine::Reactor::isShuttingDown())
    {
      itsObservationCacheAdmin.store(cacheAdmin);
      cacheAdmin->init();

      itsDatabaseStations.reset(
          new DatabaseStations(itsParameters.params, obsengine->getGeonames()));
    }

    logMessage("[PostgreSQLDatabaseDriver] Connection pool ready for " + itsDriverName,
               itsParameters.quiet);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Geonames::Engine *PostgreSQLDatabaseDriver::getGeonames() const
{
  return itsObsEngine->getGeonames();
}

void PostgreSQLDatabaseDriver::reloadStations()
{
  if (!Spine::Reactor::isShuttingDown() && responsibleForLoadingStations())
  {
    auto cache_admin = itsObservationCacheAdmin.load();
    if (cache_admin)
    {
      cache_admin->reloadStations();
    }
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
