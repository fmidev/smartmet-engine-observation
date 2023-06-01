#include "DatabaseDriverProxy.h"
#include "DummyDatabaseDriver.h"
#include "PostgreSQLDatabaseDriverForFmiData.h"
#include "PostgreSQLDatabaseDriverForMobileData.h"
#include "SpatiaLiteDatabaseDriver.h"
#include <macgyver/AnsiEscapeCodes.h>
#include <macgyver/Exception.h>
#include <spine/Convenience.h>

extern "C"
{
#include <dlfcn.h>
}

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using driver_create_t = DatabaseDriverBase *(const std::string &,
                                             const EngineParametersPtr &,
                                             Spine::ConfigBase &);

DatabaseDriverProxy::DatabaseDriverProxy(const EngineParametersPtr &p, Spine::ConfigBase &cfg)
    : itsStationtypeConfig(p->stationtypeConfig)
{
  try
  {
    //  std::cout << p->databaseDriverInfo << std::endl;

    PostgreSQLDatabaseDriverForFmiData *pPostgreSQLFmiDataDriver = nullptr;

    // Create all configured active database drivers
    // Each database table is mapped to a driver
    const std::vector<DatabaseDriverInfoItem> &ddi = p->databaseDriverInfo.getDatabaseDriverInfo();
    for (const auto &item : ddi)
    {
      if (!item.active)
        continue;
      const std::string &driver_id = item.name;
      DatabaseDriverBase *dbDriver = nullptr;

      if (boost::algorithm::starts_with(driver_id, "spatialite_"))
      {
        dbDriver = new SpatiaLiteDatabaseDriver(driver_id, p, cfg);
      }
      else if (boost::algorithm::starts_with(driver_id, "postgresql_"))
      {
        if (boost::algorithm::ends_with(driver_id, "mobile_observations"))
        {
          itsPostgreSQLMobileDataDriver =
              new PostgreSQLDatabaseDriverForMobileData(driver_id, p, cfg);
          dbDriver = itsPostgreSQLMobileDataDriver;
        }
        else if (boost::algorithm::ends_with(driver_id, "fmi_observations"))
        {
          pPostgreSQLFmiDataDriver = new PostgreSQLDatabaseDriverForFmiData(driver_id, p, cfg);
          dbDriver = pPostgreSQLFmiDataDriver;
        }
      }
      else if (boost::algorithm::starts_with(driver_id, "oracle_") &&
               boost::algorithm::ends_with(driver_id, "_observations"))
      {
        itsOracleDriver = createOracleDriver(driver_id, p, cfg);
        dbDriver = itsOracleDriver;
      }
      else if (boost::algorithm::starts_with(driver_id, "dummy"))
      {
        dbDriver = new DummyDatabaseDriver(driver_id, p);
      }

      if (dbDriver != nullptr)
      {
        itsDatabaseDriverSet.insert(dbDriver);

        for (const auto &period_item : item.table_days)
        {
          const std::string &tablename = period_item.first;
          int max_days = period_item.second;

          itsDatabaseDriverContainer.addDriver(tablename, max_days, dbDriver);
        }
      }
    }

    // If no active driver configured create dummy driver
    if (itsDatabaseDriverContainer.empty())
    {
      auto *dbDriver = new DummyDatabaseDriver("dummy", p);
      itsDatabaseDriverSet.insert(dbDriver);
      itsDatabaseDriverContainer.addDriver("*", INT_MAX, dbDriver);
      std::cout << Spine::log_time_str() << ANSI_FG_RED
                << " Note! No active database drivers configured -> creating a dummy driver!"
                << ANSI_FG_DEFAULT << std::endl;
    }

    init_tasks.on_task_error(
        [](const std::string &task_name)
        { throw Fmi::Exception::Trace(BCP, "Operation failed").addParameter("Task", task_name); });
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy constructor failed!");
  }
}  // namespace Delfoi

DatabaseDriverProxy::~DatabaseDriverProxy()
{
  for (auto *driver : itsDatabaseDriverSet)
    delete driver;
}

void DatabaseDriverProxy::init(Engine *obsengine)
{
  try
  {
    bool oracleDriverInitialized = false;
    if (itsOracleDriver && itsPostgreSQLMobileDataDriver)
    {
      // Let's initialize Oracle-driver first and fetch fmi_iot stations
      init_tasks.add("Initialize Oracle-driver and fetch fmi_iot stations",
                     [this, obsengine]()
                     {
                       itsOracleDriver->init(obsengine);
                       std::shared_ptr<FmiIoTStations> &stations =
                           itsPostgreSQLMobileDataDriver->getFmiIoTStations();
                       itsOracleDriver->getFMIIoTStations(stations);
                     });
      init_tasks.wait();
      oracleDriverInitialized = true;
    }

    for (const auto &dbdriver : itsDatabaseDriverSet)
    {
      // Do not init Oracle twice in case the previous if-block was executed
      if (!(oracleDriverInitialized && dbdriver == itsOracleDriver))
      {
        init_tasks.add("Init driver " + dbdriver->name(),
                       [&dbdriver, obsengine]() { dbdriver->init(obsengine); });
      }
    }

    init_tasks.wait();

    // Not done in parallel for thread safe assignments:
    for (const auto &dbdriver : itsDatabaseDriverSet)
    {
      if (!itsStationsDriver && dbdriver->responsibleForLoadingStations())
        itsStationsDriver = dbdriver;

      // Any driver can handle translateToFMISID
      if (!itsTranslateToFMISIDDriver)
        itsTranslateToFMISIDDriver = dbdriver;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::init function failed!");
  }
}

void DatabaseDriverProxy::getStationGroups(StationGroups &sg) const
{
  try
  {
    // Read station groups from DB
    if (itsStationsDriver)
      itsStationsDriver->getStationGroups(sg);
    else
      std::cout
          << Spine::log_time_str()
          << " [DatabaseDriverProxy] Getting station groups denied, a driver for loading stations "
             "is not set\n";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::getStationGroups function failed!");
  }
}

void DatabaseDriverProxy::getProducerGroups(ProducerGroups &pg) const
{
  try
  {
    // Read station groups from DB
    if (itsStationsDriver)
      itsStationsDriver->getProducerGroups(pg);
    else
      std::cout
          << Spine::log_time_str()
          << " [DatabaseDriverProxy] Getting producer groups denied, a driver for loading stations "
             "is not set\n";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::getProducerGroups function failed!");
  }
}

TS::TimeSeriesVectorPtr DatabaseDriverProxy::values(Settings &settings)
{
  try
  {
    DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);

    if (settings.debug_options & Settings::DUMP_SETTINGS)
      std::cout << "Database driver: " << pDriver->name() << std::endl;

    TS::TimeSeriesVectorPtr ret = pDriver->checkForEmptyQuery(settings);

    if (ret)
      return ret;

    return pDriver->values(settings);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::values function failed!");
  }
}

TS::TimeSeriesVectorPtr DatabaseDriverProxy::values(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);

    if (settings.debug_options & Settings::DUMP_SETTINGS)
      std::cout << "Database driver: " << pDriver->name() << std::endl;

    TS::TimeSeriesVectorPtr ret = pDriver->checkForEmptyQuery(settings, timeSeriesOptions);

    if (ret)
      return ret;

    return pDriver->values(settings, timeSeriesOptions);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::values function failed!");
  }
}

Spine::TaggedFMISIDList DatabaseDriverProxy::translateToFMISID(
    const Settings &settings, const StationSettings &stationSettings) const
{
  try
  {
    if (settings.stationtype == ICEBUOY_PRODUCER || settings.stationtype == COPERNICUS_PRODUCER)
    {
      Spine::TaggedFMISIDList ret;
      if (!stationSettings.fmisids.empty())
      {
        for (const auto &fmisid : stationSettings.fmisids)
          ret.emplace_back(Fmi::to_string(fmisid), fmisid);
      }
      else if (!stationSettings.bounding_box_settings.empty())
      {
        const auto &bbox = stationSettings.bounding_box_settings;
        std::string wktString =
            ("POLYGON((" + Fmi::to_string(bbox.at("minx")) + " " + Fmi::to_string(bbox.at("miny")) +
             "," + Fmi::to_string(bbox.at("minx")) + " " + Fmi::to_string(bbox.at("maxy")) + "," +
             Fmi::to_string(bbox.at("maxx")) + " " + Fmi::to_string(bbox.at("maxy")) + "," +
             Fmi::to_string(bbox.at("maxx")) + " " + Fmi::to_string(bbox.at("miny")) + "," +
             Fmi::to_string(bbox.at("minx")) + " " + Fmi::to_string(bbox.at("miny")) + "))");

        DatabaseDriverBase *pDriver = resolveDatabaseDriverByProducer(settings.stationtype);
        Spine::Stations stations;
        pDriver->getMovingStationsByArea(stations, settings, wktString);
        for (const auto &station : stations)
          ret.emplace_back(Fmi::to_string(station.fmisid), station.fmisid);
      }
      return ret;
    }

    if (itsTranslateToFMISIDDriver)
      return itsTranslateToFMISIDDriver->translateToFMISID(settings, stationSettings);

    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::translateToFMISID function failed!");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::translateToFMISID function failed!");
  }
}

void DatabaseDriverProxy::makeQuery(QueryBase *qb)
{
  // Currently only Oracle-driver is able to access these tables
  DatabaseDriverBase *pDriver = resolveDatabaseDriverByTable("radiosounding");
  return pDriver->makeQuery(qb);
}

FlashCounts DatabaseDriverProxy::getFlashCount(const boost::posix_time::ptime &starttime,
                                               const boost::posix_time::ptime &endtime,
                                               const Spine::TaggedLocationList &locations) const
{
  Settings settings;
  settings.starttime = starttime;
  settings.endtime = endtime;
  settings.stationtype = FLASH_PRODUCER;

  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getFlashCount(starttime, endtime, locations);
}

std::shared_ptr<std::vector<ObservableProperty>> DatabaseDriverProxy::observablePropertyQuery(
    std::vector<std::string> &parameters, const std::string language)
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriverByTable("measurand");
  return pDriver->observablePropertyQuery(parameters, language);
}

void DatabaseDriverProxy::getStations(Spine::Stations &stations, const Settings &settings) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getStations(stations, settings);
}

void DatabaseDriverProxy::reloadStations()
{
  if (!itsStationsDriver)
    std::cout << Spine::log_time_str()
              << " [DatabaseDriverProxy] Reload of stations denied, a driver for loading stations "
                 "is not set\n";
  else
  {
    std::cout << Spine::log_time_str() << " [DatabaseDriverProxy] Reload of stations requested\n";
    itsStationsDriver->reloadStations();
    std::cout << Spine::log_time_str()
              << "[DatabaseDriverProxy] Reload request of stations ended\n ";
  }
}

void DatabaseDriverProxy::getStationsByArea(Spine::Stations &stations,
                                            const Settings &settings,
                                            const std::string &wkt) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  if (settings.stationtype == ICEBUOY_PRODUCER || settings.stationtype == COPERNICUS_PRODUCER)
    pDriver->getMovingStationsByArea(stations, settings, wkt);
  else
    pDriver->getStationsByArea(stations, settings, wkt);
}

void DatabaseDriverProxy::getStationsByBoundingBox(Spine::Stations &stations,
                                                   const Settings &settings) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getStationsByBoundingBox(stations, settings);
}

void DatabaseDriverProxy::shutdown()
{
  init_tasks.stop();
  try
  {
    init_tasks.wait();
  }
  catch (...)
  {
    // We are not interested about possible exceptions when shutting down
  }

  for (const auto &dbdriver : itsDatabaseDriverSet)
    dbdriver->shutdown();
}

MetaData DatabaseDriverProxy::metaData(const std::string &producer) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriverByProducer(producer);
  return pDriver->metaData(producer);
}

std::string DatabaseDriverProxy::id() const
{
  std::string ret;

  for (const auto &dbdriver : itsDatabaseDriverSet)
  {
    if (!ret.empty())
      ret += ", ";
    ret += dbdriver->id();
  }

  return ret;
}

std::string DatabaseDriverProxy::name() const
{
  std::string ret;

  for (const auto &dbdriver : itsDatabaseDriverSet)
  {
    if (!ret.empty())
      ret += ", ";
    ret += dbdriver->name();
  }

  return ret;
}

DatabaseDriverBase *DatabaseDriverProxy::resolveDatabaseDriver(const Settings &settings) const
{
  try
  {
    std::string tablename =
        DatabaseDriverBase::resolveDatabaseTableName(settings.stationtype, itsStationtypeConfig);

    if (tablename.empty())
    {
      if (itsOracleDriver)
        return itsOracleDriver;

      throw Fmi::Exception::Trace(
          BCP, "No database driver found for producer '" + settings.stationtype + "'");
    }

    return itsDatabaseDriverContainer.resolveDriver(
        tablename, settings.starttime, settings.endtime);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

DatabaseDriverBase *DatabaseDriverProxy::resolveDatabaseDriverByProducer(
    const std::string &producer) const
{
  Settings settings;
  settings.starttime = boost::posix_time::not_a_date_time;
  settings.endtime = boost::posix_time::not_a_date_time;
  settings.stationtype = producer;

  return resolveDatabaseDriver(settings);
}

DatabaseDriverBase *DatabaseDriverProxy::resolveDatabaseDriverByTable(
    const std::string &tablename) const
{
  try
  {
    return itsDatabaseDriverContainer.resolveDriver(
        tablename, boost::posix_time::not_a_date_time, boost::posix_time::not_a_date_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

DatabaseDriverBase *DatabaseDriverProxy::createOracleDriver(const std::string &driver_id,
                                                            const EngineParametersPtr &p,
                                                            Spine::ConfigBase &cfg) const
{
  try
  {
    void *handle = dlopen(p->dbDriverFile.c_str(), RTLD_NOW);

    if (handle == nullptr)
    {
      // Error occurred while opening the dynamic library
      throw Fmi::Exception(BCP, "Unable to load database driver: " + std::string(dlerror()));
    }

    // Load the symbols (pointers to functions in dynamic library)

    auto *driver_create_func = reinterpret_cast<driver_create_t *>(dlsym(handle, "create"));

    // Check that pointer to create function is loaded succesfully
    if (driver_create_func == nullptr)
    {
      throw Fmi::Exception(BCP, "Cannot load symbols: " + std::string(dlerror()));
    }

    // Create an instance of the class using the pointer to "create" function

    DatabaseDriverBase *driver = driver_create_func(driver_id, p, cfg);

    if (driver == nullptr)
    {
      throw Fmi::Exception(BCP, "Unable to create a new instance of database driver class");
    }

    return driver;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to create Oracle database driver!");
  }
}

Fmi::Cache::CacheStatistics DatabaseDriverProxy::getCacheStats() const
{
  Fmi::Cache::CacheStatistics ret;

  for (const auto *driver : itsDatabaseDriverSet)
  {
    Fmi::Cache::CacheStatistics stats = driver->getCacheStats();
    for (const auto &stat : stats)
      ret.insert(std::make_pair(stat.first, stat.second));
  }

  return ret;
}

MeasurandInfo DatabaseDriverProxy::getMeasurandInfo() const
{
  if (itsStationsDriver)
    return itsStationsDriver->getMeasurandInfo();

  return MeasurandInfo();
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
