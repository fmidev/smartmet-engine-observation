#include "DatabaseDriverProxy.h"
#include "PostgreSQLDatabaseDriverForFmiData.h"
#include "PostgreSQLDatabaseDriverForMobileData.h"
#include "SpatiaLiteDatabaseDriver.h"
#include <macgyver/Exception.h>

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

typedef DatabaseDriverBase* driver_create_t(const std::string& driver_id, const EngineParametersPtr& p, Spine::ConfigBase& cfg);

DatabaseDriverProxy::DatabaseDriverProxy(const EngineParametersPtr &p,
                                         Spine::ConfigBase &cfg)
    : itsStationtypeConfig(p->stationtypeConfig)
{
  try
  {
  //  std::cout << p->databaseDriverInfo << std::endl;

    PostgreSQLDatabaseDriverForFmiData *pPostgreSQLFmiDataDriver = nullptr;

    // Create all configured active database drivers
    // Each database table is mapped to a driver
    const std::vector<DatabaseDriverInfoItem> &ddi =
        p->databaseDriverInfo.getDatabaseDriverInfo();
    for (const auto &item : ddi)
    {
      if (!item.active) continue;
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

	//    if (pPostgreSQLFmiDataDriver && itsOracleDriver)
	//      pPostgreSQLFmiDataDriver->setOracleDriver(itsOracleDriver);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy constructor failed!");
  }
}  // namespace Delfoi

DatabaseDriverProxy::~DatabaseDriverProxy()
{
  for (auto driver : itsDatabaseDriverSet)
    if (driver != nullptr) delete driver;
}

void DatabaseDriverProxy::init(Engine *obsengine)
{
  try
  {

	bool oracleDriverInitialized = false;
	if (itsOracleDriver && itsPostgreSQLMobileDataDriver)
    {
	  // Let's initialize Oracle-driver first and fetch fmi_iot stations
	  itsOracleDriver->init(obsengine);
      boost::shared_ptr<FmiIoTStations> &stations = itsPostgreSQLMobileDataDriver->getFmiIoTStations();
      itsOracleDriver->getFMIIoTStations(stations);
	  oracleDriverInitialized = true;
    }

    for (const auto &dbdriver : itsDatabaseDriverSet)
    {
	  if(oracleDriverInitialized && dbdriver == itsOracleDriver)
		  continue;
      dbdriver->init(obsengine);
      if (!itsStationsDriver && dbdriver->responsibleForLoadingStations())
        itsStationsDriver = dbdriver;
	  // Any driver can handle translateToFMISID
	  if(!itsTranslateToFMISIDDriver)
		itsTranslateToFMISIDDriver = dbdriver;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::init function failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr DatabaseDriverProxy::values(
    Settings &settings)
{
  try
  {
    DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
    return pDriver->values(settings);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::values function failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr DatabaseDriverProxy::values(
    Settings &settings,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions)
{
  try
  {
    DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
    return pDriver->values(settings, timeSeriesOptions);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "DatabaseDriverProxy::values function failed!");
  }
}

Spine::TaggedFMISIDList DatabaseDriverProxy::translateToFMISID(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &stationtype,
    const StationSettings &stationSettings) const
{
  try
  {
    if (itsTranslateToFMISIDDriver)
      return itsTranslateToFMISIDDriver->translateToFMISID(starttime, endtime, stationtype, stationSettings);
    else
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

FlashCounts DatabaseDriverProxy::getFlashCount(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const Spine::TaggedLocationList &locations) const
{
  Settings settings;
  settings.starttime = starttime;
  settings.endtime = endtime;
  settings.stationtype = "flash";

  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getFlashCount(starttime, endtime, locations);
}

boost::shared_ptr<std::vector<ObservableProperty>>
DatabaseDriverProxy::observablePropertyQuery(std::vector<std::string> &parameters,
                                             const std::string language)
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriverByTable("measurand");
  return pDriver->observablePropertyQuery(parameters, language);
}

void DatabaseDriverProxy::getStations(Spine::Stations &stations,
                                      const Settings &settings) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getStations(stations, settings);
}

void DatabaseDriverProxy::reloadStations() { 
  if(itsStationsDriver)
	itsStationsDriver->reloadStations(); 
}

void DatabaseDriverProxy::getStationsByArea(Spine::Stations &stations,
                                            const std::string &stationtype,
                                            const boost::posix_time::ptime &starttime,
                                            const boost::posix_time::ptime &endtime,
                                            const std::string &wkt) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriverByProducer(stationtype);
  return pDriver->getStationsByArea(stations, stationtype, starttime, endtime, wkt);
}

void DatabaseDriverProxy::getStationsByBoundingBox(
    Spine::Stations &stations, const Settings &settings) const
{
  DatabaseDriverBase *pDriver = resolveDatabaseDriver(settings);
  return pDriver->getStationsByBoundingBox(stations, settings);
}

void DatabaseDriverProxy::shutdown()
{
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
    if (!ret.empty()) ret += ", ";
    ret += dbdriver->id();
  }

  return ret;
}

std::string DatabaseDriverProxy::name() const
{
  std::string ret;

  for (const auto &dbdriver : itsDatabaseDriverSet)
  {
    if (!ret.empty()) ret += ", ";
    ret += dbdriver->name();
  }

  return ret;
}

DatabaseDriverBase *DatabaseDriverProxy::resolveDatabaseDriver(
    const Settings &settings) const
{
  try
  {
    std::string tablename =
        DatabaseDriverBase::resolveDatabaseTableName(settings.stationtype, itsStationtypeConfig);

    if (tablename.empty())
	  {
		if(itsOracleDriver)
		  return itsOracleDriver;

		throw Fmi::Exception::Trace(BCP, "No database driver found for prodcer '" + settings.stationtype +"'");
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

DatabaseDriverBase *DatabaseDriverProxy::createOracleDriver(const std::string &driver_id, const EngineParametersPtr &p, Spine::ConfigBase &cfg) const
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

    driver_create_t *driver_create_func = reinterpret_cast<driver_create_t *>(dlsym(handle, "create"));

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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

