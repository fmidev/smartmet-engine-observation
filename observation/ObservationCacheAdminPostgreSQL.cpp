#include "ObservationCacheAdminPostgreSQL.h"
#include "PostgreSQLObsDB.h"
#include "Utils.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCacheAdminPostgreSQL::ObservationCacheAdminPostgreSQL(
    const PostgreSQLDriverParameters& p,
    const std::unique_ptr<PostgreSQLObsDBConnectionPool>& pcp,
    Geonames::Engine* geonames,
    std::atomic<bool>& conn_ok,
    bool timer)
    : ObservationCacheAdminBase(p, geonames, conn_ok, timer), itsPostgreSQLConnectionPool(pcp)
{
  // Locations area loaded in the same driver
  if (itsParameters.loadStations)
  {
    logMessage("[PostgeSQLDatabaseDriver] Loading locations from PostgreSQL database...",
               itsParameters.quiet);

    boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
    // This is called by init() and is hence thread safe to modify stationInfo
    db->readStationLocations(itsParameters.params->stationInfo->stationLocations);

    logMessage("PostgreSQLDatabaseDriver] Locations read from PostgreSQL database.",
               itsParameters.quiet);
  }
}

void ObservationCacheAdminPostgreSQL::readObservationCacheData(
    std::vector<DataItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const std::string& fmisid,
    const std::string& measurandId,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readCacheDataFromPostgreSQL(cacheData, dataPeriod, fmisid, measurandId, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readFlashCacheData(
    std::vector<FlashDataItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readFlashCacheDataFromPostgreSQL(cacheData, dataPeriod, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readWeatherDataQCCacheData(
    std::vector<WeatherDataQCItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const std::string& fmisid,
    const std::string& measurandId,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readWeatherDataQCCacheDataFromPostgreSQL(
      cacheData, dataPeriod, fmisid, measurandId, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readObservationCacheData(
    std::vector<DataItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readCacheDataFromPostgreSQL(cacheData, startTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readFlashCacheData(
    std::vector<FlashDataItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastStrokeTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readFlashCacheDataFromPostgreSQL(
      cacheData, startTime, lastStrokeTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readWeatherDataQCCacheData(
    std::vector<WeatherDataQCItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& timezones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readWeatherDataQCCacheDataFromPostgreSQL(
      cacheData, startTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readMobileCacheData(
    const std::string& producer,
    std::vector<MobileExternalDataItem>& cacheData,
    boost::posix_time::ptime lastTime,
    boost::posix_time::ptime lastCreatedTime,
    const Fmi::TimeZones& timeZones) const
{
  boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
  db->readMobileCacheDataFromPostgreSQL(producer, cacheData, lastTime, lastCreatedTime, timeZones);
  if (producer == FMI_IOT_PRODUCER)
  {
    const auto& p = static_cast<const PostgreSQLDriverParameters&>(itsParameters);

    // Add station location info
    for (auto& item : cacheData)
    {
      if (!item.station_code)
        continue;

      if (p.fmiIoTStations->isActive(*item.station_code, item.data_time))
      {
        const FmiIoTStation& s = p.fmiIoTStations->getStation(*item.station_code, item.data_time);
        item.longitude = s.longitude;
        item.latitude = s.latitude;
        if (s.elevation >= 0.0)
          item.altitude = s.elevation;
      }
    }
  }
}

void ObservationCacheAdminPostgreSQL::loadStations(const std::string& serializedStationsFile)
{
  try
  {
    // We have no PostgreSQL connections, we cannot update
    if (!itsConnectionsOK)
    {
      std::cerr << "loadStations(): No connection to PostgreSQL." << std::endl;
      return;
    }

    if (itsShutdownRequested)
      return;

    boost::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection();
    const std::string place = "Helsinki";
    const std::string lang = "fi";
    Spine::LocationPtr loc = itsGeonames->nameSearch(place, lang);

    // Perhaps the point of above is to make sure the engine is available?
    // Moved the log message downwards accordingly.

    logMessage("[PostgreSQLDatabaseDriver] Loading stations...", itsParameters.quiet);

    auto newStationInfo = boost::make_shared<StationInfo>();

    // Get all the stations
    db->getStations(newStationInfo->stations);

    // Get wmo and lpnn and rwsid identifiers too
    db->translateToWMO(newStationInfo->stations);
    db->translateToLPNN(newStationInfo->stations);
    db->translateToRWSID(newStationInfo->stations);

    for (Spine::Station& station : newStationInfo->stations)
    {
      if (itsShutdownRequested)
        return;

      if (station.station_type == "AWS" or station.station_type == "SYNOP" or
          station.station_type == "CLIM" or station.station_type == "AVI")
      {
        station.isFMIStation = true;
      }
      else if (station.station_type == "MAREO")
      {
        station.isMareographStation = true;
      }
      else if (station.station_type == "BUOY")
      {
        station.isBuoyStation = true;
      }
      else if (station.station_type == "RWS" or station.station_type == "EXTRWS" or
               station.station_type == "EXTRWYWS")
      {
        station.isRoadStation = true;
      }
      else if (station.station_type == "EXTWATER")
      {
        station.isSYKEStation = true;
      }
      else if (station.station_type == "EXTSYNOP")
      {
        station.isForeignStation = true;
      }

      if (itsShutdownRequested)
        throw Fmi::Exception(
            BCP, "PostgreSQLDatabaseDriver: Aborting station preload due to shutdown request");
    }

    addInfoToStations(newStationInfo->stations, "");

    // Serialize stations to disk and swap the contents into itsParameters.params->stationInfo

    logMessage("[PostgreSQLDatabaseDriver] Serializing stations...", itsParameters.quiet);
    newStationInfo->serialize(serializedStationsFile);

    // We can safely copy old settings since this is the only thread updating the stationInfo
    // variable

    newStationInfo->stationLocations = itsParameters.params->stationInfo->stationLocations;

    boost::atomic_store(&itsParameters.params->stationInfo, newStationInfo);

    logMessage("[PostgreSQLDatabaseDriver] Loading stations done.", itsParameters.quiet);
  }
  catch (...)
  {
    Fmi::Exception exception(BCP, "Operation failed!", nullptr);
    std::cout << exception.getStackTrace();
  }
}

std::pair<boost::posix_time::ptime, boost::posix_time::ptime>
ObservationCacheAdminPostgreSQL::getLatestWeatherDataQCTime(
    const boost::shared_ptr<ObservationCache>& cache) const
{
  boost::posix_time::ptime last_time = boost::posix_time::second_clock::universal_time() -
                                       boost::posix_time::hours(itsParameters.extCacheDuration);

  boost::posix_time::ptime last_modified_time = cache->getLatestWeatherDataQCModifiedTime();

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = last_time;

  return std::pair<boost::posix_time::ptime, boost::posix_time::ptime>(last_time,
                                                                       last_modified_time);
}

std::pair<boost::posix_time::ptime, boost::posix_time::ptime>
ObservationCacheAdminPostgreSQL::getLatestObservationTime(
    const boost::shared_ptr<ObservationCache>& cache) const
{
  boost::posix_time::ptime last_time = boost::posix_time::second_clock::universal_time() -
                                       boost::posix_time::hours(itsParameters.finCacheDuration);

  boost::posix_time::ptime last_modified_time = cache->getLatestObservationModifiedTime();

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = last_time;

  return std::pair<boost::posix_time::ptime, boost::posix_time::ptime>(last_time,
                                                                       last_modified_time);
}

std::map<std::string, boost::posix_time::ptime> ObservationCacheAdminPostgreSQL::getLatestFlashTime(
    const boost::shared_ptr<ObservationCache>& cache) const
{
  std::map<std::string, boost::posix_time::ptime> ret;

  boost::posix_time::ptime start_time =
      (boost::posix_time::second_clock::universal_time() -
       boost::posix_time::hours(itsParameters.flashCacheDuration));
  boost::posix_time::ptime last_stroke_time = cache->getLatestFlashTime();
  boost::posix_time::ptime last_modified_time = cache->getLatestFlashModifiedTime();

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = start_time;
  if (last_stroke_time.is_not_a_date_time())
    last_stroke_time = start_time;

  ret["start_time"] = start_time;
  ret["last_stroke_time"] = last_stroke_time;
  ret["last_modified_time"] = last_modified_time;

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
