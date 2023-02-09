#include "ObservationCacheAdminPostgreSQL.h"
#include "PostgreSQLObsDB.h"
#include "Utils.h"
#include <spine/Reactor.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

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
    logMessage("[PostgeSQLDatabaseDriver] Loading locations from PostgreSQL database " +
                   itsParameters.driverName + "...",
               itsParameters.quiet);

    std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
    // This is called by init() and is hence thread safe to modify stationInfo stationLocations
    // directly
    db->readStationLocations(itsParameters.params->stationInfo.load()->stationLocations);

    logMessage("[PostgreSQLDatabaseDriver] Locations read from PostgreSQL database " +
                   itsParameters.driverName,
               itsParameters.quiet);
  }
}

void ObservationCacheAdminPostgreSQL::readObservationCacheData(
    std::vector<DataItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const std::string& fmisid,
    const std::string& measurandId,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readCacheDataFromPostgreSQL(cacheData, dataPeriod, fmisid, measurandId, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readFlashCacheData(
    std::vector<FlashDataItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readFlashCacheDataFromPostgreSQL(cacheData, dataPeriod, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readWeatherDataQCCacheData(
    std::vector<WeatherDataQCItem>& cacheData,
    const boost::posix_time::time_period& dataPeriod,
    const std::string& fmisid,
    const std::string& measurandId,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readWeatherDataQCCacheDataFromPostgreSQL(
      cacheData, dataPeriod, fmisid, measurandId, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readMovingStationsCacheData(
    std::vector<MovingLocationItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readMovingStationsCacheDataFromPostgreSQL(
      cacheData, startTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readObservationCacheData(
    std::vector<DataItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readCacheDataFromPostgreSQL(cacheData, startTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readMagnetometerCacheData(
    std::vector<MagnetometerDataItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readMagnetometerCacheDataFromPostgreSQL(cacheData, startTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readFlashCacheData(
    std::vector<FlashDataItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastStrokeTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
  db->readFlashCacheDataFromPostgreSQL(
      cacheData, startTime, lastStrokeTime, lastModifiedTime, itsTimeZones);
}

void ObservationCacheAdminPostgreSQL::readWeatherDataQCCacheData(
    std::vector<WeatherDataQCItem>& cacheData,
    const boost::posix_time::ptime& startTime,
    const boost::posix_time::ptime& lastModifiedTime,
    const Fmi::TimeZones& /* timezones */) const
{
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
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
  std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
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

    if (Spine::Reactor::isShuttingDown())
      return;

    std::shared_ptr<PostgreSQLObsDB> db = itsPostgreSQLConnectionPool->getConnection(false);
    const std::string place = "Helsinki";
    const std::string lang = "fi";
    Spine::LocationPtr loc = itsGeonames->nameSearch(place, lang);

    // Perhaps the point of above is to make sure the engine is available?
    // Moved the log message downwards accordingly.

    logMessage(
        "[PostgreSQLDatabaseDriver] Loading stations from " + itsParameters.driverName + "...",
        itsParameters.quiet);

    auto newStationInfo = boost::make_shared<StationInfo>();

    // Get all the stations
    db->getStations(newStationInfo->stations);

    // Get wmo and lpnn and rwsid identifiers too
    db->translateToWMO(newStationInfo->stations);
    db->translateToLPNN(newStationInfo->stations);
    db->translateToRWSID(newStationInfo->stations);

    for (Spine::Station& station : newStationInfo->stations)
    {
      if (Spine::Reactor::isShuttingDown())
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

      if (Spine::Reactor::isShuttingDown())
        throw Fmi::Exception(
            BCP, "PostgreSQLDatabaseDriver: Aborting station preload due to shutdown request");
    }

    addInfoToStations(newStationInfo->stations, "");

    // Serialize stations to disk and swap the contents into itsParameters.params->stationInfo

    logMessage("[PostgreSQLDatabaseDriver] Serializing stations...", itsParameters.quiet);
    newStationInfo->serialize(serializedStationsFile);

    // We can safely copy old settings since this is the only thread updating the stationInfo
    // variable

    newStationInfo->stationLocations = itsParameters.params->stationInfo.load()->stationLocations;

    itsParameters.params->stationInfo.store(newStationInfo);

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
    const std::shared_ptr<ObservationCache>& cache) const
{
  auto min_last_time = boost::posix_time::second_clock::universal_time() -
                       boost::posix_time::hours(itsParameters.extCacheDuration);

  auto last_time = cache->getLatestWeatherDataQCTime();
  auto last_modified_time = cache->getLatestWeatherDataQCModifiedTime();

  if (last_time.is_not_a_date_time())
    last_time = min_last_time;

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = last_time;

  return std::pair<boost::posix_time::ptime, boost::posix_time::ptime>(last_time,
                                                                       last_modified_time);
}

std::pair<boost::posix_time::ptime, boost::posix_time::ptime>
ObservationCacheAdminPostgreSQL::getLatestObservationTime(
    const std::shared_ptr<ObservationCache>& cache) const
{
  auto min_last_time = boost::posix_time::second_clock::universal_time() -
                       boost::posix_time::hours(itsParameters.finCacheDuration);

  auto last_time = cache->getLatestObservationTime();
  auto last_modified_time = cache->getLatestObservationModifiedTime();

  if (last_time.is_not_a_date_time())
    last_time = min_last_time;

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = last_time;

  return std::pair<boost::posix_time::ptime, boost::posix_time::ptime>(last_time,
                                                                       last_modified_time);
}

std::map<std::string, boost::posix_time::ptime> ObservationCacheAdminPostgreSQL::getLatestFlashTime(
    const std::shared_ptr<ObservationCache>& cache) const
{
  std::map<std::string, boost::posix_time::ptime> ret;

  auto min_last_time = (boost::posix_time::second_clock::universal_time() -
                        boost::posix_time::hours(itsParameters.flashCacheDuration));

  auto last_time = cache->getLatestFlashTime();
  auto last_modified_time = cache->getLatestFlashModifiedTime();

  if (last_time.is_not_a_date_time())
    last_time = min_last_time;

  if (last_modified_time.is_not_a_date_time())
    last_modified_time = last_time;

  ret["start_time"] = min_last_time;
  ret["last_stroke_time"] = last_time;
  ret["last_modified_time"] = last_modified_time;

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
