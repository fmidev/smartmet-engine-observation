#include "SpatiaLite.h"
#include "ObservableProperty.h"
#include "SpatiaLiteCacheParameters.h"
#include <fmt/format.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Exception.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesOutput.h>
#include <chrono>
#include <iostream>

namespace BO = SmartMet::Engine::Observation;
namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

template <typename Container, typename Key>
bool exists(const Container &container, const Key &key)
{
  return (container.find(key) != container.end());
}

template <typename Clock, typename Duration>
std::ostream &operator<<(std::ostream &stream,
                         const std::chrono::time_point<Clock, Duration> &time_point)
{
  const time_t time = Clock::to_time_t(time_point);
#if __GNUC__ > 4 || ((__GNUC__ == 4) && __GNUC_MINOR__ > 8 && __GNUC_REVISION__ > 1)
  // Maybe the put_time will be implemented later?
  struct tm tm;
  localtime_r(&time, &tm);
  return stream << std::put_time(&tm, "%c");  // Print standard date&time
#else
  char buffer[26];
  ctime_r(&time, buffer);
  buffer[24] = '\0';  // Removes the newline that is added
  return stream << buffer;
#endif
}

namespace SmartMet
{
// Mutex for write operations - otherwise you get table locked errors
// in MULTITHREAD-mode.

Spine::MutexType write_mutex;

namespace Engine
{
namespace Observation
{
namespace
{
// Round down to HH:00:00

boost::posix_time::ptime round_down_to_hour(const boost::posix_time::ptime &t)
{
  auto hour = t.time_of_day().hours();
  return boost::posix_time::ptime(t.date(), boost::posix_time::hours(hour));
}

void solveMeasurandIds(const std::vector<std::string> &parameters,
                       const ParameterMap &parameterMap,
                       const std::string &stationType,
                       std::multimap<int, std::string> &parameterIDs)
{
  try
  {
    // Empty list means we want all parameters
    const bool findOnlyGiven = (not parameters.empty());

    for (auto params = parameterMap.begin(); params != parameterMap.end(); ++params)
    {
      if (findOnlyGiven &&
          find(parameters.begin(), parameters.end(), params->first) == parameters.end())
        continue;

      auto gid = params->second.find(stationType);
      if (gid == params->second.end())
        continue;

      int id;
      try
      {
        id = std::stoi(gid->second);
      }
      catch (std::exception &)
      {
        // gid is either too large or not convertible (ie. something is wrong)
        continue;
      }

      parameterIDs.emplace(id, params->first);
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Solving measurand id failed!");
  }
}

boost::posix_time::ptime parse_sqlite_time(sqlite3pp::query::iterator &iter, int column)
{
  // 1 = INTEGER; 2 = FLOAT, 3 = TEXT, 4 = BLOB, 5 = NULL
  if ((*iter).column_type(column) != SQLITE_TEXT)
    throw Spine::Exception(BCP, "Invalid time column from sqlite query")
        .addParameter("columntype", Fmi::to_string((*iter).column_type(column)));

  std::string timestring = (*iter).get<char const *>(column);
  return Fmi::TimeParser::parse(timestring);
}

};  // namespace

SpatiaLite::SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options)
    : itsShutdownRequested(false),
      itsMaxInsertSize(options.maxInsertSize),
      itsDataInsertCache(options.dataInsertCacheSize),
      itsWeatherQCInsertCache(options.weatherDataQCInsertCacheSize),
      itsFlashInsertCache(options.flashInsertCacheSize)
{
  try
  {
    srid = "4326";

    // Enabling shared cache may decrease read performance:
    // https://manski.net/2012/10/sqlite-performance/
    // However, for a single shared db it may be better to share:
    // https://github.com/mapnik/mapnik/issues/797

    int flags = (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_SHAREDCACHE |
                 SQLITE_OPEN_NOMUTEX);

    itsDB.connect(spatialiteFile.c_str(), flags);
    // timeout ms
    itsDB.set_busy_timeout(options.sqlite.timeout);

    // Default is fully synchronous (2), with WAL normal (1) is supposedly
    // better, for best speed we
    // choose off (0), since this is only a cache.
    //    options += " synchronous=" + synchronous;

    void *cache = sqlite_api::spatialite_alloc_connection();
    sqlite_api::spatialite_init_ex(itsDB.sqlite3_handle(), cache, 0);

    std::string journalModePragma = "PRAGMA journal_mode=" + options.sqlite.journal_mode;
    itsDB.execute(journalModePragma.c_str());

    std::string mmapSizePragma = "PRAGMA mmap_size=" + Fmi::to_string(options.sqlite.mmap_size);
    itsDB.execute(mmapSizePragma.c_str());

    std::string synchronousPragma = "PRAGMA synchronous=" + options.sqlite.synchronous;
    itsDB.execute(synchronousPragma.c_str());

    std::string autoVacuumPragma = "PRAGMA auto_vacuum=" + options.sqlite.auto_vacuum;
    itsDB.execute(autoVacuumPragma.c_str());

    std::string threadsPragma = "PRAGMA threads=" + Fmi::to_string(options.sqlite.threads);
    itsDB.execute(threadsPragma.c_str());

    std::string walSizePragma =
        "PRAGMA wal_autocheckpoint=" + Fmi::to_string(options.sqlite.wal_autocheckpoint);
    itsDB.execute(walSizePragma.c_str());

    if (options.sqlite.cache_size != 0)
    {
      std::string cacheSizePragma =
          "PRAGMA cache_size=" + Fmi::to_string(options.sqlite.cache_size);
      itsDB.execute(cacheSizePragma.c_str());
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Connecting database '" + spatialiteFile + "' failed!");
  }
}

SpatiaLite::~SpatiaLite() {}

void SpatiaLite::createTables()
{
  try
  {
    // No locking needed during initialization phase
    initSpatialMetaData();
    createStationTable();
    createStationGroupsTable();
    createGroupMembersTable();
    createLocationsTable();
    createObservationDataTable();
    createWeatherDataQCTable();
    createFlashDataTable();
    createObservablePropertyTable();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of database tables failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void SpatiaLite::shutdown()
{
  std::cout << "  -- Shutdown requested (SpatiaLite)\n";
  itsShutdownRequested = true;
}

void SpatiaLite::createLocationsTable()
{
  try
  {
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS locations("
        "fmisid INTEGER NOT NULL PRIMARY KEY, "
        "location_id INTEGER NOT NULL,"
        "country_id INTEGER NOT NULL,"
        "location_start DATETIME, "
        "location_end DATETIME, "
        "longitude REAL, "
        "latitude REAL, "
        "x REAL, "
        "y REAL, "
        "elevation REAL, "
        "time_zone_name TEXT, "
        "time_zone_abbrev TEXT)");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of locations table failed!");
  }
}

void SpatiaLite::createStationGroupsTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS station_groups ("
        "group_id INTEGER NOT NULL PRIMARY KEY, "
        "group_code TEXT)");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of station_groups table failed!");
  }
}

void SpatiaLite::createGroupMembersTable()
{
  try
  {
    // No locking needed during initialization phase
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(
        itsDB,
        "CREATE TABLE IF NOT EXISTS group_members ("
        "group_id INTEGER NOT NULL, "
        "fmisid INTEGER NOT NULL, "
        "CONSTRAINT fk_station_groups FOREIGN KEY (group_id) "
        "REFERENCES station_groups "
        "(group_id), "
        "CONSTRAINT fk_stations FOREIGN KEY (fmisid) REFERENCES "
        "stations (fmisid)); CREATE INDEX IF NOT EXISTS gm_sg_idx ON group_members "
        "(group_id,fmisid);");
    cmd.execute();
    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of group_members table failed!");
  }
}

void SpatiaLite::createObservationDataTable()
{
  try
  {
    // No locking needed during initialization phase
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(
        itsDB,
        "CREATE TABLE IF NOT EXISTS observation_data("
        "fmisid INTEGER NOT NULL, "
        "data_time DATETIME NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "PRIMARY KEY (data_time, fmisid, measurand_id, producer_id, "
        "measurand_no)); CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON "
        "observation_data(data_time);");
    cmd.execute();
    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of observation_data table failed!");
  }
}

void SpatiaLite::createWeatherDataQCTable()
{
  try
  {
    // No locking needed during initialization phase
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS weather_data_qc ("
                           "fmisid INTEGER NOT NULL, "
                           "obstime DATETIME NOT NULL, "
                           "parameter TEXT NOT NULL, "
                           "sensor_no INTEGER NOT NULL, "
                           "value REAL NOT NULL, "
                           "flag INTEGER NOT NULL, "
                           "PRIMARY KEY (obstime, fmisid, parameter, sensor_no)); CREATE INDEX IF "
                           "NOT EXISTS weather_data_qc_obstime_idx ON "
                           "weather_data_qc(obstime);");
    cmd.execute();
    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of weather_data_qc table failed!");
  }
}

void SpatiaLite::createFlashDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS flash_data("
                           "stroke_time DATETIME NOT NULL, "
                           "stroke_time_fraction INTEGER NOT NULL, "
                           "flash_id INTEGER NOT NULL, "
                           "multiplicity INTEGER NOT NULL, "
                           "peak_current INTEGER NOT NULL, "
                           "sensors INTEGER NOT NULL, "
                           "freedom_degree INTEGER NOT NULL, "
                           "ellipse_angle REAL NOT NULL, "
                           "ellipse_major REAL NOT NULL, "
                           "ellipse_minor REAL NOT NULL, "
                           "chi_square REAL NOT NULL, "
                           "rise_time REAL NOT NULL, "
                           "ptz_time REAL NOT NULL, "
                           "cloud_indicator INTEGER NOT NULL, "
                           "angle_indicator INTEGER NOT NULL, "
                           "signal_indicator INTEGER NOT NULL, "
                           "timing_indicator INTEGER NOT NULL, "
                           "stroke_status INTEGER NOT NULL, "
                           "data_source INTEGER NOT NULL, "
                           "created  DATETIME, "
                           "modified_last DATETIME, "
                           "modified_by INTEGER, "
                           "PRIMARY KEY (stroke_time, stroke_time_fraction, flash_id)); CREATE "
                           "INDEX IF NOT EXISTS flash_data_stroke_time_idx ON "
                           "flash_data(stroke_time)");
    cmd.execute();
    xct.commit();

    bool got_data = false;
    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(stroke_location) AS latitude FROM flash_data LIMIT 1");
      got_data = qry.begin() != qry.end();
    }
    catch (std::exception const &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('flash_data', 'stroke_location', "
                           "4326, 'POINT', 'XY')");
      got_data = qry.begin() != qry.end();
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='flash_data' AND f_geometry_column = "
                         "'stroke_location'");
    int spatial_index_enabled = 0;
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (!got_data || spatial_index_enabled == 0)
    {
      sqlite3pp::query qry(itsDB, "SELECT CreateSpatialIndex('flash_data', 'stroke_location')");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of flash_data table failed!");
  }
}

void SpatiaLite::initSpatialMetaData()
{
  try
  {
    // This will create all meta data required to make spatial queries, see
    // http://www.gaia-gis.it/gaia-sins/spatialite-cookbook/html/metadata.html

    // Check whether the table exists already
    std::string name;
    sqlite3pp::query qry(itsDB,
                         "SELECT name FROM sqlite_master WHERE type='table' AND name "
                         "= 'spatial_ref_sys'");

    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
    {
      itsDB.execute("SELECT InitSpatialMetaData()");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "initSpatialMetaData failed!");
  }
}

void SpatiaLite::createStationTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS stations("
        "fmisid INTEGER NOT NULL, "
        "wmo INTEGER, "
        "geoid INTEGER, "
        "lpnn INTEGER, "
        "rwsid INTEGER, "
        "station_start DATETIME, "
        "station_end DATETIME, "
        "station_formal_name TEXT NOT NULL, "
        "PRIMARY KEY (fmisid, geoid, station_start, station_end))");

    // If geometry column doesn't exist add it
    sqlite3pp::query qry(itsDB,
                         "SELECT * FROM geometry_columns WHERE f_table_name LIKE 'stations'");

    if (qry.begin() == qry.end())
    {
      itsDB.execute("SELECT AddGeometryColumn('stations', 'the_geom', 4326, 'POINT', 'XY')");
      std::cout << "Adding spatial index to stations table" << std::endl;
      itsDB.execute("SELECT CreateSpatialIndex('stations','the_geom')");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of stations table failed!");
  }
}

size_t SpatiaLite::selectCount(const std::string &queryString)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    size_t count = 0;
    sqlite3pp::query qry(itsDB, queryString.c_str());
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      count = (*iter).get<long long int>(0);

    return count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "count(*) query failed!");
  }
}

size_t SpatiaLite::getStationCount()
{
  return selectCount("SELECT COUNT(*) FROM stations");
}

boost::posix_time::ptime SpatiaLite::getLatestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest observation time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getOldestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest observation time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(obstime) FROM weather_data_qc");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest WeatherDataQCTime query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getOldestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(obstime) FROM weather_data_qc");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest WeatherDataQCTime query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestFlashTime()
{
  try
  {
    string tablename = "flash_data";
    string time_field = "stroke_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest flash time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getOldestFlashTime()
{
  try
  {
    string tablename = "flash_data";
    string time_field = "stroke_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest flash time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT DATETIME(MAX(" + time_field + ")) FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getOldestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT DATETIME(MIN(" + time_field + ")) FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parse_sqlite_time(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest time query failed!");
  }
}

void SpatiaLite::fillLocationCache(const vector<LocationItem> &locations)
{
  // Use a loop with sleep to avoid "database locked" problems
  const int max_retries = 10;
  int retries = 0;

  while (true)
  {
    try
    {
      Spine::WriteLock lock(write_mutex);

      sqlite3pp::transaction xct(itsDB);
      sqlite3pp::command cmd(itsDB,
                             "INSERT OR REPLACE INTO locations "
                             "(location_id, fmisid, country_id, location_start, location_end, "
                             "longitude, latitude, x, "
                             "y, "
                             "elevation, time_zone_name, time_zone_abbrev) "
                             "VALUES ("
                             ":location_id,"
                             ":fmisid,"
                             ":country_id,"
                             ":location_start,"
                             ":location_end,"
                             ":longitude,"
                             ":latitude,"
                             ":x,"
                             ":y,"
                             ":elevation,"
                             ":time_zone_name,"
                             ":time_zone_abbrev)");

      for (const LocationItem &item : locations)
      {
        std::string location_start = Fmi::to_iso_extended_string(item.location_start);
        std::string location_end = Fmi::to_iso_extended_string(item.location_end);
        cmd.bind(":location_id", item.location_id);
        cmd.bind(":fmisid", item.fmisid);
        cmd.bind(":country_id", item.country_id);
        cmd.bind(":location_start", location_start, sqlite3pp::nocopy);
        cmd.bind(":location_end", location_end, sqlite3pp::nocopy);
        cmd.bind(":longitude", item.longitude);
        cmd.bind(":latitude", item.latitude);
        cmd.bind(":x", item.x);
        cmd.bind(":y", item.y);
        cmd.bind(":elevation", item.elevation);
        cmd.bind(":time_zone_name", item.time_zone_name, sqlite3pp::nocopy);
        cmd.bind(":time_zone_abbrev", item.time_zone_abbrev, sqlite3pp::nocopy);
        cmd.execute();
        cmd.reset();
      }
      xct.commit();

      return;
    }
    catch (std::exception &e)
    {
      std::cerr << "Warning, retry " << ++retries << ": " << e.what() << std::endl;
    }
    catch (...)
    {
      std::cerr << "Warning, retry " << ++retries
                << ": failed to initialize spatialite locations database" << std::endl;
    }
    if (retries == max_retries)
      throw Spine::Exception(BCP, "Filling of location cache failed!", nullptr);
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
  }
}

void SpatiaLite::cleanDataCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestObservationTime();
    if (t <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(t);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM observation_data WHERE data_time < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of data cache failed!");
  }
}

void SpatiaLite::cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestWeatherDataQCTime();
    if (t <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(t);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM weather_data_qc WHERE obstime < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of WeatherDataQCCache failed!");
  }
}

void SpatiaLite::cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestFlashTime();
    if (t <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(t);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM flash_data WHERE stroke_time < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of FlashDataCache failed!");
  }
}

std::size_t SpatiaLite::fillDataCache(const vector<DataItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    const char *sqltemplate =
        "INSERT OR REPLACE INTO observation_data "
        "(fmisid, measurand_id, producer_id, measurand_no, data_time, "
        "data_value, data_quality) "
        "VALUES "
        "(:fmisid,:measurand_id,:producer_id,:measurand_no,:data_time,:data_value,:data_quality)"
        ";";

    std::size_t pos1 = 0;
    std::size_t write_count = 0;

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;
      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        // boost::this_thread::yield();
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
      }

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2;
      for (pos2 = pos1; new_hashes.size() < itsMaxInsertSize && pos2 < cacheData.size(); ++pos2)
      {
        const auto &item = cacheData[pos2];

        auto hash = item.hash_value();

        if (!itsDataInsertCache.exists(hash))
        {
          new_items.push_back(pos2);
          new_hashes.push_back(hash);
        }
      }

      // Now insert the new items

      if (!new_items.empty())
      {
        Spine::WriteLock lock(write_mutex);
        sqlite3pp::transaction xct(itsDB);
        sqlite3pp::command cmd(itsDB, sqltemplate);

        for (const auto i : new_items)
        {
          const auto &item = cacheData[i];
          cmd.bind(":fmisid", item.fmisid);
          cmd.bind(":measurand_id", item.measurand_id);
          cmd.bind(":producer_id", item.producer_id);
          cmd.bind(":measurand_no", item.measurand_no);
          std::string timestring = Fmi::to_iso_extended_string(item.data_time);
          cmd.bind(":data_time", timestring, sqlite3pp::nocopy);
          cmd.bind(":data_value", item.data_value);
          cmd.bind(":data_quality", item.data_quality);
          cmd.execute();
          cmd.reset();
        }
        xct.commit();
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsDataInsertCache.add(hash);

      pos1 = pos2;
    }
    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}

std::size_t SpatiaLite::fillWeatherDataQCCache(const vector<WeatherDataQCItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    const char *sqltemplate =
        "INSERT OR IGNORE INTO weather_data_qc"
        "(fmisid, obstime, parameter, sensor_no, value, flag)"
        "VALUES (:fmisid,:obstime,:parameter,:sensor_no,:value,:flag)";

    std::size_t pos1 = 0;
    std::size_t write_count = 0;

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;

      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        // boost::this_thread::yield();
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
      }

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2;
      for (pos2 = pos1; new_hashes.size() < itsMaxInsertSize && pos2 < cacheData.size(); ++pos2)
      {
        const auto &item = cacheData[pos2];

        auto hash = item.hash_value();

        if (!itsWeatherQCInsertCache.exists(hash))
        {
          new_items.push_back(pos2);
          new_hashes.push_back(hash);
        }
      }

      // Now insert the new items

      if (!new_items.empty())
      {
        Spine::WriteLock lock(write_mutex);
        sqlite3pp::transaction xct(itsDB);
        sqlite3pp::command cmd(itsDB, sqltemplate);

        for (const auto i : new_items)
        {
          const auto &item = cacheData[i];
          cmd.bind(":fmisid", item.fmisid);
          std::string timestring = Fmi::to_iso_extended_string(item.obstime);
          cmd.bind(":obstime", timestring, sqlite3pp::nocopy);
          cmd.bind(":parameter", item.parameter, sqlite3pp::nocopy);
          cmd.bind(":sensor_no", item.sensor_no);
          cmd.bind(":value", item.value);
          cmd.bind(":flag", item.flag);
          cmd.execute();
          cmd.reset();
        }
        xct.commit();
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsWeatherQCInsertCache.add(hash);

      pos1 = pos2;
    }
    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of WeatherDataQCCache failed!");
  }
}

std::size_t SpatiaLite::fillFlashDataCache(const vector<FlashDataItem> &flashCacheData)
{
  try
  {
    if (flashCacheData.empty())
      return flashCacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;

    while (pos1 < flashCacheData.size())
    {
      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        // boost::this_thread::yield();
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
      }

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2;
      for (pos2 = pos1; new_hashes.size() < itsMaxInsertSize && pos2 < flashCacheData.size();
           ++pos2)
      {
        const auto &item = flashCacheData[pos2];

        auto hash = item.hash_value();

        if (!itsFlashInsertCache.exists(hash))
        {
          new_items.push_back(pos2);
          new_hashes.push_back(hash);
        }
      }

      // Now insert the new items

      if (!new_items.empty())
      {
        Spine::WriteLock lock(write_mutex);
        sqlite3pp::transaction xct(itsDB);

        for (const auto i : new_items)
        {
          const auto &item = flashCacheData[i];

          std::string stroke_location =
              "GeomFromText('POINT(" + Fmi::to_string("%.10g", item.longitude) + " " +
              Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

          std::string sqltemplate =
              "INSERT OR IGNORE INTO flash_data "
              "(stroke_time, stroke_time_fraction, flash_id, multiplicity, "
              "peak_current, sensors, freedom_degree, ellipse_angle, "
              "ellipse_major, "
              "ellipse_minor, "
              "chi_square, rise_time, ptz_time, cloud_indicator, "
              "angle_indicator, "
              "signal_indicator, timing_indicator, stroke_status, "
              "data_source, stroke_location) "
              "VALUES ("
              ":timestring,"
              ":stroke_time_fraction, "
              ":flash_id,"
              ":multiplicity,"
              ":peak_current,"
              ":sensors,"
              ":freedom_degree,"
              ":ellipse_angle,"
              ":ellipse_major,"
              ":ellipse_minor,"
              ":chi_square,"
              ":rise_time,"
              ":ptz_time,"
              ":cloud_indicator,"
              ":angle_indicator,"
              ":signal_indicator,"
              ":timing_indicator,"
              ":stroke_status,"
              ":data_source," +
              stroke_location + ");";

          sqlite3pp::command cmd(itsDB, sqltemplate.c_str());

          std::string timestring = Fmi::to_iso_extended_string(item.stroke_time);
          boost::replace_all(timestring, ",", ".");

          // @todo There is no simple way to optionally set possible NULL values.
          // Find out later how to do it.

          try
          {
            cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
            cmd.bind(":stroke_time_fraction", item.stroke_time_fraction);
            cmd.bind(":flash_id", static_cast<int>(item.flash_id));
            cmd.bind(":multiplicity", item.multiplicity);
            cmd.bind(":peak_current", item.peak_current);
            cmd.bind(":sensors", item.sensors);
            cmd.bind(":freedom_degree", item.freedom_degree);
            cmd.bind(":ellipse_angle", item.ellipse_angle);
            cmd.bind(":ellipse_major", item.ellipse_major);
            cmd.bind(":ellipse_minor", item.ellipse_minor);
            cmd.bind(":chi_square", item.chi_square);
            cmd.bind(":rise_time", item.rise_time);
            cmd.bind(":ptz_time", item.ptz_time);
            cmd.bind(":cloud_indicator", item.cloud_indicator);
            cmd.bind(":angle_indicator", item.angle_indicator);
            cmd.bind(":signal_indicator", item.signal_indicator);
            cmd.bind(":timing_indicator", item.timing_indicator);
            cmd.bind(":stroke_status", item.stroke_status);
            cmd.bind(":data_source", item.data_source);
            cmd.execute();
            cmd.reset();
          }
          catch (std::exception &e)
          {
            std::cerr << "Problem updating flash data: " << e.what() << std::endl;
          }
        }
        xct.commit();
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsFlashInsertCache.add(hash);

      pos1 = pos2;
    }
    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Flash data cache update failed!");
  }
}

void SpatiaLite::updateStationsAndGroups(const StationInfo &info)
{
  try
  {
    // The stations and the groups must be updated simultaneously,
    // hence a common lock. Note that the latter call does reads too,
    // so it would be impossible to create a single transaction of
    // both updates.

    Spine::WriteLock lock(write_mutex);
    updateStations(info.stations);
    updateStationGroups(info);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Update of stations and groups failed!");
  }
}

void SpatiaLite::updateStations(const Spine::Stations &stations)
{
  try
  {
    // Locking handled by updateStationsAndGroups

    sqlite3pp::transaction xct(itsDB);

    for (const Spine::Station &station : stations)
    {
      if (itsShutdownRequested)
        break;

      auto sql = fmt::format(
          "INSERT OR REPLACE INTO stations (fmisid, geoid, wmo, lpnn, station_formal_name, "
          "station_start, station_end, the_geom) VALUES "
          "({},{},{},{},:station_formal_name,:station_start,:station_end,GeomFromText('POINT({:."
          "10f} {:.10f})', {}))",
          station.fmisid,
          station.geoid,
          station.wmo,
          station.lpnn,
          station.longitude_out,
          station.latitude_out,
          srid);

      sqlite3pp::command cmd(itsDB, sql.c_str());

      std::string start_time = Fmi::to_iso_extended_string(station.station_start);
      std::string start_end = Fmi::to_iso_extended_string(station.station_end);
      cmd.bind(":station_formal_name", station.station_formal_name, sqlite3pp::nocopy);
      cmd.bind(":station_start", start_time, sqlite3pp::nocopy);
      cmd.bind(":station_end", start_end, sqlite3pp::nocopy);
      cmd.execute();
    }

    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Stations update failed!");
  }
}

void SpatiaLite::updateStationGroups(const StationInfo &info)
{
  try
  {
    // Locking handled by updateStationsAndGroups

    sqlite3pp::transaction xct(itsDB);

    // Station groups at the moment.
    size_t stationGroupsCount = selectCount("SELECT COUNT(*) FROM station_groups");

    for (const Spine::Station &station : info.stations)
    {
      // Skipping the empty cases.
      if (station.station_type.empty())
        continue;

      const std::string groupCodeUpper = Fmi::ascii_toupper_copy(station.station_type);

      // Search the group_id for a group_code.
      auto sql = fmt::format("SELECT group_id FROM station_groups WHERE group_code = '{}' LIMIT 1;",
                             groupCodeUpper);

      boost::optional<int> group_id;

      sqlite3pp::query qry(itsDB, sql.c_str());
      sqlite3pp::query::iterator iter = qry.begin();
      if (iter != qry.end())
        group_id = (*iter).get<int>(0);

      // Group id not found, so we must add a new one.
      if (not group_id)
      {
        stationGroupsCount++;
        group_id = stationGroupsCount;
        auto sql = fmt::format(
            "INSERT OR REPLACE INTO station_groups (group_id, group_code) VALUES ({}, '{}')",
            stationGroupsCount,
            groupCodeUpper);
        sqlite3pp::command cmd(itsDB, sql.c_str());
        cmd.execute();
      }

      // Avoid duplicates.
      auto dupsql =
          fmt::format("SELECT COUNT(*) FROM group_members WHERE group_id={} AND fmisid={}",
                      group_id.get(),
                      station.fmisid);

      int groupCount = selectCount(dupsql);

      if (groupCount == 0)
      {
        // Insert a group member. Ignore if insertion fail (perhaps group_id or
        // fmisid is not found from the stations table)
        auto sql =
            fmt::format("INSERT OR IGNORE INTO group_members (group_id, fmisid) VALUES ({}, {})",
                        group_id.get(),
                        station.fmisid);
        sqlite3pp::command cmd(itsDB, sql.c_str());
        cmd.execute();
      }
    }
    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Updating station groups failed!");
  }
}

Spine::Stations SpatiaLite::findStationsByWMO(const Settings &settings, const StationInfo &info)
{
  try
  {
    return info.findWmoStations(settings.wmos);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Searching stations by WMO numbers failed");
  }
}

Spine::Stations SpatiaLite::findStationsByLPNN(const Settings &settings, const StationInfo &info)
{
  try
  {
    return info.findLpnnStations(settings.lpnns);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Searching stations by LPNN numbers failed");
  }
}

Spine::Stations SpatiaLite::findNearestStations(const Spine::LocationPtr &location,
                                                const map<int, Spine::Station> &stationIndex,
                                                int maxdistance,
                                                int numberofstations,
                                                const std::set<std::string> &stationgroup_codes,
                                                const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &endtime)
{
  try
  {
    return findNearestStations(location->latitude,
                               location->longitude,
                               stationIndex,
                               maxdistance,
                               numberofstations,
                               stationgroup_codes,
                               starttime,
                               endtime);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Finding nearest stations failed!");
  }
}

Spine::Stations SpatiaLite::findNearestStations(double latitude,
                                                double longitude,
                                                const map<int, Spine::Station> &stationIndex,
                                                int maxdistance,
                                                int numberofstations,
                                                const std::set<std::string> &stationgroup_codes,
                                                const boost::posix_time::ptime &starttime,
                                                const boost::posix_time::ptime &endtime)
{
  try
  {
    Spine::Stations stations;

    std::string distancesql =
        "SELECT DISTINCT s.fmisid, "
        "IFNULL(ST_Distance(s.the_geom, "
        "(SELECT GeomFromText('POINT(" +
        Fmi::to_string("%.10g", longitude) + " " + Fmi::to_string("%.10g", latitude) + ")'," +
        srid +
        ")), 1), 0)/1000 dist "  // divide by 1000 to get kilometres
        ", s.wmo"
        ", s.geoid"
        ", s.lpnn"
        ", X(s.the_geom)"
        ", Y(s.the_geom)"
        ", s.station_formal_name "
        "FROM ";

    if (not stationgroup_codes.empty())
    {  // Station selection from a station
       // group or groups.
      distancesql +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s oN gm.fmisid = s.fmisid ";
    }
    else
    {
      // Do not care about station group.
      distancesql += "stations s ";
    }

    distancesql += "WHERE ";

    if (not stationgroup_codes.empty())
    {
      auto it = stationgroup_codes.begin();
      distancesql += "( sg.group_code='" + *it + "' ";
      for (it++; it != stationgroup_codes.end(); it++)
        distancesql += "OR sg.group_code='" + *it + "' ";
      distancesql += ") AND ";
    }

    distancesql += "PtDistWithin((SELECT GeomFromText('POINT(" +
                   Fmi::to_string("%.10g", longitude) + " " + Fmi::to_string("%.10g", latitude) +
                   ")', " + srid + ")), s.the_geom, " + Fmi::to_string(maxdistance) + ")=1 ";

    distancesql +=
        "AND (:starttime BETWEEN s.station_start AND s.station_end OR "
        ":endtime BETWEEN s.station_start AND s.station_end) "
        "ORDER BY dist ASC, s.fmisid ASC LIMIT " +
        Fmi::to_string(numberofstations) + ";";

    int fmisid = 0;
    int wmo = -1;
    int geoid = -1;
    int lpnn = -1;
    double longitude_out = std::numeric_limits<double>::max();
    double latitude_out = std::numeric_limits<double>::max();
    string distance = "";
    std::string station_formal_name = "";

    sqlite3pp::query qry(itsDB, distancesql.c_str());
    for (auto row : qry)
    {
      try
      {
        fmisid = row.get<int>(0);
        // Round distances to 100 meter precision
        distance = fmt::format("{:.1f}", Fmi::stod(row.get<string>(1)));
        wmo = row.get<int>(2);
        geoid = row.get<int>(3);
        lpnn = row.get<int>(4);
        longitude_out = Fmi::stod(row.get<std::string>(5));
        latitude_out = Fmi::stod(row.get<std::string>(6));
        station_formal_name = row.get<std::string>(7);

        auto stationIterator = stationIndex.find(fmisid);
        if (stationIterator != stationIndex.end())
        {
          stations.push_back(stationIterator->second);
        }
        else
        {
          continue;
        }
      }
      catch (const std::bad_cast &e)
      {
        cout << e.what() << endl;
      }

      stations.back().distance = distance;
      stations.back().station_id = fmisid;
      stations.back().fmisid = fmisid;
      stations.back().wmo = (wmo == 0 ? -1 : wmo);
      stations.back().geoid = (geoid == 0 ? -1 : geoid);
      stations.back().lpnn = (lpnn == 0) ? -1 : lpnn;
      stations.back().requestedLat = latitude;
      stations.back().requestedLon = longitude;
      stations.back().longitude_out = longitude_out;
      stations.back().latitude_out = latitude_out;
      stations.back().station_formal_name = station_formal_name;
      calculateStationDirection(stations.back());
    }

    return stations;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Finding nearest stations failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const ParameterMap &parameterMap,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = settings.stationtype;

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    std::string qstations;
    map<int, Spine::Station> tmpStations;
    for (const Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      qstations += Fmi::to_string(s.station_id) + ",";
    }
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<string, int> timeseriesPositions;
    std::map<std::string, std::string> parameterNameMap;
    map<string, int> specialPositions;

    std::string param;

    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        std::string nameInRequest = p.name();
        Fmi::ascii_tolower(nameInRequest);
        removePrefix(nameInRequest, "qc_");

        std::string shortname = parseParameterName(nameInRequest);

        if (!parameterMap.at(shortname).at(stationtype).empty())
        {
          std::string nameInDatabase = parameterMap.at(shortname).at(stationtype);
          timeseriesPositions[nameInRequest] = pos;
          parameterNameMap[nameInRequest] = nameInDatabase;

          nameInDatabase = parseParameterName(nameInDatabase);
          Fmi::ascii_toupper(nameInDatabase);
          param += "'" + nameInDatabase + "',";
        }
      }
      else
      {
        string name = p.name();
        Fmi::ascii_tolower(name);

        if (name.find("windcompass") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("winddirection").at(stationtype)) +
                   "',";
          timeseriesPositions[parameterMap.at("winddirection").at(stationtype)] = pos;
          specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("windspeedms").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("relativehumidity").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("temperature").at(stationtype)) + "',";
          specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("wawa").at(stationtype)) + "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("totalcloudcover").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("temperature").at(stationtype)) + "',";
          specialPositions[name] = pos;
        }
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns(
        new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    param = trimCommasFromEnd(param);

    std::string query;
    if (settings.latest)
    {
      query =
          "SELECT data.fmisid AS fmisid, MAX(data.obstime) AS obstime, "
          "loc.latitude, loc.longitude, loc.elevation, "
          "parameter, value, sensor_no "
          "FROM weather_data_qc data JOIN locations loc ON (data.fmisid = "
          "loc.fmisid) "
          "WHERE data.fmisid IN (" +
          qstations +
          ") "
          "AND data.obstime >= '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND data.obstime <= '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + param +
          ") "
          "GROUP BY data.fmisid, data.parameter, data.sensor_no, "
          "loc.location_id, "
          "loc.location_end, "
          "loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }
    else
    {
      query =
          "SELECT data.fmisid AS fmisid, data.obstime AS obstime, "
          "loc.latitude, loc.longitude, loc.elevation, "
          "parameter, value, sensor_no "
          "FROM weather_data_qc data JOIN locations loc ON (data.fmisid = "
          "loc.fmisid) "
          "WHERE data.fmisid IN (" +
          qstations +
          ") "
          "AND data.obstime >= '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND data.obstime <= '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + param +
          ") "
          "GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no, loc.location_id, "
          "loc.location_end, loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::posix_time::ptime> obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<std::string> > parametersAll;
    std::vector<boost::optional<double> > data_valuesAll;
    std::vector<boost::optional<double> > sensor_nosAll;

    {
      // Spine::ReadLock lock(write_mutex);
      sqlite3pp::query qry(itsDB, query.c_str());

      for (sqlite3pp::query::iterator iter = qry.begin(); iter != qry.end(); ++iter)
      {
        boost::optional<int> fmisid = (*iter).get<int>(0);
        boost::posix_time::ptime obstime = parse_sqlite_time(iter, 1);
        boost::optional<double> latitude = (*iter).get<double>(2);
        boost::optional<double> longitude = (*iter).get<double>(3);
        boost::optional<double> elevation = (*iter).get<double>(4);
        boost::optional<std::string> parameter = (*iter).get<std::string>(5);
        boost::optional<double> data_value;
        if ((*iter).column_type(6) != SQLITE_NULL)
          data_value = (*iter).get<double>(6);
        boost::optional<double> sensor_no = (*iter).get<double>(7);
        fmisidsAll.push_back(fmisid);
        obstimesAll.push_back(obstime);
        latitudesAll.push_back(latitude);
        longitudesAll.push_back(longitude);
        elevationsAll.push_back(elevation);
        parametersAll.push_back(parameter);
        data_valuesAll.push_back(data_value);
        sensor_nosAll.push_back(sensor_no);
      }
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > > data;

    for (const auto &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      std::string parameter = *parametersAll[i];
      int sensor_no = *sensor_nosAll[i];
      Fmi::ascii_tolower(parameter);
      if (sensor_no > 1)
      {
        parameter += "_" + Fmi::to_string(sensor_no);
      }

      ts::Value val;
      if (data_valuesAll[i])
        val = ts::Value(*data_valuesAll[i]);

      data[fmisid][obstime][parameter] = val;
      if (sensor_no == 1)
      {
        parameter += "_1";
        data[fmisid][obstime][parameter] = val;
      }
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value> > dataItem;

    if (settings.timestep > 1 && !settings.latest)
    {
      Spine::TimeSeriesGeneratorOptions opt;
      opt.startTime = settings.starttime;
      opt.endTime = settings.endtime;
      opt.timeStep = settings.timestep;
      opt.startTimeUTC = false;
      opt.endTimeUTC = false;
      auto tlist = Spine::TimeSeriesGenerator::generate(
          opt, timezones.time_zone_from_string(settings.timezone));

      for (const Spine::Station &s : stations)
      {
        if (data.count(s.fmisid) == 0)
        {
          continue;
        }
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            data.at(s.fmisid);
        for (const boost::local_time::local_date_time &t : tlist)
        {
          if (stationData.count(t) > 0)
          {
            dataItem item = std::make_pair(t, stationData.at(t));
            addParameterToTimeSeries(timeSeriesColumns,
                                     item,
                                     specialPositions,
                                     parameterNameMap,
                                     timeseriesPositions,
                                     parameterMap,
                                     stationtype,
                                     tmpStations.at(s.fmisid));
          }
          else
          {
            addEmptyValuesToTimeSeries(timeSeriesColumns,
                                       t,
                                       specialPositions,
                                       parameterNameMap,
                                       timeseriesPositions,
                                       stationtype,
                                       tmpStations.at(s.fmisid));
          }
        }
      }
    }
    else
    {
      for (const Spine::Station &s : stations)
      {
        int fmisid = s.station_id;
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            data[fmisid];
        for (const dataItem &item : stationData)
        {
          addParameterToTimeSeries(timeSeriesColumns,
                                   item,
                                   specialPositions,
                                   parameterNameMap,
                                   timeseriesPositions,
                                   parameterMap,
                                   stationtype,
                                   tmpStations[fmisid]);
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cached WeatherDataQCData query failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedData(const Spine::Stations &stations,
                                                                 const Settings &settings,
                                                                 const ParameterMap &parameterMap,
                                                                 const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype;
    if (settings.stationtype == "opendata_buoy" || settings.stationtype == "opendata_mareograph")
    {
      stationtype = settings.stationtype;
    }
    else
    {
      stationtype = "opendata";
    }

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    std::string qstations;
    map<int, Spine::Station> tmpStations;
    for (const Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      qstations += Fmi::to_string(s.station_id) + ",";
    }
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<int, int> timeseriesPositions;
    map<std::string, int> timeseriesPositionsString;
    std::map<std::string, std::string> parameterNameMap;
    vector<int> paramVector;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        string name = p.name();
        Fmi::ascii_tolower(name);
        removePrefix(name, "qc_");

        if (!parameterMap.at(name).at(stationtype).empty())
        {
          timeseriesPositions[Fmi::stoi(parameterMap.at(name).at(stationtype))] = pos;
          timeseriesPositionsString[name] = pos;
          parameterNameMap[name] = parameterMap.at(name).at(stationtype);
          paramVector.push_back(Fmi::stoi(parameterMap.at(name).at(stationtype)));
          param += parameterMap.at(name).at(stationtype) + ",";
        }
      }
      else
      {
        string name = p.name();
        Fmi::ascii_tolower(name);

        if (name.find("windcompass") != std::string::npos)
        {
          param += parameterMap.at("winddirection").at(stationtype) + ",";
          timeseriesPositions[Fmi::stoi(parameterMap.at("winddirection").at(stationtype))] = pos;
          specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          param += parameterMap.at("windspeedms").at(stationtype) + "," +
                   parameterMap.at("relativehumidity").at(stationtype) + "," +
                   parameterMap.at("temperature").at(stationtype) + ",";
          specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          param += parameterMap.at("wawa").at(stationtype) + "," +
                   parameterMap.at("totalcloudcover").at(stationtype) + "," +
                   parameterMap.at("temperature").at(stationtype) + ",";
          specialPositions[name] = pos;
        }
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    param = trimCommasFromEnd(param);

    std::string query =
        "SELECT data.fmisid AS fmisid, data.data_time AS obstime, "
        "loc.latitude, loc.longitude, loc.elevation, "
        "measurand_id, data_value "
        "FROM observation_data data JOIN locations loc ON (data.fmisid = "
        "loc.fmisid) "
        "WHERE data.fmisid IN (" +
        qstations +
        ") "
        "AND data.data_time >= '" +
        Fmi::to_iso_extended_string(settings.starttime) + "' AND data.data_time <= '" +
        Fmi::to_iso_extended_string(settings.endtime) + "' AND data.measurand_id IN (" + param +
        ") "
        "AND data.measurand_no = 1 "
        "GROUP BY data.fmisid, data.data_time, data.measurand_id, "
        "loc.location_id, "
        "loc.location_end, "
        "loc.latitude, loc.longitude, loc.elevation "
        "ORDER BY fmisid ASC, obstime ASC;";

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::posix_time::ptime> obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<int> > measurand_idsAll;
    std::vector<boost::optional<double> > data_valuesAll;

    {
      // Spine::ReadLock lock(write_mutex);
      sqlite3pp::query qry(itsDB, query.c_str());

      for (sqlite3pp::query::iterator iter = qry.begin(); iter != qry.end(); ++iter)
      {
        boost::optional<int> fmisid = (*iter).get<int>(0);
        boost::posix_time::ptime obstime = parse_sqlite_time(iter, 1);
        boost::optional<double> latitude = (*iter).get<double>(2);
        boost::optional<double> longitude = (*iter).get<double>(3);
        boost::optional<double> elevation = (*iter).get<double>(4);
        boost::optional<int> measurand_id = (*iter).get<int>(5);
        boost::optional<double> data_value;
        if ((*iter).column_type(6) != SQLITE_NULL)
          data_value = (*iter).get<double>(6);
        fmisidsAll.push_back(fmisid);
        obstimesAll.push_back(obstime);
        latitudesAll.push_back(latitude);
        longitudesAll.push_back(longitude);
        elevationsAll.push_back(elevation);
        measurand_idsAll.push_back(measurand_id);
        data_valuesAll.push_back(data_value);
      }
    }

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Spine::TimeSeries::TimeSeriesVectorPtr(new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<int, ts::Value> > > data;
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > >
        dataWithStringParameterId;

    for (const auto &time : obstimesAll)
    {
      // Safety against missing values. At least the latter has occasionally been missing
      if (!fmisidsAll[i] || !measurand_idsAll[i])
        continue;

      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations[fmisid].timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      int measurand_id = *measurand_idsAll[i];

      ts::Value val;
      if (data_valuesAll[i])
        val = ts::Value(*data_valuesAll[i]);

      data[fmisid][obstime][measurand_id] = val;
      dataWithStringParameterId[fmisid][obstime][Fmi::to_string(measurand_id)] = val;
      i++;
    }

    Spine::TimeSeriesGeneratorOptions opt;
    opt.startTime = settings.starttime;
    opt.endTime = settings.endtime;
    opt.timeStep = settings.timestep;
    opt.startTimeUTC = false;
    opt.endTimeUTC = false;

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value> >
        dataItemWithStringParameterId;

    if (settings.timestep > 1 || settings.latest)
    {
      auto tlist = Spine::TimeSeriesGenerator::generate(
          opt, timezones.time_zone_from_string(settings.timezone));
      {
        for (const Spine::Station &s : stations)
        {
          if (settings.latest)
          {
            // Get only the last time step if there is many
            boost::local_time::local_date_time t(boost::local_time::not_a_date_time);
            if (!data[s.fmisid].empty())
            {
              t = data[s.fmisid].rbegin()->first;
            }
            else
            {
              continue;
            }

            // Append weather parameters
            for (int pos : paramVector)
            {
              ts::Value val = data[s.fmisid][t][pos];
              timeSeriesColumns->at(timeseriesPositions[pos]).push_back(ts::TimedValue(t, val));
            }
            // Append special parameters
            for (const auto &special : specialPositions)
            {
              int pos = special.second;
              if (special.first.find("windcompass") != std::string::npos)
              {
                // Have to get wind direction first
                int winddirectionpos = Fmi::stoi(parameterMap.at("winddirection").at(stationtype));
                std::string windCompass;
                if (!data[s.fmisid][t][winddirectionpos].which())
                {
                  ts::Value missing;
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                }
                else
                {
                  if (special.first == "windcompass8")
                  {
                    windCompass =
                        windCompass8(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }
                  if (special.first == "windcompass16")
                  {
                    windCompass =
                        windCompass16(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }
                  if (special.first == "windcompass32")
                  {
                    windCompass =
                        windCompass32(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }

                  ts::Value windCompassValue = ts::Value(windCompass);
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, windCompassValue));
                }
              }
              else if (special.first.find("feelslike") != std::string::npos)
              {
                // Feels like - deduction. This ignores radiation, since it is
                // measured using
                // dedicated stations
                // dedicated stations
                int windpos = Fmi::stoi(parameterMap.at("windspeedms").at(stationtype));
                int rhpos = Fmi::stoi(parameterMap.at("relativehumidity").at(stationtype));
                int temppos = Fmi::stoi(parameterMap.at("temperature").at(stationtype));

                if (!data[s.fmisid][t][windpos].which() || !data[s.fmisid][t][rhpos].which() ||
                    !data[s.fmisid][t][temppos].which())
                {
                  ts::Value missing;
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                }
                else
                {
                  float temp = boost::get<double>(data[s.fmisid][t][temppos]);
                  float rh = boost::get<double>(data[s.fmisid][t][rhpos]);
                  float wind = boost::get<double>(data[s.fmisid][t][windpos]);

                  ts::Value feelslike =
                      ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, feelslike));
                }
              }
              else if (special.first.find("smartsymbol") != std::string::npos)
              {
                addSmartSymbolToTimeSeries(
                    pos, s, t, parameterMap, stationtype, data, timeSeriesColumns);
              }

              else
              {
                addSpecialParameterToTimeSeries(
                    special.first, timeSeriesColumns, tmpStations[s.fmisid], pos, stationtype, t);
              }
            }
          }
          else
          {
            for (const boost::local_time::local_date_time &t : tlist)
            {
              // Append weather parameters
              for (int pos : paramVector)
              {
                ts::Value val;
                if (!data[s.fmisid][t][pos].empty())
                {
                  val = data[s.fmisid][t][pos];
                }
                timeSeriesColumns->at(timeseriesPositions[pos]).push_back(ts::TimedValue(t, val));
              }
              // Append special parameters
              for (const pair<string, int> &special : specialPositions)
              {
                int pos = special.second;
                if (special.first.find("windcompass") != std::string::npos)
                {
                  // Have to get wind direction first
                  int winddirectionpos =
                      Fmi::stoi(parameterMap.at("winddirection").at(stationtype));
                  std::string windCompass;
                  if (!data[s.fmisid][t][winddirectionpos].which())
                  {
                    ts::Value missing;
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                  }
                  else
                  {
                    if (special.first == "windcompass8")
                    {
                      windCompass =
                          windCompass8(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }
                    if (special.first == "windcompass16")
                    {
                      windCompass =
                          windCompass16(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }
                    if (special.first == "windcompass32")
                    {
                      windCompass =
                          windCompass32(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }

                    ts::Value windCompassValue = ts::Value(windCompass);
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, windCompassValue));
                  }
                }
                else if (special.first.find("feelslike") != std::string::npos)
                {
                  // Feels like - deduction. This ignores radiation, since it is
                  // measured using
                  // dedicated stations
                  int windpos = Fmi::stoi(parameterMap.at("windspeedms").at(stationtype));
                  int rhpos = Fmi::stoi(parameterMap.at("relativehumidity").at(stationtype));
                  int temppos = Fmi::stoi(parameterMap.at("temperature").at(stationtype));

                  if (!data[s.fmisid][t][windpos].which() || !data[s.fmisid][t][rhpos].which() ||
                      !data[s.fmisid][t][temppos].which())
                  {
                    ts::Value missing;
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                  }
                  else
                  {
                    float temp = boost::get<double>(data[s.fmisid][t][temppos]);
                    float rh = boost::get<double>(data[s.fmisid][t][rhpos]);
                    float wind = boost::get<double>(data[s.fmisid][t][windpos]);

                    ts::Value feelslike =
                        ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, feelslike));
                  }
                }
                else if (special.first.find("smartsymbol") != std::string::npos)
                {
                  addSmartSymbolToTimeSeries(
                      pos, s, t, parameterMap, stationtype, data, timeSeriesColumns);
                }
                else
                {
                  addSpecialParameterToTimeSeries(
                      special.first, timeSeriesColumns, tmpStations[s.fmisid], pos, stationtype, t);
                }
              }
            }
          }
        }
      }
    }
    else
    {
      for (const Spine::Station &s : stations)
      {
        int fmisid = s.station_id;
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            dataWithStringParameterId[fmisid];
        for (const dataItemWithStringParameterId &item : stationData)
        {
          addParameterToTimeSeries(timeSeriesColumns,
                                   item,
                                   specialPositions,
                                   parameterNameMap,
                                   timeseriesPositionsString,
                                   parameterMap,
                                   stationtype,
                                   tmpStations[fmisid]);
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting cached data failed!");
  }
}

void SpatiaLite::addEmptyValuesToTimeSeries(
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const boost::local_time::local_date_time &obstime,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const std::string &stationtype,
    const Spine::Station &station)
{
  try
  {
    for (const auto &parameterNames : parameterNameMap)
    {
      std::string nameInDatabase = parameterNames.second;
      std::string nameInRequest = parameterNames.first;

      ts::Value val = ts::None();
      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .push_back(ts::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;
      if (special.first.find("windcompass") != std::string::npos ||
          special.first.find("feelslike") != std::string::npos ||
          special.first.find("smartsymbol") != std::string::npos)
      {
        ts::Value missing = ts::None();
        timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, missing));
      }
      else
      {
        addSpecialParameterToTimeSeries(
            special.first, timeSeriesColumns, station, pos, stationtype, obstime);
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLite::addParameterToTimeSeries(
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time, std::map<std::string, ts::Value> >
        &dataItem,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const ParameterMap &parameterMap,
    const std::string &stationtype,
    const Spine::Station &station)
{
  try
  {
    boost::local_time::local_date_time obstime = dataItem.first;
    std::map<std::string, ts::Value> data = dataItem.second;
    // Append weather parameters

    for (const auto &parameterNames : parameterNameMap)
    {
      std::string nameInRequest = parameterNames.first;
      std::string nameInDatabase = Fmi::ascii_tolower_copy(parameterNames.second);
      ts::Value val = ts::None();
      if (data.count(nameInDatabase) > 0)
      {
        val = data.at(nameInDatabase);
      }
      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .push_back(ts::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;
      if (special.first.find("windcompass") != std::string::npos)
      {
        // Have to get wind direction first
        std::string winddirectionpos = parameterMap.at("winddirection").at(stationtype);
        std::string windCompass;
        if (dataItem.second.count(winddirectionpos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          if (special.first == "windcompass8")
            windCompass = windCompass8(boost::get<double>(data.at(winddirectionpos)));

          else if (special.first == "windcompass16")
            windCompass = windCompass16(boost::get<double>(data.at(winddirectionpos)));

          else if (special.first == "windcompass32")
            windCompass = windCompass32(boost::get<double>(data.at(winddirectionpos)));

          ts::Value windCompassValue = ts::Value(windCompass);
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, windCompassValue));
        }
      }
      else if (special.first.find("feelslike") != std::string::npos)
      {
        // Feels like - deduction. This ignores radiation, since it is measured
        // using
        // dedicated stations
        std::string windpos = parameterMap.at("windspeedms").at(stationtype);
        std::string rhpos = parameterMap.at("relativehumidity").at(stationtype);
        std::string temppos = parameterMap.at("temperature").at(stationtype);

        if (data.count(windpos) == 0 || data.count(rhpos) == 0 || data.count(temppos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          float temp = boost::get<double>(data.at(temppos));
          float rh = boost::get<double>(data.at(rhpos));
          float wind = boost::get<double>(data.at(windpos));

          ts::Value feelslike = ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, feelslike));
        }
      }
      else if (special.first.find("smartsymbol") != std::string::npos)
      {
        std::string wawapos = parameterMap.at("wawa").at(stationtype);
        std::string totalcloudcoverpos = parameterMap.at("totalcloudcover").at(stationtype);
        std::string temppos = parameterMap.at("temperature").at(stationtype);
        if (data.count(wawapos) == 0 || data.count(totalcloudcoverpos) == 0 ||
            data.count(temppos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          float temp = boost::get<double>(data.at(temppos));
          int totalcloudcover = static_cast<int>(boost::get<double>(data.at(totalcloudcoverpos)));
          int wawa = static_cast<int>(boost::get<double>(data.at(wawapos)));
          double lat = station.latitude_out;
          double lon = station.longitude_out;

          ts::Value smartsymbol =
              ts::Value(*calcSmartsymbolNumber(wawa, totalcloudcover, temp, obstime, lat, lon));
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, smartsymbol));
        }
      }

      else
      {
        addSpecialParameterToTimeSeries(
            special.first, timeSeriesColumns, station, pos, stationtype, obstime);
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedFlashData(
    const Settings &settings, const ParameterMap &parameterMap, const Fmi::TimeZones &timezones)
{
  try
  {
    string stationtype = "flash";

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    map<string, int> timeseriesPositions;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      string name = p.name();
      boost::to_lower(name, std::locale::classic());
      if (not_special(p))
      {
        if (!parameterMap.at(name).at(stationtype).empty())
        {
          timeseriesPositions[parameterMap.at(name).at(stationtype)] = pos;
          param += parameterMap.at(name).at(stationtype) + ",";
        }
      }
      else
      {
        specialPositions[name] = pos;
      }
      pos++;
    }

    param = trimCommasFromEnd(param);

    string starttimeString = Fmi::to_iso_extended_string(settings.starttime);
    boost::replace_all(starttimeString, ",", ".");
    string endtimeString = Fmi::to_iso_extended_string(settings.endtime);
    boost::replace_all(endtimeString, ",", ".");

    std::string distancequery;

    std::string query;
    query =
        "SELECT DATETIME(stroke_time) AS stroke_time, "
        "stroke_time_fraction, flash_id, "
        "X(stroke_location) AS longitude, "
        "Y(stroke_location) AS latitude, " +
        param +
        " "
        "FROM flash_data flash "
        "WHERE flash.stroke_time >= '" +
        starttimeString +
        "' "
        "AND flash.stroke_time <= '" +
        endtimeString + "' ";

    if (!settings.taggedLocations.empty())
    {
      for (auto tloc : settings.taggedLocations)
      {
        if (tloc.loc->type == Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          // tloc.loc->radius in kilometers and PtDistWithin uses meters
          std::string radius = Fmi::to_string(tloc.loc->radius * 1000);
          query += " AND PtDistWithin((SELECT GeomFromText('POINT(" + lon + " " + lat +
                   ")', 4326)), flash.stroke_location, " + radius + ") = 1 ";
        }
        if (tloc.loc->type == Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);

          query += "AND MbrWithin(flash.stroke_location, BuildMbr(" + Fmi::to_string(bbox.xMin) +
                   ", " + Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMax) + ", " +
                   Fmi::to_string(bbox.yMax) + ")) ";
        }
      }
    }

    query += "ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction ASC;";

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Spine::TimeSeries::TimeSeriesVectorPtr(new Spine::TimeSeries::TimeSeriesVector);
    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    std::string stroke_time;
    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();

    {
      // Spine::ReadLock lock(write_mutex);
      sqlite3pp::query qry(itsDB, query.c_str());

      for (auto row : qry)
      {
        map<std::string, ts::Value> result;

        // These will be always in this order
        stroke_time = row.get<string>(0);
        // int stroke_time_fraction = row.get<int>(1);
        // int flash_id = row.get<int>(2);
        longitude = Fmi::stod(row.get<string>(3));
        latitude = Fmi::stod(row.get<string>(4));

        // Rest of the parameters in requested order
        for (int i = 5; i != qry.column_count(); ++i)
        {
          int data_type = row.column_type(i);
          ts::Value temp;
          if (data_type == SQLITE_TEXT)
          {
            temp = row.get<std::string>(i);
          }
          else if (data_type == SQLITE_FLOAT)
          {
            temp = row.get<double>(i);
          }
          else if (data_type == SQLITE_INTEGER)
          {
            temp = row.get<int>(i);
          }
          result[qry.column_name(i)] = temp;
        }

        boost::posix_time::ptime utctime = boost::posix_time::time_from_string(stroke_time);
        auto localtz = timezones.time_zone_from_string(settings.timezone);
        local_date_time localtime = local_date_time(utctime, localtz);

        std::pair<string, int> p;
        for (const auto &p : timeseriesPositions)
        {
          std::string name = p.first;
          int pos = p.second;

          ts::Value val = result[name];
          timeSeriesColumns->at(pos).push_back(ts::TimedValue(localtime, val));
        }
        for (const auto &p : specialPositions)
        {
          string name = p.first;
          int pos = p.second;
          if (name == "latitude")
          {
            ts::Value val = latitude;
            timeSeriesColumns->at(pos).push_back(ts::TimedValue(localtime, val));
          }
          if (name == "longitude")
          {
            ts::Value val = longitude;
            timeSeriesColumns->at(pos).push_back(ts::TimedValue(localtime, val));
          }
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLite::addSmartSymbolToTimeSeries(
    const int pos,
    const Spine::Station &s,
    const boost::local_time::local_date_time &time,
    const ParameterMap &parameterMap,
    const std::string &stationtype,
    const std::map<int, std::map<boost::local_time::local_date_time, std::map<int, ts::Value> > >
        &data,
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns)
{
  int wawapos = Fmi::stoi(parameterMap.at("wawa").at(stationtype));
  int totalcloudcoverpos = Fmi::stoi(parameterMap.at("totalcloudcover").at(stationtype));
  int temppos = Fmi::stoi(parameterMap.at("temperature").at(stationtype));

  auto dataItem = data.at(s.fmisid).at(time);

  if (!exists(dataItem, wawapos) || !exists(dataItem, totalcloudcoverpos) ||
      !exists(dataItem, temppos) || !dataItem.at(wawapos).which() ||
      !dataItem.at(totalcloudcoverpos).which() || !dataItem.at(temppos).which())
  {
    ts::Value missing;
    timeSeriesColumns->at(pos).push_back(ts::TimedValue(time, missing));
  }
  else
  {
    double temp = boost::get<double>(dataItem.at(temppos));
    int totalcloudcover = static_cast<int>(boost::get<double>(dataItem.at(totalcloudcoverpos)));
    int wawa = static_cast<int>(boost::get<double>(dataItem.at(wawapos)));
    double lat = s.latitude_out;
    double lon = s.longitude_out;
    ts::Value smartsymbol =
        ts::Value(*calcSmartsymbolNumber(wawa, totalcloudcover, temp, time, lat, lon));
    timeSeriesColumns->at(pos).push_back(ts::TimedValue(time, smartsymbol));
  }
}

void SpatiaLite::addSpecialParameterToTimeSeries(
    const std::string &paramname,
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const Spine::Station &station,
    const int pos,
    const std::string stationtype,
    const boost::local_time::local_date_time &obstime)
{
  try
  {
    if (paramname == "localtime")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, obstime));

    else if (paramname == "station_name" || paramname == "stationname")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.station_formal_name));

    else if (paramname == "fmisid")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.station_id));

    else if (paramname == "geoid")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.geoid));

    else if (paramname == "distance")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.distance));

    else if (paramname == "direction")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.stationDirection));

    else if (paramname == "stationary")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.stationary));

    else if (paramname == "lon" || paramname == "longitude")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.requestedLon));

    else if (paramname == "lat" || paramname == "latitude")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.requestedLat));

    else if (paramname == "stationlon" || paramname == "stationlongitude")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.longitude_out));

    else if (paramname == "stationlat" || paramname == "stationlatitude")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.latitude_out));

    else if (paramname == "elevation" || paramname == "station_elevation")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.station_elevation));

    else if (paramname == "wmo")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).push_back(
          ts::TimedValue(obstime, station.wmo > 0 ? station.wmo : missing));
    }
    else if (paramname == "lpnn")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).push_back(
          ts::TimedValue(obstime, station.lpnn > 0 ? station.lpnn : missing));
    }
    else if (paramname == "rwsid")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).push_back(
          ts::TimedValue(obstime, station.rwsid > 0 ? station.rwsid : missing));
    }
    else if (paramname == "sensor_no")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, 1));

    else if (paramname == "place")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, station.tag));

    else if (paramname == "model")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, stationtype));

    else if (paramname == "modtime")
      timeSeriesColumns->at(pos).push_back(ts::TimedValue(obstime, ""));

    else
    {
      std::string msg =
          "SpatiaLite::addSpecialParameterToTimeSeries : "
          "Unsupported special parameter '" +
          paramname + "'";

      Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg);
      throw exception;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::Stations SpatiaLite::findAllStationsFromGroups(
    const std::set<std::string> stationgroup_codes,
    const StationInfo &info,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime)
{
  try
  {
    return info.findStationsInGroup(stationgroup_codes, starttime, endtime);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Failed to find all stations in the given groups");
  }
}

Spine::Stations SpatiaLite::findStationsInsideArea(const Settings &settings,
                                                   const std::string &areaWkt,
                                                   const StationInfo &info)
{
  try
  {
    Spine::Stations stations;

    std::string sql = "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sql +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sql += "stations s ";
    }

    sql += "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sql += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sql += fmt::format("OR sg.group_code='{}' ", *it);
      sql += ") AND ";
    }

    sql += fmt::format(
        "Contains(GeomFromText('{}'), s.the_geom) AND ('{}' BETWEEN s.station_start AND "
        "s.station_end OR '{}' BETWEEN s.station_start AND s.station_end)",
        areaWkt,
        Fmi::to_iso_extended_string(settings.starttime),
        Fmi::to_iso_extended_string(settings.endtime));

    sqlite3pp::query qry(itsDB, sql.c_str());

    for (const auto &row : qry)
    {
      try
      {
        int geoid = row.get<int>(0);
        int station_id = row.get<int>(1);
        Spine::Station station = info.getStation(station_id, settings.stationgroup_codes);
        station.geoid = geoid;
        stations.push_back(station);
      }
      catch (const std::bad_cast &e)
      {
        cout << e.what() << endl;
        continue;
      }
      catch (...)
      {
        // Probably badly grouped stations in the database
        continue;
      }
    }

    return stations;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::Stations SpatiaLite::findStationsInsideBox(const Settings &settings, const StationInfo &info)
{
  try
  {
    Spine::Stations stations;

    std::string sql = "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sql +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sql += "stations s ";
    }

    sql += "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sql += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sql += fmt::format("OR sg.group_code='{}' ", *it);
      sql += ") AND ";
    }

    sql += fmt::format(
        "ST_EnvIntersects(s.the_geom,{:.10f},{:.10f},{:.10f},{:.10f}) AND ('{}' BETWEEN "
        "s.station_start AND "
        "s.station_end OR '{}' BETWEEN s.station_start AND s.station_end)",
        settings.boundingBox.at("minx"),
        settings.boundingBox.at("miny"),
        settings.boundingBox.at("maxx"),
        settings.boundingBox.at("maxy"),
        Fmi::to_iso_extended_string(settings.starttime),
        Fmi::to_iso_extended_string(settings.endtime));

    sqlite3pp::query qry(itsDB, sql.c_str());

    for (const auto &row : qry)
    {
      try
      {
        int geoid = row.get<int>(0);
        int station_id = row.get<int>(1);
        Spine::Station station = info.getStation(station_id, settings.stationgroup_codes);
        station.geoid = geoid;
        stations.push_back(station);
      }
      catch (const std::bad_cast &e)
      {
        cout << e.what() << endl;
        continue;
      }
      catch (...)
      {
        // Probably badly grouped stations in the database
        continue;
      }
    }

    return stations;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLite::fillMissing(Spine::Station &s, const std::set<std::string> &stationgroup_codes)
{
  try
  {
    bool missingStationId = (s.station_id == -1 or s.station_id == 0);
    bool missingFmisId = (s.fmisid == -1 or s.fmisid == 0);
    bool missingWmoId = (s.wmo == -1);
    bool missingGeoId = (s.geoid == -1);
    bool missingLpnnId = (s.lpnn == -1);
    bool missingLongitude = (s.longitude_out == std::numeric_limits<double>::max());
    bool missingLatitude = (s.latitude_out == std::numeric_limits<double>::max());
    bool missingStationFormalName = (s.station_formal_name.empty());

    // Can not fill the missing valus if all are missing.
    if (missingStationId and missingFmisId and missingWmoId and missingGeoId)
      return false;

    std::string sql =
        "SELECT s.fmisid, s.wmo, s.geoid, s.lpnn, X(s.the_geom) AS lon, Y(s.the_geom) AS lat, "
        "s.station_formal_name FROM ";

    if (not stationgroup_codes.empty())
    {
      sql +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sql += "stations s ";
    }

    sql += " WHERE";

    if (not stationgroup_codes.empty())
    {
      auto it = stationgroup_codes.begin();
      sql += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != stationgroup_codes.end(); it++)
        sql += fmt::format("OR sg.group_code='{}' ", *it);
      sql += ") AND ";
    }

    // Use the first id that is not missing.
    if (not missingStationId)
      sql += fmt::format(" s.fmisid={}", s.station_id);
    else if (not missingFmisId)
      sql += fmt::format(" s.fmisid={}", s.fmisid);
    else if (not missingWmoId)
      sql += fmt::format(" s.wmo={}", s.wmo);
    else if (not missingGeoId)
      sql += fmt::format(" s.geoid={}", s.geoid);
    else if (not missingLpnnId)
      sql += fmt::format(" s.lpnn={}", s.lpnn);
    else
      return false;

    // There might be multiple locations for a station.
    sql += " AND DATETIME('now') BETWEEN s.station_start AND s.station_end";

    boost::optional<int> fmisid;
    boost::optional<int> wmo;
    boost::optional<int> geoid;
    boost::optional<int> lpnn;
    boost::optional<double> longitude_out;
    boost::optional<double> latitude_out;
    boost::optional<std::string> station_formal_name;

    // We need only the first one (ID values are unique).
    sql += " LIMIT 1";

    // Executing the search
    sqlite3pp::query qry(itsDB, sql.c_str());
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
    {
      fmisid = (*iter).get<int>(0);
      wmo = (*iter).get<int>(1);
      geoid = (*iter).get<int>(2);
      lpnn = (*iter).get<int>(3);
      longitude_out = (*iter).get<double>(4);
      latitude_out = (*iter).get<double>(5);
      station_formal_name = (*iter).get<std::string>(6);
    }
    // Checking the default value of station_id and then data do the data
    // population.
    if (fmisid)
    {
      if (missingStationId)
        s.station_id = (fmisid ? fmisid.get() : -1);
      if (missingFmisId)
        s.fmisid = (fmisid ? fmisid.get() : -1);
      if (missingWmoId)
        s.wmo = (wmo ? wmo.get() : -1);
      if (missingGeoId)
        s.geoid = (geoid ? geoid.get() : -1);
      if (missingLpnnId)
        s.lpnn = (lpnn ? lpnn.get() : -1);
      if (missingLongitude)
        s.longitude_out =
            (longitude_out ? longitude_out.get() : std::numeric_limits<double>::max());
      if (missingLatitude)
        s.latitude_out = (latitude_out ? latitude_out.get() : std::numeric_limits<double>::max());
      if (missingStationFormalName)
        s.station_formal_name = (station_formal_name ? station_formal_name.get() : "");
    }
    else
    {
      return false;
    }

    return true;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool SpatiaLite::getStationById(Spine::Station &station,
                                int station_id,
                                const std::set<std::string> &stationgroup_codes)
{
  try
  {
    Spine::Station s;
    s.station_id = station_id;
    s.fmisid = -1;
    s.wmo = -1;
    s.geoid = -1;
    s.lpnn = -1;
    s.longitude_out = std::numeric_limits<double>::max();
    s.latitude_out = std::numeric_limits<double>::max();
    if (not fillMissing(s, stationgroup_codes))
      return false;
    station = s;
    return true;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

FlashCounts SpatiaLite::getFlashCount(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime,
                                      const Spine::TaggedLocationList &locations)
{
  try
  {
    FlashCounts flashcounts;
    flashcounts.flashcount = 0;
    flashcounts.strokecount = 0;
    flashcounts.iccount = 0;

    std::string sqltemplate =
        "SELECT "
        "IFNULL(SUM(CASE WHEN flash.multiplicity > 0 "
        "THEN 1 ELSE 0 END), 0) AS flashcount, "
        "IFNULL(SUM(CASE WHEN flash.multiplicity = 0 "
        "THEN 1 ELSE 0 END), 0) AS strokecount, "
        "IFNULL(SUM(CASE WHEN flash.cloud_indicator = 1 "
        "THEN 1 ELSE 0 END), 0) AS iccount "
        " FROM flash_data flash "
        "WHERE flash.stroke_time BETWEEN '" +
        Fmi::to_iso_extended_string(starttime) + "' AND '" + Fmi::to_iso_extended_string(endtime) +
        "'";

    if (!locations.empty())
    {
      for (auto tloc : locations)
      {
        if (tloc.loc->type == Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          // tloc.loc->radius in kilometers and PtDistWithin uses meters
          std::string radius = Fmi::to_string(tloc.loc->radius * 1000);
          sqltemplate += " AND PtDistWithin((SELECT GeomFromText('POINT(" + lon + " " + lat +
                         ")', 4326)), flash.stroke_location, " + radius + ") = 1 ";
        }
        if (tloc.loc->type == Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);

          sqltemplate += "AND MbrWithin(flash.stroke_location, BuildMbr(" +
                         Fmi::to_string(bbox.xMin) + ", " + Fmi::to_string(bbox.yMin) + ", " +
                         Fmi::to_string(bbox.xMax) + ", " + Fmi::to_string(bbox.yMax) + ")) ";
        }
      }
    }

    sqltemplate += ";";

    sqlite3pp::query qry(itsDB, sqltemplate.c_str());
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
    {
      flashcounts.flashcount = (*iter).get<int>(0);
      flashcounts.strokecount = (*iter).get<int>(1);
      flashcounts.iccount = (*iter).get<int>(2);
    }

    return flashcounts;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const ParameterMap &parameterMap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = settings.stationtype;

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    std::string qstations;
    map<int, Spine::Station> tmpStations;
    for (const Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      qstations += Fmi::to_string(s.station_id) + ",";
    }
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<string, int> timeseriesPositions;
    std::map<std::string, std::string> parameterNameMap;
    map<string, int> specialPositions;

    std::string param;

    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        std::string nameInRequest = p.name();
        Fmi::ascii_tolower(nameInRequest);
        removePrefix(nameInRequest, "qc_");

        std::string shortname = parseParameterName(nameInRequest);

        if (!parameterMap.at(shortname).at(stationtype).empty())
        {
          std::string nameInDatabase = parameterMap.at(shortname).at(stationtype);
          timeseriesPositions[nameInRequest] = pos;
          parameterNameMap[nameInRequest] = nameInDatabase;

          nameInDatabase = parseParameterName(nameInDatabase);
          Fmi::ascii_toupper(nameInDatabase);
          param += "'" + nameInDatabase + "',";
        }
      }
      else
      {
        string name = p.name();
        Fmi::ascii_tolower(name);

        if (name.find("windcompass") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("winddirection").at(stationtype)) +
                   "',";
          timeseriesPositions[parameterMap.at("winddirection").at(stationtype)] = pos;
          specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("windspeedms").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("relativehumidity").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("temperature").at(stationtype)) + "',";
          specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          param += "'" + Fmi::ascii_toupper_copy(parameterMap.at("wawa").at(stationtype)) + "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("totalcloudcover").at(stationtype)) +
                   "', '" +
                   Fmi::ascii_toupper_copy(parameterMap.at("temperature").at(stationtype)) + "',";
          specialPositions[name] = pos;
        }
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns(
        new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    param = trimCommasFromEnd(param);

    std::string query;
    if (settings.latest)
    {
      query =
          "SELECT data.fmisid AS fmisid, MAX(data.obstime) AS obstime, "
          "loc.latitude, loc.longitude, loc.elevation, "
          "parameter, value, sensor_no "
          "FROM weather_data_qc data JOIN locations loc ON (data.fmisid = "
          "loc.fmisid) "
          "WHERE data.fmisid IN (" +
          qstations +
          ") "
          "AND data.obstime >= '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND data.obstime <= '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + param +
          ") "
          "GROUP BY data.fmisid, data.parameter, data.sensor_no, "
          "loc.location_id, "
          "loc.location_end, "
          "loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }
    else
    {
      query =
          "SELECT data.fmisid AS fmisid, data.obstime AS obstime, "
          "loc.latitude, loc.longitude, loc.elevation, "
          "parameter, value, sensor_no "
          "FROM weather_data_qc data JOIN locations loc ON (data.fmisid = "
          "loc.fmisid) "
          "WHERE data.fmisid IN (" +
          qstations +
          ") "
          "AND data.obstime >= '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND data.obstime <= '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + param +
          ") "
          "GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no, loc.location_id, "
          "loc.location_end, loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::posix_time::ptime> obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<std::string> > parametersAll;
    std::vector<boost::optional<double> > data_valuesAll;
    std::vector<boost::optional<double> > sensor_nosAll;

    sqlite3pp::query qry(itsDB, query.c_str());

    for (sqlite3pp::query::iterator iter = qry.begin(); iter != qry.end(); ++iter)
    {
      boost::optional<int> fmisid = (*iter).get<int>(0);
      boost::posix_time::ptime obstime = parse_sqlite_time(iter, 1);
      boost::optional<double> latitude = (*iter).get<double>(2);
      boost::optional<double> longitude = (*iter).get<double>(3);
      boost::optional<double> elevation = (*iter).get<double>(4);
      boost::optional<std::string> parameter = (*iter).get<std::string>(5);
      boost::optional<double> data_value;
      if ((*iter).column_type(6) != SQLITE_NULL)
        data_value = (*iter).get<double>(6);
      boost::optional<double> sensor_no = (*iter).get<double>(7);

      fmisidsAll.push_back(fmisid);
      obstimesAll.push_back(obstime);
      latitudesAll.push_back(latitude);
      longitudesAll.push_back(longitude);
      elevationsAll.push_back(elevation);
      parametersAll.push_back(parameter);
      data_valuesAll.push_back(data_value);
      sensor_nosAll.push_back(sensor_no);
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > > data;

    for (const auto &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];

      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      std::string parameter = *parametersAll[i];
      int sensor_no = *sensor_nosAll[i];
      Fmi::ascii_tolower(parameter);
      if (sensor_no > 1)
      {
        parameter += "_" + Fmi::to_string(sensor_no);
      }

      ts::Value val;
      if (data_valuesAll[i])
        val = ts::Value(*data_valuesAll[i]);

      data[fmisid][obstime][parameter] = val;
      if (sensor_no == 1)
      {
        parameter += "_1";
        data[fmisid][obstime][parameter] = val;
      }
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value> > dataItem;

    if (!settings.latest && !timeSeriesOptions.all())
    {
      auto tlist = Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

      for (const Spine::Station &s : stations)
      {
        if (data.count(s.fmisid) == 0)
        {
          continue;
        }
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            data.at(s.fmisid);
        for (const boost::local_time::local_date_time &t : tlist)
        {
          if (stationData.count(t) > 0)
          {
            dataItem item = std::make_pair(t, stationData.at(t));
            addParameterToTimeSeries(timeSeriesColumns,
                                     item,
                                     specialPositions,
                                     parameterNameMap,
                                     timeseriesPositions,
                                     parameterMap,
                                     stationtype,
                                     tmpStations.at(s.fmisid));
          }
          else
          {
            addEmptyValuesToTimeSeries(timeSeriesColumns,
                                       t,
                                       specialPositions,
                                       parameterNameMap,
                                       timeseriesPositions,
                                       stationtype,
                                       tmpStations.at(s.fmisid));
          }
        }
      }
    }
    else
    {
      for (const Spine::Station &s : stations)
      {
        int fmisid = s.station_id;
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            data[fmisid];
        for (const dataItem &item : stationData)
        {
          addParameterToTimeSeries(timeSeriesColumns,
                                   item,
                                   specialPositions,
                                   parameterNameMap,
                                   timeseriesPositions,
                                   parameterMap,
                                   stationtype,
                                   tmpStations[fmisid]);
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedData(
    Spine::Stations &stations,
    Settings &settings,
    ParameterMap &parameterMap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype;
    if (settings.stationtype == "opendata_buoy" || settings.stationtype == "opendata_mareograph")
    {
      stationtype = settings.stationtype;
    }
    else
    {
      stationtype = "opendata";
    }

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    std::string qstations;
    map<int, Spine::Station> tmpStations;
    for (const Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      qstations += Fmi::to_string(s.station_id) + ",";
    }
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<int, int> timeseriesPositions;
    map<std::string, int> timeseriesPositionsString;
    std::map<std::string, std::string> parameterNameMap;
    vector<int> paramVector;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        string name = p.name();
        Fmi::ascii_tolower(name);
        removePrefix(name, "qc_");

        if (!parameterMap[name][stationtype].empty())
        {
          timeseriesPositions[Fmi::stoi(parameterMap[name][stationtype])] = pos;
          timeseriesPositionsString[name] = pos;
          parameterNameMap[name] = parameterMap[name][stationtype];
          paramVector.push_back(Fmi::stoi(parameterMap[name][stationtype]));
          param += parameterMap[name][stationtype] + ",";
        }
      }
      else
      {
        string name = p.name();
        Fmi::ascii_tolower(name);

        if (name.find("windcompass") != std::string::npos)
        {
          param += parameterMap["winddirection"][stationtype] + ",";
          timeseriesPositions[Fmi::stoi(parameterMap["winddirection"][stationtype])] = pos;
          specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          param += parameterMap["windspeedms"][stationtype] + "," +
                   parameterMap["relativehumidity"][stationtype] + "," +
                   parameterMap["temperature"][stationtype] + ",";
          specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          param += parameterMap["wawa"][stationtype] + "," +
                   parameterMap["totalcloudcover"][stationtype] + "," +
                   parameterMap["temperature"][stationtype] + ",";
          specialPositions[name] = pos;
        }
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    param = trimCommasFromEnd(param);

    std::string query =
        "SELECT data.fmisid AS fmisid, data.data_time AS obstime, "
        "loc.latitude, loc.longitude, loc.elevation, "
        "measurand_id, data_value "
        "FROM observation_data data JOIN locations loc ON (data.fmisid = "
        "loc.fmisid) "
        "WHERE data.fmisid IN (" +
        qstations +
        ") "
        "AND data.data_time >= '" +
        Fmi::to_iso_extended_string(settings.starttime) + "' AND data.data_time <= '" +
        Fmi::to_iso_extended_string(settings.endtime) + "' AND data.measurand_id IN (" + param +
        ") "
        "AND data.measurand_no = 1 "
        "GROUP BY data.fmisid, data.data_time, data.measurand_id, "
        "loc.location_id, "
        "loc.location_end, "
        "loc.latitude, loc.longitude, loc.elevation "
        "ORDER BY fmisid ASC, obstime ASC";
    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::posix_time::ptime> obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<int> > measurand_idsAll;
    std::vector<boost::optional<double> > data_valuesAll;

    sqlite3pp::query qry(itsDB, query.c_str());
    for (sqlite3pp::query::iterator iter = qry.begin(); iter != qry.end(); ++iter)
    {
      boost::optional<int> fmisid = (*iter).get<int>(0);
      boost::posix_time::ptime obstime = parse_sqlite_time(iter, 1);
      boost::optional<double> latitude = (*iter).get<double>(2);
      boost::optional<double> longitude = (*iter).get<double>(3);
      boost::optional<double> elevation = (*iter).get<double>(4);
      boost::optional<int> measurand_id = (*iter).get<int>(5);
      boost::optional<double> data_value;
      if ((*iter).column_type(6) != SQLITE_NULL)
        data_value = (*iter).get<double>(6);
      fmisidsAll.push_back(fmisid);
      obstimesAll.push_back(obstime);
      latitudesAll.push_back(latitude);
      longitudesAll.push_back(longitude);
      elevationsAll.push_back(elevation);
      measurand_idsAll.push_back(measurand_id);
      data_valuesAll.push_back(data_value);
    }

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Spine::TimeSeries::TimeSeriesVectorPtr(new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<int, ts::Value> > > data;
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > >
        dataWithStringParameterId;

    for (const auto &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations[fmisid].timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      int measurand_id = *measurand_idsAll[i];

      ts::Value val;
      if (data_valuesAll[i])
        val = ts::Value(*data_valuesAll[i]);

      data[fmisid][obstime][measurand_id] = val;
      dataWithStringParameterId[fmisid][obstime][Fmi::to_string(measurand_id)] = val;
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value> >
        dataItemWithStringParameterId;

    // Accept all time steps
    if (timeSeriesOptions.all() && !settings.latest)
    {
      for (const Spine::Station &s : stations)
      {
        int fmisid = s.station_id;
        map<boost::local_time::local_date_time, map<std::string, ts::Value> > stationData =
            dataWithStringParameterId[fmisid];
        for (const dataItemWithStringParameterId &item : stationData)
        {
          addParameterToTimeSeries(timeSeriesColumns,
                                   item,
                                   specialPositions,
                                   parameterNameMap,
                                   timeseriesPositionsString,
                                   parameterMap,
                                   stationtype,
                                   tmpStations[fmisid]);
        }
      }
    }
    else
    {
      // Accept only generated time series
      auto tlist = Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      {
        for (const Spine::Station &s : stations)

          if (settings.latest)
          {
            // Get only the last time step if there is many
            boost::local_time::local_date_time t(boost::local_time::not_a_date_time);
            if (!data[s.fmisid].empty())
            {
              t = data[s.fmisid].rbegin()->first;
            }
            else
            {
              continue;
            }

            // Append weather parameters
            for (int pos : paramVector)
            {
              ts::Value val = data[s.fmisid][t][pos];
              timeSeriesColumns->at(timeseriesPositions[pos]).push_back(ts::TimedValue(t, val));
            }
            // Append special parameters
            for (const auto &special : specialPositions)
            {
              int pos = special.second;
              if (special.first.find("windcompass") != std::string::npos)
              {
                // Have to get wind direction first
                int winddirectionpos = Fmi::stoi(parameterMap["winddirection"][stationtype]);
                std::string windCompass;
                if (!data[s.fmisid][t][winddirectionpos].which())
                {
                  ts::Value missing;
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                }
                else
                {
                  if (special.first == "windcompass8")
                  {
                    windCompass =
                        windCompass8(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }
                  if (special.first == "windcompass16")
                  {
                    windCompass =
                        windCompass16(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }
                  if (special.first == "windcompass32")
                  {
                    windCompass =
                        windCompass32(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                  }

                  ts::Value windCompassValue = ts::Value(windCompass);
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, windCompassValue));
                }
              }
              else if (special.first.find("feelslike") != std::string::npos)
              {
                // Feels like - deduction. This ignores radiation, since it is
                // measured using
                // dedicated stations
                // dedicated stations
                int windpos = boost::lexical_cast<int>(parameterMap["windspeedms"][stationtype]);
                int rhpos = boost::lexical_cast<int>(parameterMap["relativehumidity"][stationtype]);
                int temppos = boost::lexical_cast<int>(parameterMap["temperature"][stationtype]);

                if (!data[s.fmisid][t][windpos].which() || !data[s.fmisid][t][rhpos].which() ||
                    !data[s.fmisid][t][temppos].which())
                {
                  ts::Value missing;
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                }
                else
                {
                  float temp = boost::get<double>(data[s.fmisid][t][temppos]);
                  float rh = boost::get<double>(data[s.fmisid][t][rhpos]);
                  float wind = boost::get<double>(data[s.fmisid][t][windpos]);

                  ts::Value feelslike =
                      ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, feelslike));
                }
              }
              else if (special.first.find("smartsymbol") != std::string::npos)
              {
                int wawapos = Fmi::stoi(parameterMap.at("wawa").at(stationtype));
                int totalcloudcoverpos =
                    Fmi::stoi(parameterMap.at("totalcloudcover").at(stationtype));
                int temppos = Fmi::stoi(parameterMap.at("temperature").at(stationtype));

                if (!data[s.fmisid][t][wawapos].which() ||
                    !data[s.fmisid][t][totalcloudcoverpos].which() ||
                    !data[s.fmisid][t][temppos].which())
                {
                  ts::Value missing;
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                }
                else
                {
                  double temp = boost::get<double>(data[s.fmisid][t][temppos]);
                  int totalcloudcover =
                      static_cast<int>(boost::get<double>(data[s.fmisid][t][totalcloudcoverpos]));
                  int wawa = static_cast<int>(boost::get<double>(data[s.fmisid][t][wawapos]));
                  double lat = s.latitude_out;
                  double lon = s.longitude_out;
                  ts::Value smartsymbol =
                      ts::Value(*calcSmartsymbolNumber(wawa, totalcloudcover, temp, t, lat, lon));
                  timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, smartsymbol));
                }
              }
              else
              {
                addSpecialParameterToTimeSeries(
                    special.first, timeSeriesColumns, tmpStations[s.fmisid], pos, stationtype, t);
              }
            }
          }
          else
          {
            for (const boost::local_time::local_date_time &t : tlist)
            {
              // Append weather parameters
              for (int pos : paramVector)
              {
                ts::Value val;
                if (!data[s.fmisid][t][pos].empty())
                {
                  val = data[s.fmisid][t][pos];
                }
                timeSeriesColumns->at(timeseriesPositions[pos]).push_back(ts::TimedValue(t, val));
              }
              // Append special parameters
              for (const pair<string, int> &special : specialPositions)
              {
                int pos = special.second;
                if (special.first.find("windcompass") != std::string::npos)
                {
                  // Have to get wind direction first
                  int winddirectionpos = Fmi::stoi(parameterMap["winddirection"][stationtype]);
                  std::string windCompass;
                  if (!data[s.fmisid][t][winddirectionpos].which())
                  {
                    ts::Value missing;
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                  }
                  else
                  {
                    if (special.first == "windcompass8")
                    {
                      windCompass =
                          windCompass8(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }
                    if (special.first == "windcompass16")
                    {
                      windCompass =
                          windCompass16(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }
                    if (special.first == "windcompass32")
                    {
                      windCompass =
                          windCompass32(boost::get<double>(data[s.fmisid][t][winddirectionpos]));
                    }

                    ts::Value windCompassValue = ts::Value(windCompass);
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, windCompassValue));
                  }
                }
                else if (special.first.find("feelslike") != std::string::npos)
                {
                  // Feels like - deduction. This ignores radiation, since it is
                  // measured using
                  // dedicated stations
                  int windpos = boost::lexical_cast<int>(parameterMap["windspeedms"][stationtype]);
                  int rhpos =
                      boost::lexical_cast<int>(parameterMap["relativehumidity"][stationtype]);
                  int temppos = boost::lexical_cast<int>(parameterMap["temperature"][stationtype]);

                  if (!data[s.fmisid][t][windpos].which() || !data[s.fmisid][t][rhpos].which() ||
                      !data[s.fmisid][t][temppos].which())
                  {
                    ts::Value missing;
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                  }
                  else
                  {
                    float temp = boost::get<double>(data[s.fmisid][t][temppos]);
                    float rh = boost::get<double>(data[s.fmisid][t][rhpos]);
                    float wind = boost::get<double>(data[s.fmisid][t][windpos]);

                    ts::Value feelslike =
                        ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, feelslike));
                  }
                }
                else if (special.first.find("smartsymbol") != std::string::npos)
                {
                  int wawapos = Fmi::stoi(parameterMap.at("wawa").at(stationtype));
                  int totalcloudcoverpos =
                      Fmi::stoi(parameterMap.at("totalcloudcover").at(stationtype));
                  int temppos = Fmi::stoi(parameterMap.at("temperature").at(stationtype));

                  if (!data[s.fmisid][t][wawapos].which() ||
                      !data[s.fmisid][t][totalcloudcoverpos].which() ||
                      !data[s.fmisid][t][temppos].which())
                  {
                    ts::Value missing;
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, missing));
                  }
                  else
                  {
                    double temp = boost::get<double>(data[s.fmisid][t][temppos]);
                    int totalcloudcover =
                        static_cast<int>(boost::get<double>(data[s.fmisid][t][totalcloudcoverpos]));
                    int wawa = static_cast<int>(boost::get<double>(data[s.fmisid][t][wawapos]));
                    double lat = s.latitude_out;
                    double lon = s.longitude_out;

                    ts::Value smartsymbol =
                        ts::Value(*calcSmartsymbolNumber(wawa, totalcloudcover, temp, t, lat, lon));
                    timeSeriesColumns->at(pos).push_back(ts::TimedValue(t, smartsymbol));
                  }
                }
                else
                {
                  addSpecialParameterToTimeSeries(
                      special.first, timeSeriesColumns, tmpStations[s.fmisid], pos, stationtype, t);
                }
              }
            }
          }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void SpatiaLite::createObservablePropertyTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS observable_property ("
        "measurandId INTEGER,"
        "language TEXT"
        "measurandCode TEXT,"
        "observablePropertyId TEXT,"
        "observablePropertyLabel TEXT,"
        "basePhenomenon TEXT,"
        "uom TEXT,"
        "statisticalMeasureId TEXT,"
        "statisticalFunction TEXT,"
        "aggregationTimePeriod TEXT,"
        "gmlId TEXT)");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of observable_property table failed!");
  }
}

boost::shared_ptr<std::vector<ObservableProperty> > SpatiaLite::getObservableProperties(
    std::vector<std::string> &parameters,
    const std::string language,
    const std::map<std::string, std::map<std::string, std::string> > &parameterMap,
    const std::string &stationType)
{
  boost::shared_ptr<std::vector<ObservableProperty> > data(new std::vector<ObservableProperty>());
  try
  {
    // Solving measurand id's for valid parameter aliases.
    std::multimap<int, std::string> parameterIDs;
    solveMeasurandIds(parameters, parameterMap, stationType, parameterIDs);
    // Return empty list if some parameters are defined and any of those is
    // valid.
    if (parameterIDs.empty())
      return data;

    std::string sqlStmt =
        "SELECT "
        "measurandId,"
        "measurandCode,"
        "observablePropertyId,"
        "observablePropertyLabel,"
        "basePhenomenon,"
        "uom,"
        "statisticalMeasureId,"
        "statisticalFunction,"
        "aggregationTimePeriod,"
        "gmlId FROM observable_property WHERE language = '" +
        language + "'";

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());
    for (const auto &row : qry)
    {
      int measurandId = row.get<int>(0);

      // Multiple parameter name aliases may use a same measurand id (e.g. t2m
      // and temperature)
      std::pair<std::multimap<int, std::string>::iterator,
                std::multimap<int, std::string>::iterator>
          r = parameterIDs.equal_range(measurandId);
      for (std::multimap<int, std::string>::iterator it = r.first; it != r.second; ++it)
      {
        ObservableProperty op;
        op.measurandId = Fmi::to_string(measurandId);
        op.measurandCode = row.get<string>(1);
        op.observablePropertyId = row.get<string>(2);
        op.observablePropertyLabel = row.get<string>(3);
        op.basePhenomenon = row.get<string>(4);
        op.uom = row.get<string>(5);
        op.statisticalMeasureId = row.get<string>(6);
        op.statisticalFunction = row.get<string>(7);
        op.aggregationTimePeriod = row.get<string>(8);
        op.gmlId = it->second;
        data->push_back(op);
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }

  return data;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
