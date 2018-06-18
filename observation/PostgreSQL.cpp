#include "PostgreSQL.h"
#include "ObservableProperty.h"
#include "PostgreSQLCacheParameters.h"
#include <fmt/format.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Exception.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesOutput.h>
#include <chrono>
#include <iostream>
#include <thread>

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

Spine::MutexType stations_write_mutex;
Spine::MutexType locations_write_mutex;
Spine::MutexType observation_data_write_mutex;
Spine::MutexType weather_data_qc_write_mutex;
Spine::MutexType flash_data_write_mutex;

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

};  // namespace

PostgreSQL::PostgreSQL(const PostgreSQLCacheParameters &options)
    : itsShutdownRequested(false),
      itsMaxInsertSize(options.maxInsertSize),
      itsDataInsertCache(options.dataInsertCacheSize),
      itsWeatherQCInsertCache(options.weatherDataQCInsertCacheSize),
      itsFlashInsertCache(options.flashInsertCacheSize)
{
  try
  {
    srid = "4326";

    itsDB.open(options.postgresql.host,
               options.postgresql.username,
               options.postgresql.password,
               options.postgresql.database,
               options.postgresql.encoding,
               options.postgresql.port,
               options.postgresql.connect_timeout);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    if (itsDB.isConnected())
    {
      //      itsDB.setDebug(true);
      // Fetch data types
      std::map<unsigned int, std::string> itsPostgreDataTypes;
      pqxx::result result_set = itsDB.executeNonTransaction("select typname,oid from pg_type");
      for (auto row : result_set)
      {
        std::string datatype = row[0].as<string>();
        unsigned int oid = row[1].as<unsigned int>();
        itsPostgreDataTypes.insert(std::make_pair(oid, datatype));
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Connecting to database failed!");
  }
}

PostgreSQL::~PostgreSQL() {}

void PostgreSQL::createTables()
{
  try
  {
    // No locking needed during initialization phase
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

void PostgreSQL::shutdown()
{
  std::cout << "  -- Shutdown requested (PostgreSQL)\n";
  itsShutdownRequested = true;
}

/*
 */
void PostgreSQL::createLocationsTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS locations("
        "fmisid INTEGER NOT NULL PRIMARY KEY, "
        "location_id INTEGER NOT NULL,"
        "country_id INTEGER NOT NULL,"
        "location_start timestamp, "
        "location_end timestamp, "
        "longitude REAL, "
        "latitude REAL, "
        "x REAL, "
        "y REAL, "
        "elevation REAL, "
        "time_zone_name TEXT, "
        "time_zone_abbrev TEXT, "
        "last_modified timestamp default now())");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of locations table failed!");
  }
}

void PostgreSQL::createStationGroupsTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS station_groups ("
        "group_id INTEGER NOT NULL PRIMARY KEY, "
        "group_code TEXT, "
        "last_modified timestamp default now())");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of station_groups table failed!");
  }
}

void PostgreSQL::createGroupMembersTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS group_members ("
        "group_id INTEGER NOT NULL, "
        "fmisid INTEGER NOT NULL, "
        "last_modified timestamp default now(), "
        "CONSTRAINT fk_station_groups FOREIGN KEY (group_id) "
        "REFERENCES station_groups "
        "(group_id)); CREATE INDEX IF NOT EXISTS gm_sg_idx ON group_members "
        "(group_id,fmisid);");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of group_members table failed!");
  }
}

void PostgreSQL::createObservationDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS observation_data("
        "fmisid INTEGER NOT NULL, "
        "data_time timestamp NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "last_modified timestamp default now(), "
        "PRIMARY KEY (data_time, fmisid, measurand_id, producer_id, "
        "measurand_no)); CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON "
        "observation_data(data_time);");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of observation_data table failed!");
  }
}

void PostgreSQL::createWeatherDataQCTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS weather_data_qc ("
        "fmisid INTEGER NOT NULL, "
        "obstime timestamp NOT NULL, "
        "parameter TEXT NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "value REAL NOT NULL, "
        "flag INTEGER NOT NULL, "
        "last_modified timestamp default now(), "
        "PRIMARY KEY (obstime, fmisid, parameter, sensor_no)); CREATE INDEX IF "
        "NOT EXISTS weather_data_qc_obstime_idx ON "
        "weather_data_qc(obstime)");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of weather_data_qc table failed!");
  }
}

void PostgreSQL::createFlashDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS flash_data("
        "stroke_time timestamp NOT NULL, "
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
        "last_modified timestamp default now(), "
        "PRIMARY KEY (stroke_time, stroke_time_fraction, flash_id)); CREATE "
        "INDEX IF NOT EXISTS flash_data_stroke_time_idx ON "
        "flash_data(stroke_time)");

    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='flash_data'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('flash_data', 'stroke_location', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS flash_data_gix ON flash_data USING GIST (stroke_location)");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of flash_data table failed!");
  }
}

void PostgreSQL::createStationTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS stations("
        "fmisid INTEGER NOT NULL, "
        "wmo INTEGER, "
        "geoid INTEGER, "
        "lpnn INTEGER, "
        "rwsid INTEGER, "
        "station_start timestamp, "
        "station_end timestamp, "
        "station_formal_name TEXT NOT NULL, "
        "last_modified timestamp default now(), "
        "PRIMARY KEY (fmisid, geoid, station_start, station_end))");

    pqxx::result result_set =
        itsDB.executeNonTransaction("SELECT * FROM geometry_columns WHERE f_table_name='stations'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('stations', 'the_geom', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS stations_gix ON stations USING GIST (the_geom)");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of stations table failed!");
  }
}

size_t PostgreSQL::selectCount(const std::string &queryString)
{
  try
  {
    size_t count = 0;
    pqxx::result result_set = itsDB.executeNonTransaction(queryString);

    if (!result_set.empty())
    {
      pqxx::result::const_iterator row = result_set.begin();
      if (!row[0].is_null())
        count = row[0].as<size_t>();
    }
    return count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, queryString + " query failed!");
  }
}  // namespace Observation

size_t PostgreSQL::getStationCount()
{
  return selectCount("SELECT COUNT(*) FROM stations");
}

boost::posix_time::ptime PostgreSQL::getTime(const std::string &timeQuery) const
{
  try
  {
    boost::posix_time::ptime ret;

    std::string sqlStmt = "SELECT EXTRACT(EPOCH FROM(" + timeQuery + "))";

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    if (!result_set.empty())
    {
      pqxx::result::const_iterator row = result_set.begin();
      if (!row[0].is_null())
        ret = boost::posix_time::from_time_t(row[0].as<time_t>());
    }
    return ret;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Query failed: " + timeQuery);
  }
}
boost::posix_time::ptime PostgreSQL::getLatestObservationTime()
{
  return getTime("SELECT MAX(data_time) FROM observation_data");
}

boost::posix_time::ptime PostgreSQL::getOldestObservationTime()
{
  return getTime("SELECT MIN(data_time) FROM observation_data");
}

boost::posix_time::ptime PostgreSQL::getLatestWeatherDataQCTime()
{
  return getTime("SELECT MAX(obstime) FROM weather_data_qc");
}

boost::posix_time::ptime PostgreSQL::getOldestWeatherDataQCTime()
{
  return getTime("SELECT MIN(obstime) FROM weather_data_qc");
}

boost::posix_time::ptime PostgreSQL::getLatestFlashTime()
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

boost::posix_time::ptime PostgreSQL::getOldestFlashTime()
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

boost::posix_time::ptime PostgreSQL::getLatestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  std::string stmt = ("SELECT MAX(" + time_field + ") FROM " + tablename);
  return getTime(stmt);
}

boost::posix_time::ptime PostgreSQL::getOldestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  std::string stmt = ("SELECT MIN(" + time_field + ") FROM " + tablename);
  return getTime(stmt);
}

void PostgreSQL::fillLocationCache(const vector<LocationItem> &locations)
{
  try
  {
    Spine::WriteLock lock(locations_write_mutex);
    std::vector<std::string> values_vector;
    for (const auto &item : locations)
    {
      std::string values = "(";
      values += Fmi::to_string(item.fmisid) + ",";
      values += Fmi::to_string(item.location_id) + ",";
      values += Fmi::to_string(item.country_id) + ",";
      values += ("'" + Fmi::to_iso_string(item.location_start) + "',");
      values += ("'" + Fmi::to_iso_string(item.location_end) + "',");
      values += Fmi::to_string(item.longitude) + ",";
      values += Fmi::to_string(item.latitude) + ",";
      values += Fmi::to_string(item.x) + ",";
      values += Fmi::to_string(item.y) + ",";
      values += Fmi::to_string(item.elevation) + ",";
      values += ("$$" + item.time_zone_name + "$$,");
      values += ("$$" + item.time_zone_abbrev + "$$)");
      values_vector.push_back(values);

      // Insert itsMaxInsertSize rows at a time, last round (probably) less
      if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &locations.back())
      {
        std::string sqlStmt =
            "INSERT INTO locations "
            "(fmisid, location_id, country_id, location_start, location_end, "
            "longitude, latitude, x, y, "
            "elevation, time_zone_name, time_zone_abbrev) "
            "VALUES ";
        for (const auto &v : values_vector)
        {
          sqlStmt += v;
          if (&v != &values_vector.back())
            sqlStmt += ",";
        }
        sqlStmt +=
            " ON CONFLICT(fmisid) DO UPDATE SET "
            "(location_id, country_id, location_start, location_end, "
            "longitude, latitude, x, y, elevation, time_zone_name, time_zone_abbrev) = "
            "(EXCLUDED.location_id, EXCLUDED.country_id, EXCLUDED.location_start, "
            "EXCLUDED.location_end, EXCLUDED.longitude, EXCLUDED.latitude, EXCLUDED.x, "
            "EXCLUDED.y, EXCLUDED.elevation, EXCLUDED.time_zone_name, EXCLUDED.time_zone_abbrev)";
        itsDB.executeNonTransaction(sqlStmt);
        values_vector.clear();
      }
    }
    itsDB.executeNonTransaction("VACUUM ANALYZE locations");
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Filling of location cache failed!");
  }
}

void PostgreSQL::cleanDataCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestObservationTime();
    if (t <= oldest)
      return;

    Spine::WriteLock lock(observation_data_write_mutex);
    std::string sqlStmt =
        ("DELETE FROM observation_data WHERE data_time < '" + Fmi::to_iso_extended_string(t) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of data cache failed!");
  }
}

void PostgreSQL::cleanWeatherDataQCCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestWeatherDataQCTime();
    if (t <= oldest)
      return;

    Spine::WriteLock lock(weather_data_qc_write_mutex);
    std::string sqlStmt =
        ("DELETE FROM weather_data_qc WHERE obstime < '" + Fmi::to_iso_extended_string(t) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of WeatherDataQCCache failed!");
  }
}

void PostgreSQL::cleanFlashDataCache(const boost::posix_time::time_duration &timetokeep)
{
  try
  {
    boost::posix_time::ptime t = boost::posix_time::second_clock::universal_time() - timetokeep;
    t = round_down_to_hour(t);

    auto oldest = getOldestFlashTime();
    if (t <= oldest)
      return;

    Spine::WriteLock lock(flash_data_write_mutex);
    std::string sqlStmt =
        ("DELETE FROM flash_data WHERE stroke_time < '" + Fmi::to_iso_extended_string(t) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of FlashDataCache failed!");
  }
}

std::size_t PostgreSQL::fillDataCache(const vector<DataItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    itsDB.startTransaction();
    itsDB.executeTransaction("LOCK TABLE observation_data IN SHARE MODE");
    // dropIndex("observation_data_data_time_idx", true);

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;
      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

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
        Spine::WriteLock lock(observation_data_write_mutex);
        std::vector<std::size_t> observationsToUpdate = new_items;

        while (observationsToUpdate.size() > 0)
        {
          const auto &last_item = cacheData[observationsToUpdate.back()];
          std::vector<std::string> values_vector;
          std::set<std::string> key_set;  // to check duplicates
          std::vector<std::size_t> duplicateObservations;

          for (const auto i : observationsToUpdate)
          {
            const auto &item = cacheData[i];
            // data_time, fmisid, measurand_id, producer_id, measurand_no
            std::string key = Fmi::to_iso_string(item.data_time);
            key += Fmi::to_string(item.fmisid);
            key += Fmi::to_string(item.measurand_id);
            key += Fmi::to_string(item.producer_id);
            key += Fmi::to_string(item.measurand_no);
            if (key_set.find(key) != key_set.end())
            {
              duplicateObservations.push_back(i);
            }
            else
            {
              key_set.insert(key);
              std::string values = "(";
              values += Fmi::to_string(item.fmisid) + ",";
              values += ("'" + Fmi::to_iso_string(item.data_time) + "',");
              values += Fmi::to_string(item.measurand_id) + ",";
              values += Fmi::to_string(item.producer_id) + ",";
              values += Fmi::to_string(item.measurand_no) + ",";
              values += Fmi::to_string(item.data_value) + ",";
              values += Fmi::to_string(item.data_quality) + ")";
              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO observation_data "
                  "(fmisid, data_time, measurand_id, producer_id, measurand_no, "
                  "data_value, data_quality) VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }
              sqlStmt +=
                  " ON CONFLICT(data_time, fmisid, measurand_id, producer_id, measurand_no) DO "
                  "UPDATE SET "
                  "(data_value, data_quality) = "
                  "(EXCLUDED.data_value, EXCLUDED.data_quality)";
              itsDB.executeTransaction(sqlStmt);
              values_vector.clear();
            }
          }
          observationsToUpdate = duplicateObservations;
        }
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsDataInsertCache.add(hash);

      pos1 = pos2;
    }

    // createIndex("observation_data", "data_time", "observation_data_data_time_idx", true);
    itsDB.commitTransaction();
    itsDB.executeNonTransaction("VACUUM ANALYZE observation_data");

    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}  // namespace Observation

std::size_t PostgreSQL::fillWeatherDataQCCache(const vector<WeatherDataQCItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    itsDB.startTransaction();
    itsDB.executeTransaction("LOCK TABLE weather_data_qc IN SHARE MODE");
    // dropIndex("weather_data_qc_obstime_idx", true);

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;

      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

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

      if (!new_items.empty())
      {
        Spine::WriteLock lock(weather_data_qc_write_mutex);
        std::vector<std::size_t> weatherDataToUpdate = new_items;
        while (weatherDataToUpdate.size() > 0)
        {
          const auto &last_item = cacheData[weatherDataToUpdate.back()];
          std::vector<std::string> values_vector;
          std::set<std::string> key_set;  // to check duplicates
          std::vector<std::size_t> duplicateWeatherData;

          for (const auto i : weatherDataToUpdate)
          {
            const auto &item = cacheData[i];
            // obstime, fmisid, parameter, sensor_no
            std::string key = Fmi::to_iso_string(item.obstime);
            key += Fmi::to_string(item.fmisid);
            key += item.parameter;
            key += Fmi::to_string(item.sensor_no);
            if (key_set.find(key) != key_set.end())
            {
              duplicateWeatherData.push_back(i);
            }
            else
            {
              key_set.insert(key);
              std::string values = "(";
              values += Fmi::to_string(item.fmisid) + ",";
              values += ("'" + Fmi::to_iso_string(item.obstime) + "',");
              values += ("'" + item.parameter + "',");
              values += Fmi::to_string(item.sensor_no) + ",";
              values += Fmi::to_string(item.value) + ",";
              values += Fmi::to_string(item.flag) + ")";
              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO weather_data_qc "
                  "(fmisid, obstime, parameter, sensor_no, value, flag) VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }
              sqlStmt +=
                  " ON CONFLICT(fmisid, obstime, parameter, sensor_no) DO "
                  "UPDATE SET "
                  "(value, flag) = "
                  "(EXCLUDED.value, EXCLUDED.flag)";
              itsDB.executeTransaction(sqlStmt);
              values_vector.clear();
            }
          }
          weatherDataToUpdate = duplicateWeatherData;
        }
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsWeatherQCInsertCache.add(hash);

      pos1 = pos2;
    }
    // createIndex("weather_data_qc", "obstime", "weather_data_qc_obstime_idx", true);
    itsDB.commitTransaction();
    itsDB.executeNonTransaction("VACUUM ANALYZE weather_data_qc");

    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of WeatherDataQCCache failed!");
  }
}  // namespace Observation

std::size_t PostgreSQL::fillFlashDataCache(const vector<FlashDataItem> &flashCacheData)
{
  try
  {
    if (flashCacheData.empty())
      return flashCacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    itsDB.startTransaction();
    itsDB.executeTransaction("LOCK TABLE flash_data IN SHARE MODE");
    // dropIndex("flash_data_stroke_time_idx", true);
    // dropIndex("flash_data_gix", true);

    while (pos1 < flashCacheData.size())
    {
      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

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
        Spine::WriteLock lock(flash_data_write_mutex);
        std::vector<std::size_t> flashesToUpdate = new_items;

        while (flashesToUpdate.size() > 0)
        {
          const auto &last_item = flashCacheData[flashesToUpdate.back()];
          std::vector<std::string> values_vector;
          std::set<std::string> key_set;  // to check duplicates
          std::vector<std::size_t> duplicateFlashes;

          for (const auto i : flashesToUpdate)
          {
            const auto &item = flashCacheData[i];
            std::string stroke_time = Fmi::to_iso_string(item.stroke_time);
            std::string key = stroke_time;
            key += Fmi::to_string(item.stroke_time_fraction);
            key += Fmi::to_string(item.flash_id);
            // stroke_time, stroke_time_fraction, flash_id
            if (key_set.find(key) != key_set.end())
            {
              duplicateFlashes.push_back(i);
            }
            else
            {
              key_set.insert(key);

              std::string stroke_location =
                  "ST_GeomFromText('POINT(" + Fmi::to_string("%.10g", item.longitude) + " " +
                  Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

              std::string values = "(";
              values += ("'" + stroke_time + "',");
              values += Fmi::to_string(item.stroke_time_fraction) + ",";
              values += Fmi::to_string(item.flash_id) + ",";
              values += Fmi::to_string(item.multiplicity) + ",";
              values += Fmi::to_string(item.peak_current) + ",";
              values += Fmi::to_string(item.sensors) + ",";
              values += Fmi::to_string(item.freedom_degree) + ",";
              values += Fmi::to_string(item.ellipse_angle) + ",";
              values += Fmi::to_string(item.ellipse_major) + ",";
              values += Fmi::to_string(item.ellipse_minor) + ",";
              values += Fmi::to_string(item.chi_square) + ",";
              values += Fmi::to_string(item.rise_time) + ",";
              values += Fmi::to_string(item.ptz_time) + ",";
              values += Fmi::to_string(item.cloud_indicator) + ",";
              values += Fmi::to_string(item.angle_indicator) + ",";
              values += Fmi::to_string(item.signal_indicator) + ",";
              values += Fmi::to_string(item.timing_indicator) + ",";
              values += Fmi::to_string(item.stroke_status) + ",";
              values += Fmi::to_string(item.data_source) + ",";
              values += stroke_location + ")";
              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO flash_data "
                  "(stroke_time, stroke_time_fraction, flash_id, multiplicity, "
                  "peak_current, sensors, freedom_degree, ellipse_angle, "
                  "ellipse_major, ellipse_minor, chi_square, rise_time, "
                  "ptz_time, cloud_indicator, angle_indicator, signal_indicator, "
                  "timing_indicator, stroke_status, data_source, stroke_location) "
                  "VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }

              sqlStmt +=
                  " ON CONFLICT(stroke_time, stroke_time_fraction, flash_id) DO "
                  "UPDATE SET "
                  "(multiplicity, peak_current, sensors, freedom_degree, ellipse_angle, "
                  "ellipse_major, ellipse_minor, chi_square, rise_time, "
                  "ptz_time, cloud_indicator, angle_indicator, signal_indicator, "
                  "timing_indicator, stroke_status, data_source, stroke_location) = "
                  "(EXCLUDED.multiplicity, EXCLUDED.peak_current, EXCLUDED.sensors, "
                  "EXCLUDED.freedom_degree, EXCLUDED.ellipse_angle, EXCLUDED.ellipse_major, "
                  "EXCLUDED.ellipse_minor, EXCLUDED.chi_square, EXCLUDED.rise_time, "
                  "EXCLUDED.ptz_time, EXCLUDED.cloud_indicator, EXCLUDED.angle_indicator, "
                  "EXCLUDED.signal_indicator, EXCLUDED.timing_indicator, "
                  "EXCLUDED.stroke_status, "
                  "EXCLUDED.data_source, EXCLUDED.stroke_location)";
              itsDB.executeTransaction(sqlStmt);
              values_vector.clear();
            }
          }
          flashesToUpdate = duplicateFlashes;
        }
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsFlashInsertCache.add(hash);

      pos1 = pos2;
    }

    // createIndex("flash_data USING GIST", "stroke_location", "flash_data_idx", true);
    // createIndex("flash_data", "stroke_time", "flash_data_stroke_time_idx", true);
    itsDB.commitTransaction();
    itsDB.executeNonTransaction("VACUUM ANALYZE flash_data");

    return write_count;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Flash data cache update failed!");
  }
}  // namespace Observation

void PostgreSQL::updateStationsAndGroups(const StationInfo &info)
{
  try
  {
    // The stations and the groups must be updated simultaneously,
    // hence a common lock. Note that the latter call does reads too,
    // so it would be impossible to create a single transaction of
    // both updates.

    Spine::WriteLock lock(stations_write_mutex);
    updateStations(info.stations);
    updateStationGroups(info);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Update of stations and groups failed!");
  }
}

void PostgreSQL::updateStations(const Spine::Stations &stations)
{
  try
  {
    Spine::Stations stationsToUpdate = stations;

    while (stationsToUpdate.size() > 0)
    {
      // Locking handled by updateStationsAndGroups
      const auto &last_station = stationsToUpdate.back();

      // Note! Duplicate stations can not be inserted in the same bulk copy command,
      // so we put duplicates aside and insert them later.
      // If we try to insert duplicates there is an error:
      // Execution of SQL statement failed: ERROR:  ON CONFLICT DO
      // UPDATE command cannot affect row a second time HINT:  Ensure that no rows proposed
      // for insertion within the same command have duplicate constrained values
      std::vector<std::string> values_vector;
      std::set<std::string> key_set;  // to check duplicates
      Spine::Stations duplicateStations;
      for (const auto &station : stationsToUpdate)
      {
        if (itsShutdownRequested)
          break;

        std::string key;
        key += Fmi::to_string(station.fmisid);
        key += Fmi::to_string(station.geoid);
        key += Fmi::to_iso_string(station.station_start);
        key += Fmi::to_iso_string(station.station_end);
        if (key_set.find(key) != key_set.end())
        {
          duplicateStations.push_back(station);
        }
        else
        {
          key_set.insert(key);

          std::string values = "(";
          values += Fmi::to_string(station.fmisid) + ",";
          values += Fmi::to_string(station.wmo) + ",";
          values += Fmi::to_string(station.geoid) + ",";
          values += Fmi::to_string(station.lpnn) + ",";
          values += "$$" + station.station_formal_name + "$$,";
          values += "'" + Fmi::to_iso_string(station.station_start) + "',";
          values += "'" + Fmi::to_iso_string(station.station_end) + "',";
          std::string geom = "ST_GeomFromText('POINT(" +
                             Fmi::to_string("%.10g", station.longitude_out) + " " +
                             Fmi::to_string("%.10g", station.latitude_out) + ")', " + srid + ")";
          values += geom + ")";
          values_vector.push_back(values);
        }

        if ((values_vector.size() % itsMaxInsertSize == 0) || &station == &last_station)
        {
          std::string sqlStmt =
              "INSERT INTO stations (fmisid, wmo, geoid, lpnn, station_formal_name, "
              "station_start, station_end, the_geom) VALUES ";

          for (const auto &v : values_vector)
          {
            sqlStmt += v;
            if (&v != &values_vector.back())
              sqlStmt += ",";
          }
          sqlStmt +=
              " ON CONFLICT(fmisid, geoid, station_start, station_end) DO "
              "UPDATE SET "
              "(wmo, lpnn, station_formal_name, the_geom) = "
              "(EXCLUDED.wmo, EXCLUDED.lpnn, EXCLUDED.station_formal_name, "
              "EXCLUDED.the_geom)";

          itsDB.executeNonTransaction(sqlStmt);
          values_vector.clear();
        }
      }
      stationsToUpdate = duplicateStations;
    }
    itsDB.executeNonTransaction("VACUUM ANALYZE stations");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Stations update failed!");
  }
}

void PostgreSQL::updateStationGroups(const StationInfo &info)
{
  std::string sqlStmt;
  try
  {
    // Locking handled by updateStationsAndGroups
    // Station groups at the moment.
    size_t stationGroupsCount = selectCount("SELECT COUNT(*) FROM station_groups");

    for (const Spine::Station &station : info.stations)
    {
      // Skipping the empty cases.
      if (station.station_type.empty())
        continue;

      const std::string groupCodeUpper = Fmi::ascii_toupper_copy(station.station_type);

      // Search the group_id for a group_code.
      sqlStmt =
          "SELECT group_id FROM station_groups WHERE group_code = '" + groupCodeUpper + "' LIMIT 1";

      boost::optional<int> group_id;

      pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

      if (!result_set.empty())
      {
        pqxx::result::const_iterator row = result_set.begin();
        if (!row[0].is_null())
          group_id = row[0].as<int>();
      }

      // Group id not found, so we must add a new one.
      if (not group_id)
      {
        stationGroupsCount++;
        group_id = stationGroupsCount;
        sqlStmt = "INSERT INTO station_groups (group_id, group_code) VALUES (" +
                  Fmi::to_string(stationGroupsCount) + ", '" + groupCodeUpper +
                  "') "
                  " ON CONFLICT(group_id) DO "
                  "UPDATE SET "
                  "(group_code) = ROW(EXCLUDED.group_code)";
        itsDB.executeNonTransaction(sqlStmt);
      }

      // Avoid duplicates.
      sqlStmt = "SELECT COUNT(*) FROM group_members WHERE group_id=" + Fmi::to_string(*group_id) +
                " AND fmisid=" + Fmi::to_string(station.fmisid);

      int groupCount = selectCount(sqlStmt);

      if (groupCount == 0)
      {
        // Insert a group member. Ignore if insertion fail (perhaps group_id or
        // fmisid is not found from the stations table)
        sqlStmt = "INSERT INTO group_members (group_id, fmisid) VALUES (" +
                  Fmi::to_string(*group_id) + ", " + Fmi::to_string(station.fmisid) + ")";
        itsDB.executeNonTransaction(sqlStmt);
      }
    }
    itsDB.executeNonTransaction("VACUUM ANALYZE station_groups");
    itsDB.executeNonTransaction("VACUUM ANALYZE group_members");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Updating station groups failed!");
  }
}

Spine::Stations PostgreSQL::findStationsByWMO(const Settings &settings, const StationInfo &info)
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

Spine::Stations PostgreSQL::findStationsByLPNN(const Settings &settings, const StationInfo &info)
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

Spine::Stations PostgreSQL::findNearestStations(const Spine::LocationPtr &location,
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

Spine::Stations PostgreSQL::findNearestStations(double latitude,
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

    std::string sqlStmt =
        "SELECT DISTINCT s.fmisid, "
        "COALESCE(ST_Distance(s.the_geom, "
        "(SELECT ST_GeomFromText('POINT(" +
        Fmi::to_string("%.10g", longitude) + " " + Fmi::to_string("%.10g", latitude) + ")'," +
        srid +
        ")), 1), 0)/1000 dist "  // divide by 1000 to get kilometres
        ", s.wmo"
        ", s.geoid"
        ", s.lpnn"
        ", ST_X(s.the_geom)"
        ", ST_Y(s.the_geom)"
        ", s.station_formal_name "
        "FROM ";

    if (not stationgroup_codes.empty())
    {  // Station selection from a station
       // group or groups.
      sqlStmt +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s oN gm.fmisid = s.fmisid ";
    }
    else
    {
      // Do not care about station group.
      sqlStmt += "stations s ";
    }

    sqlStmt += "WHERE ";

    if (not stationgroup_codes.empty())
    {
      auto it = stationgroup_codes.begin();
      sqlStmt += "( sg.group_code='" + *it + "' ";
      for (it++; it != stationgroup_codes.end(); it++)
        sqlStmt += "OR sg.group_code='" + *it + "' ";
      sqlStmt += ") AND ";
    }

    sqlStmt += "ST_Distance_Sphere(ST_GeomFromText('POINT(" + Fmi::to_string("%.10g", longitude) +
               " " + Fmi::to_string("%.10g", latitude) + ")', " + srid +
               "), s.the_geom) <= " + Fmi::to_string(maxdistance);

    sqlStmt +=
        " AND (:starttime BETWEEN s.station_start AND s.station_end OR "
        ":endtime BETWEEN s.station_start AND s.station_end) "
        "ORDER BY dist ASC, s.fmisid ASC LIMIT " +
        Fmi::to_string(numberofstations);

    int fmisid = 0;
    int wmo = -1;
    int geoid = -1;
    int lpnn = -1;
    double longitude_out = std::numeric_limits<double>::max();
    double latitude_out = std::numeric_limits<double>::max();
    string distance = "";
    std::string station_formal_name = "";
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      try
      {
        fmisid = row[0].as<int>();
        // Round distances to 100 meter precision
        distance = fmt::format("{:.1f}", Fmi::stod(row[1].as<string>()));
        wmo = row[2].as<int>();
        geoid = row[3].as<int>();
        lpnn = row[4].as<int>();
        longitude_out = Fmi::stod(row[5].as<std::string>());
        latitude_out = Fmi::stod(row[6].as<std::string>());
        station_formal_name = row[7].as<std::string>();

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

void PostgreSQL::fetchCachedDataFromDB(const std::string &sqlStmt,
                                       struct cached_data &data,
                                       bool measurand /*= false*/)
{
  pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
  for (auto row : result_set)
  {
    boost::optional<int> fmisid = row[0].as<int>();
    boost::posix_time::ptime obstime = boost::posix_time::from_time_t(row[1].as<time_t>());
    boost::optional<double> latitude = row[2].as<double>();
    boost::optional<double> longitude = row[3].as<double>();
    boost::optional<double> elevation = row[4].as<double>();
    boost::optional<double> data_value;
    pqxx::field data_value_field = row[6];
    if (!data_value_field.is_null())
      data_value = data_value_field.as<double>(6);
    data.fmisidsAll.push_back(fmisid);
    data.obstimesAll.push_back(obstime);
    data.latitudesAll.push_back(latitude);
    data.longitudesAll.push_back(longitude);
    data.elevationsAll.push_back(elevation);
    data.data_valuesAll.push_back(data_value);
    if (measurand)
    {
      boost::optional<int> measurand_id = row[5].as<int>();
      data.measurand_idsAll.push_back(measurand_id);
    }
    else
    {
      boost::optional<std::string> parameter = row[5].as<std::string>();
      boost::optional<double> sensor_no = row[7].as<double>();
      data.parametersAll.push_back(parameter);
      data.sensor_nosAll.push_back(sensor_no);
    }
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQL::getCachedWeatherDataQCData(
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

    std::string sqlStmt;
    if (settings.latest)
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM MAX(data.obstime)) AS obstime, "
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
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM data.obstime) AS obstime, "
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
    cached_data cachedData;
    fetchCachedDataFromDB(sqlStmt, cachedData);

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value>>> data;

    for (const auto &time : cachedData.obstimesAll)
    {
      int fmisid = *cachedData.fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      std::string parameter = *cachedData.parametersAll[i];
      int sensor_no = *cachedData.sensor_nosAll[i];
      Fmi::ascii_tolower(parameter);
      if (sensor_no > 1)
      {
        parameter += "_" + Fmi::to_string(sensor_no);
      }

      ts::Value val;
      if (cachedData.data_valuesAll[i])
        val = ts::Value(*cachedData.data_valuesAll[i]);

      data[fmisid][obstime][parameter] = val;
      if (sensor_no == 1)
      {
        parameter += "_1";
        data[fmisid][obstime][parameter] = val;
      }
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value>> dataItem;

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
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQL::getCachedData(const Spine::Stations &stations,
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

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM data.data_time) AS obstime, "
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
        "loc.latitude, loc.longitude, loc.elevation, data.data_value "
        "ORDER BY fmisid ASC, obstime ASC;";

    cached_data cachedData;
    fetchCachedDataFromDB(sqlStmt, cachedData, true);

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Spine::TimeSeries::TimeSeriesVectorPtr(new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<int, ts::Value>>> data;
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value>>>
        dataWithStringParameterId;

    for (const auto &time : cachedData.obstimesAll)
    {
      // Safety against missing values. At least the latter has occasionally been missing
      if (!cachedData.fmisidsAll[i] || !cachedData.measurand_idsAll[i])
        continue;

      int fmisid = *cachedData.fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations[fmisid].timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      int measurand_id = *cachedData.measurand_idsAll[i];

      ts::Value val;
      if (cachedData.data_valuesAll[i])
        val = ts::Value(*cachedData.data_valuesAll[i]);

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

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value>>
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
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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

void PostgreSQL::addEmptyValuesToTimeSeries(
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

void PostgreSQL::addParameterToTimeSeries(
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time, std::map<std::string, ts::Value>> &dataItem,
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

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQL::getCachedFlashData(
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

    std::string sqlStmt =
        "SELECT stroke_time, "
        "stroke_time_fraction, flash_id, "
        "ST_X(stroke_location) AS longitude, "
        "ST_Y(stroke_location) AS latitude, " +
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
          sqlStmt += " AND ST_Distance_Sphere(ST_GeomFromText('POINT(" + lon + " " + lat +
                     ")', 4326), flash.stroke_location) <= " + radius;
        }
        if (tloc.loc->type == Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);

          sqlStmt += " AND ST_Within(flash.stroke_location, ST_MakeEnvelope(" +
                     Fmi::to_string(bbox.xMin) + ", " + Fmi::to_string(bbox.yMin) + ", " +
                     Fmi::to_string(bbox.xMax) + ", " + Fmi::to_string(bbox.yMax) + ", 4326)) ";
        }
      }
    }

    sqlStmt += "ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction ASC;";

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
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      map<std::string, ts::Value> result;
      stroke_time = row[0].as<string>();
      // int stroke_time_fraction = row[1].as<int>();
      // int flash_id = row[2].as<int>();
      longitude = Fmi::stod(row[3].as<string>());
      latitude = Fmi::stod(row[4].as<string>());
      // Rest of the parameters in requested order
      for (unsigned int i = 5; i != row.size(); ++i)
      {
        pqxx::field fld = row[i];
        std::string data_type = itsPostgreDataTypes[fld.type()];
        ts::Value temp;
        if (data_type == "text")
        {
          temp = row[i].as<std::string>();
        }
        else if (data_type == "float4" || data_type == "float8" || data_type == "_float4" ||
                 data_type == "_float8")
        {
          temp = row[i].as<double>();
        }
        else if (data_type == "int2" || data_type == "int4" || data_type == "int8" ||
                 data_type == "_int2" || data_type == "_int4" || data_type == "_int8")
        {
          temp = row[i].as<int>(i);
        }
        result[fld.name()] = temp;
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

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQL::addSmartSymbolToTimeSeries(
    const int pos,
    const Spine::Station &s,
    const boost::local_time::local_date_time &time,
    const ParameterMap &parameterMap,
    const std::string &stationtype,
    const std::map<int, std::map<boost::local_time::local_date_time, std::map<int, ts::Value>>>
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

void PostgreSQL::addSpecialParameterToTimeSeries(
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
          "PostgreSQL::addSpecialParameterToTimeSeries : "
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

Spine::Stations PostgreSQL::findAllStationsFromGroups(
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

Spine::Stations PostgreSQL::fetchStationsFromDB(const std::string &sqlStmt,
                                                const Settings &settings,
                                                const StationInfo &info) const
{
  Spine::Stations stations;
  pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
  for (auto row : result_set)
  {
    try
    {
      int geoid = row[0].as<int>();
      int station_id = row[1].as<int>();
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

Spine::Stations PostgreSQL::findStationsInsideArea(const Settings &settings,
                                                   const std::string &areaWkt,
                                                   const StationInfo &info)
{
  try
  {
    Spine::Stations stations;

    std::string sqlStmt = "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sqlStmt +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlStmt += "stations s ";
    }

    sqlStmt += "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sqlStmt += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sqlStmt += fmt::format("OR sg.group_code='{}' ", *it);
      sqlStmt += ") AND ";
    }

    sqlStmt += fmt::format(
        "ST_Contains(ST_GeomFromText('{}','{}'), s.the_geom) AND ('{}' BETWEEN "
        "s.station_start "
        "AND "
        "s.station_end OR '{}' BETWEEN s.station_start AND s.station_end)",
        areaWkt,
        srid,
        Fmi::to_iso_extended_string(settings.starttime),
        Fmi::to_iso_extended_string(settings.endtime));

    return fetchStationsFromDB(sqlStmt, settings, info);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::Stations PostgreSQL::findStationsInsideBox(const Settings &settings, const StationInfo &info)
{
  try
  {
    Spine::Stations stations;

    std::string sqlStmt = "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sqlStmt +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlStmt += "stations s ";
    }

    sqlStmt += "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sqlStmt += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sqlStmt += fmt::format("OR sg.group_code='{}' ", *it);
      sqlStmt += ") AND ";
    }

    sqlStmt += fmt::format(
        "ST_EnvIntersects(s.the_geom,{:.10f},{:.10f},{:.10f},{:.10f}) AND ('{}' BETWEEN "
        "s.station_start AND "
        "s.station_end OR '{}' BETWEEN s.station_start AND s.station_end)",
        settings.boundingBox.at("minx"),
        settings.boundingBox.at("miny"),
        settings.boundingBox.at("maxx"),
        settings.boundingBox.at("maxy"),
        Fmi::to_iso_extended_string(settings.starttime),
        Fmi::to_iso_extended_string(settings.endtime));

    return fetchStationsFromDB(sqlStmt, settings, info);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool PostgreSQL::fillMissing(Spine::Station &s, const std::set<std::string> &stationgroup_codes)
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

    std::string sqlStmt =
        "SELECT s.fmisid, s.wmo, s.geoid, s.lpnn, ST_X(s.the_geom) AS lon, ST_Y(s.the_geom) "
        "AS "
        "lat, "
        "s.station_formal_name FROM ";

    if (not stationgroup_codes.empty())
    {
      sqlStmt +=
          "group_members gm "
          "JOIN station_groups sg ON gm.group_id = sg.group_id "
          "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlStmt += "stations s ";
    }

    sqlStmt += " WHERE";

    if (not stationgroup_codes.empty())
    {
      auto it = stationgroup_codes.begin();
      sqlStmt += fmt::format("( sg.group_code='{}' ", *it);
      for (it++; it != stationgroup_codes.end(); it++)
        sqlStmt += fmt::format("OR sg.group_code='{}' ", *it);
      sqlStmt += ") AND ";
    }

    // Use the first id that is not missing.
    if (not missingStationId)
      sqlStmt += fmt::format(" s.fmisid={}", s.station_id);
    else if (not missingFmisId)
      sqlStmt += fmt::format(" s.fmisid={}", s.fmisid);
    else if (not missingWmoId)
      sqlStmt += fmt::format(" s.wmo={}", s.wmo);
    else if (not missingGeoId)
      sqlStmt += fmt::format(" s.geoid={}", s.geoid);
    else if (not missingLpnnId)
      sqlStmt += fmt::format(" s.lpnn={}", s.lpnn);
    else
      return false;

    // There might be multiple locations for a station.
    sqlStmt += " AND now() BETWEEN s.station_start AND s.station_end";

    boost::optional<int> fmisid;
    boost::optional<int> wmo;
    boost::optional<int> geoid;
    boost::optional<int> lpnn;
    boost::optional<double> longitude_out;
    boost::optional<double> latitude_out;
    boost::optional<std::string> station_formal_name;

    // We need only the first one (ID values are unique).
    sqlStmt += " LIMIT 1";

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    if (!result_set.empty())
    {
      pqxx::result::const_iterator row = result_set.begin();
      fmisid = row[0].as<int>();
      wmo = row[1].as<int>();
      geoid = row[2].as<int>();
      lpnn = row[3].as<int>();
      longitude_out = row[4].as<double>();
      latitude_out = row[5].as<double>();
      station_formal_name = row[6].as<std::string>();
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

bool PostgreSQL::getStationById(Spine::Station &station,
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

bool PostgreSQL::getStationByGeoid(Spine::Station &station,
                                   int geo_id,
                                   const std::set<std::string> &stationgroup_codes)
{
  try
  {
    Spine::Station s;
    s.station_id = -1;
    s.fmisid = -1;
    s.wmo = -1;
    s.geoid = geo_id;
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

FlashCounts PostgreSQL::getFlashCount(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime,
                                      const Spine::TaggedLocationList &locations)
{
  try
  {
    FlashCounts flashcounts;
    flashcounts.flashcount = 0;
    flashcounts.strokecount = 0;
    flashcounts.iccount = 0;

    std::string sqlStmt =
        "SELECT "
        "COALESCE(SUM(CASE WHEN flash.multiplicity > 0 "
        "THEN 1 ELSE 0 END), 0) AS flashcount, "
        "COALESCE(SUM(CASE WHEN flash.multiplicity = 0 "
        "THEN 1 ELSE 0 END), 0) AS strokecount, "
        "COALESCE(SUM(CASE WHEN flash.cloud_indicator = 1 "
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
          sqlStmt += " AND ST_Distance_Sphere(ST_GeomFromText('POINT(" + lon + " " + lat +
                     ")', 4326), flash.stroke_location) <= " + radius;
        }
        if (tloc.loc->type == Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);

          sqlStmt += " AND ST_Within(flash.stroke_location, ST_MakeEnvelope(" +
                     Fmi::to_string(bbox.xMin) + ", " + Fmi::to_string(bbox.yMin) + ", " +
                     Fmi::to_string(bbox.xMax) + ", " + Fmi::to_string(bbox.yMax) + ", 4326)) ";
        }
      }
    }
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    if (!result_set.empty())
    {
      pqxx::result::const_iterator row = result_set.begin();
      flashcounts.flashcount = row[0].as<int>();
      flashcounts.strokecount = row[1].as<int>();
      flashcounts.iccount = row[2].as<int>();
    }

    return flashcounts;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQL::getCachedWeatherDataQCData(
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

    std::string sqlStmt;
    if (settings.latest)
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM MAX(data.obstime)) AS obstime, "
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
          "GROUP BY data.fmisid, data.parameter, data.value, data.sensor_no, "
          "loc.location_id, "
          "loc.location_end, "
          "loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }
    else
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM data.obstime) AS obstime, "
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

    cached_data cachedData;
    fetchCachedDataFromDB(sqlStmt, cachedData);

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value>>> data;

    for (const auto &time : cachedData.obstimesAll)
    {
      int fmisid = *cachedData.fmisidsAll[i];

      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      std::string parameter = *cachedData.parametersAll[i];
      int sensor_no = *cachedData.sensor_nosAll[i];
      Fmi::ascii_tolower(parameter);
      if (sensor_no > 1)
      {
        parameter += "_" + Fmi::to_string(sensor_no);
      }

      ts::Value val;
      if (cachedData.data_valuesAll[i])
        val = ts::Value(*cachedData.data_valuesAll[i]);

      data[fmisid][obstime][parameter] = val;
      if (sensor_no == 1)
      {
        parameter += "_1";
        data[fmisid][obstime][parameter] = val;
      }
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value>> dataItem;

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
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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

Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQL::getCachedData(
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

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM data.data_time) AS obstime, "
        "loc.latitude, loc.longitude, loc.elevation, measurand_id, data_value "
        "FROM observation_data data JOIN locations loc ON (data.fmisid = "
        "loc.fmisid) WHERE data.fmisid IN (" +
        qstations + ") AND data.data_time >= '" + Fmi::to_iso_extended_string(settings.starttime) +
        "' AND data.data_time <= '" + Fmi::to_iso_extended_string(settings.endtime) +
        "' AND data.measurand_id IN (" + param +
        ") AND data.measurand_no = 1 "
        "GROUP BY data.fmisid, data.data_time, data.measurand_id, data.data_value, "
        "loc.location_id, loc.location_end, loc.latitude, loc.longitude, loc.elevation "
        "ORDER BY fmisid ASC, obstime ASC";

    cached_data cachedData;
    fetchCachedDataFromDB(sqlStmt, cachedData, true);

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Spine::TimeSeries::TimeSeriesVectorPtr(new Spine::TimeSeries::TimeSeriesVector);

    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<int, ts::Value>>> data;
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value>>>
        dataWithStringParameterId;

    for (const auto &time : cachedData.obstimesAll)
    {
      int fmisid = *cachedData.fmisidsAll[i];
      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? tmpStations[fmisid].timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      local_date_time obstime = local_date_time(utctime, localtz);

      int measurand_id = *cachedData.measurand_idsAll[i];

      ts::Value val;
      if (cachedData.data_valuesAll[i])
        val = ts::Value(*cachedData.data_valuesAll[i]);

      data[fmisid][obstime][measurand_id] = val;
      dataWithStringParameterId[fmisid][obstime][Fmi::to_string(measurand_id)] = val;
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value>>
        dataItemWithStringParameterId;

    // Accept all time steps
    if (timeSeriesOptions.all() && !settings.latest)
    {
      for (const Spine::Station &s : stations)
      {
        int fmisid = s.station_id;
        map<boost::local_time::local_date_time, map<std::string, ts::Value>> stationData =
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
                int windpos = Fmi::stoi(parameterMap["windspeedms"][stationtype]);
                int rhpos = Fmi::stoi(parameterMap["relativehumidity"][stationtype]);
                int temppos = Fmi::stoi(parameterMap["temperature"][stationtype]);

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
                  int windpos = Fmi::stoi(parameterMap["windspeedms"][stationtype]);
                  int rhpos = Fmi::stoi(parameterMap["relativehumidity"][stationtype]);
                  int temppos = Fmi::stoi(parameterMap["temperature"][stationtype]);

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

void PostgreSQL::createObservablePropertyTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS observable_property ("
        "measurandId INTEGER,"
        "language TEXT,"
        "measurandCode TEXT,"
        "observablePropertyId TEXT,"
        "observablePropertyLabel TEXT,"
        "basePhenomenon TEXT,"
        "uom TEXT,"
        "statisticalMeasureId TEXT,"
        "statisticalFunction TEXT,"
        "aggregationTimePeriod TEXT,"
        "gmlId TEXT, "
        "last_modified timestamp default now())");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of observable_property table failed!");
  }
}

boost::shared_ptr<std::vector<ObservableProperty>> PostgreSQL::getObservableProperties(
    std::vector<std::string> &parameters,
    const std::string language,
    const std::map<std::string, std::map<std::string, std::string>> &parameterMap,
    const std::string &stationType)
{
  boost::shared_ptr<std::vector<ObservableProperty>> data(new std::vector<ObservableProperty>());
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

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      int measurandId = row[0].as<int>();
      // Multiple parameter name aliases may use a same measurand id (e.g. t2m
      // and temperature)
      std::pair<std::multimap<int, std::string>::iterator,
                std::multimap<int, std::string>::iterator>
          r = parameterIDs.equal_range(measurandId);
      for (std::multimap<int, std::string>::iterator it = r.first; it != r.second; ++it)
      {
        ObservableProperty op;
        op.measurandId = Fmi::to_string(measurandId);
        op.measurandCode = row[1].as<std::string>();
        op.observablePropertyId = row[2].as<std::string>();
        op.observablePropertyLabel = row[3].as<std::string>();
        op.basePhenomenon = row[4].as<std::string>();
        op.uom = row[5].as<std::string>();
        op.statisticalMeasureId = row[6].as<std::string>();
        op.statisticalFunction = row[7].as<std::string>();
        op.aggregationTimePeriod = row[8].as<std::string>();
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

void PostgreSQL::createIndex(const std::string &table,
                             const std::string &column,
                             const std::string &idx_name,
                             bool transaction /*= false*/) const
{
  try
  {
    if (transaction)
      itsDB.executeTransaction("CREATE INDEX IF NOT EXISTS " + idx_name + " ON " + table + "(" +
                               column + ")");
    else
      itsDB.executeNonTransaction("CREATE INDEX IF NOT EXISTS " + idx_name + " ON " + table + "(" +
                                  column + ")");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creating index " + idx_name + " failed!");
  }
}

#if 0
void PostgreSQL::dropIndex(const std::string &idx_name, bool transaction /*= false*/) const
{
  try
  {
    if (transaction)
      itsDB.executeTransaction("DROP INDEX IF EXISTS " + idx_name);
    else
      itsDB.executeNonTransaction("DROP INDEX IF EXISTS " + idx_name);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Dropping index " + idx_name + " failed!");
  }
}
#endif

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
