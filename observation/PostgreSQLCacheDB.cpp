#include "PostgreSQLCacheDB.h"
#include "AsDouble.h"
#include "ExternalAndMobileDBInfo.h"
#include "PostgreSQLCacheParameters.h"
#include "QueryMapping.h"
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Reactor.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesOutput.h>
#include <chrono>
#include <iostream>
#include <thread>

#ifdef __llvm__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow"
#endif

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

namespace
{
Spine::MutexType stations_write_mutex;
Spine::MutexType locations_write_mutex;
Spine::MutexType observation_data_write_mutex;
Spine::MutexType weather_data_qc_write_mutex;
Spine::MutexType flash_data_write_mutex;
Spine::MutexType roadcloud_data_write_mutex;
Spine::MutexType netatmo_data_write_mutex;
Spine::MutexType fmi_iot_data_write_mutex;
}  // namespace

namespace Engine
{
namespace Observation
{
PostgreSQLCacheDB::PostgreSQLCacheDB(const PostgreSQLCacheParameters &options)
    : CommonPostgreSQLFunctions(
          options.postgresql, options.stationtypeConfig, options.parameterMap),
      itsMaxInsertSize(options.maxInsertSize),
      itsDataInsertCache(options.dataInsertCacheSize),
      itsWeatherQCInsertCache(options.weatherDataQCInsertCacheSize),
      itsFlashInsertCache(options.flashInsertCacheSize),
      itsRoadCloudInsertCache(options.roadCloudInsertCacheSize),
      itsNetAtmoInsertCache(options.netAtmoInsertCacheSize),
      itsFmiIoTInsertCache(options.fmiIoTInsertCacheSize),
      itsExternalAndMobileProducerConfig(options.externalAndMobileProducerConfig)
{
  srid = "4326";
  itsIsCacheDatabase = true;
}

void PostgreSQLCacheDB::createTables(const std::set<std::string> &tables)
{
  try
  {
    // No locking needed during initialization phase
    if (tables.find(OBSERVATION_DATA_TABLE) != tables.end())
      createObservationDataTable();
    if (tables.find(WEATHER_DATA_QC_TABLE) != tables.end())
      createWeatherDataQCTable();
    if (tables.find(FLASH_DATA_TABLE) != tables.end())
      createFlashDataTable();
    if (tables.find(ROADCLOUD_DATA_TABLE) != tables.end())
      createRoadCloudDataTable();
    if (tables.find(NETATMO_DATA_TABLE) != tables.end())
      createNetAtmoDataTable();
    if (tables.find(BK_HYDROMETA_DATA_TABLE) != tables.end())
      createBKHydrometaDataTable();
    if (tables.find(FMI_IOT_DATA_TABLE) != tables.end())
      createFmiIoTDataTable();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of database tables failed!");
  }
}

// ----------------------------------------------------------------------
/*!
 * \brief Shutdown connections
 */
// ----------------------------------------------------------------------

void PostgreSQLCacheDB::shutdown()
{
  std::cout << "  -- Shutdown requested (PostgreSQLCacheDB)\n";
}

void PostgreSQLCacheDB::createObservationDataTable()
{
  try
  {
    // If TABLE exists it is not re-created
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS observation_data("
        "fmisid INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "data_time timestamp NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "data_source INTEGER, "
        "modified_last timestamp NOT NULL DEFAULT now(), "
        "PRIMARY KEY (fmisid, data_time, measurand_id, producer_id, measurand_no, sensor_no));");

    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON "
        "observation_data(data_time);");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS observation_data_fmisid_idx ON observation_data(fmisid);");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS observation_data_modified_last_idx ON "
        "observation_data(modified_last);");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of observation_data table failed!");
  }
}

void PostgreSQLCacheDB::createWeatherDataQCTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS weather_data_qc ("
        "fmisid INTEGER NOT NULL, "
        "obstime timestamp NOT NULL, "
        "parameter INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "value REAL NOT NULL, "
        "flag INTEGER NOT NULL, "
        "modified_last timestamp default NULL, "
        "PRIMARY KEY (obstime, fmisid, parameter, sensor_no));");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_obstime_idx ON weather_data_qc(obstime);");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_fmisid_idx ON weather_data_qc(fmisid);");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_modified_last_idx ON "
        "weather_data_qc(modified_last);");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of weather_data_qc table failed!");
  }
}

void PostgreSQLCacheDB::createFlashDataTable()
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
        "data_source INTEGER, "
        "created  timestamp default now(), "
        "modified_last timestamp default now(), "
        "modified_by INTEGER, "
        "PRIMARY KEY (stroke_time, stroke_time_fraction, flash_id));");

    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS flash_data_stroke_time_idx on flash_data(stroke_time);");
    itsDB.executeNonTransaction(
        "CREATE INDEX IF NOT EXISTS flaash_data_modified_last_idx ON flash_data(modified_last);");

    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='flash_data'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('flash_data', 'stroke_location', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS flash_data_gix ON flash_data USING GIST (stroke_location)");
    }

    // If the old version of table exists add data_source-column
    result_set = itsDB.executeNonTransaction(
        "select EXISTS (SELECT 1 FROM information_schema.columns where table_schema = 'public' and "
        "table_name='flash_data' and column_name='data_source')");

    if (!result_set.empty())
    {
      pqxx::result::const_iterator row = result_set.begin();
      if (!row[0].is_null() && row[0].as<bool>() == false)
      {
        itsDB.executeNonTransaction("ALTER TABLE flash_data ADD COLUMN data_source INTEGER");
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of flash_data table failed!");
  }
}

void PostgreSQLCacheDB::createRoadCloudDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS ext_obsdata_roadcloud("
        "prod_id INTEGER, "
        "station_id INTEGER DEFAULT 0, "
        "dataset_id character VARYING(50) DEFAULT 0, "
        "data_level INTEGER DEFAULT 0, "
        "mid INTEGER, "
        "sensor_no INTEGER DEFAULT 0, "
        "data_time timestamp without time zone NOT NULL, "
        "data_value NUMERIC, "
        "data_value_txt character VARYING(30), "
        "data_quality INTEGER, "
        "ctrl_status INTEGER, "
        "created timestamp without time zone DEFAULT timezone('UTC'::text, now()), "
        "altitude NUMERIC)");
    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='ext_obsdata_roadcloud'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('ext_obsdata_roadcloud', 'geom', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS ext_obsdata_roadcloud_gix ON ext_obsdata_roadcloud USING "
          "GIST (geom)");
      itsDB.executeNonTransaction(
          "ALTER TABLE ext_obsdata_roadcloud ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_roadcloud table failed!");
  }
}

void PostgreSQLCacheDB::createNetAtmoDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS ext_obsdata_netatmo("
        "prod_id INTEGER, "
        "station_id INTEGER DEFAULT 0, "
        "dataset_id character VARYING(50) DEFAULT 0, "
        "data_level INTEGER DEFAULT 0, "
        "mid INTEGER, "
        "sensor_no INTEGER DEFAULT 0, "
        "data_time timestamp without time zone NOT NULL, "
        "data_value NUMERIC, "
        "data_value_txt character VARYING(30), "
        "data_quality INTEGER, "
        "ctrl_status INTEGER, "
        "created timestamp without time zone DEFAULT timezone('UTC'::text, now()), "
        "altitude NUMERIC)");
    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='ext_obsdata_netatmo'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('ext_obsdata_netatmo', 'geom', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS ext_obsdata_netatmo_gix ON ext_obsdata_netatmo USING GIST "
          "(geom)");
      itsDB.executeNonTransaction(
          "ALTER TABLE ext_obsdata_netatmo ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_netatmo table failed!");
  }
}

void PostgreSQLCacheDB::createBKHydrometaDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS ext_obsdata_bk_hydrometa("
        "prod_id INTEGER, "
        "station_id INTEGER DEFAULT 0, "
        "dataset_id character VARYING(50) DEFAULT 0, "
        "data_level INTEGER DEFAULT 0, "
        "mid INTEGER, "
        "sensor_no INTEGER DEFAULT 0, "
        "data_time timestamp without time zone NOT NULL, "
        "data_value NUMERIC, "
        "data_value_txt character VARYING(30), "
        "data_quality INTEGER, "
        "ctrl_status INTEGER, "
        "created timestamp without time zone DEFAULT timezone('UTC'::text, now()), "
        "altitude NUMERIC)");
    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='ext_obsdata_bk_hydrometa'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('ext_obsdata_bk_hydrometa', 'geom', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS ext_obsdata_bk_hydrometa_gix ON ext_obsdata_bk_hydrometa "
          "USING GIST "
          "(geom)");
      itsDB.executeNonTransaction(
          "ALTER TABLE ext_obsdata_bk_hydrometa ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_bk_hydrometa table failed!");
  }
}

void PostgreSQLCacheDB::createFmiIoTDataTable()
{
  try
  {
    itsDB.executeNonTransaction(
        "CREATE TABLE IF NOT EXISTS ext_obsdata_fmi_iot("
        "prod_id INTEGER, "
        "station_id INTEGER DEFAULT 0, "
        "dataset_id character VARYING(50) DEFAULT 0, "
        "data_level INTEGER DEFAULT 0, "
        "mid INTEGER, "
        "sensor_no INTEGER DEFAULT 0, "
        "data_time timestamp without time zone NOT NULL, "
        "data_value NUMERIC, "
        "data_value_txt character VARYING(30), "
        "data_quality INTEGER, "
        "ctrl_status INTEGER, "
        "created timestamp without time zone DEFAULT timezone('UTC'::text, now()), "
        "altitude NUMERIC)");
    pqxx::result result_set = itsDB.executeNonTransaction(
        "SELECT * FROM geometry_columns WHERE f_table_name='ext_obsdata_fmi_iot'");
    if (result_set.empty())
    {
      itsDB.executeNonTransaction(
          "SELECT AddGeometryColumn('ext_obsdata_fmi_iot', 'geom', 4326, 'POINT', 2)");
      itsDB.executeNonTransaction(
          "CREATE INDEX IF NOT EXISTS ext_obsdata_fmi_iot_gix ON ext_obsdata_fmi_iot USING GIST "
          "(geom)");
      itsDB.executeNonTransaction(
          "ALTER TABLE ext_obsdata_fmi_iot ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_fmi_iot table failed!");
  }
}

size_t PostgreSQLCacheDB::selectCount(const std::string &queryString)
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
    throw Fmi::Exception::Trace(BCP, queryString + " query failed!");
  }
}  // namespace Observation

boost::posix_time::ptime PostgreSQLCacheDB::getTime(const std::string &timeQuery) const
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
      {
        auto value = as_double(row[0]);
        time_t seconds = floor(value);
        ret = boost::posix_time::from_time_t(seconds);
        double fractions = (value - floor(value));
        if (fractions > 0.0)
          ret += milliseconds(static_cast<int64_t>(fractions * 1000));
      }
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Query failed: " + timeQuery);
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestObservationTime()
{
  return getTime("SELECT MAX(data_time) FROM observation_data");
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestObservationModifiedTime()
{
  return getTime("SELECT MAX(modified_last) FROM observation_data");
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestObservationTime()
{
  return getTime("SELECT MIN(data_time) FROM observation_data");
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestWeatherDataQCTime()
{
  return getTime("SELECT MAX(obstime) FROM weather_data_qc");
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestWeatherDataQCModifiedTime()
{
  return getTime("SELECT MAX(modified_last) FROM weather_data_qc");
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestWeatherDataQCTime()
{
  return getTime("SELECT MIN(obstime) FROM weather_data_qc");
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestFlashModifiedTime()
{
  try
  {
    string tablename = "flash_data";
    string time_field = "modified_last";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest flash time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestFlashTime()
{
  try
  {
    string tablename = "flash_data";
    string time_field = "stroke_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest flash time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestFlashTime()
{
  try
  {
    string tablename = "flash_data";
    string time_field = "stroke_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest flash time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestRoadCloudDataTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest RoadCloud data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestRoadCloudCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest RoadCloud created time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestRoadCloudDataTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest RoadCloud data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestNetAtmoDataTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest NetAtmo data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestNetAtmoDataTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest NetAtmo data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestNetAtmoCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest NetAtmo created time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestBKHydrometaDataTime()
{
  try
  {
    string tablename = "ext_obsdata_bk_hydrometa";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest bk_hydrometa data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestBKHydrometaDataTime()
{
  try
  {
    string tablename = "ext_obsdata_bk_hydrometa";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest bk_hydrometa data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestBKHydrometaCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_bk_hydrometa";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest NetAtmo created time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestFmiIoTDataTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest FmiIoT data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestFmiIoTDataTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest FmiIoT data time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestFmiIoTCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest FmiIoT created time query failed!");
  }
}

boost::posix_time::ptime PostgreSQLCacheDB::getLatestTimeFromTable(const std::string tablename,
                                                                   const std::string time_field)
{
  std::string stmt = ("SELECT MAX(" + time_field + ") FROM " + tablename);
  return getTime(stmt);
}

boost::posix_time::ptime PostgreSQLCacheDB::getOldestTimeFromTable(const std::string tablename,
                                                                   const std::string time_field)
{
  std::string stmt = ("SELECT MIN(" + time_field + ") FROM " + tablename);
  return getTime(stmt);
}

void PostgreSQLCacheDB::cleanDataCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestObservationTime();
    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(observation_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM observation_data WHERE data_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of data cache failed!");
  }
}

void PostgreSQLCacheDB::cleanWeatherDataQCCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestWeatherDataQCTime();
    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(weather_data_qc_write_mutex);
    std::string sqlStmt = ("DELETE FROM weather_data_qc WHERE obstime < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of WeatherDataQCCache failed!");
  }
}

void PostgreSQLCacheDB::cleanFlashDataCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestFlashTime();

    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(flash_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM flash_data WHERE stroke_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of FlashDataCache failed!");
  }
}

void PostgreSQLCacheDB::cleanRoadCloudCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestRoadCloudDataTime();

    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(roadcloud_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM ext_obsdata_roadcloud WHERE data_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");
    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of RoadCloud cache failed!");
  }
}

void PostgreSQLCacheDB::cleanNetAtmoCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestNetAtmoDataTime();

    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(netatmo_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM ext_obsdata_netatmo WHERE data_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");

    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of NetAtmo cache failed!");
  }
}

void PostgreSQLCacheDB::cleanBKHydrometaCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestBKHydrometaDataTime();

    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(netatmo_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM ext_obsdata_bk_hydrometa WHERE data_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");

    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of bk_hydrometa cache failed!");
  }
}

void PostgreSQLCacheDB::cleanFmiIoTCache(const boost::posix_time::ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestFmiIoTDataTime();

    if (newstarttime <= oldest)
      return;

    Spine::WriteLock lock(fmi_iot_data_write_mutex);
    std::string sqlStmt = ("DELETE FROM ext_obsdata_fmi_iot WHERE data_time < '" +
                           Fmi::to_iso_extended_string(newstarttime) + "'");

    itsDB.executeNonTransaction(sqlStmt);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of FmiIoT cache failed!");
  }
}

std::size_t PostgreSQLCacheDB::fillDataCache(const DataItems &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    auto transaction = itsDB.transaction();
    transaction->execute("LOCK TABLE observation_data IN SHARE MODE");
    // dropIndex("observation_data_data_time_idx", true);

    while (pos1 < cacheData.size())
    {
      if (Spine::Reactor::isShuttingDown())
        break;
      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2 = 0;
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
            // data_time, modified_last, fmisid, measurand_id, producer_id, measurand_no
            std::string key = Fmi::to_iso_string(item.data_time);
            //            key += Fmi::to_iso_string(item.modified_last);
            key += Fmi::to_string(item.fmisid);
            key += Fmi::to_string(item.sensor_no);
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
              values += Fmi::to_string(item.sensor_no) + ",";
              values += ("'" + Fmi::to_iso_string(item.data_time) + "',");
              values += ("'" + Fmi::to_iso_string(item.modified_last) + "',");
              values += Fmi::to_string(item.measurand_id) + ",";
              values += Fmi::to_string(item.producer_id) + ",";
              values += Fmi::to_string(item.measurand_no) + ",";
              values += item.get_value() + ",";
              values += Fmi::to_string(item.data_quality) + ",";
              values += item.get_data_source() + ")";

              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO observation_data "
                  "(fmisid, sensor_no, data_time, modified_last, measurand_id, producer_id, "
                  "measurand_no, "
                  "data_value, data_quality, data_source) VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }
              sqlStmt +=
                  " ON CONFLICT(data_time, fmisid, sensor_no, measurand_id, producer_id, "
                  "measurand_no) DO "
                  "UPDATE SET "
                  "(data_value, modified_last, data_quality, data_source) = "
                  "(EXCLUDED.data_value, EXCLUDED.modified_last, EXCLUDED.data_quality, "
                  "EXCLUDED.data_source)\n";
              transaction->execute(sqlStmt);
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
    transaction->commit();
    itsDB.executeNonTransaction("VACUUM ANALYZE observation_data");

    return write_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}  // namespace Observation

std::size_t PostgreSQLCacheDB::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData)
{
  try
  {
    if (cacheData.empty())
      return cacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    auto transaction = itsDB.transaction();
    transaction->execute("LOCK TABLE weather_data_qc IN SHARE MODE");
    // dropIndex("weather_data_qc_obstime_idx", true);

    while (pos1 < cacheData.size())
    {
      if (Spine::Reactor::isShuttingDown())
        break;

      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2 = 0;
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
              values += (Fmi::to_string(itsParameterMap->getRoadAndForeignIds().stringToInteger(
                             item.parameter)) +
                         ",");
              values += Fmi::to_string(item.sensor_no) + ",";
              if (item.value)
                values += Fmi::to_string(*item.value) + ",";
              else
                values += "NULL,";
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
              transaction->execute(sqlStmt);
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
    transaction->commit();
    itsDB.executeNonTransaction("VACUUM ANALYZE weather_data_qc");

    return write_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling of WeatherDataQCCache failed!");
  }
}  // namespace Observation

std::size_t PostgreSQLCacheDB::fillFlashDataCache(const FlashDataItems &flashCacheData)
{
  try
  {
    if (flashCacheData.empty())
      return flashCacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    auto transaction = itsDB.transaction();
    transaction->execute("LOCK TABLE flash_data IN SHARE MODE");
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

      std::size_t pos2 = 0;
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
            std::string created_time = Fmi::to_iso_string(item.created);
            std::string modified_last_time = Fmi::to_iso_string(item.modified_last);
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
              values += ("'" + created_time + "',");
              values += ("'" + modified_last_time + "',");
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
                  "timing_indicator, stroke_status, data_source, created, modified_last, "
                  "stroke_location) "
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
                  "timing_indicator, stroke_status, data_source, created, modified_last, "
                  "stroke_location) = "
                  "(EXCLUDED.multiplicity, EXCLUDED.peak_current, EXCLUDED.sensors, "
                  "EXCLUDED.freedom_degree, EXCLUDED.ellipse_angle, EXCLUDED.ellipse_major, "
                  "EXCLUDED.ellipse_minor, EXCLUDED.chi_square, EXCLUDED.rise_time, "
                  "EXCLUDED.ptz_time, EXCLUDED.cloud_indicator, EXCLUDED.angle_indicator, "
                  "EXCLUDED.signal_indicator, EXCLUDED.timing_indicator, "
                  "EXCLUDED.stroke_status, "
                  "EXCLUDED.data_source, EXCLUDED.created, EXCLUDED.modified_last, "
                  "EXCLUDED.stroke_location)";

              transaction->execute(sqlStmt);
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
    transaction->commit();
    itsDB.executeNonTransaction("VACUUM ANALYZE flash_data");

    return write_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Flash data cache update failed!");
  }
}

std::size_t PostgreSQLCacheDB::fillRoadCloudCache(
    const MobileExternalDataItems &mobileExternalCacheData)
{
  try
  {
    if (mobileExternalCacheData.empty())
      return mobileExternalCacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    auto transaction = itsDB.transaction();
    transaction->execute("LOCK TABLE ext_obsdata_roadcloud IN SHARE MODE");

    while (pos1 < mobileExternalCacheData.size())
    {
      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2 = 0;
      for (pos2 = pos1;
           new_hashes.size() < itsMaxInsertSize && pos2 < mobileExternalCacheData.size();
           ++pos2)
      {
        const auto &item = mobileExternalCacheData[pos2];

        auto hash = item.hash_value();

        if (!itsRoadCloudInsertCache.exists(hash))
        {
          new_items.push_back(pos2);
          new_hashes.push_back(hash);
        }
      }

      // Now insert the new items
      if (!new_items.empty())
      {
        Spine::WriteLock lock(roadcloud_data_write_mutex);
        std::vector<std::size_t> mobileDataToUpdate = new_items;

        while (mobileDataToUpdate.size() > 0)
        {
          const auto &last_item = mobileExternalCacheData[mobileDataToUpdate.back()];
          std::vector<std::string> values_vector;
          std::set<std::string> key_set;  // to check duplicates
          std::vector<std::size_t> duplicateMobileObs;

          for (const auto i : mobileDataToUpdate)
          {
            const auto &item = mobileExternalCacheData[i];

            std::string data_time = Fmi::to_iso_string(item.data_time);
            boost::replace_all(data_time, ",", ".");
            std::string key = Fmi::to_string(item.prod_id);
            key += Fmi::to_string(item.mid);
            key += data_time;
            key += Fmi::to_string(item.longitude);
            key += Fmi::to_string(item.latitude);
            //  prod_id, mid, data_time, longitude, latitude
            if (key_set.find(key) != key_set.end())
            {
              duplicateMobileObs.push_back(i);
            }
            else
            {
              key_set.insert(key);

              std::string obs_location =
                  "ST_GeomFromText('POINT(" + Fmi::to_string("%.10g", item.longitude) + " " +
                  Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

              std::string values = "(";
              values += Fmi::to_string(item.prod_id) + ",";
              if (item.station_id)
                values += Fmi::to_string(*item.station_id) + ",";
              else
                values += "NULL,";
              if (item.dataset_id)
                values += "'" + *item.dataset_id + "',";
              else
                values += "NULL,";
              if (item.data_level)
                values += Fmi::to_string(*item.data_level) + ",";
              else
                values += "NULL,";
              values += Fmi::to_string(item.mid) + ",";
              if (item.sensor_no)
                values += Fmi::to_string(*item.sensor_no) + ",";
              else
                values += "NULL,";
              values += "'" + data_time + "',";
              values += Fmi::to_string(item.data_value) + ",";
              if (item.data_value_txt)
                values += "'" + *item.data_value_txt + "',";
              else
                values += "NULL,";
              if (item.data_quality)
                values += Fmi::to_string(*item.data_quality) + ",";
              else
                values += "NULL,";
              if (item.ctrl_status)
                values += Fmi::to_string(*item.ctrl_status) + ",";
              else
                values += "NULL,";
              std::string created = Fmi::to_iso_string(item.created);
              boost::replace_all(created, ",", ".");
              values += "'" + created + "',";
              if (item.altitude)
                values += Fmi::to_string(*item.altitude) + ",";
              else
                values += "NULL,";
              values += obs_location + ")";
              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO ext_obsdata_roadcloud "
                  "(prod_id, station_id, dataset_id, data_level, mid, sensor_no, "
                  "data_time, data_value, data_value_txt, data_quality, ctrl_status, "
                  "created, altitude, geom) "
                  "VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }

              sqlStmt +=
                  " ON CONFLICT(prod_id, mid, data_time, geom) DO "
                  "UPDATE SET "
                  "(station_id, dataset_id, data_level, sensor_no, data_value, data_value_txt, "
                  "data_quality, ctrl_status, created, altitude) = "
                  "(EXCLUDED.station_id, EXCLUDED.dataset_id, EXCLUDED.data_level, "
                  "EXCLUDED.sensor_no, EXCLUDED.data_value, EXCLUDED.data_value_txt, "
                  "EXCLUDED.data_quality, EXCLUDED.ctrl_status, EXCLUDED.created, "
                  "EXCLUDED.altitude)";

              transaction->execute(sqlStmt);
              values_vector.clear();
            }
          }
          mobileDataToUpdate = duplicateMobileObs;
        }
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsRoadCloudInsertCache.add(hash);

      pos1 = pos2;
    }

    transaction->commit();
    itsDB.executeNonTransaction("VACUUM ANALYZE ext_obsdata_roadcloud");

    return write_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "RoadCloud cache update failed!");
  }
}

std::size_t PostgreSQLCacheDB::fillNetAtmoCache(
    const MobileExternalDataItems &mobileExternalCacheData)
{
  try
  {
    if (mobileExternalCacheData.empty())
      return mobileExternalCacheData.size();

    std::size_t pos1 = 0;
    std::size_t write_count = 0;
    auto transaction = itsDB.transaction();
    transaction->execute("LOCK TABLE ext_obsdata_netatmo IN SHARE MODE");

    while (pos1 < mobileExternalCacheData.size())
    {
      // Yield if there is more than 1 block
      if (pos1 > 0)
        boost::this_thread::yield();

      // Collect new items before taking a lock - we might avoid one completely
      std::vector<std::size_t> new_items;
      std::vector<std::size_t> new_hashes;
      new_items.reserve(itsMaxInsertSize);
      new_hashes.reserve(itsMaxInsertSize);

      std::size_t pos2 = 0;
      for (pos2 = pos1;
           new_hashes.size() < itsMaxInsertSize && pos2 < mobileExternalCacheData.size();
           ++pos2)
      {
        const auto &item = mobileExternalCacheData[pos2];

        auto hash = item.hash_value();

        if (!itsNetAtmoInsertCache.exists(hash))
        {
          new_items.push_back(pos2);
          new_hashes.push_back(hash);
        }
      }

      // Now insert the new items
      if (!new_items.empty())
      {
        Spine::WriteLock lock(netatmo_data_write_mutex);
        std::vector<std::size_t> mobileDataToUpdate = new_items;

        while (mobileDataToUpdate.size() > 0)
        {
          const auto &last_item = mobileExternalCacheData[mobileDataToUpdate.back()];
          std::vector<std::string> values_vector;
          std::set<std::string> key_set;  // to check duplicates
          std::vector<std::size_t> duplicateMobileObs;

          for (const auto i : mobileDataToUpdate)
          {
            const auto &item = mobileExternalCacheData[i];

            std::string data_time = Fmi::to_iso_string(item.data_time);
            boost::replace_all(data_time, ",", ".");
            std::string key = Fmi::to_string(item.prod_id);
            key += Fmi::to_string(item.mid);
            key += data_time;
            key += Fmi::to_string(item.longitude);
            key += Fmi::to_string(item.latitude);
            //  prod_id, mid, data_time, longitude, latitude
            if (key_set.find(key) != key_set.end())
            {
              duplicateMobileObs.push_back(i);
            }
            else
            {
              key_set.insert(key);

              std::string obs_location =
                  "ST_GeomFromText('POINT(" + Fmi::to_string("%.10g", item.longitude) + " " +
                  Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

              std::string values = "(";
              values += Fmi::to_string(item.prod_id) + ",";
              if (item.station_id)
                values += Fmi::to_string(*item.station_id) + ",";
              else
                values += "NULL,";
              if (item.dataset_id)
                values += "'" + *item.dataset_id + "',";
              else
                values += "NULL,";
              if (item.data_level)
                values += Fmi::to_string(*item.data_level) + ",";
              else
                values += "NULL,";
              values += Fmi::to_string(item.mid) + ",";
              if (item.sensor_no)
                values += Fmi::to_string(*item.sensor_no) + ",";
              else
                values += "NULL,";
              values += "'" + data_time + "',";
              values += Fmi::to_string(item.data_value) + ",";
              if (item.data_value_txt)
                values += "'" + *item.data_value_txt + "',";
              else
                values += "NULL,";
              if (item.data_quality)
                values += Fmi::to_string(*item.data_quality) + ",";
              else
                values += "NULL,";
              if (item.ctrl_status)
                values += Fmi::to_string(*item.ctrl_status) + ",";
              else
                values += "NULL,";
              std::string created = Fmi::to_iso_string(item.created);
              boost::replace_all(created, ",", ".");
              values += "'" + created + "',";
              if (item.altitude)
                values += Fmi::to_string(*item.altitude) + ",";
              else
                values += "NULL,";
              values += obs_location + ")";
              values_vector.push_back(values);
            }

            if ((values_vector.size() % itsMaxInsertSize == 0) || &item == &last_item)
            {
              std::string sqlStmt =
                  "INSERT INTO ext_obsdata_netatmo "
                  "(prod_id, station_id, dataset_id, data_level, mid, sensor_no, "
                  "data_time, data_value, data_value_txt, data_quality, ctrl_status, "
                  "created, altitude, geom) "
                  "VALUES ";

              for (const auto &v : values_vector)
              {
                sqlStmt += v;
                if (&v != &values_vector.back())
                  sqlStmt += ",";
              }

              sqlStmt +=
                  " ON CONFLICT(prod_id, mid, data_time, geom) DO "
                  "UPDATE SET "
                  "(station_id, dataset_id, data_level, sensor_no, data_value, data_value_txt, "
                  "data_quality, ctrl_status, created, altitude) = "
                  "(EXCLUDED.station_id, EXCLUDED.dataset_id, EXCLUDED.data_level, "
                  "EXCLUDED.sensor_no, EXCLUDED.data_value, EXCLUDED.data_value_txt, "
                  "EXCLUDED.data_quality, EXCLUDED.ctrl_status, EXCLUDED.created, "
                  "EXCLUDED.altitude)";

              transaction->execute(sqlStmt);
              values_vector.clear();
            }
          }
          mobileDataToUpdate = duplicateMobileObs;
        }
      }

      // We insert the new hashes only when the transaction has completed so that
      // if the above code for some reason throws, the rows may be inserted again
      // in a later attempt.

      write_count += new_hashes.size();
      for (const auto &hash : new_hashes)
        itsNetAtmoInsertCache.add(hash);

      pos1 = pos2;
    }

    transaction->commit();
    itsDB.executeNonTransaction("VACUUM ANALYZE ext_obsdata_netatmo");

    return write_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }
}

std::size_t PostgreSQLCacheDB::fillBKHydrometaCache(
    const MobileExternalDataItems & /*mobileExternalCacheData*/)
{
  return 0;
}

std::size_t PostgreSQLCacheDB::fillFmiIoTCache(
    const MobileExternalDataItems & /* mobileExternalCacheData */)
{
  return 0;
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLCacheDB::getRoadCloudData(
    const Settings &settings, const ParameterMapPtr &parameterMap, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, parameterMap, timezones);
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLCacheDB::getNetAtmoData(
    const Settings &settings, const ParameterMapPtr &parameterMap, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, parameterMap, timezones);
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLCacheDB::getFmiIoTData(
    const Settings &settings, const ParameterMapPtr &parameterMap, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, parameterMap, timezones);
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr PostgreSQLCacheDB::getMobileAndExternalData(
    const Settings &settings, const ParameterMapPtr &parameterMap, const Fmi::TimeZones &timezones)
{
  try
  {
    Spine::TimeSeries::TimeSeriesVectorPtr ret = initializeResultVector(settings);

    const ExternalAndMobileProducerMeasurand &producerMeasurand =
        itsExternalAndMobileProducerConfig.at(settings.stationtype);
    std::vector<std::string> queryfields;
    std::vector<int> measurandIds;
    const SmartMet::Engine::Observation::Measurands &measurands = producerMeasurand.measurands();
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
    {
      std::string name = Fmi::ascii_tolower_copy(p.name());
      queryfields.push_back(name);
      if (measurands.find(name) != measurands.end())
        measurandIds.push_back(measurands.at(name));
    }

    SmartMet::Spine::TimeSeriesGeneratorOptions timeSeriesOptions;
    timeSeriesOptions.startTime = settings.starttime;
    timeSeriesOptions.endTime = settings.endtime;
    SmartMet::Spine::TimeSeriesGenerator::LocalTimeList tlist;
    // The desired timeseries, unless all available data if timestep=0 or latest only
    if (!settings.latest && !timeSeriesOptions.all())
    {
      tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
    }

    ExternalAndMobileDBInfo dbInfo(&producerMeasurand);

    std::string sqlStmt = dbInfo.sqlSelectFromCache(
        measurandIds, settings.starttime, settings.endtime, settings.wktArea, settings.dataFilter);

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    SmartMet::Engine::Observation::ResultSetRows rsrs =
        SmartMet::Engine::Observation::PostgreSQLCacheDB::getResultSetForMobileExternalData(
            result_set, itsDB.dataTypes());

    itsTimeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    for (auto rsr : rsrs)
    {
      boost::local_time::local_date_time obstime =
          *(boost::get<boost::local_time::local_date_time>(&rsr["data_time"]));
      unsigned int index = 0;
      for (auto fieldname : queryfields)
      {
        if (fieldname == "created")
        {
          boost::local_time::local_date_time dt =
              *(boost::get<boost::local_time::local_date_time>(&rsr[fieldname]));

          std::string fieldValue = itsTimeFormatter->format(dt);
          ret->at(index).emplace_back(ts::TimedValue(obstime, fieldValue));
        }
        else
        {
          if (measurands.find(fieldname) == measurands.end())
          {
            const auto iter = parameterMap->find(fieldname);
            if (iter != parameterMap->end())
            {
              std::string producer = producerMeasurand.producerId().name();
              if (iter->second.find(producer) != iter->second.end())
              {
                fieldname = iter->second.at(producer);
              }
            }
          }
          else
          {
            fieldname = dbInfo.measurandFieldname(settings.stationtype, measurands.at(fieldname));
          }
          ret->at(index).emplace_back(ts::TimedValue(obstime, rsr[fieldname]));
        }
        index++;
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting mobile data from database failed!");
  }
}

void PostgreSQLCacheDB::addParameterToTimeSeries(
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time, std::map<std::string, ts::Value>> &dataItem,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const ParameterMapPtr &parameterMap,
    const std::string &stationtype,
    const Spine::Station &station,
    const std::string &missingtext)
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
          .emplace_back(ts::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;
      if (special.first.find("windcompass") != std::string::npos)
      {
        // Have to get wind direction first
        std::string winddirectionpos = parameterMap->getParameter("winddirection", stationtype);
        std::string windCompass;
        if (dataItem.second.count(winddirectionpos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          if (special.first == "windcompass8")
            windCompass = windCompass8(boost::get<double>(data.at(winddirectionpos)), missingtext);

          else if (special.first == "windcompass16")
            windCompass = windCompass16(boost::get<double>(data.at(winddirectionpos)), missingtext);

          else if (special.first == "windcompass32")
            windCompass = windCompass32(boost::get<double>(data.at(winddirectionpos)), missingtext);

          ts::Value windCompassValue = ts::Value(windCompass);
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, windCompassValue));
        }
      }
      else if (special.first.find("feelslike") != std::string::npos)
      {
        // Feels like - deduction. This ignores radiation, since it is measured
        // using
        // dedicated stations
        std::string windpos = parameterMap->getParameter("windspeedms", stationtype);
        std::string rhpos = parameterMap->getParameter("relativehumidity", stationtype);
        std::string temppos = parameterMap->getParameter("temperature", stationtype);

        if (data.count(windpos) == 0 || data.count(rhpos) == 0 || data.count(temppos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          auto temp = static_cast<float>(boost::get<double>(data.at(temppos)));
          auto rh = static_cast<float>(boost::get<double>(data.at(rhpos)));
          auto wind = static_cast<float>(boost::get<double>(data.at(windpos)));

          auto feelslike = ts::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, feelslike));
        }
      }
      else if (special.first.find("smartsymbol") != std::string::npos)
      {
        std::string wawapos = parameterMap->getParameter("wawa", stationtype);
        std::string totalcloudcoverpos = parameterMap->getParameter("totalcloudcover", stationtype);
        std::string temppos = parameterMap->getParameter("temperature", stationtype);
        if (data.count(wawapos) == 0 || data.count(totalcloudcoverpos) == 0 ||
            data.count(temppos) == 0)
        {
          ts::Value missing = ts::None();
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, missing));
        }
        else
        {
          auto temp = static_cast<float>(boost::get<double>(data.at(temppos)));
          auto totalcloudcover = static_cast<int>(boost::get<double>(data.at(totalcloudcoverpos)));
          auto wawa = static_cast<int>(boost::get<double>(data.at(wawapos)));
          double lat = station.latitude_out;
          double lon = station.longitude_out;
#ifdef __llvm__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdouble-promotion"
#endif
          ts::Value smartsymbol =
              ts::Value(*calcSmartsymbolNumber(wawa, totalcloudcover, temp, obstime, lat, lon));
#ifdef __llvm__
#pragma clang diagnostic pop
#endif
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, smartsymbol));
        }
      }

      else
      {
        if (boost::algorithm::ends_with(special.first, "data_source"))
        {
          // *data_source fields is hadled outside this function
        }
        else
        {
          addSpecialParameterToTimeSeries(
              special.first, timeSeriesColumns, station, pos, stationtype, obstime);
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding parameter to time series failed!");
  }
}

void PostgreSQLCacheDB::addSpecialParameterToTimeSeries(
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
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, obstime));

    else if (paramname == "station_name" || paramname == "stationname")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.station_formal_name));

    else if (paramname == "fmisid")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.station_id));

    else if (paramname == "geoid")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.geoid));

    else if (paramname == "distance")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.distance));

    else if (paramname == "direction")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.stationDirection));

    else if (paramname == "stationary")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.stationary));

    else if (paramname == "lon" || paramname == "longitude")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.requestedLon));

    else if (paramname == "lat" || paramname == "latitude")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.requestedLat));

    else if (paramname == "stationlon" || paramname == "stationlongitude")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.longitude_out));

    else if (paramname == "stationlat" || paramname == "stationlatitude")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.latitude_out));

    else if (paramname == "elevation" || paramname == "station_elevation")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.station_elevation));

    else if (paramname == "wmo")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).emplace_back(
          ts::TimedValue(obstime, station.wmo > 0 ? station.wmo : missing));
    }
    else if (paramname == "lpnn")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).emplace_back(
          ts::TimedValue(obstime, station.lpnn > 0 ? station.lpnn : missing));
    }
    else if (paramname == "rwsid")
    {
      const ts::Value missing = ts::None();
      timeSeriesColumns->at(pos).emplace_back(
          ts::TimedValue(obstime, station.rwsid > 0 ? station.rwsid : missing));
    }
    else if (paramname == "sensor_no")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, 1));

    else if (paramname == "place")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, station.tag));

    else if (paramname == "model")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, stationtype));

    else if (paramname == "modtime")
      timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(obstime, ""));

    else
    {
      std::string msg =
          "PostgreSQLCacheDB::addSpecialParameterToTimeSeries : "
          "Unsupported special parameter '" +
          paramname + "'";

      Fmi::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg);
      throw exception;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding special parameter to time series failed!");
  }
}

#ifdef LATER
TODO FlashCounts PostgreSQLCacheDB::getFlashCount(const boost::posix_time::ptime &starttime,
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
          sqlStmt += " AND ST_DistanceSphere(ST_GeomFromText('POINT(" + lon + " " + lat +
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
      flashcounts.flashcount = as_int(row[0]);
      flashcounts.strokecount = as_int(row[1]);
      flashcounts.iccount = as_int(row[2]);
    }

    return flashcounts;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash count failed!");
  }
}
#endif

LocationDataItems PostgreSQLCacheDB::readObservations(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const QueryMapping &qmap,
    const std::set<std::string> &stationgroup_codes)
{
  try
  {
    LocationDataItems ret;

    // Safety check
    if (qmap.measurandIds.empty())
      return ret;

    std::string measurand_ids;
    for (const auto &id : qmap.measurandIds)
      measurand_ids += Fmi::to_string(id) + ",";

    measurand_ids.resize(measurand_ids.size() - 1);  // remove last ","

    auto qstations = buildSqlStationList(stations, stationgroup_codes, stationInfo);

    if (qstations.empty())
      return ret;

    std::list<std::string> producer_id_str_list;
    for (auto prodId : settings.producer_ids)
      producer_id_str_list.emplace_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");

    std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
    std::string endtime = Fmi::to_iso_extended_string(settings.endtime);

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM "
        "data.data_time) AS obstime, "
        "measurand_id, data_value, data_quality, data_source "
        "FROM observation_data data "
        "WHERE data.fmisid IN (" +
        qstations +
        ") "
        "AND data.data_time >= '" +
        starttime + "' AND data.data_time <= '" + endtime + "' AND data.measurand_id IN (" +
        measurand_ids + ") ";
    if (!producerIds.empty())
      sqlStmt += ("AND data.producer_id IN (" + producerIds + ") ");

    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               " GROUP BY data.fmisid, data.sensor_no, data.data_time, data.measurand_id, "
               "data.data_value, data.data_quality, data.data_source "
               "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << "PostgreSQL(cache): " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      LocationDataItem obs;
      obs.data.fmisid = as_int(row[0]);
      obs.data.sensor_no = as_int(row[1]);
      obs.data.data_time = boost::posix_time::from_time_t(row[2].as<time_t>());
      obs.data.measurand_id = as_int(row[3]);
      if (!row[4].is_null())
        obs.data.data_value = as_double(row[4]);
      if (!row[5].is_null())
        obs.data.data_quality = as_int(row[5]);
      if (!row[6].is_null())
        obs.data.data_source = as_int(row[6]);
      // Get latitude, longitude, elevation from station info
      const Spine::Station &s = stationInfo.getStation(obs.data.fmisid, stationgroup_codes);
      obs.latitude = s.latitude_out;
      obs.longitude = s.longitude_out;
      obs.elevation = s.station_elevation;
      const StationLocation &sloc =
          stationInfo.stationLocations.getLocation(obs.data.fmisid, obs.data.data_time);
      // Get exact location, elevation
      if (sloc.location_id != -1)
      {
        obs.latitude = sloc.latitude;
        obs.longitude = sloc.longitude;
        obs.elevation = sloc.elevation;
      }

      ret.emplace_back(obs);
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading observations from PostgreSQL database failed!");
  }
}

void PostgreSQLCacheDB::createIndex(const std::string &table,
                                    const std::string &column,
                                    const std::string &idx_name,
                                    bool transaction /* false */) const
{
  try
  {
    itsDB.execute("CREATE INDEX IF NOT EXISTS " + idx_name + " ON " + table + "(" + column + ")");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creating index " + idx_name + " failed!");
  }
}

#if 0
void PostgreSQLCacheDB::dropIndex(const std::string &idx_name, bool transaction /*= false*/) const
{
  try
  {
    itsDB.execute("DROP INDEX IF EXISTS " + idx_name);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Dropping index " + idx_name + " failed!");
  }
}
#endif

ResultSetRows PostgreSQLCacheDB::getResultSetForMobileExternalData(
    const pqxx::result &pgResultSet, const std::map<unsigned int, std::string> &pgDataTypes)
{
  ResultSetRows ret;

  if (pgResultSet.size() == 0)
    return ret;

  try
  {
    unsigned int nColumns = pgResultSet.columns();

    for (auto row : pgResultSet)
    {
      ResultSetRow rsr;
      for (unsigned int i = 0; i < nColumns; i++)
      {
        std::string data_type = pgDataTypes.at(row.column_type(i));
        std::string column_name = pgResultSet.column_name(i);
        ts::Value val = ts::None();
        if (!row[i].is_null())
        {
          if (data_type == "text" || data_type == "varchar")
          {
            val = row[i].as<std::string>();
          }
          else if (data_type == "float4" || data_type == "float8" || data_type == "_float4" ||
                   data_type == "_float8" || data_type == "numeric")
          {
            if (column_name == "created" || column_name == "data_time")
            {
              boost::posix_time::ptime pt = epoch2ptime(as_double(row[i]));
              boost::local_time::time_zone_ptr zone(new posix_time_zone("UTC"));
              val = boost::local_time::local_date_time(pt, zone);
            }
            else
            {
              val = as_double(row[i]);
            }
          }
          else if (data_type == "int2" || data_type == "int4" || data_type == "int8" ||
                   data_type == "_int2" || data_type == "_int4" || data_type == "_int8")
          {
            val = as_int(row[i]);
          }
          else if (data_type == "timestamp")
          {
            boost::posix_time::ptime pt = epoch2ptime(as_double(row[i]));
            boost::local_time::time_zone_ptr zone(new posix_time_zone("UTC"));
            val = boost::local_time::local_date_time(pt, zone);
          }
        }
        rsr.insert(std::make_pair(column_name, val));
      }

      ret.push_back(rsr);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Result set handling of mobile data failed!");
  }

  return ret;
}

void PostgreSQLCacheDB::fetchWeatherDataQCData(const std::string &sqlStmt,
                                               const StationInfo &stationInfo,
                                               const std::set<std::string> &stationgroup_codes,
                                               const QueryMapping & /* qmap */,
                                               WeatherDataQCData &cacheData)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      boost::optional<int> fmisid = as_int(row[0]);
      boost::posix_time::ptime obstime = boost::posix_time::from_time_t(row[1].as<time_t>());
      boost::optional<int> parameter = as_int(row[2]);

      // Get latitude, longitude, elevation from station info
      const Spine::Station &s = stationInfo.getStation(*fmisid, stationgroup_codes);

      boost::optional<double> latitude = s.latitude_out;
      boost::optional<double> longitude = s.longitude_out;
      boost::optional<double> elevation = s.station_elevation;

      const StationLocation &sloc = stationInfo.stationLocations.getLocation(*fmisid, obstime);
      // Get exact location, elevation
      if (sloc.location_id != -1)
      {
        latitude = sloc.latitude;
        longitude = sloc.longitude;
        elevation = sloc.elevation;
      }

      boost::optional<double> data_value;
      boost::optional<int> data_quality;
      boost::optional<int> sensor_no;
      if (!row[3].is_null())
        data_value = row[3].as<double>();
      if (!row[4].is_null())
        sensor_no = as_int(row[4]);
      if (!row[5].is_null())
        data_quality = as_int(row[5]);

      cacheData.fmisidsAll.push_back(fmisid);
      cacheData.obstimesAll.push_back(obstime);
      cacheData.latitudesAll.push_back(latitude);
      cacheData.longitudesAll.push_back(longitude);
      cacheData.elevationsAll.push_back(elevation);
      cacheData.parametersAll.push_back(parameter);
      cacheData.data_valuesAll.push_back(data_value);
      cacheData.sensor_nosAll.push_back(sensor_no);
      cacheData.data_qualityAll.push_back(data_quality);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Fetching data from PostgreSQL WeatherDataQCData cache failed!");
  }
}

std::string PostgreSQLCacheDB::sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                              const std::string &params,
                                                              const std::string &station_ids) const
{
  try
  {
    std::string sqlStmt;

    if (settings.latest)
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM MAX(data.obstime)) AS obstime, "
          "data.parameter, data.value, data.sensor_no, data.flag as data_quality "
          "FROM weather_data_qc data "
          "WHERE data.fmisid IN (" +
          station_ids +
          ") "
          "AND data.obstime BETWEEN '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + params +
          ") AND " + settings.dataFilter.getSqlClause("data_quality", "data.flag") +
          " GROUP BY data.fmisid, data.parameter, data.value, data.sensor_no, data.flag "
          "ORDER BY fmisid ASC, obstime ASC";
    }
    else
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, EXTRACT(EPOCH FROM data.obstime) AS obstime, "
          "data.parameter, data.value, data.sensor_no, data.flag as data_quality "
          "FROM weather_data_qc data "
          "WHERE data.fmisid IN (" +
          station_ids +
          ") "
          "AND data.obstime BETWEEN '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + params +
          ") AND " + settings.dataFilter.getSqlClause("data_quality", "data.flag") +
          " GROUP BY data.fmisid, data.obstime, data.parameter, data.sensor_no "
          "ORDER BY fmisid ASC, obstime ASC";
    }

    if (itsDebug)
      std::cout << "PostgreSQL(cache): " << sqlStmt << std::endl;

    return sqlStmt;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Constructing SQL statement for PostgreSQL cache query failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
