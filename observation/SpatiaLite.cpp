#include "SpatiaLite.h"

#include "ExternalAndMobileDBInfo.h"
#include "Keywords.h"
#include "ObservationsMap.h"
#include "QueryMapping.h"
#include "SpatiaLiteCacheParameters.h"

#include <fmt/format.h>
#include <gdal/ogr_geometry.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Convenience.h>
#include <spine/Exception.h>
#include <spine/ParameterTools.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <spine/TimeSeriesOutput.h>

#include <chrono>
#include <iostream>

#ifdef __llvm__
#pragma clang diagnostic push
// Exceptions have unused parameters here
#pragma clang diagnostic ignored "-Wunused-exception-parameter"
// long long should be allowed
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#endif

namespace BO = SmartMet::Engine::Observation;
namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

using boost::local_time::local_date_time;
using boost::posix_time::ptime;

ptime parse_sqlite_time(std::string timestring)
{
  try
  {
    if (timestring.find("T") != std::string::npos)
      timestring.replace(timestring.find("T"), 1, " ");

    return Fmi::TimeParser::parse_sql(timestring);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(
        BCP, "Parsing sqlite time from string '" + timestring + "' failed!");
  }
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
// Results read from the sqlite database

LocationDataItems SpatiaLite::readObservations(const Spine::Stations &stations,
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
    for (auto &prodId : settings.producer_ids)
      producer_id_str_list.push_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");

    std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
    std::string endtime = Fmi::to_iso_extended_string(settings.endtime);

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, data.sensor_no AS sensor_no, data.data_time AS obstime, "
        "measurand_id, data_value, data_quality, data_source FROM observation_data data "
        "WHERE data.fmisid IN (" +
        qstations +
        ") "
        "AND data.data_time >= '" +
        starttime + "' AND data.data_time <= '" + endtime + "' AND data.measurand_id IN (" +
        measurand_ids + ") ";
    if (!producerIds.empty())
      sqlStmt += ("AND data.producer_id IN (" + producerIds + ") ");

    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.sqlDataFilter.getSqlClause("data_quality", "data.data_quality") +
               " GROUP BY data.fmisid, data.sensor_no, data.data_time, data.measurand_id, "
               "data.data_value, data.data_quality, data.data_source "
               "ORDER BY fmisid ASC, obstime ASC";

    //    std::cout << "sql\n" << sql << std::endl;

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    std::map<int, std::map<int, int>> default_sensors;

    for (auto iter = qry.begin(); iter != qry.end(); ++iter)
    {
      LocationDataItem obs;
      obs.data.data_time = parse_sqlite_time((*iter).get<std::string>(2));
      obs.data.fmisid = (*iter).get<int>(0);
      obs.data.sensor_no = (*iter).get<int>(1);
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

      obs.data.measurand_id = (*iter).get<int>(3);
      if ((*iter).column_type(4) != SQLITE_NULL)
        obs.data.data_value = (*iter).get<double>(4);
      if ((*iter).column_type(5) != SQLITE_NULL)
        obs.data.data_quality = (*iter).get<int>(5);
      if ((*iter).column_type(6) != SQLITE_NULL)
        obs.data.data_source = (*iter).get<int>(5);

      if (qmap.isDefaultSensor(obs.data.sensor_no, obs.data.measurand_id))
      {
        default_sensors[obs.data.fmisid][obs.data.measurand_id] = obs.data.sensor_no;
      }

      ret.emplace_back(obs);
    }
    ret.default_sensors = default_sensors;

    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP,
                                            "Reading observations from sqlite database failed!");
  }
}

SpatiaLite::SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options)
    : CommonDatabaseFunctions(options.stationtypeConfig, options.parameterMap),
      itsMaxInsertSize(options.maxInsertSize),
      itsExternalAndMobileProducerConfig(options.externalAndMobileProducerConfig),
      itsShutdownRequested(false)
{
  try
  {
    srid = "4326";

    // Enabling shared cache may decrease read performance:
    // https://manski.net/2012/10/sqlite-performance/
    // However, for a single shared db it may be better to share:
    // https://github.com/mapnik/mapnik/issues/797

    int flags = (SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE |
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

    std::string tempStorePragma = "PRAGMA temp_store=" + options.sqlite.temp_store;
    itsDB.execute(tempStorePragma.c_str());

    std::string sharedCachePragma =
        "PRAGMA schared_cache=" + Fmi::to_string(static_cast<int>(options.sqlite.shared_cache));
    itsDB.execute(sharedCachePragma.c_str());

    std::string readUncommittedPragma =
        "PRAGMA read_uncommitted=" +
        Fmi::to_string(static_cast<int>(options.sqlite.read_uncommitted));
    itsDB.execute(readUncommittedPragma.c_str());

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

void SpatiaLite::createTables(const std::set<std::string> &tables)
{
  try
  {
    // No locking needed during initialization phase
    initSpatialMetaData();
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
    if (tables.find(FMI_IOT_DATA_TABLE) != tables.end())
      createFmiIoTDataTable();
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

void SpatiaLite::createObservationDataTable()
{
  try
  {
    // Note: it is important that fmisid is first in the primary key, using data_time instead
    // can make the table more than 100 times slower. Putting data_time last had no obvious
    // benefit, putting it second provided the fastest search in a handful of tests.
    // clang-format off
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS observation_data("
        "fmisid INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "data_time DATETIME NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "data_source INTEGER, "
		"modified_last DATETIME NOT NULL DEFAULT '1970-01-01', "
        "PRIMARY KEY (fmisid, data_time, measurand_id, producer_id, measurand_no, sensor_no))");

    itsDB.execute("CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON observation_data(data_time)");
    itsDB.execute("CREATE INDEX IF NOT EXISTS observation_data_modified_last_idx ON observation_data(modified_last)");
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of observation_data table failed!");
  }

  bool data_source_column_exists = false;
  bool modified_last_column_exists = false;
  try
  {
    sqlite3pp::query qry(itsDB, "PRAGMA table_info(observation_data)");
    for (auto row : qry)
    {
      if (row.get<std::string>(1) == "data_source")
        data_source_column_exists = true;
      else if (row.get<std::string>(1) == "modified_last")
        modified_last_column_exists = true;
    }
  }
  catch (const std::exception &e)
  {
    throw Spine::Exception::Trace(BCP, "PRAGMA table_info failed!");
  }

  try
  {
    if (!data_source_column_exists)
      itsDB.execute("ALTER TABLE observation_data ADD COLUMN data_source INTEGER");
  }
  catch (const std::exception &e)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Failed to add data_source column to observation_data TABLE!");
  }

  try
  {
    // if we expand an old table, we just make an educated guess for the modified_last column
    if (!modified_last_column_exists)
    {
      std::cout << Spine::log_time_str() << " [SpatiaLite] Adding modified_last column to observation_data table" << std::endl;
      itsDB.execute("ALTER TABLE observation_data ADD COLUMN modified_last DATETIME NOT NULL DEFAULT '1970-01-01'");
      std::cout << Spine::log_time_str() << " [SpatiaLite] ... Updating all modified_last columns in observation_data table" << std::endl;
      itsDB.execute("UPDATE observation_data SET modified_last=data_time");
      std::cout << Spine::log_time_str() << " [SpatiaLite] ... Creating modified_last index in observation_data table" << std::endl;
      itsDB.execute("CREATE INDEX observation_data_modified_last_idx ON observation_data(modified_last)");
      std::cout << Spine::log_time_str() << " [SpatiaLite] modified_last processing done" << std::endl;
    }
  }
  catch (const std::exception &e)
  {
    throw Spine::Exception::Trace(BCP,
                                  "Failed to add modified_last column to observation_data TABLE!");
  }
}

void SpatiaLite::createWeatherDataQCTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.execute("CREATE TABLE IF NOT EXISTS weather_data_qc ("
                  "fmisid INTEGER NOT NULL, "
                  "obstime DATETIME NOT NULL, "
                  "parameter TEXT NOT NULL, "
                  "sensor_no INTEGER NOT NULL, "
                  "value REAL NOT NULL, "
                  "flag INTEGER NOT NULL, "
                  "PRIMARY KEY (obstime, fmisid, parameter, sensor_no)); CREATE INDEX IF "
                  "NOT EXISTS weather_data_qc_obstime_idx ON "
                  "weather_data_qc(obstime);");
    itsDB.execute("CREATE INDEX IF NOT EXISTS weather_data_qc_obstime_idx ON weather_data_qc (obstime);");
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
    itsDB.execute("CREATE TABLE IF NOT EXISTS flash_data("
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
                  "data_source INTEGER, "
                  "created  DATETIME, "
                  "modified_last DATETIME, "
                  "modified_by INTEGER, "
                  "PRIMARY KEY (stroke_time, stroke_time_fraction, flash_id)); CREATE "
                  "INDEX IF NOT EXISTS flash_data_stroke_time_idx ON "
                  "flash_data(stroke_time)");
    itsDB.execute("CREATE INDEX IF NOT EXISTS flash_data_stroke_time_idx on flash_data(stroke_time);");

    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(stroke_location) AS latitude FROM flash_data LIMIT 1");
	  qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('flash_data', 'stroke_location', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
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

    if (spatial_index_enabled == 0)
    {
	  std::cout << Spine::log_time_str() << " [SpatiaLite] Adding spatial index to flash_data table" << std::endl;
	  itsDB.execute("SELECT CreateSpatialIndex('flash_data', 'stroke_location')");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of flash_data table failed!");
  }

  bool data_source_column_exists = false;
  try
  {
    sqlite3pp::query qry(itsDB, "PRAGMA table_info(flash_data)");
    for (auto row : qry)
    {
      if (row.get<std::string>(1) == "data_source")
      {
        data_source_column_exists = true;
        break;
      }
    }
  }
  catch (const std::exception &e)
  {
    throw Spine::Exception::Trace(BCP, "PRAGMA table_info failed!");
  }

  if (!data_source_column_exists)
  {
    try
    {
      itsDB.execute("ALTER TABLE flash_data ADD COLUMN data_source INTEGER");
    }
    catch (const std::exception &e)
    {
      throw Spine::Exception::Trace(BCP, "Failed to add data_source_column to flash_data TABLE!");
    }
  }
}

void SpatiaLite::createRoadCloudDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS ext_obsdata_roadcloud("
                           "prod_id INTEGER, "
                           "station_id INTEGER, "
                           "dataset_id character VARYING(50), "
                           "data_level INTEGER, "
                           "mid INTEGER, "
                           "sensor_no INTEGER, "
                           "data_time timestamp without time zone NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created timestamp without time zone, "
                           "altitude REAL)");

    cmd.execute();
    xct.commit();

    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(geom) AS latitude FROM ext_obsdata_roadcloud LIMIT 1");
      qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('ext_obsdata_roadcloud', 'geom', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
      itsDB.execute("ALTER TABLE ext_obsdata ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='ext_obsdata_roadcloud' AND f_geometry_column = "
                         "'geom'");
    int spatial_index_enabled = 0;

    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (spatial_index_enabled == 0)
    {
	  std::cout << Spine::log_time_str() << " [SpatiaLite] Adding spatial index to ext_obsdata_roadcloud table" << std::endl;
	  itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_roadcloud', 'geom')");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of ext_obsdata_roadcloud table failed!");
  }
}

void SpatiaLite::createNetAtmoDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS ext_obsdata_netatmo("
                           "prod_id INTEGER, "
                           "station_id INTEGER, "
                           "dataset_id character VARYING(50), "
                           "data_level INTEGER, "
                           "mid INTEGER, "
                           "sensor_no INTEGER, "
                           "data_time timestamp without time zone NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created timestamp without time zone, "
                           "altitude REAL)");

    cmd.execute();
    xct.commit();

    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(geom) AS latitude FROM ext_obsdata_netatmo LIMIT 1");
      qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('ext_obsdata_netatmo', 'geom', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
      itsDB.execute(
          "ALTER TABLE ext_obsdata_netatmo ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='ext_obsdata_netatmo' AND f_geometry_column = "
                         "'geom'");
    int spatial_index_enabled = 0;
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (spatial_index_enabled == 0)
    {
	  std::cout << Spine::log_time_str() << " [SpatiaLite] Adding spatial index to ext_obsdata_netatmo table" << std::endl;
	  itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_netatmo', 'geom')");
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of ext_obsdata_netatmo table failed!");
  }
}

void SpatiaLite::createFmiIoTDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS ext_obsdata_fmi_iot("
                           "prod_id INTEGER, "
                           "station_id INTEGER, "
                           "station_code character VARYING(256), "
                           "dataset_id character VARYING(50), "
                           "data_level INTEGER, "
                           "mid INTEGER, "
                           "sensor_no INTEGER, "
                           "data_time timestamp without time zone NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created timestamp without time zone, "
                           "altitude REAL, "
						   "PRIMARY KEY (prod_id,station_id,station_code,mid,data_time))");

    cmd.execute();
    xct.commit();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Creation of ext_obsdata_fmi_iot table failed!");
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
    throw Spine::Exception::Trace(BCP, "SQL-query failed: " + queryString);
  }
}

ptime SpatiaLite::getLatestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest observation time query failed!");
  }
}

ptime SpatiaLite::getLatestObservationModifiedTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(modified_last) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Modified last observation time query failed!");
  }
}

ptime SpatiaLite::getOldestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest observation time query failed!");
  }
}

ptime SpatiaLite::getLatestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);
    sqlite3pp::query qry(itsDB, "SELECT MAX(obstime) FROM weather_data_qc");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest WeatherDataQCTime query failed!");
  }
}

ptime SpatiaLite::getOldestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(obstime) FROM weather_data_qc");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest WeatherDataQCTime query failed!");
  }
}

ptime SpatiaLite::getLatestFlashTime()
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

ptime SpatiaLite::getOldestFlashTime()
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

ptime SpatiaLite::getOldestRoadCloudDataTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest RoadCloud time query failed!");
  }
}

ptime SpatiaLite::getLatestRoadCloudDataTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest RoadCloud data time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestRoadCloudCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest RoadCloud creaed time query failed!");
  }
}

ptime SpatiaLite::getOldestNetAtmoDataTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest NetAtmo data time query failed!");
  }
}

ptime SpatiaLite::getLatestNetAtmoDataTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest NetAtmo data time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestNetAtmoCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_netatmo";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest NetAtmo created time query failed!");
  }
}

ptime SpatiaLite::getOldestFmiIoTDataTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Oldest FmiIoT data time query failed!");
  }
}

ptime SpatiaLite::getLatestFmiIoTDataTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest FmiIoT data time query failed!");
  }
}

boost::posix_time::ptime SpatiaLite::getLatestFmiIoTCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_fmi_iot";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Latest FmiIoT created time query failed!");
  }
}

ptime SpatiaLite::getLatestTimeFromTable(const std::string& tablename,
                                                            const std::string& time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT DATETIME(MAX(" + time_field + ")) FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    return boost::posix_time::not_a_date_time;
  }
}

ptime SpatiaLite::getOldestTimeFromTable(const std::string& tablename,
                                                            const std::string& time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT DATETIME(MIN(" + time_field + ")) FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;
    return parseSqliteTime(iter, 0);
  }
  catch (...)
  {
    return boost::posix_time::not_a_date_time;
  }
}

void SpatiaLite::cleanDataCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestObservationTime();
    if (newstarttime <= oldest)
      return;

    auto timestring = Fmi::to_iso_extended_string(newstarttime);

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

void SpatiaLite::cleanMemoryDataCache(const ptime &newstarttime)
{
  try
  {
    if(itsObservationMemoryCache)
      itsObservationMemoryCache->clean(newstarttime);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of memory data cache failed!");
  }
}

void SpatiaLite::cleanWeatherDataQCCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestWeatherDataQCTime();
    if (newstarttime <= oldest)
      return;

    auto timestring = Fmi::to_iso_extended_string(newstarttime);

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

void SpatiaLite::cleanFlashDataCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestFlashTime();
    if (newstarttime <= oldest)
      return;

    auto timestring = Fmi::to_iso_extended_string(newstarttime);

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

void SpatiaLite::cleanRoadCloudCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestRoadCloudDataTime();

    if (newstarttime <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(newstarttime);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB,
                           "DELETE FROM ext_obsdata_roadcloud WHERE data_time < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of RoadCloud cache failed!");
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getRoadCloudData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanNetAtmoCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestNetAtmoDataTime();
    if (newstarttime <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(newstarttime);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM ext_obsdata_netatmo WHERE data_time < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of NetAtmno cache failed!");
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getNetAtmoData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanFmiIoTCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestFmiIoTDataTime();
    if (newstarttime <= oldest)
      return;

    std::string timestring = Fmi::to_iso_extended_string(newstarttime);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM ext_obsdata_fmi_iot WHERE data_time < :timestring");
    cmd.bind(":timestring", timestring, sqlite3pp::nocopy);
    cmd.execute();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Cleaning of FmiIoT cache failed!");
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getFmiIoTData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}


SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getMobileAndExternalData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  try
  {
    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr ret =
        initializeResultVector(settings.parameters);

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

    std::string sqlStmt = dbInfo.sqlSelectFromCache(measurandIds,
                                                    settings.starttime,
                                                    settings.endtime,
                                                    settings.wktArea,
                                                    settings.sqlDataFilter,
                                                    true);

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    if (qry.begin() == qry.end())
      return ret;

	int column_count = qry.column_count();

  	for (auto row : qry)
     {
	   map<std::string, ts::Value> result;
	   boost::local_time::time_zone_ptr zone(new posix_time_zone("UTC"));
	   boost::local_time::local_date_time timestep(not_a_date_time, zone);
	   for(int i = 0; i < column_count; i++)
		 {
		   std::string column_name = qry.column_name(i);

		   int data_type = row.column_type(i);
		   ts::Value value = ts::None();
          if (data_type == SQLITE_TEXT)
          {
			std::string data_value = row.get<std::string>(i);
			if(column_name == "data_time" || column_name == "created")
			  {
				data_value.resize(19);
				boost::posix_time::ptime pt = Fmi::TimeParser::parse_iso(data_value);
				boost::local_time::local_date_time ldt(pt, zone);
				value = ldt;
				if(column_name == "data_time")
				  timestep = ldt;
			  }
			else
			  {
				value = data_value;
			  }
          }
          else if (data_type == SQLITE_FLOAT)
          {
            value = row.get<double>(i);
          }
          else if (data_type == SQLITE_INTEGER)
          {
			if(column_name != "prod_id" && column_name != "station_id" && column_name != "data_level" && column_name != "mid" && column_name != "sensor_no" && column_name != "data_quality" && column_name != "ctrl_status")
			  value = row.get<double>(i);
			else
			  value = row.get<int>(i);
          }
		  result[column_name] = value;
		 }

	   unsigned int index = 0;
	   for(auto paramname : queryfields)
		 {			 
		   ts::Value val = result[paramname];
		   ret->at(index).push_back(ts::TimedValue(timestep, val));
		   index++;
		 }
	 }

    return ret;
	}
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting mobile and external data from cache failed!");
  }
}

std::size_t SpatiaLite::fillDataCache(const DataItems &cacheData, InsertStatus &insertStatus)
{
  try
  {
    if (cacheData.empty())
      return 0;

    // Update memory cache first

    if(itsObservationMemoryCache)
      itsObservationMemoryCache->fill(cacheData);

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < cacheData.size(); i++)
    {
      const auto &item = cacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (itsShutdownRequested)
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    const char *sqltemplate =
        "INSERT OR REPLACE INTO observation_data "
        "(fmisid, sensor_no, measurand_id, producer_id, measurand_no, data_time, modified_last, "
        "data_value, data_quality, data_source) "
        "VALUES "
        "(:fmisid,:sensor_no,:measurand_id,:producer_id,:measurand_no,:data_time, :modified_last, "
        ":data_value,:data_quality,:"
        "data_source)"
        ";";

    std::size_t pos1 = 0;

    // block other writers

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (itsShutdownRequested)
        return 0;

      sqlite3pp::transaction xct(itsDB);
      sqlite3pp::command cmd(itsDB, sqltemplate);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = cacheData[new_items[i]];
        cmd.bind(":fmisid", item.fmisid);
        cmd.bind(":sensor_no", item.sensor_no);
        cmd.bind(":measurand_id", item.measurand_id);
        cmd.bind(":producer_id", item.producer_id);
        cmd.bind(":measurand_no", item.measurand_no);
        std::string data_time_str = Fmi::to_iso_extended_string(item.data_time);
        cmd.bind(":data_time", data_time_str, sqlite3pp::nocopy);
        std::string modified_last_str = Fmi::to_iso_extended_string(item.modified_last);
        cmd.bind(":modified_last", modified_last_str, sqlite3pp::nocopy);
        cmd.bind(":data_value", item.data_value);
        cmd.bind(":data_quality", item.data_quality);
        cmd.bind(":data_source", item.data_source);
        cmd.execute();
        cmd.reset();
      }
      xct.commit();

      pos1 = pos2;
    }

    for (const auto &hash : new_hashes)
      insertStatus.add(hash);

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}

std::size_t SpatiaLite::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData,
                                               InsertStatus &insertStatus)
{
  try
  {
    if (cacheData.empty())
      return 0;

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < cacheData.size(); i++)
    {
      const auto &item = cacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (itsShutdownRequested)
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    const char *sqltemplate =
        "INSERT OR IGNORE INTO weather_data_qc"
        "(fmisid, obstime, parameter, sensor_no, value, flag)"
        "VALUES (:fmisid,:obstime,:parameter,:sensor_no,:value,:flag)";

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (itsShutdownRequested)
        return 0;

      sqlite3pp::transaction xct(itsDB);
      sqlite3pp::command cmd(itsDB, sqltemplate);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = cacheData[new_items[i]];

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

      pos1 = pos2;
    }

    for (const auto &hash : new_hashes)
      insertStatus.add(hash);

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Filling of WeatherDataQCCache failed!");
  }
}

std::size_t SpatiaLite::fillFlashDataCache(const FlashDataItems &flashCacheData,
                                           InsertStatus &insertStatus)
{
  try
  {
    if (flashCacheData.empty())
      return 0;

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < flashCacheData.size(); i++)
    {
      const auto &item = flashCacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (itsShutdownRequested)
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (itsShutdownRequested)
        return 0;

      sqlite3pp::transaction xct(itsDB);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = flashCacheData[new_items[i]];

        std::string stroke_location = "GeomFromText('POINT(" +
                                      Fmi::to_string("%.10g", item.longitude) + " " +
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
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating flash data: " << e.what() << std::endl;
        }
      }
      xct.commit();

      pos1 = pos2;
    }

    for (const auto &hash : new_hashes)
      insertStatus.add(hash);

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Flash data cache update failed!");
  }
}

std::size_t SpatiaLite::fillRoadCloudCache(
    const MobileExternalDataItems &mobileExternalCacheData, InsertStatus &insertStatus)
{
  try
  {
    if (mobileExternalCacheData.empty())
      return 0;

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < mobileExternalCacheData.size(); i++)
    {
      const auto &item = mobileExternalCacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (itsShutdownRequested)
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (itsShutdownRequested)
        return 0;

      sqlite3pp::transaction xct(itsDB);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = mobileExternalCacheData[new_items[i]];

        std::string obs_location = "GeomFromText('POINT(" +
                                   Fmi::to_string("%.10g", item.longitude) + " " +
                                   Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

        std::string sqlStmt =
            "INSERT OR IGNORE INTO ext_obsdata_roadcloud "
            "(prod_id, station_id, dataset_id, data_level, mid, sensor_no, "
            "data_time, data_value, data_value_txt, data_quality, ctrl_status, "
            "created, altitude, geom) "
            "VALUES ("
            ":prod_id, "
            ":station_id, "
            ":dataset_id,"
            ":data_level,"
            ":mid,"
            ":sensor_no,"
            ":data_time,"
            ":data_value,"
            ":data_value_txt,"
            ":data_quality,"
            ":ctrl_status,"
            ":created,"
            ":altitude," +
            obs_location + ");";

        sqlite3pp::command cmd(itsDB, sqlStmt.c_str());

        try
        {
          cmd.bind(":prod_id", item.prod_id);
          if (item.station_id)
            cmd.bind(":station_id", *item.station_id);
          else
            cmd.bind(":station_id");
          if (item.dataset_id)
            cmd.bind(":dataset_id", *item.dataset_id, sqlite3pp::nocopy);
          else
            cmd.bind(":dataset_id");
          if (item.data_level)
            cmd.bind(":data_level", *item.data_level);
          else
            cmd.bind(":data_level");
          cmd.bind(":mid", item.mid);
          if (item.sensor_no)
            cmd.bind(":sensor_no", *item.sensor_no);
          else
            cmd.bind(":sensor_no");
          std::string data_time = Fmi::to_iso_extended_string(item.data_time);
          boost::replace_all(data_time, ",", ".");
          cmd.bind(":data_time", data_time, sqlite3pp::nocopy);
          cmd.bind(":data_value", item.data_value);
          if (item.data_value_txt)
            cmd.bind(":data_value_txt", *item.data_value_txt, sqlite3pp::nocopy);
          else
            cmd.bind(":data_value_txt");
          if (item.data_quality)
            cmd.bind(":data_quality", *item.data_quality);
          else
            cmd.bind(":data_quality");
          if (item.ctrl_status)
            cmd.bind(":ctrl_status", *item.ctrl_status);
          else
            cmd.bind(":ctrl_status");
          std::string created = Fmi::to_iso_extended_string(item.created);
          boost::replace_all(created, ",", ".");
          cmd.bind(":created", created, sqlite3pp::nocopy);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating RoadCloud cache: " << e.what() << std::endl;
        }
      }
      xct.commit();

      pos1 = pos2;
    }

    for (const auto &hash : new_hashes)
      insertStatus.add(hash);

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "RoadCloud cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillNetAtmoCache(
    const MobileExternalDataItems &mobileExternalCacheData, InsertStatus &insertStatus)
{
  try
  {
    if (mobileExternalCacheData.empty())
      return 0;

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < mobileExternalCacheData.size(); i++)
    {
      const auto &item = mobileExternalCacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (itsShutdownRequested)
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (itsShutdownRequested)
        return 0;

      sqlite3pp::transaction xct(itsDB);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = mobileExternalCacheData[new_items[i]];

        std::string obs_location = "GeomFromText('POINT(" +
                                   Fmi::to_string("%.10g", item.longitude) + " " +
                                   Fmi::to_string("%.10g", item.latitude) + ")', " + srid + ")";

        std::string sqlStmt =
            "INSERT OR IGNORE INTO ext_obsdata_netatmo "
            "(prod_id, station_id, dataset_id, data_level, mid, sensor_no, "
            "data_time, data_value, data_value_txt, data_quality, ctrl_status, "
            "created, altitude, geom) "
            "VALUES ("
            ":prod_id, "
            ":station_id, "
            ":dataset_id,"
            ":data_level,"
            ":mid,"
            ":sensor_no,"
            ":data_time,"
            ":data_value,"
            ":data_value_txt,"
            ":data_quality,"
            ":ctrl_status,"
            ":created,"
            ":altitude," +
            obs_location + ");";

        sqlite3pp::command cmd(itsDB, sqlStmt.c_str());

        try
        {
          cmd.bind(":prod_id", item.prod_id);
          if (item.station_id)
            cmd.bind(":station_id", *item.station_id);
          else
            cmd.bind(":station_id");
          if (item.dataset_id)
            cmd.bind(":dataset_id", *item.dataset_id, sqlite3pp::nocopy);
          else
            cmd.bind(":dataset_id");
          if (item.data_level)
            cmd.bind(":data_level", *item.data_level);
          else
            cmd.bind(":data_level");
          cmd.bind(":mid", item.mid);
          if (item.sensor_no)
            cmd.bind(":sensor_no", *item.sensor_no);
          else
            cmd.bind(":sensor_no");
          std::string data_time = Fmi::to_iso_extended_string(item.data_time);
          boost::replace_all(data_time, ",", ".");
          cmd.bind(":data_time", data_time, sqlite3pp::nocopy);
          cmd.bind(":data_value", item.data_value);
          if (item.data_value_txt)
            cmd.bind(":data_value_txt", *item.data_value_txt, sqlite3pp::nocopy);
          else
            cmd.bind(":data_value_txt");
          if (item.data_quality)
            cmd.bind(":data_quality", *item.data_quality);
          else
            cmd.bind(":data_quality");
          if (item.ctrl_status)
            cmd.bind(":ctrl_status", *item.ctrl_status);
          else
            cmd.bind(":ctrl_status");
          std::string created = Fmi::to_iso_extended_string(item.created);
          boost::replace_all(created, ",", ".");
          cmd.bind(":created", created, sqlite3pp::nocopy);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating NetAtmo cache: " << e.what() << std::endl;
        }
      }
      xct.commit();

      pos1 = pos2;
    }

    for (const auto &hash : new_hashes)
      insertStatus.add(hash);

    return new_items.size();
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData,
                            InsertStatus &insertStatus)
{
  return 0;
}

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getFlashData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  try
  {
    string stationtype = "flash";

    map<string, int> timeseriesPositions;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = p.name();
      boost::to_lower(name, std::locale::classic());
      if (not_special(p))
      {
        if (!itsParameterMap->getParameter(name, stationtype).empty())
        {
          std::string pname = itsParameterMap->getParameter(name, stationtype);
          boost::to_lower(pname, std::locale::classic());
          timeseriesPositions[pname] = pos;
          param += pname + ",";
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
        initializeResultVector(settings.parameters);

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

        ptime utctime = boost::posix_time::time_from_string(stroke_time);
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
    throw Spine::Exception::Trace(BCP, "Getting cached flash data failed!");
  }
}

FlashDataItems SpatiaLite::readFlashCacheData(const ptime& starttime)
{
  try
  {
  string starttimeString = Fmi::to_iso_extended_string(starttime);
  boost::replace_all(starttimeString, ",", ".");

    // The data is sorted for the benefit of the user and FlashMemoryCache::fill
  std::string sql =
      "SELECT DATETIME(stroke_time) as stroke_time, flash_id, "
      "multiplicity, peak_current, "
      "sensors, freedom_degree, ellipse_angle, ellipse_major, "
      "ellipse_minor, chi_square, rise_time, ptz_time, cloud_indicator, "
      "angle_indicator, signal_indicator, timing_indicator, stroke_status, "
      "data_source, DATETIME(modified_last) AS modified_last, modified_by, "
      "X(stroke_location) AS longitude, "
      "Y(stroke_location) AS latitude "
      "FROM flash_data "
      "WHERE stroke_time >= '" + starttimeString + "'"
      "ORDER BY stroke_time, flash_id";

  FlashDataItems result;

  // Spine::ReadLock lock(write_mutex);
  sqlite3pp::query qry(itsDB, sql.c_str());

  for (auto row : qry)
  {
    FlashDataItem f;

    // Note: For some reason the "created" column present in Oracle flashdata is not
    // present in the cached flash_data.

    f.stroke_time = parse_sqlite_time(row.get<string>(0));

    f.flash_id = row.get<int>(1);
    f.multiplicity = row.get<int>(2);
    f.peak_current = row.get<int>(3);
    f.sensors = row.get<int>(4);
    f.freedom_degree = row.get<int>(5);
    f.ellipse_angle = row.get<double>(6);
    f.ellipse_major = row.get<double>(7);
    f.ellipse_minor = row.get<double>(8);
    f.chi_square = row.get<double>(9);
    f.rise_time = row.get<double>(10);
    f.ptz_time = row.get<double>(11);
    f.cloud_indicator = row.get<int>(12);
    f.angle_indicator = row.get<int>(13);
    f.signal_indicator = row.get<int>(14);
    f.timing_indicator = row.get<int>(15);
    f.stroke_status = row.get<int>(16);
    f.data_source = row.get<int>(17);
    // these seem to always be null
    // f.modified_last = parse_sqlite_time(row.get<string>(18));
    // f.modified_by = row.get<int>(19);
    f.longitude = Fmi::stod(row.get<string>(20));
    f.latitude = Fmi::stod(row.get<string>(21));

    result.emplace_back(f);
  }

  return result;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Reading flash cache data failed!");
  }
}

FlashCounts SpatiaLite::getFlashCount(const ptime &starttime,
                                      const ptime &endtime,
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
    throw Spine::Exception::Trace(BCP, "Getting flash count failed!");
  }
}


Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getData(
    const Spine::Stations &stations,
    const Settings &settings,
	const StationInfo & stationInfo,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    // Always use FMI parameter numbers for the narrow table cache

    std::string stationtype = "observations_fmi";

    // This maps measurand_id and the parameter position in TimeSeriesVector

    auto qmap = buildQueryMapping(stations,settings,itsParameterMap,stationtype, false);

	// Resolve stationgroup codes
	std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet =itsStationtypeConfig.getGroupCodeSetByStationtype(
            settings.stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());	

    LocationDataItems observations;

    if(!itsObservationMemoryCache)
	  {
		observations = readObservations(stations, settings, stationInfo, qmap, stationgroup_codes);
	  }
    else
    {
      auto cache_start_time = itsObservationMemoryCache->getStartTime();

      if(!cache_start_time.is_not_a_date_time() && cache_start_time <= settings.starttime)
        observations = itsObservationMemoryCache->read_observations(stations, settings, stationInfo, stationgroup_codes, qmap);
      else
        observations = readObservations(stations, settings, stationInfo, qmap, stationgroup_codes);
    }

    std::set<int> observed_fmisids;
    for (auto item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    Engine::Observation::StationMap fmisid_to_station = mapQueryStations(stations, observed_fmisids);
    
    ObservationsMap obsmap = buildObservationsMap(observations,
                                              settings,
                                              timezones,
                                              fmisid_to_station);

    return buildTimeseries(stations, settings, stationtype, fmisid_to_station, observations, obsmap, qmap, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Getting cached data failed!");
  }
}

ptime SpatiaLite::parseSqliteTime(sqlite3pp::query::iterator &iter,
                                                     int column) const
{
  try
  {
  // 1 = INTEGER; 2 = FLOAT, 3 = TEXT, 4 = BLOB, 5 = NULL
  if ((*iter).column_type(column) != SQLITE_TEXT)
    throw Spine::Exception(BCP, "Invalid time column from sqlite query")
        .addParameter("columntype", Fmi::to_string((*iter).column_type(column)));

  std::string timestring = (*iter).get<char const *>(column);

  return parse_sqlite_time(timestring);

  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Parsing sqlite time failed!");
  }
}

void SpatiaLite::initObservationMemoryCache(const boost::posix_time::ptime &starttime)
{
  try
	{
	  // Read all observations starting from the given time
	  std::string sql =
		"SELECT data_time, modified_last, data_value, fmisid, sensor_no, measurand_id, producer_id, measurand_no, data_quality, data_source "
		"FROM observation_data "
		"WHERE observation_data.data_time >= '" +
		Fmi::to_iso_extended_string(starttime) +
		"' GROUP BY fmisid, sensor_no, data_time, measurand_id, data_value, data_quality, data_source "
		"ORDER BY fmisid ASC, data_time ASC";
	  
	  sqlite3pp::query qry(itsDB, sql.c_str());
	  
	  DataItems observations;
	  
	  for (auto iter = qry.begin(); iter != qry.end(); ++iter)
		{
		  if ((*iter).column_type(2) == SQLITE_NULL) // data_value
			continue;
		  if ((*iter).column_type(8) == SQLITE_NULL) // data_source
			continue;

		  DataItem obs;
		  obs.data_time = parse_sqlite_time((*iter).get<std::string>(0));
		  obs.modified_last = parse_sqlite_time((*iter).get<std::string>(1));
		  obs.data_value = (*iter).get<double>(2);
		  obs.fmisid = (*iter).get<int>(3);
		  obs.sensor_no = (*iter).get<int>(4);
		  obs.measurand_id = (*iter).get<int>(5);
		  obs.producer_id = (*iter).get<int>(6);
		  obs.measurand_no = (*iter).get<int>(7);
		  obs.data_quality = (*iter).get<int>(8);
		  obs.data_source = (*iter).get<int>(9);
		  observations.emplace_back(obs);
		}

	  // Feed them into the cache
	  
	  itsObservationMemoryCache.reset(new ObservationMemoryCache);
	  itsObservationMemoryCache->fill(observations);
	}
  catch (...)
	{
	  throw Spine::Exception::Trace(BCP, "Initializing observation memory cache failed!");
	}
}

void SpatiaLite::fetchWeatherDataQCData(const std::string &sqlStmt,
												 const StationInfo &stationInfo,
												 const std::set<std::string>& stationgroup_codes,
												 const QueryMapping &qmap,
												 std::map<int, std::map<int, int>>& default_sensors,
												 WeatherDataQCData &cacheData)
{
try
  {
    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    if (qry.begin() == qry.end())
      return;

    for (sqlite3pp::query::iterator iter = qry.begin(); iter != qry.end(); ++iter)
    {
      boost::optional<int> fmisid = (*iter).get<int>(0);
      boost::posix_time::ptime obstime = parseSqliteTime(iter, 1);
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

      boost::optional<std::string> parameter = (*iter).get<std::string>(2);
      boost::optional<double> data_value;
      if ((*iter).column_type(3) != SQLITE_NULL)
        data_value = (*iter).get<double>(3);
      boost::optional<int> sensor_no = (*iter).get<int>(4);
      boost::optional<int> data_quality = (*iter).get<int>(5);

      cacheData.fmisidsAll.push_back(fmisid);
      cacheData.obstimesAll.push_back(obstime);
      cacheData.latitudesAll.push_back(latitude);
      cacheData.longitudesAll.push_back(longitude);
      cacheData.elevationsAll.push_back(elevation);
      cacheData.parametersAll.push_back(parameter);
      cacheData.data_valuesAll.push_back(data_value);
      cacheData.sensor_nosAll.push_back(sensor_no);
      cacheData.data_qualityAll.push_back(data_quality);

      if (sensor_no)
      {
        int sensor_number = *sensor_no;
        std::string parameter_id = (*parameter + Fmi::to_string(sensor_number));
        Fmi::ascii_tolower(parameter_id);
        int parameter = boost::hash_value(parameter_id);  // We dont have measurand id in
                                                          // weather_data_qc table, so use
                                                          // temporarily hash value of parameter
                                                          // name + sensor number
        if (qmap.isDefaultSensor(sensor_number, parameter))
        {
          default_sensors[*fmisid][parameter] = sensor_number;
        }
      }
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Fetching data from SpatiaLite WeatherDataQCData cache failed!");
  }
}


std::string SpatiaLite::sqlSelectFromWeatherDataQCData(const Settings &settings,
													   const std::string &params, 
													   const std::string& station_ids) const
{
	try
	  {
    std::string sqlStmt;
    if (settings.latest)
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, MAX(data.obstime) AS obstime, "
          "data.parameter, data.value, data.sensor_no, data.flag as data_quality "
          "FROM weather_data_qc data "
          "WHERE data.fmisid IN (" +
          station_ids +
          ") "
          "AND data.obstime BETWEEN '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + params +
          ") "
          "GROUP BY data.fmisid, data.parameter, data.sensor_no "
          "ORDER BY fmisid ASC, obstime ASC;";
    }
    else
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, data.obstime AS obstime, "
          "data.parameter, data.value, data.sensor_no, data.flag as data_quality "
          "FROM weather_data_qc data "
          "WHERE data.fmisid IN (" +
          station_ids +
          ") "
          "AND data.obstime BETWEEN '" +
          Fmi::to_iso_extended_string(settings.starttime) + "' AND '" +
          Fmi::to_iso_extended_string(settings.endtime) + "' AND data.parameter IN (" + params +
          ") "
          "GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

	return sqlStmt;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Constructing SQL statement for SpatiaLite cache query failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
