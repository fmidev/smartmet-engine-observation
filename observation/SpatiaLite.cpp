#include "SpatiaLite.h"
#include "DataWithQuality.h"
#include "ExternalAndMobileDBInfo.h"
#include "Keywords.h"
#include "ObservationMemoryCache.h"
#include "QueryMapping.h"
#include "SpatiaLiteCacheParameters.h"
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/Join.h>
#include <macgyver/StringConversion.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Convenience.h>
#include <spine/Reactor.h>
#include <spine/Thread.h>
#include <spine/Value.h>
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>
#include <chrono>
#include <iostream>
#include <ogr_geometry.h>

#include <unistd.h>  // for access()

#ifdef __llvm__
#pragma clang diagnostic push
// Exceptions have unused parameters here
#pragma clang diagnostic ignored "-Wunused-exception-parameter"
// long long should be allowed
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#endif

using namespace std;

using Fmi::DateTime;
using Fmi::LocalDateTime;
using Fmi::date_time::from_time_t;
using Fmi::date_time::Milliseconds;

namespace
{
const Fmi::DateTime ptime_epoch_start = from_time_t(0);

// should use std::time_t or long here, but sqlitepp does not support it. Luckily intel 64-bit int
// is 8 bytes
int to_epoch(const Fmi::DateTime pt)
{
  if (pt.is_not_a_date_time())
    return 0;

  auto ret = (pt - ptime_epoch_start).total_seconds();
  if (ret > std::numeric_limits<int>::max())
    ret = std::numeric_limits<int>::max();
  return ret;
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

}  // namespace

namespace SmartMet
{
// Mutex for write operations - otherwise you get table locked errors
// in MULTITHREAD-mode.

namespace
{
Spine::MutexType write_mutex;
}

namespace Engine
{
namespace Observation
{
using namespace Utils;
// Results read from the sqlite database

LocationDataItems SpatiaLite::readObservationDataFromDB(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const QueryMapping &qmap,
    const std::set<std::string> &stationgroup_codes)
{
  try
  {
    LocationDataItems ret;

    if (qmap.measurandIds.empty())  // Safety check, has happened
      return ret;

    auto station_ids =
        buildStationList(stations, stationgroup_codes, stationInfo, settings.requestLimits);

    if (station_ids.empty())
      return ret;

    auto measurandsql = Fmi::join(qmap.measurandIds);
    auto stationsql = Fmi::join(station_ids);
    auto producersql = Fmi::join(settings.producer_ids);

    auto starttime = to_epoch(settings.starttime);
    auto endtime = to_epoch(settings.endtime);

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, data.sensor_no AS sensor_no, data.data_time AS obstime, "
        "measurand_id, measurand_no, data_value, data_quality, data_source FROM observation_data "
        "data "
        "WHERE data.fmisid IN (" +
        stationsql +
        ") "
        "AND data.data_time";

    if (starttime == endtime)
      sqlStmt += "=" + Fmi::to_string(starttime);
    else
      sqlStmt += " BETWEEN " + Fmi::to_string(starttime) + " AND " + Fmi::to_string(endtime);

    sqlStmt += " AND data.measurand_id IN (" + measurandsql + ") ";
    if (!producersql.empty())
      sqlStmt += ("AND data.producer_id IN (" + producersql + ") ");

    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << '\n';

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    std::set<int> fmisids;
    std::set<Fmi::DateTime> obstimes;

    for (const auto &row : qry)
    {
      LocationDataItem obs;
      time_t epoch_time = row.get<int>(2);
      obs.data.data_time = Fmi::date_time::from_time_t(epoch_time);
      obs.data.fmisid = row.get<int>(0);
      obs.data.sensor_no = row.get<int>(1);
      obs.data.measurand_id = row.get<int>(3);
      obs.data.measurand_no = row.get<int>(4);
      if (row.column_type(5) != SQLITE_NULL)
        obs.data.data_value = row.get<double>(5);
      if (row.column_type(6) != SQLITE_NULL)
        obs.data.data_quality = row.get<int>(6);
      if (row.column_type(7) != SQLITE_NULL)
        obs.data.data_source = row.get<int>(7);

      try
      {
        // Get latitude, longitude, elevation from station info. The databases may contain
        // observations outside the validity interval of the station, in which case we will not get
        // location information for the observation.
        const Spine::Station &s =
            stationInfo.getStation(obs.data.fmisid, stationgroup_codes, obs.data.data_time);
        obs.latitude = s.latitude;
        obs.longitude = s.longitude;
        obs.elevation = s.elevation;
        obs.stationtype = s.type;
      }
      catch (...)
      {
      }

      ret.emplace_back(obs);

      fmisids.insert(obs.data.fmisid);
      obstimes.insert(obs.data.data_time);

      check_request_limit(
          settings.requestLimits, fmisids.size(), TS::RequestLimitMember::LOCATIONS);
      check_request_limit(
          settings.requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
      check_request_limit(settings.requestLimits, ret.size(), TS::RequestLimitMember::ELEMENTS);
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading observations from sqlite database failed!");
  }
}

SpatiaLite::SpatiaLite(const std::string &spatialiteFile, const SpatiaLiteCacheParameters &options)
    : CommonDatabaseFunctions(options.stationtypeConfig, options.parameterMap),
      itsMaxInsertSize(options.maxInsertSize),
      itsExternalAndMobileProducerConfig(options.externalAndMobileProducerConfig)
{
  try
  {
    srid = "4326";

    // Enabling shared cache may decrease read performance:
    // https://manski.net/2012/10/sqlite-performance/
    // However, for a single shared db it may be better to share:
    // https://github.com/mapnik/mapnik/issues/797

    itsReadOnly = (access(spatialiteFile.c_str(), W_OK) != 0);

    if (itsReadOnly)
    {
      // The immutable option prevents shm/wal files from being created, but can apparently
      // only be specified using the URI format. Additionally, mode=ro must be in the flags
      // option, not as a mode=ro query. Strange, but works (requires sqlite >= 3.15)
      itsDB.connect(
          fmt::format("file:{}?immutable=1", spatialiteFile).c_str(),
          SQLITE_OPEN_READONLY | SQLITE_OPEN_URI | SQLITE_OPEN_PRIVATECACHE | SQLITE_OPEN_NOMUTEX);
    }
    else
    {
      itsDB.connect(spatialiteFile.c_str(),
                    SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_PRIVATECACHE |
                        SQLITE_OPEN_NOMUTEX);
    }

    // timeout ms
    itsDB.set_busy_timeout(options.sqlite.timeout);

    // Default is fully synchronous (2), with WAL normal (1) is supposedly
    // better, for best speed we
    // choose off (0), since this is only a cache.
    //    options += " synchronous=" + synchronous;
    cache = sqlite_api::spatialite_alloc_connection();
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
    throw Fmi::Exception::Trace(BCP, "Connecting database '" + spatialiteFile + "' failed!");
  }
}

SpatiaLite::~SpatiaLite()
{
  sqlite_api::spatialite_cleanup_ex(cache);
};

void SpatiaLite::createTables(const std::set<std::string> &tables)
{
  try
  {
    if (itsReadOnly)
      return;

    // No locking needed during initialization phase
    initSpatialMetaData();
    if (tables.find(OBSERVATION_DATA_TABLE) != tables.end())
    {
      createObservationDataTable();
      createMovingLocationsDataTable();
    }
    if (tables.find(WEATHER_DATA_QC_TABLE) != tables.end())
      createWeatherDataTable();
    if (tables.find(FLASH_DATA_TABLE) != tables.end())
      createFlashDataTable();
    if (tables.find(ROADCLOUD_DATA_TABLE) != tables.end())
      createRoadCloudDataTable();
    if (tables.find(NETATMO_DATA_TABLE) != tables.end())
      createNetAtmoDataTable();
    if (tables.find(FMI_IOT_DATA_TABLE) != tables.end())
      createFmiIoTDataTable();
    if (tables.find(TAPSI_QC_DATA_TABLE) != tables.end())
      createTapsiQcDataTable();
    if (tables.find(MAGNETOMETER_DATA_TABLE) != tables.end())
      createMagnetometerDataTable();
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

void SpatiaLite::shutdown()
{
  // We let the SpatiaLiteConnectionPool print just one message
  // std::cout << "  -- Shutdown requested (SpatiaLite)\n";
}

void SpatiaLite::createMovingLocationsDataTable()
{
  try
  {
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS moving_locations("
        "station_id INTEGER NOT NULL, "
        "sdate INTEGER NOT NULL, "
        "edate INTEGER NOT NULL, "
        "lon REAL, "
        "lat REAL, "
        "elev INTEGER,"
        "PRIMARY KEY (station_id, sdate, edate))");

    // Delete redundant old indices, primary key should be preferred
    itsDB.execute("DROP INDEX IF EXISTS moving_locations_station_id_idx ON");
    itsDB.execute("DROP INDEX IF EXISTS moving_locations_sdate_idx");
    itsDB.execute("DROP INDEX IF EXISTS moving_locations_edate_idx");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of observation_data table failed!");
  }
}

void SpatiaLite::createObservationDataTable()
{
  try
  {
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS observation_data("
        "fmisid INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "data_time INTEGER NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "data_source INTEGER, "
        "modified_last INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (fmisid, data_time, measurand_id, producer_id, measurand_no, sensor_no))");

    // Delete redundant old indices, primary key should be preferred
    itsDB.execute("DROP INDEX IF EXISTS observation_data_data_time_idx");
    itsDB.execute("DROP INDEX IF EXISTS observation_data_fmisid_idx");

    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS observation_data_modified_last_idx ON "
        "observation_data(modified_last);");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of observation_data table failed!");
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
    throw Fmi::Exception::Trace(BCP, "PRAGMA table_info failed!");
  }

  if (!data_source_column_exists)
  {
    try
    {
      itsDB.execute("ALTER TABLE observation_data ADD COLUMN data_source INTEGER");
    }
    catch (const std::exception &e)
    {
      throw Fmi::Exception::Trace(BCP,
                                  "Failed to add data_source column to observation_data TABLE!");
    }
  }

  // if we expand an old table, we just make an educated guess for the modified_last column
  if (!modified_last_column_exists)
  {
    try
    {
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding modified_last column to observation_data table" << '\n';
      itsDB.execute(
          "ALTER TABLE observation_data ADD COLUMN modified_last INTEGER NOT NULL DEFAULT 0");
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] ... Updating all modified_last columns in observation_data table"
                << '\n';
      itsDB.execute("UPDATE observation_data SET modified_last=data_time");
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] ... Creating modified_last index in observation_data table"
                << '\n';
      itsDB.execute(
          "CREATE INDEX observation_data_modified_last_idx ON observation_data(modified_last)");
      std::cout << Spine::log_time_str() << " [SpatiaLite] modified_last processing done" << '\n';
    }
    catch (const std::exception &e)
    {
      throw Fmi::Exception::Trace(BCP,
                                  "Failed to add modified_last column to observation_data TABLE!");
    }
  }
}

void SpatiaLite::createWeatherDataTable()
{
  try
  {
    // Crate similar table as for observation_data
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS weather_data("
        "fmisid INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "data_time INTEGER NOT NULL, "
        "measurand_id INTEGER NOT NULL,"
        "producer_id INTEGER NOT NULL,"
        "measurand_no INTEGER NOT NULL,"
        "data_value REAL, "
        "data_quality INTEGER, "
        "data_source INTEGER, "
        "modified_last INTEGER NOT NULL DEFAULT 0, "
        "PRIMARY KEY (fmisid, data_time, measurand_id, producer_id, measurand_no, sensor_no))");

    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS weather_data_modified_last_idx ON "
        "weather_data(modified_last);");

    // Delete legacy table if it exists
    itsDB.execute("DROP TABLE IF EXISTS weather_data_qc");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of weather_data table failed!");
  }
}

void SpatiaLite::createFlashDataTable()
{
  try
  {
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS flash_data("
        "stroke_time INTEGER NOT NULL, "
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
        "created  INTEGER, "
        "modified_last INTEGER, "
        "modified_by INTEGER, "
        "PRIMARY KEY (stroke_time, stroke_time_fraction, flash_id))");

    // Delete redundant old index
    itsDB.execute("DROP INDEX IF EXISTS flash_data_stroke_time_idx");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS flash_data_modified_last_idx ON flash_data(modified_last);");

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
      std::cout << Spine::log_time_str() << " [SpatiaLite] Adding spatial index to flash_data table"
                << '\n';
      itsDB.execute("SELECT CreateSpatialIndex('flash_data', 'stroke_location')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of flash_data table failed!");
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
    throw Fmi::Exception::Trace(BCP, "PRAGMA table_info failed!");
  }

  if (!data_source_column_exists)
  {
    try
    {
      itsDB.execute("ALTER TABLE flash_data ADD COLUMN data_source INTEGER");
    }
    catch (const std::exception &e)
    {
      throw Fmi::Exception::Trace(BCP, "Failed to add data_source_column to flash_data TABLE!");
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
                           "data_time INTEGER NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created INTEGER, "
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
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding spatial index to ext_obsdata_roadcloud table\n";
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_roadcloud', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_roadcloud table failed!");
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
                           "data_time INTEGER NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created INTEGER, "
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
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding spatial index to ext_obsdata_netatmo table\n";
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_netatmo', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_netatmo table failed!");
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
                           "dataset_id character VARYING(50), "
                           "data_level INTEGER, "
                           "mid INTEGER, "
                           "sensor_no INTEGER, "
                           "data_time INTEGER NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created INTEGER, "
                           "altitude REAL)");

    cmd.execute();
    xct.commit();

    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(geom) AS latitude FROM ext_obsdata_fmi_iot LIMIT 1");
      qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('ext_obsdata_fmi_iot', 'geom', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
      itsDB.execute(
          "ALTER TABLE ext_obsdata_fmi_iot ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='ext_obsdata_fmi_iot' AND f_geometry_column = "
                         "'geom'");
    int spatial_index_enabled = 0;
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (spatial_index_enabled == 0)
    {
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding spatial index to ext_obsdata_fmi_iot table\n";
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_fmi_iot', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_fmi_iot table failed!");
  }
}

void SpatiaLite::createTapsiQcDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS ext_obsdata_tapsi_qc("
                           "prod_id INTEGER, "
                           "station_id INTEGER, "
                           "dataset_id character VARYING(50), "
                           "data_level INTEGER, "
                           "mid INTEGER, "
                           "sensor_no INTEGER, "
                           "data_time INTEGER NOT NULL, "
                           "data_value NUMERIC, "
                           "data_value_txt character VARYING(30), "
                           "data_quality INTEGER, "
                           "ctrl_status INTEGER, "
                           "created INTEGER, "
                           "altitude REAL)");

    cmd.execute();
    xct.commit();

    try
    {
      sqlite3pp::query qry(itsDB, "SELECT X(geom) AS latitude FROM ext_obsdata_tapsi_qc LIMIT 1");
      qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('ext_obsdata_tapsi_qc', 'geom', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
      itsDB.execute(
          "ALTER TABLE ext_obsdata_tapsi_qc ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='ext_obsdata_tapsi_qc' AND f_geometry_column = "
                         "'geom'");
    int spatial_index_enabled = 0;
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (spatial_index_enabled == 0)
    {
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding spatial index to ext_obsdata_tapsi_qc table\n";
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_tapsi_qc', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_tapsi_qc table failed!");
  }
}

void SpatiaLite::createMagnetometerDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS magnetometer_data("
                           "station_id INTEGER NOT NULL,"
                           "magnetometer character VARYING(4) NOT NULL,"
                           "level INTEGER NOT NULL,"
                           "data_time INTEGER NOT NULL,"
                           "x NUMERIC,"
                           "y NUMERIC,"
                           "z NUMERIC,"
                           "t NUMERIC,"
                           "f NUMERIC,"
                           "data_quality INTEGER NOT NULL, "
                           "modified_last INTEGER NOT NULL DEFAULT 0)");

    cmd.execute();
    xct.commit();

    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS magnetometer_data_data_time_idx ON "
        "magnetometer_data(data_time);");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS magnetometer_data_station_idx ON "
        "magnetometer_data(station_id);");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS magnetometer_data_modified_last_idx ON "
        "magnetometer_data(modified_last);");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of magnetometer_data table failed!");
  }
}

void SpatiaLite::initSpatialMetaData()
{
  try
  {
    // This will create all meta data required to make spatial queries, see
    // http://www.gaia-gis.it/gaia-sins/spatialite-cookbook/html/metadata.html

    // Check whether the table exists already
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
    throw Fmi::Exception::Trace(BCP, "initSpatialMetaData failed!");
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
    throw Fmi::Exception::Trace(BCP, "SQL-query failed: " + queryString);
  }
}

Fmi::DateTime SpatiaLite::getLatestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest observation time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestObservationModifiedTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(modified_last) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Modified last observation time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getOldestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest observation time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(data_time) FROM weather_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest WeatherDataQCTime query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestWeatherDataQCModifiedTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(modified_last) FROM weather_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Modified last WeatherDataQCTime query failed!");
  }
}

Fmi::DateTime SpatiaLite::getOldestWeatherDataQCTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MIN(data_time) FROM weather_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest WeatherDataQCTime query failed!");
  }
}

int SpatiaLite::getMaxFlashId()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(flash_id) FROM flash_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return 0;

    return (*iter).get<int>(0);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Maximum flash_id  query failed!");
  }
}
Fmi::DateTime SpatiaLite::getLatestFlashModifiedTime()
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

Fmi::DateTime SpatiaLite::getLatestFlashTime()
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

Fmi::DateTime SpatiaLite::getOldestFlashTime()
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

Fmi::DateTime SpatiaLite::getOldestRoadCloudDataTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest RoadCloud time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestRoadCloudDataTime()
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

Fmi::DateTime SpatiaLite::getLatestRoadCloudCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_roadcloud";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest RoadCloud creaed time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getOldestNetAtmoDataTime()
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

Fmi::DateTime SpatiaLite::getLatestNetAtmoDataTime()
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

Fmi::DateTime SpatiaLite::getLatestNetAtmoCreatedTime()
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

Fmi::DateTime SpatiaLite::getOldestFmiIoTDataTime()
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

Fmi::DateTime SpatiaLite::getLatestFmiIoTDataTime()
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

Fmi::DateTime SpatiaLite::getLatestFmiIoTCreatedTime()
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

Fmi::DateTime SpatiaLite::getOldestTapsiQcDataTime()
{
  try
  {
    string tablename = "ext_obsdata_tapsi_qc";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest TapsiQc data time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestTapsiQcDataTime()
{
  try
  {
    string tablename = "ext_obsdata_tapsi_qc";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest TapsiQc data time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestTapsiQcCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_tapsi_qc";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest TapsiQc created time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestMagnetometerModifiedTime()
{
  try
  {
    string tablename = "magnetometer_data";
    string time_field = "modified_last";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest magnetometer modified time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestMagnetometerDataTime()
{
  try
  {
    string tablename = "magnetometer_data";
    string time_field = "data_time";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest magnetometer modified time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getOldestMagnetometerDataTime()
{
  try
  {
    string tablename = "magnetometer_data";
    string time_field = "data_time";
    return getOldestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest magnetometer modified time query failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestTimeFromTable(const std::string &tablename,
                                                 const std::string &time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT MAX(" + time_field + ") FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch_time = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    return Fmi::DateTime::NOT_A_DATE_TIME;
  }
}

Fmi::DateTime SpatiaLite::getOldestTimeFromTable(const std::string &tablename,
                                                 const std::string &time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT MIN(" + time_field + ") FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return Fmi::DateTime::NOT_A_DATE_TIME;

    time_t epoch = (*iter).get<int>(0);
    return Fmi::date_time::from_time_t(epoch);
  }
  catch (...)
  {
    return Fmi::DateTime::NOT_A_DATE_TIME;
  }
}

void SpatiaLite::cleanDataCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestObservationTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM observation_data WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of data cache failed!");
  }
}

void SpatiaLite::cleanMovingLocationsCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestObservationTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);
    sqlite3pp::command cmd(itsDB, "DELETE FROM moving_locations WHERE edate < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of moving_locations cache failed!");
  }
}

void SpatiaLite::cleanWeatherDataQCCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestWeatherDataQCTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM weather_data WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of WeatherDataQCCache failed!");
  }
}

void SpatiaLite::cleanFlashDataCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestFlashTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM flash_data WHERE stroke_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of FlashDataCache failed!");
  }
}

void SpatiaLite::cleanRoadCloudCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestRoadCloudDataTime();

    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB,
                           "DELETE FROM ext_obsdata_roadcloud WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of RoadCloud cache failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getRoadCloudData(const Settings &settings,
                                                     const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanNetAtmoCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestNetAtmoDataTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM ext_obsdata_netatmo WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of NetAtmo cache failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getNetAtmoData(const Settings &settings,
                                                   const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanFmiIoTCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestFmiIoTDataTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM ext_obsdata_fmi_iot WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of FmiIoT cache failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getFmiIoTData(const Settings &settings,
                                                  const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanTapsiQcCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestTapsiQcDataTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM ext_obsdata_tapsi_qc WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of TapsiQc cache failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getTapsiQcData(const Settings &settings,
                                                   const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

TS::TimeSeriesVectorPtr SpatiaLite::getMobileAndExternalData(const Settings &settings,
                                                             const Fmi::TimeZones &timezones)
{
  try
  {
    TS::TimeSeriesVectorPtr ret = initializeResultVector(settings);

    const ExternalAndMobileProducerConfigItem &producerConfig =
        itsExternalAndMobileProducerConfig.at(settings.stationtype);
    std::vector<std::string> queryfields;
    std::vector<int> measurandIds;
    const Engine::Observation::Measurands &measurands = producerConfig.measurands();
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = Fmi::ascii_tolower_copy(p.name());
      queryfields.push_back(name);
      if (measurands.find(name) != measurands.end())
        measurandIds.push_back(measurands.at(name));
    }

    TS::TimeSeriesGeneratorOptions timeSeriesOptions;
    timeSeriesOptions.startTime = settings.starttime;
    timeSeriesOptions.endTime = settings.endtime;
    TS::TimeSeriesGenerator::LocalTimeList tlist;

    // The desired timeseries, unless all available data if timestep=0 or wanted time only
    if (!settings.wantedtime && !timeSeriesOptions.all())
    {
      tlist = TS::TimeSeriesGenerator::generate(timeSeriesOptions,
                                                timezones.time_zone_from_string(settings.timezone));
    }

    ExternalAndMobileDBInfo dbInfo(&producerConfig);

    std::string sqlStmt = dbInfo.sqlSelectFromCache(measurandIds,
                                                    settings.starttime,
                                                    settings.endtime,
                                                    settings.wktArea,
                                                    settings.dataFilter,
                                                    true);

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    if (qry.begin() == qry.end())
      return ret;

    int column_count = qry.column_count();

    for (auto row : qry)
    {
      map<std::string, TS::Value> result;
      const Fmi::TimeZonePtr &zone = Fmi::TimeZonePtr::utc;
      Fmi::LocalDateTime timestep(Fmi::DateTime::NOT_A_DATE_TIME, zone);
      for (int i = 0; i < column_count; i++)
      {
        std::string column_name = qry.column_name(i);

        int data_type = row.column_type(i);
        TS::Value value = TS::None();
        if (data_type == SQLITE_TEXT)
        {
          auto data_value = row.get<std::string>(i);
          value = data_value;
        }
        else if (data_type == SQLITE_FLOAT)
        {
          value = row.get<double>(i);
        }
        else if (data_type == SQLITE_INTEGER)
        {
          if (column_name != "prod_id" && column_name != "station_id" &&
              column_name != "data_level" && column_name != "mid" && column_name != "sensor_no" &&
              column_name != "data_quality" && column_name != "ctrl_status")
            value = row.get<double>(i);
          else
          {
            if (column_name == "data_time" || column_name == "created")
            {
              time_t data_time = row.get<int>(i);
              Fmi::DateTime pt = Fmi::date_time::from_time_t(data_time);
              Fmi::LocalDateTime ldt(pt, zone);
              value = ldt;
              if (column_name == "data_time")
                timestep = ldt;
            }
            else
            {
              value = row.get<int>(i);
            }
          }
        }
        result[column_name] = value;
      }

      unsigned int index = 0;
      for (const auto &paramname : queryfields)
      {
        TS::Value val = result[paramname];
        ret->at(index).emplace_back(TS::TimedValue(timestep, val));
        index++;
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting mobile and external data from cache failed!");
  }
}

// tablename = observation_data or weather_data
std::size_t SpatiaLite::fillDataCache(const std::string &tablename,
                                      const DataItems &cacheData,
                                      InsertStatus &insertStatus)
{
  try
  {
    std::size_t new_item_count = 0;

    if (cacheData.empty())
      return new_item_count;

    // Use schema column order for improved speed
    std::string sqltemplate = fmt::format(
        "INSERT OR REPLACE INTO {} VALUES (:fmisid, :sensor_no, :data_time, :measurand_id, "
        ":producer_id, :measurand_no, :data_value, :data_quality, :data_source, :modified_last)",
        tablename);

    // Loop over all observations, inserting only new items in groups to the cache

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;
    std::vector<int> data_times;
    std::vector<int> modified_last_times;

    for (std::size_t pos = 0; pos < cacheData.size(); ++pos)
    {
      // Abort if so requested
      if (Spine::Reactor::isShuttingDown())
        return 0;

      const auto &item = cacheData[pos];
      const auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(pos);
        new_hashes.push_back(hash);
      }

      // Insert a block if necessary

      const bool block_full = (new_items.size() >= itsMaxInsertSize);
      const bool last_item = (pos == cacheData.size() - 1);

      if (!new_items.empty() && (block_full || last_item))
      {
        const auto insert_size = new_items.size();

        // Prepare strings outside the lock to minimize lock time
        data_times.reserve(insert_size);
        modified_last_times.reserve(insert_size);

        for (std::size_t i = 0; i < insert_size; i++)
        {
          const auto &obs = cacheData[new_items[i]];
          if (obs.data_value != 9999)  // Ignore parameters marked MISSING
          {
            data_times.emplace_back(to_epoch(obs.data_time));
            modified_last_times.emplace_back(to_epoch(obs.modified_last));
          }
        }

        {
          Spine::WriteLock lock(write_mutex);

          sqlite3pp::transaction xct(itsDB);
          sqlite3pp::command cmd(itsDB, sqltemplate.c_str());

          for (std::size_t i = 0; i < insert_size; i++)
          {
            const auto &data = cacheData[new_items[i]];
            cmd.bind(":fmisid", data.fmisid);
            cmd.bind(":sensor_no", data.sensor_no);
            cmd.bind(":data_time", data_times[i]);
            cmd.bind(":measurand_id", data.measurand_id);
            cmd.bind(":producer_id", data.producer_id);
            cmd.bind(":measurand_no", data.measurand_no);
            if (data.data_value)
              cmd.bind(":data_value", *data.data_value);
            else
              cmd.bind(":data_value");  // NULL
            cmd.bind(":data_quality", data.data_quality);
            if (data.data_source >= 0)
              cmd.bind(":data_source", data.data_source);
            else
              cmd.bind(":data_source");  // NULL
            cmd.bind(":modified_last", modified_last_times[i]);
            cmd.execute();
            // Must reset, previous values cannot be replaced
            cmd.reset();
          }
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (const auto &new_hash : new_hashes)
          insertStatus.add(new_hash);

        new_item_count += insert_size;

        new_items.clear();
        new_hashes.clear();
        data_times.clear();
        modified_last_times.clear();
      }
    }

    return new_item_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling of data cache failed!")
        .addParameter("Table", tablename);
  }
}

std::size_t SpatiaLite::fillMovingLocationsCache(const MovingLocationItems &cacheData,
                                                 InsertStatus &insertStatus)
{
  try
  {
    std::size_t new_item_count = 0;

    if (cacheData.empty())
      return new_item_count;

    const char *sqltemplate =
        "INSERT OR REPLACE INTO moving_locations "
        "(station_id, sdate, edate, lon, lat, elev) "
        "VALUES "
        "(:station_id,:sdate,:edate,:lon,:lat,:elev);";

    // Loop over all observations, inserting only new items in groups to the cache

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;
    std::vector<int> sdates;
    std::vector<int> edates;

    for (std::size_t pos = 0; pos < cacheData.size(); ++pos)
    {
      // Abort if so requested
      if (Spine::Reactor::isShuttingDown())
        return 0;

      const auto &item = cacheData[pos];
      const auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(pos);
        new_hashes.push_back(hash);
      }

      // Insert a block if necessary

      const bool block_full = (new_items.size() >= itsMaxInsertSize);
      const bool last_item = (pos == cacheData.size() - 1);

      if (!new_items.empty() && (block_full || last_item))
      {
        const auto insert_size = new_items.size();

        // Prepare strings outside the lock to minimize lock time
        sdates.reserve(insert_size);
        edates.reserve(insert_size);

        for (std::size_t i = 0; i < insert_size; i++)
        {
          const auto &obs = cacheData[new_items[i]];
          sdates.emplace_back(to_epoch(obs.sdate));
          edates.emplace_back(to_epoch(obs.edate));
        }

        {
          Spine::WriteLock lock(write_mutex);

          sqlite3pp::transaction xct(itsDB);
          sqlite3pp::command cmd(itsDB, sqltemplate);

          for (std::size_t i = 0; i < insert_size; i++)
          {
            const auto &data = cacheData[new_items[i]];
            cmd.bind(":station_id", data.station_id);
            cmd.bind(":sdate", sdates[i]);
            cmd.bind(":edate", edates[i]);
            cmd.bind(":lon", data.lon);
            cmd.bind(":lat", data.lat);
            cmd.bind(":elev", data.elev);
            cmd.execute();
            cmd.reset();
          }
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (auto new_hash : new_hashes)
          insertStatus.add(new_hash);

        new_item_count += insert_size;

        new_items.clear();
        new_hashes.clear();
        sdates.clear();
        edates.clear();
      }
    }

    return new_item_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}

std::size_t SpatiaLite::fillFlashDataCache(const FlashDataItems &cacheData,
                                           InsertStatus &insertStatus)
{
  try
  {
    std::size_t new_item_count = 0;

    if (cacheData.empty())
      return new_item_count;

    const char *sqltemplate =
        "INSERT OR REPLACE INTO flash_data "
        "(stroke_time, stroke_time_fraction, flash_id, multiplicity, "
        "peak_current, sensors, freedom_degree, ellipse_angle, "
        "ellipse_major, "
        "ellipse_minor, "
        "chi_square, rise_time, ptz_time, cloud_indicator, "
        "angle_indicator, "
        "signal_indicator, timing_indicator, stroke_status, "
        "data_source, created, modified_last, stroke_location) "
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
        ":data_source,"
        ":created,"
        ":modified_last,"
        "GeomFromText(:stroke_point, 4326))";

    // Loop over all observations, inserting only new items in groups to the cache

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;
    std::vector<int> stroke_times;
    std::vector<int> created_times;
    std::vector<int> modified_last_times;

    for (std::size_t pos = 0, n = cacheData.size(); pos < n; ++pos)
    {
      // Abort if so requested
      if (Spine::Reactor::isShuttingDown())
        return 0;

      const auto &item = cacheData[pos];
      const auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(pos);
        new_hashes.push_back(hash);
      }

      // Insert a block if necessary

      const bool block_full = (new_items.size() >= itsMaxInsertSize);
      const bool last_item = (pos == cacheData.size() - 1);

      if (!new_items.empty() && (block_full || last_item))
      {
        const auto insert_size = new_items.size();

        // Prepare strings outside the lock to minimize lock time
        stroke_times.reserve(insert_size);
        created_times.reserve(insert_size);
        modified_last_times.reserve(insert_size);

        for (std::size_t i = 0; i < insert_size; i++)
        {
          const auto &obs = cacheData[new_items[i]];
          stroke_times.emplace_back(to_epoch(obs.stroke_time));
          created_times.emplace_back(to_epoch(item.created));
          modified_last_times.emplace_back(to_epoch(item.modified_last));
        }

        {
          Spine::WriteLock lock(write_mutex);

          sqlite3pp::transaction xct(itsDB);
          sqlite3pp::command cmd(itsDB, sqltemplate);

          for (std::size_t i = 0; i < insert_size; i++)
          {
            const auto &data = cacheData[new_items[i]];

            // @todo There is no simple way to optionally set possible NULL values.
            // Find out later how to do it.

            try
            {
              cmd.bind(":timestring", stroke_times[i]);
              cmd.bind(":stroke_time_fraction", data.stroke_time_fraction);
              cmd.bind(":flash_id", static_cast<int>(data.flash_id));
              cmd.bind(":multiplicity", data.multiplicity);
              cmd.bind(":peak_current", data.peak_current);
              cmd.bind(":sensors", data.sensors);
              cmd.bind(":freedom_degree", data.freedom_degree);
              cmd.bind(":ellipse_angle", data.ellipse_angle);
              cmd.bind(":ellipse_major", data.ellipse_major);
              cmd.bind(":ellipse_minor", data.ellipse_minor);
              cmd.bind(":chi_square", data.chi_square);
              cmd.bind(":rise_time", data.rise_time);
              cmd.bind(":ptz_time", data.ptz_time);
              cmd.bind(":cloud_indicator", data.cloud_indicator);
              cmd.bind(":angle_indicator", data.angle_indicator);
              cmd.bind(":signal_indicator", data.signal_indicator);
              cmd.bind(":timing_indicator", data.timing_indicator);
              cmd.bind(":stroke_status", data.stroke_status);
              cmd.bind(":data_source", data.data_source);
              cmd.bind(":created", created_times[i]);
              cmd.bind(":modified_last", modified_last_times[i]);
              std::string stroke_point = "POINT(" + Fmi::to_string("%.10g", data.longitude) + " " +
                                         Fmi::to_string("%.10g", data.latitude) + ")";
              cmd.bind(":stroke_point", stroke_point, sqlite3pp::nocopy);
              cmd.execute();
              cmd.reset();
            }
            catch (const std::exception &e)
            {
              std::cerr << "Problem updating flash data: " << e.what() << '\n';
            }
          }

          // Would it be possible to place the writelock here...????
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (auto new_hash : new_hashes)
          insertStatus.add(new_hash);

        new_item_count += insert_size;

        new_items.clear();
        new_hashes.clear();
        stroke_times.clear();
        created_times.clear();
        modified_last_times.clear();
      }
    }

    return new_item_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Flash data cache update failed!");
  }
}

std::size_t SpatiaLite::fillRoadCloudCache(const MobileExternalDataItems &mobileExternalCacheData,
                                           InsertStatus &insertStatus)
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
    if (Spine::Reactor::isShuttingDown())
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (Spine::Reactor::isShuttingDown())
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
          auto data_time = to_epoch(item.data_time);
          cmd.bind(":data_time", data_time);
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
          auto created_time = to_epoch(item.created);
          cmd.bind(":created", created_time);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating RoadCloud cache: " << e.what() << '\n';
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
    throw Fmi::Exception::Trace(BCP, "RoadCloud cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillNetAtmoCache(const MobileExternalDataItems &mobileExternalCacheData,
                                         InsertStatus &insertStatus)
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
    if (Spine::Reactor::isShuttingDown())
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (Spine::Reactor::isShuttingDown())
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
          auto data_time = to_epoch(item.data_time);
          cmd.bind(":data_time", data_time);
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
          auto created_time = to_epoch(item.created);
          cmd.bind(":created", created_time);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating NetAtmo cache: " << e.what() << '\n';
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
    throw Fmi::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillFmiIoTCache(const MobileExternalDataItems &mobileExternalCacheData,
                                        InsertStatus &insertStatus)
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
    if (Spine::Reactor::isShuttingDown())
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (Spine::Reactor::isShuttingDown())
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
            "INSERT OR IGNORE INTO ext_obsdata_fmi_iot "
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
          auto data_time = to_epoch(item.data_time);
          cmd.bind(":data_time", data_time);
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
          auto created_time = to_epoch(item.created);
          cmd.bind(":created", created_time);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating FmiIoT cache: " << e.what() << '\n';
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
    throw Fmi::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillTapsiQcCache(const MobileExternalDataItems &mobileExternalCacheData,
                                         InsertStatus &insertStatus)
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
    if (Spine::Reactor::isShuttingDown())
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (Spine::Reactor::isShuttingDown())
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
            "INSERT OR IGNORE INTO ext_obsdata_tapsi_qc "
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
          auto data_time = to_epoch(item.data_time);
          cmd.bind(":data_time", data_time);
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
          auto created_time = to_epoch(item.created);
          cmd.bind(":created", created_time);
          if (item.altitude)
            cmd.bind(":altitude", *item.altitude);
          else
            cmd.bind(":altitude");
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating TapsiQc cache: " << e.what() << '\n';
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
    throw Fmi::Exception::Trace(BCP, "TapsiQc cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillMagnetometerDataCache(
    const MagnetometerDataItems &magnetometerCacheData, InsertStatus &insertStatus)
{
  try
  {
    if (magnetometerCacheData.empty())
      return 0;

    // Collect new items before taking a lock - we might avoid one completely
    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;

    for (std::size_t i = 0; i < magnetometerCacheData.size(); i++)
    {
      const auto &item = magnetometerCacheData[i];
      auto hash = item.hash_value();

      if (!insertStatus.exists(hash))
      {
        new_items.push_back(i);
        new_hashes.push_back(hash);
      }
    }

    // Abort if so requested
    if (Spine::Reactor::isShuttingDown())
      return 0;

    // Abort if nothing to do
    if (new_items.empty())
      return 0;

    // Insert the new items

    std::size_t pos1 = 0;

    Spine::WriteLock lock(write_mutex);

    while (pos1 < new_items.size())
    {
      if (Spine::Reactor::isShuttingDown())
        return 0;

      sqlite3pp::transaction xct(itsDB);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, new_items.size());
      for (std::size_t i = pos1; i < pos2; i++)
      {
        const auto &item = magnetometerCacheData[new_items[i]];

        std::string sqlStmt =
            "INSERT OR IGNORE INTO magnetometer_data "
            "(station_id, magnetometer, level, data_time, x, y, z, t, f, data_quality, "
            "modified_last)"
            "VALUES ("
            ":station_id, "
            ":magnetometer,"
            ":level,"
            ":data_time,"
            ":x,"
            ":y,"
            ":z,"
            ":t,"
            ":f,"
            ":data_quality,"
            ":modified_last)";

        sqlite3pp::command cmd(itsDB, sqlStmt.c_str());

        try
        {
          cmd.bind(":station_id", item.fmisid);
          cmd.bind(":magnetometer", item.magnetometer, sqlite3pp::nocopy);
          cmd.bind(":level", item.level);
          auto data_time = to_epoch(item.data_time);
          cmd.bind(":data_time", data_time);
          if (item.x)
            cmd.bind(":x", *item.x);
          else
            cmd.bind(":x");
          if (item.y)
            cmd.bind(":y", *item.y);
          else
            cmd.bind(":y");
          if (item.z)
            cmd.bind(":z", *item.z);
          else
            cmd.bind(":z");
          if (item.t)
            cmd.bind(":t", *item.t);
          else
            cmd.bind(":t");
          if (item.f)
            cmd.bind(":f", *item.f);
          else
            cmd.bind(":f");
          cmd.bind(":data_quality", item.data_quality);
          cmd.bind(":modified_last", to_epoch(item.modified_last));
          cmd.execute();
          cmd.reset();
        }
        catch (const std::exception &e)
        {
          std::cerr << "Problem updating Magnetometer cache: " << e.what() << '\n';
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
    throw Fmi::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }

  return 0;
}

void SpatiaLite::cleanMagnetometerCache(const Fmi::DateTime &newstarttime)
{
  try
  {
    auto oldest = getOldestMagnetometerDataTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    std::string sqlStmt =
        ("DELETE FROM magnetometer_data WHERE data_time < " + Fmi::to_string(epoch_time));

    sqlite3pp::command cmd(itsDB, sqlStmt.c_str());

    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of FmiIoT cache failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getMagnetometerData(
    const Spine::Stations & /* stations */,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    TS::TimeSeriesVectorPtr ret = initializeResultVector(settings);
    std::map<int, TS::TimeSeriesVectorPtr> fmisid_results;
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    // Stations
    std::set<std::string> fmisid_ids;
    for (const auto &s : settings.taggedFMISIDs)
      fmisid_ids.insert(Fmi::to_string(s.fmisid));

    if (fmisid_ids.empty())
      return ret;

    std::string fmisids = std::accumulate(std::begin(fmisid_ids),
                                          std::end(fmisid_ids),
                                          std::string{},
                                          [](const std::string &a, const std::string &b)
                                          { return a.empty() ? b : a + ',' + b; });

    // Measurands
    std::set<std::string> measurand_ids;
    // Positions
    std::map<std::string, int> timeseriesPositions;
    std::set<int> data_independent_positions;
    unsigned int pos = 0;
    for (const auto &p : settings.parameters)
    {
      std::string name = p.name();
      boost::to_lower(name, std::locale::classic());

      auto sparam = itsParameterMap->getParameter(name, MAGNETO_PRODUCER);

      if (!sparam.empty())
        measurand_ids.insert(sparam);

      timeseriesPositions[name] = pos;
      if (name == "fmisid" || name == "magnetometer_id" || name == "stationlon" ||
          name == "stationlat" || name == "elevation" || name == "stationtype")
        data_independent_positions.insert(pos);

      pos++;
    }

    if (measurand_ids.empty())
      return ret;

    // Starttime & endtime
    auto starttime = to_epoch(settings.starttime);
    auto endtime = to_epoch(settings.endtime);

    std::string sqlStmt =
        "SELECT station_id, magnetometer, level, data_time, x as magneto_x, y as magneto_y, "
        "z as magneto_z, t as magneto_t, f as magneto_f, data_quality from magnetometer_data "
        "where ";
    if (starttime == endtime)
      sqlStmt += ("data_time = " + Fmi::to_string(starttime));
    else
      sqlStmt +=
          ("data_time BETWEEN " + Fmi::to_string(starttime) + " AND " + Fmi::to_string(endtime));
    sqlStmt += (" AND station_id IN (" + fmisids + ") AND magnetometer NOT IN ('NUR2','GAS1')");
    if (settings.dataFilter.exist("data_quality"))
      sqlStmt += (" AND " + settings.dataFilter.getSqlClause("data_quality", "data_quality"));

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << '\n';

    auto localtz = timezones.time_zone_from_string(settings.timezone);

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    for (auto row : qry)
    {
      int fmisid = row.get<int>(0);
      // Initialize result vector and timestep set
      if (fmisid_results.find(fmisid) == fmisid_results.end())
      {
        fmisid_results.insert(std::make_pair(fmisid, initializeResultVector(settings)));
        fmisid_timesteps.insert(std::make_pair(fmisid, std::set<Fmi::LocalDateTime>()));
      }
      TS::Value station_id_value = fmisid;
      TS::Value magnetometer_id_value = row.get<std::string>(1);
      int level = row.get<int>(2);
      time_t epoch_time = row.get<int>(3);
      Fmi::DateTime data_time = Fmi::date_time::from_time_t(epoch_time);
      Fmi::LocalDateTime localtime(data_time, localtz);
      TS::Value magneto_x_value;
      TS::Value magneto_y_value;
      TS::Value magneto_z_value;
      TS::Value magneto_t_value;
      TS::Value magneto_f_value;
      TS::Value data_quality_value = TS::None();
      if (row.column_type(4) != SQLITE_NULL)
        magneto_x_value = row.get<double>(4);
      if (row.column_type(5) != SQLITE_NULL)
        magneto_y_value = row.get<double>(5);
      if (row.column_type(6) != SQLITE_NULL)
        magneto_z_value = row.get<double>(6);
      if (row.column_type(7) != SQLITE_NULL)
        magneto_t_value = row.get<double>(7);
      if (row.column_type(8) != SQLITE_NULL)
        magneto_f_value = row.get<double>(8);
      if (row.column_type(9) != SQLITE_NULL)
        data_quality_value = row.get<int>(9);

      auto &result = *(fmisid_results[fmisid]);
      auto &timesteps = fmisid_timesteps[fmisid];
      const Spine::Station &s = stationInfo.getStation(fmisid, settings.stationgroups, data_time);

      auto x_parameter_name = itsParameterMap->getParameterName(
          (level == 10 ? "667" : (level == 60 ? "668" : "MISSING")), MAGNETO_PRODUCER);
      auto y_parameter_name = itsParameterMap->getParameterName(
          (level == 10 ? "669" : (level == 60 ? "670" : "MISSING")), MAGNETO_PRODUCER);
      auto z_parameter_name = itsParameterMap->getParameterName(
          (level == 10 ? "671" : (level == 60 ? "672" : "MISSING")), MAGNETO_PRODUCER);
      auto t_parameter_name =
          itsParameterMap->getParameterName((level == 60 ? "144" : "MISSING"), MAGNETO_PRODUCER);
      auto f_parameter_name =
          itsParameterMap->getParameterName((level == 110 ? "673" : "MISSING"), MAGNETO_PRODUCER);

      if (timeseriesPositions.find(x_parameter_name) != timeseriesPositions.end())
        result[timeseriesPositions.at(x_parameter_name)].push_back(
            TS::TimedValue(localtime, magneto_x_value));

      if (timeseriesPositions.find(y_parameter_name) != timeseriesPositions.end())
        result[timeseriesPositions.at(y_parameter_name)].push_back(
            TS::TimedValue(localtime, magneto_y_value));

      if (timeseriesPositions.find(z_parameter_name) != timeseriesPositions.end())
        result[timeseriesPositions.at(z_parameter_name)].push_back(
            TS::TimedValue(localtime, magneto_z_value));

      if (timeseriesPositions.find(t_parameter_name) != timeseriesPositions.end())
        result[timeseriesPositions.at(t_parameter_name)].push_back(
            TS::TimedValue(localtime, magneto_t_value));

      if (timeseriesPositions.find(f_parameter_name) != timeseriesPositions.end())
        result[timeseriesPositions.at(f_parameter_name)].push_back(
            TS::TimedValue(localtime, magneto_f_value));

      if (timeseriesPositions.find("data_quality") != timeseriesPositions.end())
        result[timeseriesPositions.at("data_quality")].push_back(
            TS::TimedValue(localtime, data_quality_value));

      if (timeseriesPositions.find("fmisid") != timeseriesPositions.end())
        result[timeseriesPositions.at("fmisid")].push_back(
            TS::TimedValue(localtime, station_id_value));

      if (timeseriesPositions.find("magnetometer_id") != timeseriesPositions.end())
        result[timeseriesPositions.at("magnetometer_id")].push_back(
            TS::TimedValue(localtime, magnetometer_id_value));

      if (timeseriesPositions.find("stationlon") != timeseriesPositions.end())
        result[timeseriesPositions.at("stationlon")].push_back(
            TS::TimedValue(localtime, s.longitude));

      if (timeseriesPositions.find("stationlat") != timeseriesPositions.end())
        result[timeseriesPositions.at("stationlat")].push_back(
            TS::TimedValue(localtime, s.latitude));

      if (timeseriesPositions.find("stationlat") != timeseriesPositions.end())
        result[timeseriesPositions.at("stationlat")].push_back(
            TS::TimedValue(localtime, s.latitude));

      if (timeseriesPositions.find("elevation") != timeseriesPositions.end())
        result[timeseriesPositions.at("elevation")].push_back(
            TS::TimedValue(localtime, s.elevation));

      if (timeseriesPositions.find("stationtype") != timeseriesPositions.end())
        result[timeseriesPositions.at("stationtype")].push_back(TS::TimedValue(localtime, s.type));

      timesteps.insert(localtime);

      // Data in magnetometer_data table:
      // level | colname | measurand_id | measurand_code
      // -------+---------+--------------+-----------------
      //    10 | X       |          667 | MAGNX_PT10S_AVG
      //    60 | X       |          668 | MAGNX_PT1M_AVG
      //    10 | Y       |          669 | MAGNY_PT10S_AVG
      //    60 | Y       |          670 | MAGNY_PT1M_AVG
      //    10 | Z       |          671 | MAGNZ_PT10S_AVG
      //    60 | Z       |          672 | MAGNZ_PT1M_AVG
      //    60 | T       |          144 | TTECH_PT1M_AVG
      //   110 | F       |          673 | MAGN_PT10S_AVG
    }

    // Get valid timesteps based on data and request
    auto valid_timesteps_per_fmisid =
        getValidTimeSteps(settings, timeSeriesOptions, timezones, fmisid_results);

    // Set data for each valid timestep
    for (const auto &item : fmisid_results)
    {
      auto fmisid = item.first;
      auto valid_timesteps = valid_timesteps_per_fmisid.at(fmisid);
      const auto &ts_vector = *item.second;
      for (unsigned int i = 0; i < ts_vector.size(); i++)
      {
        auto ts = ts_vector.at(i);
        std::map<Fmi::LocalDateTime, TS::TimedValue> data;
        for (unsigned int j = 0; j < ts.size(); j++)
        {
          auto timed_value = ts.at(j);
          data.insert(std::make_pair(timed_value.time, timed_value));
        }

        for (const auto &timestep : valid_timesteps)
        {
          if (data.find(timestep) != data.end())
            ret->at(i).push_back(data.at(timestep));
          else
          {
            if (data_independent_positions.find(i) != data_independent_positions.end())
              ret->at(i).push_back(ts.at(0));
            else
              ret->at(i).push_back(TS::TimedValue(timestep, TS::None()));
          }
        }
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getFlashData(const Settings &settings,
                                                 const Fmi::TimeZones &timezones)
{
  try
  {
    string stationtype = FLASH_PRODUCER;

    map<string, int> timeseriesPositions;
    map<string, int> specialPositions;

    string param;
    unsigned int param_pos = 0;
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
          timeseriesPositions[pname] = param_pos;
          param += pname + ",";
        }
      }
      else
      {
        specialPositions[name] = param_pos;
      }
      param_pos++;
    }

    param = trimCommasFromEnd(param);

    auto starttimeString = Fmi::to_string(to_epoch(settings.starttime));
    auto endtimeString = Fmi::to_string(to_epoch(settings.endtime));

    std::string query =
        "SELECT stroke_time AS stroke_time, "
        "stroke_time_fraction, flash_id, "
        "X(stroke_location) AS longitude, "
        "Y(stroke_location) AS latitude";

    if (!param.empty())
      query += ", " + param;

    query +=
        " FROM flash_data flash "
        "WHERE flash.stroke_time >= " +
        starttimeString + " AND flash.stroke_time <= " + endtimeString + " ";

    if (!settings.taggedLocations.empty())
    {
      for (const auto &tloc : settings.taggedLocations)
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
        if (tloc.loc->type == Spine::Location::BoundingBox && settings.boundingBox.empty())
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);

          query += "AND MbrWithin(flash.stroke_location, BuildMbr(" + Fmi::to_string(bbox.xMin) +
                   ", " + Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMax) + ", " +
                   Fmi::to_string(bbox.yMax) + ")) ";
        }
      }
    }
    if (!settings.boundingBox.empty())
    {
      query += "AND MbrWithin(flash.stroke_location, BuildMbr(" +
               Fmi::to_string(settings.boundingBox.at("minx")) + ", " +
               Fmi::to_string(settings.boundingBox.at("miny")) + ", " +
               Fmi::to_string(settings.boundingBox.at("maxx")) + ", " +
               Fmi::to_string(settings.boundingBox.at("maxy")) + ")) ";
    }

    query += "ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction ASC;";

    if (itsDebug)
      std::cout << "SpatiaLite: " << query << '\n';

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();

    {
      // Spine::ReadLock lock(write_mutex);
      sqlite3pp::query qry(itsDB, query.c_str());

      std::set<std::string> locations;
      std::set<Fmi::DateTime> obstimes;
      size_t n_elements = 0;
      for (auto row : qry)
      {
        map<std::string, TS::Value> result;

        // These will be always in this order
        int stroke_time = row.get<int>(0);
        // int stroke_time_fraction = row.get<int>(1);
        // int flash_id = row.get<int>(2);
        longitude = Fmi::stod(row.get<string>(3));
        latitude = Fmi::stod(row.get<string>(4));

        // Rest of the parameters in requested order
        for (int i = 5; i != qry.column_count(); ++i)
        {
          int data_type = row.column_type(i);
          TS::Value temp;
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

        Fmi::DateTime utctime = Fmi::date_time::from_time_t(stroke_time);
        auto localtz = timezones.time_zone_from_string(settings.timezone);
        Fmi::LocalDateTime localtime = Fmi::LocalDateTime(utctime, localtz);

        for (const auto &p : timeseriesPositions)
        {
          std::string name = p.first;
          int pos = p.second;

          TS::Value val = result[name];
          timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(localtime, val));
        }
        for (const auto &p : specialPositions)
        {
          string name = p.first;
          int pos = p.second;
          if (name == "latitude")
          {
            TS::Value val = latitude;
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(localtime, val));
          }
          if (name == "longitude")
          {
            TS::Value val = longitude;
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(localtime, val));
          }
        }

        n_elements += timeSeriesColumns->size();
        locations.insert(Fmi::to_string(longitude) + Fmi::to_string(latitude));
        obstimes.insert(utctime);

        check_request_limit(
            settings.requestLimits, locations.size(), TS::RequestLimitMember::LOCATIONS);
        check_request_limit(
            settings.requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
        check_request_limit(settings.requestLimits, n_elements, TS::RequestLimitMember::ELEMENTS);
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting cached flash data failed!");
  }
}

FlashDataItems SpatiaLite::readFlashCacheData(const Fmi::DateTime &starttime)
{
  try
  {
    std::string starttimeString = Fmi::to_string(to_epoch(starttime));

    // The data is sorted for the benefit of the user and FlashMemoryCache::fill
    std::string sql =
        "SELECT stroke_time as stroke_time, flash_id, "
        "multiplicity, peak_current, "
        "sensors, freedom_degree, ellipse_angle, ellipse_major, "
        "ellipse_minor, chi_square, rise_time, ptz_time, cloud_indicator, "
        "angle_indicator, signal_indicator, timing_indicator, stroke_status, "
        "data_source, modified_last AS modified_last, modified_by, "
        "X(stroke_location) AS longitude, "
        "Y(stroke_location) AS latitude "
        "FROM flash_data "
        "WHERE stroke_time >= " +
        starttimeString + " ORDER BY stroke_time, flash_id";

    if (itsDebug)
      std::cout << "SpatiaLite: " << sql << '\n';

    FlashDataItems result;

    // Spine::ReadLock lock(write_mutex);
    sqlite3pp::query qry(itsDB, sql.c_str());

    for (auto row : qry)
    {
      FlashDataItem f;

      // Note: For some reason the "created" column present in Oracle flashdata is not
      // present in the cached flash_data.

      time_t stroke_time = row.get<int>(0);
      f.stroke_time = Fmi::date_time::from_time_t(stroke_time);
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
      time_t modified_last_time = row.get<int>(18);
      f.modified_last = Fmi::date_time::from_time_t(modified_last_time);
      // this seems to always be null
      // f.modified_by = row.get<int>(19);
      f.longitude = Fmi::stod(row.get<string>(20));
      f.latitude = Fmi::stod(row.get<string>(21));

      result.emplace_back(f);
    }

    return result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading flash cache data failed!");
  }
}

FlashCounts SpatiaLite::getFlashCount(const Fmi::DateTime &starttime,
                                      const Fmi::DateTime &endtime,
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
        "WHERE flash.stroke_time BETWEEN " +
        Fmi::to_string(to_epoch(starttime)) + " AND " + Fmi::to_string(to_epoch(endtime)) + " ";

    if (!locations.empty())
    {
      for (const auto &tloc : locations)
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

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqltemplate << '\n';

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
    throw Fmi::Exception::Trace(BCP, "Getting flash count failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache)
{
  try
  {
    // Legacy variable. TODO: remove unnecessary variable
    std::string stationtype = settings.stationtype;

    // This maps measurand_id and the parameter position in TimeSeriesVector

    auto qmap = buildQueryMapping(settings, stationtype, false);

    // Should we use the cache?

    bool use_memory_cache = (observationMemoryCache != nullptr);

    if (use_memory_cache)
    {
      auto cache_start_time = observationMemoryCache->getStartTime();
      use_memory_cache =
          (!cache_start_time.is_not_a_date_time() && cache_start_time <= settings.starttime);
    }

    LocationDataItems observations =
        (use_memory_cache ? observationMemoryCache->read_observations(
                                stations, settings, stationInfo, settings.stationgroups, qmap)
                          : readObservationDataFromDB(
                                stations, settings, stationInfo, qmap, settings.stationgroups));

    std::set<int> observed_fmisids;
    for (auto item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    Engine::Observation::StationMap fmisid_to_station =
        mapQueryStations(stations, observed_fmisids);

    StationTimedMeasurandData station_data =
        buildStationTimedMeasurandData(observations, settings, timezones, fmisid_to_station);

    return buildTimeseries(
        settings, stationtype, fmisid_to_station, station_data, qmap, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting cached data from SpatiaLite failed!");
  }
}

TS::TimeSeriesVectorPtr SpatiaLite::getObservationDataForMovingStations(
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    // This maps measurand_id and the parameter position in TimeSeriesVector
    auto qmap = buildQueryMapping(settings, settings.stationtype, false);

    LocationDataItems observations =
        readObservationDataOfMovingStationsFromDB(settings, qmap, settings.stationgroups);

    StationMap fmisid_to_station;
    for (auto item : observations)
    {
      Spine::Station station;
      station.fmisid = item.data.fmisid;
      station.longitude = item.longitude;
      station.latitude = item.latitude;
      station.elevation = item.elevation;
      station.type = settings.stationtype;
      fmisid_to_station[station.fmisid] = station;
    }

    StationTimedMeasurandData station_data =
        buildStationTimedMeasurandData(observations, settings, timezones, fmisid_to_station);

    return buildTimeseries(settings,
                           settings.stationtype,
                           fmisid_to_station,
                           station_data,
                           qmap,
                           timeSeriesOptions,
                           timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting cached data from SpatiaLite database failed!");
  }
}

LocationDataItems SpatiaLite::readObservationDataOfMovingStationsFromDB(
    const Settings &settings,
    const QueryMapping &qmap,
    const std::set<std::string> & /* stationgroup_codes */)
{
  try
  {
    LocationDataItems ret;

    // Safety check
    if (qmap.measurandIds.empty())
      return ret;

    auto measurand_ids = Fmi::join(qmap.measurandIds);
    auto producerIds = Fmi::join(settings.producer_ids);

    auto fmisids =
        Fmi::join(settings.taggedFMISIDs, [](const auto &value) { return value.fmisid; });

    std::string starttime = Fmi::to_string(to_epoch(settings.starttime));
    std::string endtime = Fmi::to_string(to_epoch(settings.endtime));

    std::string sqlStmt =
        "SELECT data.fmisid, data.sensor_no, data.data_time, data.measurand_id, data.data_value, "
        "data.data_quality, data.data_source, m.lon, m.lat, m.elev FROM observation_data "
        "data JOIN moving_locations m ON (m.station_id = data.fmisid and data.data_time between "
        "m.sdate and m.edate) "
        "AND data.fmisid IN (" +
        fmisids +
        ") "
        "AND data.data_time >= " +
        starttime + " AND data.data_time <= " + endtime + " AND data.measurand_id IN (" +
        measurand_ids + ") ";
    if (!producerIds.empty())
      sqlStmt += ("AND data.producer_id IN (" + producerIds + ") ");
    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               " GROUP BY data.fmisid, data.sensor_no, data.data_time, data.measurand_id, "
               "data.data_value, data.data_quality, data.data_source, m.lon, m.lat, m.elev "
               "ORDER BY data.fmisid ASC, data.data_time ASC";

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << '\n';

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    for (const auto &row : qry)
    {
      if (row.column_type(4) == SQLITE_NULL)  // data_value
        continue;
      if (row.column_type(5) == SQLITE_NULL)  // data_quality
        continue;

      LocationDataItem obs;
      obs.data.fmisid = row.get<int>(0);
      obs.data.sensor_no = row.get<int>(1);
      time_t data_time = row.get<int>(2);
      obs.data.data_time = Fmi::date_time::from_time_t(data_time);
      obs.data.measurand_id = row.get<int>(3);
      obs.data.data_value = row.get<double>(4);
      obs.data.data_quality = row.get<double>(5);
      obs.data.data_source = row.get<double>(6);
      obs.longitude = row.get<double>(7);
      obs.latitude = row.get<double>(8);
      obs.elevation = row.get<double>(9);
      obs.stationtype = settings.stationtype;
      ret.emplace_back(obs);
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Fetching data from SpatiaLite database failed!");
  }
}

void SpatiaLite::initObservationMemoryCache(
    const Fmi::DateTime &starttime,
    const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache)
{
  try
  {
    // Read all observations starting from the given time
    std::string sql =
        "SELECT data_time, modified_last, data_value, fmisid, sensor_no, measurand_id, "
        "producer_id, measurand_no, data_quality, data_source "
        "FROM observation_data "
        "WHERE observation_data.data_time >= " +
        Fmi::to_string(to_epoch(starttime)) + " ORDER BY fmisid ASC, data_time ASC";

    sqlite3pp::query qry(itsDB, sql.c_str());

    DataItems observations;

    for (const auto &row : qry)
    {
      if (row.column_type(2) == SQLITE_NULL)  // data_value
        continue;
      if (row.column_type(8) == SQLITE_NULL)  // data_quality
        continue;

      DataItem obs;
      time_t data_time = row.get<int>(0);
      time_t modified_last_time = row.get<int>(1);
      obs.data_time = Fmi::date_time::from_time_t(data_time);
      obs.modified_last = Fmi::date_time::from_time_t(modified_last_time);
      obs.data_value = row.get<double>(2);
      obs.fmisid = row.get<int>(3);
      obs.sensor_no = row.get<int>(4);
      obs.measurand_id = row.get<int>(5);
      obs.producer_id = row.get<int>(6);
      obs.measurand_no = row.get<int>(7);
      obs.data_quality = row.get<int>(8);
      obs.data_source = row.get<int>(9);
      observations.emplace_back(obs);
    }

    // Feed them into the cache

    observationMemoryCache->fill(observations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Initializing observation memory cache failed!");
  }
}

void SpatiaLite::initExtMemoryCache(const Fmi::DateTime &starttime,
                                    const std::unique_ptr<ObservationMemoryCache> &extMemoryCache)
{
  try
  {
    // Read all observations starting from the given time
    std::string sql =
        "SELECT data_time, modified_last, data_value, fmisid, sensor_no, measurand_id, "
        "producer_id, measurand_no, data_quality, data_source "
        "FROM weather_data "
        "WHERE weather_data.data_time >= " +
        Fmi::to_string(to_epoch(starttime)) + " ORDER BY fmisid ASC, data_time ASC";

    sqlite3pp::query qry(itsDB, sql.c_str());

    DataItems observations;

    for (const auto &row : qry)
    {
      if (row.column_type(2) == SQLITE_NULL)  // data_value
        continue;
      if (row.column_type(8) == SQLITE_NULL)  // data_quality
        continue;

      DataItem obs;
      time_t data_time = row.get<int>(0);
      time_t modified_last_time = row.get<int>(1);
      obs.data_time = Fmi::date_time::from_time_t(data_time);
      obs.modified_last = Fmi::date_time::from_time_t(modified_last_time);
      obs.data_value = row.get<double>(2);
      obs.fmisid = row.get<int>(3);
      obs.sensor_no = row.get<int>(4);
      obs.measurand_id = row.get<int>(5);
      obs.producer_id = row.get<int>(6);
      obs.measurand_no = row.get<int>(7);
      obs.data_quality = row.get<int>(8);
      obs.data_source = row.get<int>(9);
      observations.emplace_back(obs);
    }

    // Feed them into the cache

    extMemoryCache->fill(observations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Initializing observation memory cache failed!");
  }
}

void SpatiaLite::fetchWeatherDataQCData(const std::string &sqlStmt,
                                        const StationInfo &stationInfo,
                                        const std::set<std::string> &stationgroup_codes,
                                        const TS::RequestLimits &requestLimits,
                                        LocationDataItems &cacheData)
{
  try
  {
    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    std::set<int> fmisids;
    std::set<Fmi::DateTime> obstimes;
    for (const auto &row : qry)
    {
      int fmisid = row.get<int>(0);
      unsigned int obstime_db = row.get<int>(1);
      Fmi::DateTime obstime = Fmi::date_time::from_time_t(obstime_db);
      // Get latitude, longitude, elevation from station info
      const Spine::Station &s = stationInfo.getStation(fmisid, stationgroup_codes, obstime);

      std::optional<double> latitude = s.latitude;
      std::optional<double> longitude = s.longitude;
      std::optional<double> elevation = s.elevation;
      std::optional<std::string> stationtype = s.type;

      int measurand_id = row.get<int>(2);
      std::optional<double> data_value;
      if (row.column_type(3) != SQLITE_NULL)
        data_value = row.get<double>(3);
      int measurand_no = row.get<int>(4);
      int sensor_no = row.get<int>(5);
      int data_quality = 0;
      if (row.column_type(6) != SQLITE_NULL)
        data_quality = row.get<int>(6);
      int data_source = -1;
      if (row.column_type(7) != SQLITE_NULL)
        data_source = row.get<int>(7);
      int producer_id = row.get<int>(8);

      DataItem item{obstime,
                    obstime,
                    data_value,
                    fmisid,
                    sensor_no,
                    measurand_id,
                    producer_id,
                    measurand_no,
                    data_quality,
                    data_source};
      LocationDataItem loc_item{
          item, *longitude, *latitude, *elevation, stationtype ? *stationtype : "UNKNOWN"};
      cacheData.push_back(loc_item);

      fmisids.insert(fmisid);
      obstimes.insert(obstime);

      check_request_limit(requestLimits, fmisids.size(), TS::RequestLimitMember::LOCATIONS);
      check_request_limit(requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
      check_request_limit(requestLimits, cacheData.size(), TS::RequestLimitMember::ELEMENTS);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

std::string SpatiaLite::sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                       const std::string &params,
                                                       const std::string &station_ids) const
{
  // This should be close to readObservationDataFromDB
  try
  {
    if (station_ids.empty())
      return {};

    auto starttime = to_epoch(settings.starttime);
    auto endtime = to_epoch(settings.endtime);

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, data.data_time AS obstime, measurand_id, data_value, "
        "measurand_no, data.sensor_no AS sensor_no, data_quality, data_source, producer_id FROM "
        "weather_data data "
        "WHERE data.fmisid IN (" +
        station_ids +
        ") "
        "AND data.data_time";

    if (starttime == endtime)
      sqlStmt += "=" + Fmi::to_string(starttime);
    else
      sqlStmt += " BETWEEN " + Fmi::to_string(starttime) + " AND " + Fmi::to_string(endtime);

    sqlStmt += " AND data.measurand_id IN (" + params + ") ";

    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << '\n';

    return sqlStmt;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Constructing SQL statement for SpatiaLite cache query failed!");
  }
}

std::string SpatiaLite::getWeatherDataQCParams(const std::set<std::string> &param_set) const
{
  try
  {
    // In sqlite cache parameters are stored as integer
    std::string params;
    for (const auto &pname : param_set)
    {
      int int_parameter = itsParameterMap->getRoadAndForeignIds().stringToInteger(pname);
      if (!params.empty())
        params += ",";
      params += Fmi::to_string(int_parameter);
    }
    return params;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

void SpatiaLite::getMovingStations(Spine::Stations &stations,
                                   const Settings &settings,
                                   const std::string &wkt)
{
  try
  {
    auto sdate = Fmi::to_string(to_epoch(settings.starttime));
    auto edate = Fmi::to_string(to_epoch(settings.endtime));

    std::string sqlStmt =
        ("SELECT distinct station_id FROM moving_locations_v1 WHERE ((sdate BETWEEN " + sdate +
         " AND " + edate + ") OR (edate BETWEEN " + sdate + " AND " + edate +
         ") OR (sdate <= " + sdate + " AND edate >=" + edate +
         ")) AND ST_Contains(ST_GeomFromText('" + wkt + "'),ST_MakePoint(lon, lat))");

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << '\n';

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    DataItems observations;

    for (const auto &row : qry)
    {
      Spine::Station station;
      station.fmisid = row.get<int>(0);
      stations.push_back(station);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Fmi::DateTime SpatiaLite::getLatestDataUpdateTime(const std::string &tablename,
                                                  const Fmi::DateTime &starttime,
                                                  const std::string &producer_ids,
                                                  const std::string &measurand_ids)
{
  try
  {
    auto start_time = to_epoch(starttime);

    Fmi::DateTime ret = Fmi::DateTime::NOT_A_DATE_TIME;

    std::string sqlStmt;
    if (tablename == OBSERVATION_DATA_TABLE)
    {
      sqlStmt =
          "select max(modified_last) from observation_data where modified_last IS NOT NULL AND "
          "modified_last >=" +
          Fmi::to_string(start_time);
      if (!producer_ids.empty())
        sqlStmt.append(" AND producer_id in(" + producer_ids + ")");
    }
    else if (tablename == WEATHER_DATA_QC_TABLE)
    {
      sqlStmt =
          "select max(modified_last) from weather_data where modified_last IS NOT NULL AND "
          "modified_last >=" +
          Fmi::to_string(start_time);
    }
    else if (tablename == FLASH_DATA_TABLE)
      sqlStmt =
          "select max(modified_last) from flash_data where modified_last IS NOT NULL AND "
          "modified_last >=" +
          Fmi::to_string(start_time);

    // std::cout << "Spatialite::getLatestDataUpdateTime: "<< sqlStmt << '\n';

    if (!sqlStmt.empty())
    {
      sqlite3pp::query qry(itsDB, sqlStmt.c_str());

      for (const auto &row : qry)
      {
        if (row.column_type(0) != SQLITE_NULL)
        {
          auto epoch_time = row.get<int>(0);
          auto ptime_time = Fmi::date_time::from_time_t(epoch_time);
          ret = ptime_time;
        }
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
