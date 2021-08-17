#include "SpatiaLite.h"
#include "DataWithQuality.h"
#include "ExternalAndMobileDBInfo.h"
#include "Keywords.h"
#include "ObservationMemoryCache.h"
#include "QueryMapping.h"
#include "SpatiaLiteCacheParameters.h"
#include <fmt/format.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/Convenience.h>
#include <spine/ParameterTools.h>
#include <spine/Reactor.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesGeneratorOptions.h>
#include <spine/TimeSeriesOutput.h>
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

namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

using boost::local_time::local_date_time;
using boost::posix_time::ptime;

namespace
{
const ptime ptime_epoch_start = from_time_t(0);

// should use std::time_t or long here, but sqlitepp does not support it. Luckily intel 64-bit int
// is 8 bytes
int to_epoch(const boost::posix_time::ptime pt)
{
  if (pt.is_not_a_date_time())
    return 0;

  return (pt - ptime_epoch_start).total_seconds();
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

    auto starttime = to_epoch(settings.starttime);
    auto endtime = to_epoch(settings.endtime);

    std::string sqlStmt =
        "SELECT data.fmisid AS fmisid, data.sensor_no AS sensor_no, data.data_time AS obstime, "
        "measurand_id, measurand_no, data_value, data_quality, data_source FROM observation_data "
        "data "
        "WHERE data.fmisid IN (" +
        qstations +
        ") "
        "AND data.data_time >= " +
        Fmi::to_string(starttime) + " AND data.data_time <= " + Fmi::to_string(endtime) +
        " AND data.measurand_id IN (" + measurand_ids + ") ";
    if (!producerIds.empty())
      sqlStmt += ("AND data.producer_id IN (" + producerIds + ") ");

    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               " GROUP BY data.fmisid, data.sensor_no, data.data_time, data.measurand_id, "
               "data.data_value, data.data_quality, data.data_source "
               "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << std::endl;

    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    for (const auto &row : qry)
    {
      LocationDataItem obs;
      time_t epoch_time = row.get<int>(2);
      obs.data.data_time = boost::posix_time::from_time_t(epoch_time);
      obs.data.fmisid = row.get<int>(0);
      obs.data.sensor_no = row.get<int>(1);
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

      obs.data.measurand_id = row.get<int>(3);
      obs.data.measurand_no = row.get<int>(4);
      if (row.column_type(5) != SQLITE_NULL)
        obs.data.data_value = row.get<double>(5);
      if (row.column_type(6) != SQLITE_NULL)
        obs.data.data_quality = row.get<int>(6);
      if (row.column_type(7) != SQLITE_NULL)
        obs.data.data_source = row.get<int>(7);

      ret.emplace_back(obs);
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
    throw Fmi::Exception::Trace(BCP, "Connecting database '" + spatialiteFile + "' failed!");
  }
}

SpatiaLite::~SpatiaLite() = default;

void SpatiaLite::createTables(const std::set<std::string> &tables)
{
  try
  {
    if (itsReadOnly)
      return;

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
    if (tables.find(BK_HYDROMETA_DATA_TABLE) != tables.end())
      createBKHydrometaDataTable();
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
  std::cout << "  -- Shutdown requested (SpatiaLite)\n";
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

    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON "
        "observation_data(data_time);");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS observation_data_fmisid_idx ON observation_data(fmisid);");
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
                << " [SpatiaLite] Adding modified_last column to observation_data table"
                << std::endl;
      itsDB.execute(
          "ALTER TABLE observation_data ADD COLUMN modified_last INTEGER NOT NULL DEFAULT 0");
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] ... Updating all modified_last columns in observation_data table"
                << std::endl;
      itsDB.execute("UPDATE observation_data SET modified_last=data_time");
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] ... Creating modified_last index in observation_data table"
                << std::endl;
      itsDB.execute(
          "CREATE INDEX observation_data_modified_last_idx ON observation_data(modified_last)");
      std::cout << Spine::log_time_str() << " [SpatiaLite] modified_last processing done"
                << std::endl;
    }
    catch (const std::exception &e)
    {
      throw Fmi::Exception::Trace(BCP,
                                  "Failed to add modified_last column to observation_data TABLE!");
    }
  }
}

void SpatiaLite::createWeatherDataQCTable()
{
  try
  {
    // No locking needed during initialization phase
    itsDB.execute(
        "CREATE TABLE IF NOT EXISTS weather_data_qc ("
        "fmisid INTEGER NOT NULL, "
        "obstime INTEGER NOT NULL, "
        "parameter INTEGER NOT NULL, "
        "sensor_no INTEGER NOT NULL, "
        "value REAL NOT NULL, "
        "flag INTEGER NOT NULL, "
        "modified_last INTEGER, "
        "PRIMARY KEY (obstime, fmisid, parameter, sensor_no));");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_obstime_idx ON weather_data_qc(obstime);");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_fmisid_idx ON weather_data_qc(fmisid);");
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS weather_data_qc_modified_last_idx ON "
        "weather_data_qc(modified_last);");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of weather_data_qc table failed!");
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
    itsDB.execute(
        "CREATE INDEX IF NOT EXISTS flash_data_stroke_time_idx on flash_data(stroke_time);");
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
                << std::endl;
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
                << " [SpatiaLite] Adding spatial index to ext_obsdata_roadcloud table" << std::endl;
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
                << " [SpatiaLite] Adding spatial index to ext_obsdata_netatmo table" << std::endl;
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
                << " [SpatiaLite] Adding spatial index to ext_obsdata_fmi_iot table" << std::endl;
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_fmi_iot', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_fmi_iot table failed!");
  }
}

void SpatiaLite::createBKHydrometaDataTable()
{
  try
  {
    sqlite3pp::transaction xct(itsDB);
    sqlite3pp::command cmd(itsDB,
                           "CREATE TABLE IF NOT EXISTS ext_obsdata_bk_hydrometa("
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
      sqlite3pp::query qry(itsDB,
                           "SELECT X(geom) AS latitude FROM ext_obsdata_bk_hydrometa LIMIT 1");
      qry.begin();
    }
    catch (const std::exception &e)
    {
      sqlite3pp::query qry(itsDB,
                           "SELECT AddGeometryColumn('ext_obsdata_bk_hydrometa', 'geom', "
                           "4326, 'POINT', 'XY')");
      qry.begin();
      itsDB.execute(
          "ALTER TABLE ext_obsdata_bk_hydrometa ADD PRIMARY KEY (prod_id,mid,data_time, geom)");
    }

    // Check whether the spatial index exists already
    sqlite3pp::query qry(itsDB,
                         "SELECT spatial_index_enabled FROM geometry_columns "
                         "WHERE f_table_name='ext_obsdata_bk_hydrometa' AND f_geometry_column = "
                         "'geom'");
    int spatial_index_enabled = 0;
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter != qry.end())
      (*iter).getter() >> spatial_index_enabled;

    if (spatial_index_enabled == 0)
    {
      std::cout << Spine::log_time_str()
                << " [SpatiaLite] Adding spatial index to ext_obsdata_bk_hydrometa table"
                << std::endl;
      itsDB.execute("SELECT CreateSpatialIndex('ext_obsdata_bk_hydrometa', 'geom')");
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Creation of ext_obsdata_bk_hydrometa table failed!");
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

ptime SpatiaLite::getLatestObservationTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(data_time) FROM observation_data");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest observation time query failed!");
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

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Modified last observation time query failed!");
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

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Oldest observation time query failed!");
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

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest WeatherDataQCTime query failed!");
  }
}

ptime SpatiaLite::getLatestWeatherDataQCModifiedTime()
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    sqlite3pp::query qry(itsDB, "SELECT MAX(modified_last) FROM weather_data_qc");
    sqlite3pp::query::iterator iter = qry.begin();
    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Modified last WeatherDataQCTime query failed!");
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

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
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
ptime SpatiaLite::getLatestFlashModifiedTime()
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
    throw Fmi::Exception::Trace(BCP, "Latest flash time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Oldest flash time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Oldest RoadCloud time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest RoadCloud data time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest RoadCloud creaed time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Oldest NetAtmo data time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest NetAtmo data time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest NetAtmo created time query failed!");
  }
}

ptime SpatiaLite::getOldestBKHydrometaDataTime()
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

ptime SpatiaLite::getLatestBKHydrometaDataTime()
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

boost::posix_time::ptime SpatiaLite::getLatestBKHydrometaCreatedTime()
{
  try
  {
    string tablename = "ext_obsdata_bk_hydrometa";
    string time_field = "created";
    return getLatestTimeFromTable(tablename, time_field);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Latest bk_hydrometa created time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Oldest FmiIoT data time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest FmiIoT data time query failed!");
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
    throw Fmi::Exception::Trace(BCP, "Latest FmiIoT created time query failed!");
  }
}

ptime SpatiaLite::getLatestTimeFromTable(const std::string &tablename,
                                         const std::string &time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT MAX(" + time_field + ") FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;

    time_t epoch_time = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch_time);
  }
  catch (...)
  {
    return boost::posix_time::not_a_date_time;
  }
}

ptime SpatiaLite::getOldestTimeFromTable(const std::string &tablename,
                                         const std::string &time_field)
{
  try
  {
    // Spine::ReadLock lock(write_mutex);

    std::string stmt = ("SELECT MIN(" + time_field + ") FROM " + tablename);
    sqlite3pp::query qry(itsDB, stmt.c_str());
    sqlite3pp::query::iterator iter = qry.begin();

    if (iter == qry.end() || (*iter).column_type(0) == SQLITE_NULL)
      return boost::posix_time::not_a_date_time;

    time_t epoch = (*iter).get<int>(0);
    return boost::posix_time::from_time_t(epoch);
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

void SpatiaLite::cleanWeatherDataQCCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestWeatherDataQCTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB, "DELETE FROM weather_data_qc WHERE obstime < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of WeatherDataQCCache failed!");
  }
}

void SpatiaLite::cleanFlashDataCache(const ptime &newstarttime)
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

void SpatiaLite::cleanRoadCloudCache(const ptime &newstarttime)
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

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getNetAtmoData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  return getMobileAndExternalData(settings, timezones);
}

void SpatiaLite::cleanBKHydrometaCache(const ptime &newstarttime)
{
  try
  {
    auto oldest = getOldestBKHydrometaDataTime();
    if (newstarttime <= oldest)
      return;

    auto epoch_time = to_epoch(newstarttime);

    Spine::WriteLock lock(write_mutex);

    sqlite3pp::command cmd(itsDB,
                           "DELETE FROM ext_obsdata_bk_hydrometa WHERE data_time < :timestring");

    cmd.bind(":timestring", epoch_time);
    cmd.execute();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Cleaning of bk_hydrometa cache failed!");
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getBKHydrometaData(
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
                                                    settings.dataFilter,
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
      for (int i = 0; i < column_count; i++)
      {
        std::string column_name = qry.column_name(i);

        int data_type = row.column_type(i);
        ts::Value value = ts::None();
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
              boost::posix_time::ptime pt = boost::posix_time::from_time_t(data_time);
              boost::local_time::local_date_time ldt(pt, zone);
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
        ts::Value val = result[paramname];
        ret->at(index).emplace_back(ts::TimedValue(timestep, val));
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

std::size_t SpatiaLite::fillDataCache(const DataItems &cacheData, InsertStatus &insertStatus)
{
  try
  {
    std::size_t new_item_count = 0;

    if (cacheData.empty())
      return new_item_count;

    const char *sqltemplate =
        "INSERT OR REPLACE INTO observation_data "
        "(fmisid, sensor_no, measurand_id, producer_id, measurand_no, data_time, modified_last, "
        "data_value, data_quality, data_source) "
        "VALUES "
        "(:fmisid,:sensor_no,:measurand_id,:producer_id,:measurand_no,:data_time, :modified_last, "
        ":data_value,:data_quality,:data_source);";

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
          data_times.emplace_back(to_epoch(obs.data_time));
          modified_last_times.emplace_back(to_epoch(obs.modified_last));
        }

        {
          Spine::WriteLock lock(write_mutex);

          sqlite3pp::transaction xct(itsDB);
          sqlite3pp::command cmd(itsDB, sqltemplate);

          for (std::size_t i = 0; i < insert_size; i++)
          {
            const auto &item = cacheData[new_items[i]];
            cmd.bind(":fmisid", item.fmisid);
            cmd.bind(":sensor_no", item.sensor_no);
            cmd.bind(":measurand_id", item.measurand_id);
            cmd.bind(":producer_id", item.producer_id);
            cmd.bind(":measurand_no", item.measurand_no);
            cmd.bind(":data_time", data_times[i]);
            cmd.bind(":modified_last", modified_last_times[i]);
            if (item.data_value)
              cmd.bind(":data_value", *item.data_value);
            else
              cmd.bind(":data_value");  // NULL
            cmd.bind(":data_quality", item.data_quality);
            if (item.data_source >= 0)
              cmd.bind(":data_source", item.data_source);
            else
              cmd.bind(":data_source");  // NULL
            cmd.execute();
            cmd.reset();
          }
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (const auto &hash : new_hashes)
          insertStatus.add(hash);

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
    throw Fmi::Exception::Trace(BCP, "Filling of data cache failed!");
  }
}

std::size_t SpatiaLite::fillWeatherDataQCCache(const WeatherDataQCItems &cacheData,
                                               InsertStatus &insertStatus)
{
  try
  {
    std::size_t new_item_count = 0;

    if (cacheData.empty())
      return new_item_count;

    const char *sqltemplate =
        "INSERT OR IGNORE INTO weather_data_qc"
        "(fmisid, obstime, parameter, sensor_no, value, flag, modified_last)"
        "VALUES (:fmisid,:obstime,:parameter,:sensor_no,:value,:flag,:modified_last)";

    // Loop over all observations, inserting only new items in groups to the cache

    std::vector<std::size_t> new_items;
    std::vector<std::size_t> new_hashes;
    std::vector<int> data_times;
    std::vector<int> modified_last_times;
    std::vector<int> parameter_ids;

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
          data_times.emplace_back(to_epoch(obs.obstime));
          if (item.modified_last.is_not_a_date_time())
            modified_last_times.push_back(0);
          else
            modified_last_times.emplace_back(to_epoch(obs.modified_last));
          parameter_ids.emplace_back(
              itsParameterMap->getRoadAndForeignIds().stringToInteger(obs.parameter));
        }

        {
          Spine::WriteLock lock(write_mutex);

          sqlite3pp::transaction xct(itsDB);
          sqlite3pp::command cmd(itsDB, sqltemplate);

          for (std::size_t i = 0; i < insert_size; i++)
          {
            const auto &item = cacheData[new_items[i]];

            cmd.bind(":fmisid", item.fmisid);
            cmd.bind(":obstime", data_times[i]);
            cmd.bind(":parameter", parameter_ids[i]);
            cmd.bind(":sensor_no", item.sensor_no);
            if (item.value)
              cmd.bind(":value", *item.value);
            else
              cmd.bind(":value");  // NULL
            cmd.bind(":flag", item.flag);
            cmd.bind(":modified_last", modified_last_times[i]);
            cmd.execute();
            cmd.reset();
          }
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (const auto &hash : new_hashes)
          insertStatus.add(hash);

        new_item_count += insert_size;

        new_items.clear();
        new_hashes.clear();
        data_times.clear();
        modified_last_times.clear();
        parameter_ids.clear();
      }
    }

    return new_item_count;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Filling of WeatherDataQCCache failed!");
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
        "INSERT OR IGNORE INTO flash_data "
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
            const auto &item = cacheData[new_items[i]];

            // @todo There is no simple way to optionally set possible NULL values.
            // Find out later how to do it.

            try
            {
              cmd.bind(":timestring", stroke_times[i]);
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
              cmd.bind(":created", created_times[i]);
              cmd.bind(":modified_last", modified_last_times[i]);
              std::string stroke_point = "POINT(" + Fmi::to_string("%.10g", item.longitude) + " " +
                                         Fmi::to_string("%.10g", item.latitude) + ")";
              cmd.bind(":stroke_point", stroke_point, sqlite3pp::nocopy);
              cmd.execute();
              cmd.reset();
            }
            catch (const std::exception &e)
            {
              std::cerr << "Problem updating flash data: " << e.what() << std::endl;
            }
          }

          // Would it be possible to place the writelock here...????
          xct.commit();
          // lock is released
        }

        // Update insert status, giving readers some time to obtain a read lock
        for (const auto &hash : new_hashes)
          insertStatus.add(hash);

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
    throw Fmi::Exception::Trace(BCP, "NetAtmo cache update failed!");
  }

  return 0;
}

std::size_t SpatiaLite::fillBKHydrometaCache(const MobileExternalDataItems &mobileExternalCacheData,
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
            "INSERT OR IGNORE INTO ext_obsdata_bk_hydrometa "
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
          std::cerr << "Problem updating BK_hydrometa cache: " << e.what() << std::endl;
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
    throw Fmi::Exception::Trace(BCP, "BK_hydrometa cache update failed!");
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
          std::cerr << "Problem updating FmiIoT cache: " << e.what() << std::endl;
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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getFlashData(const Settings &settings,
                                                                const Fmi::TimeZones &timezones)
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

    auto starttimeString = Fmi::to_string(to_epoch(settings.starttime));
    auto endtimeString = Fmi::to_string(to_epoch(settings.endtime));

    std::string distancequery;

    std::string query;
    query =
        "SELECT stroke_time AS stroke_time, "
        "stroke_time_fraction, flash_id, "
        "X(stroke_location) AS longitude, "
        "Y(stroke_location) AS latitude, " +
        param +
        " "
        "FROM flash_data flash "
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

    if (itsDebug)
      std::cout << "SpatiaLite: " << query << std::endl;

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        initializeResultVector(settings.parameters);

    int stroke_time = 0;
    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();

    {
      // Spine::ReadLock lock(write_mutex);
      sqlite3pp::query qry(itsDB, query.c_str());

      for (auto row : qry)
      {
        map<std::string, ts::Value> result;

        // These will be always in this order
        stroke_time = row.get<int>(0);
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

        ptime utctime = boost::posix_time::from_time_t(stroke_time);
        auto localtz = timezones.time_zone_from_string(settings.timezone);
        local_date_time localtime = local_date_time(utctime, localtz);

        std::pair<string, int> p;
        for (const auto &p : timeseriesPositions)
        {
          std::string name = p.first;
          int pos = p.second;

          ts::Value val = result[name];
          timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(localtime, val));
        }
        for (const auto &p : specialPositions)
        {
          string name = p.first;
          int pos = p.second;
          if (name == "latitude")
          {
            ts::Value val = latitude;
            timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(localtime, val));
          }
          if (name == "longitude")
          {
            ts::Value val = longitude;
            timeSeriesColumns->at(pos).emplace_back(ts::TimedValue(localtime, val));
          }
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting cached flash data failed!");
  }
}

FlashDataItems SpatiaLite::readFlashCacheData(const ptime &starttime)
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
      std::cout << "SpatiaLite: " << sql << std::endl;

    FlashDataItems result;

    // Spine::ReadLock lock(write_mutex);
    sqlite3pp::query qry(itsDB, sql.c_str());

    for (auto row : qry)
    {
      FlashDataItem f;

      // Note: For some reason the "created" column present in Oracle flashdata is not
      // present in the cached flash_data.

      time_t stroke_time = row.get<int>(0);
      f.stroke_time = boost::posix_time::from_time_t(stroke_time);
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
      f.modified_last = boost::posix_time::from_time_t(modified_last_time);
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
      std::cout << "SpatiaLite: " << sqltemplate << std::endl;

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

Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache)
{
  try
  {
    // Legacy variable. TODO: remove unnecessary variable
    std::string stationtype = settings.stationtype;

    // This maps measurand_id and the parameter position in TimeSeriesVector

    auto qmap = buildQueryMapping(stations, settings, itsParameterMap, stationtype, false);

    // Resolve stationgroup codes
    std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet =
        itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    // Should we use the cache?

    bool use_memory_cache = (observationMemoryCache.get() != nullptr);
    if (use_memory_cache)
    {
      auto cache_start_time = observationMemoryCache->getStartTime();
      use_memory_cache =
          (!cache_start_time.is_not_a_date_time() && cache_start_time <= settings.starttime);
    }

    LocationDataItems observations =
        (use_memory_cache ? observationMemoryCache->read_observations(
                                stations, settings, stationInfo, stationgroup_codes, qmap)
                          : readObservationDataFromDB(
                                stations, settings, stationInfo, qmap, stationgroup_codes));

    std::set<int> observed_fmisids;
    for (auto item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    Engine::Observation::StationMap fmisid_to_station =
        mapQueryStations(stations, observed_fmisids);

    StationTimedMeasurandData station_data =
        buildStationTimedMeasurandData(observations, settings, timezones, fmisid_to_station);

    return buildTimeseries(stations,
                           settings,
                           stationtype,
                           fmisid_to_station,
                           station_data,
                           qmap,
                           timeSeriesOptions,
                           timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting cached data failed!");
  }
}

void SpatiaLite::initObservationMemoryCache(
    const boost::posix_time::ptime &starttime,
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
        Fmi::to_string(to_epoch(starttime)) +
        " GROUP BY fmisid, sensor_no, data_time, measurand_id, data_value, data_quality, "
        "data_source "
        "ORDER BY fmisid ASC, data_time ASC";

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
      obs.data_time = boost::posix_time::from_time_t(data_time);
      obs.modified_last = boost::posix_time::from_time_t(modified_last_time);
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

void SpatiaLite::fetchWeatherDataQCData(const std::string &sqlStmt,
                                        const StationInfo &stationInfo,
                                        const std::set<std::string> &stationgroup_codes,
                                        const QueryMapping & /* qmap */,
                                        WeatherDataQCData &cacheData)
{
  try
  {
    sqlite3pp::query qry(itsDB, sqlStmt.c_str());

    for (const auto &row : qry)
    {
      boost::optional<int> fmisid = row.get<int>(0);
      unsigned int obstime_db = row.get<int>(1);
      boost::posix_time::ptime obstime = boost::posix_time::from_time_t(obstime_db);
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

      int parameter = row.get<int>(2);
      boost::optional<double> data_value;
      if (row.column_type(3) != SQLITE_NULL)
        data_value = row.get<double>(3);
      boost::optional<int> sensor_no = row.get<int>(4);
      boost::optional<int> data_quality = row.get<int>(5);

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
                                "Fetching data from SpatiaLite WeatherDataQCData cache failed!");
  }
}

std::string SpatiaLite::sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                       const std::string &params,
                                                       const std::string &station_ids) const
{
  try
  {
    std::string sqlStmt;
    auto starttime = to_epoch(settings.starttime);
    auto endtime = to_epoch(settings.endtime);

    if (settings.latest)
    {
      sqlStmt =
          "SELECT data.fmisid AS fmisid, MAX(data.obstime) AS obstime, "
          "data.parameter, data.value, data.sensor_no, data.flag as data_quality "
          "FROM weather_data_qc data "
          "WHERE data.fmisid IN (" +
          station_ids +
          ") "
          "AND data.obstime BETWEEN " +
          Fmi::to_string(starttime) + " AND " + Fmi::to_string(endtime) +
          " AND data.parameter IN (" + params + ") AND " +
          settings.dataFilter.getSqlClause("data_quality", "data.flag") +
          " GROUP BY data.fmisid, data.parameter, data.sensor_no "
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
          "AND data.obstime BETWEEN " +
          Fmi::to_string(starttime) + " AND " + Fmi::to_string(endtime) +
          " AND data.parameter IN (" + params + ") AND " +
          settings.dataFilter.getSqlClause("data_quality", "data.flag") +
          " GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

    if (itsDebug)
      std::cout << "SpatiaLite: " << sqlStmt << std::endl;

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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
