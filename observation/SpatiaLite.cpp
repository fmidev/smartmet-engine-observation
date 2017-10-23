#include "SpatiaLite.h"

#include <spine/Exception.h>
#include <spine/Thread.h>
#include <spine/TimeSeriesOutput.h>

#include "ObservableProperty.h"
#include <macgyver/StringConversion.h>

#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation

#include <boost-tuple.h>

#include <chrono>
#include <iostream>

namespace BO = SmartMet::Engine::Observation;
namespace ts = SmartMet::Spine::TimeSeries;

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

typedef soci::rowset<soci::row> SociRow;
typedef std::unique_ptr<SociRow> SociRowPtr;

namespace SmartMet
{
// Mutex for write operations - otherwise you get table locked errors
// in MULTITHREAD-mode.

SmartMet::Spine::MutexType write_mutex;

namespace Engine
{
namespace Observation
{
namespace
{
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

};  // anonaymous namespace

SpatiaLite::SpatiaLite(const std::string &spatialiteFile,
                       std::size_t max_insert_size,
                       const std::string &synchronous,
                       const std::string &journal_mode,
                       std::size_t mmap_size,
                       bool shared_cache,
                       int timeout)
    : itsMaxInsertSize(max_insert_size)
{
  try
  {
    itsShutdownRequested = false;
    srid = "4326";

    std::string options = "db=" + spatialiteFile + "." + DATABASE_VERSION;

    // Enabling shared cache may decrease read performance:
    // https://manski.net/2012/10/sqlite-performance/
    // However, for a single shared db it may be better to share:
    // https://github.com/mapnik/mapnik/issues/797

    options += " shared_cache=";
    options += (shared_cache ? "true" : "false");

    // Timeout
    options += " timeout=" + Fmi::to_string(timeout);

    // Default is fully synchronous (2), with WAL normal (1) is supposedly
    // better, for best speed we
    // choose off (0), since this is only a cache.
    options += " synchronous=" + synchronous;

    soci::connection_parameters parameters("sqlite3", options);
    itsSession.open(parameters);

    void *cache;
    cache = sqlite_api::spatialite_alloc_connection();

    soci::sqlite3_session_backend *backend =
        reinterpret_cast<soci::sqlite3_session_backend *>(itsSession.get_backend());

    sqlite_api::spatialite_init_ex((backend->conn_), cache, 0);

    // SOCI executes plain strings immediately
    itsSession << "PRAGMA journal_mode=" << journal_mode << ";";
    itsSession << "PRAGMA mmap_size=" << mmap_size << ";";
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SpatiaLite::~SpatiaLite()
{
}

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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);

    itsSession << "CREATE TABLE IF NOT EXISTS locations("
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
                  "time_zone_abbrev TEXT);)";

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createStationGroupsTable()
{
  try
  {
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);
    itsSession << "CREATE TABLE IF NOT EXISTS station_groups ("
                  "group_id INTEGER NOT NULL PRIMARY KEY, "
                  "group_code TEXT);";
    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createGroupMembersTable()
{
  try
  {
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);
    itsSession << "CREATE TABLE IF NOT EXISTS group_members ("
                  "group_id INTEGER NOT NULL, "
                  "fmisid INTEGER NOT NULL, "
                  "CONSTRAINT fk_station_groups FOREIGN KEY (group_id) "
                  "REFERENCES station_groups "
                  "(group_id), "
                  "CONSTRAINT fk_stations FOREIGN KEY (fmisid) REFERENCES "
                  "stations (fmisid)"
                  "); "
                  "CREATE INDEX IF NOT EXISTS gm_sg_idx ON group_members "
                  "(group_id,fmisid);";
    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createObservationDataTable()
{
  try
  {
    // No locking needed during initialization phase
    soci::transaction tr(itsSession);

    itsSession << "CREATE TABLE IF NOT EXISTS observation_data("
                  "fmisid INTEGER NOT NULL, "
                  "data_time DATETIME NOT NULL, "
                  "measurand_id INTEGER NOT NULL,"
                  "producer_id INTEGER NOT NULL,"
                  "measurand_no INTEGER NOT NULL,"
                  "data_value REAL, "
                  "data_quality INTEGER, "
                  "PRIMARY KEY (fmisid, data_time, measurand_id, producer_id, "
                  "measurand_no));";
    itsSession << "CREATE INDEX IF NOT EXISTS observation_data_data_time_idx ON "
                  "observation_data(data_time);";

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createWeatherDataQCTable()
{
  try
  {
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);

    itsSession << "CREATE TABLE IF NOT EXISTS weather_data_qc ("
                  "fmisid INTEGER NOT NULL, "
                  "obstime DATETIME NOT NULL, "
                  "parameter TEXT NOT NULL, "
                  "sensor_no INTEGER NOT NULL, "
                  "value REAL NOT NULL, "
                  "flag INTEGER NOT NULL, "
                  "PRIMARY KEY (fmisid, obstime, parameter, sensor_no));";
    itsSession << "CREATE INDEX IF NOT EXISTS weather_data_qc_obstime_idx ON "
                  "weather_data_qc(obstime);";

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createFlashDataTable()
{
  try
  {
    std::string createsql;
    createsql =
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
        "PRIMARY KEY (flash_id, stroke_time, stroke_time_fraction));";

    itsSession << createsql;
    itsSession << "CREATE INDEX IF NOT EXISTS flash_data_stroke_time_idx ON "
                  "flash_data(stroke_time);";

    try
    {
      double test;
      itsSession << "SELECT X(stroke_location) AS latitude FROM flash_data LIMIT 1",
          soci::into(test);
    }
    catch (std::exception const &e)
    {
      itsSession << "SELECT AddGeometryColumn('flash_data', 'stroke_location', "
                    "4326, 'POINT', 'XY');";
    }

    // Check whether the spatial index exists already

    int spatial_index_enabled;
    itsSession << "SELECT spatial_index_enabled FROM geometry_columns "
                  "WHERE f_table_name='flash_data' AND f_geometry_column = "
                  "'stroke_location';",
        soci::into(spatial_index_enabled);

    if (!itsSession.got_data() || spatial_index_enabled == 0)
    {
      itsSession << "SELECT CreateSpatialIndex('flash_data', 'stroke_location');";
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    soci::indicator indicator;
    itsSession << "SELECT name FROM sqlite_master WHERE type='table' AND name "
                  "= 'spatial_ref_sys';",
        soci::into(name, indicator);

    if (indicator == soci::i_null)
    {
      itsSession << "SELECT InitSpatialMetaData();";
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createStationTable()
{
  try
  {
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);

    itsSession << "CREATE TABLE IF NOT EXISTS stations("
                  "fmisid INTEGER NOT NULL, "
                  "wmo INTEGER, "
                  "geoid INTEGER, "
                  "lpnn INTEGER, "
                  "rwsid INTEGER, "
                  "station_start DATETIME, "
                  "station_end DATETIME, "
                  "station_formal_name TEXT NOT NULL, "
                  "PRIMARY KEY (fmisid, geoid, station_start, station_end)); "
                  "SELECT AddGeometryColumn('stations', 'the_geom', 4326, "
                  "'POINT', 'XY');";

    tr.commit();

    int spatial_index_enabled = 0;
    itsSession << "SELECT spatial_index_enabled FROM geometry_columns "
                  "WHERE f_table_name='stations' AND f_geometry_column='the_geom';",
        soci::into(spatial_index_enabled);

    if (!itsSession.got_data() || spatial_index_enabled == 0)
    {
      std::cout << "Adding spatial index to stations table" << std::endl;
      itsSession << "SELECT CreateSpatialIndex('stations','the_geom')";
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

size_t SpatiaLite::getStationCount()
{
  try
  {
    size_t count;
    itsSession << "SELECT COUNT(*) FROM stations", soci::into(count);
    return count;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getLatestObservationTime()
{
  try
  {
    boost::optional<std::tm> time;

    const char *latestTime = "SELECT MAX(data_time) FROM observation_data";
    itsSession << latestTime, soci::into(time);

    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return a bad time
      return boost::posix_time::not_a_date_time;
    };
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getOldestObservationTime()
{
  try
  {
    boost::optional<std::tm> time;

    const char *oldestTime = "SELECT MIN(data_time) FROM observation_data";
    itsSession << oldestTime, soci::into(time);

    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return a bad time
      return boost::posix_time::not_a_date_time;
    };
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getLatestWeatherDataQCTime()
{
  try
  {
    boost::optional<std::tm> time;

    // Add 2 hours more observation for safety reasons
    const char *latestTime = "SELECT MAX(obstime) FROM weather_data_qc";
    itsSession << latestTime, soci::into(time);

    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return a bad time
      return boost::posix_time::not_a_date_time;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getOldestWeatherDataQCTime()
{
  try
  {
    boost::optional<std::tm> time;

    const char *oldestTime = "SELECT MIN(obstime) FROM weather_data_qc";
    itsSession << oldestTime, soci::into(time);

    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return a bad time
      return boost::posix_time::not_a_date_time;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getLatestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  try
  {
    boost::optional<std::tm> time;

    std::string latestTime = "SELECT DATETIME(MAX(" + time_field + ")) FROM " + tablename;
    try
    {
      itsSession << latestTime, soci::into(time);
    }
    catch (...)
    {
    }
    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return
      // not_a_date_time
      return boost::posix_time::not_a_date_time;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

boost::posix_time::ptime SpatiaLite::getOldestTimeFromTable(const std::string tablename,
                                                            const std::string time_field)
{
  try
  {
    boost::optional<std::tm> time;

    std::string oldestTime = "SELECT DATETIME(MIN(" + time_field + ")) FROM " + tablename;
    try
    {
      itsSession << oldestTime, soci::into(time);
    }
    catch (...)
    {
    }
    if (time.is_initialized())
      return boost::posix_time::ptime_from_tm(time.get());
    else
    {
      // If there is no cached observations in the database, return
      // not_a_date_time
      return boost::posix_time::not_a_date_time;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
      const char *sqltemplate =
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
          ":time_zone_abbrev);";

      SmartMet::Spine::WriteLock lock(write_mutex);

      soci::transaction tr(itsSession);
      for (const LocationItem &item : locations)
      {
        itsSession << sqltemplate, soci::use(item.location_id), soci::use(item.fmisid),
            soci::use(item.country_id), soci::use(to_tm(item.location_start)),
            soci::use(to_tm(item.location_end)), soci::use(item.longitude),
            soci::use(item.latitude), soci::use(item.x), soci::use(item.y),
            soci::use(item.elevation), soci::use(item.time_zone_name),
            soci::use(item.time_zone_abbrev);
      }

      tr.commit();
      // Success!
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
      throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
    boost::this_thread::sleep(boost::posix_time::milliseconds(1000));
  }
}

void SpatiaLite::cleanDataCache(const boost::posix_time::ptime &timetokeep)
{
  try
  {
    SmartMet::Spine::WriteLock lock(write_mutex);

    itsSession << "DELETE FROM observation_data WHERE data_time < :timetokeep;",
        soci::use(to_tm(timetokeep));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::cleanWeatherDataQCCache(const boost::posix_time::ptime &timetokeep)
{
  try
  {
    SmartMet::Spine::WriteLock lock(write_mutex);
    itsSession << "DELETE FROM weather_data_qc WHERE obstime < :timetokeep;",
        soci::use(to_tm(timetokeep));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::cleanFlashDataCache(const boost::posix_time::ptime &timetokeep)
{
  try
  {
    SmartMet::Spine::WriteLock lock(write_mutex);
    itsSession << "DELETE FROM flash_data WHERE stroke_time < :timetokeep",
        soci::use(to_tm(timetokeep));
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::fillDataCache(const vector<DataItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return;

    const char *sqltemplate =
        "INSERT OR REPLACE INTO observation_data "
        "(fmisid, measurand_id, producer_id, measurand_no, data_time, "
        "data_value, data_quality) "
        "VALUES "
        "(:fmisid,:measurand_id,:producer_id,:measurand_no,:data_time,:data_"
        "value,:data_quality);";

    std::size_t pos1 = 0;

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;
      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        boost::this_thread::yield();
        // std::cout << "," << std::flush;
      }

      SmartMet::Spine::WriteLock lock(write_mutex);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, cacheData.size());

      {
        // auto begin = std::chrono::high_resolution_clock::now();

        soci::transaction tr(itsSession);
        for (std::size_t i = pos1; i < pos2; ++i)
        {
          const auto &item = cacheData[i];
          itsSession << sqltemplate, soci::use(item.fmisid), soci::use(item.measurand_id),
              soci::use(item.producer_id), soci::use(item.measurand_no),
              soci::use(to_tm(item.data_time)), soci::use(item.data_value),
              soci::use(item.data_quality);
        }
        tr.commit();
        // auto end = std::chrono::high_resolution_clock::now();
        // std::cout << "Cached " << (pos2 - pos1 + 1) << " observations in "
        // << std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count()
        // << " ms" << std::endl;
      }

      pos1 += itsMaxInsertSize;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::fillWeatherDataQCCache(const vector<WeatherDataQCItem> &cacheData)
{
  try
  {
    if (cacheData.empty())
      return;

    const char *sqltemplate =
        "INSERT OR IGNORE INTO weather_data_qc"
        "(fmisid, obstime, parameter, sensor_no, value, flag)"
        "VALUES (:fmisid,:obstime,:parameter,:sensor_no,:value,:flag);";

    std::size_t pos1 = 0;

    while (pos1 < cacheData.size())
    {
      if (itsShutdownRequested)
        break;

      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        boost::this_thread::yield();
        // std::cout << "-" << std::flush;
      }

      SmartMet::Spine::WriteLock lock(write_mutex);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, cacheData.size());

      soci::transaction tr(itsSession);
      for (std::size_t i = pos1; i < pos2; ++i)
      {
        const auto &item = cacheData[i];
        itsSession << sqltemplate, soci::use(item.fmisid), soci::use(to_tm(item.obstime)),
            soci::use(item.parameter), soci::use(item.sensor_no), soci::use(item.value),
            soci::use(item.flag);
      }
      tr.commit();

      pos1 += itsMaxInsertSize;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::fillFlashDataCache(const vector<FlashDataItem> &flashCacheData)
{
  try
  {
    std::size_t pos1 = 0;

    while (pos1 < flashCacheData.size())
    {
      // Yield if there is more than 1 block
      if (pos1 > 0)
      {
        boost::this_thread::yield();
        // std::cout << "f" << std::flush;
      }

      SmartMet::Spine::WriteLock lock(write_mutex);

      std::size_t pos2 = std::min(pos1 + itsMaxInsertSize, flashCacheData.size());

      soci::transaction tr(itsSession);
      for (std::size_t i = pos1; i < pos2; ++i)
      {
        const auto &item = flashCacheData[i];

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

        std::string timestring = boost::posix_time::to_iso_extended_string(item.stroke_time);
        boost::replace_all(timestring, ",", ".");

        // @todo There is no simple way to optionally set possible NULL values.
        // Find out later how to do it.
        // soci::use(to_tm(item.created)),
        // soci::use(to_tm(item.modified_last)), soci::use(item.modified_by);

        try
        {
          itsSession << sqltemplate, soci::use(to_tm(item.stroke_time)),
              soci::use(item.stroke_time_fraction), soci::use(item.flash_id),
              soci::use(item.multiplicity), soci::use(item.peak_current), soci::use(item.sensors),
              soci::use(item.freedom_degree), soci::use(item.ellipse_angle),
              soci::use(item.ellipse_major), soci::use(item.ellipse_minor),
              soci::use(item.chi_square), soci::use(item.rise_time), soci::use(item.ptz_time),
              soci::use(item.cloud_indicator), soci::use(item.angle_indicator),
              soci::use(item.signal_indicator), soci::use(item.timing_indicator),
              soci::use(item.stroke_status), soci::use(item.data_source);
        }
        catch (std::exception &e)
        {
          std::cerr << "Problem updating flash data: " << e.what() << std::endl;
        }
      }

      tr.commit();
      pos1 += itsMaxInsertSize;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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

    SmartMet::Spine::WriteLock lock(write_mutex);
    updateStations(info.stations);
    updateStationGroups(info);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Update of stations and groups failed!", NULL);
  }
}

void SpatiaLite::updateStations(const SmartMet::Spine::Stations &stations)
{
  try
  {
    // Locking handled by updateStationsAndGroups

    soci::transaction tr(itsSession);

    stringstream insertsql;
    for (const SmartMet::Spine::Station &station : stations)
    {
      if (itsShutdownRequested)
        break;

      insertsql.str("");
      insertsql.precision(10);  // So that latitude and longitude values do not get rounded

      insertsql << "INSERT OR REPLACE INTO stations "
                << "(fmisid, geoid, wmo, lpnn, station_formal_name, "
                   "station_start, station_end, "
                   "the_geom) "
                << "VALUES (" << station.fmisid << "," << station.geoid << "," << station.wmo << ","
                << station.lpnn << ","
                << ":station_formal_name,"
                << ":station_start,"
                << ":station_end,"
                << "GeomFromText('POINT(" << station.longitude_out << " " << station.latitude_out
                << ")', " << srid << "));";

      itsSession << insertsql.str(), soci::use(station.station_formal_name),
          soci::use(to_tm(station.station_start)), soci::use(to_tm(station.station_end));
    }

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::updateStationGroups(const StationInfo &info)
{
  try
  {
    // Locking handled by updateStationsAndGroups

    soci::transaction tr(itsSession);

    // Station groups at the moment.
    size_t stationGroupsCount = 0;
    itsSession << "SELECT COUNT(*) FROM station_groups;", soci::into(stationGroupsCount);

    for (const SmartMet::Spine::Station &station : info.stations)
    {
      // Skipping the empty cases.
      if (station.station_type.empty())
        continue;

      const std::string groupCodeUpper = Fmi::ascii_toupper_copy(station.station_type);

      // Search the group_id for a group_code.
      std::stringstream groupIdSql;
      groupIdSql << "SELECT group_id FROM station_groups WHERE group_code = '" << groupCodeUpper
                 << "' LIMIT 1;";
      boost::optional<int> group_id;
      itsSession << groupIdSql.str(), soci::into(group_id);

      // Group id not found, so we must add a new one.
      if (not group_id)
      {
        stationGroupsCount++;
        group_id = stationGroupsCount;
        std::stringstream insertStationGroupSql;
        insertStationGroupSql << "INSERT OR REPLACE INTO station_groups (group_id, group_code) "
                              << "VALUES (" << stationGroupsCount << ", '" << groupCodeUpper
                              << "');";
        itsSession << insertStationGroupSql.str();
      }

      // Avoid duplicates.
      std::stringstream memberCountSql;
      memberCountSql << "SELECT COUNT(*) FROM group_members WHERE group_id = " << group_id.get()
                     << " AND fmisid = " << station.fmisid << ";";
      int groupCount;
      itsSession << memberCountSql.str(), soci::into(groupCount);

      if (groupCount == 0)
      {
        // Insert a group member. Ignore if insertion fail (perhaps group_id or
        // fmisid is not found
        // from the stations table)
        std::stringstream insertGroupMemberSql;
        insertGroupMemberSql << "INSERT OR IGNORE INTO group_members (group_id, fmisid) "
                             << "VALUES (" << group_id.get() << ", " << station.fmisid << ");";
        itsSession << insertGroupMemberSql.str();
      }
    }

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

// Manipulates vector to string list, e.g. ids[0] = 2094, id[1] = 2095 =>
// idstring = "2094,2095"
std::string getIdString(const std::vector<int> &ids)
{
  try
  {
    if (ids.empty())
      return {};
    stringstream ss;
    std::copy(ids.begin(), ids.end(), std::ostream_iterator<int>(ss, ","));
    std::string ret = ss.str();
    ret.resize(ret.size() - 1);
    return ret;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findStationsByWMO(const Settings &settings,
                                                        const StationInfo &info)
{
  try
  {
    return info.findWmoStations(settings.wmos);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Searching stations by WMO numbers failed", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findStationsByLPNN(const Settings &settings,
                                                         const StationInfo &info)
{
  try
  {
    return info.findLpnnStations(settings.lpnns);
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Searching stations by LPNN numbers failed", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findNearestStations(
    const SmartMet::Spine::LocationPtr &location,
    const map<int, SmartMet::Spine::Station> &stationIndex,
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
    throw SmartMet::Spine::Exception(BCP, "SpatiaLite::findNearestStations!", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findNearestStations(
    double latitude,
    double longitude,
    const map<int, SmartMet::Spine::Station> &stationIndex,
    int maxdistance,
    int numberofstations,
    const std::set<std::string> &stationgroup_codes,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime)
{
  try
  {
    SmartMet::Spine::Stations stations;

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

    SociRowPtr rs;
    {
      rs.reset(new SociRow((itsSession.prepare << distancesql,
                            soci::use(to_tm(starttime)),
                            soci::use(to_tm(endtime)))));
    }

    int fmisid = 0;
    int wmo = -1;
    int geoid = -1;
    int lpnn = -1;
    double longitude_out = std::numeric_limits<double>::max();
    double latitude_out = std::numeric_limits<double>::max();
    string distance = "";
    std::string station_formal_name = "";

    // Reuse the stream in the loop
    std::ostringstream ss;

    for (const auto &row : *rs)
    {
      try
      {
        fmisid = row.get<int>(0);
        // Round distances to 100 meter precision
        ss.str("");
        ss << std::fixed << std::setprecision(1);
        ss << Fmi::stod(row.get<string>(1));
        distance = ss.str();
        wmo = row.get<int>(2);
        geoid = row.get<int>(3);
        // lpnn = row.get<int>(4);
        longitude_out = Fmi::stod(row.get<std::string>(5));
        latitude_out = Fmi::stod(row.get<std::string>(6));
        station_formal_name = row.get<std::string>(7);
      }
      catch (const std::bad_cast &e)
      {
        cout << e.what() << endl;
      }

      auto stationIterator = stationIndex.find(fmisid);
      if (stationIterator != stationIndex.end())
      {
        stations.push_back(stationIterator->second);
      }
      else
      {
        continue;
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedWeatherDataQCData(
    const SmartMet::Spine::Stations &stations,
    const Settings &settings,
    const ParameterMap &parameterMap,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = settings.stationtype;

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    stringstream ss;
    map<int, SmartMet::Spine::Station> tmpStations;
    for (const SmartMet::Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<string, int> timeseriesPositions;
    std::map<std::string, std::string> parameterNameMap;
    map<string, int> specialPositions;

    std::string param;

    unsigned int pos = 0;
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
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
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns(
        new SmartMet::Spine::TimeSeries::TimeSeriesVector);

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
          "AND data.obstime >= :starttime "
          "AND data.obstime <= :endtime "
          "AND data.parameter IN (" +
          param +
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
          "AND data.obstime >= :starttime "
          "AND data.obstime <= :endtime "
          "AND data.parameter IN (" +
          param +
          ") "
          "GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no, loc.location_id, "
          "loc.location_end, loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

    unsigned int resultSize = 10000;

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::optional<std::tm> > obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<std::string> > parametersAll;
    std::vector<boost::optional<double> > data_valuesAll;
    std::vector<boost::optional<double> > sensor_nosAll;

    std::vector<boost::optional<int> > fmisids(resultSize);
    std::vector<boost::optional<std::tm> > obstimes(resultSize);
    std::vector<boost::optional<double> > longitudes(resultSize);
    std::vector<boost::optional<double> > latitudes(resultSize);
    std::vector<boost::optional<double> > elevations(resultSize);
    std::vector<boost::optional<std::string> > parameters(resultSize);
    std::vector<boost::optional<double> > data_values(resultSize);
    std::vector<boost::optional<double> > sensor_nos(resultSize);

    soci::statement st = (itsSession.prepare << query,
                          soci::into(fmisids),
                          soci::into(obstimes),
                          soci::into(longitudes),
                          soci::into(latitudes),
                          soci::into(elevations),
                          soci::into(parameters),
                          soci::into(data_values),
                          soci::into(sensor_nos),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)));

    st.execute();

    while (st.fetch())
    {
      fmisidsAll.insert(fmisidsAll.end(), fmisids.begin(), fmisids.end());
      obstimesAll.insert(obstimesAll.end(), obstimes.begin(), obstimes.end());
      longitudesAll.insert(longitudesAll.end(), longitudes.begin(), longitudes.end());
      latitudesAll.insert(latitudesAll.end(), latitudes.begin(), latitudes.end());
      elevationsAll.insert(elevationsAll.end(), elevations.begin(), elevations.end());
      parametersAll.insert(parametersAll.end(), parameters.begin(), parameters.end());
      data_valuesAll.insert(data_valuesAll.end(), data_values.begin(), data_values.end());
      sensor_nosAll.insert(sensor_nosAll.end(), sensor_nos.begin(), sensor_nos.end());

      // Should resize back to original size guarantee space for next iteration
      // (SOCI manual)
      fmisids.resize(resultSize);
      obstimes.resize(resultSize);
      longitudes.resize(resultSize);
      latitudes.resize(resultSize);
      elevations.resize(resultSize);
      parameters.resize(resultSize);
      data_values.resize(resultSize);
      sensor_nos.resize(resultSize);
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > > data;

    for (const boost::optional<std::tm> &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = boost::posix_time::ptime_from_tm(*time);
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
      SmartMet::Spine::TimeSeriesGeneratorOptions opt;
      opt.startTime = settings.starttime;
      opt.endTime = settings.endtime;
      opt.timeStep = settings.timestep;
      opt.startTimeUTC = false;
      opt.endTimeUTC = false;
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          opt, timezones.time_zone_from_string(settings.timezone));

      for (const SmartMet::Spine::Station &s : stations)
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
      for (const SmartMet::Spine::Station &s : stations)
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedData(
    const SmartMet::Spine::Stations &stations,
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

    stringstream ss;
    map<int, SmartMet::Spine::Station> tmpStations;
    for (const SmartMet::Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<int, int> timeseriesPositions;
    map<std::string, int> timeseriesPositionsString;
    std::map<std::string, std::string> parameterNameMap;
    vector<int> paramVector;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
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
        "AND data.data_time >= :starttime "
        "AND data.data_time <= :endtime "
        "AND data.measurand_id IN (" +
        param +
        ") "
        "AND data.measurand_no = 1 "
        "GROUP BY data.fmisid, data.data_time, data.measurand_id, "
        "loc.location_id, "
        "loc.location_end, "
        "loc.latitude, loc.longitude, loc.elevation "
        "ORDER BY fmisid ASC, obstime ASC;";

    unsigned int resultSize = 10000;

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::optional<std::tm> > obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<int> > measurand_idsAll;
    std::vector<boost::optional<double> > data_valuesAll;

    std::vector<boost::optional<int> > fmisids(resultSize);
    std::vector<boost::optional<std::tm> > obstimes(resultSize);
    std::vector<boost::optional<double> > longitudes(resultSize);
    std::vector<boost::optional<double> > latitudes(resultSize);
    std::vector<boost::optional<double> > elevations(resultSize);
    std::vector<boost::optional<int> > measurand_ids(resultSize);
    std::vector<boost::optional<double> > data_values(resultSize);

    soci::statement st = (itsSession.prepare << query,
                          soci::into(fmisids),
                          soci::into(obstimes),
                          soci::into(longitudes),
                          soci::into(latitudes),
                          soci::into(elevations),
                          soci::into(measurand_ids),
                          soci::into(data_values),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)));

    st.execute();

    while (st.fetch())
    {
      fmisidsAll.insert(fmisidsAll.end(), fmisids.begin(), fmisids.end());
      obstimesAll.insert(obstimesAll.end(), obstimes.begin(), obstimes.end());
      longitudesAll.insert(longitudesAll.end(), longitudes.begin(), longitudes.end());
      latitudesAll.insert(latitudesAll.end(), latitudes.begin(), latitudes.end());
      elevationsAll.insert(elevationsAll.end(), elevations.begin(), elevations.end());
      measurand_idsAll.insert(measurand_idsAll.end(), measurand_ids.begin(), measurand_ids.end());
      data_valuesAll.insert(data_valuesAll.end(), data_values.begin(), data_values.end());

      // Should resize back to original size guarantee space for next iteration
      // (SOCI manual)
      fmisids.resize(resultSize);
      obstimes.resize(resultSize);
      longitudes.resize(resultSize);
      latitudes.resize(resultSize);
      elevations.resize(resultSize);
      measurand_ids.resize(resultSize);
      data_values.resize(resultSize);
    }

    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
            new SmartMet::Spine::TimeSeries::TimeSeriesVector);

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

    for (const boost::optional<std::tm> &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = boost::posix_time::ptime_from_tm(*time);
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

    SmartMet::Spine::TimeSeriesGeneratorOptions opt;
    opt.startTime = settings.starttime;
    opt.endTime = settings.endtime;
    opt.timeStep = settings.timestep;
    opt.startTimeUTC = false;
    opt.endTimeUTC = false;

    typedef std::pair<boost::local_time::local_date_time, map<std::string, ts::Value> >
        dataItemWithStringParameterId;

    if (settings.timestep > 1 || settings.latest)
    {
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          opt, timezones.time_zone_from_string(settings.timezone));
      {
        for (const SmartMet::Spine::Station &s : stations)
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
      for (const SmartMet::Spine::Station &s : stations)
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::addEmptyValuesToTimeSeries(
    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const boost::local_time::local_date_time &obstime,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const std::string &stationtype,
    const SmartMet::Spine::Station &station)
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
          special.first.find("feelslike") != std::string::npos)
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::addParameterToTimeSeries(
    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time, std::map<std::string, ts::Value> >
        &dataItem,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const ParameterMap &parameterMap,
    const std::string &stationtype,
    const SmartMet::Spine::Station &station)
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
      else
      {
        addSpecialParameterToTimeSeries(
            special.first, timeSeriesColumns, station, pos, stationtype, obstime);
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedFlashData(
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
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        string name = p.name();
        boost::to_lower(name);
        if (!parameterMap.at(name).at(stationtype).empty())
        {
          timeseriesPositions[parameterMap.at(name).at(stationtype)] = pos;
          param += parameterMap.at(name).at(stationtype) + ",";
        }
      }
      else
      {
        string name = p.name();
        boost::to_lower(name);
        specialPositions[name] = pos;
      }
      pos++;
    }

    param = trimCommasFromEnd(param);

    string starttimeString = boost::posix_time::to_iso_extended_string(settings.starttime);
    boost::replace_all(starttimeString, ",", ".");
    string endtimeString = boost::posix_time::to_iso_extended_string(settings.endtime);
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
        "WHERE flash.stroke_time >= DATETIME('" +
        starttimeString +
        "') "
        "AND flash.stroke_time <= DATETIME('" +
        endtimeString + "') ";

    if (!settings.taggedLocations.empty())
    {
      for (auto tloc : settings.taggedLocations)
      {
        if (tloc.loc->type == SmartMet::Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          // tloc.loc->radius in kilometers and PtDistWithin uses meters
          std::string radius = Fmi::to_string(tloc.loc->radius * 1000);
          query += " AND PtDistWithin((SELECT GeomFromText('POINT(" + lon + " " + lat +
                   ")', 4326)), flash.stroke_location, " + radius + ") = 1 ";
        }
        if (tloc.loc->type == SmartMet::Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          SmartMet::Spine::BoundingBox bbox(bboxString);

          query += "AND MbrWithin(flash.stroke_location, BuildMbr(" + Fmi::to_string(bbox.xMin) +
                   ", " + Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMax) + ", " +
                   Fmi::to_string(bbox.yMax) + ")) ";
        }
      }
    }

    query += "ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction ASC;";

    soci::rowset<soci::row> rs = (itsSession.prepare << query);

    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
            new SmartMet::Spine::TimeSeries::TimeSeriesVector);
    // Set timeseries objects for each requested parameter
    for (unsigned int i = 0; i < settings.parameters.size(); i++)
    {
      timeSeriesColumns->push_back(ts::TimeSeries());
    }

    std::string stroke_time;
    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();

    for (soci::rowset<soci::row>::const_iterator it = rs.begin(); it != rs.end(); ++it)
    {
      soci::row const &row = *it;
      map<std::string, ts::Value> result;

      // These will be always in this order
      stroke_time = row.get<string>(0);
      // int stroke_time_fraction = row.get<int>(1);
      // int flash_id = row.get<int>(2);
      longitude = Fmi::stod(row.get<string>(3));
      latitude = Fmi::stod(row.get<string>(4));

      // Rest of the parameters in requested order
      for (std::size_t i = 5; i != row.size(); ++i)
      {
        const soci::column_properties &props = row.get_properties(i);
        ts::Value temp;
        auto data_type = props.get_data_type();
        if (data_type == soci::dt_string)
        {
          temp = row.get<std::string>(i);
        }
        else if (data_type == soci::dt_double)
        {
          temp = row.get<double>(i);
        }
        else if (data_type == soci::dt_integer)
        {
          temp = row.get<int>(i);
        }
        result[props.get_name()] = temp;
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::addSpecialParameterToTimeSeries(
    const std::string &paramname,
    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const SmartMet::Spine::Station &station,
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

      SmartMet::Spine::Exception exception(BCP, "Operation processing failed!");
      // exception.setExceptionCode(Obs_EngineException::OPERATION_PROCESSING_FAILED);
      exception.addDetail(msg);
      throw exception;
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findAllStationsFromGroups(
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
    throw SmartMet::Spine::Exception(BCP, "Failed to find all stations in the given groups", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findStationsInsideArea(const Settings &settings,
                                                             const std::string &areaWkt,
                                                             const StationInfo &info)
{
  try
  {
    SmartMet::Spine::Stations stations;

    stringstream sqlstmt;
    sqlstmt.precision(10);  // To avoid rounding

    sqlstmt << "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sqlstmt << "group_members gm "
              << "JOIN station_groups sg ON gm.group_id = sg.group_id "
              << "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlstmt << "stations s ";
    }

    sqlstmt << "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sqlstmt << "( sg.group_code='" << *it << "' ";
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sqlstmt << "OR sg.group_code='" << *it << "' ";
      sqlstmt << ") AND ";
    }

    sqlstmt << "Contains(GeomFromText('" << areaWkt << "'), s.the_geom) AND ("
            << ":starttime BETWEEN s.station_start AND s.station_end OR "
            << ":endtime BETWEEN s.station_start AND s.station_end)";

    SociRowPtr rs;
    rs.reset(new SociRow((itsSession.prepare << sqlstmt.str(),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)))));

    for (const auto &row : *rs)
    {
      try
      {
        int geoid = row.get<int>(0);
        int station_id = row.get<int>(1);
        SmartMet::Spine::Station station = info.getStation(station_id, settings.stationgroup_codes);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::Stations SpatiaLite::findStationsInsideBox(const Settings &settings,
                                                            const StationInfo &info)
{
  try
  {
    SmartMet::Spine::Stations stations;

    stringstream sqlstmt;
    sqlstmt.precision(10);  // To avoid rounding

    sqlstmt << "SELECT distinct s.geoid, s.fmisid FROM ";

    if (not settings.stationgroup_codes.empty())
    {
      sqlstmt << "group_members gm "
              << "JOIN station_groups sg ON gm.group_id = sg.group_id "
              << "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlstmt << "stations s ";
    }

    sqlstmt << "WHERE ";

    if (not settings.stationgroup_codes.empty())
    {
      auto it = settings.stationgroup_codes.begin();
      sqlstmt << "( sg.group_code='" << *it << "' ";
      for (it++; it != settings.stationgroup_codes.end(); it++)
        sqlstmt << "OR sg.group_code='" << *it << "' ";
      sqlstmt << ") AND ";
    }

    sqlstmt << "ST_EnvIntersects(s.the_geom," << Fmi::to_string(settings.boundingBox.at("minx"))
            << ',' << Fmi::to_string(settings.boundingBox.at("miny")) << ','
            << Fmi::to_string(settings.boundingBox.at("maxx")) << ','
            << Fmi::to_string(settings.boundingBox.at("maxy")) << ") AND ("
            << ":starttime BETWEEN s.station_start AND s.station_end OR "
            << ":endtime BETWEEN s.station_start AND s.station_end)";

    SociRowPtr rs;
    rs.reset(new SociRow((itsSession.prepare << sqlstmt.str(),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)))));

    for (const auto &row : *rs)
    {
      try
      {
        int geoid = row.get<int>(0);
        int station_id = row.get<int>(1);
        SmartMet::Spine::Station station = info.getStation(station_id, settings.stationgroup_codes);
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLite::fillMissing(SmartMet::Spine::Station &s,
                             const std::set<std::string> &stationgroup_codes)
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

    stringstream sqlStr;
    sqlStr.precision(10);  // To avoid rounding
    sqlStr << "SELECT s.fmisid, s.wmo, s.geoid, s.lpnn"
           << ", X(s.the_geom) AS lon"
           << ", Y(s.the_geom) AS lat"
           << ", s.station_formal_name"
           << " FROM ";

    if (not stationgroup_codes.empty())
    {
      sqlStr << "group_members gm "
             << "JOIN station_groups sg ON gm.group_id = sg.group_id "
             << "JOIN stations s ON gm.fmisid = s.fmisid ";
    }
    else
    {
      sqlStr << "stations s ";
    }

    sqlStr << " WHERE";

    if (not stationgroup_codes.empty())
    {
      auto it = stationgroup_codes.begin();
      sqlStr << "( sg.group_code='" << *it << "' ";
      for (it++; it != stationgroup_codes.end(); it++)
        sqlStr << "OR sg.group_code='" << *it << "' ";
      sqlStr << ") AND ";
    }

    // Use the first id that is not missing.
    if (not missingStationId)
      sqlStr << " s.fmisid = " << s.station_id;
    else if (not missingFmisId)
      sqlStr << " s.fmisid = " << s.fmisid;
    else if (not missingWmoId)
      sqlStr << " s.wmo = " << s.wmo;
    else if (not missingGeoId)
      sqlStr << " s.geoid = " << s.geoid;
    else if (not missingLpnnId)
      sqlStr << " s.lpnn = " << s.lpnn;
    else
      return false;

    // There might be multiple locations for a station.
    sqlStr << " AND DATETIME('now') BETWEEN s.station_start AND s.station_end";

    boost::optional<int> fmisid;
    boost::optional<int> wmo;
    boost::optional<int> geoid;
    boost::optional<int> lpnn;
    boost::optional<double> longitude_out;
    boost::optional<double> latitude_out;
    boost::optional<std::string> station_formal_name;

    // We need only the first one (ID values are unique).
    sqlStr << " LIMIT 1";

    // Executing the search

    itsSession << sqlStr.str(), soci::into(fmisid), soci::into(wmo), soci::into(geoid),
        soci::into(lpnn), soci::into(longitude_out), soci::into(latitude_out),
        soci::into(station_formal_name);

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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool SpatiaLite::getStationById(SmartMet::Spine::Station &station,
                                int station_id,
                                const std::set<std::string> &stationgroup_codes)
{
  try
  {
    SmartMet::Spine::Station s;
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

FlashCounts SpatiaLite::getFlashCount(const boost::posix_time::ptime &starttime,
                                      const boost::posix_time::ptime &endtime,
                                      const SmartMet::Spine::TaggedLocationList &locations)
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
        "WHERE flash.stroke_time BETWEEN "
        ":starttime AND :endtime ";

    if (!locations.empty())
    {
      for (auto tloc : locations)
      {
        if (tloc.loc->type == SmartMet::Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          // tloc.loc->radius in kilometers and PtDistWithin uses meters
          std::string radius = Fmi::to_string(tloc.loc->radius * 1000);
          sqltemplate += " AND PtDistWithin((SELECT GeomFromText('POINT(" + lon + " " + lat +
                         ")', 4326)), flash.stroke_location, " + radius + ") = 1 ";
        }
        if (tloc.loc->type == SmartMet::Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          SmartMet::Spine::BoundingBox bbox(bboxString);

          sqltemplate += "AND MbrWithin(flash.stroke_location, BuildMbr(" +
                         Fmi::to_string(bbox.xMin) + ", " + Fmi::to_string(bbox.yMin) + ", " +
                         Fmi::to_string(bbox.xMax) + ", " + Fmi::to_string(bbox.yMax) + ")) ";
        }
      }
    }

    sqltemplate += ";";

    itsSession << sqltemplate, soci::use(to_tm(starttime)), soci::use(to_tm(endtime)),
        soci::into(flashcounts.flashcount), soci::into(flashcounts.strokecount),
        soci::into(flashcounts.iccount);

    return flashcounts;
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedWeatherDataQCData(
    const SmartMet::Spine::Stations &stations,
    const Settings &settings,
    const ParameterMap &parameterMap,
    const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = settings.stationtype;

    boost::shared_ptr<Fmi::TimeFormatter> timeFormatter;
    timeFormatter.reset(Fmi::TimeFormatter::create(settings.timeformat));

    stringstream ss;
    map<int, SmartMet::Spine::Station> tmpStations;
    for (const SmartMet::Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<string, int> timeseriesPositions;
    std::map<std::string, std::string> parameterNameMap;
    map<string, int> specialPositions;

    std::string param;

    unsigned int pos = 0;
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
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
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns(
        new SmartMet::Spine::TimeSeries::TimeSeriesVector);

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
          "AND data.obstime >= :starttime "
          "AND data.obstime <= :endtime "
          "AND data.parameter IN (" +
          param +
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
          "AND data.obstime >= :starttime "
          "AND data.obstime <= :endtime "
          "AND data.parameter IN (" +
          param +
          ") "
          "GROUP BY data.fmisid, data.obstime, data.parameter, "
          "data.sensor_no, loc.location_id, "
          "loc.location_end, loc.latitude, loc.longitude, loc.elevation "
          "ORDER BY fmisid ASC, obstime ASC;";
    }

    unsigned int resultSize = 10000;

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::optional<std::tm> > obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<std::string> > parametersAll;
    std::vector<boost::optional<double> > data_valuesAll;
    std::vector<boost::optional<double> > sensor_nosAll;

    std::vector<boost::optional<int> > fmisids(resultSize);
    std::vector<boost::optional<std::tm> > obstimes(resultSize);
    std::vector<boost::optional<double> > longitudes(resultSize);
    std::vector<boost::optional<double> > latitudes(resultSize);
    std::vector<boost::optional<double> > elevations(resultSize);
    std::vector<boost::optional<std::string> > parameters(resultSize);
    std::vector<boost::optional<double> > data_values(resultSize);
    std::vector<boost::optional<double> > sensor_nos(resultSize);

    soci::statement st = (itsSession.prepare << query,
                          soci::into(fmisids),
                          soci::into(obstimes),
                          soci::into(longitudes),
                          soci::into(latitudes),
                          soci::into(elevations),
                          soci::into(parameters),
                          soci::into(data_values),
                          soci::into(sensor_nos),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)));

    st.execute();

    while (st.fetch())
    {
      fmisidsAll.insert(fmisidsAll.end(), fmisids.begin(), fmisids.end());
      obstimesAll.insert(obstimesAll.end(), obstimes.begin(), obstimes.end());
      longitudesAll.insert(longitudesAll.end(), longitudes.begin(), longitudes.end());
      latitudesAll.insert(latitudesAll.end(), latitudes.begin(), latitudes.end());
      elevationsAll.insert(elevationsAll.end(), elevations.begin(), elevations.end());
      parametersAll.insert(parametersAll.end(), parameters.begin(), parameters.end());
      data_valuesAll.insert(data_valuesAll.end(), data_values.begin(), data_values.end());
      sensor_nosAll.insert(sensor_nosAll.end(), sensor_nos.begin(), sensor_nos.end());

      // Should resize back to original size guarantee space for next iteration
      // (SOCI manual)
      fmisids.resize(resultSize);
      obstimes.resize(resultSize);
      longitudes.resize(resultSize);
      latitudes.resize(resultSize);
      elevations.resize(resultSize);
      parameters.resize(resultSize);
      data_values.resize(resultSize);
      sensor_nos.resize(resultSize);
    }

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    map<int, map<boost::local_time::local_date_time, map<std::string, ts::Value> > > data;

    for (const boost::optional<std::tm> &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];

      boost::posix_time::ptime utctime = boost::posix_time::ptime_from_tm(*time);
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
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

      for (const SmartMet::Spine::Station &s : stations)
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
      for (const SmartMet::Spine::Station &s : stations)
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr SpatiaLite::getCachedData(
    SmartMet::Spine::Stations &stations,
    Settings &settings,
    ParameterMap &parameterMap,
    const SmartMet::Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
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

    stringstream ss;
    map<int, SmartMet::Spine::Station> tmpStations;
    for (const SmartMet::Spine::Station &s : stations)
    {
      tmpStations.insert(std::make_pair(s.station_id, s));
      ss << s.station_id << ",";
    }
    string qstations = ss.str();
    qstations = qstations.substr(0, qstations.length() - 1);

    // This maps measurand_id and the parameter position in TimeSeriesVector
    map<int, int> timeseriesPositions;
    map<std::string, int> timeseriesPositionsString;
    std::map<std::string, std::string> parameterNameMap;
    vector<int> paramVector;
    map<string, int> specialPositions;

    string param;
    unsigned int pos = 0;
    for (const SmartMet::Spine::Parameter &p : settings.parameters)
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
        "AND data.data_time >= :starttime "
        "AND data.data_time <= :endtime "
        "AND data.measurand_id IN (" +
        param +
        ") "
        "AND data.measurand_no = 1 "
        "GROUP BY data.fmisid, data.data_time, data.measurand_id, "
        "loc.location_id, "
        "loc.location_end, "
        "loc.latitude, loc.longitude, loc.elevation "
        "ORDER BY fmisid ASC, obstime ASC;";

    unsigned int resultSize = 10000;

    std::vector<boost::optional<int> > fmisidsAll;
    std::vector<boost::optional<std::tm> > obstimesAll;
    std::vector<boost::optional<double> > longitudesAll;
    std::vector<boost::optional<double> > latitudesAll;
    std::vector<boost::optional<double> > elevationsAll;
    std::vector<boost::optional<int> > measurand_idsAll;
    std::vector<boost::optional<double> > data_valuesAll;

    std::vector<boost::optional<int> > fmisids(resultSize);
    std::vector<boost::optional<std::tm> > obstimes(resultSize);
    std::vector<boost::optional<double> > longitudes(resultSize);
    std::vector<boost::optional<double> > latitudes(resultSize);
    std::vector<boost::optional<double> > elevations(resultSize);
    std::vector<boost::optional<int> > measurand_ids(resultSize);
    std::vector<boost::optional<double> > data_values(resultSize);

    soci::statement st = (itsSession.prepare << query,
                          soci::into(fmisids),
                          soci::into(obstimes),
                          soci::into(longitudes),
                          soci::into(latitudes),
                          soci::into(elevations),
                          soci::into(measurand_ids),
                          soci::into(data_values),
                          soci::use(to_tm(settings.starttime)),
                          soci::use(to_tm(settings.endtime)));

    st.execute();

    while (st.fetch())
    {
      fmisidsAll.insert(fmisidsAll.end(), fmisids.begin(), fmisids.end());
      obstimesAll.insert(obstimesAll.end(), obstimes.begin(), obstimes.end());
      longitudesAll.insert(longitudesAll.end(), longitudes.begin(), longitudes.end());
      latitudesAll.insert(latitudesAll.end(), latitudes.begin(), latitudes.end());
      elevationsAll.insert(elevationsAll.end(), elevations.begin(), elevations.end());
      measurand_idsAll.insert(measurand_idsAll.end(), measurand_ids.begin(), measurand_ids.end());
      data_valuesAll.insert(data_valuesAll.end(), data_values.begin(), data_values.end());

      // Should resize back to original size guarantee space for next iteration
      // (SOCI manual)
      fmisids.resize(resultSize);
      obstimes.resize(resultSize);
      longitudes.resize(resultSize);
      latitudes.resize(resultSize);
      elevations.resize(resultSize);
      measurand_ids.resize(resultSize);
      data_values.resize(resultSize);
    }

    SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr(
            new SmartMet::Spine::TimeSeries::TimeSeriesVector);

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

    for (const boost::optional<std::tm> &time : obstimesAll)
    {
      int fmisid = *fmisidsAll[i];
      boost::posix_time::ptime utctime = boost::posix_time::ptime_from_tm(*time);
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
      for (const SmartMet::Spine::Station &s : stations)
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
      auto tlist = SmartMet::Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      {
        for (const SmartMet::Spine::Station &s : stations)

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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

void SpatiaLite::createObservablePropertyTable()
{
  try
  {
    // No locking needed during initialization phase

    soci::transaction tr(itsSession);
    itsSession << "CREATE TABLE IF NOT EXISTS observable_property ("
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
                  "gmlId TEXT);";

    tr.commit();
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
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
        "gmlId FROM observable_property WHERE language = :language";

    SociRowPtr rs;
    rs.reset(new SociRow((itsSession.prepare << sqlStmt, soci::use(language))));

    for (const auto &row : *rs)
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
    throw SmartMet::Spine::Exception(BCP, "Operation failed!", NULL);
  }

  return data;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
