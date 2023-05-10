#include "PostgreSQLObsDB.h"
#include "AsDouble.h"
#include "ExternalAndMobileDBInfo.h"
#include "Keywords.h"
#include "PostgreSQLCacheDB.h"
#include "Utils.h"
#include <boost/functional/hash.hpp>
#include <fmt/format.h>
#include <gis/OGR.h>
#include <macgyver/AsyncTask.h>
#include <macgyver/Exception.h>
#include <spine/Convenience.h>

// #define MYDEBUG 1

using namespace std;
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// This is global so that different threads will not repeat the same task.
// No locking is used, we assume different threads are so out of sync so
// that an atomic will do here.

std::atomic<bool> bigFlashRequestReported{false};

using namespace Utils;

PostgreSQLObsDB::PostgreSQLObsDB(
    const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
    const StationtypeConfig &stc,
    const ParameterMapPtr &pm)
    : CommonPostgreSQLFunctions(connectionOptions, stc, pm)
{
}

void PostgreSQLObsDB::get(const std::string & /* sqlStatement */,
                          std::shared_ptr<QueryResultBase> /* qrb */,
                          const Fmi::TimeZones & /* timezones */)
{
  try
  {
    /*
      // makeQuery -> get(): Not implemented in PostgreSQLObsDB
    */
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readMobileCacheDataFromPostgreSQL(const std::string &producer,
                                                        vector<MobileExternalDataItem> &cacheData,
                                                        boost::posix_time::ptime lastTime,
                                                        boost::posix_time::ptime lastCreatedTime,
                                                        const Fmi::TimeZones & /* timezones */)
{
  try
  {
    std::string sqlStmt =
        ExternalAndMobileDBInfo::sqlSelectForCache(producer, lastTime, lastCreatedTime);

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    // Execute SQL statement
    Fmi::Database::PostgreSQLConnection &conn = getConnection();
    pqxx::result result_set = conn.executeNonTransaction(sqlStmt);

    ResultSetRows rsrs =
        PostgreSQLCacheDB::getResultSetForMobileExternalData(result_set, conn.dataTypes());

    TS::Value none = TS::None();
    for (auto rsr : rsrs)
    {
      Fmi::AsyncTask::interruption_point();

      MobileExternalDataItem dataItem;
      dataItem.prod_id = boost::get<int>(rsr["prod_id"]);
      if (rsr["station_id"] != none)
        dataItem.station_id = boost::get<int>(rsr["station_id"]);
      if (rsr["dataset_id"] != none)
        dataItem.dataset_id = boost::get<std::string>(rsr["dataset_id"]);
      if (rsr["data_level"] != none)
        dataItem.data_level = boost::get<int>(rsr["data_level"]);
      dataItem.mid = boost::get<int>(rsr["mid"]);
      if (rsr["sensor_no"] != none)
        dataItem.sensor_no = boost::get<int>(rsr["sensor_no"]);
      dataItem.data_time =
          (boost::get<boost::local_time::local_date_time>(rsr["data_time"]).utc_time());
      dataItem.data_value = boost::get<double>(rsr["data_value"]);
      if (rsr["data_value_txt"] != none)
        dataItem.data_value_txt = boost::get<std::string>(rsr["data_value_txt"]);
      if (rsr["data_quality"] != none)
        dataItem.data_quality = boost::get<int>(rsr["data_quality"]);
      if (rsr["ctrl_status"] != none)
        dataItem.ctrl_status = boost::get<int>(rsr["ctrl_status"]);
      dataItem.created =
          (boost::get<boost::local_time::local_date_time>(rsr["created"]).utc_time());
      if (producer == FMI_IOT_PRODUCER)
      {
        if (rsr["station_code"] != none)
          dataItem.station_code = boost::get<std::string>(rsr["station_code"]);
      }
      else
      {
        if (rsr["longitude"] != none)
          dataItem.longitude = boost::get<double>(rsr["longitude"]);
        if (rsr["latitude"] != none)
          dataItem.latitude = boost::get<double>(rsr["latitude"]);
        if (rsr["altitude"] != none)
          dataItem.altitude = boost::get<double>(rsr["altitude"]);
      }
      cacheData.push_back(dataItem);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading cache data from PostgreSQL database failed!");
  }
}

void PostgreSQLObsDB::readMovingStationsCacheDataFromPostgreSQL(
    std::vector<MovingLocationItem> &cacheData,
    const boost::posix_time::ptime &startTime,
    const boost::posix_time::ptime & /* lastModifiedTime */,
    const Fmi::TimeZones & /* timezones */)
{
  try
  {
    std::string sqlStmt =
        "SELECT station_id, EXTRACT(EPOCH FROM date_trunc('seconds', sdate)), EXTRACT(EPOCH FROM "
        "date_trunc('seconds', edate)), lon, lat, elev "
        "FROM moving_locations_v1 data WHERE edate >= '" +
        Fmi::to_iso_extended_string(startTime) + "' ORDER BY station_id ASC, sdate ASC";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();

      MovingLocationItem item;
      item.station_id = as_int(row[0]);
      item.sdate = boost::posix_time::from_time_t(row[1].as<time_t>());
      item.edate = boost::posix_time::from_time_t(row[2].as<time_t>());
      item.lon = as_double(row[3]);
      item.lat = as_double(row[4]);
      item.elev = as_double(row[5]);
      cacheData.emplace_back(item);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                  const std::string &sqlStmt,
                                                  const Fmi::TimeZones & /* timezones */)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();

      DataItem item;
      item.fmisid = as_int(row[0]);
      item.sensor_no = as_int(row[1]);
      item.measurand_id = as_int(row[2]);
      item.producer_id = as_int(row[3]);
      item.measurand_no = as_int(row[4]);
      item.data_time = boost::posix_time::from_time_t(row[5].as<time_t>());
      if (!row[6].is_null())
        item.data_value = as_double(row[6]);
      if (!row[7].is_null())
        item.data_quality = as_int(row[7]);
      if (!row[8].is_null())
        item.data_source = as_int(row[8]);
      item.modified_last = boost::posix_time::from_time_t(row[9].as<time_t>());

      cacheData.emplace_back(item);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                  const boost::posix_time::time_period &dataPeriod,
                                                  const std::string &fmisid,
                                                  const std::string &measurandId,
                                                  const Fmi::TimeZones &timezones)
{
  try
  {
    std::string sqlStmt =
        "SELECT station_id, sensor_no, measurand_id, producer_id, measurand_no, EXTRACT(EPOCH FROM "
        "date_trunc('seconds', data_time)) as data_time, "
        "data_value, data_quality, data_source, EXTRACT(EPOCH FROM date_trunc('seconds', "
        "modified_last)) as modified_last "
        "FROM observation_data_r1 data WHERE data_time >= '" +
        Fmi::to_iso_extended_string(dataPeriod.begin()) + "' AND data_time <= '" +
        Fmi::to_iso_extended_string(dataPeriod.last()) + ",";
    if (!measurandId.empty())
      sqlStmt += (" AND measurand_id IN (" + measurandId + ")");
    if (!fmisid.empty())
      sqlStmt += (" AND station_id IN (" + fmisid + ")");
    sqlStmt += " AND data_value IS NOT NULL ORDER BY station_id ASC, data_time ASC";

    return readCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                  const boost::posix_time::ptime & /* startTime */,
                                                  const boost::posix_time::ptime &lastModifiedTime,
                                                  const Fmi::TimeZones &timezones)
{
  try
  {
    const boost::posix_time::ptime now = second_clock::universal_time();
    const auto diff = now - lastModifiedTime;
    const bool big_request = (diff >= boost::posix_time::hours(24));

    std::string sqlStmt =
        "SELECT station_id, sensor_no, measurand_id, producer_id, measurand_no, EXTRACT(EPOCH FROM "
        "date_trunc('seconds', data_time)) as data_time, "
        "data_value, data_quality, data_source, EXTRACT(EPOCH FROM date_trunc('seconds', "
        "modified_last)) as modified_last "
        "FROM observation_data_r1 data WHERE "
        "data.modified_last >= '" +
        Fmi::to_iso_extended_string(lastModifiedTime) + "' ORDER BY station_id ASC, data_time ASC";

    if (big_request)
    {
      std::cout << (Spine::log_time_str() +
                    " [PostgreSQLObsDB] Performing a large OBS cache update starting from " +
                    Fmi::to_simple_string(lastModifiedTime))
                << std::endl;
    }

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    return readCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readFlashCacheDataFromPostgreSQL(std::vector<FlashDataItem> &cacheData,
                                                       const std::string &sqlStmt,
                                                       const Fmi::TimeZones & /* timezones */)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();

      FlashDataItem item;

      int epoch = as_int(row[0]);
      item.stroke_time = epoch2ptime(epoch);
      item.stroke_time_fraction = as_int(row[1]);
      item.flash_id = as_int(row[2]);
      item.multiplicity = as_int(row[3]);
      item.peak_current = as_int(row[4]);
      item.sensors = as_int(row[5]);
      item.freedom_degree = as_int(row[6]);
      item.ellipse_angle = as_double(row[7]);
      item.ellipse_major = as_double(row[8]);
      item.ellipse_minor = as_double(row[9]);
      item.chi_square = as_double(row[10]);
      item.rise_time = as_double(row[11]);
      item.ptz_time = as_double(row[12]);
      item.cloud_indicator = as_int(row[13]);
      item.angle_indicator = as_int(row[14]);
      item.signal_indicator = as_int(row[15]);
      item.timing_indicator = as_int(row[16]);
      item.stroke_status = as_int(row[17]);
      if (!row[18].is_null())
        item.data_source = as_int(row[18]);
      int created = as_int(row[19]);
      item.created = epoch2ptime(created);
      int modified_last = as_int(row[20]);
      item.modified_last = epoch2ptime(modified_last);
      if (!row[21].is_null())
        item.modified_by = as_int(row[21]);
      item.longitude = as_double(row[22]);
      item.latitude = as_double(row[23]);
      cacheData.push_back(item);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readFlashCacheDataFromPostgreSQL(
    std::vector<FlashDataItem> &cacheData,
    const boost::posix_time::time_period &dataPeriod,
    const Fmi::TimeZones &timezones)
{
  try
  {
    // clang-format off
    std::string sqlStmt = fmt::format(R"SQL(
SELECT Extract(epoch FROM Date_trunc('seconds', stroke_time))   AS stroke_time,
       nseconds                                                 AS nanoseconds,
       flash_id,
       multiplicity,
       peak_current,
       sensors,
       freedom_degree,
       ellipse_angle,
       ellipse_major,
       ellipse_minor,
       chi_square,
       rise_time,
       ptz_time,
       cloud_indicator,
       angle_indicator,
       signal_indicator,
       timing_indicator,
       stroke_status,
       data_source,
       Extract(epoch FROM Date_trunc('seconds', created))       AS created,
       Extract(epoch FROM Date_trunc('seconds', modified_last)) AS modified_last,
       modified_by,
       St_x(stroke_location)                                    longitude,
       St_y(stroke_location)                                    AS latitude
FROM   flashdata flash
WHERE  stroke_time BETWEEN '{}' AND '{}'
ORDER  BY stroke_time,
          flash_id;)SQL",
    Fmi::to_iso_extended_string(dataPeriod.begin()),
    Fmi::to_iso_extended_string(dataPeriod.last()));
    // clang-format on

    return readFlashCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readFlashCacheDataFromPostgreSQL(
    std::vector<FlashDataItem> &cacheData,
    const boost::posix_time::ptime & /* startTime */,
    const boost::posix_time::ptime & /* lastStrokeTime */,
    const boost::posix_time::ptime &lastModifiedTime,
    const Fmi::TimeZones &timezones)
{
  try
  {
    const boost::posix_time::ptime now = second_clock::universal_time();
    const auto diff = now - lastModifiedTime;
    const bool big_request = (diff >= boost::posix_time::hours(24));

    if (big_request)
    {
      if (!bigFlashRequestReported)
      {
        bigFlashRequestReported = true;
        std::cout << (Spine::log_time_str() +
                      " [PostgreSQLObsDB] Performing a large FLASH cache update starting from " +
                      Fmi::to_simple_string(lastModifiedTime))
                  << std::endl;
      }
    }

    std::string sqlStmt =
        "SELECT EXTRACT(EPOCH FROM date_trunc('seconds', stroke_time)) as stroke_time, nseconds as "
        "nanoseconds, flash_id, "
        "multiplicity, peak_current, "
        "sensors, freedom_degree, ellipse_angle, ellipse_major, "
        "ellipse_minor, chi_square, rise_time, ptz_time, cloud_indicator, "
        "angle_indicator, signal_indicator, timing_indicator, stroke_status, "
        "data_source,  EXTRACT(EPOCH FROM date_trunc('seconds', created)) as created, "
        "EXTRACT(EPOCH "
        "FROM date_trunc('seconds', modified_last)) as modified_last, modified_by, "
        "ST_X(stroke_location) longitude, "
        "ST_Y(stroke_location) AS latitude "
        "FROM flashdata flash "
        "WHERE modified_last >= '" +
        Fmi::to_iso_extended_string(lastModifiedTime) + "' ORDER BY stroke_time, flash_id";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    readFlashCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readWeatherDataQCCacheDataFromPostgreSQL(
    std::vector<WeatherDataQCItem> &cacheData,
    const std::string &sqlStmt,
    const Fmi::TimeZones & /* timezones */)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    unsigned int count = 0;
    for (auto row : result_set)
    {
      if (count++ % 64 == 0)
      {
        Fmi::AsyncTask::interruption_point();
      }
      WeatherDataQCItem item;

      item.fmisid = as_int(row[0]);
      item.obstime = boost::posix_time::from_time_t(row[1].as<time_t>());
      item.parameter = row[2].as<std::string>();
      item.sensor_no = as_int(row[3]);
      if (!row[4].is_null())
        item.value = as_double(row[4]);
      item.flag = as_int(row[5]);
      item.modified_last = boost::posix_time::from_time_t(row[6].as<time_t>());

      cacheData.emplace_back(item);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readWeatherDataQCCacheDataFromPostgreSQL(
    std::vector<WeatherDataQCItem> &cacheData,
    const boost::posix_time::time_period &dataPeriod,
    const std::string &fmisid,
    const std::string &measurandId,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string sqlStmt =
        ("select fmisid, EXTRACT(EPOCH FROM "
         "date_trunc('seconds', obstime)) as obstime, parameter, sensor_no, value, flag, "
         "EXTRACT(EPOCH FROM date_trunc('seconds', "
         "modified_last)) as modified_last from "
         "weather_data_qc where obstime >= '" +
         Fmi::to_iso_extended_string(dataPeriod.begin()) + "' AND obstime <= '" +
         Fmi::to_iso_extended_string(dataPeriod.last()) + "'  AND value IS NOT NULL");
    if (!measurandId.empty())
      sqlStmt += (" AND parameter IN (" + measurandId + ")");
    if (!fmisid.empty())
      sqlStmt += (" AND fmisid IN (" + fmisid + ")");

    return readWeatherDataQCCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readWeatherDataQCCacheDataFromPostgreSQL(
    std::vector<WeatherDataQCItem> &cacheData,
    boost::posix_time::ptime lastTime,
    boost::posix_time::ptime lastModifiedTime,
    const Fmi::TimeZones &timezones)
{
  try
  {
    auto starttime = lastModifiedTime;

    const boost::posix_time::ptime now = second_clock::universal_time();
    auto diff = now - starttime;

    // Sometimes lastModifiedTime is 1.1.1970 due to problems, disable huge updates
    if (diff > boost::posix_time::hours(366 * 24))
    {
      starttime = lastTime;
      diff = now - starttime;
    }

    const bool big_request = (diff >= boost::posix_time::hours(24));

    if (big_request)
    {
      std::cout << (Spine::log_time_str() +
                    " [PostgreSQLObsDB] Performing a large EXT cache update starting from " +
                    Fmi::to_simple_string(starttime))
                << std::endl;
    }

    std::string sqlStmt =
        ("select fmisid, EXTRACT(EPOCH FROM "
         "date_trunc('seconds', obstime)) as obstime, parameter, sensor_no, value, flag, "
         "EXTRACT(EPOCH FROM date_trunc('seconds', "
         "modified_last)) as modified_last from "
         "weather_data_qc where modified_last >= '" +
         Fmi::to_iso_extended_string(starttime) + "'");

    return readWeatherDataQCCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readMagnetometerCacheDataFromPostgreSQL(
    std::vector<MagnetometerDataItem> &cacheData,
    boost::posix_time::ptime lastTime,
    boost::posix_time::ptime lastModifiedTime,
    const Fmi::TimeZones & /* timezones */)
{
  try
  {
    auto starttime = lastModifiedTime;

    const boost::posix_time::ptime now = second_clock::universal_time();
    auto diff = now - starttime;

    // Sometimes lastModifiedTime is 1.1.1970 due to problems, disable huge updates (?? perhaps not
    // possible fpr magnetometer data)
    if (diff > boost::posix_time::hours(366 * 24))
    {
      starttime = lastTime;
      diff = now - starttime;
    }

    const bool big_request = (diff >= boost::posix_time::hours(24));

    if (big_request)
    {
      std::cout
          << (Spine::log_time_str() +
              " [PostgreSQLObsDB] Performing a large Magnetometer cache update starting from " +
              Fmi::to_simple_string(starttime))
          << std::endl;
    }

    std::string sqlStmt =
        "SELECT station_id, magnetometer, level, EXTRACT(EPOCH FROM date_trunc('seconds', "
        "data_time)) AS obstime, "
        "x as magneto_x, y as magneto_y, z as magneto_z, t as magneto_t, f as magneto_f, "
        "data_quality,  EXTRACT(EPOCH FROM date_trunc('seconds', modified_last)) AS modtime from "
        "magnetometer_data";
    sqlStmt += (" where modified_last >= '" + Fmi::to_iso_extended_string(starttime) + "'");
    sqlStmt += (" AND magnetometer NOT IN ('NUR2','GAS1')");

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    unsigned int count = 0;
    for (auto row : result_set)
    {
      if (count++ % 64 == 0)
      {
        Fmi::AsyncTask::interruption_point();
      }
      MagnetometerDataItem item;

      item.fmisid = as_int(row[0]);
      item.magnetometer = row[1].as<std::string>();
      item.level = as_int(row[2]);
      item.data_time = boost::posix_time::from_time_t(row[3].as<time_t>());
      if (!row[4].is_null())
        item.x = as_double(row[4]);
      if (!row[5].is_null())
        item.y = as_double(row[5]);
      if (!row[6].is_null())
        item.z = as_double(row[6]);
      if (!row[7].is_null())
        item.t = as_double(row[7]);
      if (!row[8].is_null())
        item.f = as_double(row[8]);
      item.data_quality = as_int(row[9]);
      item.modified_last = boost::posix_time::from_time_t(row[10].as<time_t>());
      cacheData.emplace_back(item);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

/*
 * Set time interval for database query.
 */

void PostgreSQLObsDB::setTimeInterval(const ptime &theStartTime,
                                      const ptime &theEndTime,
                                      int theTimeStep)
{
  try
  {
    //    exactStartTime = theStartTime;
    timeStep = (wantedTime ? 1 : theTimeStep);
    startTime = theStartTime;
    endTime = theEndTime;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::fetchWeatherDataQCData(const std::string &sqlStmt,
                                             const StationInfo &stationInfo,
                                             const std::set<std::string> &stationgroup_codes,
                                             const TS::RequestLimits &requestLimits,
                                             WeatherDataQCData &cacheData)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    std::set<int> fmisids;
    std::set<boost::posix_time::ptime> obstimes;
    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();
      boost::optional<int> fmisid = as_int(row[0]);
      boost::posix_time::ptime obstime = boost::posix_time::from_time_t(row[1].as<time_t>());
      boost::optional<std::string> parameter = row[2].as<std::string>();
      int int_parameter = itsParameterMap->getRoadAndForeignIds().stringToInteger(*parameter);

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
        data_value = as_double(row[3]);
      if (!row[4].is_null())
        sensor_no = as_int(row[4]);
      if (!row[5].is_null())
        data_quality = as_int(row[5]);

      cacheData.fmisidsAll.push_back(fmisid);
      cacheData.obstimesAll.push_back(obstime);
      cacheData.latitudesAll.push_back(latitude);
      cacheData.longitudesAll.push_back(longitude);
      cacheData.elevationsAll.push_back(elevation);
      cacheData.parametersAll.push_back(int_parameter);
      cacheData.data_valuesAll.push_back(data_value);
      cacheData.sensor_nosAll.push_back(sensor_no);
      cacheData.data_qualityAll.push_back(data_quality);

      if (fmisid)
        fmisids.insert(*fmisid);
      obstimes.insert(obstime);

      check_request_limit(requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
      check_request_limit(requestLimits, fmisids.size(), TS::RequestLimitMember::LOCATIONS);
      check_request_limit(
          requestLimits, cacheData.data_valuesAll.size(), TS::RequestLimitMember::ELEMENTS);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Fetching data from PostgreSQL WeatherDataQCData cache failed!");
  }
}

std::string PostgreSQLObsDB::sqlSelectFromWeatherDataQCData(const Settings &settings,
                                                            const std::string &params,
                                                            const std::string &station_ids) const
{
  try
  {
    std::string sqlStmt;

    if (settings.wantedtime)
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
          " GROUP BY data.fmisid, data.obstime, data.parameter, data.value, data.sensor_no, "
          "data.flag "
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
          " GROUP BY  data.fmisid, data.obstime, data.parameter, data.value, data.sensor_no, "
          "data.flag "
          "ORDER BY fmisid ASC, obstime ASC";
    }

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    return sqlStmt;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP,
                                "Constructing SQL statement for PostgreSQL cache query failed!");
  }
}

void PostgreSQLObsDB::translateToIdFunction(Spine::Stations &stations, int net_id) const
{
  std::string sqlStmtStart = ("select getMemberId(" + Fmi::to_string(net_id) + ",");

  for (auto &s : stations)
  {
    if (net_id == 10 && s.lpnn > 0)
      continue;
    if (net_id == 20 && s.wmo > 0)
      continue;
    if (net_id == 30 && s.rwsid > 0)
      continue;

    std::string sqlStmt = (sqlStmtStart + Fmi::to_string(static_cast<int>(s.station_id)));
    // For RWSID dont use date
    if (net_id != 30)
      sqlStmt += (+",'" + Fmi::to_simple_string(s.station_start) + "'");
    sqlStmt += ")";

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      if (!row[0].is_null())
      {
        if (net_id == 10)
        {
          s.lpnn = as_int(row[0]);
        }
        else if (net_id == 20)
        {
          s.wmo = as_int(row[0]);
        }
        else if (net_id == 30)
        {
          s.rwsid = as_int(row[0]);
        }
      }
      break;
    }
  }
}

void PostgreSQLObsDB::translateToLPNN(Spine::Stations &stations) const
{
  translateToIdFunction(stations, 10);
}

void PostgreSQLObsDB::translateToWMO(Spine::Stations &stations) const
{
  translateToIdFunction(stations, 20);
}

void PostgreSQLObsDB::translateToRWSID(Spine::Stations &stations) const
{
  translateToIdFunction(stations, 30);
}

void PostgreSQLObsDB::getStations(Spine::Stations &stations) const
{
  try
  {
    // clang-format off
    string sqlStmt = R"SQL(SELECT DISTINCT tg.group_name                                 AS group_code,
                t.target_id                                   AS station_id,
                t.access_policy                               AS access_policy_id,
                t.target_status                               AS station_status_id,
                t.language_code                               AS language_code,
                t.target_formal_name                          AS station_formal_name,
                svname.target_formal_name                     AS sv_formal_name,
                enname.target_formal_name                     AS en_formal_name,
                t.target_start                                AS station_start,
                Min(tgm.valid_from)
                  over(
                    PARTITION BY t.target_id, tg.group_name)  AS valid_from,
                Max(tgm.valid_to)
                  over(
                    PARTITION BY t.target_id, tg.group_name)  AS valid_to,
                t.target_end                                  AS station_end,
                t.target_category,
                t.stationary,
                First_value(lpnn.member_code)
                  over(
                    PARTITION BY t.target_id
                    ORDER BY lpnn.membership_start DESC)      AS lpnn,
                First_value(wmon.member_code)
                  over(
                    PARTITION BY t.target_id
                    ORDER BY wmon.membership_start DESC)      AS wmon,
                Round(First_value(St_x(geom) :: NUMERIC)
                        over(
                          PARTITION BY t.target_id
                          ORDER BY l.location_start DESC), 5) AS last_longitude,
                Round(First_value(St_y(geom) :: NUMERIC)
                        over(
                          PARTITION BY t.target_id
                          ORDER BY l.location_start DESC), 5) AS last_latitude,
                Count(l.location_id)
                  over(
                    PARTITION BY t.target_id)                 AS locations,
                t.modified_last,
                t.modified_by,
                tg.rgb                                        AS rgb,
                tg.group_class_id
FROM   target_group_t1 tg
       join target_group_member_t1 tgm
         ON ( tgm.target_group_id = tg.target_group_id )
       join target_t1 t
         ON( t.target_id = tgm.target_id )
       join location_t1 l
         ON( l.target_id = t.target_id )
       left outer join network_member_t1 lpnn
                    ON( lpnn.target_id = t.target_id
                        AND lpnn.network_id = 10 )
       left outer join network_member_t1 wmon
                    ON( wmon.target_id = t.target_id
                        AND wmon.network_id = 20 )
       left outer join target_tl1 svname
                    ON ( svname.language_code = 'sv' and svname.target_id = t.target_id )
       left outer join target_tl1 enname
                    ON ( enname.language_code = 'en' and enname.target_id = t.target_id )
WHERE  tg.group_class_id IN( 1, 81 )
       AND tg.group_name IN( 'STUKRAD', 'STUKAIR', 'RWSFIN', 'AIRQCOMM',
                             'AIRQUAL', 'ASC', 'AVI', 'AWS',
                             'BUOY', 'CLIM', ' COMM', 'EXTAIRQUAL',
                             'EXTASC', 'EXTAVI', 'EXTAWS', 'EXTBUOY',
                             'EXTFLASH', 'EXTFROST', 'EXTICE', ' EXTMAGNET',
                             'EXTMAREO', 'EXTMAST', 'EXTRADACT', 'EXTRWS',
                             'EXTRWYWS', 'EXTSNOW', 'EXTSOUNDING', 'EXTSYNOP',
                             'EXTWATER', 'EXTWIND', 'FLASH', 'HTB',
                             'ICE', 'MAGNET', 'MAREO', 'MAST',
                             'PREC', 'RADACT', 'RADAR', ' RESEARCH',
                             'RWS', 'SEA', 'SHIP', 'SOLAR',
                             'SOUNDING', 'SYNOP' )
UNION ALL
SELECT DISTINCT tg.group_code,
                t.target_id                                   AS station_id,
                t.access_policy                               AS access_policy_id,
                t.target_status                               AS station_status_id,
                t.language_code                               AS language_code,
                t.target_formal_name                          AS station_formal_name,
                svname.target_formal_name                     AS sv_formal_name,
                enname.target_formal_name                     AS en_formal_name,
                t.target_start                                AS station_start,
                Min(tgm.membership_start)
                  over(
                    PARTITION BY t.target_id, tg.group_code)  AS valid_from,
                Max(tgm.membership_end)
                  over(
                    PARTITION BY t.target_id, tg.group_code)  AS valid_to,
                t.target_end                                  AS station_end,
                t.target_category,
                t.stationary,
                First_value(lpnn.member_code)
                  over(
                    PARTITION BY t.target_id
                    ORDER BY lpnn.membership_start DESC)      AS lpnn,
                First_value(wmon.member_code)
                  over(
                    PARTITION BY t.target_id
                    ORDER BY wmon.membership_start DESC)      AS wmon,
                Round(First_value(St_x(geom) :: NUMERIC)
                        over(
                          PARTITION BY t.target_id
                          ORDER BY l.location_start DESC), 5) AS last_longitude,
                Round(First_value(St_y(geom) :: NUMERIC)
                        over(
                          PARTITION BY t.target_id
                          ORDER BY l.location_start DESC), 5) AS last_latitude,
                Count(l.location_id)
                  over(
                    PARTITION BY t.target_id)                 AS locations,
                t.modified_last,
                t.modified_by,
                tg.rgb                                        AS rgb,
                tg.group_class_id
FROM   network_t1 tg
       join network_member_t1 tgm
         ON ( tgm.network_id = tg.network_id )
       join target_t1 t
         ON( t.target_id = tgm.target_id )
       join location_t1 l
         ON( l.target_id = t.target_id )
       left outer join network_member_t1 lpnn
                    ON ( lpnn.target_id = t.target_id
                         AND lpnn.network_id = 10 )
       left outer join network_member_t1 wmon
                    ON ( wmon.target_id = t.target_id
                         AND wmon.network_id = 20 )
       left outer join target_tl1 svname
                    ON ( svname.language_code = 'sv' and svname.target_id = t.target_id )
       left outer join target_tl1 enname
                    ON ( enname.language_code = 'en' and enname.target_id = t.target_id )
WHERE  tg.group_class_id IN( 1, 81 )
       AND tg.group_code IN( 'STUKRAD', 'STUKAIR', 'RWSFIN', 'AIRQCOMM',
                             'AIRQUAL', 'ASC', 'AVI', 'AWS',
                             'BUOY', 'CLIM', ' COMM', 'EXTAIRQUAL',
                             'EXTASC', 'EXTAVI', 'EXTAWS', 'EXTBUOY',
                             'EXTFLASH', 'EXTFROST', 'EXTICE', ' EXTMAGNET',
                             'EXTMAREO', 'EXTMAST', 'EXTRADACT', 'EXTRWS',
                             'EXTRWYWS', 'EXTSNOW', 'EXTSOUNDING', 'EXTSYNOP',
                             'EXTWATER', 'EXTWIND', 'FLASH', 'HTB',
                             'ICE', 'MAGNET', 'MAREO', 'MAST',
                             'PREC', 'RADACT', 'RADAR', ' RESEARCH',
                             'RWS', 'SEA', 'SHIP', 'SOLAR',
                             'SOUNDING', 'SYNOP' );)SQL";
    // clang-format on

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Spine::Station s;
      s.station_type = row[0].as<std::string>();
      s.station_id = as_int(row[1]);
      s.access_policy_id = as_int(row[2]);

      // Skip private stations unless EXTRWYWS (runway stations)
      if (s.access_policy_id != 0 && s.station_type != "EXTRWYWS")
      {
        // std::cerr << "PROTECTED station " << station_id << " " << station_formal_name_fi << " of
        // type " << station_type << std::endl;
        continue;
      }

      s.fmisid = s.station_id;
      s.lpnn = -1;
      s.wmo = -1;
      s.rwsid = -1;
      s.geoid = -1;
      s.distance = "-1";
      s.stationDirection = -1;

      s.station_status_id = as_int(row[3]);
      s.language_code = row[4].as<std::string>();
      s.station_formal_name_fi = row[5].as<std::string>();
      if (!row[6].is_null())
        s.station_formal_name_sv = row[6].as<std::string>();
      if (!row[7].is_null())
        s.station_formal_name_en = row[7].as<std::string>();
      auto station_start = row[8].as<std::string>();
      auto station_end = row[11].as<std::string>();
      s.station_start = Fmi::TimeParser::parse(row[9].as<std::string>());
      s.station_end = Fmi::TimeParser::parse(row[10].as<std::string>());
      s.target_category = as_int(row[12]);
      s.stationary = row[13].as<std::string>();
      if (!row[14].is_null())
        s.lpnn = as_int(row[14]);
      if (!row[15].is_null())
        s.wmo = as_int(row[15]);
      if (!row[16].is_null())
        s.longitude_out = as_double(row[16]);
      if (!row[17].is_null())
        s.latitude_out = as_double(row[17]);
      s.modified_last = Fmi::TimeParser::parse(row[19].as<std::string>());
      s.modified_by = as_int(row[20]);
      stations.push_back(s);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading stations from PostgreSQL database failed!");
  }
}

void PostgreSQLObsDB::readStationLocations(StationLocations &stationLocations) const
{
  try
  {
    std::string sqlStmt =
        "SELECT location_id, station_id, country_id, location_start, location_end, lon, lat, "
        "station_elevation from locations_v2";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      if (row[5].is_null() || row[6].is_null() || row[7].is_null())
        continue;
      try
      {
        StationLocation item;
        item.location_id = as_int(row[0]);
        item.fmisid = as_int(row[1]);
        item.country_id = as_int(row[2]);
        item.location_start = Fmi::TimeParser::parse(row[3].as<std::string>());
        item.location_end = Fmi::TimeParser::parse(row[4].as<std::string>());
        item.longitude = as_double(row[5]);
        item.latitude = as_double(row[6]);
        item.elevation = as_double(row[7]);
        stationLocations[item.fmisid].push_back(item);
      }
      catch (Fmi::Exception &e)
      {
        std::cerr << "Warning while reading station metadata: " << e.what() << std::endl;
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::getStationGroups(StationGroups &sg) const
{
  try
  {
    // First get groups
    std::string sqlStmt =
        "select group_id, group_name from station_groups_v1 where class_id in (1,81)";

    if (itsDebug)
      std::cout << "PostgreSQL (station groups): " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    std::map<int, std::string> groups;  // group_id -> group_name
    for (auto row : result_set)
    {
      int group_id = as_int(row[0]);
      auto group_name = row[1].as<std::string>();
      groups[group_id] = group_name;
    }

    // Then get group members
    sqlStmt = "select group_id, station_id, valid_from, valid_to from group_members_v1";

    if (itsDebug)
      std::cout << "PostgreSQL (station group members): " << sqlStmt << std::endl;

    result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      int group_id = as_int(row[0]);
      if (groups.find(group_id) == groups.end())
        continue;

      int station_id = as_int(row[1]);
      boost::posix_time::ptime starttime = Fmi::TimeParser::parse(row[2].as<std::string>());
      boost::posix_time::ptime endtime = Fmi::TimeParser::parse(row[3].as<std::string>());
      if (groups.find(group_id) != groups.end())
      {
        const std::string &group_name = groups.at(group_id);
        sg.addGroupPeriod(station_id, group_name, starttime, endtime);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::getProducerGroups(ProducerGroups &pg) const
{
  try
  {
    std::string sqlStmt =
        "select group_name,producer_id,membership_valid_from,membership_valid_to FROM "
        "producer_group_members_v1 where group_in_use=1 and namespace='cldb'";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      auto group_name = row[0].as<std::string>();
      int producer_id = as_int(row[1]);
      boost::posix_time::ptime starttime = Fmi::TimeParser::parse(row[2].as<std::string>());
      boost::posix_time::ptime endtime = Fmi::TimeParser::parse(row[3].as<std::string>());
      pg.addGroupPeriod(group_name, producer_id, starttime, endtime);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::getMovingStations(Spine::Stations &stations,
                                        const std::string & /* stationtype */,
                                        const boost::posix_time::ptime &startTime,
                                        const boost::posix_time::ptime &endTime,
                                        const std::string &wkt) const
{
  try
  {
    auto sdate = Fmi::to_iso_extended_string(startTime);
    auto edate = Fmi::to_iso_extended_string(endTime);
    std::string sqlStmt =
        ("SELECT distinct station_id FROM moving_locations_v1 WHERE ((sdate BETWEEN '" + sdate +
         "' AND '" + edate + "') OR (edate BETWEEN '" + sdate + "' AND '" + edate +
         "') OR (sdate <= '" + sdate + "' AND edate >='" + edate +
         "')) AND ST_Contains(ST_GeomFromText('" + wkt + "'),ST_MakePoint(lon, lat))");

    //	std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    auto result_set = itsDB.executeNonTransaction(sqlStmt);

    for (const auto &row : result_set)
    {
      Spine::Station station;
      station.station_id = row[0].as<int>();
      station.fmisid = station.station_id;
      stations.push_back(station);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
