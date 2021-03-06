#include "PostgreSQLObsDB.h"
#include "ExternalAndMobileDBInfo.h"
#include "PostgreSQLCacheDB.h"
#include "Utils.h"
#include <boost/functional/hash.hpp>
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
PostgreSQLObsDB::PostgreSQLObsDB(
    const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
    const StationtypeConfig &stc,
    const ParameterMapPtr &pm)
    : CommonPostgreSQLFunctions(connectionOptions, stc, pm)
{
}

void PostgreSQLObsDB::get(const std::string &sqlStatement,
                          std::shared_ptr<QueryResultBase> qrb,
                          const Fmi::TimeZones &timezones)
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
                                                        const Fmi::TimeZones &timezones)
{
  try
  {
    ExternalAndMobileDBInfo dbInfo;
    std::string sqlStmt = dbInfo.sqlSelectForCache(producer, lastTime, lastCreatedTime);

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    // Execute SQL statement
    Fmi::Database::PostgreSQLConnection &conn = getConnection();
    pqxx::result result_set = conn.executeNonTransaction(sqlStmt);

    ResultSetRows rsrs =
        PostgreSQLCacheDB::getResultSetForMobileExternalData(result_set, conn.dataTypes());

    SmartMet::Spine::TimeSeries::Value none = SmartMet::Spine::TimeSeries::None();
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

void PostgreSQLObsDB::readCacheDataFromPostgreSQL(std::vector<DataItem> &cacheData,
                                                  const std::string &sqlStmt,
                                                  const Fmi::TimeZones &timezones)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();

      DataItem item;
      item.fmisid = row[0].as<int>();
      item.sensor_no = row[1].as<int>();
      item.measurand_id = row[2].as<int>();
      item.producer_id = row[3].as<int>();
      item.measurand_no = row[4].as<int>();
      item.data_time = boost::posix_time::from_time_t(row[5].as<time_t>());
      if (!row[6].is_null())
        item.data_value = row[6].as<double>();
      if (!row[7].is_null())
        item.data_quality = row[7].as<int>();
      if (!row[8].is_null())
        item.data_source = row[8].as<int>();
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
        "FROM observation_data_v1 data WHERE data_time >= '" +
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
                                                  const boost::posix_time::ptime &startTime,
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
        "FROM observation_data_v1 data WHERE "
        "data.modified_last >= '" +
        Fmi::to_iso_extended_string(lastModifiedTime) + "' ORDER BY station_id ASC, data_time ASC";

    if (big_request)
    {
      std::cout << (Spine::log_time_str() +
                    " [PostgreSQLObsDB] Performing a large OBS cache update starting from " +
                    Fmi::to_simple_string(lastModifiedTime) + "\n");
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
                                                       const Fmi::TimeZones &timezones)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();

      FlashDataItem item;

      int epoch = row[0].as<int>();
      item.stroke_time = epoch2ptime(epoch);
      item.stroke_time_fraction = row[1].as<int>();
      item.flash_id = row[2].as<int>();
      item.multiplicity = row[3].as<int>();
      item.peak_current = row[4].as<int>();
      item.sensors = row[5].as<int>();
      item.freedom_degree = row[6].as<int>();
      item.ellipse_angle = row[7].as<double>();
      item.ellipse_major = row[8].as<double>();
      item.ellipse_minor = row[9].as<double>();
      item.chi_square = row[10].as<double>();
      item.rise_time = row[11].as<double>();
      item.ptz_time = row[12].as<double>();
      item.cloud_indicator = row[13].as<int>();
      item.angle_indicator = row[14].as<int>();
      item.signal_indicator = row[15].as<int>();
      item.timing_indicator = row[16].as<int>();
      item.stroke_status = row[17].as<int>();
      if (!row[18].is_null())
        item.data_source = row[18].as<int>();
      int created = row[19].as<int>();
      item.created = epoch2ptime(created);
      int modified_last = row[20].as<int>();
      item.modified_last = epoch2ptime(modified_last);
      if (!row[21].is_null())
        item.modified_by = row[21].as<int>();
      item.longitude = row[22].as<double>();
      item.latitude = row[23].as<double>();
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
        "WHERE stroke_time BETWEEN '" +
        Fmi::to_iso_extended_string(dataPeriod.begin()) + "'  AND '" +
        Fmi::to_iso_extended_string(dataPeriod.last()) + "' ORDER BY stroke_time, flash_id";

    return readFlashCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readFlashCacheDataFromPostgreSQL(
    std::vector<FlashDataItem> &cacheData,
    const boost::posix_time::ptime &startTime,
    const boost::posix_time::ptime &lastStrokeTime,
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
      std::cout << (Spine::log_time_str() +
                    " [PostgreSQLObsDB] Performing a large FLASH cache update starting from " +
                    Fmi::to_simple_string(lastModifiedTime) + "\n");
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

    return readFlashCacheDataFromPostgreSQL(cacheData, sqlStmt, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void PostgreSQLObsDB::readWeatherDataQCCacheDataFromPostgreSQL(
    std::vector<WeatherDataQCItem> &cacheData,
    const std::string &sqlStmt,
    const Fmi::TimeZones &timezones)
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

      item.fmisid = row[0].as<int>();
      item.obstime = boost::posix_time::from_time_t(row[1].as<time_t>());
      item.parameter = row[2].as<std::string>();
      item.sensor_no = row[3].as<int>();
      if (!row[4].is_null())
        item.value = row[4].as<double>();
      item.flag = row[5].as<int>();
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
                    Fmi::to_simple_string(starttime) + "\n");
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
    timeStep = (latest ? 1 : theTimeStep);
    startTime = theStartTime;
    endTime = theEndTime;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

LocationDataItems PostgreSQLObsDB::readObservations(const Spine::Stations &stations,
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
      producer_id_str_list.emplace_back(std::to_string(prodId));
    std::string producerIds = boost::algorithm::join(producer_id_str_list, ",");

    std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
    std::string endtime = Fmi::to_iso_extended_string(settings.endtime);

    std::string sqlStmt =
        "SELECT data.station_id AS fmisid, data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM "
        "date_trunc('seconds', data.data_time)) AS obstime, "
        "measurand_id, data_value, data_quality, data_source "
        "FROM observation_data_v1 data "
        "WHERE data.station_id IN (" +
        qstations +
        ") "
        "AND data.data_time >= '" +
        starttime + "' AND data.data_time <= '" + endtime + "' AND data.measurand_id IN (" +
        measurand_ids + ") ";
    if (!producerIds.empty())
      sqlStmt += ("AND data.producer_id IN (" + producerIds + ") ");

    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") +
               " GROUP BY data.station_id, data.sensor_no, data.data_time, data.measurand_id, "
               "data.data_value, data.data_quality, data.data_source "
               "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();
      LocationDataItem obs;
      obs.data.fmisid = row[0].as<int>();
      obs.data.sensor_no = row[1].as<int>();
      obs.data.data_time = boost::posix_time::from_time_t(row[2].as<time_t>());
      obs.data.measurand_id = row[3].as<int>();
      if (!row[4].is_null())
        obs.data.data_value = row[4].as<double>();
      if (!row[5].is_null())
        obs.data.data_quality = row[5].as<int>();
      if (!row[6].is_null())
        obs.data.data_source = row[6].as<int>();
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

void PostgreSQLObsDB::fetchWeatherDataQCData(const std::string &sqlStmt,
                                             const StationInfo &stationInfo,
                                             const std::set<std::string> &stationgroup_codes,
                                             const QueryMapping &qmap,
                                             WeatherDataQCData &cacheData)
{
  try
  {
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      Fmi::AsyncTask::interruption_point();
      boost::optional<int> fmisid = row[0].as<int>();
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
        data_value = row[3].as<double>();
      if (!row[4].is_null())
        sensor_no = row[4].as<int>();
      if (!row[5].is_null())
        data_quality = row[5].as<int>();

      cacheData.fmisidsAll.push_back(fmisid);
      cacheData.obstimesAll.push_back(obstime);
      cacheData.latitudesAll.push_back(latitude);
      cacheData.longitudesAll.push_back(longitude);
      cacheData.elevationsAll.push_back(elevation);
      cacheData.parametersAll.push_back(int_parameter);
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

std::string PostgreSQLObsDB::sqlSelectFromWeatherDataQCData(const Settings &settings,
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

void PostgreSQLObsDB::translateToIdFunction(SmartMet::Spine::Stations &stations, int net_id) const
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
      sqlStmt += (+",'" + boost::posix_time::to_simple_string(s.station_start) + "'");
    sqlStmt += ")";

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      if (!row[0].is_null())
      {
        if (net_id == 10)
        {
          s.lpnn = row[0].as<int>();
        }
        else if (net_id == 20)
        {
          s.wmo = row[0].as<int>();
        }
        else if (net_id == 30)
        {
          s.rwsid = row[0].as<int>();
        }
      }
      break;
    }
  }
}

void PostgreSQLObsDB::translateToLPNN(SmartMet::Spine::Stations &stations) const
{
  translateToIdFunction(stations, 10);
}

void PostgreSQLObsDB::translateToWMO(SmartMet::Spine::Stations &stations) const
{
  translateToIdFunction(stations, 20);
}

void PostgreSQLObsDB::translateToRWSID(SmartMet::Spine::Stations &stations) const
{
  translateToIdFunction(stations, 30);
}

void PostgreSQLObsDB::getStations(SmartMet::Spine::Stations &stations) const
{
  try
  {
    string sqlStmt =
        "SELECT DISTINCT tg.group_name AS group_code, t.target_id AS station_id, t.access_policy "
        "AS "
        "access_policy_id, t.target_status AS station_status_id, t.language_code AS language_code, "
        "t.target_formal_name AS station_formal_name, t.target_start AS station_start, "
        "MIN(tgm.valid_from) OVER(PARTITION BY t.target_id, tg.group_name) AS valid_from, "
        "MAX(tgm.valid_to) OVER(PARTITION BY t.target_id, tg.group_name) AS valid_to, t.target_end "
        "AS station_end, t.target_category, t.stationary, FIRST_VALUE(lpnn.member_code) "
        "OVER(PARTITION BY t.target_id ORDER BY lpnn.membership_start DESC) AS lpnn, "
        "FIRST_VALUE(wmon.member_code) OVER(PARTITION BY t.target_id ORDER BY "
        "wmon.membership_start "
        "DESC) AS wmon, ROUND(FIRST_VALUE(ST_X(geom)::numeric) OVER(PARTITION BY t.target_id ORDER "
        "BY l.location_start DESC), 5) AS last_longitude, ROUND(FIRST_VALUE(ST_Y(geom)::numeric) "
        "OVER(PARTITION BY t.target_id ORDER BY l.location_start DESC), 5) AS last_latitude, "
        "COUNT(l.location_id) OVER(PARTITION BY t.target_id) AS locations, t.modified_last, "
        "t.modified_by, tg.rgb as rgb, tg.group_class_id FROM target_group_t1 tg JOIN "
        "target_group_member_t1 tgm ON (tgm.target_group_id = tg.target_group_id) JOIN target_t1 t "
        "ON(t.target_id = tgm.target_id) JOIN location_t1 l ON(l.target_id = t.target_id) LEFT "
        "OUTER "
        "JOIN network_member_t1 lpnn ON(lpnn.target_id = t.target_id AND lpnn.network_id = 10) "
        "LEFT "
        "OUTER JOIN network_member_t1 wmon ON(wmon.target_id = t.target_id AND wmon.network_id = "
        "20) "
        "WHERE tg.group_class_id IN(1, 81) AND tg.group_name "
        "IN('STUKRAD','STUKAIR','RWSFIN','AIRQCOMM','AIRQUAL','ASC','AVI','AWS','BUOY','CLIM','"
        "COMM',"
        "'EXTAIRQUAL','EXTASC','EXTAVI','EXTAWS','EXTBUOY','EXTFLASH','EXTFROST','EXTICE','"
        "EXTMAGNET'"
        ",'EXTMAREO','EXTMAST','EXTRADACT','EXTRWS','EXTRWYWS','EXTSNOW','EXTSOUNDING','EXTSYNOP','"
        "EXTWATER','EXTWIND','FLASH','HTB','ICE','MAGNET','MAREO','MAST','PREC','RADACT','RADAR','"
        "RESEARCH','RWS','SEA','SHIP','SOLAR','SOUNDING','SYNOP') UNION ALL SELECT DISTINCT "
        "tg.group_code, t.target_id AS station_id, t.access_policy AS access_policy_id, "
        "t.target_status AS station_status_id, t.language_code AS language_code, "
        "t.target_formal_name AS station_formal_name, t.target_start AS station_start, "
        "MIN(tgm.membership_start) OVER(PARTITION BY t.target_id, tg.group_code) AS valid_from, "
        "MAX(tgm.membership_end) OVER(PARTITION BY t.target_id, tg.group_code) AS valid_to, "
        "t.target_end AS station_end, t.target_category, t.stationary, "
        "FIRST_VALUE(lpnn.member_code) "
        "OVER(PARTITION BY t.target_id ORDER BY lpnn.membership_start DESC) AS lpnn, "
        "FIRST_VALUE(wmon.member_code) OVER(PARTITION BY t.target_id ORDER BY "
        "wmon.membership_start "
        "DESC) AS wmon, ROUND(FIRST_VALUE(ST_X(geom)::numeric) OVER(PARTITION BY t.target_id ORDER "
        "BY l.location_start DESC), 5) AS last_longitude, ROUND(FIRST_VALUE(ST_Y(geom)::numeric) "
        "OVER(PARTITION BY t.target_id ORDER BY l.location_start DESC), 5) AS last_latitude, "
        "COUNT(l.location_id) OVER(PARTITION BY t.target_id) AS locations, t.modified_last, "
        "t.modified_by, tg.rgb as rgb, tg.group_class_id FROM network_t1 tg JOIN network_member_t1 "
        "tgm ON (tgm.network_id = tg.network_id) JOIN target_t1 t ON(t.target_id = tgm.target_id) "
        "JOIN location_t1 l ON(l.target_id = t.target_id) LEFT OUTER JOIN network_member_t1 lpnn "
        "ON "
        "(lpnn.target_id = t.target_id AND lpnn.network_id = 10) LEFT OUTER JOIN network_member_t1 "
        "wmon ON (wmon.target_id = t.target_id AND wmon.network_id = 20) WHERE tg.group_class_id "
        "IN(1, 81) AND tg.group_code "
        "IN('STUKRAD','STUKAIR','RWSFIN','AIRQCOMM','AIRQUAL','ASC','AVI','AWS','BUOY','CLIM','"
        "COMM',"
        "'EXTAIRQUAL','EXTASC','EXTAVI','EXTAWS','EXTBUOY','EXTFLASH','EXTFROST','EXTICE','"
        "EXTMAGNET'"
        ",'EXTMAREO','EXTMAST','EXTRADACT','EXTRWS','EXTRWYWS','EXTSNOW','EXTSOUNDING','EXTSYNOP','"
        "EXTWATER','EXTWIND','FLASH','HTB','ICE','MAGNET','MAREO','MAST','PREC','RADACT','RADAR','"
        "RESEARCH','RWS','SEA','SHIP','SOLAR','SOUNDING','SYNOP');";

    if (itsDebug)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      std::string station_start, station_end;
      SmartMet::Spine::Station s;
      s.station_type = row[0].as<std::string>();
      s.station_id = row[1].as<int>();
      s.access_policy_id = row[2].as<int>();

      // Skip private stations unless EXTRWYWS (runway stations)
      if (s.access_policy_id != 0 && s.station_type != "EXTRWYWS")
      {
        // std::cerr << "PROTECTED station " << station_id << " " << station_formal_name << " of
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

      s.station_status_id = row[3].as<int>();
      s.language_code = row[4].as<std::string>();
      s.station_formal_name = row[5].as<std::string>();
      station_start = row[6].as<std::string>();
      station_end = row[9].as<std::string>();
      s.station_start = boost::posix_time::time_from_string(row[7].as<std::string>());
      s.station_end = boost::posix_time::time_from_string(row[8].as<std::string>());
      s.target_category = row[10].as<int>();
      s.stationary = row[11].as<std::string>();
      if (!row[12].is_null())
        s.lpnn = row[12].as<int>();
      if (!row[13].is_null())
        s.wmo = row[13].as<int>();
      if (!row[14].is_null())
        s.longitude_out = row[14].as<double>();
      if (!row[15].is_null())
        s.latitude_out = row[15].as<double>();
      s.modified_last = boost::posix_time::time_from_string(row[17].as<std::string>());
      s.modified_by = row[18].as<int>();

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

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (auto row : result_set)
    {
      if (row[5].is_null() || row[6].is_null() || row[7].is_null())
        continue;
      StationLocation item;
      item.location_id = row[0].as<int>();
      item.fmisid = row[1].as<int>();
      item.country_id = row[2].as<int>();
      item.location_start = boost::posix_time::time_from_string(row[3].as<std::string>());
      item.location_end = boost::posix_time::time_from_string(row[4].as<std::string>());
      item.longitude = row[5].as<double>();
      item.latitude = row[6].as<double>();
      item.elevation = row[7].as<double>();

      stationLocations[item.fmisid].push_back(item);
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
