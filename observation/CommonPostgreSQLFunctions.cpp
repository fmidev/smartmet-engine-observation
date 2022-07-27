#include "CommonPostgreSQLFunctions.h"
#include "AsDouble.h"
#include "DatabaseDriverInfo.h"
#include "Keywords.h"
#include "Utils.h"
#include <gis/OGR.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeFormatter.h>
#include <spine/Value.h>
#include <thread>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

CommonPostgreSQLFunctions::CommonPostgreSQLFunctions(
    const Fmi::Database::PostgreSQLConnectionOptions &connectionOptions,
    const StationtypeConfig &stc,
    const ParameterMapPtr &pm)
    : CommonDatabaseFunctions(stc, pm)
{
  try
  {
    itsDB.open(connectionOptions);
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    if (itsDB.isConnected())
      itsPostgreDataTypes = itsDB.dataTypes();
  }
  catch (const std::exception &e)
  {
    std::string msg = "PostgreSQL error: " + std::string(e.what());
    throw Fmi::Exception(BCP, msg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "Engine::Observation::CommonPostgreSQLFunctions constructor failed!");
  }
}

CommonPostgreSQLFunctions::~CommonPostgreSQLFunctions()
{
  itsDB.close();
}

void CommonPostgreSQLFunctions::shutdown()
{
  std::cout << "  -- Shutdown requested (PostgreSQL)\n";
  itsDB.cancel();
}

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache)
{
  try
  {
    // Producer 'fmi' is deprecated
    std::string stationtype = settings.stationtype;
    if (stationtype == "fmi")
      stationtype = "observations_fmi";

    // This maps measurand_id and the parameter position in TimeSeriesVector
    auto qmap = buildQueryMapping(stations, settings, itsParameterMap, stationtype, false);

    // Resolve stationgroup codes
    std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet = itsStationtypeConfig.getGroupCodeSetByStationtype(stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    LocationDataItems observations =
        readObservationDataFromDB(stations, settings, stationInfo, qmap, stationgroup_codes);

    std::set<int> observed_fmisids;
    for (auto item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    StationMap fmisid_to_station = mapQueryStations(stations, observed_fmisids);

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
    throw Fmi::Exception::Trace(BCP, "Fetching data from PostgreSQL database failed!");
  }
}

LocationDataItems CommonPostgreSQLFunctions::readObservationDataFromDB(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const QueryMapping &qmap,
    const std::set<std::string> &stationgroup_codes) const
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

    std::string sqlStmt;
    if (itsIsCacheDatabase)
    {
      sqlStmt =
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
    }
    else
    {
      sqlStmt =
          "SELECT data.station_id AS fmisid, data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM "
          "date_trunc('seconds', data.data_time)) AS obstime, "
          "measurand_id, data_value, data_quality, data_source "
          "FROM observation_data_r1 data "
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
    }

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt
                << std::endl;

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

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getFlashData(const Settings &settings,
                                                                const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = FLASH_PRODUCER;

    std::map<std::string, int> timeseriesPositions;
    std::map<std::string, int> specialPositions;

    std::string param;
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

    std::string starttimeString = Fmi::to_iso_extended_string(settings.starttime);
    boost::replace_all(starttimeString, ",", ".");
    std::string endtimeString = Fmi::to_iso_extended_string(settings.endtime);
    boost::replace_all(endtimeString, ",", ".");

    std::string distancequery;

    std::string sqlStmt;

    if (itsIsCacheDatabase)
    {
      sqlStmt =
          "SELECT EXTRACT(EPOCH FROM "
          "date_trunc('seconds', stroke_time)) AS stroke_time, stroke_time_fraction, "
          "flash_id, X(stroke_location) AS longitude, "
          "Y(stroke_location) AS latitude, " +
          param +
          " "
          "FROM flash_data flash "
          "WHERE flash.stroke_time >= '" +
          starttimeString +
          "' "
          "AND flash.stroke_time <= '" +
          endtimeString + "' ";
    }
    else
    {
      sqlStmt =
          "SELECT EXTRACT(EPOCH FROM "
          "date_trunc('seconds', stroke_time)), nseconds, "
          "flash_id, ST_X(stroke_location) AS longitude, "
          "ST_Y(stroke_location) AS latitude, " +
          param +
          " "
          "FROM flashdata flash "
          "WHERE flash.stroke_time >= '" +
          starttimeString +
          "' "
          "AND flash.stroke_time <= '" +
          endtimeString + "' ";
    }

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

    if (itsIsCacheDatabase)
      sqlStmt += " ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction;";
    else
      sqlStmt += " ORDER BY flash.stroke_time ASC, flash.nseconds ASC;";

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt
                << std::endl;

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      std::map<std::string, TS::Value> result;
      boost::posix_time::ptime stroke_time = boost::posix_time::from_time_t(row[0].as<time_t>());
      // int stroke_time_fraction = as_int(row[1]);
      TS::Value flashIdValue = as_int(row[2]);
      result["flash_id"] = flashIdValue;
      longitude = Fmi::stod(row[3].as<std::string>());
      latitude = Fmi::stod(row[4].as<std::string>());
      // Rest of the parameters in requested order
      for (int i = 5; i != int(row.size()); ++i)
      {
        pqxx::field fld = row[i];
        std::string data_type = itsPostgreDataTypes.at(fld.type());

        TS::Value temp;
        if (data_type == "text")
        {
          temp = row[i].as<std::string>();
        }
        else if (data_type == "numeric" || data_type == "decimal" || data_type == "float4" ||
                 data_type == "float8" || data_type == "_float4" || data_type == "_float8")
        {
          temp = as_double(row[i]);
        }
        else if (data_type == "int2" || data_type == "int4" || data_type == "int8" ||
                 data_type == "_int2" || data_type == "_int4" || data_type == "_int8")
        {
          temp = as_int(row[i]);
        }

        result[fld.name()] = temp;
      }

      auto localtz = timezones.time_zone_from_string(settings.timezone);
      boost::local_time::local_date_time localtime(stroke_time, localtz);

      std::pair<std::string, int> p;
      for (const auto &p : timeseriesPositions)
      {
        std::string name = p.first;
        int pos = p.second;

        TS::Value val = result[name];
        timeSeriesColumns->at(pos).push_back(TS::TimedValue(localtime, val));
      }
      for (const auto &p : specialPositions)
      {
        std::string name = p.first;
        int pos = p.second;
        if (name == "latitude")
        {
          TS::Value val = latitude;
          timeSeriesColumns->at(pos).push_back(TS::TimedValue(localtime, val));
        }
        if (name == "longitude")
        {
          TS::Value val = longitude;
          timeSeriesColumns->at(pos).push_back(TS::TimedValue(localtime, val));
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash data from PostgreSQL database failed!");
  }
}

FlashCounts CommonPostgreSQLFunctions::getFlashCount(const boost::posix_time::ptime &startt,
                                                     const boost::posix_time::ptime &endt,
                                                     const Spine::TaggedLocationList &locations)
{
  try
  {
    FlashCounts flashcounts;
    flashcounts.flashcount = 0;
    flashcounts.strokecount = 0;
    flashcounts.iccount = 0;

    std::string starttime = Fmi::to_iso_extended_string(startt);
    std::string endtime = Fmi::to_iso_extended_string(endt);

    std::string sqlStmt =
        "SELECT "
        "SUM(CASE WHEN flash.multiplicity > 0 THEN 1 ELSE 0 END) AS "
        "flashcount, "
        "SUM(CASE WHEN flash.multiplicity = 0 THEN 1 ELSE 0 END) AS "
        "strokecount, "
        "SUM(CASE WHEN flash.cloud_indicator = 1 THEN 1 ELSE 0 END) AS iccount ";

    if (itsIsCacheDatabase)
      sqlStmt += "FROM flash_data flash ";
    else
      sqlStmt += "FROM flashdata flash ";
    sqlStmt += "WHERE flash.stroke_time BETWEEN '" + starttime + "' AND '" + endtime + "' ";

    if (!locations.empty())
    {
      for (const auto &tloc : locations)
      {
        if (tloc.loc->type == Spine::Location::CoordinatePoint)
        {
          std::string lon = Fmi::to_string(tloc.loc->longitude);
          std::string lat = Fmi::to_string(tloc.loc->latitude);
          std::string wkt = "POINT(" + lon + " " + lat + ")";

          OGRGeometry *geom = Fmi::OGR::createFromWkt(wkt, 4326);
          std::unique_ptr<OGRGeometry> circle;
          circle.reset(Fmi::OGR::expandGeometry(geom, tloc.loc->radius * 1000));
          OGRGeometryFactory::destroyGeometry(geom);
          std::string circleWkt = Fmi::OGR::exportToWkt(*circle.get());

          sqlStmt +=
              " AND ST_Within(flash.stroke_location, ST_GeomFromText('" + circleWkt + "',4326))";
        }
        else if (tloc.loc->type == Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          Spine::BoundingBox bbox(bboxString);
          std::string bboxWkt = "POLYGON((" + Fmi::to_string(bbox.xMin) + " " +
                                Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMin) + " " +
                                Fmi::to_string(bbox.yMax) + ", " + Fmi::to_string(bbox.xMax) + " " +
                                Fmi::to_string(bbox.yMax) + ", " + Fmi::to_string(bbox.xMax) + " " +
                                Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMin) + " " +
                                Fmi::to_string(bbox.yMin) + "))";

          sqlStmt +=
              " AND ST_Within(flash.stroke_location, ST_GeomFromText('" + bboxWkt + "',4326))";
        }
      }
    }

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt
                << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      if (!row[0].is_null())
        flashcounts.flashcount = as_int(row[0]);
      if (!row[1].is_null())
        flashcounts.strokecount = as_int(row[1]);
      if (!row[2].is_null())
        flashcounts.iccount = as_int(row[2]);
    }

    return flashcounts;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool CommonPostgreSQLFunctions::isConnected()
{
  return itsDB.isConnected();
}

void CommonPostgreSQLFunctions::reConnect()
{
  try
  {
    itsDB.reopen();
  }
  catch (const std::exception &e)
  {
    std::string msg = "PostgreSQL reConnect error: " + std::string(e.what());
    throw Fmi::Exception(BCP, msg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reconnecting PostgreSQL database failed!");
  }
}

const std::shared_ptr<Fmi::TimeFormatter> &CommonPostgreSQLFunctions::resetTimeFormatter(
    const std::string &format)
{
  try
  {
    itsTimeFormatter.reset(Fmi::TimeFormatter::create(format));
    return itsTimeFormatter;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "resetTimeFormatter failed!");
  }
}

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getMagnetometerData(
    const Settings &settings, const Fmi::TimeZones &timezones)
{
  return CommonDatabaseFunctions::getMagnetometerData(settings, timezones);
}

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getMagnetometerData(
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  TS::TimeSeriesVectorPtr ret = initializeResultVector(settings);
  std::map<int, TS::TimeSeriesVectorPtr> fmisid_results;
  std::map<int, std::set<boost::local_time::local_date_time>> fmisid_timesteps;

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
  unsigned int pos = 0;
  for (const auto &p : settings.parameters)
  {
    auto sparam = itsParameterMap->getParameter(p.name(), MAGNETO_PRODUCER);
    if (!sparam.empty())
      measurand_ids.insert(sparam);

    std::string name = p.name();
    boost::to_lower(name, std::locale::classic());
    timeseriesPositions[name] = pos;
    pos++;
  }

  if (measurand_ids.empty())
    return ret;

  // Starttime & endtime
  std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
  std::string endtime = Fmi::to_iso_extended_string(settings.endtime);

  std::string sqlStmt =
      "SELECT station_id, magnetometer, level, EXTRACT(EPOCH FROM date_trunc('seconds', "
      "data_time)) AS obstime, "
      "x as magneto_x, y as magneto_y, z as magneto_z, t as magneto_t, f as magneto_f, "
      "data_quality ";
  if (starttime == endtime)
    sqlStmt += ("from magnetometer_data where data_time = '" + starttime + "'");
  else
    sqlStmt += ("from magnetometer_data where (data_time >= '" + starttime +
                "' AND data_time <= '" + endtime + "')");
  sqlStmt += (" AND station_id IN (" + fmisids + ") AND magnetometer NOT IN ('NUR2','GAS1')");
  if (settings.dataFilter.exist("data_quality"))
    sqlStmt += (" AND " + settings.dataFilter.getSqlClause("data_quality", "data_quality"));

  if (itsDebug)
    std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt
              << std::endl;

  pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

  auto localtz = timezones.time_zone_from_string(settings.timezone);

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

  std::set<boost::local_time::local_date_time> timesteps_inserted;
  for (auto row : result_set)
  {
    int fmisid = as_int(row[0]);
    // Initialize result vector and timestep set
    if (fmisid_results.find(fmisid) == fmisid_results.end())
    {
      fmisid_results.insert(std::make_pair(fmisid, initializeResultVector(settings)));
      fmisid_timesteps.insert(
          std::make_pair(fmisid, std::set<boost::local_time::local_date_time>()));
    }
    TS::Value station_id_value = as_int(row[0]);
    TS::Value magnetometer_id_value = row[1].as<std::string>();
    int level = as_int(row[2]);
    boost::posix_time::ptime date_time = boost::posix_time::from_time_t(row[3].as<time_t>());
    boost::local_time::local_date_time localtime(date_time, localtz);
    TS::Value magneto_x_value;
    TS::Value magneto_y_value;
    TS::Value magneto_z_value;
    TS::Value magneto_t_value;
    TS::Value magneto_f_value;
    if (!row[4].is_null())
      magneto_x_value = as_double(row[4]);
    if (!row[5].is_null())
      magneto_y_value = as_double(row[5]);
    if (!row[6].is_null())
      magneto_z_value = as_double(row[6]);
    if (!row[7].is_null())
      magneto_t_value = as_double(row[7]);
    if (!row[8].is_null())
      magneto_f_value = as_double(row[8]);
    TS::Value data_quality_value = as_int(row[9]);

    auto &result = *(fmisid_results[fmisid]);
    auto &timesteps = fmisid_timesteps[fmisid];

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

    timesteps.insert(localtime);
  }

  // Add missing timesteps
  for (const auto &item : fmisid_timesteps)
  {
    const auto &fmisid = item.first;
    const auto &timesteps = item.second;
    auto &db_results = *(fmisid_results[fmisid]);
    for (auto &ts : db_results)
    {
      std::map<boost::local_time::local_date_time, TS::TimedValue> db_values;
      for (const auto &ts_value : ts)
        db_values.insert(std::make_pair(ts_value.time, ts_value));

      TS::TimeSeries new_ts(ts.getLocalTimePool());
      for (const auto &timestep : timesteps)
      {
        if (db_values.find(timestep) != db_values.end())
          new_ts.push_back(db_values.at(timestep));
        else
          new_ts.push_back(TS::TimedValue(timestep, TS::None()));
      }
      ts = new_ts;
    }
  }

  // Add final results to return structure fmisuid by fmisid
  for (const auto &item : fmisid_results)
  {
    const auto &ts_vector = *item.second;
    for (unsigned int i = 0; i < ts_vector.size(); i++)
      ret->at(i).insert(ret->at(i).end(), ts_vector.at(i).begin(), ts_vector.at(i).end());
  }

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
