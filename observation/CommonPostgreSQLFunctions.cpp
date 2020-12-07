#include "CommonPostgreSQLFunctions.h"
#include "Utils.h"
#include <gis/OGR.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>
#include <macgyver/TimeFormatter.h>
#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
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
        BCP, "SmartMet::Engine::Observation::CommonPostgreSQLFunctions constructor failed!");
  }
}

CommonPostgreSQLFunctions::~CommonPostgreSQLFunctions()
{
  itsDB.close();
}

void CommonPostgreSQLFunctions::shutdown()
{
  std::cout << "  -- Shutdown requested (PostgreSQL)\n";
  itsShutdownRequested = true;
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getObservationData(
    const Spine::Stations &stations,
    const SmartMet::Engine::Observation::Settings &settings,
    const SmartMet::Engine::Observation::StationInfo &stationInfo,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    // Always use FMI parameter numbers for the narrow table Cache except for solar and mareograph
    std::string stationtype = (itsIsCacheDatabase ? "observations_fmi" : settings.stationtype);
    if (itsIsCacheDatabase && (settings.stationtype == "solar" ||
                               settings.stationtype.find("mareograph") != std::string::npos))
      stationtype = settings.stationtype;

    // This maps measurand_id and the parameter position in TimeSeriesVector
    auto qmap = buildQueryMapping(stations, settings, itsParameterMap, stationtype, false);

    // Resolve stationgroup codes
    std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet = itsStationtypeConfig.getGroupCodeSetByStationtype(stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    Engine::Observation::LocationDataItems observations =
        readObservationDataFromDB(stations, settings, stationInfo, qmap, stationgroup_codes);

    std::set<int> observed_fmisids;
    for (auto item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    Engine::Observation::StationMap fmisid_to_station =
        mapQueryStations(stations, observed_fmisids);

    Engine::Observation::ObservationsMap obsmap =
        buildObservationsMap(observations, settings, timezones, fmisid_to_station);

    Spine::TimeSeries::TimeSeriesVectorPtr ret;

    return buildTimeseries(stations,
                           settings,
                           stationtype,
                           fmisid_to_station,
                           observations,
                           obsmap,
                           qmap,
                           timeSeriesOptions,
                           timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Fetching data from PostgreSQL database failed!");
  }
}

Engine::Observation::LocationDataItems CommonPostgreSQLFunctions::readObservationDataFromDB(
    const Spine::Stations &stations,
    const Engine::Observation::Settings &settings,
    const Engine::Observation::StationInfo &stationInfo,
    const Engine::Observation::QueryMapping &qmap,
    const std::set<std::string> &stationgroup_codes) const
{
  try
  {
    Engine::Observation::LocationDataItems ret;

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
      sqlStmt += "AND " + settings.sqlDataFilter.getSqlClause("data_quality", "data.data_quality") +
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
      sqlStmt += "AND " + settings.sqlDataFilter.getSqlClause("data_quality", "data.data_quality") +
                 " GROUP BY data.station_id, data.sensor_no, data.data_time, data.measurand_id, "
                 "data.data_value, data.data_quality, data.data_source "
                 "ORDER BY fmisid ASC, obstime ASC";
    }

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt
                << std::endl;

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    std::map<int, std::map<int, int>> default_sensors;

    for (auto row : result_set)
    {
      Engine::Observation::LocationDataItem obs;
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
      const Engine::Observation::StationLocation &sloc =
          stationInfo.stationLocations.getLocation(obs.data.fmisid, obs.data.data_time);
      // Get exact location, elevation
      if (sloc.location_id != -1)
      {
        obs.latitude = sloc.latitude;
        obs.longitude = sloc.longitude;
        obs.elevation = sloc.elevation;
      }
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
    throw Fmi::Exception::Trace(BCP, "Reading observations from PostgreSQL database failed!");
  }
}

SmartMet::Spine::TimeSeries::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getFlashData(
    const SmartMet::Engine::Observation::Settings &settings, const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = "flash";

    std::map<std::string, int> timeseriesPositions;
    std::map<std::string, int> specialPositions;

    std::string param;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = p.name();
      boost::to_lower(name, std::locale::classic());
      if (Engine::Observation::not_special(p))
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

    param = Engine::Observation::trimCommasFromEnd(param);

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

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        Engine::Observation::initializeResultVector(settings.parameters);

    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    for (auto row : result_set)
    {
      std::map<std::string, SmartMet::Spine::TimeSeries::Value> result;
      boost::posix_time::ptime stroke_time = boost::posix_time::from_time_t(row[0].as<time_t>());
      // int stroke_time_fraction = row[1].as<int>();
      SmartMet::Spine::TimeSeries::Value flashIdValue = row[2].as<int>();
      result["flash_id"] = flashIdValue;
      longitude = Fmi::stod(row[3].as<std::string>());
      latitude = Fmi::stod(row[4].as<std::string>());
      // Rest of the parameters in requested order
      for (unsigned int i = 5; i != row.size(); ++i)
      {
        pqxx::field fld = row[i];
        std::string data_type = itsPostgreDataTypes.at(fld.type());

        SmartMet::Spine::TimeSeries::Value temp;
        if (data_type == "text")
        {
          temp = row[i].as<std::string>();
        }
        else if (data_type == "numeric" || data_type == "decimal" || data_type == "float4" ||
                 data_type == "float8" || data_type == "_float4" || data_type == "_float8")
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

      auto localtz = timezones.time_zone_from_string(settings.timezone);
      boost::local_time::local_date_time localtime(stroke_time, localtz);

      std::pair<std::string, int> p;
      for (const auto &p : timeseriesPositions)
      {
        std::string name = p.first;
        int pos = p.second;

        SmartMet::Spine::TimeSeries::Value val = result[name];
        timeSeriesColumns->at(pos).push_back(
            SmartMet::Spine::TimeSeries::TimedValue(localtime, val));
      }
      for (const auto &p : specialPositions)
      {
        std::string name = p.first;
        int pos = p.second;
        if (name == "latitude")
        {
          SmartMet::Spine::TimeSeries::Value val = latitude;
          timeSeriesColumns->at(pos).push_back(
              SmartMet::Spine::TimeSeries::TimedValue(localtime, val));
        }
        if (name == "longitude")
        {
          SmartMet::Spine::TimeSeries::Value val = longitude;
          timeSeriesColumns->at(pos).push_back(
              SmartMet::Spine::TimeSeries::TimedValue(localtime, val));
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

FlashCounts CommonPostgreSQLFunctions::getFlashCount(
    const boost::posix_time::ptime &startt,
    const boost::posix_time::ptime &endt,
    const SmartMet::Spine::TaggedLocationList &locations)
{
  try
  {
    Engine::Observation::FlashCounts flashcounts;
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
        if (tloc.loc->type == SmartMet::Spine::Location::CoordinatePoint)
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
        else if (tloc.loc->type == SmartMet::Spine::Location::BoundingBox)
        {
          std::string bboxString = tloc.loc->name;
          SmartMet::Spine::BoundingBox bbox(bboxString);
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
        flashcounts.flashcount = row[0].as<int>();
      if (!row[1].is_null())
        flashcounts.strokecount = row[1].as<int>();
      if (!row[2].is_null())
        flashcounts.iccount = row[2].as<int>();
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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
