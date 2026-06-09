#include "CommonPostgreSQLFunctions.h"
#include "AsDouble.h"
#include "DatabaseDriverInfo.h"
#include "Keywords.h"
#include "Utils.h"
#include <gis/OGR.h>
#include <macgyver/Exception.h>
#include <macgyver/Join.h>
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
const std::string globe = "POLYGON ((-180 -90,-180 90,180 90,180 -90,-180 -90))";

using namespace Utils;

namespace
{
// Build the SELECT SQL for readObservationDataOfMovingStationsFromDB on the cache DB path.
std::string buildMovingStationsCacheSQL(const std::string &starttime,
                                        const std::string &endtime,
                                        const std::string &measurand_ids,
                                        const std::string &fmisids,
                                        const std::string &producerIds,
                                        const std::string &sensorCondition,
                                        const std::string &qualityClause,
                                        const std::string &wktString)
{
  std::string sql =
      "SELECT data.fmisid AS fmisid, data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM "
      "data.data_time) AS obstime, "
      "measurand_id, data_value, data_quality, data_source "
      "FROM observation_data data WHERE "
      "data.data_time >= '" +
      starttime + "' AND data.data_time <= '" + endtime + "' AND data.measurand_id IN (" +
      measurand_ids + ") ";
  if (!fmisids.empty())
    sql += "AND data.fmisid IN (" + fmisids + ") ";
  if (!producerIds.empty())
    sql += "AND data.producer_id IN (" + producerIds + ") ";
  sql += sensorCondition;
  sql += "AND " + qualityClause;
  if (!wktString.empty() && wktString != globe)
    sql += "AND ST_Contains(ST_GeomFromText('" + wktString +
           "',4326),ST_SetSRID(ST_MakePoint(m.lon, m.lat),4326)) ";
  sql +=
      " GROUP BY data.fmisid, data.sensor_no, data.data_time, data.measurand_id, "
      "data.data_value, data.data_quality, data.data_source "
      "ORDER BY fmisid ASC, obstime ASC";
  return sql;
}

// Build the SELECT SQL for readObservationDataOfMovingStationsFromDB on the direct DB path.
std::string buildMovingStationsDirectSQL(const std::string &starttime,
                                         const std::string &endtime,
                                         const std::string &measurand_ids,
                                         const std::string &fmisids,
                                         const std::string &producerIds,
                                         const std::string &sensorCondition,
                                         const std::string &qualityClause,
                                         const std::string &wktString)
{
  std::string sql =
      "SELECT data.station_id AS fmisid, data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM "
      "date_trunc('seconds', data.data_time)) AS obstime, "
      "data.measurand_id, data.data_value, data.data_quality, data.data_source, m.lon, m.lat, "
      "m.elev "
      "FROM observation_data_r1 data JOIN moving_locations_v1 m ON (m.station_id = "
      "data.station_id and data.data_time between m.sdate and m.edate) WHERE "
      "data.data_time >= '" +
      starttime + "' AND data.data_time <= '" + endtime + "' AND data.measurand_id IN (" +
      measurand_ids + ")";
  if (!fmisids.empty())
    sql += " AND data.station_id IN (" + fmisids + ") ";
  if (!producerIds.empty())
    sql += "AND data.producer_id IN (" + producerIds + ") ";
  sql += sensorCondition;
  sql += " AND " + qualityClause;
  if (!wktString.empty() && wktString != globe)
    sql += " AND ST_Contains(ST_GeomFromText('" + wktString +
           "',4326),ST_SetSRID(ST_MakePoint(m.lon, m.lat),4326)) ";
  sql +=
      " GROUP BY data.station_id, data.sensor_no, data.data_time, data.measurand_id, "
      "data.data_value, data.data_quality, data.data_source, m.lon, m.lat, m.elev "
      "ORDER BY fmisid ASC, obstime ASC";
  return sql;
}

// Parse a single row from readObservationDataOfMovingStationsFromDB result sets.
// The row type is templated to accept both pqxx::row (libpqxx <= 7) and the pqxx::row_ref proxy
// returned when iterating a result in libpqxx 8.
template <typename Row>
LocationDataItem parseMovingStationObservation(const Row &row)
{
  LocationDataItem obs;
  obs.data.fmisid = as_int(row[0]);
  obs.data.sensor_no = as_int(row[1]);
  obs.data.data_time = Fmi::date_time::from_time_t(row[2].template as<time_t>());
  obs.data.measurand_id = as_int(row[3]);
  if (!row[4].is_null())
    obs.data.data_value = as_double(row[4]);
  if (!row[5].is_null())
    obs.data.data_quality = as_int(row[5]);
  if (!row[6].is_null())
    obs.data.data_source = as_int(row[6]);
  obs.longitude = as_double(row[7]);
  obs.latitude = as_double(row[8]);
  obs.elevation = as_double(row[9]);
  return obs;
}

// ── getFlashData helpers ──────────────────────────────────────────────────────

// Convert a pqxx field to a TS::Value based on its PostgreSQL data type string.
// The field type is templated to accept both pqxx::field (libpqxx <= 7) and the pqxx::field_ref
// proxy returned when indexing a row in libpqxx 8.
template <typename Field>
TS::Value parseFlashFieldValue(const Field &fld, const std::string &data_type)
{
  if (data_type == "text")
    return fld.template as<std::string>();
  if (data_type == "numeric" || data_type == "decimal" || data_type == "float4" ||
      data_type == "float8" || data_type == "_float4" || data_type == "_float8")
    return as_double(fld);
  if (data_type == "int2" || data_type == "int4" || data_type == "int8" || data_type == "_int2" ||
      data_type == "_int4" || data_type == "_int8")
    return as_int(fld);
  return {};
}

// Build the base SELECT clause for getFlashData (cache vs. direct DB).
std::string buildFlashBaseSQL(bool isCacheDB,
                              const std::string &param,
                              const std::string &starttime,
                              const std::string &endtime)
{
  std::string separator;
  if (!param.empty())
    separator = ", ";

  if (isCacheDB)
  {
    return "SELECT EXTRACT(EPOCH FROM date_trunc('seconds', stroke_time)) AS stroke_time, "
           "stroke_time_fraction, flash_id, X(stroke_location) AS longitude, "
           "Y(stroke_location) AS latitude" +
           separator + param +
           " FROM flash_data flash "
           "WHERE flash.stroke_time >= '" +
           starttime + "' AND flash.stroke_time <= '" + endtime + "' ";
  }
  return "SELECT EXTRACT(EPOCH FROM date_trunc('seconds', stroke_time)), nseconds, "
         "flash_id, ST_X(stroke_location) AS longitude, "
         "ST_Y(stroke_location) AS latitude" +
         separator + param +
         " FROM flashdata flash "
         "WHERE flash.stroke_time >= '" +
         starttime + "' AND flash.stroke_time <= '" + endtime + "' ";
}

// Append spatial WHERE clauses from taggedLocations and boundingBox to the flash SQL.
void appendFlashLocationFilters(std::string &sql,
                                const Spine::TaggedLocationList &taggedLocations,
                                const std::map<std::string, double> &boundingBox)
{
  for (const auto &tloc : taggedLocations)
  {
    if (tloc.loc->type == Spine::Location::CoordinatePoint)
    {
      std::string lon = Fmi::to_string(tloc.loc->longitude);
      std::string lat = Fmi::to_string(tloc.loc->latitude);
      std::string radius = Fmi::to_string(tloc.loc->radius * 1000);
      sql += " AND ST_DistanceSphere(ST_GeomFromText('POINT(" + lon + " " + lat +
             ")', 4326), flash.stroke_location) <= " + radius;
    }
    if (tloc.loc->type == Spine::Location::BoundingBox && boundingBox.empty())
    {
      Spine::BoundingBox bbox(tloc.loc->name);
      sql += " AND ST_Within(flash.stroke_location, ST_MakeEnvelope(" + Fmi::to_string(bbox.xMin) +
             ", " + Fmi::to_string(bbox.yMin) + ", " + Fmi::to_string(bbox.xMax) + ", " +
             Fmi::to_string(bbox.yMax) + ", 4326)) ";
    }
  }
  if (!boundingBox.empty())
    sql += " AND ST_Within(flash.stroke_location, ST_MakeEnvelope(" +
           Fmi::to_string(boundingBox.at("minx")) + ", " + Fmi::to_string(boundingBox.at("miny")) +
           ", " + Fmi::to_string(boundingBox.at("maxx")) + ", " +
           Fmi::to_string(boundingBox.at("maxy")) + ", 4326)) ";
}

// Build the full flash SQL including spatial filters and ORDER BY.
std::string buildFlashSQL(bool isCacheDB,
                          const std::string &param,
                          const std::string &starttime,
                          const std::string &endtime,
                          const Spine::TaggedLocationList &taggedLocations,
                          const std::map<std::string, double> &boundingBox)
{
  std::string sql = buildFlashBaseSQL(isCacheDB, param, starttime, endtime);
  appendFlashLocationFilters(sql, taggedLocations, boundingBox);
  if (isCacheDB)
    sql += " ORDER BY flash.stroke_time ASC, flash.stroke_time_fraction;";
  else
    sql += " ORDER BY flash.stroke_time ASC, flash.nseconds ASC;";
  return sql;
}

// ── getMagnetometerData helpers ───────────────────────────────────────────────

struct MagnetometerRowData
{
  int fmisid{0};
  std::string magnetometer_id;
  int level{0};
  Fmi::DateTime data_time;
  TS::Value magneto_x, magneto_y, magneto_z, magneto_t, magneto_f, data_quality;
};

// Parse one magnetometer_data result row; nullable fields become TS::None().
// The row type is templated to accept both pqxx::row (libpqxx <= 7) and the pqxx::row_ref proxy
// returned when iterating a result in libpqxx 8.
template <typename Row>
MagnetometerRowData parseMagnetometerRow(const Row &row)
{
  MagnetometerRowData d;
  d.fmisid = as_int(row[0]);
  d.magnetometer_id = row[1].template as<std::string>();
  d.level = as_int(row[2]);
  d.data_time = Fmi::date_time::from_time_t(row[3].template as<time_t>());
  if (!row[4].is_null())
    d.magneto_x = as_double(row[4]);
  if (!row[5].is_null())
    d.magneto_y = as_double(row[5]);
  if (!row[6].is_null())
    d.magneto_z = as_double(row[6]);
  if (!row[7].is_null())
    d.magneto_t = as_double(row[7]);
  if (!row[8].is_null())
    d.magneto_f = as_double(row[8]);
  if (!row[9].is_null())
    d.data_quality = as_int(row[9]);
  return d;
}

// Measurement parameter names for a given magnetometer level.
struct MagnetometerParamNames
{
  std::string x, y, z, t, f;
};

// Map a magnetometer level (10 / 60 / 110) to parameter names via the parameter map.
MagnetometerParamNames getMagnetometerParamNames(int level, const ParameterMapPtr &paramMap)
{
  std::string x_id = "MISSING", y_id = "MISSING", z_id = "MISSING";
  std::string t_id = "MISSING", f_id = "MISSING";
  switch (level)
  {
    case 10:
      x_id = "667";
      y_id = "669";
      z_id = "671";
      break;
    case 60:
      x_id = "668";
      y_id = "670";
      z_id = "672";
      t_id = "144";
      break;
    case 110:
      f_id = "673";
      break;
    default:
      break;
  }
  return {paramMap->getParameterName(x_id, MAGNETO_PRODUCER),
          paramMap->getParameterName(y_id, MAGNETO_PRODUCER),
          paramMap->getParameterName(z_id, MAGNETO_PRODUCER),
          paramMap->getParameterName(t_id, MAGNETO_PRODUCER),
          paramMap->getParameterName(f_id, MAGNETO_PRODUCER)};
}

// Emit all timed values for one magnetometer row into the per-fmisid result vector.
void emitMagnetometerValues(TS::TimeSeriesVector &result,
                            const std::map<std::string, int> &positions,
                            const Fmi::LocalDateTime &localtime,
                            const MagnetometerRowData &row,
                            const MagnetometerParamNames &names,
                            const Spine::Station &s)
{
  auto emit = [&](const std::string &name, const TS::Value &value)
  {
    auto it = positions.find(name);
    if (it != positions.end())
      result[it->second].push_back(TS::TimedValue(localtime, value));
  };
  emit(names.x, row.magneto_x);
  emit(names.y, row.magneto_y);
  emit(names.z, row.magneto_z);
  emit(names.t, row.magneto_t);
  emit(names.f, row.magneto_f);
  emit("data_quality", row.data_quality);
  emit("fmisid", TS::Value{row.fmisid});
  emit("magnetometer_id", TS::Value{row.magnetometer_id});
  emit("stationlon", TS::Value{s.longitude});
  emit("stationlat", TS::Value{s.latitude});
  emit("elevation", TS::Value{s.elevation});
  emit("stationtype", TS::Value{s.type});
}

// Build the final result from per-fmisid time series, inserting None for missing timesteps.
void fillMagnetometerTimestepData(
    const std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results,
    const std::map<int, std::set<Fmi::LocalDateTime>> &valid_timesteps_per_fmisid,
    const std::set<int> &data_independent_positions,
    TS::TimeSeriesVectorPtr &ret)
{
  for (const auto &item : fmisid_results)
  {
    const auto &valid_timesteps = valid_timesteps_per_fmisid.at(item.first);
    const auto &ts_vector = *item.second;
    for (unsigned int i = 0; i < ts_vector.size(); i++)
    {
      const auto &ts = ts_vector.at(i);
      std::map<Fmi::LocalDateTime, TS::TimedValue> data;
      for (unsigned int j = 0; j < ts.size(); j++)
        data.insert({ts.at(j).time, ts.at(j)});
      for (const auto &timestep : valid_timesteps)
      {
        auto it = data.find(timestep);
        if (it != data.end())
          ret->at(i).push_back(it->second);
        else if (!ts.empty() && data_independent_positions.count(i))
          ret->at(i).push_back(ts.at(0));
        else
          ret->at(i).push_back(TS::TimedValue(timestep, TS::None()));
      }
    }
  }
}

}  // namespace

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
  // We let the PG connection pool print just one message
  // std::cout << "  -- Shutdown requested (PostgreSQL)\n";
  itsDB.cancel();
}

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getObservationDataForMovingStations(
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
    for (const auto &item : observations)
    {
      Spine::Station station;
      station.fmisid = item.data.fmisid;
      station.longitude = item.longitude;
      station.latitude = item.latitude;
      station.elevation = item.elevation;
      station.type = item.stationtype;
      fmisid_to_station[station.fmisid] = station;
    }

    return buildTimeSeriesFromObservations(observations,
                                           settings,
                                           settings.stationtype,
                                           fmisid_to_station,
                                           qmap,
                                           timeSeriesOptions,
                                           timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Fetching data from PostgreSQL database failed!");
  }
}

TS::TimeSeriesVectorPtr CommonPostgreSQLFunctions::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> & /* observationMemoryCache */)
{
  try
  {
    // Producer 'fmi' is deprecated
    std::string stationtype = settings.stationtype;
    if (stationtype == "fmi")
      stationtype = "observations_fmi";

    // This maps measurand_id and the parameter position in TimeSeriesVector
    auto qmap = buildQueryMapping(settings, stationtype, false);

    // Resolve stationgroup codes
    std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet = itsStationtypeConfig.getGroupCodeSetByStationtype(stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    LocationDataItems observations =
        readObservationDataFromDB(stations, settings, stationInfo, qmap, stationgroup_codes);

    std::set<int> observed_fmisids;
    for (const auto &item : observations)
      observed_fmisids.insert(item.data.fmisid);

    // Map fmisid to station information
    StationMap fmisid_to_station = mapQueryStations(stations, observed_fmisids);

    return buildTimeSeriesFromObservations(
        observations, settings, stationtype, fmisid_to_station, qmap, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Fetching data from PostgreSQL database failed!");
  }
}

LocationDataItems CommonPostgreSQLFunctions::readObservationDataOfMovingStationsFromDB(
    const Settings &settings,
    const QueryMapping &qmap,
    const std::set<std::string> & /* stationgroup_codes */) const
{
  try
  {
    LocationDataItems ret;

    auto wktString = settings.wktArea;
    if (wktString.empty() && !settings.boundingBox.empty())
      wktString = ("POLYGON((" + Fmi::to_string(settings.boundingBox.at("minx")) + " " +
                   Fmi::to_string(settings.boundingBox.at("miny")) + "," +
                   Fmi::to_string(settings.boundingBox.at("minx")) + " " +
                   Fmi::to_string(settings.boundingBox.at("maxy")) + "," +
                   Fmi::to_string(settings.boundingBox.at("maxx")) + " " +
                   Fmi::to_string(settings.boundingBox.at("maxy")) + "," +
                   Fmi::to_string(settings.boundingBox.at("maxx")) + " " +
                   Fmi::to_string(settings.boundingBox.at("miny")) + "," +
                   Fmi::to_string(settings.boundingBox.at("minx")) + " " +
                   Fmi::to_string(settings.boundingBox.at("miny")) + "))");

    // Safety check
    if (qmap.measurandIds.empty())
      return ret;

    const std::string measurand_ids = Fmi::join(qmap.measurandIds);
    const std::string producerIds = Fmi::join(settings.producer_ids);
    const std::string fmisids =
        Fmi::join(settings.taggedFMISIDs, [](const auto &value) { return value.fmisid; });

    if (fmisids.empty() && wktString.empty())
      throw Fmi::Exception::Trace(
          BCP, "Fetching data from PostgreSQL database failed, no fmisids or area given!");

    const std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
    const std::string endtime = Fmi::to_iso_extended_string(settings.endtime);
    const std::string sensorCondition = getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    const std::string qualityClause =
        settings.dataFilter.getSqlClause("data_quality", "data.data_quality");

    const std::string sqlStmt = itsIsCacheDatabase ? buildMovingStationsCacheSQL(starttime,
                                                                                 endtime,
                                                                                 measurand_ids,
                                                                                 fmisids,
                                                                                 producerIds,
                                                                                 sensorCondition,
                                                                                 qualityClause,
                                                                                 wktString)
                                                   : buildMovingStationsDirectSQL(starttime,
                                                                                  endtime,
                                                                                  measurand_ids,
                                                                                  fmisids,
                                                                                  producerIds,
                                                                                  sensorCondition,
                                                                                  qualityClause,
                                                                                  wktString);

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt << '\n';

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    for (const auto &row : result_set)
      ret.emplace_back(parseMovingStationObservation(row));

    return ret;
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

    std::string measurand_ids = Fmi::join(qmap.measurandIds);

    auto station_ids =
        buildStationList(stations, stationgroup_codes, stationInfo, settings.requestLimits);

    if (station_ids.empty())
      return ret;

    // Generate comma separated values
    std::string stationsql = Fmi::join(station_ids);

    std::string producerIds = Fmi::join(settings.producer_ids);

    std::string starttime = Fmi::to_iso_extended_string(settings.starttime);
    std::string endtime = Fmi::to_iso_extended_string(settings.endtime);

    // Determine table and columns based on database type
    std::string tableName = itsIsCacheDatabase ? "observation_data" : "observation_data_r1";
    std::string idColumn = itsIsCacheDatabase ? "fmisid" : "station_id";
    std::string stationColumn = itsIsCacheDatabase ? "fmisid" : "station_id AS fmisid";
    std::string timestampColumn =
        itsIsCacheDatabase ? "data.data_time" : "date_trunc('seconds', data.data_time)";

    // Construct base SQL statement
    std::string sqlStmt =
        "SELECT data." + stationColumn + ", data.sensor_no AS sensor_no, EXTRACT(EPOCH FROM " +
        timestampColumn +
        ") AS obstime, data.measurand_id, data_value, data_quality, data_source FROM " + tableName +
        " data ";

    // Using JOIN on large station lists may be faster than just using an IN clause.
    // PostgreSQL converts the IN clause into a "if x=A or x=B or x=C ..." clause, so
    // wrapping even a single value into IN() does not slow things up.

    if (station_ids.size() < 10)
      sqlStmt += "WHERE " + idColumn + " IN (" + stationsql + ")";
    else
      sqlStmt +=
          "JOIN observations_v2 o ON (o.observation_id = data.observation_id ) "
          "WHERE o." +
          idColumn + " IN (" + stationsql + ")";

    sqlStmt += " AND data.data_time >= '" + starttime + "' AND data.data_time <= '" + endtime +
               "' AND data.measurand_id IN (" + measurand_ids + ") ";

    // Add producer ID filter if needed
    if (!producerIds.empty())
      sqlStmt += "AND data.producer_id IN (" + producerIds + ") ";

    // Add sensor query condition and data quality filter
    sqlStmt += getSensorQueryCondition(qmap.sensorNumberToMeasurandIds);
    sqlStmt += "AND " + settings.dataFilter.getSqlClause("data_quality", "data.data_quality") + " ";

    // Add ordering clause
    sqlStmt += "ORDER BY fmisid ASC, obstime ASC";

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt << '\n';

    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);

    std::set<Fmi::DateTime> obstimes;
    std::set<int> fmisids;
    for (auto row : result_set)
    {
      LocationDataItem obs;
      obs.data.fmisid = as_int(row[0]);
      obs.data.sensor_no = as_int(row[1]);
      obs.data.data_time = Fmi::date_time::from_time_t(row[2].as<time_t>());
      obs.data.measurand_id = as_int(row[3]);
      if (!row[4].is_null())
        obs.data.data_value = as_double(row[4]);
      if (!row[5].is_null())
        obs.data.data_quality = as_int(row[5]);
      if (!row[6].is_null())
        obs.data.data_source = as_int(row[6]);
      try
      {
        // Get latitude, longitude, elevation from station info. Databases may contain data values
        // outside the validity range of the station (according to the metadata), then we just
        // omit the coordinates etc.
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
      obstimes.insert(obs.data.data_time);
      fmisids.insert(obs.data.fmisid);

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

    std::string starttimeString = Fmi::to_iso_extended_string(settings.starttime);
    boost::replace_all(starttimeString, ",", ".");
    std::string endtimeString = Fmi::to_iso_extended_string(settings.endtime);
    boost::replace_all(endtimeString, ",", ".");

    const std::string sqlStmt = buildFlashSQL(itsIsCacheDatabase,
                                              param,
                                              starttimeString,
                                              endtimeString,
                                              settings.taggedLocations,
                                              settings.boundingBox);

    if (itsDebug)
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt << '\n';

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    double longitude = std::numeric_limits<double>::max();
    double latitude = std::numeric_limits<double>::max();
    pqxx::result result_set = itsDB.executeNonTransaction(sqlStmt);
    std::set<std::string> locations;
    std::set<Fmi::DateTime> obstimes;
    size_t n_elements = 0;
    for (auto row : result_set)
    {
      std::map<std::string, TS::Value> result;
      Fmi::DateTime stroke_time = Fmi::date_time::from_time_t(row[0].as<time_t>());
      // int stroke_time_fraction = as_int(row[1]);
      TS::Value flashIdValue = as_int(row[2]);
      result["flash_id"] = flashIdValue;
      longitude = Fmi::stod(row[3].as<std::string>());
      latitude = Fmi::stod(row[4].as<std::string>());
      // Rest of the parameters in requested order
      for (int i = 5; i != int(row.size()); ++i)
      {
        auto fld = row[i];
        result[fld.name()] = parseFlashFieldValue(fld, itsPostgreDataTypes.at(fld.type()));
      }

      auto localtz = timezones.time_zone_from_string(settings.timezone);
      Fmi::LocalDateTime localtime(stroke_time, localtz);

      for (const auto &p : timeseriesPositions)
        timeSeriesColumns->at(p.second).push_back(TS::TimedValue(localtime, result[p.first]));
      for (const auto &p : specialPositions)
      {
        if (p.first == "latitude")
          timeSeriesColumns->at(p.second).push_back(TS::TimedValue(localtime, TS::Value{latitude}));
        if (p.first == "longitude")
          timeSeriesColumns->at(p.second).push_back(
              TS::TimedValue(localtime, TS::Value{longitude}));
      }

      n_elements += timeSeriesColumns->size();
      locations.insert(Fmi::to_string(longitude) + Fmi::to_string(latitude));
      obstimes.insert(stroke_time);

      check_request_limit(
          settings.requestLimits, locations.size(), TS::RequestLimitMember::LOCATIONS);
      check_request_limit(
          settings.requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
      check_request_limit(settings.requestLimits, n_elements, TS::RequestLimitMember::ELEMENTS);
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting flash data from PostgreSQL database failed!");
  }
}

FlashCounts CommonPostgreSQLFunctions::getFlashCount(const Fmi::DateTime &startt,
                                                     const Fmi::DateTime &endt,
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
          std::string circleWkt = Fmi::OGR::exportToWkt(*circle);

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
      std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt << '\n';

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
    const Spine::Stations & /* stations */,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
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

  // Resolve stationgroup codes
  std::set<std::string> stationgroup_codes;
  auto stationgroupCodeSet =
      itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
  stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

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
        name == "stationlat" || name == "elevation")
      data_independent_positions.insert(pos);

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
    std::cout << (itsIsCacheDatabase ? "PostgreSQL(cache): " : "PostgreSQL: ") << sqlStmt << '\n';

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

  for (auto row : result_set)
  {
    const auto rowData = parseMagnetometerRow(row);
    if (fmisid_results.find(rowData.fmisid) == fmisid_results.end())
    {
      fmisid_results.insert({rowData.fmisid, initializeResultVector(settings)});
      fmisid_timesteps.insert({rowData.fmisid, std::set<Fmi::LocalDateTime>()});
    }
    Fmi::LocalDateTime localtime(rowData.data_time, localtz);
    const Spine::Station &s =
        stationInfo.getStation(rowData.fmisid, stationgroup_codes, rowData.data_time);
    const auto names = getMagnetometerParamNames(rowData.level, itsParameterMap);
    emitMagnetometerValues(
        *fmisid_results[rowData.fmisid], timeseriesPositions, localtime, rowData, names, s);
    fmisid_timesteps[rowData.fmisid].insert(localtime);
  }

  const auto valid_timesteps_per_fmisid =
      getValidTimeSteps(settings, timeSeriesOptions, timezones, fmisid_results);

  fillMagnetometerTimestepData(
      fmisid_results, valid_timesteps_per_fmisid, data_independent_positions, ret);

  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
