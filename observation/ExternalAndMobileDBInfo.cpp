#include "ExternalAndMobileDBInfo.h"
#include <boost/algorithm/string/predicate.hpp>
#include <macgyver/StringConversion.h>
#include <iostream>

namespace SmartMet
{
using TS::DataFilter;

namespace Engine
{
namespace Observation
{
namespace
{
void add_where_conditions(std::string &sqlStmt,
                          const std::string &producer,
                          const std::vector<int> &measurandIds,
                          const Fmi::DateTime &starttime,
                          const Fmi::DateTime &endtime,
                          const std::string &wktAreaFilter,
                          const DataFilter &dataFilter)
{
  if (!wktAreaFilter.empty() && producer != FMI_IOT_PRODUCER)
  {
    sqlStmt += " AND ST_Contains(ST_GeomFromText('";
    sqlStmt += wktAreaFilter;
    sqlStmt += ("', 4326), " +
                std::string(((producer == NETATMO_PRODUCER || producer == BK_HYDROMETA_PRODUCER)
                                 ? "stat.geom)"
                                 : "obs.geom)")));
  }

  if (!measurandIds.empty())
  {
    sqlStmt += " AND obs.mid IN (";
    std::string mids;
    for (auto i : measurandIds)
    {
      if (!mids.empty())
        mids += ',';
      mids += std::to_string(i);
    }
    sqlStmt += (mids + ") ");
  }

  if (!starttime.is_not_a_date_time())
  {
    sqlStmt += " AND obs.data_time>='" + Fmi::to_iso_extended_string(starttime) + "'";
  }
  if (!endtime.is_not_a_date_time())
  {
    sqlStmt += " AND obs.data_time<='" + Fmi::to_iso_extended_string(endtime) + "'";
  }

  if (dataFilter.exist("station_id"))
    sqlStmt += " AND " + dataFilter.getSqlClause("station_id", "obs.station_id");
  if (dataFilter.exist("data_quality"))
    sqlStmt += " AND " + dataFilter.getSqlClause("data_quality", "obs.data_quality");

  boost::algorithm::replace_all(sqlStmt, "WHERE AND", "WHERE");
}

}  // namespace

ExternalAndMobileDBInfo::ExternalAndMobileDBInfo(
    const ExternalAndMobileProducerConfigItem *producerConfig /*= nullptr*/)
    : itsProducerConfig(producerConfig)
{
}

std::string ExternalAndMobileDBInfo::sqlSelect(const std::vector<int> &measurandIds,
                                               const Fmi::DateTime &starttime,
                                               const Fmi::DateTime &endtime,
                                               const std::vector<std::string> &station_ids,
                                               const DataFilter &dataFilter) const
{
  std::string sqlStmt;

  if (!itsProducerConfig)
    return "";

  std::string producerName = itsProducerConfig->producerId().name();
  std::string producerId = itsProducerConfig->producerId().asString();
  if (producerName == FMI_IOT_PRODUCER)
  {
    sqlStmt = "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level";
    for (auto mid : measurandIds)
    {
      sqlStmt += ", MAX(CASE WHEN obs.mid=";
      sqlStmt += Fmi::to_string(mid);
      sqlStmt += " THEN obs.data_value END) AS ";
      sqlStmt += measurandFieldname(mid);
    }
    sqlStmt +=
        (", obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value_txt, "
         "obs.data_quality, obs.ctrl_status, MAX(EXTRACT(EPOCH FROM obs.created)) as created, "
         "stat.station_code FROM " +
         itsProducerConfig->databaseTable() +
         " obs, ext_station_v1 stat WHERE "
         "obs.prod_id=stat.prod_id and "
         "obs.station_id=stat.station_id and obs.prod_id=");
    sqlStmt += producerId;
    if (!station_ids.empty())
    {
      std::string requested_stations;
      for (auto const &s : station_ids)
      {
        if (!requested_stations.empty())
          requested_stations += ", ";
        requested_stations += ("'" + s + "'");
      }

      sqlStmt += " AND stat.station_code IN (" + requested_stations + ") ";
    }
  }
  else
  {
    throw Fmi::Exception(BCP, "SQL select not defined for producer " + producerName);
  }

  add_where_conditions(sqlStmt, producerName, measurandIds, starttime, endtime, "", dataFilter);

  sqlStmt +=
      " GROUP BY "
      "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,obs."
      "data_value_txt,obs.data_quality,obs.ctrl_status,stat."
      "station_id, stat.station_code ORDER BY obs.data_time, stat.station_id ASC";

  // std::cout << "SQL: " << sqlStmt << std::endl;

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelect(const std::vector<int> &measurandIds,
                                               const Fmi::DateTime &starttime,
                                               const Fmi::DateTime &endtime,
                                               const std::string &wktAreaFilter,
                                               const DataFilter &dataFilter) const
{
  std::string sqlStmt;

  if (!itsProducerConfig)
    return "";

  std::string producerName = itsProducerConfig->producerId().name();
  std::string producerId = itsProducerConfig->producerId().asString();
  if (producerName == ROADCLOUD_PRODUCER)
  {
    sqlStmt = "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level";
    for (auto mid : measurandIds)
    {
      sqlStmt += ", MAX(CASE WHEN obs.mid=";
      sqlStmt += Fmi::to_string(mid);
      sqlStmt += " THEN obs.data_value END) AS ";
      sqlStmt += measurandFieldname(mid);
    }

    sqlStmt +=
        (", obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, MAX(EXTRACT(EPOCH FROM "
         "obs.created)) as created, ST_X(obs.geom) as longitude, "
         "ST_Y(obs.geom) as latitude, altitude FROM " +
         itsProducerConfig->databaseTable() + " obs WHERE obs.prod_id=");
    sqlStmt += producerId;
  }
  else if (producerName == NETATMO_PRODUCER || producerName == BK_HYDROMETA_PRODUCER)
  {
    sqlStmt = "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level";
    for (auto mid : measurandIds)
    {
      sqlStmt += ", MAX(CASE WHEN obs.mid=";
      sqlStmt += Fmi::to_string(mid);
      sqlStmt += " THEN obs.data_value END) AS ";
      sqlStmt += measurandFieldname(mid);
    }
    sqlStmt +=
        (", obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value_txt, "
         "obs.data_quality, obs.ctrl_status, MAX(EXTRACT(EPOCH FROM obs.created)) as created, "
         "ST_X(stat.geom) as longitude, ST_Y(stat.geom) as latitude, stat.altitude FROM " +
         itsProducerConfig->databaseTable() +
         " obs, ext_station_v1 stat WHERE obs.prod_id=stat.prod_id and "
         "obs.station_id=stat.station_id and obs.prod_id=");
    sqlStmt += producerId;
  }
  else
  {
    throw Fmi::Exception(BCP, "SQL select not defined for producer " + producerName);
  }

  add_where_conditions(
      sqlStmt, producerName, measurandIds, starttime, endtime, wktAreaFilter, dataFilter);

  if (producerName == ROADCLOUD_PRODUCER)
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,longitude,latitude,altitude,"
        "station_id ORDER BY obs.data_time, obs.station_id ASC";
  else if (producerName == NETATMO_PRODUCER || producerName == BK_HYDROMETA_PRODUCER)
  {
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,longitude,latitude,stat.altitude,stat."
        "station_id ORDER BY obs.data_time, stat.station_id ASC";
  }
  else if (producerName == FMI_IOT_PRODUCER)
  {
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,stat."
        "station_id, stat.station_code ORDER BY obs.data_time, stat.station_id ASC";
  }

  // std::cout << "SQL: " << sqlStmt << std::endl;

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelectForCache(
    const std::string &producer,
    const Fmi::DateTime &from_data_time,
    const Fmi::DateTime &from_created_time)
{
  std::string sqlStmt;
  std::string created_stmt;
  std::string tablename = "ext_obsdata";
  if (!from_created_time.is_not_a_date_time())
  {
    std::string timestamp = Fmi::to_iso_extended_string(from_created_time);
    boost::replace_last(timestamp, ",", ".");
    created_stmt = (" and obs.created>='" + timestamp + "'");
  }

  if (producer == ROADCLOUD_PRODUCER)
  {
    // Add data from ext_obsdata table
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(obs.geom) as longitude, ST_Y(obs.geom) as latitude, obs.altitude "
         "as altitude FROM ext_obsdata" +
         tablename + " obs WHERE obs.prod_id = 1 AND obs.data_time>='" +
         Fmi::to_iso_extended_string(from_data_time) + "'" + created_stmt);
  }
  else if (producer == NETATMO_PRODUCER || producer == BK_HYDROMETA_PRODUCER)
  {
    // Join ext_obsdata and ext_station_v1 tables
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(stat.geom) as longitude, ST_Y(stat.geom) as latitude, "
         "stat.altitude as altitude FROM " +
         tablename + " obs, ext_station_v1 stat WHERE obs.prod_id= " +
         (producer == NETATMO_PRODUCER ? "3 " : "7 ") +
         "AND obs.prod_id=stat.prod_id AND obs.station_id=stat.station_id AND obs.data_time>='" +
         Fmi::to_iso_extended_string(from_data_time) + "'" + created_stmt);
  }
  else if (producer == TECONER_PRODUCER)
  {
    // TBD
  }
  else if (producer == FMI_IOT_PRODUCER)
  {
    // Join ext_obsdata and ext_station_v1 tables
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, stat.station_code FROM " +
         tablename +
         " obs, ext_station_v1 stat WHERE "
         "obs.prod_id=4 "
         "AND obs.prod_id=stat.prod_id AND obs.station_id=stat.station_id AND obs.data_time>='" +
         Fmi::to_iso_extended_string(from_data_time) + "'" + created_stmt);
  }

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelectFromCache(const std::vector<int> &measurandIds,
                                                        const Fmi::DateTime &starttime,
                                                        const Fmi::DateTime &endtime,
                                                        const std::string &wktAreaFilter,
                                                        const DataFilter &dataFilter,
                                                        bool spatialite /* = false*/) const
{
  if (!itsProducerConfig)
    return "";

  std::string producerName = itsProducerConfig->producerId().name();

  if (producerName != NETATMO_PRODUCER && producerName != ROADCLOUD_PRODUCER &&
      producerName != TECONER_PRODUCER && producerName != FMI_IOT_PRODUCER &&
      producerName != BK_HYDROMETA_PRODUCER)
  {
    throw Fmi::Exception(BCP, "SQL select not defined for producer " + producerName);
  }

  std::string sqlStmt;

  if (producerName == FMI_IOT_PRODUCER)
  {
    sqlStmt =
        "SELECT obs.prod_id, obs.station_id, obs.station_code, obs.dataset_id, obs.data_level";
    if (spatialite)
      sqlStmt +=
          ", obs.sensor_no, obs.data_time as data_time, obs.data_value_txt, "
          "obs.data_quality, obs.ctrl_status, MAX(obs.created) as created ";
    else
      sqlStmt +=
          ", obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value_txt, "
          "obs.data_quality, obs.ctrl_status, MAX(EXTRACT(EPOCH FROM obs.created)) as created ";
  }
  else
  {
    sqlStmt = "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level";
    if (spatialite)
      sqlStmt +=
          ", obs.sensor_no, obs.data_time as data_time, obs.data_value_txt, "
          "obs.data_quality, obs.ctrl_status, MAX(obs.created) as created, "
          "ST_X(obs.geom) as longitude, ST_Y(obs.geom) as latitude, obs.altitude ";
    else
      sqlStmt +=
          ", obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value_txt, "
          "obs.data_quality, obs.ctrl_status, MAX(EXTRACT(EPOCH FROM obs.created)) as created, "
          "ST_X(obs.geom) as longitude, ST_Y(obs.geom) as latitude, obs.altitude ";
  }

  for (auto mid : measurandIds)
  {
    sqlStmt += ", MAX(CASE WHEN obs.mid=";
    sqlStmt += Fmi::to_string(mid);
    sqlStmt += " THEN obs.data_value END) AS ";
    sqlStmt += measurandFieldname(mid);
  }
  sqlStmt += " FROM ext_obsdata_";
  sqlStmt += producerName;
  sqlStmt += " obs WHERE";

  add_where_conditions(
      sqlStmt, producerName, measurandIds, starttime, endtime, wktAreaFilter, dataFilter);

  if (producerName == ROADCLOUD_PRODUCER)
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,"
        "obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,longitude,latitude,altitude,obs."
        "station_id ORDER BY obs.data_time, obs.station_id ASC";
  else if (producerName == NETATMO_PRODUCER || producerName == BK_HYDROMETA_PRODUCER)
  {
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.dataset_id,obs.data_level,obs.sensor_no,obs.data_time,"
        "obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,longitude,latitude,obs.altitude,obs."
        "station_id ORDER BY obs.data_time, obs.station_id ASC";
  }
  else if (producerName == FMI_IOT_PRODUCER)
  {
    sqlStmt +=
        " GROUP BY "
        "obs.prod_id,obs.station_id,obs.station_code,obs.dataset_id,obs.data_level,obs.sensor_no,"
        "obs.data_time,"
        "obs."
        "data_value_txt,obs.data_quality,obs.ctrl_status,obs."
        "station_id ORDER BY obs.data_time, obs.station_id ASC";
  }

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::measurandFieldname(int measurandId) const
{
  const std::string &producer = itsProducerConfig->producerId().name();

  if (producer == ROADCLOUD_PRODUCER)
  {
    switch (measurandId)
    {
      case 1:
        return "speed";
      case 2:
        return "friction";
      case 3:
        return "road_state";
      case 4:
        return "road_quality_z";
      case 5:
        return "road_quality_roll";
      case 6:
        return "road_quality_pitch";
      case 7:
        return "road_quality";
      case 8:
        return "water_accumulation";
      case 9:
        return "slippery_road";
      case 10:
        return "decreased_visibility";
      case 11:
        return "exceptional_weather";
      case 12:
        return "ABC_activation";
      case 13:
        return "ESC_activation";
      case 14:
        return "ASR_activation";
      case 15:
        return "emergency_light";
      case 16:
        return "traffic_congestion";
      case 17:
        return "";
      case 18:
        return "heading";
      case 19:
        return "rain_sensor";
      case 20:
        return "fog_light";
      case 21:
        return "windshield_wiper";
      case 22:
        return "x_acceleration";
      case 23:
        return "y_acceleration";
      case 24:
        return "z_acceleration";
      case 25:
        return "x_acceleration_variance";
      case 26:
        return "y_acceleration_variance";
      case 27:
        return "z_acceleration_variance";
      case 28:
        return "roll_rate";
      case 29:
        return "pitch_rate";
      case 30:
        return "yaw_rate";
      case 31:
        return "roll_rate_variance";
      case 32:
        return "pitch_rate_variance";
      case 33:
        return "yaw_rate_variance";
      case 34:
        return "ambient_temperature";
      case 35:
        return "";
      case 36:
        return "dry_time";
      default:
        return "";
    }
  }
  else if (producer == NETATMO_PRODUCER)
  {
    switch (measurandId)
    {
      case 37:
        return "temperature";
      case 38:
        return "humidity";
      case 39:
        return "pressure";
      case 40:
        return "rain";
      case 41:
        return "rain_sum";
      case 42:
        return "wind";
      case 43:
        return "wind_gust";
      case 44:
        return "wind_angle";
      case 45:
        return "gust_angle";
      default:
        return "";
    }
  }
  else if (producer == FMI_IOT_PRODUCER)
  {
    switch (measurandId)
    {
      case 49:
        return "rh";
      case 8164:
        return "pa";
      case 8165:
        return "ta";
      case 113093:
        return "t_ext2";
      case 113094:
        return "t_internal_tech";
      case 113095:
        return "v_bat";
      case 113096:
        return "t_ext";
      case 113097:
        return "solar_rad";
      case 113098:
        return "prec";
      case 113099:
        return "ws";
      case 113100:
        return "wd";
      case 113101:
        return "ws_max";
      case 113102:
        return "p0";
      case 113103:
        return "ws_n";
      case 113104:
        return "ws_e";
      default:
        return "";
    }
  }
  else if (producer == BK_HYDROMETA_PRODUCER)
  {
    switch (measurandId)
    {
      case 8185:
        return "WG";
      case 8186:
        return "PA";
      case 8187:
        return "P_ST";
      case 8188:
        return "WD";
      case 8189:
        return "WS";
      case 8190:
        return "PREC_24H";
      case 8191:
        return "RH";
      case 8192:
        return "TD";
      case 8193:
        return "TA";
      case 8194:
        return "PREC_1H";
      case 23240:
        return "relative_humidity";
      case 23241:
        return "wind_speed";
      case 23242:
        return "absolute_air_pressure";
      case 23243:
        return "wind_direction_compass";
      case 23244:
        return "global_radiation";
      case 23245:
        return "precipitation_type";
      case 23246:
        return "precipitation_intensity_h";
      case 23247:
        return "compass_direction";
      case 23248:
        return "air_temperature";
      case 23249:
        return "absolute_humidity";
      case 23250:
        return "wind_direction";
      case 23251:
        return "relative_air_pressure";
      case 23252:
        return "dewpoint_temperature";
      case 23253:
        return "precipitation";
      case 23254:
        return "precipitation_diff";
      default:
        return "";
    }
  }

  return "";
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
