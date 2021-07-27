#include "ExternalAndMobileDBInfo.h"
#include <boost/algorithm/string/predicate.hpp>
#include <macgyver/StringConversion.h>
#include <iostream>

namespace SmartMet
{
using Spine::DataFilter;

namespace Engine
{
namespace Observation
{
namespace
{
void add_where_conditions(std::string &sqlStmt,
                          const std::string &producer,
                          const std::vector<int> &measurandIds,
                          const boost::posix_time::ptime &starttime,
                          const boost::posix_time::ptime &endtime,
                          const std::string &wktAreaFilter,
                          const DataFilter &dataFilter)
{
  if (!wktAreaFilter.empty() && producer != FMI_IOT_PRODUCER)
  {
    sqlStmt += " AND ST_Contains(ST_GeomFromText('";
    sqlStmt += wktAreaFilter;
    sqlStmt +=
        ("', 4326), " + std::string(((producer == NETATMO_PRODUCER) ? "stat.geom)" : "obs.geom)")));
  }

  std::string mids;
  if (measurandIds.size() > 0)
  {
    sqlStmt += " AND obs.mid IN (";
    for (auto i : measurandIds)
    {
      if (mids.size() > 0)
        mids += ",";
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
    const ExternalAndMobileProducerMeasurand *producerMeasurand /*= nullptr*/)
    : itsProducerMeasurand(producerMeasurand)
{
}

std::string ExternalAndMobileDBInfo::sqlSelect(const std::vector<int> &measurandIds,
                                               const boost::posix_time::ptime &starttime,
                                               const boost::posix_time::ptime &endtime,
                                               const std::vector<std::string> &station_ids,
                                               const DataFilter &dataFilter) const
{
  std::string sqlStmt;

  if (!itsProducerMeasurand)
    return "";

  std::string producerName = itsProducerMeasurand->producerId().name();
  std::string producerId = itsProducerMeasurand->producerId().asString();
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
         itsDatabaseTableName +
         " obs, ext_station_v1 stat WHERE "
         "obs.prod_id=stat.prod_id and "
         "obs.station_id=stat.station_id and obs.prod_id=");
    sqlStmt += producerId;
    if (station_ids.size() > 0)
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
                                               const boost::posix_time::ptime &starttime,
                                               const boost::posix_time::ptime &endtime,
                                               const std::string &wktAreaFilter,
                                               const DataFilter &dataFilter) const
{
  std::string sqlStmt;

  if (!itsProducerMeasurand)
    return "";

  std::string producerName = itsProducerMeasurand->producerId().name();
  std::string producerId = itsProducerMeasurand->producerId().asString();
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
         itsDatabaseTableName + " obs WHERE obs.prod_id=");
    sqlStmt += producerId;
  }
  else if (producerName == NETATMO_PRODUCER)
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
         itsDatabaseTableName +
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
  else if (producerName == NETATMO_PRODUCER)
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
    const boost::posix_time::ptime &from_data_time,
    const boost::posix_time::ptime &from_created_time) const
{
  std::string sqlStmt;

  std::string created_stmt;
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
         "as altitude FROM " +
         itsDatabaseTableName + " obs WHERE obs.prod_id = 1 AND obs.data_time>='" +
         Fmi::to_iso_extended_string(from_data_time) + "'" + created_stmt);
  }
  else if (producer == NETATMO_PRODUCER)
  {
    // Join ext_obsdata and ext_station_v1 tables
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(stat.geom) as longitude, ST_Y(stat.geom) as latitude, "
         "stat.altitude as altitude FROM " +
         itsDatabaseTableName +
         " obs, ext_station_v1 stat WHERE obs.prod_id=3 "
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
         itsDatabaseTableName +
         " obs, ext_station_v1 stat WHERE "
         "obs.prod_id=4 "
         "AND obs.prod_id=stat.prod_id AND obs.station_id=stat.station_id AND obs.data_time>='" +
         Fmi::to_iso_extended_string(from_data_time) + "'" + created_stmt);
  }

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelectFromCache(const std::vector<int> &measurandIds,
                                                        const boost::posix_time::ptime &starttime,
                                                        const boost::posix_time::ptime &endtime,
                                                        const std::string &wktAreaFilter,
                                                        const DataFilter &dataFilter,
                                                        bool spatialite /* = false*/) const
{
  if (!itsProducerMeasurand)
    return "";

  std::string producerName = itsProducerMeasurand->producerId().name();

  if (producerName != NETATMO_PRODUCER && producerName != ROADCLOUD_PRODUCER &&
      producerName != TECONER_PRODUCER && producerName != FMI_IOT_PRODUCER)
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
  else if (producerName == NETATMO_PRODUCER)
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
  std::string ret = "";

  switch (measurandId)
  {
    case 1:
      ret = "speed";
      break;
    case 2:
      ret = "friction";
      break;
    case 3:
      ret = "road_state";
      break;
    case 4:
      ret = "road_quality_z";
      break;
    case 5:
      ret = "road_quality_roll";
      break;
    case 6:
      ret = "road_quality_pitch";
      break;
    case 7:
      ret = "road_quality";
      break;
    case 8:
      ret = "water_accumulation";
      break;
    case 9:
      ret = "slippery_road";
      break;
    case 10:
      ret = "decreased_visibility";
      break;
    case 11:
      ret = "exceptional_weather";
      break;
    case 12:
      ret = "ABC_activation";
      break;
    case 13:
      ret = "ESC_activation";
      break;
    case 14:
      ret = "ASR_activation";
      break;
    case 15:
      ret = "emergency_light";
      break;
    case 16:
      ret = "traffic_congestion";
      break;
    case 17:
      ret = "";
      break;
    case 18:
      ret = "heading";
      break;
    case 19:
      ret = "rain_sensor";
      break;
    case 20:
      ret = "fog_light";
      break;
    case 21:
      ret = "windshield_wiper";
      break;
    case 22:
      ret = "x_acceleration";
      break;
    case 23:
      ret = "y_acceleration";
      break;
    case 24:
      ret = "z_acceleration";
      break;
    case 25:
      ret = "x_acceleration_variance";
      break;
    case 26:
      ret = "y_acceleration_variance";
      break;
    case 27:
      ret = "z_acceleration_variance";
      break;
    case 28:
      ret = "roll_rate";
      break;
    case 29:
      ret = "pitch_rate";
      break;
    case 30:
      ret = "yaw_rate";
      break;
    case 31:
      ret = "roll_rate_variance";
      break;
    case 32:
      ret = "pitch_rate_variance";
      break;
    case 33:
      ret = "yaw_rate_variance";
      break;
    case 34:
      ret = "ambient_temperature";
      break;
    case 35:
      ret = "";
      break;
    case 36:
      ret = "dry_time";
      break;
    case 37:
      ret = "temperature";
      break;
    case 38:
      ret = "humidity";
      break;
    case 39:
      ret = "pressure";
      break;
    case 40:
      ret = "rain";
      break;
    case 41:
      ret = "rain_sum";
      break;
    case 42:
      ret = "wind";
      break;
    case 43:
      ret = "wind_gust";
      break;
    case 44:
      ret = "wind_angle";
      break;
    case 45:
      ret = "gust_angle";
      break;
    case 49:
      ret = "relative_humidity";
      break;
    case 8164:
      ret = "pressure";
      break;
    case 8165:
      ret = "temperature";
      break;
    default:
      break;
  }

  return ret;
}

void ExternalAndMobileDBInfo::setDatabaseTableName(const std::string &tablename)
{
  itsDatabaseTableName = tablename;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
