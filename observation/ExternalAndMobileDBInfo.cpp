#include "ExternalAndMobileDBInfo.h"
#include <boost/algorithm/string/predicate.hpp>
#include <macgyver/StringConversion.h>
#include <iostream>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
std::vector<std::string> field_names()
{
  std::vector<std::string> ret;

  ret.push_back("prod_id");
  ret.push_back("station_id");
  ret.push_back("dataset_id");
  ret.push_back("data_level");
  ret.push_back("mid");
  ret.push_back("sensor_no");
  ret.push_back("data_time");
  ret.push_back("data_value");
  ret.push_back("data_value_txt");
  ret.push_back("data_quality");
  ret.push_back("ctrl_status");
  ret.push_back("created");
  ret.push_back("longitude");
  ret.push_back("latitude");
  ret.push_back("altitude");

  return ret;
}

#ifdef __llvm__
#pragma clang diagnostic push
#endif

std::map<std::string, unsigned int> key_fields()
{
  std::map<std::string, unsigned int> ret;

  ret.insert(std::make_pair("data_time", 0));
  ret.insert(std::make_pair("prod_id", 1));
  ret.insert(std::make_pair("station_id", 2));
  ret.insert(std::make_pair("longitude", 3));
  ret.insert(std::make_pair("latitude", 4));
  ret.insert(std::make_pair("altitude", 5));

  return ret;
}

FieldType field_type(const std::string &fieldName)
{
  if (fieldName == "prod_id" || fieldName == "station_id" || fieldName == "data_level" ||
      fieldName == "mid" || fieldName == "sensor_no" || fieldName == "data_quality" ||
      fieldName == "ctrl_status")
    return FieldType::Integer;
  else if (fieldName == "dataset_id" || fieldName == "data_value_txt")
    return FieldType::String;
  else if (fieldName == "data_time" || fieldName == "created")
    return FieldType::DateTime;
  else if (fieldName == "data_value" || fieldName.find("altitude") != std::string::npos ||
           fieldName.find("latitude") != std::string::npos ||
           fieldName.find("longitude") != std::string::npos)
    return FieldType::Double;

  return FieldType::Unknown;
}

void add_where_conditions(std::string &sqlStmt,
                          const std::string &areaFilterField,
                          const std::vector<int> &measurandIds,
                          const boost::posix_time::ptime &starttime,
                          const boost::posix_time::ptime &endtime,
                          const std::string &wktAreaFilter,
                          const std::map<std::string, std::vector<std::string>> &data_filter)
{
  if (!wktAreaFilter.empty())
  {
    sqlStmt += " AND ST_Contains(ST_GeomFromText('";
    sqlStmt += wktAreaFilter;
    sqlStmt += ("', 4326), " + areaFilterField + ")");
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
    sqlStmt += " AND obs.data_time>='" + boost::posix_time::to_iso_extended_string(starttime) + "'";
  }
  if (!endtime.is_not_a_date_time())
  {
    sqlStmt += " AND obs.data_time<='" + boost::posix_time::to_iso_extended_string(endtime) + "'";
  }

  for (auto f : data_filter)
  {
    const std::string &fieldname = f.first;
    const std::vector<std::string> &fieldvalues = f.second;

    sqlStmt += " AND (";
    for (unsigned int i = 0; i < fieldvalues.size(); i++)
    {
      if (i > 0)
        sqlStmt += " OR ";
      sqlStmt += ("obs." + fieldname + "=" + fieldvalues.at(i));
    }
    sqlStmt += ")";
  }

  boost::algorithm::replace_all(sqlStmt, "WHERE AND", "WHERE");
}

}  // namespace

ExternalAndMobileDBInfo::ExternalAndMobileDBInfo(
    const ExternalAndMobileProducerMeasurand *producerMeasurand /*= nullptr*/)
    : itsProducerMeasurand(producerMeasurand)
{
  itsFieldNames = field_names();
  itsKeyFields = key_fields();
  for (const auto &f : itsFieldNames)
  {
    itsFieldDescription.push_back(FieldDescription(f, field_type(f)));
    itsFieldIndexes.insert(std::make_pair(f, itsFieldDescription.size() - 1));
  }
}

const std::vector<std::string> ExternalAndMobileDBInfo::fieldNames() const
{
  return itsFieldNames;
}

const std::map<std::string, unsigned int> &ExternalAndMobileDBInfo::fieldIndexes() const
{
  return itsFieldIndexes;
}

const FieldDescription &ExternalAndMobileDBInfo::fieldDescription(
    const std::string &fieldName) const
{
  if (itsFieldIndexes.find(fieldName) != itsFieldIndexes.end())
  {
    unsigned int index = itsFieldIndexes.at(fieldName);
    return itsFieldDescription[index];
  }

  return itsEmptyField;
}

std::set<std::string> ExternalAndMobileDBInfo::keyFields() const
{
  std::set<std::string> ret;

  for (auto item : itsKeyFields)
    ret.insert(item.first);

  return ret;
}

bool ExternalAndMobileDBInfo::isKeyField(const std::string &fieldName) const
{
  return itsKeyFields.find(fieldName) != itsKeyFields.end();
}

const std::map<std::string, unsigned int> &ExternalAndMobileDBInfo::keyFieldIndexes() const
{
  return itsKeyFields;
}

FieldType ExternalAndMobileDBInfo::fieldType(const std::string &fieldName) const
{
  if (itsFieldIndexes.find(fieldName) != itsFieldIndexes.end())
  {
    unsigned int index = itsFieldIndexes.at(fieldName);
    return itsFieldDescription[index].field_type;
  }

  return FieldType::Unknown;
}

std::string ExternalAndMobileDBInfo::sqlSelect(
    const std::vector<int> &measurandIds,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &wktAreaFilter,
    const std::map<std::string, std::vector<std::string>> &data_filter) const
{
  std::string sqlStmt;

  if (!itsProducerMeasurand)
    return "";

  std::string producerName = itsProducerMeasurand->producerId().name();
  std::string producerId = itsProducerMeasurand->producerId().asString();
  if (producerName == ROADCLOUD_PRODUCER)
  {
    sqlStmt =
        ("SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, "
         "EXTRACT(EPOCH FROM obs.created) as created, ST_X(obs.geom) as longitude, "
         "ST_Y(obs.geom) "
         "as latitude, altitude FROM ext_obsdata obs WHERE obs.prod_id=" +
         producerId);
  }
  else if (producerName == NETATMO_PRODUCER)
  {
    sqlStmt =
        ("SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(stat.geom) as longitude, ST_Y(stat.geom) as latitude, stat.altitude "
         "FROM "
         "ext_obsdata obs, ext_station_v1 stat WHERE obs.prod_id=stat.prod_id and "
         "obs.station_id=stat.station_id and obs.prod_id=" +
         producerId);
  }
  else
  {
    throw SmartMet::Spine::Exception(BCP, "SQL select not defined for producer " + producerName);
  }

  add_where_conditions(sqlStmt,
                       (producerName == NETATMO_PRODUCER ? "stat.geom" : "obs.geom"),
                       measurandIds,
                       starttime,
                       endtime,
                       wktAreaFilter,
                       data_filter);

  if (producerName == ROADCLOUD_PRODUCER)
    sqlStmt += " ORDER BY obs.data_time, obs.station_id, obs.mid ASC";
  else if (producerName == NETATMO_PRODUCER)
    sqlStmt += " ORDER BY obs.data_time, stat.station_id, obs.mid ASC";

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelectForCache(
    const std::string &producer, const boost::posix_time::ptime &starttime) const
{
  std::string sqlStmt;

  if (producer == ROADCLOUD_PRODUCER)
  {
    // Add data from ext_obsdata table
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(obs.geom) as longitude, ST_Y(obs.geom) as latitude, obs.altitude "
         "as altitude FROM ext_obsdata obs WHERE obs.prod_id = 1 AND obs.data_time>='" +
         boost::posix_time::to_iso_extended_string(starttime) + "' ");
  }
  else if (producer == NETATMO_PRODUCER)
  {
    // Join ext_obsdata and ext_station_v1 tables
    sqlStmt =
        ("select obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
         ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
         "obs.data_value_txt, obs.data_quality, obs.ctrl_status, EXTRACT(EPOCH FROM obs.created) "
         "as created, ST_X(stat.geom) as longitude, ST_Y(stat.geom) as latitude, "
         "stat.altitude as altitude FROM ext_obsdata obs, ext_station_v1 stat WHERE obs.prod_id=3 "
         "AND obs.prod_id=stat.prod_id AND obs.station_id=stat.station_id AND obs.data_time>='" +
         boost::posix_time::to_iso_extended_string(starttime) + "' ");
  }
  else if (producer == TECONER_PRODUCER)
  {
    // TBD
  }

  return sqlStmt;
}

std::string ExternalAndMobileDBInfo::sqlSelectFromCache(
    const std::vector<int> &measurandIds,
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &wktAreaFilter,
    const std::map<std::string, std::vector<std::string>> &data_filter,
    bool spatialite /* = false*/) const
{
  if (!itsProducerMeasurand)
    return "";

  std::string producerName = itsProducerMeasurand->producerId().name();

  if (producerName != NETATMO_PRODUCER && producerName != ROADCLOUD_PRODUCER &&
      producerName != TECONER_PRODUCER)
  {
    throw SmartMet::Spine::Exception(BCP, "SQL select not defined for producer " + producerName);
  }

  std::string sqlStmt;

  if (spatialite)
    sqlStmt +=
        "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
        ",obs.sensor_no, obs.data_time as data_time, obs.data_value, "
        "obs.data_value_txt, obs.data_quality, obs.ctrl_status, "
        "obs.created as created, ST_X(obs.geom) as longitude, "
        "ST_Y(obs.geom) as latitude, obs.altitude FROM ext_obsdata_";
  else
    sqlStmt +=
        "SELECT obs.prod_id, obs.station_id, obs.dataset_id, obs.data_level, obs.mid "
        ",obs.sensor_no, EXTRACT(EPOCH FROM obs.data_time) as data_time, obs.data_value, "
        "obs.data_value_txt, obs.data_quality, obs.ctrl_status, "
        "EXTRACT(EPOCH FROM obs.created) as created, ST_X(obs.geom) as longitude, "
        "ST_Y(obs.geom) as latitude, obs.altitude FROM ext_obsdata_";

  sqlStmt += producerName;
  sqlStmt += " obs WHERE";

  add_where_conditions(
      sqlStmt, "obs.geom", measurandIds, starttime, endtime, wktAreaFilter, data_filter);

  sqlStmt += " ORDER BY obs.data_time, obs.mid ASC";

  return sqlStmt;
}

boost::posix_time::ptime ExternalAndMobileDBInfo::epoch2ptime(double epoch)
{
  boost::posix_time::ptime ret =
      boost::posix_time::from_time_t(static_cast<std::time_t>(floor(epoch)));
  ret += boost::posix_time::microseconds(static_cast<long>((epoch - floor(epoch)) * 1000000));
  return ret;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
