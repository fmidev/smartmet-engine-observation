#include "QueryExternalAndMobileData.h"
#include "ExternalAndMobileDBInfo.h"
#include "PostgreSQLCacheDB.h"
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <timeseries/TimeSeriesInclude.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace std;

QueryExternalAndMobileData::QueryExternalAndMobileData(
    const ExternalAndMobileProducerConfig &producerConfig,
    const std::shared_ptr<FmiIoTStations> &stations)
    : itsProducerConfig(producerConfig), itsStations(stations)
{
}

QueryExternalAndMobileData::~QueryExternalAndMobileData() = default;

class my_visitor : public boost::static_visitor<double>
{
 public:
  my_visitor() = default;
  double operator()(TS::None /* none */) { return static_cast<double>(kFloatMissing); }
  double operator()(double luku) { return luku; }
  double operator()(const std::string & /* s */) { return static_cast<double>(kFloatMissing); }
  double operator()(int i) { return static_cast<double>(i); }
  double operator()(const Fmi::LocalDateTime & /* i */)
  {
    return static_cast<double>(kFloatMissing);
  }
  double operator()(const TS::LonLat & /* i */) { return static_cast<double>(kFloatMissing); }
};

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::executeQuery(PostgreSQLObsDB &db,
                                                                 const Settings& settings,
                                                                 const Fmi::TimeZones &timezones)
{
  try
  {
    TS::TimeSeriesGeneratorOptions timeSeriesOptions;
    timeSeriesOptions.startTime = db.startTime;
    timeSeriesOptions.endTime = db.endTime;
    timeSeriesOptions.timeStep = settings.timestep;

    timeSeriesOptions.startTimeUTC = false;
    timeSeriesOptions.endTimeUTC = false;

    return executeQuery(db, settings, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::values(PostgreSQLObsDB &db,
                                                           const Settings& settings,
                                                           const Fmi::TimeZones &timezones)
{
  try
  {
    return executeQuery(db, settings, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::values(
    PostgreSQLObsDB &db,
    const Settings& settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    return executeQuery(db, settings, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

namespace
{
// True for producers whose station coordinates come from the FmiIoT station list
bool isIoTProducer(const std::string &stationtype)
{
  return (stationtype == FMI_IOT_PRODUCER) || (stationtype == TAPSI_QC_PRODUCER);
}

// Station coordinates of an FmiIoT or TapsiQc observation
struct StationCoordinates
{
  std::optional<double> longitude;
  std::optional<double> latitude;
  std::optional<double> elevation;
};

// Build the SQL statement, or return nothing if no stations are in the requested area
std::optional<std::string> buildSqlStatement(const Settings &settings,
                                             const ExternalAndMobileDBInfo &dbInfo,
                                             const std::vector<int> &measurandIds,
                                             const FmiIoTStations &stations)
{
  if (!isIoTProducer(settings.stationtype))
    return dbInfo.sqlSelect(measurandIds,
                            settings.starttime,
                            settings.endtime,
                            settings.wktArea,
                            settings.dataFilter);

  std::vector<const FmiIoTStation *> validStations = stations.getStations(settings.wktArea);

  if (!settings.wktArea.empty() && validStations.empty())
    return {};

  std::vector<std::string> station_ids;
  for (const auto &item : validStations)
  {
    if (settings.stationtype_specifier == "itmf")
    {
      if (item->target_group_id == 1201)
        station_ids.push_back(item->station_id);
      continue;
    }

    station_ids.push_back(item->station_id);
  }

  return dbInfo.sqlSelect(
      measurandIds, settings.starttime, settings.endtime, station_ids, settings.dataFilter);
}

// Coordinates of the station of an FmiIoT or TapsiQc observation, if the station is active
StationCoordinates stationCoordinates(const Settings &settings,
                                      ResultSetRow &rsr,
                                      const Fmi::LocalDateTime &obstime,
                                      const FmiIoTStations &stations)
{
  StationCoordinates coords;
  if (isIoTProducer(settings.stationtype))
  {
    std::string station_code = *(std::get_if<std::string>(&rsr["station_code"]));

    if (stations.isActive(station_code, obstime.utc_time()))
    {
      const FmiIoTStation &s = stations.getStation(station_code, obstime.utc_time());
      coords.longitude = s.longitude;
      coords.latitude = s.latitude;
      if (s.elevation >= 0.0)
        coords.elevation = s.elevation;
    }
  }
  return coords;
}

// Value of a coordinate field of an FmiIoT or TapsiQc observation
TS::Value coordinateValue(const std::string &fieldname, const StationCoordinates &coords)
{
  TS::Value value;
  if (fieldname == "longitude" && coords.longitude)
  {
    value = *coords.longitude;
  }
  else if (fieldname == "latitude" && coords.latitude)
  {
    value = *coords.latitude;
  }
  // BUG ??? elevation is configurable (null op test), altitude is not
  else if ((fieldname == "altitude" || fieldname == "elevation") && coords.elevation)
  {
    value = *coords.elevation;
  }
  return value;
}

// Name of the result set column holding the given query field
std::string resultFieldname(const std::string &queryfield,
                            PostgreSQLObsDB &db,
                            const ExternalAndMobileProducerConfigItem &producerConfig,
                            const ExternalAndMobileDBInfo &dbInfo)
{
  const Measurands &measurands = producerConfig.measurands();
  std::string fieldname = queryfield;
  if (measurands.find(fieldname) == measurands.end())
  {
    const auto iter = db.getParameterMap()->find(fieldname);
    if (iter != db.getParameterMap()->end())
    {
      std::string producer = producerConfig.producerId().name();
      if (iter->second.find(producer) != iter->second.end())
      {
        fieldname = iter->second.at(producer);
      }
    }
  }
  else
  {
    fieldname = dbInfo.measurandFieldname(measurands.at(fieldname));
  }
  return fieldname;
}
}  // namespace

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::executeQuery(
    PostgreSQLObsDB &db,
    const Settings& settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    TS::TimeSeriesVectorPtr ret = std::make_shared<TS::TimeSeriesVector>();
    const ExternalAndMobileProducerConfigItem &producerConfig =
        itsProducerConfig.at(settings.stationtype);
    ExternalAndMobileDBInfo dbInfo(&producerConfig);
    std::vector<std::string> queryfields;
    std::vector<int> measurandIds;
    const Measurands &measurands = producerConfig.measurands();
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = Fmi::ascii_tolower_copy(p.name());
      queryfields.push_back(name);
      if (measurands.find(name) != measurands.end())
      {
        measurandIds.push_back(measurands.at(name));
      }
    }

    auto sqlStatement = buildSqlStatement(settings, dbInfo, measurandIds, *itsStations);
    if (!sqlStatement)
      return ret;
    const std::string &sqlStmt = *sqlStatement;

    if (settings.debug_options)
      std::cout << "PostgreSQL: " << sqlStmt << '\n';

    // Execute SQL statement
    Fmi::Database::PostgreSQLConnection &conn = db.getConnection();
    pqxx::result result_set = conn.executeNonTransaction(sqlStmt);

    for (unsigned int i = 0; i <= queryfields.size(); i++)
      ret->emplace_back();

    TS::TimeSeriesGenerator::LocalTimeList tlist;
    // The desired timeseries, unless all available data if timestep=0 or a specific time only
    if (!settings.wantedtime && !timeSeriesOptions.all())
    {
      tlist = TS::TimeSeriesGenerator::generate(timeSeriesOptions,
                                                timezones.time_zone_from_string(settings.timezone));
    }
    ResultSetRows rsrs =
        PostgreSQLCacheDB::getResultSetForMobileExternalData(result_set, conn.dataTypes());

    std::set<std::string> locations;
    std::set<Fmi::LocalDateTime> obstimes;
    size_t n_elements = 0;
    for (auto rsr : rsrs)
    {
      // FIXME: may dereference nullptr
      Fmi::LocalDateTime obstime = *(std::get_if<Fmi::LocalDateTime>(&rsr["data_time"]));

      const auto coords = stationCoordinates(settings, rsr, obstime, *itsStations);

      unsigned int index = 0;
      for (const auto &fieldname : queryfields)
      {
        if (fieldname == "created")
        {
          Fmi::LocalDateTime dt = std::get<Fmi::LocalDateTime>(rsr[fieldname]);
          std::string fieldValue = db.getTimeFormatter()->format(dt);
          ret->at(index).emplace_back(TS::TimedValue(obstime, fieldValue));
        }
        else if (isIoTProducer(settings.stationtype) &&
                 (fieldname == "longitude" || fieldname == "latitude" || fieldname == "altitude"))
        {
          ret->at(index).emplace_back(TS::TimedValue(obstime, coordinateValue(fieldname, coords)));
        }
        else
        {
          TS::Value value = rsr[resultFieldname(fieldname, db, producerConfig, dbInfo)];
          ret->at(index).emplace_back(TS::TimedValue(obstime, value));
        }
        index++;

        n_elements += ret->size();
        if (coords.longitude && coords.latitude)
          locations.insert(Fmi::to_string(*coords.longitude) + Fmi::to_string(*coords.latitude));
        obstimes.insert(obstime);

        check_request_limit(
            settings.requestLimits, locations.size(), TS::RequestLimitMember::LOCATIONS);
        check_request_limit(
            settings.requestLimits, obstimes.size(), TS::RequestLimitMember::TIMESTEPS);
        check_request_limit(settings.requestLimits, n_elements, TS::RequestLimitMember::ELEMENTS);
      }
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Fetching mobile data from database failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
