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
using namespace boost::gregorian;
using namespace boost::posix_time;
using namespace boost::local_time;

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
  double operator()(TS::None /* none */)
  {
    return static_cast<double>(kFloatMissing);
  }
  double operator()(double luku) { return luku; }
  double operator()(const std::string & /* s */) { return static_cast<double>(kFloatMissing); }
  double operator()(int i) { return static_cast<double>(i); }
  double operator()(boost::local_time::local_date_time /* i */)
  {
    return static_cast<double>(kFloatMissing);
  }
  double operator()(TS::LonLat /* i */)
  {
    return static_cast<double>(kFloatMissing);
  }
};

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::executeQuery(
    PostgreSQLObsDB &db, Settings &settings, const Fmi::TimeZones &timezones)
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

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::values(
    PostgreSQLObsDB &db, Settings &settings, const Fmi::TimeZones &timezones)
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
    Settings &settings,
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

TS::TimeSeriesVectorPtr QueryExternalAndMobileData::executeQuery(
    PostgreSQLObsDB &db,
    Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    TS::TimeSeriesVectorPtr ret =
        boost::make_shared<TS::TimeSeriesVector>();
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

    std::string sqlStmt;
    if (settings.stationtype == FMI_IOT_PRODUCER)
    {
      std::vector<const FmiIoTStation *> validStations = itsStations->getStations(settings.wktArea);

      if (!settings.wktArea.empty() && validStations.empty())
        return ret;

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

      sqlStmt = dbInfo.sqlSelect(
          measurandIds, settings.starttime, settings.endtime, station_ids, settings.dataFilter);
    }
    else
    {
      sqlStmt = dbInfo.sqlSelect(measurandIds,
                                 settings.starttime,
                                 settings.endtime,
                                 settings.wktArea,
                                 settings.dataFilter);
    }

    if (settings.debug_options)
      std::cout << "PostgreSQL: " << sqlStmt << std::endl;

    // Execute SQL statement
    Fmi::Database::PostgreSQLConnection &conn = db.getConnection();
    pqxx::result result_set = conn.executeNonTransaction(sqlStmt);

    for (unsigned int i = 0; i <= queryfields.size(); i++)
      ret->emplace_back(TS::TimeSeries(settings.localTimePool));

	TS::TimeSeriesGenerator::LocalTimeList tlist;
    // The desired timeseries, unless all available data if timestep=0 or latest only
    if (!settings.latest && !timeSeriesOptions.all())
    {
      tlist = TS::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
    }
    ResultSetRows rsrs =
        PostgreSQLCacheDB::getResultSetForMobileExternalData(result_set, conn.dataTypes());

    for (auto rsr : rsrs)
    {
      boost::local_time::local_date_time obstime =
          *(boost::get<boost::local_time::local_date_time>(&rsr["data_time"]));

      boost::optional<double> longitudeValue;
      boost::optional<double> latitudeValue;
      boost::optional<double> elevationValue;
      if (settings.stationtype == FMI_IOT_PRODUCER)
      {
        std::string station_code = *(boost::get<std::string>(&rsr["station_code"]));

        if (itsStations->isActive(station_code, obstime.utc_time()))
        {
          const FmiIoTStation &s = itsStations->getStation(station_code, obstime.utc_time());
          longitudeValue = s.longitude;
          latitudeValue = s.latitude;
          if (s.elevation >= 0.0)
            elevationValue = s.elevation;
        }
      }

      unsigned int index = 0;
      for (auto fieldname : queryfields)
      {
        if (fieldname == "created")
        {
          boost::local_time::local_date_time dt =
              *(boost::get<boost::local_time::local_date_time>(&rsr[fieldname]));
          std::string fieldValue = db.getTimeFormatter()->format(dt);
          ret->at(index).emplace_back(TS::TimedValue(obstime, fieldValue));
        }
        else if (settings.stationtype == FMI_IOT_PRODUCER &&
                 (fieldname == "longitude" || fieldname == "latitude" || fieldname == "altitude"))
        {
          TS::Value value;
          if (fieldname == "longitude" && longitudeValue)
          {
            value = *longitudeValue;
          }
          else if (fieldname == "latitude" && latitudeValue)
          {
            value = *latitudeValue;
          }
          else if ((fieldname == "altitude" || fieldname == "elevation") && elevationValue)
          {
            value = *elevationValue;
          }
          ret->at(index).emplace_back(TS::TimedValue(obstime, value));
        }
        else
        {
          TS::Value value;
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
          value = rsr[fieldname];

          ret->at(index).emplace_back(TS::TimedValue(obstime, value));
        }
        index++;
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
