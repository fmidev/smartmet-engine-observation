#include "CommonDatabaseFunctions.h"
#include "Utils.h"
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <macgyver/Join.h>
#include <macgyver/StringConversion.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>
#include <algorithm>
#include <numeric>

namespace ba = boost::algorithm;

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

TS::TimeSeriesVectorPtr CommonDatabaseFunctions::getMagnetometerData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Fmi::TimeZones &timezones)
{
  TS::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getMagnetometerData(stations, settings, stationInfo, opt, timezones);
}

TS::TimeSeriesVectorPtr CommonDatabaseFunctions::getWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &extMemoryCache)
{
  TS::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getWeatherDataQCData(stations, settings, stationInfo, opt, timezones, extMemoryCache);
}

TS::TimeSeriesVectorPtr CommonDatabaseFunctions::getWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &extMemoryCache)
{
  try
  {
    // TODO: extMemoryCache???

    // Producer 'fmi' is deprecated
    std::string stationtype = settings.stationtype;
    if (stationtype == "fmi")
      stationtype = "observations_fmi";

    std::string qstations;
    std::map<int, Spine::Station> fmisid_to_station;

    for (const Spine::Station &s : stations)
    {
      if (stationInfo.belongsToGroup(s.fmisid, settings.stationgroups))
      {
        fmisid_to_station.insert(std::make_pair(s.fmisid, s));
        qstations += Fmi::to_string(s.fmisid) + ",";
      }
    }

    // Extra safety check or pop_back() would be undefined behaviour (this has happened)
    if (qstations.empty())
      throw Fmi::Exception(BCP, "Requested stations do not belong to the correct station type");

    qstations.pop_back();  // remove extra comma

    // This maps measurand_id and the parameter position in TimeSeriesVector
    std::map<std::string, int> timeseriesPositions;
    std::map<std::string, std::string> parameterNameMap;
    std::map<std::string, int> specialPositions;

    std::set<std::string> param_set;
    unsigned int pos = 0;
    for (const Spine::Parameter &p : settings.parameters)
    {
      if (not_special(p))
      {
        std::string name = p.name();
        Fmi::ascii_tolower(name);
        bool dataQualityField = removePrefix(name, "qc_");
        if (!dataQualityField)
          dataQualityField = (p.getSensorParameter() == "qc");

        std::string shortname = parseParameterName(name);

        if (!itsParameterMap->getParameter(shortname, stationtype).empty())
        {
          std::string nameInDatabase = itsParameterMap->getParameter(shortname, stationtype);
          std::string sensor_number_string =
              (p.getSensorNumber() ? Fmi::to_string(*(p.getSensorNumber())) : "default");
          std::string name_plus_sensor_number = name + "_sensornumber_" + sensor_number_string;

          timeseriesPositions[name_plus_sensor_number] = pos;
          parameterNameMap[name_plus_sensor_number] = nameInDatabase;
          nameInDatabase = parseParameterName(nameInDatabase);
          Fmi::ascii_toupper(nameInDatabase);
          param_set.insert(nameInDatabase);
        }
      }
      else
      {
        std::string name = p.name();
        Fmi::ascii_tolower(name);

        if (boost::algorithm::starts_with(name, "windcompass"))
        {
          param_set.insert(itsParameterMap->getParameter("winddirection", stationtype));
          timeseriesPositions[itsParameterMap->getParameter("winddirection", stationtype)] = pos;
          specialPositions[name] = pos;
        }
        else if (name == "feelslike")
        {
          param_set.insert(itsParameterMap->getParameter("windspeedms", stationtype));
          param_set.insert(itsParameterMap->getParameter("relativehumidity", stationtype));
          param_set.insert(itsParameterMap->getParameter("relativehumidity", stationtype));
          specialPositions[name] = pos;
        }
        else if (name == "smartsymbol")
        {
          param_set.insert(itsParameterMap->getParameter("wawa", stationtype));
          param_set.insert(itsParameterMap->getParameter("totalcloudcover", stationtype));
          param_set.insert(itsParameterMap->getParameter("temperature", stationtype));

          specialPositions[name] = pos;
        }
        else if (name == "cloudceiling" || name == "cloudceilingft" || name == "cloudceilinghft")
        {
          param_set.insert(itsParameterMap->getParameter("cla1_pt1m_acc", stationtype));
          param_set.insert(itsParameterMap->getParameter("cla2_pt1m_acc", stationtype));
          param_set.insert(itsParameterMap->getParameter("cla3_pt1m_acc", stationtype));
          param_set.insert(itsParameterMap->getParameter("cla4_pt1m_acc", stationtype));
          param_set.insert(itsParameterMap->getParameter("cla5_pt1m_acc", stationtype));
          param_set.insert(itsParameterMap->getParameter("clhb1_pt1m_instant", stationtype));
          param_set.insert(itsParameterMap->getParameter("clhb2_pt1m_instant", stationtype));
          param_set.insert(itsParameterMap->getParameter("clhb3_pt1m_instant", stationtype));
          param_set.insert(itsParameterMap->getParameter("clhb4_pt1m_instant", stationtype));
          param_set.insert(itsParameterMap->getParameter("clh5_pt1m_instant", stationtype));

          specialPositions[name] = pos;
        }
        else
        {
          specialPositions[name] = pos;
        }
      }
      pos++;
    }

    std::string params = getWeatherDataQCParams(param_set);

    if (params.empty())
    {
      std::vector<std::string> param_names;
      std::transform(settings.parameters.begin(),
                     settings.parameters.end(),
                     std::back_inserter(param_names),
                     [](const Spine::Parameter &p) { return p.name(); });
      Fmi::Exception error(BCP, "No available parameters found for weather data query");
      error.addParameter("Stationtype", stationtype);
      error.addParameter("Requested parameters", Fmi::join(param_names, ", "));
      throw error;
    }

    auto qmap = buildQueryMapping(settings, stationtype, true);

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    std::string query = sqlSelectFromWeatherDataQCData(settings, params, qstations);

    LocationDataItems weatherDataQCData;

    fetchWeatherDataQCData(
        query, stationInfo, settings.stationgroups, settings.requestLimits, weatherDataQCData);

    StationTimedMeasurandData station_data;

    for (const auto &item : weatherDataQCData)
    {
      int fmisid = item.data.fmisid;

      Fmi::DateTime utctime = item.data.data_time;
      std::string zone(settings.timezone == "localtime" ? fmisid_to_station.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      Fmi::LocalDateTime obstime = Fmi::LocalDateTime(utctime, localtz);

      int measurand_id = item.data.measurand_id;
      int sensor_no = item.data.sensor_no;

      TS::Value val;
      if (item.data.data_value)
        val = TS::Value(*item.data.data_value);

      TS::Value val_quality = TS::None();
      if (item.data.data_quality)
        val_quality = TS::Value(item.data.data_quality);

      bool data_from_default_sensor = (sensor_no == 1);

      station_data[fmisid][obstime][measurand_id][sensor_no] =
          DataWithQuality(val, val_quality, TS::None(), data_from_default_sensor);
    }

    return buildTimeseries(
        settings, stationtype, fmisid_to_station, station_data, qmap, timeSeriesOptions, timezones);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting weather data qc data failed!");
  }
}

TS::TimeSeriesVectorPtr CommonDatabaseFunctions::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Fmi::TimeZones &timezones,
    const std::unique_ptr<ObservationMemoryCache> &observationMemoryCache)
{
  TS::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getObservationData(
      stations, settings, stationInfo, opt, timezones, observationMemoryCache);
}

std::string CommonDatabaseFunctions::getWeatherDataQCParams(
    const std::set<std::string> &param_set) const
{
  std::string params;
  for (const auto &pname : param_set)
    params += ("'" + pname + "',");
  params = trimCommasFromEnd(params);
  return params;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
