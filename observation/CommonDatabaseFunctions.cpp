#include "CommonDatabaseFunctions.h"
#include "Utils.h"
#include <boost/algorithm/string.hpp>
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <timeseries/ParameterTools.h>
#include <timeseries/TimeSeriesInclude.h>
#include <numeric>

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
    const Fmi::TimeZones &timezones)
{
  TS::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getWeatherDataQCData(stations, settings, stationInfo, opt, timezones);
}

TS::TimeSeriesVectorPtr CommonDatabaseFunctions::getWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    // Producer 'fmi' is deprecated
    std::string stationtype = settings.stationtype;
    if (stationtype == "fmi")
      stationtype = "observations_fmi";

    std::string qstations;
    std::map<int, Spine::Station> fmisid_to_station;

    std::set<std::string> stationgroup_codes;
    auto stationgroupCodeSet =
        itsStationtypeConfig.getGroupCodeSetByStationtype(settings.stationtype);
    stationgroup_codes.insert(stationgroupCodeSet->begin(), stationgroupCodeSet->end());

    for (const Spine::Station &s : stations)
    {
      if (stationInfo.belongsToGroup(s.fmisid, stationgroup_codes))
      {
        fmisid_to_station.insert(std::make_pair(s.station_id, s));
        qstations += Fmi::to_string(s.station_id) + ",";
      }
    }
    qstations = qstations.substr(0, qstations.length() - 1);

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

    auto qmap = buildQueryMapping(settings, stationtype, true);

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    std::string query = sqlSelectFromWeatherDataQCData(settings, params, qstations);

    WeatherDataQCData weatherDataQCData;

    fetchWeatherDataQCData(
        query, stationInfo, stationgroup_codes, settings.requestLimits, weatherDataQCData);

    StationTimedMeasurandData station_data;

    unsigned int i = 0;

    for (const auto &time : weatherDataQCData.obstimesAll)
    {
      int fmisid = *weatherDataQCData.fmisidsAll[i];

      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? fmisid_to_station.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      boost::local_time::local_date_time obstime =
          boost::local_time::local_date_time(utctime, localtz);

      int measurand_id = *weatherDataQCData.parametersAll[i];
      int sensor_no = *weatherDataQCData.sensor_nosAll[i];

      TS::Value val;
      if (weatherDataQCData.data_valuesAll[i])
        val = TS::Value(*weatherDataQCData.data_valuesAll[i]);

      TS::Value val_quality = TS::None();
      if (weatherDataQCData.data_qualityAll[i])
        val_quality = TS::Value(*weatherDataQCData.data_qualityAll[i]);

      bool data_from_default_sensor = (sensor_no == 1);

      station_data[fmisid][obstime][measurand_id][sensor_no] =
          DataWithQuality(val, val_quality, TS::None(), data_from_default_sensor);
      i++;
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
