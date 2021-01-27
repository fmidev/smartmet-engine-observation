#include "CommonDatabaseFunctions.h"
#include "SpecialParameters.h"
#include "Utils.h"
#include <boost/functional/hash.hpp>
#include <macgyver/StringConversion.h>
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <spine/ParameterTools.h>
#include <spine/TimeSeriesGenerator.h>
#include <spine/TimeSeriesOutput.h>
#include <numeric>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
template <typename Container, typename Key>
bool exists(const Container &container, const Key &key)
{
  return (container.find(key) != container.end());
}

bool is_data_source_field(const std::string &fieldname)
{
  return (fieldname.find("_data_source_sensornumber_") != std::string::npos);
}
bool is_data_quality_field(const std::string &fieldname)
{
  return (fieldname.length() > 3 &&
          (fieldname.compare(0, 3, "qc_") == 0 ||
           fieldname.find("_data_quality_sensornumber_") != std::string::npos));
}

}  // namespace

QueryMapping CommonDatabaseFunctions::buildQueryMapping(const Spine::Stations &stations,
                                                        const Settings &settings,
                                                        const ParameterMapPtr &parameterMap,
                                                        const std::string &stationtype,
                                                        bool isWeatherDataQCTable) const
{
  try
  {
    QueryMapping ret;

    unsigned int pos = 0;
    std::set<int> mids;
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = p.name();

      if (not_special(p))
      {
        bool isDataQualityField = removePrefix(name, "qc_");
        if (!isDataQualityField)
          isDataQualityField = (p.getSensorParameter() == "qc");

        Fmi::ascii_tolower(name);
        std::string sensor_number_string =
            (p.getSensorNumber() ? Fmi::to_string(*(p.getSensorNumber())) : "default");
        std::string name_plus_sensor_number = name;
        if (isDataQualityField)
          name_plus_sensor_number += "_data_quality";
        name_plus_sensor_number += ("_sensornumber_" + sensor_number_string);
        bool isDataSourceField = is_data_source_field(name_plus_sensor_number);

        if (isDataQualityField || isDataSourceField)
        {
          ret.specialPositions[name_plus_sensor_number] = pos;
        }
        else
        {
          auto sparam = parameterMap->getParameter(name, stationtype);

          if (!sparam.empty())
          {
            int nparam =
                (!isWeatherDataQCTable
                     ? Fmi::stoi(sparam)
                     : boost::hash_value(Fmi::ascii_tolower_copy(sparam + sensor_number_string)));

            ret.timeseriesPositionsString[name_plus_sensor_number] = pos;
            ret.parameterNameMap[name_plus_sensor_number] = sparam;
            ret.paramVector.push_back(nparam);
            if (mids.find(nparam) == mids.end())
              ret.measurandIds.push_back(nparam);
            int sensor_number = (p.getSensorNumber() ? *(p.getSensorNumber()) : -1);
            ret.sensorNumberToMeasurandIds[sensor_number].insert(nparam);
            mids.insert(nparam);
          }
          else
          {
            throw Fmi::Exception::Trace(
                BCP, "Parameter " + name + " for stationtype " + stationtype + " not found!");
          }
        }
      }
      else
      {
        std::string name = p.name();
        Fmi::ascii_tolower(name);

        if (name.find("windcompass") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam = Fmi::stoi(parameterMap->getParameter("winddirection", stationtype));
            ret.measurandIds.push_back(nparam);
          }
          ret.specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam1 = Fmi::stoi(parameterMap->getParameter("windspeedms", stationtype));
            auto nparam2 = Fmi::stoi(parameterMap->getParameter("relativehumidity", stationtype));
            auto nparam3 = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));
            ret.measurandIds.push_back(nparam1);
            ret.measurandIds.push_back(nparam2);
            ret.measurandIds.push_back(nparam3);
          }
          ret.specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam1 = Fmi::stoi(parameterMap->getParameter("wawa", stationtype));
            auto nparam2 = Fmi::stoi(parameterMap->getParameter("totalcloudcover", stationtype));
            auto nparam3 = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));
            ret.measurandIds.push_back(nparam1);
            ret.measurandIds.push_back(nparam2);
            ret.measurandIds.push_back(nparam3);
          }
          ret.specialPositions[name] = pos;
        }
        else
        {
          ret.specialPositions[name] = pos;
        }
      }
      pos++;
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

std::string CommonDatabaseFunctions::getSensorQueryCondition(
    const std::map<int, std::set<int>> &sensorNumberToMeasurandIds) const
{
  std::string ret;

  std::string sensorNumberCondition;
  bool defaultSensorRequested = false;
  for (const auto &item : sensorNumberToMeasurandIds)
  {
    if (item.first == -1)
    {
      defaultSensorRequested = true;
      continue;
    }
    for (const auto &mid : item.second)
    {
      if (sensorNumberCondition.size() > 0)
        sensorNumberCondition += " OR ";
      sensorNumberCondition += ("(data.sensor_no = " + Fmi::to_string(item.first) +
                                " AND data.measurand_id = " + Fmi::to_string(mid) + ") ");
    }
  }
  if (!sensorNumberCondition.empty())
  {
    ret += "AND (" + sensorNumberCondition;
    // If no sensor number given get default
    if (defaultSensorRequested)
    {
      ret += "OR data.measurand_no = 1";
    }
    ret += ") ";
  }
  else
  {
    ret += "AND data.measurand_no = 1 ";
  }

  return ret;
}
// Returns value of default sensor for measurand
Spine::TimeSeries::Value CommonDatabaseFunctions::getDefaultSensorValue(
    const std::map<int, std::map<int, int>> *defaultSensors,
    const std::map<std::string, Spine::TimeSeries::Value> &sensorValues,
    int measurandId,
    int fmisid) const
{
  try
  {
    Spine::TimeSeries::Value ret = kFloatMissing;
    if (defaultSensors)
    {
      if (defaultSensors->find(fmisid) != defaultSensors->end())
      {
        const std::map<int, int> &defaultSensorsOfFmisid = defaultSensors->at(fmisid);
        if (defaultSensorsOfFmisid.find(measurandId) != defaultSensorsOfFmisid.end())
        {
          std::string sensor_no = Fmi::to_string(defaultSensorsOfFmisid.at(measurandId));
          if (sensorValues.find(sensor_no) != sensorValues.end())
          {
            ret = sensorValues.at(sensor_no);
          }
        }
        else if (sensorValues.find("1") != sensorValues.end())
        {
          // Default sensor requested, but not detected in result set, so probably it was requested
          // explicitly And because default sensor number is usually 1, lets try it
          ret = sensorValues.at("1");
        }
      }
    }

    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting default sensor value failed!");
  }
}

bool CommonDatabaseFunctions::isDataSourceField(const std::string &fieldname) const
{
  return (fieldname.find("_data_source_sensornumber_") != std::string::npos);
}
bool CommonDatabaseFunctions::isDataQualityField(const std::string &fieldname) const
{
  return (fieldname.length() > 3 &&
          (fieldname.compare(0, 3, "qc_") == 0 ||
           fieldname.find("_data_quality_sensornumber_") != std::string::npos));
}

bool CommonDatabaseFunctions::isDataSourceOrDataQualityField(const std::string &fieldname) const
{
  return (isDataSourceField(fieldname) || isDataQualityField(fieldname));
}

void CommonDatabaseFunctions::solveMeasurandIds(const std::vector<std::string> &parameters,
                                                const ParameterMapPtr &parameterMap,
                                                const std::string &stationType,
                                                std::multimap<int, std::string> &parameterIDs) const
{
  try
  {
    // Empty list means we want all parameters
    const bool findOnlyGiven = (not parameters.empty());

    for (auto params = parameterMap->begin(); params != parameterMap->end(); ++params)
    {
      if (findOnlyGiven &&
          find(parameters.begin(), parameters.end(), params->first) == parameters.end())
        continue;

      auto gid = params->second.find(stationType);
      if (gid == params->second.end())
        continue;

      try
      {
        int id = std::stoi(gid->second);
        parameterIDs.emplace(id, params->first);
      }
      catch (const std::exception &)
      {
        // gid is either too large or not convertible (ie. something is wrong)
        continue;
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Solving measurand id failed!");
  }
}

StationMap CommonDatabaseFunctions::mapQueryStations(const Spine::Stations &stations,
                                                     const std::set<int> &observed_fmisids) const
{
  try
  {
    StationMap ret;
    for (const Spine::Station &s : stations)
    {
      if (observed_fmisids.find(s.station_id) == observed_fmisids.end())
        continue;
      ret.insert(std::make_pair(s.station_id, s));
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Mapping stations failed!");
  }
}

// Build fmisid1,fmisid2,... list
std::string CommonDatabaseFunctions::buildSqlStationList(
    const Spine::Stations &stations,
    const std::set<std::string> &stationgroup_codes,
    const StationInfo &stationInfo) const
{
  try
  {
    std::string ret;
    std::set<std::string> station_ids;
    for (const Spine::Station &s : stations)
    {
      if (stationInfo.belongsToGroup(s.station_id, stationgroup_codes))
        station_ids.insert(Fmi::to_string(s.station_id));
    }
    return std::accumulate(
        std::begin(station_ids),
        std::end(station_ids),
        std::string{},
        [](const std::string &a, const std::string &b) { return a.empty() ? b : a + ',' + b; });
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building station list failed!");
  }
}

ObservationsMap CommonDatabaseFunctions::buildObservationsMap(
    const LocationDataItems &observations,
    const Settings &settings,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station) const
{
  ObservationsMap ret;

  try
  {
    for (const auto &obs : observations)
    {
      int fmisid = obs.data.fmisid;

      std::string zone(settings.timezone == "localtime" ? fmisid_to_station.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      boost::local_time::local_date_time obstime =
          boost::local_time::local_date_time(obs.data.data_time, localtz);

      Spine::TimeSeries::Value val =
          (obs.data.data_value ? Spine::TimeSeries::Value(*obs.data.data_value)
                               : Spine::TimeSeries::None());

      Spine::TimeSeries::Value data_source_val =
          (obs.data.data_source > -1 ? Spine::TimeSeries::Value(obs.data.data_source)
                                     : Spine::TimeSeries::None());

      ret.data[fmisid][obstime][obs.data.measurand_id][obs.data.sensor_no] = val;
      ret.dataWithStringParameterId[fmisid][obstime][Fmi::to_string(obs.data.measurand_id)]
                                   [Fmi::to_string(obs.data.sensor_no)] = val;
      ret.dataSourceWithStringParameterId[fmisid][obstime][Fmi::to_string(obs.data.measurand_id)]
                                         [Fmi::to_string(obs.data.sensor_no)] = data_source_val;
      ret.dataQualityWithStringParameterId[fmisid][obstime][Fmi::to_string(obs.data.measurand_id)]
                                          [Fmi::to_string(obs.data.sensor_no)] =
          Spine::TimeSeries::Value(obs.data.data_quality);
    }
    ret.default_sensors = &observations.default_sensors;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Mapping observations failed!");
  }

  return ret;
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::buildTimeseries(
    const Spine::Stations &stations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const LocationDataItems &observations,
    ObservationsMap &obsmap,
    const QueryMapping &qmap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  // Accept all time steps
  if (timeSeriesOptions.all() && !settings.latest)
    return buildTimeseriesAllTimeSteps(
        stations, settings, stationtype, fmisid_to_station, obsmap, qmap);

  // Accept only latest time from generated time steps
  if (settings.latest)
    return buildTimeseriesLatestTimeStep(stations,
                                         settings,
                                         stationtype,
                                         fmisid_to_station,
                                         observations,
                                         obsmap,
                                         qmap,
                                         timeSeriesOptions,
                                         timezones);

  // All requested timesteps
  Spine::TimeSeries::TimeSeriesVectorPtr ret = buildTimeseriesListedTimeSteps(stations,
                                                                              settings,
                                                                              stationtype,
                                                                              fmisid_to_station,
                                                                              observations,
                                                                              obsmap,
                                                                              qmap,
                                                                              timeSeriesOptions,
                                                                              timezones);

  return ret;
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::buildTimeseriesAllTimeSteps(
    const Spine::Stations &stations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    ObservationsMap &obsmap,
    const QueryMapping &qmap) const
{
  using dataItemWithStringParameterId =
      std::pair<boost::local_time::local_date_time,
                std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>;

  Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
      initializeResultVector(settings.parameters);

  try
  {
    bool addDataQualityField = false;
    bool addDataSourceField = false;

    for (auto const &item : qmap.specialPositions)
    {
      if (isDataSourceField(item.first))
        addDataSourceField = true;
      if (isDataQualityField(item.first))
        addDataQualityField = true;
    }

    for (const auto &item : obsmap.data)
    {
      int fmisid = item.first;

      if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
        continue;

      if (obsmap.dataWithStringParameterId.find(fmisid) == obsmap.dataWithStringParameterId.end())
        continue;

      // obstime -> measurand_id -> sensor_no -> value
      // This may create an empty object for stations from which we got no values - which is fine
      std::map<boost::local_time::local_date_time,
               std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
          &stationData = obsmap.dataWithStringParameterId.at(fmisid);

      for (const dataItemWithStringParameterId &item : stationData)
      {
        addParameterToTimeSeries(timeSeriesColumns,
                                 item,
                                 fmisid,
                                 qmap.specialPositions,
                                 qmap.parameterNameMap,
                                 qmap.timeseriesPositionsString,
                                 qmap.sensorNumberToMeasurandIds,
                                 obsmap.default_sensors,
                                 stationtype,
                                 fmisid_to_station.at(fmisid),
                                 settings);
      }
      if (addDataSourceField && obsmap.dataSourceWithStringParameterId.find(fmisid) !=
                                    obsmap.dataSourceWithStringParameterId.end())
      {
        std::list<boost::local_time::local_date_time> tlist;

        for (const dataItemWithStringParameterId &item :
             obsmap.dataSourceWithStringParameterId.at(fmisid))
          tlist.push_back(item.first);

        addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                     obsmap.dataSourceWithStringParameterId.at(fmisid),
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     obsmap.default_sensors,
                                     tlist,
                                     true);
      }
      if (addDataQualityField && obsmap.dataQualityWithStringParameterId.find(fmisid) !=
                                     obsmap.dataQualityWithStringParameterId.end())
      {
        std::list<boost::local_time::local_date_time> tlist;
        for (const dataItemWithStringParameterId &item :
             obsmap.dataQualityWithStringParameterId.at(fmisid))
          tlist.push_back(item.first);

        addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                     obsmap.dataQualityWithStringParameterId.at(fmisid),
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     obsmap.default_sensors,
                                     tlist,
                                     false);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series with all timesteps failed!");
  }

  return timeSeriesColumns;
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::buildTimeseriesLatestTimeStep(
    const Spine::Stations &stations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const LocationDataItems &observations,
    ObservationsMap &obsmap,
    const QueryMapping &qmap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        initializeResultVector(settings.parameters);

    auto tlist = Spine::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    for (const auto &item : obsmap.data)
    {
      int fmisid = item.first;

      if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
        continue;

      const Spine::Station &s = fmisid_to_station.at(fmisid);

      // Get only the last time step if there is many
      boost::local_time::local_date_time t(boost::local_time::not_a_date_time);

      auto fmisid_pos = obsmap.data.find(fmisid);

      if (fmisid_pos == obsmap.data.end())
        continue;

      t = fmisid_pos->second.rbegin()->first;

      if (obsmap.dataWithStringParameterId.find(fmisid) != obsmap.dataWithStringParameterId.end())
        appendWeatherParameters(s,
                                t,
                                timeSeriesColumns,
                                stationtype,
                                fmisid_to_station,
                                observations,
                                obsmap,
                                qmap,
                                settings);
      else
        addEmptyValuesToTimeSeries(timeSeriesColumns,
                                   t,
                                   qmap.specialPositions,
                                   qmap.parameterNameMap,
                                   qmap.timeseriesPositionsString,
                                   stationtype,
                                   fmisid_to_station.at(fmisid),
                                   settings.timezone);
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series with latest timestep failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::buildTimeseriesListedTimeSteps(
    const Spine::Stations &stations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const LocationDataItems &observations,
    ObservationsMap &obsmap,
    const QueryMapping &qmap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        initializeResultVector(settings.parameters);
    auto tlist = Spine::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    bool addDataQualityField = false;
    bool addDataSourceField = false;

    for (auto const &item : qmap.specialPositions)
    {
      if (isDataSourceField(item.first))
        addDataSourceField = true;
      if (isDataQualityField(item.first))
        addDataQualityField = true;
    }

    for (const auto &item : obsmap.data)
    {
      int fmisid = item.first;

      if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
        continue;

      const Spine::Station &s = fmisid_to_station.at(fmisid);

      for (const boost::local_time::local_date_time &t : tlist)
      {
        if (obsmap.dataWithStringParameterId.find(fmisid) != obsmap.dataWithStringParameterId.end())
          appendWeatherParameters(s,
                                  t,
                                  timeSeriesColumns,
                                  stationtype,
                                  fmisid_to_station,
                                  observations,
                                  obsmap,
                                  qmap,
                                  settings);
        else
          addEmptyValuesToTimeSeries(timeSeriesColumns,
                                     t,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     qmap.timeseriesPositionsString,
                                     stationtype,
                                     fmisid_to_station.at(fmisid),
                                     settings.timezone);
      }

      if (addDataSourceField && obsmap.dataSourceWithStringParameterId.find(fmisid) !=
                                    obsmap.dataSourceWithStringParameterId.end())
      {
        addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                     obsmap.dataSourceWithStringParameterId.at(fmisid),
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     obsmap.default_sensors,
                                     tlist,
                                     true);
      }
      if (addDataQualityField && obsmap.dataQualityWithStringParameterId.find(fmisid) !=
                                     obsmap.dataQualityWithStringParameterId.end())
      {
        addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                     obsmap.dataQualityWithStringParameterId.at(fmisid),
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     obsmap.default_sensors,
                                     tlist,
                                     false);
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series with listed timesteps failed!");
  }
}

void CommonDatabaseFunctions::appendWeatherParameters(
    const Spine::Station &s,
    const boost::local_time::local_date_time &t,
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const LocationDataItems &observations,
    ObservationsMap &obsmap,
    const QueryMapping &qmap,
    const Settings &settings) const

{
  try
  {
    if (obsmap.dataWithStringParameterId.find(s.fmisid) == obsmap.dataWithStringParameterId.end())
      return;

    // This may create an empty object for stations from which we got no values - which is fine
    const std::map<boost::local_time::local_date_time,
                   std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
        &stationData = obsmap.dataWithStringParameterId.at(s.fmisid);

    for (const auto &item : qmap.parameterNameMap)
    {
      std::string param_name = item.first;
      std::string sensor_number = "default";
      if (param_name.find("_sensornumber_") != std::string::npos)
        sensor_number = param_name.substr(param_name.rfind("_") + 1);
      int pos = qmap.timeseriesPositionsString.at(param_name);  // qmap.paramVector.at(i);

      std::string measurand_id = Fmi::ascii_tolower_copy(item.second);
      Spine::TimeSeries::Value val = Spine::TimeSeries::None();

      if (stationData.find(t) != stationData.end())
      {
        const std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> &data =
            stationData.at(t);
        if (data.count(measurand_id) > 0)
        {
          std::map<std::string, Spine::TimeSeries::Value> sensor_values = data.at(measurand_id);
          if (sensor_number == "default")
          {
            val = getDefaultSensorValue(
                obsmap.default_sensors, sensor_values, Fmi::stoi(item.second), s.fmisid);
          }
          else
          {
            if (sensor_values.find(sensor_number) != sensor_values.end())
              val = sensor_values.at(sensor_number);
          }
        }
      }
      timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, val));
    }
    for (const auto &special : qmap.specialPositions)
    {
      int pos = special.second;

      Spine::TimeSeries::Value missing;

      const std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> *data = nullptr;
      if (stationData.find(t) != stationData.end())
        data = &(stationData.at(t));

      if (special.first.find("windcompass") != std::string::npos)
      {
        if (data == nullptr)
        {
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
          continue;
        }

        // Have to get wind direction first
        std::string mid = itsParameterMap->getParameter("winddirection", stationtype);
        if (data->count(mid) == 0)
        {
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
        }
        else
        {
          if (stationData.find(t) == stationData.end())
          {
            timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
            continue;
          }

          const std::map<std::string, Spine::TimeSeries::Value> &sensor_values = data->at(mid);

          Spine::TimeSeries::Value none = Spine::TimeSeries::None();
          Spine::TimeSeries::Value val = getDefaultSensorValue(
              obsmap.default_sensors, sensor_values, Fmi::stoi(mid), s.fmisid);

          if (val != none)
          {
            std::string windCompass;
            if (special.first == "windcompass8")
              windCompass = windCompass8(boost::get<double>(val), settings.missingtext);
            if (special.first == "windcompass16")
              windCompass = windCompass16(boost::get<double>(val), settings.missingtext);
            if (special.first == "windcompass32")
              windCompass = windCompass32(boost::get<double>(val), settings.missingtext);
            Spine::TimeSeries::Value windCompassValue = Spine::TimeSeries::Value(windCompass);
            timeSeriesColumns->at(pos).push_back(
                Spine::TimeSeries::TimedValue(t, windCompassValue));
          }
        }
      }
      else if (special.first.find("feelslike") != std::string::npos)
      {
        if (data == nullptr)
        {
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
          continue;
        }

        // Feels like - deduction. This ignores radiation, since it is measured
        // using
        // dedicated stations
        std::string windpos = itsParameterMap->getParameter("windspeedms", stationtype);
        std::string rhpos = itsParameterMap->getParameter("relativehumidity", stationtype);
        std::string temppos = itsParameterMap->getParameter("temperature", stationtype);

        if (data->count(windpos) == 0 || data->count(rhpos) == 0 || data->count(temppos) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
        }
        else
        {
          std::map<std::string, Spine::TimeSeries::Value> sensor_values = data->at(temppos);
          float temp = boost::get<double>(getDefaultSensorValue(
              obsmap.default_sensors, sensor_values, Fmi::stoi(temppos), s.fmisid));
          sensor_values = data->at(rhpos);
          float rh = boost::get<double>(getDefaultSensorValue(
              obsmap.default_sensors, sensor_values, Fmi::stoi(rhpos), s.fmisid));
          sensor_values = data->at(windpos);
          float wind = boost::get<double>(getDefaultSensorValue(
              obsmap.default_sensors, sensor_values, Fmi::stoi(windpos), s.fmisid));
          Spine::TimeSeries::Value feelslike =
              Spine::TimeSeries::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, feelslike));
        }
      }
      else if (special.first.find("smartsymbol") != std::string::npos)
      {
        if (data == nullptr)
        {
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(t, missing));
          continue;
        }

        addSmartSymbolToTimeSeries(pos,
                                   s,
                                   t,
                                   stationtype,
                                   *data,
                                   obsmap.data,
                                   s.fmisid,
                                   obsmap.default_sensors,
                                   timeSeriesColumns);
      }
      else
      {
        std::string fieldname = special.first;
        if (isDataSourceOrDataQualityField(fieldname))
        {
          // *data_source fields is handled outside this function
          // *data_quality fields is handled outside this function
        }
        else
        {
          addSpecialParameterToTimeSeries(fieldname,
                                          timeSeriesColumns,
                                          fmisid_to_station.at(s.fmisid),
                                          pos,
                                          stationtype,
                                          t,
                                          settings.timezone);
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Appending weather parameter failed!");
  }
}

void CommonDatabaseFunctions::addSpecialFieldsToTimeSeries(
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::map<boost::local_time::local_date_time,
                   std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
        &stationData,
    int fmisid,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<int, std::map<int, int>> *defaultSensors,
    const std::list<boost::local_time::local_date_time> &tlist,
    bool addDataSourceField) const
{
  // Add *data_source- and data_quality-fields

  using dataItemWithStringParameterId =
      std::pair<boost::local_time::local_date_time,
                std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>;
  std::map<int, Spine::TimeSeries::TimeSeries> data_source_ts;
  for (const dataItemWithStringParameterId &item : stationData)
  {
    const auto &obstime = item.first;
    bool timeOK = false;
    for (const auto &t : tlist)
    {
      if (obstime == t)
      {
        timeOK = true;
        break;
      }
    }
    if (!timeOK)
      continue;

    // measurand_id -> sensor_no -> value
    std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> data = item.second;
    for (const auto &special : specialPositions)
    {
      const auto &fieldname = special.first;

      int pos = special.second;
      Spine::TimeSeries::Value val = Spine::TimeSeries::None();
      if (addDataSourceField && isDataSourceField(fieldname))
      {
        auto masterParamName = fieldname.substr(0, fieldname.find("_data_source_sensornumber_"));
        if (!masterParamName.empty())
          masterParamName = masterParamName.substr(0, masterParamName.length());
        std::string sensor_number = fieldname.substr(fieldname.rfind("_") + 1);
        bool default_sensor = (sensor_number == "default");
        for (const auto &item : parameterNameMap)
        {
          if (boost::algorithm::starts_with(item.first, masterParamName + "_sensornumber_"))
          {
            const auto &nameInDatabase = parameterNameMap.at(item.first);
            if (data.count(nameInDatabase) > 0)
            {
              std::map<std::string, Spine::TimeSeries::Value> sensor_values =
                  data.at(nameInDatabase);
              if (default_sensor)
              {
                val = getDefaultSensorValue(
                    defaultSensors, sensor_values, Fmi::stoi(nameInDatabase), fmisid);
              }
              else if (sensor_values.find(sensor_number) != sensor_values.end())
              {
                val = sensor_values.at(sensor_number);
              }
            }
            break;
          }
        }
        data_source_ts[pos].push_back(Spine::TimeSeries::TimedValue(obstime, val));
      }
      else if (!addDataSourceField && isDataQualityField(fieldname))
      {
        // Handle data_quality field
        std::string sensor_number = fieldname.substr(fieldname.rfind("_") + 1);
        bool default_sensor = (sensor_number == "default");

        for (const auto &pn : parameterNameMap)
        {
          const auto &nameInDatabase = Fmi::ascii_tolower_copy(pn.second);

          if (data.count(nameInDatabase) > 0)
          {
            std::map<std::string, Spine::TimeSeries::Value> sensor_values = data.at(nameInDatabase);
            if (default_sensor)
            {
              val = getDefaultSensorValue(
                  defaultSensors, sensor_values, Fmi::stoi(nameInDatabase), fmisid);
            }
            else if (sensor_values.find(sensor_number) != sensor_values.end())
            {
              val = sensor_values.at(sensor_number);
            }
          }
        }
        data_source_ts[pos].push_back(Spine::TimeSeries::TimedValue(obstime, val));
      }
    }
  }
  // Add data to result vector + handle missing time steps
  Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
  for (const auto &item : data_source_ts)
  {
    int pos = item.first;
    const auto &ts = item.second;
    auto time_iterator = tlist.begin();
    for (const auto &val : ts)
    {
      auto obstime = val.time;
      while (time_iterator != tlist.end() && *time_iterator < obstime)
      {
        timeSeriesColumns->at(pos).push_back(
            Spine::TimeSeries::TimedValue(*time_iterator, missing));
        time_iterator++;
      }
      if (time_iterator != tlist.end() && *time_iterator == obstime)
        time_iterator++;
      timeSeriesColumns->at(pos).push_back(val);
    }
    // Timesteps after last timestep in data
    while (time_iterator != tlist.end())
    {
      timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(*time_iterator, missing));
      time_iterator++;
    }
  }
}

void CommonDatabaseFunctions::addParameterToTimeSeries(
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time,
                    std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
        &dataItem,
    int fmisid,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const std::map<int, std::set<int>> &sensorNumberToMeasurandIds,
    const std::map<int, std::map<int, int>> *defaultSensors,  // measurand_id -> sensor_no
    const std::string &stationtype,
    const Spine::Station &station,
    const Settings &settings) const
{
  try
  {
    boost::local_time::local_date_time obstime = dataItem.first;
    // measurand id -> sensor no -> data
    std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> data = dataItem.second;
    // Append weather parameters

    for (const auto &parameterNames : parameterNameMap)
    {
      std::string name_plus_snumber = parameterNames.first;
      size_t snumber_pos = name_plus_snumber.find("_sensornumber_");
      std::string sensor_no = "default";
      if (snumber_pos != std::string::npos)
        sensor_no = name_plus_snumber.substr(name_plus_snumber.rfind("_") + 1);

      std::string nameInRequest = parameterNames.first;
      std::string nameInDatabase = Fmi::ascii_tolower_copy(parameterNames.second);
      Spine::TimeSeries::Value val = Spine::TimeSeries::None();

      if (data.count(nameInDatabase) > 0)
      {
        bool is_number = isdigit(nameInDatabase.at(0));
        int parameter_id =
            (is_number ? Fmi::stoi(nameInDatabase)
                       : boost::hash_value(Fmi::ascii_tolower_copy(nameInDatabase + sensor_no)));
        std::map<std::string, Spine::TimeSeries::Value> sensor_values = data.at(nameInDatabase);
        if (sensor_no == "default")
        {
          val = getDefaultSensorValue(defaultSensors, sensor_values, parameter_id, fmisid);
        }
        else
        {
          if (sensor_values.find(sensor_no) != sensor_values.end())
          {
            val = sensor_values.at(sensor_no);
          }
        }
      }

      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .push_back(Spine::TimeSeries::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;

      if (special.first.find("windcompass") != std::string::npos)
      {
        // Have to get wind direction first
        std::string mid = itsParameterMap->getParameter("winddirection", stationtype);
        if (dataItem.second.count(mid) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          std::map<std::string, Spine::TimeSeries::Value> sensor_values = data.at(mid);

          Spine::TimeSeries::Value val =
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(mid), fmisid);

          Spine::TimeSeries::Value none = Spine::TimeSeries::None();
          if (val != none)
          {
            std::string windCompass;
            if (special.first == "windcompass8")
              windCompass = windCompass8(boost::get<double>(val), settings.missingtext);
            if (special.first == "windcompass16")
              windCompass = windCompass16(boost::get<double>(val), settings.missingtext);
            if (special.first == "windcompass32")
              windCompass = windCompass32(boost::get<double>(val), settings.missingtext);
            Spine::TimeSeries::Value windCompassValue = Spine::TimeSeries::Value(windCompass);
            timeSeriesColumns->at(pos).push_back(
                Spine::TimeSeries::TimedValue(obstime, windCompassValue));
          }
        }
      }
      else if (special.first.find("feelslike") != std::string::npos)
      {
        // Feels like - deduction. This ignores radiation, since it is measured
        // using
        // dedicated stations
        std::string windpos = itsParameterMap->getParameter("windspeedms", stationtype);
        std::string rhpos = itsParameterMap->getParameter("relativehumidity", stationtype);
        std::string temppos = itsParameterMap->getParameter("temperature", stationtype);

        if (data.count(windpos) == 0 || data.count(rhpos) == 0 || data.count(temppos) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          std::map<std::string, Spine::TimeSeries::Value> &sensor_values = data.at(temppos);
          float temp = boost::get<double>(
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(temppos), fmisid));
          sensor_values = data.at(rhpos);
          float rh = boost::get<double>(
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(rhpos), fmisid));
          sensor_values = data.at(windpos);
          float wind = boost::get<double>(
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(windpos), fmisid));
          Spine::TimeSeries::Value feelslike =
              Spine::TimeSeries::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, feelslike));
        }
      }
      else if (special.first.find("smartsymbol") != std::string::npos)
      {
        std::string wawapos = itsParameterMap->getParameter("wawa", stationtype);
        std::string totalcloudcoverpos =
            itsParameterMap->getParameter("totalcloudcover", stationtype);
        std::string temppos = itsParameterMap->getParameter("temperature", stationtype);
        if (data.count(wawapos) == 0 || data.count(totalcloudcoverpos) == 0 ||
            data.count(temppos) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          std::map<std::string, Spine::TimeSeries::Value> &sensor_values = data.at(temppos);
          float temp = boost::get<double>(
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(temppos), fmisid));
          sensor_values = data.at(totalcloudcoverpos);
          int totalcloudcover = static_cast<int>(boost::get<double>(getDefaultSensorValue(
              defaultSensors, sensor_values, Fmi::stoi(totalcloudcoverpos), fmisid)));
          sensor_values = data.at(wawapos);
          int wawa = static_cast<int>(boost::get<double>(
              getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(wawapos), fmisid)));
          double lat = station.latitude_out;
          double lon = station.longitude_out;
          Spine::TimeSeries::Value smartsymbol = Spine::TimeSeries::Value(
              *calcSmartsymbolNumber(wawa, totalcloudcover, temp, obstime, lat, lon));
          timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, smartsymbol));
        }
      }
      else
      {
        std::string fieldname = special.first;
        if (isDataSourceOrDataQualityField(fieldname))
        {
          // *data_source fields is handled outside this function
          // *data_quality fields is handled outside this function
        }
        else
        {
          addSpecialParameterToTimeSeries(
              fieldname, timeSeriesColumns, station, pos, stationtype, obstime, settings.timezone);
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding parameter to time series failed!");
  }
}

void CommonDatabaseFunctions::addEmptyValuesToTimeSeries(
    Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const boost::local_time::local_date_time &obstime,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    const std::map<std::string, int> &timeseriesPositions,
    const std::string &stationtype,
    const Spine::Station &station,
    const std::string &timezone) const
{
  try
  {
    for (const auto &parameterNames : parameterNameMap)
    {
      std::string nameInDatabase = parameterNames.second;
      std::string nameInRequest = parameterNames.first;

      Spine::TimeSeries::Value val = Spine::TimeSeries::None();
      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .push_back(Spine::TimeSeries::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;

      if (special.first.find("windcompass") != std::string::npos ||
          special.first.find("feelslike") != std::string::npos ||
          special.first.find("smartsymbol") != std::string::npos ||
          isDataSourceField(special.first) || isDataQualityField(special.first))
      {
        Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
        timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(obstime, missing));
      }
      else
      {
        addSpecialParameterToTimeSeries(
            special.first, timeSeriesColumns, station, pos, stationtype, obstime, timezone);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding empty values to time series failed!");
  }
}

void CommonDatabaseFunctions::addSmartSymbolToTimeSeries(
    const int pos,
    const Spine::Station &s,
    const boost::local_time::local_date_time &time,
    const std::string &stationtype,
    const std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>> &stationData,
    const std::map<int,
                   std::map<boost::local_time::local_date_time,
                            std::map<int, std::map<int, Spine::TimeSeries::Value>>>> &data,
    int fmisid,
    const std::map<int, std::map<int, int>> *defaultSensors,
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns) const
{
  try
  {
    auto dataItem = data.at(s.fmisid).at(time);
    std::string wawapos = itsParameterMap->getParameter("wawa", stationtype);
    std::string totalcloudcoverpos = itsParameterMap->getParameter("totalcloudcover", stationtype);
    std::string temppos = itsParameterMap->getParameter("temperature", stationtype);
    int wawapos_int = Fmi::stoi(wawapos);
    int totalcloudcoverpos_int = Fmi::stoi(totalcloudcoverpos);
    int temppos_int = Fmi::stoi(temppos);

    if (!exists(dataItem, wawapos_int) || !exists(dataItem, totalcloudcoverpos_int) ||
        !exists(dataItem, temppos_int))
    {
      Spine::TimeSeries::Value missing;
      timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(time, missing));
    }
    else
    {
      std::map<std::string, Spine::TimeSeries::Value> sensor_values = stationData.at(temppos);
      float temp = boost::get<double>(
          getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(temppos), fmisid));
      sensor_values = stationData.at(totalcloudcoverpos);
      int totalcloudcover = static_cast<int>(boost::get<double>(getDefaultSensorValue(
          defaultSensors, sensor_values, Fmi::stoi(totalcloudcoverpos), fmisid)));
      sensor_values = stationData.at(wawapos);
      int wawa = static_cast<int>(boost::get<double>(
          getDefaultSensorValue(defaultSensors, sensor_values, Fmi::stoi(wawapos), fmisid)));
      double lat = s.latitude_out;
      double lon = s.longitude_out;
      Spine::TimeSeries::Value smartsymbol = Spine::TimeSeries::Value(
          *calcSmartsymbolNumber(wawa, totalcloudcover, temp, time, lat, lon));

      timeSeriesColumns->at(pos).push_back(Spine::TimeSeries::TimedValue(time, smartsymbol));
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding smart symbol to time series failed!");
  }
}

void CommonDatabaseFunctions::addSpecialParameterToTimeSeries(
    const std::string &paramname,
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const Spine::Station &station,
    const int pos,
    const std::string &stationtype,
    const boost::local_time::local_date_time &obstime,
    const std::string &timezone) const
{
  try
  {
    boost::local_time::local_date_time now(boost::posix_time::second_clock::universal_time(),
                                           obstime.zone());
    Spine::TimeSeries::TimedValue value =
        getSpecialParameterValue(station, stationtype, paramname, obstime, now, timezone);
    timeSeriesColumns->at(pos).push_back(value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding special parameter to times series failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::getWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Fmi::TimeZones &timezones)
{
  Spine::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getWeatherDataQCData(stations, settings, stationInfo, opt, timezones);
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::getWeatherDataQCData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones)
{
  try
  {
    std::string stationtype = settings.stationtype;

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

        if (name.find("windcompass") != std::string::npos)
        {
          param_set.insert(itsParameterMap->getParameter("winddirection", stationtype));
          timeseriesPositions[itsParameterMap->getParameter("winddirection", stationtype)] = pos;
          specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          param_set.insert(itsParameterMap->getParameter("windspeedms", stationtype));
          param_set.insert(itsParameterMap->getParameter("relativehumidity", stationtype));
          param_set.insert(itsParameterMap->getParameter("relativehumidity", stationtype));
          specialPositions[name] = pos;
        }
        else if (name.find("smartsymbol") != std::string::npos)
        {
          param_set.insert(itsParameterMap->getParameter("wawa", stationtype));
          param_set.insert(itsParameterMap->getParameter("totalcloudcover", stationtype));
          param_set.insert(itsParameterMap->getParameter("temperature", stationtype));

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

    auto qmap = buildQueryMapping(stations, settings, itsParameterMap, stationtype, true);

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        initializeResultVector(settings.parameters);

    std::string query = sqlSelectFromWeatherDataQCData(settings, params, qstations);

    WeatherDataQCData weatherDataQCData;
    std::map<int, std::map<int, int>> default_sensors;

    fetchWeatherDataQCData(
        query, stationInfo, stationgroup_codes, qmap, default_sensors, weatherDataQCData);

    unsigned int i = 0;

    // Generate data structure which can be transformed to TimeSeriesVector
    std::map<int,
             std::map<boost::local_time::local_date_time,
                      std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>>
        data;
    std::map<int,
             std::map<boost::local_time::local_date_time,
                      std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>>
        data_quality;
    std::set<boost::local_time::local_date_time> obstimes;

    for (const auto &time : weatherDataQCData.obstimesAll)
    {
      int fmisid = *weatherDataQCData.fmisidsAll[i];

      boost::posix_time::ptime utctime = time;
      std::string zone(settings.timezone == "localtime" ? fmisid_to_station.at(fmisid).timezone
                                                        : settings.timezone);
      auto localtz = timezones.time_zone_from_string(zone);
      boost::local_time::local_date_time obstime =
          boost::local_time::local_date_time(utctime, localtz);
      obstimes.insert(obstime);

      std::string parameter = *weatherDataQCData.parametersAll[i];
      int sensor_no = *weatherDataQCData.sensor_nosAll[i];
      Fmi::ascii_tolower(parameter);

      Spine::TimeSeries::Value val;
      if (weatherDataQCData.data_valuesAll[i])
        val = Spine::TimeSeries::Value(*weatherDataQCData.data_valuesAll[i]);

      data[fmisid][obstime][parameter][Fmi::to_string(sensor_no)] = val;
      Spine::TimeSeries::Value val_quality = Spine::TimeSeries::None();
      if (weatherDataQCData.data_qualityAll[i])
        val_quality = Spine::TimeSeries::Value(*weatherDataQCData.data_qualityAll[i]);
      data_quality[fmisid][obstime][parameter][Fmi::to_string(sensor_no)] = val_quality;
      i++;
    }

    typedef std::pair<boost::local_time::local_date_time,
                      std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
        dataItem;

    auto tlist = Spine::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    for (const auto &t : tlist)
      obstimes.insert(t);

    if (!settings.latest && !timeSeriesOptions.all())
    {
      for (const auto &item : data)
      {
        int fmisid = item.first;

        if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
          continue;

        std::map<boost::local_time::local_date_time,
                 std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
            stationData = item.second;

        for (const boost::local_time::local_date_time &t : obstimes)
        {
          if (stationData.count(t) > 0)
          {
            dataItem item = std::make_pair(t, stationData.at(t));
            addParameterToTimeSeries(timeSeriesColumns,
                                     item,
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     qmap.timeseriesPositionsString,
                                     qmap.sensorNumberToMeasurandIds,
                                     &default_sensors,
                                     stationtype,
                                     fmisid_to_station.at(fmisid),
                                     settings);
          }
          else
          {
            addEmptyValuesToTimeSeries(timeSeriesColumns,
                                       t,
                                       specialPositions,
                                       parameterNameMap,
                                       timeseriesPositions,
                                       stationtype,
                                       fmisid_to_station.at(fmisid),
                                       settings.timezone);
          }
        }
        stationData = data_quality.at(fmisid);

        addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                     stationData,
                                     fmisid,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     &default_sensors,
                                     tlist,
                                     false);
      }
    }
    else
    {
      for (const auto &item : data)
      {
        int fmisid = item.first;

        if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
          continue;

        std::map<boost::local_time::local_date_time,
                 std::map<std::string, std::map<std::string, Spine::TimeSeries::Value>>>
            stationData = item.second;

        boost::local_time::local_date_time latest_obstime(boost::local_time::not_a_date_time);

        tlist.clear();
        std::set<boost::local_time::local_date_time> fmisid_timesteps;
        for (const dataItem &item : stationData)
        {
          if (settings.latest)
          {
            if (latest_obstime.is_not_a_date_time() || latest_obstime < item.first)
              latest_obstime = item.first;
          }
          else
          {
            tlist.push_back(item.first);
            fmisid_timesteps.insert(item.first);
          }
        }
        if (latest_obstime.is_not_a_date_time())
          tlist.push_back(latest_obstime);

        for (const dataItem &item : stationData)
        {
          if (settings.latest && item.first != latest_obstime)
            continue;

          addParameterToTimeSeries(timeSeriesColumns,
                                   item,
                                   fmisid,
                                   qmap.specialPositions,
                                   qmap.parameterNameMap,
                                   qmap.timeseriesPositionsString,
                                   qmap.sensorNumberToMeasurandIds,
                                   &default_sensors,
                                   stationtype,
                                   fmisid_to_station.at(fmisid),
                                   settings);
        }

        if (data_quality.find(fmisid) != data_quality.end())
        {
          stationData = data_quality.at(fmisid);
          addSpecialFieldsToTimeSeries(timeSeriesColumns,
                                       stationData,
                                       fmisid,
                                       qmap.specialPositions,
                                       qmap.parameterNameMap,
                                       &default_sensors,
                                       tlist,
                                       false);
        }
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting weather data qc data failed!");
  }
}

Spine::TimeSeries::TimeSeriesVectorPtr CommonDatabaseFunctions::getObservationData(
    const Spine::Stations &stations,
    const Settings &settings,
    const StationInfo &stationInfo,
    const Fmi::TimeZones &timezones)
{
  Spine::TimeSeriesGeneratorOptions opt;
  opt.startTime = settings.starttime;
  opt.endTime = settings.endtime;
  opt.timeStep = settings.timestep;
  opt.startTimeUTC = false;
  opt.endTimeUTC = false;

  return getObservationData(stations, settings, stationInfo, opt, timezones);
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
