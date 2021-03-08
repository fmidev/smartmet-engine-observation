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
std::ostream &operator<<(std::ostream &out,
                         const SmartMet::Engine::Observation::StationTimedMeasurandData &data)
{
  for (const auto &item1 : data)
  {
    std::cout << "fmisid: " << item1.first << " -> " << std::endl;
    for (const auto &item2 : item1.second)
    {
      std::cout << " observationtime: " << item2.first << " -> " << std::endl;
      for (const auto &item3 : item2.second)
      {
        std::cout << "  measurand: " << item3.first << " -> " << std::endl;
        for (const auto &item4 : item3.second)
        {
          std::cout << "   sensor -> value: " << item4.first << " -> " << item4.second.value << ", "
                    << item4.second.data_quality << ", " << item4.second.data_source << ", "
                    << item4.second.is_default_sensor_data << std::endl;
        }
      }
    }
  }
  return out;
}

namespace
{
enum class DataFieldSpecifier
{
  Value,
  DataQuality,
  DataSource
};

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

Spine::TimeSeries::Value get_default_sensor_value(
    const SensorData &sensor_data,
    int fmisid,
    int measurand_id,
    DataFieldSpecifier specifier = DataFieldSpecifier::Value)
{
  for (const auto &item : sensor_data)
  {
    if (item.second.is_default_sensor_data == true)
    {
      if (specifier == DataFieldSpecifier::Value)
        return item.second.value;
      else if (specifier == DataFieldSpecifier::DataQuality)
        return item.second.data_quality;
      else if (specifier == DataFieldSpecifier::DataSource)
        return item.second.data_source;
    }
  }

  // If no default sensor found return the first
  const auto &default_item = *(sensor_data.begin());

  if (specifier == DataFieldSpecifier::Value)
    return default_item.second.value;
  else if (specifier == DataFieldSpecifier::DataQuality)
    return default_item.second.data_quality;
  else if (specifier == DataFieldSpecifier::DataSource)
    return default_item.second.data_source;

  return Spine::TimeSeries::None();
}

Spine::TimeSeries::Value get_sensor_value(const SensorData &sensor_data,
                                          const std::string &sensor_no,
                                          int fmisid,
                                          int measurand_id,
                                          DataFieldSpecifier specifier = DataFieldSpecifier::Value)
{
  if (sensor_data.empty())
    return Spine::TimeSeries::None();

  if (sensor_no == "default" || sensor_no.empty())
    return get_default_sensor_value(sensor_data, fmisid, measurand_id, specifier);

  int sensor_nro = Fmi::stoi(sensor_no);
  if (sensor_data.find(sensor_nro) != sensor_data.end())
  {
    if (specifier == DataFieldSpecifier::Value)
    {
      return sensor_data.at(sensor_nro).value;
    }
    else if (specifier == DataFieldSpecifier::DataQuality)
    {
      return sensor_data.at(sensor_nro).data_quality;
    }
    else if (specifier == DataFieldSpecifier::DataSource)
    {
      return sensor_data.at(sensor_nro).data_source;
    }
  }

  return Spine::TimeSeries::None();
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
            int nparam = (!isWeatherDataQCTable
                              ? Fmi::stoi(sparam)
                              : itsParameterMap->getRoadAndForeignIds().stringToInteger(sparam));

            ret.timeseriesPositionsString[name_plus_sensor_number] = pos;
            ret.parameterNameMap[name_plus_sensor_number] = sparam;
            ret.parameterNameIdMap[name_plus_sensor_number] = nparam;
            ret.paramVector.push_back(nparam);
            if (mids.find(nparam) == mids.end())
              ret.measurandIds.push_back(nparam);
            int sensor_number = (p.getSensorNumber() ? *(p.getSensorNumber()) : -1);
            if (sensor_number >= 0)
              ret.sensorNumberToMeasurandIds[sensor_number].insert(nparam);
            mids.insert(nparam);
          }
          else
          {
            // Note: settings.stationtype may have been converted to a generic narrow table producer
            // into the stationtype variable, but the original requested type is still in the
            // settings.
            throw Fmi::Exception::Trace(
                BCP,
                "Parameter " + name + " for stationtype " + settings.stationtype + " not found!");
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

StationTimedMeasurandData CommonDatabaseFunctions::buildStationTimedMeasurandData(
    const LocationDataItems &observations,
    const Settings &settings,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station) const
{
  StationTimedMeasurandData ret;

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

      Spine::TimeSeries::Value value =
          (obs.data.data_value ? Spine::TimeSeries::Value(*obs.data.data_value)
                               : Spine::TimeSeries::None());
      Spine::TimeSeries::Value data_quality(obs.data.data_quality);
      Spine::TimeSeries::Value data_source =
          (obs.data.data_source > -1 ? Spine::TimeSeries::Value(obs.data.data_source)
                                     : Spine::TimeSeries::None());

      bool data_from_default_sensor = (obs.data.measurand_no == 1);

      ret[fmisid][obstime][obs.data.measurand_id][obs.data.sensor_no] =
          DataWithQuality(value, data_quality, data_source, data_from_default_sensor);
    }
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
    const StationTimedMeasurandData &station_data,
    const QueryMapping &qmap,
    const Spine::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    // Resolve timesteps for each fmisid
    std::map<int, std::set<boost::local_time::local_date_time>> fmisid_timesteps;
    if (timeSeriesOptions.all() && !settings.latest)
    {
      // std::cout << "**** ALL timesteps in data \n";
      // All timesteps
      for (const auto &item : station_data)
      {
        int fmisid = item.first;
        const auto &timed_measurand_data = item.second;
        for (const auto &item2 : timed_measurand_data)
        {
          fmisid_timesteps[fmisid].insert(item2.first);
        }
      }
    }
    else if (settings.latest)
    {
      // std::cout << "**** LATEST timesteps\n";
      // Latest timestep
      for (const auto &item : station_data)
      {
        int fmisid = item.first;
        const auto &timed_measurand_data = item.second;
        const auto obstime = timed_measurand_data.rbegin()->first;
        fmisid_timesteps[fmisid].insert(obstime);
      }
    }
    else if (!timeSeriesOptions.all() && !settings.latest &&
             itsGetRequestedAndDataTimesteps == AdditionalTimestepOption::RequestedAndDataTimesteps)
    {
      // std::cout << "**** ALL timesteps in data + listed timesteps\n";
      // All FMISDS must have all timesteps in data and all listed timesteps
      std::set<int> fmisids;
      std::set<boost::local_time::local_date_time> timesteps;
      for (const auto &item : station_data)
      {
        fmisids.insert(item.first);
        const auto &timed_measurand_data = item.second;
        for (const auto &item2 : timed_measurand_data)
        {
          timesteps.insert(item2.first);
        }
      }
      const auto tlist = Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      timesteps.insert(tlist.begin(), tlist.end());

      for (const auto &fmisid : fmisids)
        fmisid_timesteps[fmisid].insert(timesteps.begin(), timesteps.end());
    }
    else
    {
      // std::cout << "**** LISTED timesteps\n";
      // Listed timesteps
      const auto tlist = Spine::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      for (const auto &item : station_data)
        fmisid_timesteps[item.first].insert(tlist.begin(), tlist.end());
    }

    //	  std::cout << "station_data:\n" << station_data << std::endl;

    Spine::TimeSeries::TimeSeriesVectorPtr timeSeriesColumns =
        initializeResultVector(settings.parameters);

    bool addDataQualityField = false;
    bool addDataSourceField = false;

    for (auto const &item : qmap.specialPositions)
    {
      if (isDataSourceField(item.first))
        addDataSourceField = true;
      if (isDataQualityField(item.first))
        addDataQualityField = true;
    }

    std::set<int> not_null_columns;

    for (const auto &item : qmap.specialPositions)
    {
      if (Spine::is_special_parameter(item.first))
        not_null_columns.insert(item.second);
    }

    Spine::TimeSeries::TimeSeriesVectorPtr resultVector =
        initializeResultVector(settings.parameters);
    for (const auto &item : station_data)
    {
      int fmisid = item.first;

      const auto &timed_measurand_data = item.second;
      const auto &valid_timesteps = fmisid_timesteps.at(fmisid);

      if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
      {
        //		       std::cout << "Station not found for " << fmisid << std::endl;
        continue;
      }
      for (const auto &data : timed_measurand_data)
      {
        if (valid_timesteps.find(data.first) == valid_timesteps.end())
        {
          //				  std::cout << "Invalid timestep " << data.first << " for
          //station
          //"
          //<< fmisid  << std::endl;
          continue;
        }

        addParameterToTimeSeries(resultVector,
                                 data,
                                 fmisid,
                                 qmap.specialPositions,
                                 qmap.parameterNameIdMap,
                                 qmap.timeseriesPositionsString,
                                 stationtype,
                                 fmisid_to_station.at(fmisid),
                                 settings);
      }

      if (addDataSourceField)
        addSpecialFieldsToTimeSeries(resultVector,
                                     fmisid,
                                     timed_measurand_data,
                                     valid_timesteps,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     true);
      if (addDataQualityField)
        addSpecialFieldsToTimeSeries(resultVector,
                                     fmisid,
                                     timed_measurand_data,
                                     valid_timesteps,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     false);

      // All possible missing timesteps
      for (unsigned int i = 0; i < resultVector->size(); i++)
      {
        auto &ts = resultVector->at(i);
        if (ts.empty())
          continue;

        Spine::TimeSeries::Value missing_value = Spine::TimeSeries::None();
        if (ts.size() > 0 && not_null_columns.find(i) != not_null_columns.end())
        {
          missing_value = ts.back().value;
        }

        Spine::TimeSeries::TimeSeries new_ts;
        std::set<boost::local_time::local_date_time>::const_iterator timestep_iter =
            valid_timesteps.begin();
        for (auto &timed_value : ts)
        {
          while (*timestep_iter < timed_value.time && timestep_iter != valid_timesteps.end())
          {
            new_ts.emplace_back(Spine::TimeSeries::TimedValue(*timestep_iter, missing_value));
            timestep_iter++;
          }
          new_ts.push_back(timed_value);
          timestep_iter++;
        }

        if (timestep_iter != valid_timesteps.end() &&
            (new_ts.size() > 0 && *timestep_iter == new_ts.back().time))
          timestep_iter++;

        while (timestep_iter != valid_timesteps.end())
        {
          new_ts.emplace_back(Spine::TimeSeries::TimedValue(*timestep_iter, missing_value));
          timestep_iter++;
        }
        /*
        ts = new_ts;
        timeSeriesColumns->at(i).insert(timeSeriesColumns->at(i).end(), ts.begin(), ts.end());
        */
        timeSeriesColumns->at(i).insert(
            timeSeriesColumns->at(i).end(), new_ts.begin(), new_ts.end());
        ts.clear();
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series with all timesteps failed!");
  }
}

void CommonDatabaseFunctions::addSpecialFieldsToTimeSeries(
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    int fmisid,
    const TimedMeasurandData &timed_measurand_data,
    const std::set<boost::local_time::local_date_time> &valid_timesteps,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, std::string> &parameterNameMap,
    bool addDataSourceField) const
{
  // Add *data_source- and data_quality-fields
  try
  {
    std::map<int, Spine::TimeSeries::TimeSeries> data_source_ts;
    std::set<boost::local_time::local_date_time> timesteps;
    for (const auto &item : timed_measurand_data)
    {
      const auto &obstime = item.first;
      if (valid_timesteps.find(obstime) == valid_timesteps.end())
        continue;

      // measurand_id -> sensor_no -> value
      const auto &measurand_data = item.second;
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
          for (const auto &item : parameterNameMap)
          {
            if (boost::algorithm::starts_with(item.first, masterParamName + "_sensornumber_"))
            {
              int measurand_id = Fmi::stoi(parameterNameMap.at(item.first));
              if (measurand_data.count(measurand_id) > 0)
              {
                const auto &sensor_values = measurand_data.at(measurand_id);
                val = get_sensor_value(sensor_values,
                                       sensor_number,
                                       fmisid,
                                       measurand_id,
                                       DataFieldSpecifier::DataSource);
              }
              break;
            }
          }
          data_source_ts[pos].emplace_back(Spine::TimeSeries::TimedValue(obstime, val));
          timesteps.insert(obstime);
        }
        else if (!addDataSourceField && isDataQualityField(fieldname))
        {
          // Handle data_quality field
          std::string sensor_number = fieldname.substr(fieldname.rfind("_") + 1);
          for (const auto &pn : parameterNameMap)
          {
            int measurand_id = Fmi::stoi(pn.second);

            if (measurand_data.count(measurand_id) > 0)
            {
              const auto &sensor_values = measurand_data.at(measurand_id);
              val = get_sensor_value(sensor_values,
                                     sensor_number,
                                     fmisid,
                                     measurand_id,
                                     DataFieldSpecifier::DataQuality);
            }
          }
          data_source_ts[pos].emplace_back(Spine::TimeSeries::TimedValue(obstime, val));
          timesteps.insert(obstime);
        }
      }
    }

    // Add data to result vector + handle missing time steps
    Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
    for (const auto &item : data_source_ts)
    {
      int pos = item.first;
      const auto &ts = item.second;
      auto time_iterator = timesteps.begin();
      for (const auto &val : ts)
      {
        auto obstime = val.time;
        while (time_iterator != timesteps.end() && *time_iterator < obstime)
        {
          timeSeriesColumns->at(pos).emplace_back(
              Spine::TimeSeries::TimedValue(*time_iterator, missing));
          time_iterator++;
        }
        if (time_iterator != timesteps.end() && *time_iterator == obstime)
          time_iterator++;
        timeSeriesColumns->at(pos).push_back(val);
      }
      // Timesteps after last timestep in data
      while (time_iterator != timesteps.end())
      {
        timeSeriesColumns->at(pos).emplace_back(
            Spine::TimeSeries::TimedValue(*time_iterator, missing));
        time_iterator++;
      }
    }
  }
  catch (...)
  {
    if (addDataSourceField)
      throw Fmi::Exception::Trace(BCP, "Adding special data source to time series failed!");
    else
      throw Fmi::Exception::Trace(BCP, "Adding special data quality to time series failed!");
  }
}

void CommonDatabaseFunctions::addParameterToTimeSeries(
    const Spine::TimeSeries::TimeSeriesVectorPtr &timeSeriesColumns,
    const std::pair<boost::local_time::local_date_time, MeasurandData> &dataItem,
    int fmisid,
    const std::map<std::string, int> &specialPositions,
    const std::map<std::string, int> &parameterNameIdMap,
    const std::map<std::string, int> &timeseriesPositions,
    const std::string &stationtype,
    const Spine::Station &station,
    const Settings &settings) const
{
  try
  {
    boost::local_time::local_date_time obstime = dataItem.first;
    const MeasurandData &data = dataItem.second;
    // Append weather parameters
    for (const auto &parameter_item : parameterNameIdMap)
    {
      std::string nameInRequest = parameter_item.first;
      int parameter_id = parameter_item.second;

      Spine::TimeSeries::Value val = Spine::TimeSeries::None();

      if (data.count(parameter_id) > 0)
      {
        const auto &sensor_values = data.at(parameter_id);
        std::string name_plus_snumber = parameter_item.first;
        std::string sensor_no = "default";
        if (name_plus_snumber.find("_sensornumber_") != std::string::npos)
          sensor_no = name_plus_snumber.substr(name_plus_snumber.rfind("_") + 1);
        val = get_sensor_value(
            sensor_values, sensor_no, fmisid, parameter_id, DataFieldSpecifier::Value);
      }

      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .emplace_back(Spine::TimeSeries::TimedValue(obstime, val));
    }

    for (const auto &special : specialPositions)
    {
      int pos = special.second;

      if (special.first.find("windcompass") != std::string::npos)
      {
        // Have to get wind direction first
        bool isWeatherDataQCTable = (stationtype == "foreign" || stationtype == "road");
        std::string sparam = itsParameterMap->getParameter("winddirection", stationtype);
        int mid = (!isWeatherDataQCTable
                       ? Fmi::stoi(sparam)
                       : itsParameterMap->getRoadAndForeignIds().stringToInteger(sparam));
        if (data.count(mid) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).emplace_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          const auto &sensor_values = data.at(mid);

          Spine::TimeSeries::Value val = get_default_sensor_value(sensor_values, fmisid, mid);

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
            timeSeriesColumns->at(pos).emplace_back(
                Spine::TimeSeries::TimedValue(obstime, windCompassValue));
          }
        }
      }
      else if (special.first.find("feelslike") != std::string::npos)
      {
        // Feels like - deduction. This ignores radiation, since it is measured
        // using dedicated stations
        int windpos = Fmi::stoi(itsParameterMap->getParameter("windspeedms", stationtype));
        int rhpos = Fmi::stoi(itsParameterMap->getParameter("relativehumidity", stationtype));
        int temppos = Fmi::stoi(itsParameterMap->getParameter("temperature", stationtype));

        if (data.count(windpos) == 0 || data.count(rhpos) == 0 || data.count(temppos) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).emplace_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          auto sensor_values = data.at(temppos);
          float temp = boost::get<double>(get_default_sensor_value(sensor_values, fmisid, temppos));
          sensor_values = data.at(rhpos);
          float rh = boost::get<double>(get_default_sensor_value(sensor_values, fmisid, rhpos));
          sensor_values = data.at(windpos);
          float wind = boost::get<double>(get_default_sensor_value(sensor_values, fmisid, windpos));

          Spine::TimeSeries::Value feelslike =
              Spine::TimeSeries::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
          timeSeriesColumns->at(pos).emplace_back(
              Spine::TimeSeries::TimedValue(obstime, feelslike));
        }
      }
      else if (special.first.find("smartsymbol") != std::string::npos)
      {
        int wawapos = Fmi::stoi(itsParameterMap->getParameter("wawa", stationtype));
        int totalcloudcoverpos =
            Fmi::stoi(itsParameterMap->getParameter("totalcloudcover", stationtype));
        int temppos = Fmi::stoi(itsParameterMap->getParameter("temperature", stationtype));
        if (data.count(wawapos) == 0 || data.count(totalcloudcoverpos) == 0 ||
            data.count(temppos) == 0)
        {
          Spine::TimeSeries::Value missing = Spine::TimeSeries::None();
          timeSeriesColumns->at(pos).emplace_back(Spine::TimeSeries::TimedValue(obstime, missing));
        }
        else
        {
          auto sensor_values = data.at(temppos);
          float temp = boost::get<double>(get_default_sensor_value(sensor_values, fmisid, temppos));
          sensor_values = data.at(totalcloudcoverpos);
          int totalcloudcover = static_cast<int>(boost::get<double>(
              get_default_sensor_value(sensor_values, fmisid, totalcloudcoverpos)));
          sensor_values = data.at(wawapos);
          int wawa = static_cast<int>(
              boost::get<double>(get_default_sensor_value(sensor_values, fmisid, wawapos)));

          double lat = station.latitude_out;
          double lon = station.longitude_out;
          Spine::TimeSeries::Value smartsymbol = Spine::TimeSeries::Value(
              *calcSmartsymbolNumber(wawa, totalcloudcover, temp, obstime, lat, lon));
          timeSeriesColumns->at(pos).emplace_back(
              Spine::TimeSeries::TimedValue(obstime, smartsymbol));
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

    fetchWeatherDataQCData(query, stationInfo, stationgroup_codes, qmap, weatherDataQCData);

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

      Spine::TimeSeries::Value val;
      if (weatherDataQCData.data_valuesAll[i])
        val = Spine::TimeSeries::Value(*weatherDataQCData.data_valuesAll[i]);

      Spine::TimeSeries::Value val_quality = Spine::TimeSeries::None();
      if (weatherDataQCData.data_qualityAll[i])
        val_quality = Spine::TimeSeries::Value(*weatherDataQCData.data_qualityAll[i]);

      bool data_from_default_sensor = (sensor_no == 1);

      station_data[fmisid][obstime][measurand_id][sensor_no] =
          DataWithQuality(val, val_quality, Spine::TimeSeries::None(), data_from_default_sensor);
      i++;
    }

    return buildTimeseries(stations,
                           settings,
                           stationtype,
                           fmisid_to_station,
                           station_data,
                           qmap,
                           timeSeriesOptions,
                           timezones);
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
