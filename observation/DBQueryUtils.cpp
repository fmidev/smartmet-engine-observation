#include "DBQueryUtils.h"
#include "SpecialParameters.h"
#include "Utils.h"
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <timeseries/ParameterTools.h>

#include <timeseries/TimeSeriesOutput.h>
#include <unordered_map>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

#define LONGITUDE_MEASURAND_ID 6767676
#define LATITUDE_MEASURAND_ID 4545454
#define ELEVATION_MEASURAND_ID 2323232

std::ostream &operator<<(std::ostream &out,
                         const Engine::Observation::StationTimedMeasurandData &data)
{
  for (const auto &item1 : data)
  {
    std::cout << "fmisid: " << item1.first << " -> \n";
    for (const auto &item2 : item1.second)
    {
      std::cout << " observationtime: " << item2.first << " -> \n";
      for (const auto &item3 : item2.second)
      {
        std::cout << "  measurand: " << item3.first << " -> \n";
        for (const auto &item4 : item3.second)
        {
          std::cout << "   sensor -> value: " << item4.first << " -> " << item4.second.value << ", "
                    << item4.second.data_quality << ", " << item4.second.data_source << ", "
                    << item4.second.is_default_sensor_data << '\n';
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

int get_mid(const std::string &param_name,
            const std::string &stationtype,
            const ParameterMapPtr &parameterMap)
{
  try
  {
    auto sparam = parameterMap->getParameter(param_name, stationtype);

    if (stationtype == "foreign" || stationtype == "road")
      return parameterMap->getRoadAndForeignIds().stringToInteger(sparam, stationtype);

    return Fmi::stoi(sparam);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

template <typename Container, typename Key>
bool exists(const Container &container, const Key &key)
{
  return (container.find(key) != container.end());
}

bool is_data_source_field(const std::string &fieldname)
{
  return (fieldname.find("_data_source_sensornumber_") != std::string::npos);
}

TS::Value get_default_sensor_value(const SensorData &sensor_data,
                                   int /* fmisid */,
                                   int /* measurand_id */,
                                   DataFieldSpecifier specifier = DataFieldSpecifier::Value)
{
  try
  {
    for (const auto &item : sensor_data)
    {
      if (item.second.is_default_sensor_data)
      {
        if (specifier == DataFieldSpecifier::Value)
          return item.second.value;
        if (specifier == DataFieldSpecifier::DataQuality)
          return item.second.data_quality;
        if (specifier == DataFieldSpecifier::DataSource)
          return item.second.data_source;
      }
    }

    // If no default sensor found return the first
    const auto &default_item = *(sensor_data.begin());

    if (specifier == DataFieldSpecifier::Value)
      return default_item.second.value;
    if (specifier == DataFieldSpecifier::DataQuality)
      return default_item.second.data_quality;
    if (specifier == DataFieldSpecifier::DataSource)
      return default_item.second.data_source;

    return TS::None();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

TS::Value get_sensor_value(const SensorData &sensor_data,
                           const std::string &sensor_no,
                           int fmisid,
                           int measurand_id,
                           DataFieldSpecifier specifier = DataFieldSpecifier::Value)
{
  try
  {
    if (sensor_data.empty())
      return TS::None();

    if (sensor_no == "default" || sensor_no.empty())
      return get_default_sensor_value(sensor_data, fmisid, measurand_id, specifier);

    int sensor_nro = Fmi::stoi(sensor_no);
    if (sensor_data.find(sensor_nro) != sensor_data.end())
    {
      if (specifier == DataFieldSpecifier::Value)
        return sensor_data.at(sensor_nro).value;

      if (specifier == DataFieldSpecifier::DataQuality)
        return sensor_data.at(sensor_nro).data_quality;

      if (specifier == DataFieldSpecifier::DataSource)
        return sensor_data.at(sensor_nro).data_source;
    }

    return TS::None();
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed");
  }
}

bool isDataSourceField(const std::string &fieldname)
{
  return (fieldname.find("_data_source_sensornumber_") != std::string::npos);
}

bool isDataQualityField(const std::string &fieldname)
{
  return (fieldname.length() > 3 &&
          (fieldname.compare(0, 3, "qc_") == 0 ||
           fieldname.find("_data_quality_sensornumber_") != std::string::npos));
}

bool isDataSourceOrDataQualityField(const std::string &fieldname)
{
  return (isDataSourceField(fieldname) || isDataQualityField(fieldname));
}

void addSpecialFieldsToTimeSeries(const TS::TimeSeriesVectorPtr &timeSeriesColumns,
                                  int fmisid,
                                  const TimedMeasurandData &timed_measurand_data,
                                  const std::set<Fmi::LocalDateTime> &valid_timesteps,
                                  const std::map<std::string, int> &specialPositions,
                                  const std::map<std::string, std::string> &parameterNameMap,
                                  bool addDataSourceField)
{
  // Add *data_source- and data_quality-fields
  try
  {
    std::map<int, TS::TimeSeries> data_source_ts;
    std::set<Fmi::LocalDateTime> timesteps;
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
        TS::Value val = TS::None();
        if (addDataSourceField && isDataSourceField(fieldname))
        {
          auto masterParamName = fieldname.substr(0, fieldname.find("_data_source_sensornumber_"));
          if (!masterParamName.empty())
            masterParamName = masterParamName.substr(0, masterParamName.length());
          std::string sensor_number = fieldname.substr(fieldname.rfind('_') + 1);
          for (const auto &item2 : parameterNameMap)
          {
            if (boost::algorithm::starts_with(item2.first, masterParamName + "_sensornumber_"))
            {
              int measurand_id = Fmi::stoi(parameterNameMap.at(item2.first));
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
          if (data_source_ts.find(pos) == data_source_ts.end())
            data_source_ts.insert(std::make_pair(pos, TS::TimeSeries()));
          data_source_ts.at(pos).emplace_back(TS::TimedValue(obstime, val));
          timesteps.insert(obstime);
        }
        else if (!addDataSourceField && isDataQualityField(fieldname))
        {
          // Handle data_quality field
          std::string sensor_number = fieldname.substr(fieldname.rfind('_') + 1);
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
          if (data_source_ts.find(pos) == data_source_ts.end())
            data_source_ts.insert(std::make_pair(pos, TS::TimeSeries()));
          data_source_ts.at(pos).emplace_back(TS::TimedValue(obstime, val));
          timesteps.insert(obstime);
        }
      }
    }

    // Add data to result vector + handle missing time steps
    TS::Value missing = TS::None();
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
          timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(*time_iterator, missing));
          time_iterator++;
        }
        if (time_iterator != timesteps.end() && *time_iterator == obstime)
          time_iterator++;
        timeSeriesColumns->at(pos).push_back(val);
      }
      // Timesteps after last timestep in data
      while (time_iterator != timesteps.end())
      {
        timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(*time_iterator, missing));
        time_iterator++;
      }
    }
  }
  catch (...)
  {
    if (addDataSourceField)
      throw Fmi::Exception::Trace(BCP, "Adding special data source to time series failed!");

    throw Fmi::Exception::Trace(BCP, "Adding special data quality to time series failed!");
  }
}

void addSpecialParameterToTimeSeries(const std::string &paramname,
                                     const TS::TimeSeriesVectorPtr &timeSeriesColumns,
                                     const int pos,
                                     const SpecialParameters::Args &args)
{
  try
  {
    TS::TimedValue value = SpecialParameters::instance().getTimedValue(paramname, args);
    timeSeriesColumns->at(pos).push_back(value);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding special parameter to times series failed!");
  }
}

void addParameterToTimeSeries(const TS::TimeSeriesVectorPtr &timeSeriesColumns,
                              const std::pair<Fmi::LocalDateTime, MeasurandData> &dataItem,
                              int fmisid,
                              const std::map<std::string, int> &specialPositions,
                              const std::map<std::string, int> &parameterNameIdMap,
                              const std::map<std::string, int> &timeseriesPositions,
                              const std::string &stationtype,
                              const Spine::Station &station,
                              const Settings &settings,
                              const ParameterMapPtr &parameterMap)
{
  try
  {
    Fmi::LocalDateTime obstime = dataItem.first;
    const MeasurandData &data = dataItem.second;
    // Append weather parameters
    for (const auto &parameter_item : parameterNameIdMap)
    {
      std::string nameInRequest = parameter_item.first;
      int parameter_id = parameter_item.second;

      TS::Value val = TS::None();

      if (data.count(parameter_id) > 0)
      {
        const auto &sensor_values = data.at(parameter_id);
        std::string name_plus_snumber = parameter_item.first;
        std::string sensor_no = "default";
        if (name_plus_snumber.find("_sensornumber_") != std::string::npos)
          sensor_no = name_plus_snumber.substr(name_plus_snumber.rfind('_') + 1);
        val = get_sensor_value(
            sensor_values, sensor_no, fmisid, parameter_id, DataFieldSpecifier::Value);
      }

      timeSeriesColumns->at(timeseriesPositions.at(nameInRequest))
          .emplace_back(TS::TimedValue(obstime, val));
    }

    Fmi::LocalDateTime now(Fmi::SecondClock::universal_time(), obstime.zone());
    SpecialParameters::Args args(station, stationtype, obstime, now, settings.timezone, &settings);

    TS::Value missing = TS::None();

    for (const auto &special : specialPositions)
    {
      int pos = special.second;

      try
      {
        if (special.first == "longitude" || special.first == "lon" || special.first == "latitude" ||
            special.first == "lat" || special.first == "elevation")
        {
          int mid = 0;
          if (special.first == "longitude" || special.first == "lon")
            mid = LONGITUDE_MEASURAND_ID;
          else if (special.first == "latitude" || special.first == "lat")
            mid = LATITUDE_MEASURAND_ID;
          else if (boost::algorithm::starts_with(special.first, "elevation"))
            mid = ELEVATION_MEASURAND_ID;

          if (mid > 0)
          {
            const auto &sensor_values = data.at(mid);
            TS::Value val = get_default_sensor_value(sensor_values, fmisid, mid);
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, val));
          }
        }
        else if (boost::algorithm::starts_with(special.first, "windcompass"))
        {
          // Have to get wind direction first
          bool isWeatherDataQCTable = (stationtype == "foreign" || stationtype == "road");
          std::string sparam = parameterMap->getParameter("winddirection", stationtype);
          int mid = (!isWeatherDataQCTable ? Fmi::stoi(sparam)
                                           : parameterMap->getRoadAndForeignIds().stringToInteger(
                                                 sparam, stationtype));
          if (data.count(mid) == 0)
          {
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, missing));
          }
          else
          {
            const auto &sensor_values = data.at(mid);

            TS::Value val = get_default_sensor_value(sensor_values, fmisid, mid);

            TS::Value none = TS::None();
            if (val != none)
            {
              std::string windCompass;
              if (special.first == "windcompass8")
                windCompass = windCompass8(std::get<double>(val), settings.missingtext);
              else if (special.first == "windcompass16")
                windCompass = windCompass16(std::get<double>(val), settings.missingtext);
              else if (special.first == "windcompass32")
                windCompass = windCompass32(std::get<double>(val), settings.missingtext);
              TS::Value windCompassValue = TS::Value(windCompass);
              timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, windCompassValue));
            }
          }
        }
        else if (special.first == "feelslike")
        {
          // Feels like - deduction. This ignores radiation, since it is measured
          // using dedicated stations
          int windpos = Fmi::stoi(parameterMap->getParameter("windspeedms", stationtype));
          int rhpos = Fmi::stoi(parameterMap->getParameter("relativehumidity", stationtype));
          int temppos = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));

          if (data.count(windpos) == 0 || data.count(rhpos) == 0 || data.count(temppos) == 0)
          {
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, missing));
          }
          else
          {
            auto sensor_values = data.at(temppos);
            float temp = std::get<double>(get_default_sensor_value(sensor_values, fmisid, temppos));
            sensor_values = data.at(rhpos);
            float rh = std::get<double>(get_default_sensor_value(sensor_values, fmisid, rhpos));
            sensor_values = data.at(windpos);
            float wind = std::get<double>(get_default_sensor_value(sensor_values, fmisid, windpos));

            TS::Value feelslike = TS::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing));
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, feelslike));
          }
        }
        else if (special.first == "smartsymbol")
        {
          int wawapos = Fmi::stoi(parameterMap->getParameter("wawa", stationtype));
          int totalcloudcoverpos =
              Fmi::stoi(parameterMap->getParameter("totalcloudcover", stationtype));
          int temppos = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));
          if (data.count(wawapos) == 0 || data.count(totalcloudcoverpos) == 0 ||
              data.count(temppos) == 0)
          {
            timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, missing));
          }
          else
          {
            auto sensor_values = data.at(temppos);
            float temp = std::get<double>(get_default_sensor_value(sensor_values, fmisid, temppos));
            sensor_values = data.at(totalcloudcoverpos);
            int totalcloudcover = static_cast<int>(std::get<double>(
                get_default_sensor_value(sensor_values, fmisid, totalcloudcoverpos)));
            sensor_values = data.at(wawapos);
            int wawa = static_cast<int>(
                std::get<double>(get_default_sensor_value(sensor_values, fmisid, wawapos)));

            double lat = station.latitude;
            double lon = station.longitude;
            auto value = calcSmartsymbolNumber(wawa, totalcloudcover, temp, obstime, lat, lon);
            if (!value)
              timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, missing));
            else
              timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, TS::Value(*value)));
          }
        }
        else if (special.first == "cloudceiling" || special.first == "cloudceilingft" ||
                 special.first == "cloudceilinghft")
        {
          int cla1_pos = get_mid("cla1_pt1m_acc", stationtype, parameterMap);
          int cla2_pos = get_mid("cla2_pt1m_acc", stationtype, parameterMap);
          int cla3_pos = get_mid("cla3_pt1m_acc", stationtype, parameterMap);
          int cla4_pos = get_mid("cla4_pt1m_acc", stationtype, parameterMap);
          int cla5_pos = get_mid("cla5_pt1m_acc", stationtype, parameterMap);
          int clhb1_pos = get_mid("clhb1_pt1m_instant", stationtype, parameterMap);
          int clhb2_pos = get_mid("clhb2_pt1m_instant", stationtype, parameterMap);
          int clhb3_pos = get_mid("clhb3_pt1m_instant", stationtype, parameterMap);
          int clhb4_pos = get_mid("clhb4_pt1m_instant", stationtype, parameterMap);
          int clh5_pos = get_mid("clh5_pt1m_instant", stationtype, parameterMap);

          std::vector<int> cla_pos_vector;
          std::vector<int> clhb_pos_vector;
          cla_pos_vector.push_back(cla1_pos);
          cla_pos_vector.push_back(cla2_pos);
          cla_pos_vector.push_back(cla3_pos);
          cla_pos_vector.push_back(cla4_pos);
          cla_pos_vector.push_back(cla5_pos);
          clhb_pos_vector.push_back(clhb1_pos);
          clhb_pos_vector.push_back(clhb2_pos);
          clhb_pos_vector.push_back(clhb3_pos);
          clhb_pos_vector.push_back(clhb4_pos);
          clhb_pos_vector.push_back(clh5_pos);

          TS::Value cloudceiling_value = TS::None();
          for (unsigned int i = 0; i < 5; i++)
          {
            auto cla_pos = cla_pos_vector.at(i);
            auto clhb_pos = clhb_pos_vector.at(i);

            if (data.count(cla_pos) > 0 && data.count(clhb_pos) > 0)
            {
              auto cla_sensor_values = data.at(cla_pos);
              auto clhb_sensor_values = data.at(clhb_pos);

              double cla_val =
                  std::get<double>(get_default_sensor_value(cla_sensor_values, fmisid, cla_pos));
              double clhb_val =
                  std::get<double>(get_default_sensor_value(clhb_sensor_values, fmisid, clhb_pos));
              if (cla_val >= 5 && cla_val <= 9)
              {
                if (special.first == "cloudceilingft")
                  clhb_val = (clhb_val * 3.28);
                else if (special.first == "cloudceilinghft")
                  clhb_val = (clhb_val * 0.0328);
                cloudceiling_value = clhb_val;
                break;
              }
            }
          }
          timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, cloudceiling_value));
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
            addSpecialParameterToTimeSeries(fieldname, timeSeriesColumns, pos, args);
          }
        }
      }
      catch (...)
      {
        timeSeriesColumns->at(pos).emplace_back(TS::TimedValue(obstime, missing));
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Adding parameter to time series failed!");
  }
}

// Extract all timesteps from data
std::map<int, std::set<Fmi::LocalDateTime>> getAllTimestepsFromData(
    const StationTimedMeasurandData &station_data)
{
  try
  {
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    for (const auto &item : station_data)
    {
      int fmisid = item.first;
      const auto &timed_measurand_data = item.second;
      for (const auto &item2 : timed_measurand_data)
      {
        fmisid_timesteps[fmisid].insert(item2.first);
      }
    }

    return fmisid_timesteps;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

Fmi::LocalDateTime find_wanted_time(const TimedMeasurandData &timed_measurand_data,
                                    const Settings &settings)
{
  if (timed_measurand_data.size() == 1)  // speed optimization if there is only one choice
    return timed_measurand_data.begin()->first;

  if (*settings.wantedtime <= settings.starttime)  // earliest time
    return timed_measurand_data.begin()->first;

  if (*settings.wantedtime >= settings.endtime)  // latest time
    return timed_measurand_data.rbegin()->first;

  // Look for the closest time

  auto best_time = timed_measurand_data.begin()->first;
  auto best_diff = std::abs((best_time.utc_time() - *settings.wantedtime).total_seconds());

  for (const auto &tmp : timed_measurand_data)
  {
    auto diff = std::abs((tmp.first.utc_time() - *settings.wantedtime).total_seconds());
    if (diff < best_diff)
    {
      best_diff = diff;
      best_time = tmp.first;
    }
  }

  return best_time;
}

// Extract wanted timesteps
std::map<int, std::set<Fmi::LocalDateTime>> getWantedTimesteps(
    const StationTimedMeasurandData &station_data, const Settings &settings)
{
  try
  {
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    for (const auto &item : station_data)
    {
      int fmisid = item.first;
      const auto &timed_measurand_data = item.second;
      auto obstime = find_wanted_time(timed_measurand_data, settings);
      fmisid_timesteps[fmisid].insert(obstime);
    }

    return fmisid_timesteps;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

// Extract data timesteps and requested timesteps
std::map<int, std::set<Fmi::LocalDateTime>> getDataAndRequestedTimesteps(
    const StationTimedMeasurandData &station_data,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const Settings &settings)
{
  try
  {
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    std::set<int> fmisids;
    std::set<Fmi::LocalDateTime> timesteps;

    // Collect all timesteps from data
    for (const auto &item : station_data)
    {
      fmisids.insert(item.first);
      const auto &timed_measurand_data = item.second;
      for (const auto &item2 : timed_measurand_data)
      {
        timesteps.insert(item2.first);
      }
    }

    // Add generated timesteps
    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
    timesteps.insert(tlist.begin(), tlist.end());

    // Assign all timesteps to all stations
    for (const auto &fmisid : fmisids)
      fmisid_timesteps[fmisid].insert(timesteps.begin(), timesteps.end());

    return fmisid_timesteps;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

// Extract listed timesteps
std::map<int, std::set<Fmi::LocalDateTime>> getListedTimesteps(
    const StationTimedMeasurandData &station_data,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const Settings &settings)
{
  try
  {
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    for (const auto &item : station_data)
      fmisid_timesteps[item.first].insert(tlist.begin(), tlist.end());

    return fmisid_timesteps;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

// Extract special field determination
struct SpecialFieldFlags
{
  bool addDataQualityField = false;
  bool addDataSourceField = false;
};

SpecialFieldFlags determineSpecialFields(const QueryMapping &qmap)
{
  SpecialFieldFlags flags;

  for (auto const &item : qmap.specialPositions)
  {
    if (isDataSourceField(item.first))
      flags.addDataSourceField = true;
    if (isDataQualityField(item.first))
      flags.addDataQualityField = true;
  }

  return flags;
}

// Extract continuous parameter map building
std::map<int, std::string> buildContinuousParameterMap(const QueryMapping &qmap)
{
  try
  {
    std::map<int, std::string> continuous;

    for (const auto &item : qmap.specialPositions)
    {
      if (SpecialParameters::instance().is_supported(item.first))
        continuous.emplace(item.second, item.first);
    }

    return continuous;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

// Missing timestep filling logic
void fillMissingTimesteps(const TS::TimeSeriesVectorPtr &resultVector,
                          const TS::TimeSeriesVectorPtr &timeSeriesColumns,
                          const std::set<Fmi::LocalDateTime> &valid_timesteps,
                          const std::map<int, std::string> &continuous,
                          const Spine::Station &station,
                          const std::string &stationtype,
                          const Settings &settings)
{
  // All possible missing timesteps
  for (unsigned int i = 0; i < resultVector->size(); i++)
  {
    auto &ts = resultVector->at(i);

    TS::Value missing_value = TS::None();

    TS::TimeSeries new_ts;

    auto timestep_iter = valid_timesteps.cbegin();

    // TODO: This is rather ugly and perhaps not very efficient.
    //       Perhaps could be optimized sometimes
    const auto fill_missing = [&]() -> void
    {
      auto it = continuous.find(i);
      if (it != continuous.end())
      {
        Fmi::LocalDateTime now(Fmi::SecondClock::universal_time(), timestep_iter->zone());
        SpecialParameters::Args args(
            station, stationtype, *timestep_iter, now, settings.timezone, &settings);
        auto value = SpecialParameters::instance().getTimedValue(it->second, args);
        new_ts.emplace_back(value);
      }
      else
      {
        new_ts.emplace_back(TS::TimedValue(*timestep_iter, missing_value));
      }
    };

    // Merge missing values to actual data if such timestamps have been requested
    for (auto &timed_value : ts)
    {
      while (timestep_iter != valid_timesteps.cend() && *timestep_iter < timed_value.time)
      {
        fill_missing();
        timestep_iter++;
      }
      new_ts.push_back(timed_value);
      timestep_iter++;
    }

    // And fill the end with missing values if needed

    if (timestep_iter != valid_timesteps.cend() &&
        (!new_ts.empty() && *timestep_iter == new_ts.back().time))
      timestep_iter++;

    while (timestep_iter != valid_timesteps.cend())
    {
      fill_missing();
      timestep_iter++;
    }

    // Add the new timeseries result to the output
    auto &pos = timeSeriesColumns->at(i);
    pos.insert(pos.end(), new_ts.begin(), new_ts.end());

    // Old timeseries has been processed
    ts.clear();
  }
}

// Pre-computed info for a regular (non-special) output parameter
struct RegularParamEntry
{
  int output_pos;
  int measurand_id;
  int sensor_no;  // -1 = use default sensor
};

// Pre-computed info for a special/derived output parameter
struct SpecialParamEntry
{
  enum class Kind
  {
    Longitude,
    Latitude,
    Elevation,
    WindCompass8,
    WindCompass16,
    WindCompass32,
    FeelsLike,
    SmartSymbol,
    CloudCeiling,
    CloudCeilingFt,
    CloudCeilingHFt,
    DataSource,
    DataQuality,
    Other
  };

  int output_pos = 0;
  Kind kind{Kind::Other};
  std::string name;                   // for Kind::Other
  int mid1 = 0, mid2 = 0, mid3 = 0;  // pre-resolved measurand IDs
  std::array<int, 5> cla_mids{};
  std::array<int, 5> clhb_mids{};
  int measurand_id = 0;       // for DataSource: the pre-resolved measurand
  std::string sensor_number;  // for DataSource/DataQuality
};

// All pre-computed parameter processing info for a request
struct PrecomputedParams
{
  std::vector<RegularParamEntry> regular;
  std::vector<SpecialParamEntry> special;
  std::vector<int> all_quality_measurand_ids;  // for DataQuality "last match wins"
};

PrecomputedParams buildPrecomputedParams(const QueryMapping &qmap,
                                         const std::string &stationtype,
                                         const ParameterMapPtr &parameterMap)
{
  PrecomputedParams result;
  const bool isQCTable = (stationtype == "foreign" || stationtype == "road");

  // Regular parameters
  for (const auto &item : qmap.parameterNameIdMap)
  {
    RegularParamEntry entry;
    entry.output_pos = qmap.timeseriesPositionsString.at(item.first);
    entry.measurand_id = item.second;
    entry.sensor_no = -1;
    if (item.first.find("_sensornumber_") != std::string::npos)
    {
      const std::string sno = item.first.substr(item.first.rfind('_') + 1);
      if (sno != "default")
        entry.sensor_no = Fmi::stoi(sno);
    }
    result.regular.push_back(entry);
  }

  // All measurand IDs for DataQuality iteration, in parameterNameIdMap order
  for (const auto &item : qmap.parameterNameIdMap)
    result.all_quality_measurand_ids.push_back(item.second);

  // Special parameters
  for (const auto &special : qmap.specialPositions)
  {
    SpecialParamEntry entry;
    entry.output_pos = special.second;
    const std::string &name = special.first;

    if (name == "longitude" || name == "lon")
    {
      entry.kind = SpecialParamEntry::Kind::Longitude;
    }
    else if (name == "latitude" || name == "lat")
    {
      entry.kind = SpecialParamEntry::Kind::Latitude;
    }
    else if (boost::algorithm::starts_with(name, "elevation"))
    {
      entry.kind = SpecialParamEntry::Kind::Elevation;
    }
    else if (boost::algorithm::starts_with(name, "windcompass"))
    {
      if (name == "windcompass8")
        entry.kind = SpecialParamEntry::Kind::WindCompass8;
      else if (name == "windcompass16")
        entry.kind = SpecialParamEntry::Kind::WindCompass16;
      else
        entry.kind = SpecialParamEntry::Kind::WindCompass32;
      if (!isQCTable)
        entry.mid1 = Fmi::stoi(parameterMap->getParameter("winddirection", stationtype));
    }
    else if (name == "feelslike")
    {
      entry.kind = SpecialParamEntry::Kind::FeelsLike;
      if (!isQCTable)
      {
        entry.mid1 = Fmi::stoi(parameterMap->getParameter("windspeedms", stationtype));
        entry.mid2 = Fmi::stoi(parameterMap->getParameter("relativehumidity", stationtype));
        entry.mid3 = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));
      }
    }
    else if (name == "smartsymbol")
    {
      entry.kind = SpecialParamEntry::Kind::SmartSymbol;
      if (!isQCTable)
      {
        entry.mid1 = Fmi::stoi(parameterMap->getParameter("wawa", stationtype));
        entry.mid2 = Fmi::stoi(parameterMap->getParameter("totalcloudcover", stationtype));
        entry.mid3 = Fmi::stoi(parameterMap->getParameter("temperature", stationtype));
      }
    }
    else if (name == "cloudceiling" || name == "cloudceilingft" || name == "cloudceilinghft")
    {
      if (name == "cloudceiling")
        entry.kind = SpecialParamEntry::Kind::CloudCeiling;
      else if (name == "cloudceilingft")
        entry.kind = SpecialParamEntry::Kind::CloudCeilingFt;
      else
        entry.kind = SpecialParamEntry::Kind::CloudCeilingHFt;
      if (!isQCTable)
      {
        entry.cla_mids[0] = get_mid("cla1_pt1m_acc", stationtype, parameterMap);
        entry.cla_mids[1] = get_mid("cla2_pt1m_acc", stationtype, parameterMap);
        entry.cla_mids[2] = get_mid("cla3_pt1m_acc", stationtype, parameterMap);
        entry.cla_mids[3] = get_mid("cla4_pt1m_acc", stationtype, parameterMap);
        entry.cla_mids[4] = get_mid("cla5_pt1m_acc", stationtype, parameterMap);
        entry.clhb_mids[0] = get_mid("clhb1_pt1m_instant", stationtype, parameterMap);
        entry.clhb_mids[1] = get_mid("clhb2_pt1m_instant", stationtype, parameterMap);
        entry.clhb_mids[2] = get_mid("clhb3_pt1m_instant", stationtype, parameterMap);
        entry.clhb_mids[3] = get_mid("clhb4_pt1m_instant", stationtype, parameterMap);
        entry.clhb_mids[4] = get_mid("clh5_pt1m_instant", stationtype, parameterMap);
      }
    }
    else if (isDataSourceField(name))
    {
      entry.kind = SpecialParamEntry::Kind::DataSource;
      const auto masterParamName = name.substr(0, name.find("_data_source_sensornumber_"));
      entry.sensor_number = name.substr(name.rfind('_') + 1);
      for (const auto &item : qmap.parameterNameMap)
      {
        if (boost::algorithm::starts_with(item.first, masterParamName + "_sensornumber_"))
        {
          entry.measurand_id = Fmi::stoi(item.second);
          break;
        }
      }
    }
    else if (isDataQualityField(name))
    {
      entry.kind = SpecialParamEntry::Kind::DataQuality;
      entry.sensor_number = name.substr(name.rfind('_') + 1);
    }
    else
    {
      entry.kind = SpecialParamEntry::Kind::Other;
      entry.name = name;
    }
    result.special.push_back(entry);
  }

  return result;
}

// Compute valid timestep sets from sorted observations (no LocalDateTime map key needed)
std::map<int, std::set<Fmi::LocalDateTime>> computeValidTimestepsSorted(
    const std::vector<const LocationDataItem *> &sorted_obs,
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station,
    AdditionalTimestepOption additionalTimestepOption)
{
  std::map<int, std::set<Fmi::LocalDateTime>> result;

  // Case 4: only generated (listed) timesteps
  if (!timeSeriesOptions.all() && !settings.wantedtime &&
      additionalTimestepOption == AdditionalTimestepOption::JustRequestedTimesteps)
  {
    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
    for (const auto *obs : sorted_obs)
    {
      if (fmisid_to_station.count(obs->data.fmisid))
        result[obs->data.fmisid];  // ensure key exists
    }
    for (auto &kv : result)
      kv.second.insert(tlist.begin(), tlist.end());
    return result;
  }

  // Case 2: closest timestamp to wantedtime per fmisid
  if (settings.wantedtime)
  {
    struct FmisidRange
    {
      Fmi::DateTime first_utctime;
      Fmi::DateTime last_utctime;
      Fmi::DateTime closest_utctime;
      long long closest_diff = 0;
      bool found = false;
    };
    std::map<int, FmisidRange> ranges;

    for (const auto *obs : sorted_obs)
    {
      const int fmisid = obs->data.fmisid;
      if (!fmisid_to_station.count(fmisid))
        continue;
      const Fmi::DateTime &utctime = obs->data.data_time;
      auto &rng = ranges[fmisid];
      const auto diff = std::abs((utctime - *settings.wantedtime).total_seconds());
      if (!rng.found)
      {
        rng.first_utctime = utctime;
        rng.closest_utctime = utctime;
        rng.closest_diff = diff;
        rng.found = true;
      }
      else if (diff < rng.closest_diff)
      {
        rng.closest_diff = diff;
        rng.closest_utctime = utctime;
      }
      rng.last_utctime = utctime;
    }

    for (const auto &kv : ranges)
    {
      const int fmisid = kv.first;
      const auto &rng = kv.second;
      const auto tz = (settings.timezone == "localtime")
                          ? timezones.time_zone_from_string(fmisid_to_station.at(fmisid).timezone)
                          : timezones.time_zone_from_string(settings.timezone);
      Fmi::DateTime winner;
      if (*settings.wantedtime <= settings.starttime)
        winner = rng.first_utctime;
      else if (*settings.wantedtime >= settings.endtime)
        winner = rng.last_utctime;
      else
        winner = rng.closest_utctime;
      result[fmisid].insert(Fmi::LocalDateTime(winner, tz));
    }
    return result;
  }

  // Cases 1 and 3: scan data timestamps
  auto current_tz_name = settings.timezone;
  auto current_tz = timezones.time_zone_from_string(current_tz_name);
  int prev_fmisid = -1;
  Fmi::DateTime prev_utctime;
  bool prev_valid = false;
  std::set<int> observed_fmisids;
  std::set<Fmi::LocalDateTime> all_data_timestamps;  // for case 3

  for (const auto *obs : sorted_obs)
  {
    const int fmisid = obs->data.fmisid;
    if (!fmisid_to_station.count(fmisid))
      continue;
    observed_fmisids.insert(fmisid);

    const Fmi::DateTime &utctime = obs->data.data_time;
    if (fmisid == prev_fmisid && prev_valid && utctime == prev_utctime)
      continue;

    if (fmisid != prev_fmisid && settings.timezone == "localtime")
    {
      const auto &tz_name = fmisid_to_station.at(fmisid).timezone;
      if (tz_name != current_tz_name)
      {
        current_tz_name = tz_name;
        current_tz = timezones.time_zone_from_string(current_tz_name);
      }
    }
    prev_fmisid = fmisid;
    prev_utctime = utctime;
    prev_valid = true;

    Fmi::LocalDateTime ldt(utctime, current_tz);
    if (timeSeriesOptions.all())
      result[fmisid].insert(ldt);  // case 1: per-fmisid data timestamps
    else
      all_data_timestamps.insert(ldt);  // case 3: accumulate across fmisids
  }

  if (!timeSeriesOptions.all())
  {
    // Case 3: union of data timestamps and generated timestamps, assigned to all fmisids
    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
    all_data_timestamps.insert(tlist.begin(), tlist.end());
    for (const int fmisid : observed_fmisids)
      result[fmisid] = all_data_timestamps;
  }

  return result;
}

}  // namespace

QueryMapping DBQueryUtils::buildQueryMapping(const Settings &settings,
                                             const std::string &stationtype,
                                             bool isWeatherDataQCTable) const
{
  try
  {
    //	std::cout << "DbQueryUtils::buildQueryMapping\n";

    QueryMapping ret;

    unsigned int pos = 0;
    std::set<int> mids;
    for (const Spine::Parameter &p : settings.parameters)
    {
      std::string name = p.name();
      Fmi::ascii_tolower(name);

      if (not_special(p))
      {
        bool isDataQualityField = removePrefix(name, "qc_");
        if (!isDataQualityField)
          isDataQualityField = (p.getSensorParameter() == "qc");

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
          auto sparam = itsParameterMap->getParameter(name, stationtype);

          if (!sparam.empty())
          {
            int nparam =
                (!isWeatherDataQCTable ? Fmi::stoi(sparam)
                                       : itsParameterMap->getRoadAndForeignIds().stringToInteger(
                                             sparam, stationtype));

            ret.timeseriesPositionsString[name_plus_sensor_number] = pos;
            ret.parameterNameMap[name_plus_sensor_number] = sparam;
            ret.parameterNameIdMap[name_plus_sensor_number] = nparam;
            ret.paramVector.push_back(nparam);
            if (mids.find(nparam) == mids.end())
              ret.measurandIds.push_back(nparam);
            int sensor_number = (p.getSensorNumber() ? *(p.getSensorNumber()) : -1);
            // -1 indicates default sensor
            if (sensor_number >= -1)
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
        if (name.find("windcompass") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam = Fmi::stoi(itsParameterMap->getParameter("winddirection", stationtype));
            ret.measurandIds.push_back(nparam);
          }
          ret.specialPositions[name] = pos;
        }
        else if (name.find("feelslike") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam1 = Fmi::stoi(itsParameterMap->getParameter("windspeedms", stationtype));
            auto nparam2 =
                Fmi::stoi(itsParameterMap->getParameter("relativehumidity", stationtype));
            auto nparam3 = Fmi::stoi(itsParameterMap->getParameter("temperature", stationtype));
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
            auto nparam1 = Fmi::stoi(itsParameterMap->getParameter("wawa", stationtype));
            auto nparam2 = Fmi::stoi(itsParameterMap->getParameter("totalcloudcover", stationtype));
            auto nparam3 = Fmi::stoi(itsParameterMap->getParameter("temperature", stationtype));
            ret.measurandIds.push_back(nparam1);
            ret.measurandIds.push_back(nparam2);
            ret.measurandIds.push_back(nparam3);
          }
          ret.specialPositions[name] = pos;
        }
        else if (name.find("cloudceiling") != std::string::npos ||
                 name.find("cloudceilingft") != std::string::npos ||
                 name.find("cloudceilinghft") != std::string::npos)
        {
          if (!isWeatherDataQCTable)
          {
            auto nparam1 = Fmi::stoi(itsParameterMap->getParameter("cla1_pt1m_acc", stationtype));
            auto nparam2 = Fmi::stoi(itsParameterMap->getParameter("cla2_pt1m_acc", stationtype));
            auto nparam3 = Fmi::stoi(itsParameterMap->getParameter("cla3_pt1m_acc", stationtype));
            auto nparam4 = Fmi::stoi(itsParameterMap->getParameter("cla4_pt1m_acc", stationtype));
            auto nparam5 = Fmi::stoi(itsParameterMap->getParameter("cla5_pt1m_acc", stationtype));
            auto nparam6 =
                Fmi::stoi(itsParameterMap->getParameter("clhb1_pt1m_instant", stationtype));
            auto nparam7 =
                Fmi::stoi(itsParameterMap->getParameter("clhb2_pt1m_instant", stationtype));
            auto nparam8 =
                Fmi::stoi(itsParameterMap->getParameter("clhb3_pt1m_instant", stationtype));
            auto nparam9 =
                Fmi::stoi(itsParameterMap->getParameter("clhb4_pt1m_instant", stationtype));
            auto nparam10 =
                Fmi::stoi(itsParameterMap->getParameter("clh5_pt1m_instant", stationtype));
            ret.measurandIds.push_back(nparam1);
            ret.measurandIds.push_back(nparam2);
            ret.measurandIds.push_back(nparam3);
            ret.measurandIds.push_back(nparam4);
            ret.measurandIds.push_back(nparam5);
            ret.measurandIds.push_back(nparam6);
            ret.measurandIds.push_back(nparam7);
            ret.measurandIds.push_back(nparam8);
            ret.measurandIds.push_back(nparam9);
            ret.measurandIds.push_back(nparam10);
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


TS::TimeSeriesVectorPtr DBQueryUtils::buildTimeseries(
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const StationTimedMeasurandData &station_data,
    const QueryMapping &qmap,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    // Resolve timesteps for each fmisid
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps =
        resolveTimestepsForStations(settings, station_data, timeSeriesOptions, timezones);

    auto specialFieldFlags = determineSpecialFields(qmap);
    auto continuous = buildContinuousParameterMap(qmap);

    // std::cout << "station_data:\n" << station_data << '\n';

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);
    TS::TimeSeriesVectorPtr resultVector = initializeResultVector(settings);

    for (const auto &item : station_data)
    {
      int fmisid = item.first;

      // Skip unknown stations that might have been added to the DB and where for example found with
      // a BBOX but are not yet known to the server
      if (fmisid_to_station.find(fmisid) == fmisid_to_station.end())
        continue;

      const auto &station = fmisid_to_station.at(fmisid);

      const auto &timed_measurand_data = item.second;
      const auto &valid_timesteps = fmisid_timesteps.at(fmisid);

      for (const auto &data : timed_measurand_data)
      {
        if (valid_timesteps.find(data.first) == valid_timesteps.end())
          continue;

        addParameterToTimeSeries(resultVector,
                                 data,
                                 fmisid,
                                 qmap.specialPositions,
                                 qmap.parameterNameIdMap,
                                 qmap.timeseriesPositionsString,
                                 stationtype,
                                 station,
                                 settings,
                                 itsParameterMap);
      }

      if (specialFieldFlags.addDataSourceField)
        addSpecialFieldsToTimeSeries(resultVector,
                                     fmisid,
                                     timed_measurand_data,
                                     valid_timesteps,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     true);
      if (specialFieldFlags.addDataQualityField)
        addSpecialFieldsToTimeSeries(resultVector,
                                     fmisid,
                                     timed_measurand_data,
                                     valid_timesteps,
                                     qmap.specialPositions,
                                     qmap.parameterNameMap,
                                     false);

      // If no results found return from here
      if (resultVector->empty() || resultVector->at(0).empty())
        return timeSeriesColumns;

      fillMissingTimesteps(resultVector,
                           timeSeriesColumns,
                           valid_timesteps,
                           continuous,
                           station,
                           stationtype,
                           settings);
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series with all timesteps failed!");
  }
}

TimestepsByFMISID DBQueryUtils::getValidTimeSteps(
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results) const
{
  // Resolve timesteps for each fmisid
  std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

  if (timeSeriesOptions.all() && !settings.wantedtime)
  {
    // std::cout << "**** ALL timesteps in data \n";
    // All timesteps
    for (const auto &item : fmisid_results)
    {
      int fmisid = item.first;
      const auto &ts_vector = *item.second;
      for (const auto &item2 : ts_vector)
        for (const auto &item3 : item2)
        {
          fmisid_timesteps[fmisid].insert(item3.time);
        }
    }
  }
  else if (settings.wantedtime)
  {
    // std::cout << "**** WANTED timestep\n";

    for (const auto &item : fmisid_results)
    {
      int fmisid = item.first;
      const auto &ts_vector = *item.second;
      for (const auto &item2 : ts_vector)
      {
        if (item2.size() == 1 ||
            *settings.wantedtime <= settings.starttime)  // quick select for earliest time
          fmisid_timesteps[fmisid].insert(item2.front().time);
        else if (*settings.wantedtime >= settings.endtime)  // quick select for latest time
          fmisid_timesteps[fmisid].insert(item2.back().time);
        else
        {
          // Find closest time
          auto best_time = item2.front().time;
          auto best_diff = std::abs((*settings.wantedtime - best_time.utc_time()).total_seconds());
          for (const auto &item3 : item2)
          {
            auto diff = std::abs((*settings.wantedtime - item3.time.utc_time()).total_seconds());
            if (diff < best_diff)
            {
              best_diff = diff;
              best_time = item3.time;
            }
          }
          fmisid_timesteps[fmisid].insert(best_time);
        }
      }
    }
  }
  else if (!timeSeriesOptions.all() && !settings.wantedtime &&
           itsGetRequestedAndDataTimesteps == AdditionalTimestepOption::RequestedAndDataTimesteps)
  {
    // std::cout << "**** ALL timesteps in data + listed timesteps\n";
    // All FMISDS must have all timesteps in data and all listed timesteps
    std::set<int> fmisids;
    std::set<Fmi::LocalDateTime> timesteps;
    for (const auto &item : fmisid_results)
    {
      int fmisid = item.first;
      fmisids.insert(fmisid);
      const auto &ts_vector = *item.second;
      for (const auto &item2 : ts_vector)
        for (const auto &item3 : item2)
        {
          timesteps.insert(item3.time);
        }
    }

    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    timesteps.insert(tlist.begin(), tlist.end());

    for (const auto &fmisid : fmisids)
      fmisid_timesteps[fmisid].insert(timesteps.begin(), timesteps.end());
  }
  else
  {
    //	  std::cout << "**** LISTED timesteps\n";
    // Listed timesteps
    const auto tlist = TS::TimeSeriesGenerator::generate(
        timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));

    for (const auto &item : fmisid_results)
    {
      int fmisid = item.first;
      fmisid_timesteps[fmisid].insert(tlist.begin(), tlist.end());
    }
  }

  return fmisid_timesteps;
}

void DBQueryUtils::setAdditionalTimestepOption(AdditionalTimestepOption opt)
{
  itsGetRequestedAndDataTimesteps = opt;
}

StationMap DBQueryUtils::mapQueryStations(const Spine::Stations &stations,
                                          const std::set<int> &observed_fmisids) const
{
  try
  {
    StationMap ret;
    for (const Spine::Station &s : stations)
    {
      if (observed_fmisids.find(s.fmisid) == observed_fmisids.end())
        continue;
      ret.insert(std::make_pair(s.fmisid, s));
    }
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Mapping stations failed!");
  }
}

// Build fmisid1,fmisid2,... list
std::set<int> DBQueryUtils::buildStationList(const Spine::Stations &stations,
                                             const std::set<std::string> &stationgroup_codes,
                                             const StationInfo &stationInfo,
                                             const TS::RequestLimits &requestLimits) const
{
  try
  {
    std::set<int> station_ids;
    for (const Spine::Station &s : stations)
      if (stationInfo.belongsToGroup(s.fmisid, stationgroup_codes))
        station_ids.insert(s.fmisid);

    check_request_limit(requestLimits, station_ids.size(), TS::RequestLimitMember::LOCATIONS);
    return station_ids;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building station list failed!");
  }
}

std::string DBQueryUtils::getSensorQueryCondition(
    const std::map<int, std::set<int>> &sensorNumberToMeasurandIds)
{
  try
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
        if (!sensorNumberCondition.empty())
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
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Getting sensor query condition failed!");
  }
}

// Extract timestep resolution logic
std::map<int, std::set<Fmi::LocalDateTime>> DBQueryUtils::resolveTimestepsForStations(
    const Settings &settings,
    const StationTimedMeasurandData &station_data,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

    if (timeSeriesOptions.all() && !settings.wantedtime)
      return getAllTimestepsFromData(station_data);

    if (settings.wantedtime)
      return getWantedTimesteps(station_data, settings);

    if (!timeSeriesOptions.all() && !settings.wantedtime &&
        itsGetRequestedAndDataTimesteps == AdditionalTimestepOption::RequestedAndDataTimesteps)
      return getDataAndRequestedTimesteps(station_data, timeSeriesOptions, timezones, settings);

    return getListedTimesteps(station_data, timeSeriesOptions, timezones, settings);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building query mapping failed!");
  }
}

TS::TimeSeriesVectorPtr DBQueryUtils::buildTimeSeriesFromObservations(
    const LocationDataItems &observations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const QueryMapping &qmap,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones) const
{
  try
  {
    if (observations.empty())
      return initializeResultVector(settings);

    // Pre-compute parameter processing info once per request (avoids per-observation string lookups)
    const auto procParams = buildPrecomputedParams(qmap, stationtype, itsParameterMap);
    const auto continuous = buildContinuousParameterMap(qmap);

    // Sort by (fmisid, data_time) using fast integer/ptime comparison instead of LocalDateTime
    std::vector<const LocationDataItem *> sorted_obs;
    sorted_obs.reserve(observations.size());
    for (const auto &obs : observations)
      sorted_obs.push_back(&obs);
    std::sort(sorted_obs.begin(),
              sorted_obs.end(),
              [](const LocationDataItem *a, const LocationDataItem *b)
              {
                if (a->data.fmisid != b->data.fmisid)
                  return a->data.fmisid < b->data.fmisid;
                return a->data.data_time < b->data.data_time;
              });

    // Compute valid timestep sets from sorted data (first pass, UTC comparisons only)
    const auto fmisid_valid_timesteps = computeValidTimestepsSorted(
        sorted_obs, settings, timeSeriesOptions, timezones, fmisid_to_station,
        itsGetRequestedAndDataTimesteps);

    auto timeSeriesColumns = initializeResultVector(settings);
    auto resultVector = initializeResultVector(settings);

    // Track current timezone lazily (only updated on fmisid change)
    auto current_tz_name = settings.timezone;
    auto current_tz = timezones.time_zone_from_string(current_tz_name);
    int current_fmisid = -1;
    const Spine::Station *current_station = nullptr;

    auto it = sorted_obs.cbegin();
    while (it != sorted_obs.cend())
    {
      const int grp_fmisid = (*it)->data.fmisid;
      const Fmi::DateTime grp_utctime = (*it)->data.data_time;

      // Handle station change
      if (grp_fmisid != current_fmisid)
      {
        // Finalize the previous station's time series
        if (current_station)
        {
          auto ts_it = fmisid_valid_timesteps.find(current_fmisid);
          if (ts_it != fmisid_valid_timesteps.end())
          {
            if (resultVector->empty() || resultVector->at(0).empty())
              return timeSeriesColumns;
            fillMissingTimesteps(resultVector,
                                 timeSeriesColumns,
                                 ts_it->second,
                                 continuous,
                                 *current_station,
                                 stationtype,
                                 settings);
          }
        }

        // Skip stations not in fmisid_to_station (unknown stations)
        if (fmisid_to_station.find(grp_fmisid) == fmisid_to_station.end())
        {
          while (it != sorted_obs.cend() && (*it)->data.fmisid == grp_fmisid)
            ++it;
          current_fmisid = grp_fmisid;
          current_station = nullptr;
          continue;
        }

        current_fmisid = grp_fmisid;
        current_station = &fmisid_to_station.at(grp_fmisid);

        // Update timezone for new station (same lazy logic as buildStationTimedMeasurandData)
        if (settings.timezone == "localtime")
        {
          const auto &tz_name = current_station->timezone;
          if (tz_name != current_tz_name)
          {
            current_tz_name = tz_name;
            current_tz = timezones.time_zone_from_string(current_tz_name);
          }
        }
      }

      if (!current_station)
      {
        ++it;
        continue;
      }

      const auto ts_it = fmisid_valid_timesteps.find(grp_fmisid);
      if (ts_it == fmisid_valid_timesteps.end())
      {
        // Skip all observations for this fmisid
        while (it != sorted_obs.cend() && (*it)->data.fmisid == grp_fmisid)
          ++it;
        continue;
      }

      // Convert UTC time to LocalDateTime ONCE per (fmisid, utc_time) group
      Fmi::LocalDateTime ldt(grp_utctime, current_tz);

      // Collect this group's observations into a temporary unordered_map (O(1) lookups)
      std::unordered_map<int, SensorData> group_data;
      while (it != sorted_obs.cend() && (*it)->data.fmisid == grp_fmisid &&
             (*it)->data.data_time == grp_utctime)
      {
        const auto &obs = **it;
        const bool is_default = (obs.data.measurand_no == 1);
        const auto value = (obs.data.data_value ? TS::Value(*obs.data.data_value) : TS::None());
        const auto dq = obs.data.data_quality;
        const auto ds = (obs.data.data_source > -1 ? TS::Value(obs.data.data_source) : TS::None());

        group_data[obs.data.measurand_id][obs.data.sensor_no] = DataWithQuality(value, dq, ds, is_default);
        group_data[LONGITUDE_MEASURAND_ID][obs.data.sensor_no] =
            DataWithQuality(TS::Value(obs.longitude), dq, ds, is_default);
        group_data[LATITUDE_MEASURAND_ID][obs.data.sensor_no] =
            DataWithQuality(TS::Value(obs.latitude), dq, ds, is_default);
        group_data[ELEVATION_MEASURAND_ID][obs.data.sensor_no] =
            DataWithQuality(TS::Value(obs.elevation), dq, ds, is_default);
        ++it;
      }

      // Skip groups not in the valid timestep set
      if (ts_it->second.find(ldt) == ts_it->second.end())
        continue;

      // Emit regular parameters (pre-computed vector, no string map lookups)
      for (const auto &rp : procParams.regular)
      {
        TS::Value val = TS::None();
        const auto mit = group_data.find(rp.measurand_id);
        if (mit != group_data.end())
        {
          if (rp.sensor_no < 0)
            val = get_default_sensor_value(mit->second, grp_fmisid, rp.measurand_id);
          else
          {
            const std::string sno = Fmi::to_string(rp.sensor_no);
            val = get_sensor_value(
                mit->second, sno, grp_fmisid, rp.measurand_id, DataFieldSpecifier::Value);
          }
        }
        resultVector->at(rp.output_pos).emplace_back(TS::TimedValue(ldt, val));
      }

      // Emit special parameters (pre-computed kinds and measurand IDs)
      const Fmi::LocalDateTime now(Fmi::SecondClock::universal_time(), ldt.zone());
      const SpecialParameters::Args args(
          *current_station, stationtype, ldt, now, settings.timezone, &settings);
      const TS::Value missing = TS::None();

      for (const auto &sp : procParams.special)
      {
        const int pos = sp.output_pos;
        try
        {
          switch (sp.kind)
          {
          case SpecialParamEntry::Kind::Longitude:
          {
            const auto mit = group_data.find(LONGITUDE_MEASURAND_ID);
            const TS::Value val =
                (mit != group_data.end())
                    ? get_default_sensor_value(mit->second, grp_fmisid, LONGITUDE_MEASURAND_ID)
                    : missing;
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
            break;
          }
          case SpecialParamEntry::Kind::Latitude:
          {
            const auto mit = group_data.find(LATITUDE_MEASURAND_ID);
            const TS::Value val =
                (mit != group_data.end())
                    ? get_default_sensor_value(mit->second, grp_fmisid, LATITUDE_MEASURAND_ID)
                    : missing;
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
            break;
          }
          case SpecialParamEntry::Kind::Elevation:
          {
            const auto mit = group_data.find(ELEVATION_MEASURAND_ID);
            const TS::Value val =
                (mit != group_data.end())
                    ? get_default_sensor_value(mit->second, grp_fmisid, ELEVATION_MEASURAND_ID)
                    : missing;
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
            break;
          }
          case SpecialParamEntry::Kind::WindCompass8:
          case SpecialParamEntry::Kind::WindCompass16:
          case SpecialParamEntry::Kind::WindCompass32:
          {
            const auto mit = group_data.find(sp.mid1);
            if (mit == group_data.end())
            {
              resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
            }
            else
            {
              const TS::Value val = get_default_sensor_value(mit->second, grp_fmisid, sp.mid1);
              if (val == TS::None())
              {
                resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
              }
              else
              {
                const double wd = std::get<double>(val);
                std::string compass;
                if (sp.kind == SpecialParamEntry::Kind::WindCompass8)
                  compass = windCompass8(wd, settings.missingtext);
                else if (sp.kind == SpecialParamEntry::Kind::WindCompass16)
                  compass = windCompass16(wd, settings.missingtext);
                else
                  compass = windCompass32(wd, settings.missingtext);
                resultVector->at(pos).emplace_back(TS::TimedValue(ldt, TS::Value(compass)));
              }
            }
            break;
          }
          case SpecialParamEntry::Kind::FeelsLike:
          {
            const auto wit = group_data.find(sp.mid1);  // windspeedms
            const auto rit = group_data.find(sp.mid2);  // relativehumidity
            const auto tit = group_data.find(sp.mid3);  // temperature
            if (wit == group_data.end() || rit == group_data.end() || tit == group_data.end())
            {
              resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
            }
            else
            {
              float wind =
                  std::get<double>(get_default_sensor_value(wit->second, grp_fmisid, sp.mid1));
              float rh =
                  std::get<double>(get_default_sensor_value(rit->second, grp_fmisid, sp.mid2));
              float temp =
                  std::get<double>(get_default_sensor_value(tit->second, grp_fmisid, sp.mid3));
              resultVector->at(pos).emplace_back(
                  TS::TimedValue(ldt, TS::Value(FmiFeelsLikeTemperature(wind, rh, temp, kFloatMissing))));
            }
            break;
          }
          case SpecialParamEntry::Kind::SmartSymbol:
          {
            const auto wit = group_data.find(sp.mid1);  // wawa
            const auto cit = group_data.find(sp.mid2);  // totalcloudcover
            const auto tit = group_data.find(sp.mid3);  // temperature
            if (wit == group_data.end() || cit == group_data.end() || tit == group_data.end())
            {
              resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
            }
            else
            {
              const int wawa = static_cast<int>(
                  std::get<double>(get_default_sensor_value(wit->second, grp_fmisid, sp.mid1)));
              const int totalcc = static_cast<int>(
                  std::get<double>(get_default_sensor_value(cit->second, grp_fmisid, sp.mid2)));
              const float temp =
                  std::get<double>(get_default_sensor_value(tit->second, grp_fmisid, sp.mid3));
              const auto value = calcSmartsymbolNumber(
                  wawa, totalcc, temp, ldt, current_station->latitude, current_station->longitude);
              if (!value)
                resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
              else
                resultVector->at(pos).emplace_back(TS::TimedValue(ldt, TS::Value(*value)));
            }
            break;
          }
          case SpecialParamEntry::Kind::CloudCeiling:
          case SpecialParamEntry::Kind::CloudCeilingFt:
          case SpecialParamEntry::Kind::CloudCeilingHFt:
          {
            TS::Value cloudceiling = TS::None();
            for (int i = 0; i < 5; i++)
            {
              const auto cla_it = group_data.find(sp.cla_mids[i]);
              const auto clhb_it = group_data.find(sp.clhb_mids[i]);
              if (cla_it != group_data.end() && clhb_it != group_data.end())
              {
                double cla_val = std::get<double>(
                    get_default_sensor_value(cla_it->second, grp_fmisid, sp.cla_mids[i]));
                double clhb_val = std::get<double>(
                    get_default_sensor_value(clhb_it->second, grp_fmisid, sp.clhb_mids[i]));
                if (cla_val >= 5 && cla_val <= 9)
                {
                  if (sp.kind == SpecialParamEntry::Kind::CloudCeilingFt)
                    clhb_val *= 3.28;
                  else if (sp.kind == SpecialParamEntry::Kind::CloudCeilingHFt)
                    clhb_val *= 0.0328;
                  cloudceiling = clhb_val;
                  break;
                }
              }
            }
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, cloudceiling));
            break;
          }
          case SpecialParamEntry::Kind::DataSource:
          {
            TS::Value val = TS::None();
            if (sp.measurand_id > 0)
            {
              const auto mit = group_data.find(sp.measurand_id);
              if (mit != group_data.end())
                val = get_sensor_value(mit->second,
                                       sp.sensor_number,
                                       grp_fmisid,
                                       sp.measurand_id,
                                       DataFieldSpecifier::DataSource);
            }
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
            break;
          }
          case SpecialParamEntry::Kind::DataQuality:
          {
            // Replicate "last match wins" behavior of addSpecialFieldsToTimeSeries
            TS::Value val = TS::None();
            for (const int mid : procParams.all_quality_measurand_ids)
            {
              const auto mit = group_data.find(mid);
              if (mit != group_data.end())
                val = get_sensor_value(
                    mit->second, sp.sensor_number, grp_fmisid, mid, DataFieldSpecifier::DataQuality);
            }
            resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
            break;
          }
          default:  // Kind::Other
            addSpecialParameterToTimeSeries(sp.name, resultVector, pos, args);
            break;
          }
        }
        catch (...)
        {
          resultVector->at(pos).emplace_back(TS::TimedValue(ldt, missing));
        }
      }
    }

    // Finalize the last station
    if (current_station)
    {
      const auto ts_it = fmisid_valid_timesteps.find(current_fmisid);
      if (ts_it != fmisid_valid_timesteps.end())
      {
        if (resultVector->empty() || resultVector->at(0).empty())
          return timeSeriesColumns;
        fillMissingTimesteps(resultVector,
                             timeSeriesColumns,
                             ts_it->second,
                             continuous,
                             *current_station,
                             stationtype,
                             settings);
      }
    }

    return timeSeriesColumns;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Building time series from observations failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
