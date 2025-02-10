#include "DBQueryUtils.h"
#include "SpecialParameters.h"
#include "Utils.h"
#include <newbase/NFmiMetMath.h>  //For FeelsLike calculation
#include <timeseries/ParameterTools.h>

#include <timeseries/TimeSeriesOutput.h>

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

int get_mid(const std::string &param_name,
            const std::string &stationtype,
            const ParameterMapPtr &parameterMap)
{
  auto sparam = parameterMap->getParameter(param_name, stationtype);

  if (stationtype == "foreign" || stationtype == "road")
    return parameterMap->getRoadAndForeignIds().stringToInteger(sparam);

  return Fmi::stoi(sparam);
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

TS::Value get_sensor_value(const SensorData &sensor_data,
                           const std::string &sensor_no,
                           int fmisid,
                           int measurand_id,
                           DataFieldSpecifier specifier = DataFieldSpecifier::Value)
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
          int mid = (!isWeatherDataQCTable
                         ? Fmi::stoi(sparam)
                         : parameterMap->getRoadAndForeignIds().stringToInteger(sparam));
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

}  // namespace

QueryMapping DBQueryUtils::buildQueryMapping(const Settings &settings,
                                             const std::string &stationtype,
                                             bool isWeatherDataQCTable) const
{
  try
  {
    //	std::cout << "DbQueryUtils::buildQueryMapping" << std::endl;

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

StationTimedMeasurandData DBQueryUtils::buildStationTimedMeasurandData(
    const LocationDataItems &observations,
    const Settings &settings,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station) const
{
  StationTimedMeasurandData ret;

  // Avoid calling time_zone_from_string too repeatedly:
  auto current_timezone = settings.timezone;
  auto current_tz = timezones.time_zone_from_string(current_timezone);

  try
  {
    for (const auto &obs : observations)
    {
      int fmisid = obs.data.fmisid;

      // Update current_tz only if necessary
      if (settings.timezone == "localtime")
      {
        const auto &new_timezone = fmisid_to_station.at(fmisid).timezone;
        if (new_timezone != current_timezone)
        {
          current_timezone = new_timezone;
          current_tz = timezones.time_zone_from_string(current_timezone);
        }
      }

      Fmi::LocalDateTime obstime(obs.data.data_time, current_tz);

      auto value = (obs.data.data_value ? TS::Value(*obs.data.data_value) : TS::None());
      auto data_quality = obs.data.data_quality;
      auto data_source = (obs.data.data_source > -1 ? TS::Value(obs.data.data_source) : TS::None());

      bool data_from_default_sensor = (obs.data.measurand_no == 1);

      ret[fmisid][obstime][obs.data.measurand_id][obs.data.sensor_no] =
          DataWithQuality(value, data_quality, data_source, data_from_default_sensor);
      ret[fmisid][obstime][LONGITUDE_MEASURAND_ID][obs.data.sensor_no] = DataWithQuality(
          TS::Value(obs.longitude), data_quality, data_source, data_from_default_sensor);
      ret[fmisid][obstime][LATITUDE_MEASURAND_ID][obs.data.sensor_no] = DataWithQuality(
          TS::Value(obs.latitude), data_quality, data_source, data_from_default_sensor);
      ret[fmisid][obstime][ELEVATION_MEASURAND_ID][obs.data.sensor_no] = DataWithQuality(
          TS::Value(obs.elevation), data_quality, data_source, data_from_default_sensor);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Mapping observations failed!");
  }

  return ret;
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
    std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;
    if (timeSeriesOptions.all() && !settings.wantedtime)
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
    else if (settings.wantedtime)
    {
      // std::cout << "**** WANTED timestep\n";

      for (const auto &item : station_data)
      {
        int fmisid = item.first;
        const auto &timed_measurand_data = item.second;
        auto obstime = find_wanted_time(timed_measurand_data, settings);
        fmisid_timesteps[fmisid].insert(obstime);
      }
    }
    else if (!timeSeriesOptions.all() && !settings.wantedtime &&
             itsGetRequestedAndDataTimesteps == AdditionalTimestepOption::RequestedAndDataTimesteps)
    {
      // std::cout << "**** ALL timesteps in data + listed timesteps\n";
      // All FMISDS must have all timesteps in data and all listed timesteps
      std::set<int> fmisids;
      std::set<Fmi::LocalDateTime> timesteps;
      for (const auto &item : station_data)
      {
        fmisids.insert(item.first);
        const auto &timed_measurand_data = item.second;
        for (const auto &item2 : timed_measurand_data)
        {
          timesteps.insert(item2.first);
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
      // std::cout << "**** LISTED timesteps\n";
      // Listed timesteps
      const auto tlist = TS::TimeSeriesGenerator::generate(
          timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
      for (const auto &item : station_data)
        fmisid_timesteps[item.first].insert(tlist.begin(), tlist.end());
    }

    // std::cout << "station_data:\n" << station_data << std::endl;

    TS::TimeSeriesVectorPtr timeSeriesColumns = initializeResultVector(settings);

    bool addDataQualityField = false;
    bool addDataSourceField = false;

    for (auto const &item : qmap.specialPositions)
    {
      if (isDataSourceField(item.first))
        addDataSourceField = true;
      if (isDataQualityField(item.first))
        addDataQualityField = true;
    }

    std::map<int, std::string> continuous;

    for (const auto &item : qmap.specialPositions)
    {
      if (SpecialParameters::instance().is_supported(item.first))
        continuous.emplace(item.second, item.first);
    }

    TS::TimeSeriesVectorPtr resultVector = initializeResultVector(settings);
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

      const auto &station = fmisid_to_station.at(fmisid);

      for (const auto &data : timed_measurand_data)
      {
        if (valid_timesteps.find(data.first) == valid_timesteps.end())
        {
          //				  std::cout << "Invalid timestep " << data.first << " for
          // station
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
                                 station,
                                 settings,
                                 itsParameterMap);
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

      // If no results found return from here
      if (resultVector->empty() || resultVector->at(0).empty())
        return timeSeriesColumns;

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

        if (timestep_iter != valid_timesteps.cend() &&
            (!new_ts.empty() && *timestep_iter == new_ts.back().time))
          timestep_iter++;

        while (timestep_iter != valid_timesteps.cend())
        {
          fill_missing();
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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
