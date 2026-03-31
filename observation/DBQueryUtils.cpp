#include "DBQueryUtils.h"
#include "DataWithQuality.h"
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

TS::Value get_default_sensor_value(const SensorData &sensor_data,
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
                           DataFieldSpecifier specifier = DataFieldSpecifier::Value)
{
  try
  {
    if (sensor_data.empty())
      return TS::None();

    if (sensor_no == "default" || sensor_no.empty())
      return get_default_sensor_value(sensor_data, specifier);

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

SpecialParamEntry::Kind windCompassKind(const std::string &name)
{
  if (name == "windcompass8") return SpecialParamEntry::Kind::WindCompass8;
  if (name == "windcompass16") return SpecialParamEntry::Kind::WindCompass16;
  return SpecialParamEntry::Kind::WindCompass32;
}

SpecialParamEntry::Kind cloudCeilingKind(const std::string &name)
{
  if (name == "cloudceilingft") return SpecialParamEntry::Kind::CloudCeilingFt;
  if (name == "cloudceilinghft") return SpecialParamEntry::Kind::CloudCeilingHFt;
  return SpecialParamEntry::Kind::CloudCeiling;
}

// Resolves the measurand_id for a data_source special parameter by searching parameterNameMap
// for the matching sensornumber entry of the master parameter.
SpecialParamEntry resolveDataSourceEntry(const std::string &name,
                                         unsigned pos,
                                         const QueryMapping &qmap)
{
  SpecialParamEntry entry;
  entry.output_pos = static_cast<int>(pos);
  entry.kind = SpecialParamEntry::Kind::DataSource;
  entry.sensor_number = name.substr(name.rfind('_') + 1);
  const auto masterParamName = name.substr(0, name.find("_data_source_sensornumber_"));
  for (const auto &item : qmap.parameterNameMap)
  {
    if (boost::algorithm::starts_with(item.first, masterParamName + "_sensornumber_"))
    {
      entry.measurand_id = Fmi::stoi(item.second);
      break;
    }
  }
  return entry;
}

SpecialParamEntry buildSpecialParamEntry(const std::string &name,
                                         unsigned pos,
                                         bool isQCTable,
                                         const ParameterMapPtr &parameterMap,
                                         const std::string &stationtype,
                                         const QueryMapping &qmap)
{
  SpecialParamEntry entry;
  entry.output_pos = static_cast<int>(pos);

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
    entry.kind = windCompassKind(name);
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
    entry.kind = cloudCeilingKind(name);
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
  else if (is_data_source_field(name))
  {
    return resolveDataSourceEntry(name, pos, qmap);
  }
  else if (is_data_quality_field(name))
  {
    entry.kind = SpecialParamEntry::Kind::DataQuality;
    entry.sensor_number = name.substr(name.rfind('_') + 1);
  }
  else
  {
    entry.kind = SpecialParamEntry::Kind::Other;
    entry.name = name;
  }
  return entry;
}

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

  // Special parameters — each entry is built by a separate function to keep this loop flat
  for (const auto &special : qmap.specialPositions)
    result.special.push_back(
        buildSpecialParamEntry(special.first, special.second, isQCTable, parameterMap, stationtype, qmap));

  return result;
}

// Compute valid timestep sets from sorted observations (no LocalDateTime map key needed)
// Case 2 of computeValidTimestepsSorted: find the single closest timestamp per fmisid to wantedtime.
std::map<int, std::set<Fmi::LocalDateTime>> computeWantedTimestepsSorted(
    const std::vector<const LocationDataItem *> &sorted_obs,
    const Settings &settings,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station)
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
      rng.first_utctime = rng.closest_utctime = utctime;
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

  std::map<int, std::set<Fmi::LocalDateTime>> result;
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

// Case 4 of computeValidTimestepsSorted: only the generated (listed) timesteps, assigned to all
// fmisids found in sorted_obs.
std::map<int, std::set<Fmi::LocalDateTime>> computeListedTimestepsSorted(
    const std::vector<const LocationDataItem *> &sorted_obs,
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station)
{
  std::map<int, std::set<Fmi::LocalDateTime>> result;
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

// Branch 1 of getValidTimeSteps: collect all data timestamps per fmisid.
TimestepsByFMISID collectAllDataTimesteps(const std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results)
{
  TimestepsByFMISID fmisid_timesteps;
  for (const auto &item : fmisid_results)
  {
    const int fmisid = item.first;
    for (const auto &ts : *item.second)
      for (const auto &tv : ts)
        fmisid_timesteps[fmisid].insert(tv.time);
  }
  return fmisid_timesteps;
}

// Branch 3 of getValidTimeSteps: union of data timestamps and generated timestamps for all fmisids.
TimestepsByFMISID collectDataAndListedTimesteps(
    const std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const Settings &settings)
{
  std::set<int> fmisids;
  std::set<Fmi::LocalDateTime> timesteps;
  for (const auto &item : fmisid_results)
  {
    fmisids.insert(item.first);
    for (const auto &ts : *item.second)
      for (const auto &tv : ts)
        timesteps.insert(tv.time);
  }
  const auto tlist = TS::TimeSeriesGenerator::generate(
      timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
  timesteps.insert(tlist.begin(), tlist.end());

  TimestepsByFMISID result;
  for (const int fmisid : fmisids)
    result[fmisid].insert(timesteps.begin(), timesteps.end());
  return result;
}

std::map<int, std::set<Fmi::LocalDateTime>> computeValidTimestepsSorted(
    const std::vector<const LocationDataItem *> &sorted_obs,
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    const StationMap &fmisid_to_station,
    AdditionalTimestepOption additionalTimestepOption)
{
  // Case 4: only generated (listed) timesteps
  if (!timeSeriesOptions.all() && !settings.wantedtime &&
      additionalTimestepOption == AdditionalTimestepOption::JustRequestedTimesteps)
    return computeListedTimestepsSorted(
        sorted_obs, settings, timeSeriesOptions, timezones, fmisid_to_station);

  // Case 2: closest timestamp to wantedtime per fmisid
  if (settings.wantedtime)
    return computeWantedTimestepsSorted(sorted_obs, settings, timezones, fmisid_to_station);

  // Cases 1 and 3: scan data timestamps
  std::map<int, std::set<Fmi::LocalDateTime>> result;
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

// Case 2 of getValidTimeSteps: find the single closest timestamp per fmisid in already-built
// time series vectors.
TimestepsByFMISID collectWantedTimesteps(const std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results,
                                          const Settings &settings)
{
  TimestepsByFMISID fmisid_timesteps;
  for (const auto &item : fmisid_results)
  {
    const int fmisid = item.first;
    const auto &ts_vector = *item.second;
    for (const auto &series : ts_vector)
    {
      if (series.empty())
        continue;
      if (series.size() == 1 || *settings.wantedtime <= settings.starttime)
      {
        fmisid_timesteps[fmisid].insert(series.front().time);
        continue;
      }
      if (*settings.wantedtime >= settings.endtime)
      {
        fmisid_timesteps[fmisid].insert(series.back().time);
        continue;
      }
      // Find the closest timestamp to wantedtime
      auto best_time = series.front().time;
      auto best_diff = std::abs((*settings.wantedtime - best_time.utc_time()).total_seconds());
      for (const auto &tv : series)
      {
        const auto diff = std::abs((*settings.wantedtime - tv.time.utc_time()).total_seconds());
        if (diff < best_diff)
        {
          best_diff = diff;
          best_time = tv.time;
        }
      }
      fmisid_timesteps[fmisid].insert(best_time);
    }
  }
  return fmisid_timesteps;
}

using GroupData = std::unordered_map<int, SensorData>;

// Collect all observations at a single (fmisid, data_time) into a measurand_id → sensor_no → value map.
// Advances 'it' past the consumed observations.
GroupData collectGroupData(std::vector<const LocationDataItem *>::const_iterator &it,
                           std::vector<const LocationDataItem *>::const_iterator end,
                           int grp_fmisid,
                           const Fmi::DateTime &grp_utctime,
                           bool isWeatherDataQCTable)
{
  GroupData group_data;
  while (it != end && (*it)->data.fmisid == grp_fmisid && (*it)->data.data_time == grp_utctime)
  {
    const auto &obs = **it;
    const bool is_default = isWeatherDataQCTable ? (obs.data.sensor_no == 1)
                                                 : (obs.data.measurand_no == 1);
    const auto value = (obs.data.data_value ? TS::Value(*obs.data.data_value) : TS::None());
    TS::Value dq;
    if (isWeatherDataQCTable)
      dq = obs.data.data_quality ? TS::Value(obs.data.data_quality) : TS::None();
    else
      dq = TS::Value(obs.data.data_quality);
    TS::Value ds;
    if (isWeatherDataQCTable)
      ds = TS::None();
    else
      ds = obs.data.data_source > -1 ? TS::Value(obs.data.data_source) : TS::None();

    group_data[obs.data.measurand_id][obs.data.sensor_no] = DataWithQuality(value, dq, ds, is_default);
    if (!isWeatherDataQCTable)
    {
      group_data[LONGITUDE_MEASURAND_ID][obs.data.sensor_no] =
          DataWithQuality(TS::Value(obs.longitude), dq, ds, is_default);
      group_data[LATITUDE_MEASURAND_ID][obs.data.sensor_no] =
          DataWithQuality(TS::Value(obs.latitude), dq, ds, is_default);
      group_data[ELEVATION_MEASURAND_ID][obs.data.sensor_no] =
          DataWithQuality(TS::Value(obs.elevation), dq, ds, is_default);
    }
    ++it;
  }
  return group_data;
}

// Emit all regular (non-special) parameters for a single (fmisid, timestep) group.
void emitRegularParams(const PrecomputedParams &procParams,
                       const GroupData &group_data,
                       const Fmi::LocalDateTime &ldt,
                       TS::TimeSeriesVectorPtr &resultVector)
{
  for (const auto &rp : procParams.regular)
  {
    TS::Value val = TS::None();
    const auto mit = group_data.find(rp.measurand_id);
    if (mit != group_data.end())
    {
      if (rp.sensor_no < 0)
        val = get_default_sensor_value(mit->second);
      else
      {
        const auto sit = mit->second.find(rp.sensor_no);
        val = (sit != mit->second.end()) ? sit->second.value : TS::None();
      }
    }
    resultVector->at(rp.output_pos).emplace_back(TS::TimedValue(ldt, val));
  }
}

// Compute cloud ceiling value (in meters, feet, or hundreds-of-feet) from group data.
TS::Value computeCloudCeiling(const SpecialParamEntry &sp, const GroupData &group_data)
{
  for (int i = 0; i < 5; i++)
  {
    const auto cla_it = group_data.find(sp.cla_mids[i]);
    const auto clhb_it = group_data.find(sp.clhb_mids[i]);
    if (cla_it != group_data.end() && clhb_it != group_data.end())
    {
      double cla_val = std::get<double>(get_default_sensor_value(cla_it->second));
      double clhb_val = std::get<double>(get_default_sensor_value(clhb_it->second));
      if (cla_val >= 5 && cla_val <= 9)
      {
        if (sp.kind == SpecialParamEntry::Kind::CloudCeilingFt)
          clhb_val *= 3.28;
        else if (sp.kind == SpecialParamEntry::Kind::CloudCeilingHFt)
          clhb_val *= 0.0328;
        return clhb_val;
      }
    }
  }
  return TS::None();
}

// Convert a wind direction value to a wind compass string (8-, 16-, or 32-point rose).
std::string windCompassString(SpecialParamEntry::Kind kind,
                              double wd,
                              const std::string &missingtext)
{
  if (kind == SpecialParamEntry::Kind::WindCompass8)
    return windCompass8(wd, missingtext);
  if (kind == SpecialParamEntry::Kind::WindCompass16)
    return windCompass16(wd, missingtext);
  return windCompass32(wd, missingtext);
}

// Resolve the data_source value for a DataSource special parameter entry.
TS::Value dataSourceValue(const SpecialParamEntry &sp, const GroupData &group_data)
{
  if (sp.measurand_id <= 0)
    return TS::None();
  const auto mit = group_data.find(sp.measurand_id);
  if (mit == group_data.end())
    return TS::None();
  return get_sensor_value(mit->second, sp.sensor_number, DataFieldSpecifier::DataSource);
}

// Resolve the data_quality value for a DataQuality entry ("last match wins").
TS::Value dataQualityValue(const SpecialParamEntry &sp,
                           const GroupData &group_data,
                           const PrecomputedParams &procParams)
{
  TS::Value val = TS::None();
  for (const int mid : procParams.all_quality_measurand_ids)
  {
    const auto mit = group_data.find(mid);
    if (mit != group_data.end())
      val = get_sensor_value(mit->second, sp.sensor_number, DataFieldSpecifier::DataQuality);
  }
  return val;
}

// Emit a single special parameter entry (one switch-case dispatch + try/catch).
void emitSpecialParam(const SpecialParamEntry &sp,
                      const Fmi::LocalDateTime &ldt,
                      const GroupData &group_data,
                      const PrecomputedParams &procParams,
                      const SpecialParameters::Args &args,
                      const Settings &settings,
                      const Spine::Station &station,
                      TS::TimeSeriesVectorPtr &resultVector)
{
  const int pos = sp.output_pos;
  const TS::Value missing = TS::None();
  try
  {
    switch (sp.kind)
    {
    case SpecialParamEntry::Kind::Longitude:
    {
      const auto mit = group_data.find(LONGITUDE_MEASURAND_ID);
      const TS::Value val = (mit != group_data.end()) ? get_default_sensor_value(mit->second) : missing;
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
      break;
    }
    case SpecialParamEntry::Kind::Latitude:
    {
      const auto mit = group_data.find(LATITUDE_MEASURAND_ID);
      const TS::Value val = (mit != group_data.end()) ? get_default_sensor_value(mit->second) : missing;
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
      break;
    }
    case SpecialParamEntry::Kind::Elevation:
    {
      const auto mit = group_data.find(ELEVATION_MEASURAND_ID);
      const TS::Value val = (mit != group_data.end()) ? get_default_sensor_value(mit->second) : missing;
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, val));
      break;
    }
    case SpecialParamEntry::Kind::WindCompass8:
    case SpecialParamEntry::Kind::WindCompass16:
    case SpecialParamEntry::Kind::WindCompass32:
    {
      const auto mit = group_data.find(sp.mid1);
      const TS::Value val = (mit != group_data.end()) ? get_default_sensor_value(mit->second) : missing;
      const TS::Value result =
          (val == TS::None())
              ? missing
              : TS::Value(windCompassString(sp.kind, std::get<double>(val), settings.missingtext));
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, result));
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
        const float wind = std::get<double>(get_default_sensor_value(wit->second));
        const float rh = std::get<double>(get_default_sensor_value(rit->second));
        const float temp = std::get<double>(get_default_sensor_value(tit->second));
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
        const int wawa = static_cast<int>(std::get<double>(get_default_sensor_value(wit->second)));
        const int totalcc = static_cast<int>(std::get<double>(get_default_sensor_value(cit->second)));
        const float temp = std::get<double>(get_default_sensor_value(tit->second));
        const auto value =
            calcSmartsymbolNumber(wawa, totalcc, temp, ldt, station.latitude, station.longitude);
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
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, computeCloudCeiling(sp, group_data)));
      break;
    case SpecialParamEntry::Kind::DataSource:
      resultVector->at(pos).emplace_back(TS::TimedValue(ldt, dataSourceValue(sp, group_data)));
      break;
    case SpecialParamEntry::Kind::DataQuality:
      // Replicate "last match wins" behavior of addSpecialFieldsToTimeSeries
      resultVector->at(pos).emplace_back(
          TS::TimedValue(ldt, dataQualityValue(sp, group_data, procParams)));
      break;
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

// Advance iterator past all observations for the given fmisid.
void skipFmisidGroup(std::vector<const LocationDataItem *>::const_iterator &it,
                     std::vector<const LocationDataItem *>::const_iterator end,
                     int fmisid)
{
  while (it != end && (*it)->data.fmisid == fmisid)
    ++it;
}

// Finalize a completed station: fills resultVector into timeSeriesColumns for the station's valid
// timesteps. Returns timeSeriesColumns if the caller should early-return (no data yet), else nullptr.
// No-op (returns nullptr) when station is nullptr.
TS::TimeSeriesVectorPtr tryFinalizeStation(int fmisid,
                                           const TimestepsByFMISID &fmisid_valid_timesteps,
                                           const std::map<int, std::string> &continuous,
                                           const Spine::Station *station,
                                           const std::string &stationtype,
                                           const Settings &settings,
                                           TS::TimeSeriesVectorPtr &resultVector,
                                           TS::TimeSeriesVectorPtr &timeSeriesColumns)
{
  if (!station)
    return nullptr;
  const auto ts_it = fmisid_valid_timesteps.find(fmisid);
  if (ts_it == fmisid_valid_timesteps.end())
    return nullptr;
  if (resultVector->empty() || resultVector->at(0).empty())
    return timeSeriesColumns;
  fillMissingTimesteps(
      resultVector, timeSeriesColumns, ts_it->second, continuous, *station, stationtype, settings);
  return nullptr;
}

// Process a single (fmisid, utc_time) observation group: collect measurand data and emit all
// parameters. Advances 'it' past consumed observations. Skips if ldt is not in valid_timesteps.
void processObservationGroup(std::vector<const LocationDataItem *>::const_iterator &it,
                             std::vector<const LocationDataItem *>::const_iterator end,
                             int grp_fmisid,
                             const Fmi::DateTime &grp_utctime,
                             const Fmi::LocalDateTime &ldt,
                             const std::set<Fmi::LocalDateTime> &valid_timesteps,
                             bool isWeatherDataQCTable,
                             const PrecomputedParams &procParams,
                             const Spine::Station &station,
                             const std::string &stationtype,
                             const Settings &settings,
                             TS::TimeSeriesVectorPtr &resultVector)
{
  auto group_data = collectGroupData(it, end, grp_fmisid, grp_utctime, isWeatherDataQCTable);
  if (valid_timesteps.find(ldt) == valid_timesteps.end())
    return;
  emitRegularParams(procParams, group_data, ldt, resultVector);
  const Fmi::LocalDateTime now(Fmi::SecondClock::universal_time(), ldt.zone());
  const SpecialParameters::Args args(station, stationtype, ldt, now, settings.timezone, &settings);
  for (const auto &sp : procParams.special)
    emitSpecialParam(sp, ldt, group_data, procParams, args, settings, station, resultVector);
}

}  // namespace

void DBQueryUtils::addDependentMeasurandIds(QueryMapping &ret,
                                            const std::string &name,
                                            const std::string &stationtype) const
{
  auto push = [&](const char *pname)
  { ret.measurandIds.push_back(Fmi::stoi(itsParameterMap->getParameter(pname, stationtype))); };

  if (name.find("windcompass") != std::string::npos)
  {
    push("winddirection");
  }
  else if (name.find("feelslike") != std::string::npos)
  {
    push("windspeedms");
    push("relativehumidity");
    push("temperature");
  }
  else if (name.find("smartsymbol") != std::string::npos)
  {
    push("wawa");
    push("totalcloudcover");
    push("temperature");
  }
  else if (name.find("cloudceiling") != std::string::npos)
  {
    // cloudceiling, cloudceilingft and cloudceilinghft all share the same source measurands
    for (const char *pname :
         {"cla1_pt1m_acc", "cla2_pt1m_acc", "cla3_pt1m_acc", "cla4_pt1m_acc", "cla5_pt1m_acc",
          "clhb1_pt1m_instant", "clhb2_pt1m_instant", "clhb3_pt1m_instant", "clhb4_pt1m_instant",
          "clh5_pt1m_instant"})
      push(pname);
  }
}

void DBQueryUtils::processRegularParam(const Spine::Parameter &p,
                                       std::string name,  // by value: removePrefix mutates it
                                       const std::string &stationtype,
                                       bool isWeatherDataQCTable,
                                       unsigned pos,
                                       QueryMapping &ret,
                                       std::set<int> &mids) const
{
  bool isDataQualityField = removePrefix(name, "qc_");
  if (!isDataQualityField)
    isDataQualityField = (p.getSensorParameter() == "qc");

  const std::string sensor_number_string =
      (p.getSensorNumber() ? Fmi::to_string(*(p.getSensorNumber())) : "default");
  std::string name_plus_sensor_number = name;
  if (isDataQualityField)
    name_plus_sensor_number += "_data_quality";
  name_plus_sensor_number += ("_sensornumber_" + sensor_number_string);

  if (isDataQualityField || is_data_source_field(name_plus_sensor_number))
  {
    ret.specialPositions[name_plus_sensor_number] = pos;
    return;
  }

  const auto sparam = itsParameterMap->getParameter(name, stationtype);
  if (sparam.empty())
    throw Fmi::Exception::Trace(
        BCP, "Parameter " + name + " for stationtype " + stationtype + " not found!");

  const int nparam = isWeatherDataQCTable
                         ? itsParameterMap->getRoadAndForeignIds().stringToInteger(sparam, stationtype)
                         : Fmi::stoi(sparam);

  ret.timeseriesPositionsString[name_plus_sensor_number] = pos;
  ret.parameterNameMap[name_plus_sensor_number] = sparam;
  ret.parameterNameIdMap[name_plus_sensor_number] = nparam;
  ret.paramVector.push_back(nparam);
  if (mids.find(nparam) == mids.end())
    ret.measurandIds.push_back(nparam);
  // -1 indicates default sensor
  const int sensor_number = (p.getSensorNumber() ? *(p.getSensorNumber()) : -1);
  ret.sensorNumberToMeasurandIds[sensor_number].insert(nparam);
  mids.insert(nparam);
}

QueryMapping DBQueryUtils::buildQueryMapping(const Settings &settings,
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
      Fmi::ascii_tolower(name);

      if (not_special(p))
        processRegularParam(p, name, stationtype, isWeatherDataQCTable, pos, ret, mids);
      else
      {
        if (!isWeatherDataQCTable)
          addDependentMeasurandIds(ret, name, stationtype);
        ret.specialPositions[name] = pos;
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



TimestepsByFMISID DBQueryUtils::getValidTimeSteps(
    const Settings &settings,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    std::map<int, TS::TimeSeriesVectorPtr> &fmisid_results) const
{
  // Resolve timesteps for each fmisid
  std::map<int, std::set<Fmi::LocalDateTime>> fmisid_timesteps;

  if (timeSeriesOptions.all() && !settings.wantedtime)
    return collectAllDataTimesteps(fmisid_results);

  if (settings.wantedtime)
    return collectWantedTimesteps(fmisid_results, settings);

  if (!timeSeriesOptions.all() && !settings.wantedtime &&
      itsGetRequestedAndDataTimesteps == AdditionalTimestepOption::RequestedAndDataTimesteps)
    return collectDataAndListedTimesteps(fmisid_results, timeSeriesOptions, timezones, settings);

  // Listed timesteps only
  const auto tlist = TS::TimeSeriesGenerator::generate(
      timeSeriesOptions, timezones.time_zone_from_string(settings.timezone));
  for (const auto &item : fmisid_results)
    fmisid_timesteps[item.first].insert(tlist.begin(), tlist.end());

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


TS::TimeSeriesVectorPtr DBQueryUtils::buildTimeSeriesFromObservations(
    const LocationDataItems &observations,
    const Settings &settings,
    const std::string &stationtype,
    const StationMap &fmisid_to_station,
    const QueryMapping &qmap,
    const TS::TimeSeriesGeneratorOptions &timeSeriesOptions,
    const Fmi::TimeZones &timezones,
    bool isWeatherDataQCTable) const
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
        if (auto result = tryFinalizeStation(current_fmisid, fmisid_valid_timesteps, continuous,
                                             current_station, stationtype, settings,
                                             resultVector, timeSeriesColumns))
          return result;

        // Skip stations not in fmisid_to_station (unknown stations)
        if (fmisid_to_station.find(grp_fmisid) == fmisid_to_station.end())
        {
          skipFmisidGroup(it, sorted_obs.cend(), grp_fmisid);
          current_fmisid = grp_fmisid;
          current_station = nullptr;
          continue;
        }

        current_fmisid = grp_fmisid;
        current_station = &fmisid_to_station.at(grp_fmisid);

        // Update timezone for new station (same lazy logic as buildStationTimedMeasurandData)
        if (settings.timezone == "localtime" && current_station->timezone != current_tz_name)
        {
          current_tz_name = current_station->timezone;
          current_tz = timezones.time_zone_from_string(current_tz_name);
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
        skipFmisidGroup(it, sorted_obs.cend(), grp_fmisid);
        continue;
      }

      // Convert UTC time to LocalDateTime ONCE per (fmisid, utc_time) group, then collect and
      // emit all parameters for this (fmisid, utc_time) group (skips if ldt not in valid set).
      const Fmi::LocalDateTime ldt(grp_utctime, current_tz);
      processObservationGroup(it, sorted_obs.cend(), grp_fmisid, grp_utctime, ldt,
                              ts_it->second, isWeatherDataQCTable, procParams,
                              *current_station, stationtype, settings, resultVector);
    }

    // Finalize the last station
    if (auto result = tryFinalizeStation(current_fmisid, fmisid_valid_timesteps, continuous,
                                         current_station, stationtype, settings,
                                         resultVector, timeSeriesColumns))
      return result;

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
