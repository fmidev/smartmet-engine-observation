#include "DatabaseDriverBase.h"
#include "Utils.h"
#include <macgyver/StringConversion.h>
#include <macgyver/TimeParser.h>
#include <spine/Convenience.h>
#include <timeseries/TimeSeriesInclude.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
using namespace Utils;

std::string DatabaseDriverBase::resolveCacheTableName(const std::string &producer,
                                                      const StationtypeConfig &stationtypeConfig)

{
  try
  {
    if (producer == FLASH_PRODUCER)
      return FLASH_DATA_TABLE;
    if (producer == MAGNETO_PRODUCER)
      return MAGNETOMETER_DATA_TABLE;
    if (producer == NETATMO_PRODUCER)
      return NETATMO_DATA_TABLE;
    if (producer == ROADCLOUD_PRODUCER)
      return ROADCLOUD_DATA_TABLE;
    if (producer == FMI_IOT_PRODUCER)
      return FMI_IOT_DATA_TABLE;
    if (producer == TAPSI_QC_PRODUCER)
      return TAPSI_QC_DATA_TABLE;

    auto tablename = stationtypeConfig.getDatabaseTableNameByStationtype(producer);

    if (tablename == "observation_data_r1")
      return OBSERVATION_DATA_TABLE;
    if (tablename == "weather_data_qc")
      return WEATHER_DATA_QC_TABLE;
    return "";
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string DatabaseDriverBase::resolveDatabaseTableName(const std::string &producer,
                                                         const StationtypeConfig &stationtypeConfig)
{
  std::string tablename;

  try
  {
    if (producer == FLASH_PRODUCER)
      tablename = FLASH_DATA_TABLE;
    else if (producer == MAGNETO_PRODUCER)
      tablename = MAGNETOMETER_DATA_TABLE;
    else if (producer == NETATMO_PRODUCER || producer == ROADCLOUD_PRODUCER ||
             producer == FMI_IOT_PRODUCER || producer == TAPSI_QC_PRODUCER)
      tablename = EXT_OBSDATA_TABLE;
    else if (producer == ICEBUOY_PRODUCER || producer == COPERNICUS_PRODUCER)
      tablename = OBSERVATION_DATA_TABLE;
    else
    {
      tablename = stationtypeConfig.getDatabaseTableNameByStationtype(producer);

      if (tablename == "observation_data_r1")
        tablename = OBSERVATION_DATA_TABLE;
      else if (tablename == "weather_data_qc")
        tablename = WEATHER_DATA_QC_TABLE;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }

  return tablename;
}

DatabaseDriverBase::~DatabaseDriverBase() = default;

void DatabaseDriverBase::readConfig(Spine::ConfigBase &cfg, DatabaseDriverParameters &parameters)
{
  try
  {
    const DatabaseDriverInfoItem &driverInfo =
        parameters.params->databaseDriverInfo.getDatabaseDriverInfo(itsDriverName);

    itsTimer = driverInfo.getIntParameterValue("timer", itsTimer);

    parameters.quiet = driverInfo.getIntParameterValue("quiet", parameters.quiet);
    itsQuiet = parameters.quiet;
    parameters.loadStations =
        driverInfo.getIntParameterValue("loadStations", parameters.loadStations);
    itsLoadStations = parameters.loadStations;
    parameters.connectionTimeoutSeconds =
        driverInfo.getIntParameterValue("connectionTimeout", parameters.connectionTimeoutSeconds);
    parameters.disableAllCacheUpdates = driverInfo.getIntParameterValue(
        "disableAllCacheUpdates", parameters.disableAllCacheUpdates);
    parameters.finCacheUpdateInterval = driverInfo.getIntParameterValue(
        "finCacheUpdateInterval", parameters.finCacheUpdateInterval);
    parameters.extCacheUpdateInterval = driverInfo.getIntParameterValue(
        "extCacheUpdateInterval", parameters.extCacheUpdateInterval);
    parameters.flashCacheUpdateInterval = driverInfo.getIntParameterValue(
        "flashCacheUpdateInterval", parameters.flashCacheUpdateInterval);
    parameters.stationsCacheUpdateInterval = driverInfo.getIntParameterValue(
        "stationsCacheUpdateInterval", parameters.stationsCacheUpdateInterval);
    parameters.magnetometerCacheUpdateInterval = driverInfo.getIntParameterValue(
        "magnetometerCacheUpdateInterval", parameters.magnetometerCacheUpdateInterval);

    // update 10 seconds before max(modified_last) for extra safety with Oracle views
    parameters.updateExtraInterval = driverInfo.getIntParameterValue(
        "parameters.finCacheUpdateInterval", parameters.finCacheUpdateInterval);

    if (!parameters.disableAllCacheUpdates)
    {
      parameters.magnetometerCacheDuration = driverInfo.getIntParameterValue(
          "magnetometerCacheDuration", parameters.magnetometerCacheDuration);
      parameters.finCacheDuration =
          driverInfo.getIntParameterValue("finCacheDuration", parameters.finCacheDuration);
      parameters.finMemoryCacheDuration = driverInfo.getIntParameterValue(
          "finMemoryCacheDuration", parameters.finMemoryCacheDuration);
      parameters.extCacheDuration =
          driverInfo.getIntParameterValue("extCacheDuration", parameters.extCacheDuration);
      parameters.flashCacheDuration =
          driverInfo.getIntParameterValue("flashCacheDuration", parameters.flashCacheDuration);
      parameters.flashMemoryCacheDuration = driverInfo.getIntParameterValue(
          "flashMemoryCacheDuration", parameters.flashMemoryCacheDuration);
      parameters.finCacheUpdateSize =
          driverInfo.getIntParameterValue("finCacheUpdateSize", parameters.finCacheUpdateSize);
      parameters.extCacheUpdateSize =
          driverInfo.getIntParameterValue("extCacheUpdateSize", parameters.extCacheUpdateSize);
    }

    if (driverInfo.getStringParameterValue("flash_emulator_active", "false") == "true")
    {
      parameters.flashEmulator.active = true;
      parameters.flashEmulator.bbox = Spine::BoundingBox(
          driverInfo.getStringParameterValue("flash_emulator_bbox", "20,60,30,70"));
      parameters.flashEmulator.strokes_per_minute =
          driverInfo.getIntParameterValue("flash_emulator_strokes", 10);
    }

    readMetaData(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(
        BCP, "Reading database driver configuration failed for " + itsDriverName);
  }
}

void DatabaseDriverBase::readMetaData(Spine::ConfigBase &cfg)
{
  // iterate stationtypes and find out metaparameters
  // metaparameter are defined in 'meta_data.bbox'group like 'meta_data.bbox.<producer>= value'
  // for example meta_data.bbox.flash="18.0,59.0,33.0,70.0,EPSG:4326"
  std::vector<std::string> stationtypes =
      cfg.get_mandatory_config_array<std::string>("stationtypes");

  for (const std::string &type : stationtypes)
  {
    if (type.empty())
      continue;

    // bbox
    auto bbox = cfg.get_optional_config_param<std::string>("meta_data.bbox." + type, "");
    if (bbox.empty())
      bbox = cfg.get_optional_config_param<std::string>(
          "meta_data.bbox.default",
          "-180.0,-90.0,180.0,90.0,EPSG:4326");  // default value: whole world

    // first observation
    auto first_observation_time =
        cfg.get_optional_config_param<std::string>("meta_data.first_observation." + type, "");
    if (first_observation_time.empty())
      first_observation_time = cfg.get_optional_config_param<std::string>(
          "meta_data.first_observation.default",
          "190001010000");  // default value: 1900.01.01 00:00

    // last observation time can be given in configuration file (for regression tests)
    auto last_observation_time =
        cfg.get_optional_config_param<std::string>("meta_data.last_observation." + type, "");
    if (last_observation_time.empty())
      last_observation_time =
          cfg.get_optional_config_param<std::string>("meta_data.last_observation.default",
                                                     "now");  // default value: 1900.01.01 00:00
    bool fixedPeriodEndTime = (last_observation_time != "now");

    Fmi::DateTime obs_period_starttime = Fmi::TimeParser::parse(first_observation_time);
    Fmi::DateTime obs_period_endtime =
        (fixedPeriodEndTime ? Fmi::TimeParser::parse(last_observation_time)
                            : Fmi::SecondClock::universal_time());

    Fmi::TimePeriod time_period(obs_period_starttime, obs_period_endtime);

    // timestep
    int timestep = cfg.get_optional_config_param<int>("meta_data.timestep." + type, -1);
    if (timestep == -1)
      timestep = cfg.get_optional_config_param<int>("meta_data.timestep.default" + type,
                                                    1);  // default value 1 min

    Spine::BoundingBox bounding_box(bbox);

    MetaData meta(bounding_box, time_period, timestep);
    meta.fixedPeriodEndTime = fixedPeriodEndTime;

    itsMetaData.insert(make_pair(type, meta));
  }
}

MetaData DatabaseDriverBase::metaData(const std::string &producer, const Settings &settings)
{
  MetaData empty;

  try
  {
    if (itsMetaData.find(producer) != itsMetaData.end())
    {
      auto &ret = itsMetaData.at(producer);
      if (!ret.fixedPeriodEndTime)
      {
        // update period end time
        Fmi::DateTime currentTime = Fmi::SecondClock::universal_time();
        // subtract seconds so we have even minutes
        long sec = currentTime.time_of_day().seconds();
        currentTime = currentTime - Fmi::Seconds(sec);
        ret.periodLevelMetaData.period = Fmi::TimePeriod(ret.period().begin(), currentTime);
      }

      return metaData(producer, settings, ret);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading meta data (PostgreSQL database driver) failed!");
  }

  return empty;
}

std::shared_ptr<ObservationCache> DatabaseDriverBase::resolveCache(
    const std::string &producer, const EngineParametersPtr &parameters) const
{
  std::string tablename = resolveCacheTableName(producer, parameters->stationtypeConfig);

  /*
        Is this needed?
  if (tablename.empty())
    logMessage("No cache for producer " + producer, itsQuiet);
  */

  return parameters->observationCacheProxy->getCacheByTableName(tablename);
}

Spine::TaggedFMISIDList DatabaseDriverBase::translateToFMISID(
    const Settings &settings, const StationSettings &stationSettings) const
{
  try
  {
    return itsDatabaseStations->translateToFMISID(settings, stationSettings);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::getStationsByArea(Spine::Stations &stations,
                                           const Settings &settings,
                                           const std::string &wkt) const
{
  try
  {
    itsDatabaseStations->getStationsByArea(stations, settings, wkt);
    stations = removeDuplicateStations(stations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::getStationsByBoundingBox(Spine::Stations &stations,
                                                  const Settings &settings) const
{
  try
  {
    itsDatabaseStations->getStationsByBoundingBox(stations, settings, settings.boundingBox);
    stations = removeDuplicateStations(stations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::getStations(Spine::Stations &stations, const Settings &settings) const
{
  try
  {
    itsDatabaseStations->getStations(stations, settings);
    stations = removeDuplicateStations(stations);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr DatabaseDriverBase::checkForEmptyQuery(Settings &settings) const
{
  try
  {
    TS::TimeSeriesGeneratorOptions timeSeriesOptions;
    timeSeriesOptions.startTime = settings.starttime;
    timeSeriesOptions.endTime = settings.endtime;
    timeSeriesOptions.timeStep = settings.timestep;
    timeSeriesOptions.startTimeUTC = false;
    timeSeriesOptions.endTimeUTC = false;

    return checkForEmptyQuery(settings, timeSeriesOptions);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

TS::TimeSeriesVectorPtr DatabaseDriverBase::checkForEmptyQuery(
    Settings &settings, const TS::TimeSeriesGeneratorOptions &timeSeriesOptions) const
{
  try
  {
    TS::TimeSeriesVectorPtr result = nullptr;

    std::vector<const Spine::Parameter *> fmisidPlaceParams;

    for (const auto &p : settings.parameters)
    {
      if (p.name() == "fmisid" || p.name() == "place")
        fmisidPlaceParams.push_back(&p);
    }

    // If only fmisid and place parameters requested return timeseries with null values
    if (settings.parameters.size() == fmisidPlaceParams.size())
    {
      result = TS::TimeSeriesVectorPtr(new TS::TimeSeriesVector);

      TS::TimeSeriesGenerator::LocalTimeList tlist = TS::TimeSeriesGenerator::generate(
          timeSeriesOptions, itsTimeZones.time_zone_from_string(settings.timezone));
      TS::TimeSeries ts_fmisid;
      TS::TimeSeries ts_place;
      for (const auto &tfmisid : settings.taggedFMISIDs)
      {
        for (const auto &t : tlist)
        {
          if (fmisidPlaceParams.size() == 2)
            ts_place.push_back(TS::TimedValue(t, tfmisid.tag));
          ts_fmisid.push_back(TS::TimedValue(t, tfmisid.fmisid));
        }
      }
      if (fmisidPlaceParams.size() == 2)
      {
        if (fmisidPlaceParams.front()->name() == "place")
        {
          result->emplace_back(ts_place);
          result->emplace_back(ts_fmisid);
        }
        else
        {
          result->emplace_back(ts_fmisid);
          result->emplace_back(ts_place);
        }
      }
      else
        result->emplace_back(ts_fmisid);
    }

    return result;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::parameterSanityCheck(const std::string &stationtype,
                                              const std::vector<Spine::Parameter> &parameters,
                                              const ParameterMap &parameterMap)
{
  try
  {
    // Do sanity check for the parameters
    for (const Spine::Parameter &p : parameters)
    {
      if (not_special(p))
      {
        std::string name = parseParameterName(p.name());
        if (!isParameter(name, stationtype, parameterMap) &&
            !isParameterVariant(name, parameterMap))
        {
          throw Fmi::Exception(BCP, "No parameter name " + name + " configured.");
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Sanity check for parameters failed!");
  }
}

void DatabaseDriverBase::updateProducers(const EngineParametersPtr &p, Settings &settings)
{
  try
  {
    if (p->stationtypeConfig.getUseCommonQueryMethod(settings.stationtype) and
        settings.producer_ids.empty())
    {
      settings.producer_ids =
          p->stationtypeConfig.getProducerIdSetByStationtype(settings.stationtype);

      // If ids from config not found try from database
      if (settings.producer_ids.empty())
        settings.producer_ids = p->producerGroups.getProducerIds(
            settings.stationtype, settings.starttime, settings.endtime);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

Fmi::DateTime DatabaseDriverBase::getLatestDataUpdateTime(const std::string &producer,
                                                          const Fmi::DateTime &from,
                                                          const MeasurandInfo &measurand_info) const
{
  try
  {
    // By default not_a_date_time, the actual database driver will return valid time
    return Fmi::DateTime::NOT_A_DATE_TIME;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::getMeasurandAndProducerIds(const std::string &producer,
                                                    const MeasurandInfo &minfo,
                                                    const EngineParametersPtr &ep,
                                                    std::string &producerIds,
                                                    std::string &measurandIds) const
{
  try
  {
    const auto &pids = ep->stationtypeConfig.getProducerIdSetByStationtype(producer);
    for (const auto &pid : pids)
    {
      if (!producerIds.empty())
        producerIds.append(",");
      producerIds.append(Fmi::to_string(pid));
    }

    MeasurandInfo actual_minfo;
    if (producer == FOREIGN_PRODUCER || producer == ROAD_PRODUCER)
    {
      auto producer_params = ep->getProducerParameters(producer);
      for (const auto &p : producer_params)
      {
        measurand_info p_minfo;
        p_minfo.measurand_id = ("'" + p + "'");
        actual_minfo[p] = p_minfo;
      }
    }
    else
    {
      actual_minfo = minfo;
    }

    for (const auto &item : actual_minfo)
    {
      const auto &mi = item.second;
      //		const auto& producers = mi.producers;
      for (const auto &pid : pids)
      {
        if (mi.producers.find(pid) != mi.producers.end())
        {
          if (!measurandIds.empty())
            measurandIds.append(",");
          measurandIds.append(mi.measurand_id);
          break;
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
