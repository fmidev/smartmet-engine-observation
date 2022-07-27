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

DatabaseDriverBase::~DatabaseDriverBase() = default;

void DatabaseDriverBase::parameterSanityCheck(const std::string &stationtype,
                                              const std::vector<Spine::Parameter> &parameters,
                                              const ParameterMap &parameterMap) const
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

void DatabaseDriverBase::updateProducers(const EngineParametersPtr &p, Settings &settings) const
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

    boost::posix_time::ptime obs_period_starttime = Fmi::TimeParser::parse(first_observation_time);
    boost::posix_time::ptime obs_period_endtime =
        (fixedPeriodEndTime ? Fmi::TimeParser::parse(last_observation_time)
                            : boost::posix_time::second_clock::universal_time());

    boost::posix_time::time_period time_period(obs_period_starttime, obs_period_endtime);

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

MetaData DatabaseDriverBase::metaData(const std::string &producer) const
{
  MetaData ret;

  try
  {
    if (itsMetaData.find(producer) != itsMetaData.end())
    {
      ret = itsMetaData.at(producer);
      if (!ret.fixedPeriodEndTime)
      {
        // update period end time
        boost::posix_time::ptime currentTime = boost::posix_time::second_clock::universal_time();
        // subtract seconds so we have even minutes
        long sec = currentTime.time_of_day().seconds();
        currentTime = currentTime - boost::posix_time::seconds(sec);
        ret.period = boost::posix_time::time_period(ret.period.begin(), currentTime);
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading meta data (PostgreSQL database driver) failed!");
  }

  return ret;
}

std::shared_ptr<ObservationCache> DatabaseDriverBase::resolveCache(
    const std::string &producer, const EngineParametersPtr &parameters) const
{
  std::string tablename = resolveCacheTableName(producer, parameters->stationtypeConfig);

  if (tablename.empty())
    logMessage("No cache for producer " + producer, itsQuiet);

  return parameters->observationCacheProxy->getCacheByTableName(tablename);
}

std::string DatabaseDriverBase::resolveCacheTableName(
    const std::string &producer, const StationtypeConfig &stationtypeConfig) const

{
  std::string tablename;

  try
  {
    if (producer == FLASH_PRODUCER)
      tablename = FLASH_DATA_TABLE;
    else if (producer == MAGNETO_PRODUCER)
      tablename = MAGNETOMETER_DATA_TABLE;
    else if (producer == NETATMO_PRODUCER)
      tablename = NETATMO_DATA_TABLE;
    else if (producer == ROADCLOUD_PRODUCER)
      tablename = ROADCLOUD_DATA_TABLE;
    else if (producer == FMI_IOT_PRODUCER)
      tablename = FMI_IOT_DATA_TABLE;
    else if (producer == BK_HYDROMETA_PRODUCER)
      tablename = BK_HYDROMETA_DATA_TABLE;
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

Spine::TaggedFMISIDList DatabaseDriverBase::translateToFMISID(
    const boost::posix_time::ptime &starttime,
    const boost::posix_time::ptime &endtime,
    const std::string &stationtype,
    const StationSettings &stationSettings) const
{
  try
  {
    return itsDatabaseStations->translateToFMISID(starttime, endtime, stationtype, stationSettings);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

void DatabaseDriverBase::getStationsByArea(Spine::Stations &stations,
                                           const std::string &stationtype,
                                           const boost::posix_time::ptime &starttime,
                                           const boost::posix_time::ptime &endtime,
                                           const std::string &wkt) const
{
  try
  {
    itsDatabaseStations->getStationsByArea(stations, stationtype, starttime, endtime, wkt);
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
    itsDatabaseStations->getStationsByBoundingBox(stations, settings);
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
             producer == FMI_IOT_PRODUCER || producer == BK_HYDROMETA_PRODUCER)
      tablename = EXT_OBSDATA_TABLE;
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
      TS::TimeSeries ts_fmisid(settings.localTimePool);
      TS::TimeSeries ts_place(settings.localTimePool);
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
          result->emplace_back(ts_place);
          result->emplace_back(ts_fmisid);
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

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
