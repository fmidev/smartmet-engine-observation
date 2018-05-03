#include "EngineParameters.h"
#include <fmt/format.h>
#include <macgyver/StringConversion.h>
#include <spine/ConfigBase.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
ParameterMap createParameterMapping(Spine::ConfigBase &cfg)
{
  try
  {
    ParameterMap pm;
    namespace ba = boost::algorithm;

    try
    {
      // Use parameter mapping container like this: parameterMap["parameter"]["station_type"]
      // Example: parameterMap["t2m"]["road"]

      // Phase 1: Establish producer setting
      std::vector<std::string> param_names =
          cfg.get_mandatory_config_array<std::string>("parameters");

      // Phase 2: Parse parameter conversions

      for (const std::string &paramname : param_names)
      {
        if (Fmi::ascii_tolower_copy(paramname).compare(0, 3, "qc_") == 0)
          throw Spine::Exception(
              BCP,
              "Observation error: Parameter aliases with 'qc_' prefix are not allowed. Fix the '" +
                  paramname + "' parameter.");

        auto &param = cfg.get_mandatory_config_param<libconfig::Setting &>(paramname);
        cfg.assert_is_group(param);

        std::map<std::string, std::string> p;
        for (int j = 0; j < param.getLength(); ++j)
        {
          std::string name = param[j].getName();
          p.insert(std::make_pair(name, static_cast<const char *>(param[j])));
        }

        const std::string lower_parame_name = Fmi::ascii_tolower_copy(paramname);

        if (pm.find(lower_parame_name) != pm.end())
          throw Spine::Exception(
              BCP, "Observation error: Duplicate parameter alias '" + paramname + "' found.");

        // All internal comparisons between parameter names are done with lower case names
        // to prevent confusion and typos
        pm.insert(make_pair(lower_parame_name, p));
      }
    }
    catch (const libconfig::ConfigException &)
    {
      cfg.handle_libconfig_exceptions("createParameterMapping");
    }

    return pm;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // anonymous namespace

EngineParameters::EngineParameters(Spine::ConfigBase &cfg)
{
  try
  {
    quiet = cfg.get_optional_config_param<bool>("quiet", true);

    locationCacheSize = cfg.get_mandatory_config_param<int>("cache.locationCacheSize");

    queryResultBaseCacheSize =
        cfg.get_optional_config_param<size_t>("cache.queryResultBaseCacheSize", 1000);

    serializedStationsFile = cfg.get_mandatory_path("serializedStationsFile");
    dbRegistryFolderPath = cfg.get_mandatory_path("dbRegistryFolderPath");

    dbDriverFile = cfg.get_optional_config_param<std::string>(
        "dbDriverFile", "");  // when empty or 'dummy'-> create dummy drver, when 'spatialite' ->
                              // create spatialite driver
    observationCacheId =
        cfg.get_optional_config_param<std::string>("observationCacheId", "spatialite");

    readStationTypeConfig(cfg);
    parameterMap = createParameterMapping(cfg);
  }
  catch (...)
  {
    throw Spine::Exception(BCP, "Configuration file read failed!", NULL);
  }
}

void EngineParameters::readStationTypeConfig(Spine::ConfigBase &cfg)
{
  try
  {
    std::vector<std::string> stationtypes =
        cfg.get_mandatory_config_array<std::string>("stationtypes");
    libconfig::Setting &stationtypelistGroup =
        cfg.get_mandatory_config_param<libconfig::Setting &>("oracle_stationtypelist");
    cfg.assert_is_group(stationtypelistGroup);

    for (const std::string &type : stationtypes)
    {
      if (type.empty())
        continue;

      std::string stationtype_param = "oracle_stationtype." + type;
      stationTypeMap[type] = cfg.get_mandatory_config_param<std::string>(stationtype_param);

      libconfig::Setting &stationtypeGroup =
          cfg.get_mandatory_config_param<libconfig::Setting &>("oracle_stationtypelist." + type);
      cfg.assert_is_group(stationtypeGroup);

      bool useCommonQueryMethod =
          cfg.get_optional_config_param<bool>(stationtypeGroup, "useCommonQueryMethod", false);
      bool stationTypeIsCached =
          cfg.get_optional_config_param<bool>(stationtypeGroup, "cached", false);

      // Ignore empty vectors
      std::vector<std::string> stationgroupCodeVector =
          cfg.get_mandatory_config_array<std::string>(stationtypeGroup, "stationGroups");
      if (stationgroupCodeVector.size())
        stationtypeConfig.addStationtype(type, stationgroupCodeVector);

      if (useCommonQueryMethod || stationTypeIsCached)
      {
        std::vector<uint> producerIdVector =
            cfg.get_mandatory_config_array<uint>(stationtypeGroup, "producerIds");
        if (producerIdVector.empty())
          throw Spine::Exception(BCP, "Invalid parameter value!")
              .addDetail(fmt::format(
                  "At least one producer id must be defined into producerIds "
                  "array for the stationtype '{}' if the useCommonQueryMethod value is true.",
                  type));
        stationtypeConfig.setProducerIds(type, producerIdVector);
      }

      std::string databaseTableName =
          cfg.get_optional_config_param<std::string>(stationtypeGroup, "databaseTableName", "");
      if (not databaseTableName.empty())
      {
        stationtypeConfig.setDatabaseTableName(type, databaseTableName);
      }
      else if (useCommonQueryMethod)
        throw Spine::Exception(BCP, "Invalid parameter value!")
            .addDetail(fmt::format("databaseTableName parameter definition is required for the "
                                   "stationtype '{}' if the useCommonQueryMethod value is true.",
                                   type));

      stationtypeConfig.setUseCommonQueryMethod(type, useCommonQueryMethod);
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

Spine::Parameter EngineParameters::makeParameter(const std::string &name) const
{
  try
  {
    if (name.empty())
      throw Spine::Exception(BCP, "Empty parameters are not allowed");

    std::string p = boost::algorithm::to_lower_copy(name, std::locale::classic());
    Spine::Parameter::Type type = Spine::Parameter::Type::Data;

    if (p == "level" || p == "latitude" || p == "longitude" || p == "latlon" || p == "lonlat" ||
        p == "geoid" || p == "place" || p == "stationname" || p == "name" || p == "iso2" ||
        p == "region" || p == "country" || p == "elevation" || p == "sunelevation" ||
        p == "sundeclination" || p == "sunazimuth" || p == "dark" || p == "sunrise" ||
        p == "sunset" || p == "noon" || p == "sunrisetoday" || p == "sunsettoday" ||
        p == "moonphase" || p == "model" || p == "time" || p == "localtime" || p == "utctime" ||
        p == "epochtime" || p == "isotime" || p == "xmltime" || p == "localtz" || p == "tz" ||
        p == "origintime" || p == "wday" || p == "weekday" || p == "mon" || p == "month" ||
        p == "hour" || p == "timestring" || p == "station_name" || p == "distance" ||
        p == "direction" || p == "stationary" || p == "lon" || p == "lat" || p == "stationlon" ||
        p == "stationlat" || p == "stationlongitude" || p == "stationlatitude" ||
        p == "station_elevation" || p == "wmo" || p == "lpnn" || p == "fmisid" || p == "rwsid" ||
        p == "sensor_no")
    {
      type = Spine::Parameter::Type::DataIndependent;
    }
    else if (p == "windcompass8" || p == "windcompass16" || p == "windcompass32" ||
             p == "cloudiness8th" || p == "windchill" || p == "weather" || p == "smartsymbol")
    {
      type = Spine::Parameter::Type::DataDerived;
    }

    return Spine::Parameter(name, type);
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineParameters::isParameter(const std::string &alias, const std::string &stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterAliasName);

    if (namePtr == parameterMap.end())
      return false;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineParameters::isParameterVariant(const std::string &name) const
{
  try
  {
    std::string parameterLowerCase = Fmi::ascii_tolower_copy(name);
    Engine::Observation::removePrefix(parameterLowerCase, "qc_");
    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterLowerCase);

    if (namePtr == parameterMap.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

uint64_t EngineParameters::getParameterId(const std::string &alias,
                                          const std::string &stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator namePtr =
        parameterMap.find(parameterAliasName);

    if (namePtr == parameterMap.end())
      return 0;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return 0;

    // Conversion from string to unsigned int.
    // There is possibility that the configured value is not an integer.
    try
    {
      return Fmi::stoul(stationTypeMapPtr->second);
    }
    catch (...)
    {
      return 0;
    }
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
