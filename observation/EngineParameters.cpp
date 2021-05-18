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
ParameterMapPtr createParameterMapping(Spine::ConfigBase &cfg)
{
  try
  {
    ParameterMapPtr ret;
    auto *pm = new ParameterMap();

    try
    {
      // Use parameter mapping container like this: parameterMap.getParameter("parameter",
      // "station_type") Example: parameterMap.getParameter("t2m", "road")

      // Phase 1: Establish producer setting
      std::vector<std::string> param_names =
          cfg.get_mandatory_config_array<std::string>("parameters");

      // Phase 2: Parse parameter conversions

      for (const std::string &paramname : param_names)
      {
        if (Fmi::ascii_tolower_copy(paramname).compare(0, 3, "qc_") == 0)
          throw Fmi::Exception(
              BCP,
              "Observation error: Parameter aliases with 'qc_' prefix are not allowed. Fix the '" +
                  paramname + "' parameter.");

        auto &param = cfg.get_mandatory_config_param<libconfig::Setting &>(paramname);
        cfg.assert_is_group(param);

        const std::string lower_parame_name = Fmi::ascii_tolower_copy(paramname);

        std::map<std::string, std::string> p;
        for (int j = 0; j < param.getLength(); ++j)
        {
          std::string name = param[j].getName();
          p.insert(std::make_pair(name, static_cast<const char *>(param[j])));
        }

        if (pm->find(lower_parame_name) != pm->end())
          throw Fmi::Exception(
              BCP, "Observation error: Duplicate parameter alias '" + paramname + "' found.");

        // All internal comparisons between parameter names are done with lower case names
        // to prevent confusion and typos
        pm->addStationParameterMap(lower_parame_name, p);
      }
    }
    catch (const libconfig::ConfigException &)
    {
      cfg.handle_libconfig_exceptions("createParameterMapping");
    }

    ret.reset(pm);
    return ret;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

}  // anonymous namespace

EngineParameters::EngineParameters(Spine::ConfigBase &cfg)
{
  try
  {
    quiet = cfg.get_optional_config_param<bool>("quiet", true);

    queryResultBaseCacheSize =
        cfg.get_optional_config_param<size_t>("cache.queryResultBaseCacheSize", 1000);

    serializedStationsFile = cfg.get_mandatory_path("serializedStationsFile");
    dbRegistryFolderPath = cfg.get_mandatory_path("dbRegistryFolderPath");

    dbDriverFile = cfg.get_optional_config_param<std::string>(
        "dbDriverFile", "");  // when  'dummy'-> create dummy drver, otherwise read driver info from
                              // configuration, if no driver configured create dummy driver

    cacheDB = cfg.get_optional_config_param<std::string>("cacheDB", "spatialite");

    parameterMap = createParameterMapping(cfg);
    readStationTypeConfig(cfg);
    readDataQualityConfig(cfg);
    databaseDriverInfo.readConfig(cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Configuration file read failed!");
  }
}

void EngineParameters::readDataQualityConfig(Spine::ConfigBase &cfg)
{
  try
  {
    // Default filter
    auto default_filter =
        cfg.get_optional_config_param<std::string>("data_quality_filter.default_filter", "le 5");

    std::vector<std::string> stationtypes =
        cfg.get_mandatory_config_array<std::string>("stationtypes");

    for (const auto &type : stationtypes)
    {
      if (type.empty())
        continue;

      dataQualityFilters[type] = cfg.get_optional_config_param<std::string>(
          "data_quality_filter.override." + type, default_filter);
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading data quality config failed!");
  }
}

void EngineParameters::readStationTypeConfig(Spine::ConfigBase &cfg)
{
  try
  {
    // Oracle stationtypes
    std::vector<std::string> stationtypes =
        cfg.get_mandatory_config_array<std::string>("stationtypes");
    auto &stationtypelistGroup =
        cfg.get_mandatory_config_param<libconfig::Setting &>("oracle_stationtypelist");
    cfg.assert_is_group(stationtypelistGroup);

    for (const std::string &type : stationtypes)
    {
      if (type.empty())
        continue;

      std::string stationtype_param = "oracle_stationtype." + type;
      if (cfg.get_config().exists(stationtype_param))
      {
        stationTypeMap[type] = cfg.get_mandatory_config_param<std::string>(stationtype_param);

        auto &stationtypeGroup =
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
            throw Fmi::Exception(BCP, "Invalid parameter value!")
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
          throw Fmi::Exception(BCP, "Invalid parameter value!")
              .addDetail(fmt::format("databaseTableName parameter definition is required for the "
                                     "stationtype '{}' if the useCommonQueryMethod value is true.",
                                     type));

        stationtypeConfig.setUseCommonQueryMethod(type, useCommonQueryMethod);
      }
      else
      {
        std::vector<std::string> stationgroupCodeVector;

        // PostgreSQL station types
        stationtype_param = "postgresql_stationtype." + type;
        if (cfg.get_config().exists(stationtype_param))
        {
          std::string producer_name = type;
          std::string producer_id = cfg.get_mandatory_config_param<std::string>(stationtype_param);
          stationTypeMap[producer_name] = producer_id;

          std::string cached_param_key = "postgresql_stationtypelist." + producer_name + ".cached";
          if (cfg.get_config().exists(cached_param_key))
			externalAndMobileProducerConfig.setCached(cfg.get_mandatory_config_param<bool>(cached_param_key));

          std::string tablename_param_key = "postgresql_stationtypelist." + producer_name + ".databaseTableName";
          if (cfg.get_config().exists(tablename_param_key))
			externalAndMobileProducerConfig.setDatabaseTableName(cfg.get_mandatory_config_param<std::string>(tablename_param_key));

          // Sort out measurands for producer
          Measurands measurands;
          ParameterMap::NameToStationParameterMap::const_iterator iter;
          for (iter = parameterMap->begin(); iter != parameterMap->end(); iter++)
          {
            std::string parameter_name = iter->first;
            for (const auto &i : iter->second)
            {
              if (producer_name == i.first)
              {
                std::string parameter_id = i.second;

                // Only measurands added here (parameter_id is integer)
                if (std::string::npos != parameter_id.find_first_not_of("0123456789"))
                  continue;

                measurands.insert(std::make_pair(parameter_name, Fmi::stoi(parameter_id)));
              }
            }
          }
          externalAndMobileProducerConfig.insert(std::make_pair(
              type, ExternalAndMobileProducerMeasurand(Fmi::stoi(producer_id), measurands)));
        }
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading Stationtype config failed!");
  }
}

bool EngineParameters::isParameter(const std::string &alias, const std::string &stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    if (boost::algorithm::ends_with(parameterAliasName, "data_source"))
      return true;

    // Is the alias configured.
    const auto namePtr = parameterMap->find(parameterAliasName);

    if (namePtr == parameterMap->end())
      return false;

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    const auto stationTypeMapPtr = namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineParameters::isParameterVariant(const std::string &name) const
{
  try
  {
    std::string parameterLowerCase = Fmi::ascii_tolower_copy(name);
    Engine::Observation::removePrefix(parameterLowerCase, "qc_");

    if (boost::algorithm::ends_with(parameterLowerCase, "data_source"))
      return true;

    // Is the alias configured.
    const auto namePtr = parameterMap->find(parameterLowerCase);

    if (namePtr == parameterMap->end())
      return false;

    return true;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

std::string EngineParameters::getParameterIdAsString(const std::string &alias,
                                                     const std::string &stationType) const
{
  try
  {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    const auto namePtr = parameterMap->find(parameterAliasName);

    if (namePtr == parameterMap->end())
      return "";

    // Is the stationType configured inside configuration block of the alias.
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);
    std::map<std::string, std::string>::const_iterator stationTypeMapPtr =
        namePtr->second.find(stationTypeLowerCase);

    if (stationTypeMapPtr == namePtr->second.end())
      return "";

    return stationTypeMapPtr->second;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

uint64_t EngineParameters::getParameterId(const std::string &alias,
                                          const std::string &stationType) const
{
  try
  {
    std::string idStr = getParameterIdAsString(alias, stationType);

    // Conversion from string to unsigned int.
    // There is possibility that the configured value is not an integer.
    try
    {
      return Fmi::stoul(idStr);
    }
    catch (...)
    {
      return 0;
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Operation failed!");
  }
}

bool EngineParameters::isExternalOrMobileProducer(const std::string &stationType) const
{
  return (externalAndMobileProducerConfig.find(stationType) !=
          externalAndMobileProducerConfig.end());
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
