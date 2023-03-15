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
    libconfig::Config &config = cfg.get_config();

    // Stationtype settings
    if (config.exists("stationtypelist"))
    {
      libconfig::Setting &stationtypelist_settings = config.lookup("stationtypelist");
      for (int i = 0; i < stationtypelist_settings.getLength(); i++)
      {
        libconfig::Setting &stationtype_settings = stationtypelist_settings[i];

        if (!stationtype_settings.exists("stationtype"))
          throw Fmi::Exception(
              BCP,
              "Invalid stationtypelist configuration. Mandatory setting 'stationtype' missing!");

        auto stationtype = std::string("");
        auto databaseTableName = std::string("");
        std::vector<uint> producerIdVector;

        stationtype_settings.lookupValue("stationtype", stationtype);
        stationtype_settings.lookupValue("databaseTableName", databaseTableName);
        if (stationtype_settings.exists("producerIds"))
          producerIdVector =
              cfg.get_mandatory_config_array<uint>(stationtype_settings, "producerIds");

        // Mobile and external producers
        if (stationtype == "roadcloud" || stationtype == "teconer" || stationtype == "netatmo" ||
            stationtype == "fmi_iot" || stationtype == "bk_hydrometa")
        {
          if (producerIdVector.empty())
            throw Fmi::Exception(BCP, "Invalid parameter value!")
                .addDetail(fmt::format(
                    "One producer id must be defined for external and mobile producers ",
                    stationtype));

          if (databaseTableName.empty())
            databaseTableName = "ext_obsdata";

          // Sort out measurands for mobile and external producers
          Measurands measurands;
          ParameterMap::NameToStationParameterMap::const_iterator iter;
          for (iter = parameterMap->begin(); iter != parameterMap->end(); iter++)
          {
            std::string parameter_name = iter->first;
            for (const auto &i : iter->second)
            {
              if (stationtype == i.first)
              {
                std::string parameter_id = i.second;

                // Only measurands added here (parameter_id is integer)
                if (std::string::npos != parameter_id.find_first_not_of("0123456789"))
                  continue;

                measurands.insert(std::make_pair(parameter_name, Fmi::stoi(parameter_id)));
              }
            }
          }
          externalAndMobileProducerConfig.insert(
              std::make_pair(stationtype,
                             ExternalAndMobileProducerConfigItem(
                                 producerIdVector.front(), measurands, databaseTableName)));
          continue;
        }
        else
        {
          auto useCommonQueryMethod = false;
          auto stationTypeIsCached = false;
          std::vector<std::string> stationgroupCodeVector;

          stationtype_settings.lookupValue("useCommonQueryMethod", useCommonQueryMethod);
          stationtype_settings.lookupValue("cached", stationTypeIsCached);

          if (stationtype_settings.exists("stationGroups"))
            stationgroupCodeVector =
                cfg.get_mandatory_config_array<std::string>(stationtype_settings, "stationGroups");
          else
            stationgroupCodeVector.emplace_back("VOID_AND_MISSING");

          /*
          // Producer ids are now fetched from database
          if ((useCommonQueryMethod || stationTypeIsCached) && producerIdVector.empty())
                {
                  throw Fmi::Exception(BCP, "Invalid parameter value!")
                        .addDetail(fmt::format(
                                                                   "At least one producer id must be
          defined into producerIds " "array for the stationtype '{}' if the useCommonQueryMethod
          value is true.", stationtype));
                }
          */
          if (databaseTableName.empty() && useCommonQueryMethod)
          {
            throw Fmi::Exception(BCP, "Invalid parameter value!")
                .addDetail(
                    fmt::format("databaseTableName parameter definition is required for the "
                                "stationtype '{}' if the useCommonQueryMethod value is true.",
                                stationtype));
          }

          stationtypeConfig.addStationtype(stationtype, stationgroupCodeVector);
          stationtypeConfig.setUseCommonQueryMethod(stationtype, useCommonQueryMethod);
        }
        if (!producerIdVector.empty())
          stationtypeConfig.setProducerIds(stationtype, producerIdVector);
        if (!databaseTableName.empty())
          stationtypeConfig.setDatabaseTableName(stationtype, databaseTableName);
      }
    }
    else
    {
      throw Fmi::Exception::Trace(BCP, "Configuration error: stationtypelist missing!");
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
    return Utils::isParameter(alias, stationType, *parameterMap);
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
    return Utils::isParameterVariant(name, *parameterMap);
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
    Utils::removePrefix(parameterAliasName, "qc_");
    std::string stationTypeLowerCase = Fmi::ascii_tolower_copy(stationType);

    return parameterMap->getParameter(parameterAliasName, stationTypeLowerCase);
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
