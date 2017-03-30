#include "EngineParameters.h"
#include "DatabaseDriverParameters.h"
#include <macgyver/StringConversion.h>

#include <spine/ConfigBase.h>

namespace SmartMet {
namespace Engine {
namespace Observation {
EngineParameters::EngineParameters(const std::string configfile) {
  try {
    Spine::ConfigBase cfg(configfile);

    // Database may be defined locally as a group or as a group in an external
    // file
    if (!cfg.get_config().exists("database"))
      throw std::runtime_error("Database settings missing");

    auto &cfgsetting = cfg.get_config().lookup("database");
    if (cfgsetting.isGroup()) {
      service = cfg.get_mandatory_config_param<std::string>("database.service");
      username =
          cfg.get_mandatory_config_param<std::string>("database.username");
      password =
          cfg.get_mandatory_config_param<std::string>("database.password");
      nlsLang = cfg.get_optional_config_param<std::string>("database.nls_lang",
                                                           "NLS_LANG=.UTF8");
    } else {
      std::string extfile = cfgsetting;
      Spine::ConfigBase extcfg(extfile);
      service =
          extcfg.get_mandatory_config_param<std::string>("database.service");
      username =
          extcfg.get_mandatory_config_param<std::string>("database.username");
      password =
          extcfg.get_mandatory_config_param<std::string>("database.password");
      nlsLang = extcfg.get_optional_config_param<std::string>(
          "database.nls_lang", "NLS_LANG=.UTF8");
    }

    maxInsertSize = cfg.get_optional_config_param<std::size_t>(
        "maxInsertSize", 9999999999); // default = all at once

    quiet = cfg.get_optional_config_param<bool>("quiet", true);
    timer = cfg.get_optional_config_param<bool>("timer", false);

    threadingMode = cfg.get_optional_config_param<std::string>(
        "sqlite.threading_mode", "SERIALIZED");
    cacheTimeout =
        cfg.get_optional_config_param<size_t>("sqlite.timeout", 30000);
    sharedCache =
        cfg.get_optional_config_param<bool>("sqlite.shared_cache", false);
    memstatus = cfg.get_optional_config_param<bool>("sqlite.memstatus", false);
    synchronous = cfg.get_optional_config_param<std::string>(
        "sqlite.synchronous", "NORMAL");
    journalMode = cfg.get_optional_config_param<std::string>(
        "sqlite.journal_mode", "WAL");
    mmapSize = cfg.get_optional_config_param<long>("sqlite.mmap_size", 0);

    disableAllCacheUpdates = cfg.get_optional_config_param<bool>(
        "cache.disableAllCacheUpdates", false);
    finCacheUpdateInterval = cfg.get_optional_config_param<std::size_t>(
        "cache.finCacheUpdateInterval", 0);
    extCacheUpdateInterval = cfg.get_optional_config_param<std::size_t>(
        "cache.extCacheUpdateInterval", 0);
    flashCacheUpdateInterval = cfg.get_optional_config_param<std::size_t>(
        "cache.flashCacheUpdateInterval", 0);

    finCacheDuration =
        cfg.get_mandatory_config_param<int>("cache.finCacheDuration");
    extCacheDuration =
        cfg.get_mandatory_config_param<int>("cache.extCacheDuration");
    flashCacheDuration =
        cfg.get_mandatory_config_param<int>("cache.extCacheDuration");

    locationCacheSize =
        cfg.get_mandatory_config_param<int>("cache.locationCacheSize");

    queryResultBaseCacheSize = cfg.get_optional_config_param<size_t>(
        "cache.queryResultBaseCacheSize", 1000);

    poolSize = cfg.get_mandatory_config_param<int>("poolsize");
    spatiaLitePoolSize =
        cfg.get_mandatory_config_param<int>("spatialitePoolSize");

    connectionTimeoutSeconds = cfg.get_optional_config_param<size_t>(
        "oracleConnectionPoolGetConnectionTimeOutSeconds", 30);

    spatiaLiteFile =
        cfg.get_mandatory_config_param<std::string>("spatialiteFile");
    serializedStationsFile =
        cfg.get_mandatory_config_param<std::string>("serializedStationsFile");
    dbRegistryFolderPath =
        cfg.get_mandatory_config_param<std::string>("dbRegistryFolderPath");

    dbDriverFile = cfg.get_optional_config_param<std::string>(
        "dbDriverFile", ""); // when empty, create dummy drver
    observationCacheId = cfg.get_optional_config_param<std::string>(
        "observationCacheId", "spatialite");

    readStationTypeConfig(configfile);
    parameterMap = createParameterMapping(configfile);

    observationCacheParameters.reset(new ObservationCacheParameters);
    databaseDriverParameters.reset(new DatabaseDriverParameters);

    observationCacheParameters->cacheId = observationCacheId;
    observationCacheParameters->connectionPoolSize = spatiaLitePoolSize;
    observationCacheParameters->cacheFile = spatiaLiteFile;
    observationCacheParameters->maxInsertSize = maxInsertSize;
    observationCacheParameters->synchronous = synchronous;
    observationCacheParameters->journalMode = journalMode;
    observationCacheParameters->mmapSize = mmapSize;
    observationCacheParameters->threadingMode = threadingMode;
    observationCacheParameters->memstatus = memstatus;
    observationCacheParameters->sharedCache = sharedCache;
    observationCacheParameters->cacheTimeout = cacheTimeout;
    observationCacheParameters->finCacheDuration = finCacheDuration;
    observationCacheParameters->extCacheDuration = extCacheDuration;
    observationCacheParameters->flashCacheDuration = flashCacheDuration;
    observationCacheParameters->quiet = quiet;
    observationCacheParameters->cacheHasStations =
        false; // this is set later by cache
    observationCacheParameters->finCachePeriod = &finCachePeriod;
    observationCacheParameters->extCachePeriod = &extCachePeriod;
    observationCacheParameters->flashCachePeriod = &flashCachePeriod;

    observationCacheParameters->parameterMap = &parameterMap;
    observationCacheParameters->stationtypeConfig = &stationtypeConfig;
    observationCacheParameters->stationInfo = &stationInfo;

    databaseDriverParameters->driverFile = dbDriverFile;
    databaseDriverParameters->service = service;
    databaseDriverParameters->username = username;
    databaseDriverParameters->password = password;
    databaseDriverParameters->nlsLang = nlsLang;
    databaseDriverParameters->connectionPoolSize = poolSize;
    databaseDriverParameters->connectionTimeoutSeconds =
        connectionTimeoutSeconds;
    databaseDriverParameters->geonames =
        0; // this is set later in observation engine
    databaseDriverParameters->parameterMap = &parameterMap;
    databaseDriverParameters->stationInfo = &stationInfo;
    databaseDriverParameters->locationCache = &locationCache;
    databaseDriverParameters->queryResultBaseCache = &queryResultBaseCache;
    databaseDriverParameters->shutdownRequested = &shutdownRequested;
    databaseDriverParameters->connectionsOK = &connectionsOK;
    databaseDriverParameters->stationtypeConfig = &stationtypeConfig;
    databaseDriverParameters->quiet = quiet;
    databaseDriverParameters->quiet = timer;
    databaseDriverParameters->observationCache =
        0; // this is set later in observation engine
  }
  catch (...) {
    throw Spine::Exception(BCP, "Configuration file read failed!", NULL);
  }
}

void EngineParameters::readStationTypeConfig(const std::string &configfile) {
  try {
    Spine::ConfigBase cfg(configfile);

    std::vector<std::string> stationtypes =
        cfg.get_mandatory_config_array<std::string>("stationtypes");
    libconfig::Setting &stationtypelistGroup =
        cfg.get_mandatory_config_param<libconfig::Setting &>(
            "oracle_stationtypelist");
    cfg.assert_is_group(stationtypelistGroup);

    for (const std::string &type : stationtypes) {
      if (type.empty())
        continue;

      std::string stationtype_param = "oracle_stationtype." + type;
      stationTypeMap[type] =
          cfg.get_mandatory_config_param<std::string>(stationtype_param);

      libconfig::Setting &stationtypeGroup =
          cfg.get_mandatory_config_param<libconfig::Setting &>(
              "oracle_stationtypelist." + type);
      cfg.assert_is_group(stationtypeGroup);

      bool useCommonQueryMethod = cfg.get_optional_config_param<bool>(
          stationtypeGroup, "useCommonQueryMethod", false);
      bool stationTypeIsCached = cfg.get_optional_config_param<bool>(
          stationtypeGroup, "cached", false);

      // Ignore empty vectors
      std::vector<std::string> stationgroupCodeVector =
          cfg.get_mandatory_config_array<std::string>(stationtypeGroup,
                                                      "stationGroups");
      if (stationgroupCodeVector.size())
        stationtypeConfig.addStationtype(type, stationgroupCodeVector);

      if (useCommonQueryMethod || stationTypeIsCached) {
        std::vector<uint> producerIdVector =
            cfg.get_mandatory_config_array<uint>(stationtypeGroup,
                                                 "producerIds");
        if (producerIdVector.empty()) {
          std::ostringstream msg;
          msg << "At least one producer id must be defined into producerIds "
                 "array for the "
                 "stationtype '" << type
              << "' if the useCommonQueryMethod value is true.";

          Spine::Exception exception(BCP, "Invalid parameter value!");
          // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
          exception.addDetail(msg.str());
          throw exception;
        }
        stationtypeConfig.setProducerIds(type, producerIdVector);
      }

      std::string databaseTableName =
          cfg.get_optional_config_param<std::string>(stationtypeGroup,
                                                     "databaseTableName", "");
      if (not databaseTableName.empty()) {
        stationtypeConfig.setDatabaseTableName(type, databaseTableName);
      } else if (useCommonQueryMethod) {
        std::ostringstream msg;
        msg << "databaseTableName parameter definition is required for the "
               "stationtype '" << type
            << "' if the useCommonQueryMethod value is true.";

        Spine::Exception exception(BCP, "Invalid parameter value!");
        // exception.setExceptionCode(Obs_EngineException::INVALID_PARAMETER_VALUE);
        exception.addDetail(msg.str());
        throw exception;
      }

      stationtypeConfig.setUseCommonQueryMethod(type, useCommonQueryMethod);
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

Spine::Parameter
EngineParameters::makeParameter(const std::string &name) const {
  try {
    if (name.empty())
      throw Spine::Exception(BCP, "Empty parameters are not allowed");

    std::string paramname = name;
    Spine::Parameter::Type type = Spine::Parameter::Type::Data;

    if (paramname == "level" || paramname == "latitude" ||
        paramname == "longitude" || paramname == "latlon" ||
        paramname == "lonlat" || paramname == "geoid" || paramname == "place" ||
        paramname == "stationname" || paramname == "name" ||
        paramname == "iso2" || paramname == "region" ||
        paramname == "country" || paramname == "elevation" ||
        paramname == "sunelevation" || paramname == "sundeclination" ||
        paramname == "sunazimuth" || paramname == "dark" ||
        paramname == "sunrise" || paramname == "sunset" ||
        paramname == "noon" || paramname == "sunrisetoday" ||
        paramname == "sunsettoday" || paramname == "moonphase" ||
        paramname == "model" || paramname == "time" ||
        paramname == "localtime" || paramname == "utctime" ||
        paramname == "epochtime" || paramname == "isotime" ||
        paramname == "xmltime" || paramname == "localtz" || paramname == "tz" ||
        paramname == "origintime" || paramname == "wday" ||
        paramname == "weekday" || paramname == "mon" || paramname == "month" ||
        paramname == "hour" || paramname == "timestring" ||
        paramname == "station_name" || paramname == "distance" ||
        paramname == "direction" || paramname == "stationary" ||
        paramname == "lon" || paramname == "lat" || paramname == "stationlon" ||
        paramname == "stationlat" || paramname == "stationlongitude" ||
        paramname == "stationlatitude" || paramname == "station_elevation" ||
        paramname == "wmo" || paramname == "lpnn" || paramname == "fmisid" ||
        paramname == "rwsid" || paramname == "sensor_no") {
      type = Spine::Parameter::Type::DataIndependent;
    } else if (paramname == "WindCompass8" || paramname == "WindCompass16" ||
               paramname == "WindCompass32" || paramname == "Cloudiness8th" ||
               paramname == "WindChill" || paramname == "Weather") {
      type = Spine::Parameter::Type::DataDerived;
    }

    return Spine::Parameter(paramname, type);
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool EngineParameters::isParameter(const std::string &alias,
                                   const std::string &stationType) const {
  try {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator
    namePtr = parameterMap.find(parameterAliasName);

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
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

bool EngineParameters::isParameterVariant(const std::string &name) const {
  try {
    std::string parameterLowerCase = Fmi::ascii_tolower_copy(name);
    Engine::Observation::removePrefix(parameterLowerCase, "qc_");
    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator
    namePtr = parameterMap.find(parameterLowerCase);

    if (namePtr == parameterMap.end())
      return false;

    return true;
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

uint64_t
EngineParameters::getParameterId(const std::string &alias,
                                 const std::string &stationType) const {
  try {
    std::string parameterAliasName = Fmi::ascii_tolower_copy(alias);
    Engine::Observation::removePrefix(parameterAliasName, "qc_");

    // Is the alias configured.
    std::map<std::string, std::map<std::string, std::string> >::const_iterator
    namePtr = parameterMap.find(parameterAliasName);

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
    try {
      return Fmi::stoul(stationTypeMapPtr->second);
    }
    catch (...) {
      return 0;
    }
  }
  catch (...) {
    throw Spine::Exception(BCP, "Operation failed!", NULL);
  }
}

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
