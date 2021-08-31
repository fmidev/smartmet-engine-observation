#include "DatabaseDriverInfo.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <macgyver/AnsiEscapeCodes.h>
#include <macgyver/Exception.h>
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace
{
const DatabaseDriverInfoItem emptyDriverInfoItem;
const CacheInfoItem emptyCacheInfoItem;
}  // namespace

void CacheInfoItem::mergeCacheInfo(const CacheInfoItem& cii_from)
{
  // Tables
  tables.insert(cii_from.tables.begin(), cii_from.tables.end());
  // Params
  for (const auto& param_item : cii_from.params)
  {
    if (params.find(param_item.first) == params.end())
      params[param_item.first] = param_item.second;
  }
  // Param vectors
  for (const auto& param_vector_item : cii_from.params_vector)
  {
    if (params_vector.find(param_vector_item.first) == params_vector.end())
      params_vector[param_vector_item.first] = param_vector_item.second;
  }
}

void DatabaseDriverInfo::readConfig(Spine::ConfigBase& cfg)
{
  try
  {
    libconfig::Config& lc = cfg.get_config();

    if (!lc.exists("database_driver_info"))
      throw Fmi::Exception::Trace(BCP, "database_driver_info section missing");

    const libconfig::Setting& driver_settings = lc.lookup("database_driver_info");
    int count = driver_settings.getLength();
    for (int i = 0; i < count; ++i)
    {
      bool active = driver_settings[i]["active"];
      if (!active)
        continue;
      std::string name = driver_settings[i]["name"];
      // Tables
      std::set<std::string> table_set;
      std::map<std::string, int> table_days;
      const libconfig::Setting& tables = driver_settings[i]["tables"];
      int num = tables.getLength();
      for (int j = 0; j < num; ++j)
      {
        std::string tablename = tables[j];
        if (tablename.find(":") != std::string::npos)
        {
          std::vector<std::string> parts;
          boost::algorithm::split(parts, tablename, boost::algorithm::is_any_of(":"));
          tablename = parts.at(0);
          std::string table_day_string = parts.at(1);
          table_days[tablename] = Fmi::stoi(table_day_string);
        }
        else
          table_days[tablename] = INT_MAX;

        if (!tablename.empty())
          table_set.insert(tablename);
      }  // for int j
      // Caches
      std::set<std::string> cache_set;
      const libconfig::Setting& caches = driver_settings[i]["caches"];
      num = caches.getLength();
      for (int j = 0; j < num; ++j)
      {
        std::string cachestring = caches[j];
        if (!cachestring.empty())
          cache_set.insert(cachestring);
      }  // for int j
      itsDatabaseDriverInfoItems.emplace_back(name, active, table_set, table_days, cache_set);
    }  // for int i

    for (auto& item : itsDatabaseDriverInfoItems)
    {
      if (!item.active)
        continue;

      std::string name = item.name;

      if (!boost::algorithm::ends_with(name, "_observations"))
        continue;

      if (boost::algorithm::starts_with(name, "spatialite_"))
      {
        readSpatiaLiteCommonInfo(cfg, name, item.params);
        if (boost::algorithm::ends_with(name, "_cache"))
          readSpatiaLiteConnectInfo(cfg, name, item.params);
      }
      if (boost::algorithm::starts_with(name, "postgresql_"))
      {
        if (boost::algorithm::ends_with(name, "mobile_observations"))
          readPostgreSQLMobileCommonInfo(cfg, name, item.params);
        else
          readPostgreSQLCommonInfo(cfg, name, item.params);
        readPostgreSQLConnectInfo(cfg, name, item.params);
      }
      if (boost::algorithm::starts_with(name, "oracle_"))
      {
        readOracleCommonInfo(cfg, name, item.params);
        readOracleConnectInfo(cfg, name, item.params_vector);
      }

      std::map<std::string, CacheInfoItem>& cii_map = item.itsCacheInfoItems;
      for (auto& cii : cii_map)
      {
        name = cii.first;

        if (!boost::algorithm::ends_with(name, "_cache"))
          continue;

        if (boost::algorithm::starts_with(name, "spatialite_"))
        {
          readSpatiaLiteCommonInfo(cfg, name, cii.second.params);
          readSpatiaLiteConnectInfo(cfg, name, cii.second.params);
        }
        if (boost::algorithm::starts_with(name, "postgresql_"))
        {
          if (boost::algorithm::ends_with(name, "mobile_observations"))
            readPostgreSQLMobileCommonInfo(cfg, name, cii.second.params);
          else
            readPostgreSQLCommonInfo(cfg, name, cii.second.params);
          readPostgreSQLConnectInfo(cfg, name, cii.second.params);
        }
      }
    }

    unsigned int loadStationsParam = 0;
    // Cache info (with same name) from different drivers are aggregated to one place
    for (auto& ddii : itsDatabaseDriverInfoItems)
    {
      if (ddii.parameterExists("loadStations") && ddii.getIntParameterValue("loadStations", 0) > 0)
      {
        loadStationsParam++;
      }
      for (const auto& cii_from : ddii.itsCacheInfoItems)
        if (itsCacheInfoItems.find(cii_from.first) == itsCacheInfoItems.end())
          itsCacheInfoItems[cii_from.first] = cii_from.second;
        else
          itsCacheInfoItems[cii_from.first].mergeCacheInfo(cii_from.second);
    }

    if (loadStationsParam > 1)
      throw Fmi::Exception::Trace(
          BCP, "Parameter loadStations defined to be true in more than one database driver!");
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Error in reading database configuration");
  }
}

void DatabaseDriverInfo::readSpatiaLiteConnectInfo(Spine::ConfigBase& cfg,
                                                   const std::string& name,
                                                   std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.connect_info." + name);

  params["spatialiteFile"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".spatialiteFile");
}

void DatabaseDriverInfo::readPostgreSQLConnectInfo(Spine::ConfigBase& cfg,
                                                   const std::string& name,
                                                   std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.connect_info." + name);

  params["host"] = cfg.get_mandatory_config_param<std::string>(common_key + ".host");
  params["port"] = Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".port"));
  params["database"] = cfg.get_mandatory_config_param<std::string>(common_key + ".database");
  params["username"] = cfg.get_mandatory_config_param<std::string>(common_key + ".username");
  params["password"] = cfg.get_mandatory_config_param<std::string>(common_key + ".password");
  params["encoding"] = cfg.get_mandatory_config_param<std::string>(common_key + ".encoding");
  params["connect_timeout"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".connect_timeout"));
}

void DatabaseDriverInfo::readOracleConnectInfo(
    Spine::ConfigBase& cfg,
    const std::string& name,
    std::map<std::string, std::vector<std::string>>& params_vector)
{
  try
  {
    std::string common_key = ("database_info.connect_info." + name);

    const std::string& name = boost::asio::ip::host_name();
    const std::string& defaultLang = "NLS_LANG=.UTF8";

    auto& cfgsetting = cfg.get_config().lookup(common_key);
    const std::string cfile =
        cfgsetting.isGroup() ? cfg.get_file_name() : cfg.get_mandatory_path(common_key);

    Spine::ConfigBase ccfg(cfile);

    const std::string scope = "override";

    params_vector["service"].push_back(
        lookupDatabase(common_key, "service", name, scope, ccfg.get_config()));
    params_vector["username"].push_back(
        lookupDatabase(common_key, "username", name, scope, ccfg.get_config()));
    params_vector["password"].push_back(
        lookupDatabase(common_key, "password", name, scope, ccfg.get_config()));

    auto nlsLangSetting = ccfg.get_config().exists("database.nls_lang");
    auto nlsLang = nlsLangSetting
                       ? lookupDatabase(common_key, "nls_lang", name, scope, ccfg.get_config())
                       : defaultLang;
    params_vector["nlsLang"].push_back(nlsLang);
    auto poolSizeSetting = ccfg.get_config().exists(common_key + ".poolSize");
    auto poolSize = poolSizeSetting
                        ? lookupDatabase(common_key, "poolSize", name, scope, ccfg.get_config())
                        : cfg.get_mandatory_config_param<int>("database_driver.poolSize");
    params_vector["poolSize"].emplace_back(Fmi::to_string(poolSize));

    const std::string extra = "extra";
    if (ccfg.get_config().exists(common_key + "." + extra))
    {
      const libconfig::Setting& extras = ccfg.get_config().lookup(common_key + "." + extra);
      bool nameFound = false;
      int count = extras.getLength();
      for (int i = 0; i < count; ++i)
      {
        const libconfig::Setting& names = extras[i]["name"];
        int num = names.getLength();
        for (int j = 0; j < num; ++j)
        {
          std::string host = names[j];
          if (boost::algorithm::starts_with(name, host))
            nameFound = true;
        }
      }
      if (nameFound)
      {
        params_vector["service"].push_back(
            lookupDatabase(common_key, "service", name, extra, ccfg.get_config()));
        params_vector["username"].push_back(
            lookupDatabase(common_key, "username", name, extra, ccfg.get_config()));
        params_vector["password"].push_back(
            lookupDatabase(common_key, "password", name, extra, ccfg.get_config()));
        auto nlsLangE = nlsLangSetting
                            ? lookupDatabase(common_key, "nls_lang", name, extra, ccfg.get_config())
                            : defaultLang;
        params_vector["nlslang"].push_back(nlsLangE);
        auto poolSizeE =
            poolSizeSetting ? lookupDatabase(common_key, "poolSize", name, extra, ccfg.get_config())
                            : cfg.get_mandatory_config_param<int>("database_driver.poolSize");
        params_vector["poolsize"].emplace_back(Fmi::to_string(poolSizeE));
      }
    }
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Reading Oracle database driver configuration failed!");
  }
}

void DatabaseDriverInfo::readOracleCommonInfo(Spine::ConfigBase& cfg,
                                              const std::string& name,
                                              std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.common_info." + name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  params["loadStations"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".loadStations", false));
  params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));
  params["connectionTimeout"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
  params["timer"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".timer", false));
  params["disableAllCacheUpdates"] = Fmi::to_string(
      cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
  params["finCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".finCacheUpdateInterval", 0));
  params["extCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".extCacheUpdateInterval", 0));
  params["flashCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".flashCacheUpdateInterval", 0));
  params["updateExtraInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".updateExtraInterval", 10));
  params["finCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finCacheDuration", 0));
  params["finMemoryCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finMemoryCacheDuration", 0));
  params["extCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".extCacheDuration", 0));
  params["flashCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".flashCacheDuration", 0));
  params["flashMemoryCacheDuration"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".flashMemoryCacheDuration", 0));
  params["stationsCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".stationsCacheUpdateInterval", 0));

  params["flash_emulator_active"] = "false";
  if (cfg.get_optional_config_param<bool>(common_key + ".flash_emulator.active", false))
  {
    params["flash_emulator_active"] = "true";
    params["flash_emulator_bbox"] = cfg.get_optional_config_param<std::string>(
        common_key + ".flash_emulator.bbox", "20,60,30,70");
    params["flash_emulator_strokes"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".flash_emulator.strokes_per_minute", 0));
  }
  else
    std::cout << "Flash emulator not active in driver " << name << std::endl;
}

void DatabaseDriverInfo::readPostgreSQLCommonInfo(Spine::ConfigBase& cfg,
                                                  const std::string& name,
                                                  std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.common_info." + name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));

  if (boost::algorithm::ends_with(name, "_cache"))
  {
    params["maxInsertSize"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".maxInsertSize", 0));
    params["locationCacheSize"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".locationCacheSize", 0));
    params["dataInsertCacheSize"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".dataInsertCacheSize", 0));
    params["weatherDataQCInsertCacheSize"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".weatherDataQCInsertCacheSize", 0));
    params["flashInsertCacheSize"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".flashInsertCacheSize", 0));
    params["roadCloudInsertCacheSize"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".roadCloudInsertCacheSize", 0));
    params["netAtmoInsertCacheSize"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".netAtmoInsertCacheSize", 0));
    params["bkHydrometaInsertCacheSize"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".bkHydrometaInsertCacheSize", 0));
    params["fmiIoTInsertCacheSize"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".fmiIoTInsertCacheSize", 0));
  }
  else
  {
    params["loadStations"] =
        Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".loadStations", false));
    params["connectionTimeout"] = Fmi::to_string(
        cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
    params["timer"] =
        Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".timer", false));
    params["disableAllCacheUpdates"] = Fmi::to_string(
        cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
    params["finCacheUpdateInterval"] = Fmi::to_string(
        cfg.get_optional_config_param<std::size_t>(common_key + ".finCacheUpdateInterval", 0));
    params["extCacheUpdateInterval"] = Fmi::to_string(
        cfg.get_optional_config_param<std::size_t>(common_key + ".extCacheUpdateInterval", 0));
    params["flashCacheUpdateInterval"] = Fmi::to_string(
        cfg.get_optional_config_param<std::size_t>(common_key + ".flashCacheUpdateInterval", 0));
    params["updateExtraInterval"] = Fmi::to_string(
        cfg.get_optional_config_param<std::size_t>(common_key + ".updateExtraInterval", 10));
    params["finCacheDuration"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finCacheDuration", 0));
    params["finMemoryCacheDuration"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".finMemoryCacheDuration", 0));
    params["extCacheDuration"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".extCacheDuration", 0));
    params["flashCacheDuration"] =
        Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".flashCacheDuration", 0));
    params["flashMemoryCacheDuration"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".flashMemoryCacheDuration", 0));
    params["stationsCacheUpdateInterval"] = Fmi::to_string(
        cfg.get_optional_config_param<std::size_t>(common_key + ".stationsCacheUpdateInterval", 0));
  }

  params["flash_emulator_active"] = "false";
  if (cfg.get_optional_config_param<bool>(common_key + ".flash_emulator.active", false))
  {
    params["flash_emulator_active"] = "true";
    params["flash_emulator_bbox"] = cfg.get_optional_config_param<std::string>(
        common_key + ".flash_emulator.bbox", "20,60,30,70");
    params["flash_emulator_strokes"] = Fmi::to_string(
        cfg.get_optional_config_param<int>(common_key + ".flash_emulator.strokes_per_minute", 0));
  }
  else
    std::cout << "Flash emulator not active in driver " << name << std::endl;
}

void DatabaseDriverInfo::readPostgreSQLMobileCommonInfo(Spine::ConfigBase& cfg,
                                                        const std::string& name,
                                                        std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.common_info." + name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));
  params["connectionTimeout"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
  params["disableAllCacheUpdates"] = Fmi::to_string(
      cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
  params["roadCloudCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".roadCloudCacheUpdateInterval", 0));
  params["netAtmoCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".netAtmoCacheUpdateInterval", 0));
  params["bkHydrometaCacheUpdateInterval"] =
      Fmi::to_string(cfg.get_optional_config_param<std::size_t>(
          common_key + ".bkHydrometaCacheUpdateInterval", 0));
  params["fmiIoTCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".fmiIoTCacheUpdateInterval", 0));
  params["roadCloudCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".roadCloudCacheDuration", 0));
  params["netAtmoCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".netAtmoCacheDuration", 0));
  params["bkHydrometaCacheDuration"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".bkHydrometaCacheDuration", 0));
  params["fmiIoTCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".fmiIoTCacheDuration", 0));
}

void DatabaseDriverInfo::readFakeCacheInfo(Spine::ConfigBase& cfg,
                                           const std::string& name,
                                           std::map<std::string, std::string>& params)
{
  libconfig::Config& lc = cfg.get_config();
  std::vector<std::string> table_names{"observation_data", "weather_data_qc", "flash_data"};

  for (const auto& tablename : table_names)
  {
    std::string id = name + "." + tablename;
    if (lc.exists(id))
    {
      std::string settings_str;
      const libconfig::Setting& settings = lc.lookup(id);
      int count = settings.getLength();

      for (int i = 0; i < count; ++i)
      {
        std::string starttime = settings[i]["starttime"];
        std::string endtime = settings[i]["endtime"];
        std::string measurand_id;
        std::string fmisid;
        if (settings[i].exists("measurand_id"))
          measurand_id = (settings[i]["measurand_id"]).c_str();
        if (settings[i].exists("fmisid"))
          fmisid = (settings[i]["fmisid"]).c_str();
        settings_str += (starttime + ";" + endtime + ";" + measurand_id + ";" + fmisid + "#");
      }
      params[tablename] = settings_str;
    }
  }
}

void DatabaseDriverInfo::readSpatiaLiteCommonInfo(Spine::ConfigBase& cfg,
                                                  const std::string& name,
                                                  std::map<std::string, std::string>& params)
{
  std::string common_key = ("database_info.common_info." + name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));

  if (boost::algorithm::ends_with(name, "_observations"))
  {
    params["loadStations"] =
        Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".loadStations", false));
    params["connectionTimeout"] = Fmi::to_string(
        cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
    params["timer"] =
        Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".timer", false));
    params["disableAllCacheUpdates"] = Fmi::to_string(
        cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
    return;
  }

  if (cfg.get_config().exists(common_key + ".fake_cache"))
    readFakeCacheInfo(cfg, common_key + ".fake_cache", params);

  params["synchronous"] = cfg.get_mandatory_config_param<std::string>(common_key + ".synchronous");
  params["journal_mode"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".journal_mode");
  params["auto_vacuum"] = cfg.get_mandatory_config_param<std::string>(common_key + ".auto_vacuum");
  params["temp_store"] = cfg.get_mandatory_config_param<std::string>(common_key + ".temp_store");
  params["timeout"] = Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".timeout"));
  params["threads"] = Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".threads"));
  params["cache_size"] =
      Fmi::to_string(cfg.get_mandatory_config_param<long>(common_key + ".cache_size"));
  params["shared_cache"] =
      Fmi::to_string(cfg.get_mandatory_config_param<bool>(common_key + ".shared_cache"));
  params["read_uncommitted"] =
      Fmi::to_string(cfg.get_mandatory_config_param<bool>(common_key + ".read_uncommitted"));
  params["wal_autocheckpoint"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".wal_autocheckpoint"));
  params["mmap_size"] =
      Fmi::to_string(cfg.get_mandatory_config_param<long>(common_key + ".mmap_size"));
  params["poolSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".poolSize"));
  params["maxInsertSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".maxInsertSize"));
  params["locationCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".locationCacheSize"));
  params["dataInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".dataInsertCacheSize"));
  params["weatherDataQCInsertCacheSize"] = Fmi::to_string(
      cfg.get_mandatory_config_param<int>(common_key + ".weatherDataQCInsertCacheSize"));
  params["flashInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".flashInsertCacheSize"));
  params["roadCloudInsertCacheSize"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".roadCloudInsertCacheSize", 0));
  params["netAtmoInsertCacheSize"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".netAtmoInsertCacheSize", 0));
  params["bkHydrometaInsertCacheSize"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".bkHydrometaInsertCacheSize", 0));
  params["fmiIoTInsertCacheSize"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".fmiIoTInsertCacheSize", 0));
}

/*!
 * \brief Lookup configuration value for the database considering overrides
 */
// ----------------------------------------------------------------------

const libconfig::Setting& DatabaseDriverInfo::lookupDatabase(const std::string& common_key,
                                                             const std::string& setting,
                                                             const std::string& name,
                                                             const std::string& scope,
                                                             const libconfig::Config& conf) const
{
  try
  {
    const auto& default_value = conf.lookup(common_key + "." + setting);
    if (conf.exists("database." + scope))
    {
      const libconfig::Setting& override = conf.lookup(common_key + "." + scope);
      int count = override.getLength();
      for (int i = 0; i < count; ++i)
      {
        const libconfig::Setting& names = override[i]["name"];
        int num = names.getLength();
        for (int j = 0; j < num; ++j)
        {
          std::string host = names[j];

          if (boost::algorithm::starts_with(name, host) && override[i].exists(setting))
            return override[i][setting.c_str()];
        }  // for int j
      }    // for int i
    }      // if
    return default_value;
  }
  catch (libconfig::SettingNotFoundException& ex)
  {
    throw Fmi::Exception::Trace(BCP, "Override configuration error: " + setting);
  }
}

const DatabaseDriverInfoItem& DatabaseDriverInfo::getDatabaseDriverInfo(
    const std::string& name) const
{
  for (const auto& item : itsDatabaseDriverInfoItems)
    if (item.name == name)
      return item;

  return emptyDriverInfoItem;
}

const CacheInfoItem& DatabaseDriverInfo::getAggregateCacheInfo(const std::string& cachename) const
{
  if (itsCacheInfoItems.find(cachename) != itsCacheInfoItems.end())
    return itsCacheInfoItems.at(cachename);

  return emptyCacheInfoItem;
}

const std::map<std::string, CacheInfoItem>& DatabaseDriverInfo::getAggregateCacheInfo() const
{
  return itsCacheInfoItems;  // Cache name -> cahecinfo
}

const std::vector<DatabaseDriverInfoItem>& DatabaseDriverInfo::getDatabaseDriverInfo() const
{
  return itsDatabaseDriverInfoItems;
}

bool DatabaseDriverInfoItem::parameterExists(const std::string& name) const
{
  return params.find(name) != params.end();
}
bool DatabaseDriverInfoItem::parameterVectorExists(const std::string& name) const
{
  return params_vector.find(name) != params_vector.end();
}

int DatabaseDriverInfoItem::getIntParameterValue(const std::string& name, int defaultValue) const
{
  if (!parameterExists(name))
    return defaultValue;

  return Fmi::stoi(params.at(name));
}

const std::string& DatabaseDriverInfoItem::getStringParameterValue(
    const std::string& name, const std::string& defaultValue) const
{
  if (!parameterExists(name))
    return defaultValue;

  return params.at(name);
}

// Cache info stirng format is 'cachename:tablename1,tablename2,...'
void DatabaseDriverInfoItem::parseCacheInfo(const std::set<std::string>& cacheinfostring)
{
  for (const auto& c : cacheinfostring)
  {
    std::vector<std::string> parts;
    boost::algorithm::split(parts, c, boost::algorithm::is_any_of(":"));
    std::string cachename = parts[0];
    std::string tablenames = parts[1];
    std::set<std::string> table_set;
    boost::algorithm::split(table_set, tablenames, boost::algorithm::is_any_of(","));
    if (table_set.find("*") != table_set.end())
    {
      table_set.clear();
      table_set.insert(OBSERVATION_DATA_TABLE);
      table_set.insert(WEATHER_DATA_QC_TABLE);
      table_set.insert(FLASH_DATA_TABLE);
      table_set.insert(NETATMO_DATA_TABLE);
      table_set.insert(ROADCLOUD_DATA_TABLE);
      table_set.insert(FMI_IOT_DATA_TABLE);
    }
    if (itsCacheInfoItems.find(cachename) == itsCacheInfoItems.end())
    {
      // Add new table
      itsCacheInfoItems[cachename] = CacheInfoItem(cachename, true, table_set);
      caches.insert(cachename);
    }
    else
    {
      // Merge new info to existsting (same cache used by multiple drivers)
      itsCacheInfoItems[cachename].mergeCacheInfo(CacheInfoItem(cachename, true, table_set));
    }
  }
}

const CacheInfoItem& DatabaseDriverInfoItem::getCacheInfo(const std::string& name) const
{
  if (itsCacheInfoItems.find(name) != itsCacheInfoItems.end())
    return itsCacheInfoItems.at(name);

  return emptyCacheInfoItem;
}

const std::map<std::string, CacheInfoItem>& DatabaseDriverInfoItem::getCacheInfo() const
{
  return itsCacheInfoItems;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::DatabaseDriverInfo& driverInfo)
{
  out << "** DatabaseDriverInfo **" << std::endl;
  out << " ** Driver settings **" << std::endl;
  const std::vector<SmartMet::Engine::Observation::DatabaseDriverInfoItem>& driverInfoItems =
      driverInfo.getDatabaseDriverInfo();

  for (const auto& item : driverInfoItems)
  {
    out << ANSI_FG_GREEN << "  name: " << item.name << ANSI_FG_DEFAULT << std::endl;
    out << "  active: " << item.active << std::endl;

    out << "  tables: " << std::endl;
    for (const auto& t : item.tables)
    {
      if (item.table_days.at(t) == INT_MAX)
        out << "   " << t << " -> all data available" << std::endl;
      else
        out << "   " << t << " -> max " << item.table_days.at(t) << " days" << std::endl;
    }

    out << "  caches: " << std::endl;
    for (const auto& cache : item.caches)
      out << "   " << cache << std::endl;

    out << "  parameters: " << std::endl;
    if (!item.params.empty())
    {
      for (const auto& param : item.params)
        out << "  " << param.first << " -> " << param.second << std::endl;
    }

    if (!item.params_vector.empty())
    {
      for (const auto& param : item.params_vector)
      {
        out << "  " << param.first << " -> " << std::endl;
        for (const auto& param2 : param.second)
        {
          out << "   " << param2 << std::endl;
        }
      }
    }
  }

  out << " ** Cache settings **" << std::endl;

  const std::vector<SmartMet::Engine::Observation::DatabaseDriverInfoItem>& ddi_vector =
      driverInfo.getDatabaseDriverInfo();

  for (const auto& ddi : ddi_vector)
  {
    const std::map<std::string, SmartMet::Engine::Observation::CacheInfoItem>& cii_map =
        ddi.getCacheInfo();

    for (const auto& ci : cii_map)
    {
      out << ANSI_FG_GREEN << "  name: " << ci.second.name << ANSI_FG_DEFAULT << std::endl;
      out << "  active: " << ci.second.active << std::endl;
      out << "  tables: " << std::endl;
      for (const auto& table : ci.second.tables)
        out << "   " << table << std::endl;

      if (!ci.second.params.empty())
      {
        for (const auto& param : ci.second.params)
          out << "  " << param.first << " -> " << param.second << std::endl;
      }

      if (!ci.second.params_vector.empty())
      {
        for (const auto& param : ci.second.params_vector)
        {
          out << "  " << param.first << " -> " << std::endl;
          for (const auto& param2 : param.second)
          {
            out << "   " << param2 << std::endl;
          }
        }
      }
    }
  }

  out << " *\n* Aggregate cache settings **" << std::endl;

  const std::map<std::string, SmartMet::Engine::Observation::CacheInfoItem>& aggregateCacheInfo =
      driverInfo.getAggregateCacheInfo();

  for (const auto& item : aggregateCacheInfo)
  {
    out << ANSI_FG_GREEN << "  name: " << item.first << ANSI_FG_DEFAULT << std::endl;
    out << "  active: " << item.second.active << std::endl;
    out << "  tables: " << std::endl;
    for (const auto& table : item.second.tables)
      out << "   " << table << std::endl;

    if (!item.second.params.empty())
    {
      for (const auto& param : item.second.params)
        out << "  " << param.first << " -> " << param.second << std::endl;
    }

    if (!item.second.params_vector.empty())
    {
      for (const auto& param : item.second.params_vector)
      {
        out << "  " << param.first << " -> " << std::endl;
        for (const auto& param2 : param.second)
        {
          out << "   " << param2 << std::endl;
        }
      }
    }
  }

  return out;
}
