#include "DatabaseDriverInfo.h"
#include <boost/algorithm/string.hpp>
#include <boost/asio/ip/host_name.hpp>
#include <macgyver/StringConversion.h>
#include <spine/Exception.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
static DatabaseDriverInfoItem emptyInfoItem;

void DatabaseDriverInfo::readConfig(Spine::ConfigBase& cfg)
{
  try
  {
    libconfig::Config& lc = cfg.get_config();

    if (!lc.exists("database_driver_info.observation_database"))
      throw SmartMet::Spine::Exception::Trace(
          BCP, "database_driver_info.observation_database section missing");

    const libconfig::Setting& database_drivers =
        lc.lookup("database_driver_info.observation_database");
    int count = database_drivers.getLength();
    for (int i = 0; i < count; ++i)
    {
      bool active = database_drivers[i]["active"];
      std::string name = database_drivers[i]["name"];
      const libconfig::Setting& producers = database_drivers[i]["producers"];
      int num = producers.getLength();
      std::set<std::string> producer_set;
      for (int j = 0; j < num; ++j)
      {
        producer_set.insert(producers[j]);
      }  // for int j
      itsDatabaseDriverInfoItems.emplace_back(name, active, producer_set);
    }  // for int i

    if (lc.exists("database_driver_info.observation_cache"))
    {
      const libconfig::Setting& database_drivers =
          lc.lookup("database_driver_info.observation_cache");
      int count = database_drivers.getLength();
      for (int i = 0; i < count; ++i)
      {
        bool active = database_drivers[i]["active"];
        std::string name = database_drivers[i]["name"];
        const libconfig::Setting& producers = database_drivers[i]["producers"];
        int num = producers.getLength();
        std::set<std::string> producer_set;
        for (int j = 0; j < num; ++j)
        {
          producer_set.insert(producers[j]);
        }  // for int j
        itsCacheInfoItems.emplace_back(name, active, producer_set);
      }  // for int i
    }    // if

    for (auto& item : itsDatabaseDriverInfoItems)
    {
      if (boost::algorithm::starts_with(item.name, "spatialite_"))
      {
        readSpatiaLiteCommonInfo(cfg, item);
        readSpatiaLiteConnectInfo(cfg, item);
      }
      if (boost::algorithm::starts_with(item.name, "postgresql_"))
      {
        if (boost::algorithm::ends_with(item.name, "mobile_observations"))
          readPostgreSQLMobileCommonInfo(cfg, item);
        else
          readPostgreSQLCommonInfo(cfg, item);
        readPostgreSQLConnectInfo(cfg, item);
      }
      if (boost::algorithm::starts_with(item.name, "oracle_"))
      {
        readOracleCommonInfo(cfg, item);
        readOracleConnectInfo(cfg, item);
      }
    }

    for (auto& item : itsCacheInfoItems)
    {
      if (boost::algorithm::starts_with(item.name, "spatialite_"))
      {
        readSpatiaLiteCommonInfo(cfg, item);
        readSpatiaLiteConnectInfo(cfg, item);
      }
      if (boost::algorithm::starts_with(item.name, "postgresql_"))
      {
        if (boost::algorithm::ends_with(item.name, "mobile_observations"))
          readPostgreSQLMobileCommonInfo(cfg, item);
        else
          readPostgreSQLCommonInfo(cfg, item);
        readPostgreSQLConnectInfo(cfg, item);
      }
      if (boost::algorithm::starts_with(item.name, "oracle_"))
      {
        readOracleCommonInfo(cfg, item);
        readOracleConnectInfo(cfg, item);
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP, "Error in reading database configuration");
  }
}

void DatabaseDriverInfo::readSpatiaLiteConnectInfo(Spine::ConfigBase& cfg,
                                                   DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("connect_info." + infoItem.name);

  infoItem.params["spatialiteFile"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".spatialiteFile");
}

void DatabaseDriverInfo::readPostgreSQLConnectInfo(Spine::ConfigBase& cfg,
                                                   DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("connect_info." + infoItem.name);

  infoItem.params["host"] = cfg.get_mandatory_config_param<std::string>(common_key + ".host");
  infoItem.params["port"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".port"));
  infoItem.params["database"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".database");
  infoItem.params["username"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".username");
  infoItem.params["password"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".password");
  infoItem.params["encoding"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".encoding");
  infoItem.params["connect_timeout"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".connect_timeout"));
}

void DatabaseDriverInfo::readOracleConnectInfo(Spine::ConfigBase& cfg,
                                               DatabaseDriverInfoItem& infoItem)
{
  try
  {
    std::string common_key = ("connect_info." + infoItem.name);

    const std::string& name = boost::asio::ip::host_name();
    const std::string& defaultLang = "NLS_LANG=.UTF8";

    auto& cfgsetting = cfg.get_config().lookup(common_key);
    const std::string cfile =
        cfgsetting.isGroup() ? cfg.get_file_name() : cfg.get_mandatory_path(common_key);

    Spine::ConfigBase ccfg(cfile);

    const std::string scope = "override";

    infoItem.params_vector["service"].push_back(
        lookupDatabase(common_key, "service", name, scope, ccfg.get_config()));
    infoItem.params_vector["username"].push_back(
        lookupDatabase(common_key, "username", name, scope, ccfg.get_config()));
    infoItem.params_vector["password"].push_back(
        lookupDatabase(common_key, "password", name, scope, ccfg.get_config()));

    auto nlsLangSetting = ccfg.get_config().exists("database.nls_lang");
    auto nlsLang = nlsLangSetting
                       ? lookupDatabase(common_key, "nls_lang", name, scope, ccfg.get_config())
                       : defaultLang;
    infoItem.params_vector["nlsLang"].push_back(nlsLang);
    auto poolSizeSetting = ccfg.get_config().exists(common_key + ".poolSize");
    auto poolSize = poolSizeSetting
                        ? lookupDatabase(common_key, "poolSize", name, scope, ccfg.get_config())
                        : cfg.get_mandatory_config_param<int>("database_driver.poolSize");
    infoItem.params_vector["poolSize"].push_back(Fmi::to_string(poolSize));

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
        infoItem.params_vector["service"].push_back(
            lookupDatabase(common_key, "service", name, extra, ccfg.get_config()));
        infoItem.params_vector["username"].push_back(
            lookupDatabase(common_key, "username", name, extra, ccfg.get_config()));
        infoItem.params_vector["password"].push_back(
            lookupDatabase(common_key, "password", name, extra, ccfg.get_config()));
        auto nlsLangE = nlsLangSetting
                            ? lookupDatabase(common_key, "nls_lang", name, extra, ccfg.get_config())
                            : defaultLang;
        infoItem.params_vector["nlslang"].push_back(nlsLangE);
        auto poolSizeE =
            poolSizeSetting ? lookupDatabase(common_key, "poolSize", name, extra, ccfg.get_config())
                            : cfg.get_mandatory_config_param<int>("database_driver.poolSize");
        infoItem.params_vector["poolsize"].push_back(Fmi::to_string(poolSizeE));
      }
    }
  }
  catch (...)
  {
    throw SmartMet::Spine::Exception::Trace(BCP,
                                            "Reading Oracle database driver configuration failed!");
  }
}

void DatabaseDriverInfo::readOracleCommonInfo(Spine::ConfigBase& cfg,
                                              DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("common_info." + infoItem.name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  infoItem.params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  infoItem.params["serializedStationsFile"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".serializedStationsFile");
  infoItem.params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));
  infoItem.params["connectionTimeout"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
  infoItem.params["timer"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".timer", false));
  infoItem.params["disableAllCacheUpdates"] = Fmi::to_string(
      cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
  infoItem.params["finCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".finCacheUpdateInterval", 0));
  infoItem.params["extCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".extCacheUpdateInterval", 0));
  infoItem.params["flashCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".flashCacheUpdateInterval", 0));
  infoItem.params["updateExtraInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".updateExtraInterval", 10));
  infoItem.params["finCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finCacheDuration", 0));
  infoItem.params["finMemoryCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finMemoryCacheDuration", 0));
  infoItem.params["extCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".extCacheDuration", 0));
  infoItem.params["flashCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".flashCacheDuration", 0));
  infoItem.params["flashMemoryCacheDuration"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".flashMemoryCacheDuration", 0));
}

void DatabaseDriverInfo::readPostgreSQLCommonInfo(Spine::ConfigBase& cfg,
                                                  DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("common_info." + infoItem.name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  infoItem.params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  infoItem.params["serializedStationsFile"] =
      cfg.get_optional_config_param<std::string>(common_key + ".serializedStationsFile", "");
  infoItem.params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));
  infoItem.params["connectionTimeout"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
  infoItem.params["timer"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".timer", false));
  infoItem.params["disableAllCacheUpdates"] = Fmi::to_string(
      cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
  infoItem.params["finCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".finCacheUpdateInterval", 0));
  infoItem.params["extCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".extCacheUpdateInterval", 0));
  infoItem.params["flashCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".flashCacheUpdateInterval", 0));
  infoItem.params["updateExtraInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".updateExtraInterval", 10));
  infoItem.params["finCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finCacheDuration", 0));
  infoItem.params["finMemoryCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".finMemoryCacheDuration", 0));
  infoItem.params["extCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".extCacheDuration", 0));
  infoItem.params["flashCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".flashCacheDuration", 0));
  infoItem.params["flashMemoryCacheDuration"] = Fmi::to_string(
      cfg.get_optional_config_param<int>(common_key + ".flashMemoryCacheDuration", 0));
}

void DatabaseDriverInfo::readPostgreSQLMobileCommonInfo(Spine::ConfigBase& cfg,
                                                        DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("common_info." + infoItem.name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  infoItem.params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  infoItem.params["poolSize"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".poolSize", 10));
  infoItem.params["connectionTimeout"] =
      Fmi::to_string(cfg.get_optional_config_param<size_t>(common_key + ".connectionTimeout", 30));
  infoItem.params["disableAllCacheUpdates"] = Fmi::to_string(
      cfg.get_optional_config_param<bool>(common_key + ".disableAllCacheUpdates", false));
  infoItem.params["roadCloudCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".roadCloudCacheUpdateInterval", 0));
  infoItem.params["netAtmoCacheUpdateInterval"] = Fmi::to_string(
      cfg.get_optional_config_param<std::size_t>(common_key + ".netAtmoCacheUpdateInterval", 0));
  infoItem.params["roadCloudCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".roadCloudCacheDuration", 0));
  infoItem.params["netAtmoCacheDuration"] =
      Fmi::to_string(cfg.get_optional_config_param<int>(common_key + ".netAtmoCacheDuration", 0));
}

void DatabaseDriverInfo::readSpatiaLiteCommonInfo(Spine::ConfigBase& cfg,
                                                  DatabaseDriverInfoItem& infoItem)
{
  std::string common_key = ("common_info." + infoItem.name);

  bool defaultQuiet = cfg.get_optional_config_param<bool>("quiet", false);

  infoItem.params["quiet"] =
      Fmi::to_string(cfg.get_optional_config_param<bool>(common_key + ".quiet", defaultQuiet));
  infoItem.params["threading_mode"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".threading_mode");
  infoItem.params["synchronous"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".synchronous");
  infoItem.params["journal_mode"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".journal_mode");
  infoItem.params["auto_vacuum"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".auto_vacuum");
  infoItem.params["temp_store"] =
      cfg.get_mandatory_config_param<std::string>(common_key + ".temp_store");
  infoItem.params["timeout"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".timeout"));
  infoItem.params["threads"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".threads"));
  infoItem.params["cache_size"] =
      Fmi::to_string(cfg.get_mandatory_config_param<long>(common_key + ".cache_size"));
  infoItem.params["shared_cache"] =
      Fmi::to_string(cfg.get_mandatory_config_param<bool>(common_key + ".shared_cache"));
  infoItem.params["read_uncommitted"] =
      Fmi::to_string(cfg.get_mandatory_config_param<bool>(common_key + ".read_uncommitted"));
  infoItem.params["memstatus"] =
      Fmi::to_string(cfg.get_mandatory_config_param<bool>(common_key + ".memstatus"));
  infoItem.params["wal_autocheckpoint"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".wal_autocheckpoint"));
  infoItem.params["mmap_size"] =
      Fmi::to_string(cfg.get_mandatory_config_param<long>(common_key + ".mmap_size"));
  infoItem.params["poolSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".poolSize"));
  infoItem.params["maxInsertSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".maxInsertSize"));
  infoItem.params["locationCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".locationCacheSize"));
  infoItem.params["dataInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".dataInsertCacheSize"));
  infoItem.params["weatherDataQCInsertCacheSize"] = Fmi::to_string(
      cfg.get_mandatory_config_param<int>(common_key + ".weatherDataQCInsertCacheSize"));
  infoItem.params["flashInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".flashInsertCacheSize"));
  infoItem.params["roadCloudInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".roadCloudInsertCacheSize"));
  infoItem.params["netAtmoInsertCacheSize"] =
      Fmi::to_string(cfg.get_mandatory_config_param<int>(common_key + ".netAtmoInsertCacheSize"));
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
    throw SmartMet::Spine::Exception::Trace(BCP, "Override configuration error: " + setting);
  }
}

const DatabaseDriverInfoItem& DatabaseDriverInfo::getDatabaseDriverInfo(
    const std::string& name) const
{
  for (const auto& item : itsDatabaseDriverInfoItems)
    if (item.name == name)
      return item;

  return emptyInfoItem;
}

const DatabaseDriverInfoItem& DatabaseDriverInfo::getCacheInfo(const std::string& name) const
{
  for (const auto& item : itsCacheInfoItems)
    if (item.name == name)
      return item;

  return emptyInfoItem;
}

const std::vector<DatabaseDriverInfoItem>& DatabaseDriverInfo::getDatabaseDriverInfo() const
{
  return itsDatabaseDriverInfoItems;
}
const std::vector<DatabaseDriverInfoItem>& DatabaseDriverInfo::getCacheInfo() const
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
  /*
  out << " ** Common settings **" << std::endl;
  const SmartMet::Engine::Observation::CommonDatabaseDriverSettings& commonSettings =
      driverInfo.getCommonSettings();

  out << "  serializedStationsFile: " << commonSettings.serializedStationsFile << std::endl;
  out << "  connectionTimeoutSeconds: " << commonSettings.connectionTimeoutSeconds << std::endl;
  out << "  timer: " << commonSettings.timer << std::endl;
  out << "  disableAllCacheUpdates: " << commonSettings.disableAllCacheUpdates << std::endl;
  out << "  finCacheUpdateInterval: " << commonSettings.finCacheUpdateInterval << std::endl;
  out << "  extCacheUpdateInterval: " << commonSettings.extCacheUpdateInterval << std::endl;
  out << "  flashCacheUpdateInterval: " << commonSettings.flashCacheUpdateInterval << std::endl;
  out << "  updateExtraInterval: " << commonSettings.updateExtraInterval << std::endl;
  out << "  finCacheDuration: " << commonSettings.finCacheDuration << std::endl;
  out << "  finMemoryCacheDuration: " << commonSettings.finMemoryCacheDuration << std::endl;
  out << "  extCacheDuration: " << commonSettings.extCacheDuration << std::endl;
  out << "  flashCacheDuration: " << commonSettings.flashCacheDuration << std::endl;
  out << "  flashMemoryCacheDuration: " << commonSettings.flashMemoryCacheDuration << std::endl;
  */
  out << " ** Driver settings **" << std::endl;
  const std::vector<SmartMet::Engine::Observation::DatabaseDriverInfoItem>& driverInfoItems =
      driverInfo.getDatabaseDriverInfo();

  for (const auto& item : driverInfoItems)
  {
    out << "  \033[4mname: " << item.name << "\033[24m" << std::endl;
    out << "  active: " << item.active << std::endl;
    out << "  producers: " << std::endl;
    for (const auto& producer : item.producers)
      out << "   " << producer << std::endl;

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
  const std::vector<SmartMet::Engine::Observation::DatabaseDriverInfoItem>& cacheInfoItems =
      driverInfo.getCacheInfo();

  for (const auto& item : cacheInfoItems)
  {
    out << "  \033[4mname: " << item.name << "\033[24m" << std::endl;
    out << "  active: " << item.active << std::endl;
    out << "  producers: " << std::endl;
    for (const auto& producer : item.producers)
      out << "   " << producer << std::endl;

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

  return out;
}
