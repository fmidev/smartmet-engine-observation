#pragma once

#include "CacheInfoItem.h"
#include <spine/ConfigBase.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
#define ORACLE_DB "oracle"
#define POSTGRESQL_DB "postgresql"
#define OBSERVATION_DATA_TABLE "observation_data"
#define WEATHER_DATA_QC_TABLE "weather_data_qc"
#define FLASH_DATA_TABLE "flash_data"
#define NETATMO_DATA_TABLE "ext_obsdata_netatmo"
#define ROADCLOUD_DATA_TABLE "ext_obsdata_roadcloud"
#define FMI_IOT_DATA_TABLE "ext_obsdata_fmi_iot"
#define EXT_OBSDATA_TABLE "ext_obsdata"

struct DatabaseDriverInfoItem
{
  DatabaseDriverInfoItem() {}
  DatabaseDriverInfoItem(const std::string& n,
                         bool a,
                         const std::set<std::string>& t,
                         const std::map<std::string, int>& td,
                         const std::set<std::string>& c)
      : name(n), active(a), tables(t), table_days(td)
  {
    parseCacheInfo(c);
  }
  std::string name{""};
  bool active{false};
  std::set<std::string> tables;           // Table names
  std::map<std::string, int> table_days;  // Number of day table contains data
  std::set<std::string> caches;           // Cache names
  std::map<std::string, std::string> params;
  std::map<std::string, std::vector<std::string>> params_vector;

  bool parameterExists(const std::string& name) const;
  bool parameterVectorExists(const std::string& name) const;
  int getIntParameterValue(const std::string& name, int defaultValue) const;
  const std::string& getStringParameterValue(const std::string& name,
                                             const std::string& defaultValue) const;
  const CacheInfoItem& getCacheInfo(const std::string& name) const;
  const std::map<std::string, CacheInfoItem>& getCacheInfo() const;

 private:
  void parseCacheInfo(const std::set<std::string>& cacheinfostring);
  std::map<std::string, CacheInfoItem> itsCacheInfoItems;

  friend class DatabaseDriverInfo;
};

class DatabaseDriverInfo
{
 public:
  DatabaseDriverInfo() {}

  void readConfig(Spine::ConfigBase& cfg);

  const DatabaseDriverInfoItem& getDatabaseDriverInfo(const std::string& name) const;
  const std::vector<DatabaseDriverInfoItem>& getDatabaseDriverInfo() const;

  const CacheInfoItem& getAggregateCacheInfo(const std::string& cachename) const;

  const std::map<std::string, CacheInfoItem>& getAggregateCacheInfo() const;

 private:
  void readSpatiaLiteConfig(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readSpatiaLiteConnectInfo(Spine::ConfigBase& cfg,
                                 const std::string& name,
                                 std::map<std::string, std::string>& params);
  void readPostgreSQLConnectInfo(Spine::ConfigBase& cfg,
                                 const std::string& name,
                                 std::map<std::string, std::string>& params);
  void readOracleConnectInfo(Spine::ConfigBase& cfg,
                             const std::string& name,
                             std::map<std::string, std::vector<std::string>>& params_vector);
  void readSpatiaLiteCommonInfo(Spine::ConfigBase& cfg,
                                const std::string& name,
                                std::map<std::string, std::string>& params);
  void readPostgreSQLCommonInfo(Spine::ConfigBase& cfg,
                                const std::string& name,
                                std::map<std::string, std::string>& params);
  void readPostgreSQLMobileCommonInfo(Spine::ConfigBase& cfg,
                                      const std::string& name,
                                      std::map<std::string, std::string>& params);
  void readOracleCommonInfo(Spine::ConfigBase& cfg,
                            const std::string& name,
                            std::map<std::string, std::string>& params);

  void readOracleCommonConfig(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  const libconfig::Setting& lookupDatabase(const std::string& common_key,
                                           const std::string& setting,
                                           const std::string& name,
                                           const std::string& scope,
                                           const libconfig::Config& conf) const;
  void parseCacheInfo(const std::set<std::string>& cacheinfostring,
                      std::set<std::string>& cachenames);

  std::vector<DatabaseDriverInfoItem> itsDatabaseDriverInfoItems;
  std::map<std::string, CacheInfoItem> itsCacheInfoItems;  // Cache name -> cahecinfo
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::DatabaseDriverInfo& driverInfo);
