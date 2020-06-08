#pragma once

#include <spine/ConfigBase.h>
#include <set>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
/*!
 * \brief Central holder for database related information.
 *
 * We hold the information of connection strings and other
 * database related settings.
 *
 */
/*
struct CommonDatabaseDriverInfo
{
  std::string serializedStationsFile;
  size_t poolSize{0};
  size_t connectionTimeoutSeconds{30};
  bool timer{false};
  bool disableAllCacheUpdates{false};
  size_t finCacheUpdateInterval{0};
  size_t extCacheUpdateInterval{0};
  size_t flashCacheUpdateInterval{0};
  size_t updateExtraInterval{0};
  int finCacheDuration{0};
  int finMemoryCacheDuration{0};
  int extCacheDuration{0};
  int flashCacheDuration{0};
  int flashMemoryCacheDuration{0};
};
*/
struct DatabaseDriverInfoItem
{
  DatabaseDriverInfoItem() {}
  DatabaseDriverInfoItem(const std::string& n, bool a, std::set<std::string> p)
      : name(n), active(a), producers(p)
  {
  }
  std::string name{""};
  bool active{false};
  std::set<std::string> producers;
  std::map<std::string, std::string> params;
  std::map<std::string, std::vector<std::string>> params_vector;
  // std::map<std::string, CommonDatabaseDriverInfo> common_params;
};

class DatabaseDriverInfo
{
 public:
  DatabaseDriverInfo() {}

  void readConfig(Spine::ConfigBase& cfg);

  const DatabaseDriverInfoItem& getDatabaseDriverInfo(const std::string& name) const;
  const DatabaseDriverInfoItem& getCacheInfo(const std::string& name) const;
  const std::vector<DatabaseDriverInfoItem>& getDatabaseDriverInfo() const;
  const std::vector<DatabaseDriverInfoItem>& getCacheInfo() const;

  //  const CommonDatabaseDriverInfo& getCommonInfo() const { return itsCommonSettings; }

 private:
  void readSpatiaLiteConfig(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readSpatiaLiteConnectInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readPostgreSQLConnectInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readOracleConnectInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readSpatiaLiteCommonInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readPostgreSQLCommonInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readPostgreSQLMobileCommonInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  void readOracleCommonInfo(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);

  void readOracleCommonConfig(Spine::ConfigBase& cfg, DatabaseDriverInfoItem& infoItem);
  const libconfig::Setting& lookupDatabase(const std::string& common_key,
                                           const std::string& setting,
                                           const std::string& name,
                                           const std::string& scope,
                                           const libconfig::Config& conf) const;

  std::vector<DatabaseDriverInfoItem> itsDatabaseDriverInfoItems;
  std::vector<DatabaseDriverInfoItem> itsCacheInfoItems;
  //  CommonDatabaseDriverSettings itsCommonSettings;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet

std::ostream& operator<<(std::ostream& out,
                         const SmartMet::Engine::Observation::DatabaseDriverInfo& driverInfo);
