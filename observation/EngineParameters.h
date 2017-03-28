#pragma once

#include "StationInfo.h"
#include "StationtypeConfig.h"
#include "ObservationCache.h"
#include "DatabaseDriver.h"
#include <macgyver/Cache.h>
#include <spine/Parameter.h>

namespace SmartMet {
namespace Engine {
namespace Observation {
class QueryResultBase;
struct EngineParameters {
  EngineParameters(const std::string configfile);

  void readStationTypeConfig(const std::string &configfile);
  Spine::Parameter makeParameter(const std::string &name) const;
  bool isParameter(const std::string &alias,
                   const std::string &stationType) const;
  bool isParameterVariant(const std::string &name) const;
  uint64_t getParameterId(const std::string &alias,
                          const std::string &stationType) const;

  size_t connectionTimeoutSeconds = 0;

  // Database specific variables
  std::string username;
  std::string password;
  std::string service;
  std::string nlsLang;
  // sqlite settings
  std::string threadingMode;
  int cacheTimeout;
  bool sharedCache;
  bool memstatus;
  std::string synchronous;
  std::string journalMode;
  std::size_t mmapSize;

  // Cache size settings
  int locationCacheSize;
  // Cache updates
  bool disableUpdates = false;
  std::size_t finUpdateInterval;
  std::size_t extUpdateInterval;
  std::size_t flashUpdateInterval;

  // How many hours to keep observations in SpatiaLite database
  int spatialiteCacheDuration;
  // The time interval which is cached in the observation_data table (Finnish
  // observations)
  jss::atomic_shared_ptr<boost::posix_time::time_period> spatialitePeriod;
  // The time interval which is cached in the weather_data_qc table (Foreign &
  // road observations)
  jss::atomic_shared_ptr<boost::posix_time::time_period> qcDataPeriod;
  // How many hours to keep flash data in SpatiaLite
  int spatialiteFlashCacheDuration;
  // The time interval for flash observations which is cached in the SpatiaLite
  jss::atomic_shared_ptr<boost::posix_time::time_period> flashPeriod;
  // Max inserts in one commit
  std::size_t maxInsertSize;

  size_t queryResultBaseCacheSize = 100;
  int poolSize;
  int spatiaLitePoolSize;

  std::string serializedStationsFile;
  std::string dbRegistryFolderPath;
  std::string spatiaLiteFile;

  bool preloaded = false;
  bool forceReload = false;

  //  bool spatiaLiteHasStations = false;

  std::map<std::string, std::string> stationTypeMap;
  StationtypeConfig stationtypeConfig;
  //  volatile int activeThreadCount = 0;
  volatile bool shutdownRequested = false;

  using ParameterMap =
      std::map<std::string, std::map<std::string, std::string> >;
  ParameterMap parameterMap;

  std::string observationCacheId;
  std::string dbDriverFile;

  jss::atomic_shared_ptr<StationInfo> stationInfo;
  Fmi::Cache::Cache<std::string, std::vector<Spine::Station> > locationCache;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase> >
  queryResultBaseCache;

  bool quiet;
  bool timer;

  bool connectionsOK = false;

  boost::shared_ptr<ObservationCacheParameters> observationCacheParameters;
  boost::shared_ptr<DatabaseDriverParameters> databaseDriverParameters;
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
