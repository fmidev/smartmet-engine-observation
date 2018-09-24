#pragma once

#include <engines/geonames/Engine.h>

#include "ObservationCache.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include <macgyver/Cache.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryResultBase;
struct EngineParameters
{
  EngineParameters(Spine::ConfigBase &cfg);

  void readStationTypeConfig(Spine::ConfigBase &cfg);
  Spine::Parameter makeParameter(const std::string &name) const;
  bool isParameter(const std::string &alias, const std::string &stationType) const;
  bool isParameterVariant(const std::string &name) const;
  uint64_t getParameterId(const std::string &alias, const std::string &stationType) const;

  // Cache size settings
  int locationCacheSize;

  std::size_t stationIdCacheSize = 10000;
  std::size_t queryResultBaseCacheSize = 100;
  std::size_t spatiaLitePoolSize;

  std::string serializedStationsFile;
  std::string dbRegistryFolderPath;
  std::string spatiaLiteFile;

  std::map<std::string, std::string> stationTypeMap;
  StationtypeConfig stationtypeConfig;

  ParameterMapPtr parameterMap;

  std::string cacheDB;
  std::string dbDriverFile;

  boost::shared_ptr<StationInfo> stationInfo;
  Fmi::Cache::Cache<std::string, std::vector<Spine::Station>> locationCache;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase>> queryResultBaseCache;

  bool quiet;

  boost::shared_ptr<ObservationCache> observationCache;
  Geonames::Engine *geonames = nullptr;
};

using EngineParametersPtr = boost::shared_ptr<EngineParameters>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
