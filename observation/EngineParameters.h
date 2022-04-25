#pragma once

#include <engines/geonames/Engine.h>

#include "DatabaseDriverInfo.h"
#include "ExternalAndMobileProducerConfig.h"
#include "ObservationCache.h"
#include "ObservationCacheProxy.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include "ProducerGroups.h"
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
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

  void readDataQualityConfig(Spine::ConfigBase &cfg);
  void readStationTypeConfig(Spine::ConfigBase &cfg);
  bool isParameter(const std::string &alias, const std::string &stationType) const;
  bool isParameterVariant(const std::string &name) const;
  std::string getParameterIdAsString(const std::string &alias,
                                     const std::string &stationType) const;
  uint64_t getParameterId(const std::string &alias, const std::string &stationType) const;
  bool isExternalOrMobileProducer(const std::string &stationType) const;

  // Cache size settings

  std::size_t queryResultBaseCacheSize = 100;
  std::size_t spatiaLitePoolSize = 0;

  std::string serializedStationsFile;
  std::string dbRegistryFolderPath;
  std::string spatiaLiteFile;

  std::map<std::string, std::string> dataQualityFilters;  // stationtype
  StationtypeConfig stationtypeConfig;
  ExternalAndMobileProducerConfig externalAndMobileProducerConfig;
  ProducerGroups producerGroups;

  ParameterMapPtr parameterMap;

  std::string cacheDB;
  std::string dbDriverFile;
  DatabaseDriverInfo databaseDriverInfo;

  // May be modified by the driver in a separate thread. This is the only copy of the
  // StationInfo data, other classes should just point to this one instead of copying
  // the shared pointer.
  mutable boost::atomic_shared_ptr<StationInfo> stationInfo;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase>> queryResultBaseCache;

  bool quiet;

  std::shared_ptr<ObservationCacheProxy> observationCacheProxy;
  Geonames::Engine *geonames = nullptr;
};

using EngineParametersPtr = std::shared_ptr<EngineParameters>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
