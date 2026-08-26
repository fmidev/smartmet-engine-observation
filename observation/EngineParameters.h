#pragma once

#include <engines/geonames/Engine.h>

#include "DatabaseDriverInfo.h"
#include "ExternalAndMobileProducerConfig.h"
#include "ObservationCache.h"
#include "ObservationCacheProxy.h"
#include "ProducerGroups.h"
#include "StationInfo.h"
#include "StationtypeConfig.h"
#include <boost/smart_ptr/atomic_shared_ptr.hpp>
#include <macgyver/Cache.h>
#include <spine/Location.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class QueryResultBase;
struct EngineParameters
{
  explicit EngineParameters(Spine::ConfigBase& cfg);

  void readDataQualityConfig(Spine::ConfigBase& cfg);
  void readStationTypeConfig(Spine::ConfigBase& cfg);
  void collectValidStationTypes();
  void readExternalProducerConfig(const std::string& stationtype,
                                  std::string databaseTableName,
                                  const std::vector<uint>& producerIdVector);
  bool isParameter(const std::string& alias, const std::string& stationType) const;
  bool isParameterVariant(const std::string& name) const;
  std::string getParameterIdAsString(const std::string& alias,
                                     const std::string& stationType) const;
  uint64_t getParameterId(const std::string& alias, const std::string& stationType) const;
  bool isExternalOrMobileProducer(const std::string& stationType) const;
  std::set<std::string> getProducerParameters(const std::string& stationType) const;

  // All station types mentioned in the configuration. Collected once at construction: the
  // configuration is read only once, while the plugins ask whether a producer is a station
  // type once per layer or query.
  const std::set<std::string>& getValidStationTypes() const { return validStationTypes; }
  bool isValidStationType(const std::string& stationType) const
  {
    return validStationTypes.find(stationType) != validStationTypes.end();
  }

  // Cache size settings

  // The member initializers below are the single definition of the defaults:
  // the configuration reader falls back to the current member value.
  std::size_t queryResultBaseCacheSize = 1000;
  std::size_t spatiaLitePoolSize = 0;

  // Sizes (max number of entries) of the targeted lookup caches used to speed up
  // repeated station resolution across parallel time steps. The nearest-station
  // candidate lists are cached by StationInfo, the geoid lookups below.
  std::size_t nearestStationsCacheSize = StationInfo::defaultCandidateCacheSize;
  std::size_t geoIdCacheSize = 10000;

  std::string serializedStationsFile;
  std::string dbRegistryFolderPath;
  std::string spatiaLiteFile;

  std::map<std::string, std::string> dataQualityFilters;  // stationtype
  std::set<std::string> validStationTypes;                // collected at construction
  StationtypeConfig stationtypeConfig;
  ExternalAndMobileProducerConfig externalAndMobileProducerConfig;
  ProducerGroups producerGroups;

  ParameterMapPtr parameterMap;

  // How many extra candidate stations to fetch from the spatial index when filtering
  // nearest station results by observation data availability. The engine fetches
  // N + this value candidates and returns at most N that have data. Default is 3.
  int nearestStationExtraCandidates = 3;

  std::string cacheDB;
  std::string dbDriverFile;
  DatabaseDriverInfo databaseDriverInfo;

  // May be modified by the driver in a separate thread. This is the only copy of the
  // StationInfo data, other classes should just point to this one instead of copying
  // the shared pointer.
  mutable Fmi::AtomicSharedPtr<StationInfo> stationInfo;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase>> queryResultBaseCache;

  // (geoid,language) --> resolved locations (Locus idSearch results). Shared by all
  // database drivers, since the resolution depends neither on the driver nor on the
  // observation time.
  mutable Fmi::Cache::Cache<std::string, Spine::LocationList> geoIdCache;

  bool quiet;

  std::shared_ptr<ObservationCacheProxy> observationCacheProxy;
  Geonames::Engine* geonames = nullptr;
};

using EngineParametersPtr = std::shared_ptr<EngineParameters>;

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
