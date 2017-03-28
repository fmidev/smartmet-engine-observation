#pragma once

#include <engines/geonames/Engine.h>
#include <macgyver/Cache.h>
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

#include <jssatomic/atomic_shared_ptr.hpp>

namespace SmartMet {
namespace Engine {
namespace Observation {
class ObservationCache;
class StationInfo;
class StationtypeConfig;

// Parameters for database drivers
struct DatabaseDriverParameters {
  std::string username;
  std::string password;
  std::string service;
  std::string nlsLang;
  std::string driverFile;
  int connectionPoolSize;
  size_t connectionTimeoutSeconds;
  Geonames::Engine *geonames;
  std::map<std::string, std::map<std::string, std::string> > *parameterMap;
  jss::atomic_shared_ptr<StationInfo> *stationInfo;
  Fmi::Cache::Cache<std::string, std::vector<Spine::Station> > *locationCache;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase> > *
  queryResultBaseCache;
  StationtypeConfig *stationtypeConfig;
  volatile bool *shutdownRequested;
  bool *connectionsOK;
  bool quiet;
  bool timer;
  std::shared_ptr<ObservationCache> observationCache;
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
