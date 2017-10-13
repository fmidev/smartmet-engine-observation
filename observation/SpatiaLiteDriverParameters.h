#pragma once

#include "EngineParameters.h"
#include <engines/geonames/Engine.h>
#include <macgyver/Cache.h>
#include <spine/Station.h>
//#include <spine/TimeSeries.h>
//#include <spine/TimeSeriesGeneratorOptions.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCache;
class StationInfo;
class StationtypeConfig;
class QueryResultBase;

struct SpatiaLiteDriverParameters
{
  SpatiaLiteDriverParameters(boost::shared_ptr<EngineParameters> p)
      : parameterMap(p->parameterMap),
        stationInfo(p->stationInfo),
        locationCache(p->locationCache),
        queryResultBaseCache(p->queryResultBaseCache),
        observationCache(p->observationCache),
        stationtypeConfig(p->stationtypeConfig),
        shutdownRequested(&p->shutdownRequested)
  {
  }

  Geonames::Engine* geonames;
  std::map<std::string, std::map<std::string, std::string> >& parameterMap;
  boost::shared_ptr<StationInfo> stationInfo;
  Fmi::Cache::Cache<std::string, std::vector<Spine::Station> >& locationCache;
  Fmi::Cache::Cache<std::string, std::shared_ptr<QueryResultBase> >& queryResultBaseCache;
  boost::shared_ptr<ObservationCache> observationCache;
  StationtypeConfig& stationtypeConfig;
  volatile bool* shutdownRequested;
  bool quiet;
  int finCacheDuration = 0;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
