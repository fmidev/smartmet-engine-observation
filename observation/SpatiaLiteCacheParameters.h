#pragma once

#include "DataItem.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "Settings.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <spine/Station.h>
#include <spine/TimeSeries.h>
#include <spine/TimeSeriesGeneratorOptions.h>

#include <macgyver/Cache.h>

#include <string>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class StationInfo;
class StationtypeConfig;

struct SpatiaLiteCacheParameters
{
  SpatiaLiteCacheParameters(boost::shared_ptr<Engine::Observation::EngineParameters> p)
      : quiet(p->quiet),
        stationInfo(p->stationInfo),
        parameterMap(p->parameterMap),
        stationtypeConfig(p->stationtypeConfig)

  {
  }

  int connectionPoolSize;
  std::string cacheFile;
  std::size_t maxInsertSize;
  std::string synchronous;
  std::string journalMode;
  std::size_t mmapSize;
  std::string threadingMode;
  bool memstatus;
  bool sharedCache;
  int cacheTimeout;
  bool quiet;
  bool cacheHasStations;
  boost::shared_ptr<boost::posix_time::time_period> flashCachePeriod;
  boost::shared_ptr<StationInfo> stationInfo;
  std::map<std::string, std::map<std::string, std::string> >& parameterMap;
  StationtypeConfig& stationtypeConfig;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
