#pragma once

#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "Settings.h"
#include "SpatiaLiteOptions.h"
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
  SpatiaLiteCacheParameters(const boost::shared_ptr<Engine::Observation::EngineParameters>& p)
      : quiet(p->quiet),
        stationInfo(p->stationInfo),
        parameterMap(p->parameterMap),
        stationtypeConfig(p->stationtypeConfig)

  {
  }

  SpatiaLiteOptions sqlite;
  int connectionPoolSize;
  std::string cacheFile;
  std::size_t maxInsertSize = 5000;
  bool quiet = true;
  bool cacheHasStations;
  boost::shared_ptr<boost::posix_time::time_period> flashCachePeriod;
  boost::shared_ptr<StationInfo> stationInfo;
  std::map<std::string, std::map<std::string, std::string> >& parameterMap;
  StationtypeConfig& stationtypeConfig;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
