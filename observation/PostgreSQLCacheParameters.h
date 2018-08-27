#pragma once

#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "PostgreSQLOptions.h"
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

struct PostgreSQLCacheParameters
{
  PostgreSQLCacheParameters(const boost::shared_ptr<Engine::Observation::EngineParameters>& p)

      : quiet(p->quiet),
        stationInfo(p->stationInfo),
        parameterMap(p->parameterMap),
        stationtypeConfig(p->stationtypeConfig)
  {
  }

  PostgreSQLOptions postgresql;
  int connectionPoolSize;
  std::size_t maxInsertSize = 5000;
  std::size_t dataInsertCacheSize = 0;
  std::size_t weatherDataQCInsertCacheSize = 0;
  std::size_t flashInsertCacheSize = 0;

  bool quiet = true;
  bool cacheHasStations;
  boost::shared_ptr<boost::posix_time::time_period> flashCachePeriod;
  boost::shared_ptr<StationInfo> stationInfo;
  ParameterMapPtr parameterMap;
  StationtypeConfig& stationtypeConfig;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
