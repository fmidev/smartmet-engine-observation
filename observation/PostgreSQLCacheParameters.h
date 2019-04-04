#pragma once

#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "LocationItem.h"
#include "Settings.h"
#include "Utils.h"
#include "WeatherDataQCItem.h"
#include <macgyver/PostgreSQLConnection.h>
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
  PostgreSQLCacheParameters(const EngineParametersPtr& p)

      : quiet(p->quiet),
        stationInfo(p->stationInfo),
        parameterMap(p->parameterMap),
        stationtypeConfig(p->stationtypeConfig),
        externalAndMobileProducerConfig(p->externalAndMobileProducerConfig)
  {
  }

  Fmi::Database::PostgreSQLConnectionOptions postgresql;
  int connectionPoolSize;
  std::size_t maxInsertSize = 5000;
  std::size_t dataInsertCacheSize = 0;
  std::size_t weatherDataQCInsertCacheSize = 0;
  std::size_t flashInsertCacheSize = 0;
  std::size_t roadCloudInsertCacheSize = 0;
  std::size_t netAtmoInsertCacheSize = 0;

  bool quiet = true;
  bool cacheHasStations;
  boost::shared_ptr<boost::posix_time::time_period> flashCachePeriod;
  boost::shared_ptr<StationInfo> stationInfo;
  const ParameterMapPtr& parameterMap;
  StationtypeConfig& stationtypeConfig;
  const ExternalAndMobileProducerConfig& externalAndMobileProducerConfig;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
