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
#include <timeseries/TimeSeriesInclude.h>
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
  SpatiaLiteCacheParameters(const EngineParametersPtr& p)
      : stationInfo(&p->stationInfo),
        quiet(p->quiet),
        stationtypeConfig(p->stationtypeConfig),
        externalAndMobileProducerConfig(p->externalAndMobileProducerConfig),
        parameterMap(p->parameterMap),
        databaseDriverInfo(p->databaseDriverInfo)
  {
  }

  SpatiaLiteOptions sqlite;

  // May be modified by the driver in a separate thread. This is intentionally a reference to the
  // actual stationInfo object in EngineParameters.

  const boost::atomic_shared_ptr<StationInfo>* stationInfo = nullptr;

  std::shared_ptr<boost::posix_time::time_period> flashCachePeriod;
  std::string cacheFile;
  std::size_t maxInsertSize = 5000;
  int connectionPoolSize = 0;
  bool quiet = true;

  const StationtypeConfig& stationtypeConfig;
  const ExternalAndMobileProducerConfig& externalAndMobileProducerConfig;
  const ParameterMapPtr& parameterMap;
  const DatabaseDriverInfo& databaseDriverInfo;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
