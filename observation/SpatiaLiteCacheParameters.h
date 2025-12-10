#pragma once

#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "MovingLocationItem.h"
#include "Settings.h"
#include "SpatiaLiteOptions.h"
#include "Utils.h"
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
  explicit SpatiaLiteCacheParameters(const EngineParametersPtr& p)
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

  const Fmi::AtomicSharedPtr<StationInfo>* stationInfo = nullptr;

  std::shared_ptr<Fmi::TimePeriod> flashCachePeriod;
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
