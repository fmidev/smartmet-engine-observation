#pragma once

#include "DataItem.h"
#include "EngineParameters.h"
#include "FlashDataItem.h"
#include "MovingLocationItem.h"
#include "Settings.h"
#include "Utils.h"
#include "DataItem.h"
#include <macgyver/PostgreSQLConnection.h>
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

struct PostgreSQLCacheParameters
{
  explicit PostgreSQLCacheParameters(const EngineParametersPtr& p)
      : quiet(p->quiet),
        stationInfo(&p->stationInfo),
        parameterMap(p->parameterMap),
        stationtypeConfig(p->stationtypeConfig),
        externalAndMobileProducerConfig(p->externalAndMobileProducerConfig),
        databaseDriverInfo(p->databaseDriverInfo)
  {
  }

  Fmi::Database::PostgreSQLConnectionOptions postgresql;
  int connectionPoolSize = 1;
  std::size_t maxInsertSize = 5000;
  std::size_t dataInsertCacheSize = 0;
  std::size_t weatherDataQCInsertCacheSize = 0;
  std::size_t flashInsertCacheSize = 0;
  std::size_t roadCloudInsertCacheSize = 0;
  std::size_t netAtmoInsertCacheSize = 0;
  std::size_t fmiIoTInsertCacheSize = 0;
  std::size_t tapsiQcInsertCacheSize = 0;

  bool quiet = true;
  std::shared_ptr<Fmi::TimePeriod> flashCachePeriod;
  // Externally owned, may be modified by a different thread
  const Fmi::AtomicSharedPtr<StationInfo>* stationInfo;
  const ParameterMapPtr& parameterMap;
  StationtypeConfig& stationtypeConfig;
  const ExternalAndMobileProducerConfig& externalAndMobileProducerConfig;
  const DatabaseDriverInfo& databaseDriverInfo;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
