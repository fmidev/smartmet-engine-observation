#pragma once

#include "EngineParameters.h"
#include "ExternalAndMobileProducerConfig.h"
#include "ParameterMap.h"
#include <macgyver/DateTime.h>
#include <macgyver/PostgreSQLConnection.h>
#include <cstddef>
#include <memory>

namespace Fmi
{
template <typename Type>
class AtomicSharedPtr;
}  // namespace Fmi
namespace SmartMet::Engine::Observation
{
class DatabaseDriverInfo;
}  // namespace SmartMet::Engine::Observation

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
