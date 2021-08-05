#pragma once

#include "EngineParameters.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
// Common parameters for all database drivers
struct DatabaseDriverParameters
{
  DatabaseDriverParameters(const std::string& drivername, const EngineParametersPtr& p)
      : driverName(drivername), params(p)
  {
  }

  std::string driverName;
  std::vector<int> connectionPoolSize;
  size_t connectionTimeoutSeconds = 0;
  bool disableAllCacheUpdates = false;
  std::size_t finCacheUpdateInterval = 0;
  std::size_t extCacheUpdateInterval = 0;
  std::size_t flashCacheUpdateInterval = 0;
  std::size_t netAtmoCacheUpdateInterval = 0;
  std::size_t roadCloudCacheUpdateInterval = 0;
  std::size_t fmiIoTCacheUpdateInterval = 0;
  std::size_t bkHydrometaCacheUpdateInterval = 0;
  std::size_t stationsCacheUpdateInterval = 0;
  int updateExtraInterval = 10;  // update 10 seconds before max(modified_last) for safety
  int finCacheDuration = 0;
  int finMemoryCacheDuration = 0;
  int extCacheDuration = 0;
  int flashCacheDuration = 0;
  int flashMemoryCacheDuration = 0;
  int netAtmoCacheDuration = 0;
  int roadCloudCacheDuration = 0;
  int fmiIoTCacheDuration = 0;
  int bkHydrometaCacheDuration = 0;
  bool quiet = false;
  bool loadStations = false;

  const EngineParametersPtr& params;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
