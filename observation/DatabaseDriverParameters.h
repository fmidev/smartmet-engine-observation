#pragma once

#include "EngineParameters.h"
#include <spine/Value.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
struct FlashEmulatorParameters
{
  bool active{false};
  Spine::BoundingBox bbox;
  unsigned int strokes_per_minute{0};
};
// Common parameters for all database drivers
struct DatabaseDriverParameters
{
  DatabaseDriverParameters(std::string drivername, const EngineParametersPtr& p)
      : driverName(std::move(drivername)), params(p)
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
  std::size_t magnetometerCacheUpdateInterval = 0;
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
  int magnetometerCacheDuration = 0;
  int finCacheUpdateSize = 0;  // in hours, zero for unlimited size
  int extCacheUpdateSize = 0;
  bool quiet = false;
  bool loadStations = false;
  FlashEmulatorParameters flashEmulator;

  const EngineParametersPtr& params;
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
