#include "ObservationCacheFactory.h"
#include "DummyCache.h"
#include "PostgreSQLCache.h"
#include "SpatiaLiteCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCache* ObservationCacheFactory::create(const EngineParametersPtr& p,
                                                  Spine::ConfigBase& cfg)
{
  if (p->cacheDB == "spatialite")
    return (new SpatiaLiteCache(p, cfg));
  else if (p->cacheDB == "postgresql")
    return (new PostgreSQLCache(p, cfg));
  else if (p->cacheDB == "dummy")
    return (new DummyCache(p));

  return nullptr;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
