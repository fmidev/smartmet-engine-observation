#include "ObservationCacheFactory.h"
#include "PostgreSQLCache.h"
#include "SpatiaLiteCache.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCache* ObservationCacheFactory::create(boost::shared_ptr<EngineParameters> p,
                                                  Spine::ConfigBase& cfg)
{
  if (p->cacheDB == "spatialite")
    return (new SpatiaLiteCache(p, cfg));
  else if (p->cacheDB == "postgresql")
    return (new PostgreSQLCache(p, cfg));

  return nullptr;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
