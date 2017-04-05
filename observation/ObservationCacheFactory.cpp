#include "ObservationCacheFactory.h"
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
  if (p->observationCacheId == "spatialite")
    return (new SpatiaLiteCache(p, cfg));

  return nullptr;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
