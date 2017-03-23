#include "ObservationCacheFactory.h"
#include "SpatiaLiteCache.h"

namespace SmartMet {
namespace Engine {
namespace Observation {

ObservationCache *ObservationCacheFactory::create(
    boost::shared_ptr<ObservationCacheParameters> p) {
  if (p->cacheId == "spatialite")
    return (new SpatiaLiteCache(p));

  return nullptr;
}

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
