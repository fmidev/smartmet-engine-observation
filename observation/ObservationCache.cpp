#include "ObservationCache.h"

namespace SmartMet {
namespace Engine {
namespace Observation {

ObservationCache::ObservationCache(
    boost::shared_ptr<ObservationCacheParameters> p)
    : itsParameters(p) {}

ObservationCache::~ObservationCache() {}

bool ObservationCache::cacheHasStations() const {
  return itsParameters->cacheHasStations;
}

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
