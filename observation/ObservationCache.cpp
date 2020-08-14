#include "ObservationCache.h"
#include <macgyver/StringConversion.h>

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
ObservationCache::ObservationCache(const std::string& name) : itsCacheName(name) {}

ObservationCache::~ObservationCache() {}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
