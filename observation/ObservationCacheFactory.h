#pragma once

#include "ObservationCache.h"

namespace SmartMet {
namespace Engine {
namespace Observation {
class ObservationCache;

class ObservationCacheFactory {
public:
  static ObservationCache *
  create(boost::shared_ptr<ObservationCacheParameters> p);
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
