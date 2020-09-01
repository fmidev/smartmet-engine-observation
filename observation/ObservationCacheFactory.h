#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"
#include "ObservationCacheProxy.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class ObservationCache;

class ObservationCacheFactory
{
 public:
  static ObservationCacheProxy* create(const EngineParametersPtr& p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
