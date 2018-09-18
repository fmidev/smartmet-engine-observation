#pragma once

#include "EngineParameters.h"
#include "ObservationCache.h"

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
  static ObservationCache* create(const EngineParametersPtr& p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
