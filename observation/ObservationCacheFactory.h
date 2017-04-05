#pragma once

#include "ObservationCache.h"
#include "EngineParameters.h"

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
  static ObservationCache* create(boost::shared_ptr<EngineParameters> p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
