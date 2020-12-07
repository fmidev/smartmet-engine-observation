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
namespace ObservationCacheFactory
{
boost::shared_ptr<ObservationCacheProxy> create(const EngineParametersPtr& p,
                                              const Spine::ConfigBase& cfg);
}  // namespace ObservationCacheFactory
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
