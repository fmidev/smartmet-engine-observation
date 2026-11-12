#pragma once

#include "EngineParameters.h"
#include "ObservationCacheProxy.h"
#include <memory>

namespace SmartMet::Spine
{
class ConfigBase;
}  // namespace SmartMet::Spine

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
namespace ObservationCacheFactory
{
std::shared_ptr<ObservationCacheProxy> create(const EngineParametersPtr& p,
                                              const Spine::ConfigBase& cfg);
}  // namespace ObservationCacheFactory
}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
