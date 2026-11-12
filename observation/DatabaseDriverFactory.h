#pragma once

#include "EngineParameters.h"

namespace SmartMet::Engine::Observation
{
class DatabaseDriverInterface;
}  // namespace SmartMet::Engine::Observation
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
class DatabaseDriverFactory
{
 public:
  static DatabaseDriverInterface* create(const EngineParametersPtr& p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
