#pragma once

#include "DatabaseDriverInterface.h"
#include "EngineParameters.h"
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
