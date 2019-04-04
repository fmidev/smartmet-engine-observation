#pragma once

#include "DatabaseDriverInterface.h"
#include "EngineParameters.h"
namespace SmartMet
{
namespace Engine
{
namespace Observation
{
typedef DatabaseDriverInterface* driver_create_t(const EngineParametersPtr& p,
                                                 Spine::ConfigBase& cfg);

class DatabaseDriverFactory
{
 public:
  static DatabaseDriverInterface* create(const EngineParametersPtr& p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
