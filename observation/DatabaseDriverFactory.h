#pragma once

#include "DatabaseDriver.h"
#include "EngineParameters.h"

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
class DatabaseDriver;
typedef DatabaseDriver* driver_create_t(const EngineParametersPtr& p, Spine::ConfigBase& cfg);

class DatabaseDriverFactory
{
 public:
  static DatabaseDriver* create(const EngineParametersPtr& p, Spine::ConfigBase& cfg);
};

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
