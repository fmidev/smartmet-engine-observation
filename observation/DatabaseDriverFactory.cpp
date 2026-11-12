#include "DatabaseDriverFactory.h"
#include "DatabaseDriverProxy.h"
#include <macgyver/Exception.h>
#include <string>

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
DatabaseDriverInterface *DatabaseDriverFactory::create(const EngineParametersPtr &p,
                                                       Spine::ConfigBase &cfg)
{
  try
  {
    return new DatabaseDriverProxy(p, cfg);
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to create database driver!");
  }
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
