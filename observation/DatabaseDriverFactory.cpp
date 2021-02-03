#include "DatabaseDriverFactory.h"
#include "DatabaseDriverProxy.h"
#include "DummyDatabaseDriver.h"

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

#ifdef __llvm__
#pragma clang diagnostic pop
#endif

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
