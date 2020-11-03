#include "DatabaseDriverFactory.h"
#include "DummyDatabaseDriver.h"
#include "DatabaseDriverProxy.h"

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
	bool activeDriverFound = false;
	
	const std::vector<DatabaseDriverInfoItem> &ddi =
	  p->databaseDriverInfo.getDatabaseDriverInfo();
	
    for (const auto &item : ddi)
	  {
		if (!item.active) continue;
		activeDriverFound = true;
		break;
	  }

	// Create proxy driver which distributes requests to appropriate database driver
    if (activeDriverFound)
	  return new DatabaseDriverProxy(p, cfg);
	
	// If no active driver found  create dummy driver
	return new DummyDatabaseDriver(p);
	
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
