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
	
	// If no active driver found  or dbDriverFile is 'dummy' create dummy driver
    if (!activeDriverFound || p->dbDriverFile == "dummy")
	  {
		return new DummyDatabaseDriver(p);
	  }
	
	// Create proxy driver which distributes requests to appropriate database driver
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
