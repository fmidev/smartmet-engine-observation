#include "DatabaseDriverFactory.h"
#include "DummyDatabaseDriver.h"
#include "SpatiaLiteDatabaseDriverInterface.h"

extern "C"
{
#include <dlfcn.h>
}

namespace SmartMet
{
namespace Engine
{
namespace Observation
{
#ifdef __llvm__
#pragma clang diagnostic push
// observation/DatabaseDriverFactory.cpp:31:9: error: cast between pointer-to-function and
// pointer-to-object is incompatible with C++98 [-Werror,-Wc++98-compat-pedantic]
#pragma clang diagnostic ignored "-Wc++98-compat-pedantic"
#endif

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
      const std::string &driver_id = item.name;
       if (boost::algorithm::starts_with(driver_id, "spatialite_"))
      {              
		// Spatialite driver is created in observation engine
		return (new SpatiaLiteDatabaseDriverInterface(new SpatiaLiteDatabaseDriver(driver_id, p, cfg)));
	  }
	}

	// If no active driver found or dbDriverFile is empty or 'dummy' create dummy driver
    if (!activeDriverFound || p->dbDriverFile.empty() || p->dbDriverFile == "dummy")
	  {
		return new DummyDatabaseDriver(p);
	  }


    void *handle = dlopen(p->dbDriverFile.c_str(), RTLD_NOW);

    if (handle == nullptr)
    {
      // Error occurred while opening the dynamic library
      throw Fmi::Exception(BCP, "Unable to load database driver: " + std::string(dlerror()));
    }

    // Load the symbols (pointers to functions in dynamic library)

    driver_create_t *driver_create_func =
        reinterpret_cast<driver_create_t *>(dlsym(handle, "create"));

    // Check that pointer to create function is loaded succesfully
    if (driver_create_func == nullptr)
    {
      throw Fmi::Exception(BCP, "Cannot load symbols: " + std::string(dlerror()));
    }

    // Create an instance of the class using the pointer to "create" function

    DatabaseDriverInterface *driver = driver_create_func(p, cfg);

    if (driver == nullptr)
    {
      throw Fmi::Exception(BCP, "Unable to create a new instance of database driver class");
    }

    driver->itsHandle = handle;
    return driver;
  }
  catch (...)
  {
    throw Fmi::Exception::Trace(BCP, "Failed to create database driver!");
  }

  // This return would never be executed
  // return nullptr;
}

#ifdef __llvm__
#pragma clang diagnostic pop
#endif

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
