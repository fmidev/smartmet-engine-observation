#include "DatabaseDriverFactory.h"
#include "DummyDatabaseDriver.h"
#include "SpatiaLiteDatabaseDriver.h"
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
DatabaseDriver *DatabaseDriverFactory::create(boost::shared_ptr<EngineParameters> p,
                                              Spine::ConfigBase &cfg)
{
  try
  {
    if (p->dbDriverFile.empty() ||
        p->dbDriverFile == "dummy")  // if no filename given create dummy driver
      return (new DummyDatabaseDriver());
    else if (p->dbDriverFile == "spatialite")
      return (new SpatiaLiteDatabaseDriver(p, cfg));

    void *handle = dlopen(p->dbDriverFile.c_str(), RTLD_NOW);

    if (handle == 0)
    {
      // Error occurred while opening the dynamic library
      throw Spine::Exception(BCP, "Unable to load database driver: " + std::string(dlerror()));
    }

    // Load the symbols (pointers to functions in dynamic library)

    driver_create_t *driver_create_func =
        reinterpret_cast<driver_create_t *>(dlsym(handle, "create"));

    // Check that pointer to create function is loaded succesfully
    if (driver_create_func == 0)
    {
      throw Spine::Exception(BCP, "Cannot load symbols: " + std::string(dlerror()));
    }

    // Create an instance of the class using the pointer to "create" function

    DatabaseDriver *driver = driver_create_func(p, cfg);

    if (driver == 0)
    {
      throw Spine::Exception(BCP, "Unable to create a new instance of database driver class");
    }

    driver->itsHandle = handle;
    return driver;
  }
  catch (...)
  {
    throw Spine::Exception::Trace(BCP, "Failed to create database driver!");
  }

  return nullptr;
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
