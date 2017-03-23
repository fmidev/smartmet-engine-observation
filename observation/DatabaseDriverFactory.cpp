#include "DatabaseDriverFactory.h"
#include "DummyDatabaseDriver.h"
#include "SpatiaLiteDatabaseDriver.h"
#include "DatabaseDriverParameters.h"
#include <delfoi/OracleDatabaseDriver.h>

namespace SmartMet {
namespace Engine {
namespace Observation {

DatabaseDriver *
DatabaseDriverFactory::create(boost::shared_ptr<DatabaseDriverParameters> p) {

  if (p->driverId == "oracle")
    return (new OracleDatabaseDriver(p));
  if (p->driverId == "spatialite")
    return (new SpatiaLiteDatabaseDriver(p));
  else if (p->driverId == "dummy")
    return (new DummyDatabaseDriver(p));

  return nullptr;
}

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
