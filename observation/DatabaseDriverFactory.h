#pragma once

#include "DatabaseDriver.h"

namespace SmartMet {
namespace Engine {
namespace Observation {
class DatabaseDriver;
typedef DatabaseDriver *
driver_create_t(boost::shared_ptr<DatabaseDriverParameters>);

class DatabaseDriverFactory {
public:
  static DatabaseDriver *create(boost::shared_ptr<DatabaseDriverParameters> p);
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
