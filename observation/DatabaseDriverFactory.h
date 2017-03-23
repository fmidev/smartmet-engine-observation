#pragma once

#include "DatabaseDriver.h"

namespace SmartMet {
namespace Engine {
namespace Observation {
class DatabaseDriver;

class DatabaseDriverFactory {
public:
  static DatabaseDriver *create(boost::shared_ptr<DatabaseDriverParameters> p);
};

} // namespace Observation
} // namespace Engine
} // namespace SmartMet
