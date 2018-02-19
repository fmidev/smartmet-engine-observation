#include "DatabaseDriver.h"

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
DatabaseDriver::~DatabaseDriver()
{
  // Close the dynamic library
  if (itsHandle)
    dlclose(itsHandle);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
