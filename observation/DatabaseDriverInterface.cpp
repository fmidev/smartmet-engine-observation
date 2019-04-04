#include "DatabaseDriverInterface.h"

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
DatabaseDriverInterface::~DatabaseDriverInterface()
{
  // Close the dynamic library
  if (itsHandle)
    dlclose(itsHandle);
}

}  // namespace Observation
}  // namespace Engine
}  // namespace SmartMet
